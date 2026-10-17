"""Tests for the remote live view of the capture browser.

On a machine with no screen (the Raspberry Pi this was written for) the CAPTCHA
browser runs where nobody can see it, and the dashboard drives it over DevTools:
remote_frame() sends a JPEG out, remote_input() sends clicks and keys back in.

That input path is the part worth pinning. It takes coordinates and text from a
browser over the network and turns them into real events in a real browser
session, so the guards on it are load-bearing, and nothing about a passing
download would reveal that one had been dropped.

RemoteViewTests in test_capture_regressions.py already covers the click
sequence, the coordinate clamp, text handling, Enter, the frame guard and the
Xvfb fallback. This file picks up what that leaves: the DevTools socket
underneath (choosing a tab, reusing it, matching replies), the dashboard
endpoint above it, and two unrelated fixes that rode in on the same branch.
"""

import re
import socket
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from browser_capture import CaptureSession  # noqa: E402
from ps5_streamer import Manager  # noqa: E402
from transfer_core import TransferError, valid_url  # noqa: E402


def waiting_session(url="https://host.tld/f/abc"):
    """A session in the only state that accepts remote input, with the DevTools
    round-trip replaced by a recorder."""
    s = CaptureSession(url)
    s.state = "waiting"
    s.calls = []
    s._remote_call = lambda method, params=None, want_result=False: (
        s.calls.append((method, params or {})) or {})
    return s


# ---------------------------------------------------------------------------
# remote_input: events arriving from the network
# ---------------------------------------------------------------------------

class RemoteInputTests(unittest.TestCase):
    def test_missing_coordinates_default_to_the_origin(self):
        s = waiting_session()
        s.remote_input({"type": "click"})
        self.assertEqual([p["x"] for _, p in s.calls], [0, 0, 0])

    def test_a_non_numeric_coordinate_raises_for_the_caller_to_catch(self):
        # Documents today's behaviour: float() rejects it, and browser_resolve
        # turns that into a readable error rather than a 500.
        with self.assertRaises((ValueError, TypeError)):
            waiting_session().remote_input({"type": "click", "x": "abc", "y": 0})

    def test_wheel_clamps_delta_and_defaults_dx(self):
        s = waiting_session()
        s.remote_input({"type": "wheel", "x": 10, "y": 20, "dy": 999999})
        (method, p), = s.calls
        self.assertEqual(method, "Input.dispatchMouseEvent")
        self.assertEqual(p["type"], "mouseWheel")
        self.assertEqual(p["deltaY"], 3000)
        self.assertEqual(p["deltaX"], 0)   # absent dx must not be forwarded as junk

        s = waiting_session()
        s.remote_input({"type": "wheel", "x": 0, "y": 0, "dx": -99999, "dy": -99999})
        (_, p), = s.calls
        self.assertEqual((p["deltaX"], p["deltaY"]), (-3000, -3000))

    def test_known_key_sends_down_and_up(self):
        s = waiting_session()
        s.remote_input({"type": "key", "key": "Backspace"})
        self.assertEqual([p["type"] for _, p in s.calls], ["keyDown", "keyUp"])
        for _, p in s.calls:
            self.assertEqual(p["windowsVirtualKeyCode"], 8)
            self.assertEqual(p["nativeVirtualKeyCode"], 8)
            self.assertNotIn("text", p)    # only Enter carries text

    def test_unmapped_key_and_unknown_type_are_ignored(self):
        for ev in ({"type": "key", "key": "F11"},
                   {"type": "key", "key": ""},
                   {"type": "teleport", "x": 1},
                   {},
                   {"type": "text"}):
            s = waiting_session()
            s.remote_input(ev)
            self.assertEqual(s.calls, [], ev)

    def test_input_is_refused_unless_the_session_is_waiting(self):
        # The guard that matters: once a session is done, errored or cancelled
        # its tab may be showing something else entirely, or be shutting down.
        for state in ("starting", "done", "error", "cancelled"):
            s = waiting_session()
            s.state = state
            s.remote_input({"type": "click", "x": 10, "y": 10})
            s.remote_input({"type": "text", "text": "password"})
            self.assertEqual(s.calls, [], state)



# ---------------------------------------------------------------------------
# _remote_ws: choosing and reusing the DevTools socket
# ---------------------------------------------------------------------------

class FakeWS:
    def __init__(self, url, timeout=5):
        self.url = url
        self.closed = False
        self.sent = []
        self.inbox = []

    def send(self, msg):
        self.sent.append(msg)

    def recv(self):
        if not self.inbox:
            raise socket.timeout()
        return self.inbox.pop(0)

    def close(self):
        self.closed = True


def page(tid, url="https://host.tld/f/abc"):
    return {"id": tid, "type": "page", "url": url,
            "webSocketDebuggerUrl": "ws://127.0.0.1/devtools/page/" + tid}


class RemoteWsTests(unittest.TestCase):
    def session(self, targets, navigated=True, port=9222):
        s = CaptureSession("https://host.tld/f/abc")
        s.state = "waiting"
        s.devtools_port = port
        s._navigated = navigated
        s._devtools = lambda p, path, timeout=2.0: targets
        return s

    def test_no_browser_yet(self):
        s = self.session([page("A")], port=None)
        with self.assertRaises(RuntimeError):
            s._remote_ws()

    def test_no_page_target(self):
        s = self.session([{"id": "X", "type": "service_worker"}])
        with self.assertRaises(RuntimeError):
            s._remote_ws()

    def test_blank_start_page_is_refused(self):
        # Screenshotting the tab during its first navigation left it stuck on a
        # blank page, so the live view has to wait for the real page.
        s = self.session([page("A", "about:blank")])
        with self.assertRaises(RuntimeError):
            s._remote_ws()
        s = self.session([page("A")], navigated=False)
        with self.assertRaises(RuntimeError):
            s._remote_ws()

    def test_socket_is_reused_for_the_same_tab(self):
        s = self.session([page("A")])
        with mock.patch("browser_capture.WebSocket", FakeWS):
            first = s._remote_ws()
            self.assertIs(s._remote_ws(), first)
            self.assertFalse(first.closed)

    def test_switching_tab_closes_the_old_socket(self):
        targets = [page("A")]
        s = self.session(targets)
        with mock.patch("browser_capture.WebSocket", FakeWS):
            first = s._remote_ws()
            targets[:] = [page("B")]
            second = s._remote_ws()
        self.assertIsNot(second, first)
        self.assertTrue(first.closed)

    def test_the_main_tab_wins_over_a_popup(self):
        s = self.session([page("POPUP"), page("MAIN")])
        s._main_tid = "MAIN"
        with mock.patch("browser_capture.WebSocket", FakeWS):
            self.assertTrue(s._remote_ws().url.endswith("/MAIN"))

    def test_cleanup_closes_the_remote_socket(self):
        s = self.session([page("A")])
        with mock.patch("browser_capture.WebSocket", FakeWS):
            ws = s._remote_ws()
        s._cleanup()
        self.assertTrue(ws.closed)
        self.assertIsNone(s._rws)


# ---------------------------------------------------------------------------
# _remote_call: matching replies on a socket shared with event traffic
# ---------------------------------------------------------------------------

class RemoteCallTests(unittest.TestCase):
    def call_with(self, inbox, **kw):
        s = CaptureSession("https://host.tld/f/abc")
        s.state = "waiting"
        s._rid = 100
        ws = FakeWS("ws://x")
        ws.inbox = inbox
        s._remote_ws = lambda: ws
        return s, ws, s._remote_call("Page.captureScreenshot", {"format": "jpeg"}, **kw)

    def test_unrelated_messages_are_skipped(self):
        # This socket also carries CDP events; a reply is matched by id, never
        # by arrival order.
        s, ws, result = self.call_with([
            {"method": "Page.frameNavigated", "params": {}},
            {"id": 7, "result": {"stale": True}},
            {"id": 101, "result": {"data": "JPEG"}},
        ])
        self.assertEqual(result, {"data": "JPEG"})
        self.assertEqual(ws.sent[0]["id"], 101)
        self.assertEqual(ws.sent[0]["method"], "Page.captureScreenshot")

    def test_ids_keep_increasing(self):
        s = CaptureSession("https://host.tld/f/abc")
        s.state = "waiting"
        s._rid = 100
        ws = FakeWS("ws://x")
        s._remote_ws = lambda: ws
        for n in (101, 102, 103):
            ws.inbox = [{"id": n, "result": {}}]
            s._remote_call("Input.dispatchMouseEvent")
        self.assertEqual([m["id"] for m in ws.sent], [101, 102, 103])

    def test_devtools_error_is_raised(self):
        with self.assertRaises(RuntimeError) as cm:
            self.call_with([{"id": 101, "error": {"message": "Target closed"}}])
        self.assertIn("Target closed", str(cm.exception))

    def test_silence_times_out(self):
        # time.time(): once for the deadline, then once per loop check.
        with mock.patch("browser_capture.time.time", side_effect=[0, 0, 99]):
            with self.assertRaises(TimeoutError):
                self.call_with([])


# ---------------------------------------------------------------------------
# The dashboard endpoint wrapping these calls
# ---------------------------------------------------------------------------

class FakeSession:
    state = "waiting"
    started_at = 0

    def __init__(self, frame=None, boom=None):
        self._frame = frame or {"frame": "B64", "width": 1100, "height": 860}
        self._boom = boom
        self.events = []

    def snapshot(self):
        return {"state": self.state, "url": "", "error": "", "remaining": 30,
                "skipped": [], "blocked_popups": []}

    def remote_frame(self):
        if self._boom:
            raise self._boom
        return dict(self._frame)

    def remote_input(self, ev):
        if self._boom:
            raise self._boom
        self.events.append(ev)


class BrowserResolveEndpointTests(unittest.TestCase):
    def manager_with(self, session):
        m = Manager(tempfile.mkdtemp())
        m.captures["sid1"] = session
        return m

    def test_frame_action_returns_the_image_with_the_snapshot(self):
        m = self.manager_with(FakeSession())
        out = m.browser_resolve({"action": "frame", "id": "sid1"})
        self.assertEqual(out["frame"], "B64")
        self.assertEqual(out["id"], "sid1")
        self.assertEqual(out["state"], "waiting")

    def test_input_action_forwards_the_event(self):
        s = FakeSession()
        out = self.manager_with(s).browser_resolve(
            {"action": "input", "id": "sid1", "event": {"type": "click", "x": 1, "y": 2}})
        self.assertEqual(s.events, [{"type": "click", "x": 1, "y": 2}])
        self.assertNotIn("frame", out)

    def test_a_missing_event_is_not_a_crash(self):
        s = FakeSession()
        self.manager_with(s).browser_resolve({"action": "input", "id": "sid1"})
        self.assertEqual(s.events, [{}])

    def test_browser_failures_become_a_readable_error(self):
        # A dead tab must reach the dashboard as a message, not a 500.
        for action in ("frame", "input"):
            m = self.manager_with(FakeSession(boom=RuntimeError("Target closed")))
            with self.assertRaises(TransferError) as cm:
                m.browser_resolve({"action": action, "id": "sid1"})
            self.assertIn("Target closed", str(cm.exception))

    def test_unknown_session_is_rejected_before_any_browser_work(self):
        m = self.manager_with(FakeSession())
        for action in ("frame", "input"):
            with self.assertRaises(TransferError):
                m.browser_resolve({"action": action, "id": "nope"})


# ---------------------------------------------------------------------------
# URL encoding (same branch): filenames with spaces must not fail the request
# ---------------------------------------------------------------------------

class UrlEncodingTests(unittest.TestCase):
    def test_spaces_are_encoded(self):
        # http.client rejects a request line containing a raw space, so a link
        # copied straight from a host used to fail outright.
        self.assertEqual(valid_url("https://h.tld/Game (USA) v1.pkg"),
                         "https://h.tld/Game%20(USA)%20v1.pkg")

    def test_existing_escapes_are_not_doubled(self):
        self.assertEqual(valid_url("https://h.tld/Game%20(USA).pkg"),
                         "https://h.tld/Game%20(USA).pkg")

    def test_query_and_fragment_survive(self):
        self.assertEqual(valid_url("https://h.tld/p/f.pkg?a=1&b=2#frag"),
                         "https://h.tld/p/f.pkg?a=1&b=2#frag")

    def test_non_ascii_is_encoded_rather_than_rejected(self):
        self.assertEqual(valid_url("https://h.tld/Pokémon.pkg"),
                         "https://h.tld/Pok%C3%A9mon.pkg")

    def test_bad_urls_are_still_refused(self):
        for bad in ("ftp://h.tld/f.pkg", "https://user:pw@h.tld/f.pkg",
                    "not a url", "https:///f.pkg"):
            with self.assertRaises(TransferError, msg=bad):
                valid_url(bad)


# ---------------------------------------------------------------------------
# The PS5 payload's extract_result line
# ---------------------------------------------------------------------------

REASON = re.compile(r"reason=(.+?)(?=\s+\w+=|$)")


class ExtractReasonTests(unittest.TestCase):
    def test_a_multi_word_reason_is_captured_whole(self):
        # The old [^\s]+ pattern reported "no" for "no space left on device",
        # which told the user nothing.
        m = REASON.search("extract_result code=1 reason=no space left on device path=/data")
        self.assertEqual(m.group(1), "no space left on device")

    def test_a_reason_at_the_end_of_the_line_is_captured(self):
        self.assertEqual(REASON.search("extract_result code=2 reason=bad password").group(1),
                         "bad password")

    def test_a_single_word_reason_still_works(self):
        self.assertEqual(REASON.search("extract_result code=0 reason=success").group(1),
                         "success")

    def test_it_stops_before_the_next_field(self):
        self.assertEqual(REASON.search("extract_result reason=wrong key code=3").group(1),
                         "wrong key")


if __name__ == "__main__":
    unittest.main()
