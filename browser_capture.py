"""Direct Stream for PlayStation 5 — Browser-assisted link capture.

Some file hosts (AkiraBox, 1fichier, DataNodes, VikingFile, ...) only hand out a
direct download URL after a human solves a CAPTCHA or waits out a timer. Those
links cannot be resolved from the server side.

This module opens a real browser window on the landing page so the user can solve
the check themselves, then captures the direct URL the site hands out via the
Chrome DevTools Protocol. The download itself is denied, so no bytes are written
to disk -- only the URL is kept, and it is handed back to the transfer engine.

It deliberately does NOT attempt to defeat any CAPTCHA: the human solves it.

Pure standard library (no websocket/requests packages): a minimal RFC 6455 client
is implemented here because the project ships with zero pip dependencies.
"""

from __future__ import annotations

import base64
import json
import os
import re
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


from resolver import SUPPORTED_EXTENSIONS, is_installer_url

CAPTURE_TIMEOUT = 300          # seconds a capture window may stay open
SW_SHOWNORMAL = 1              # Windows: show the window normally, not minimised
SW_RESTORE = 9
HWND_TOPMOST = -1
HWND_NOTOPMOST = -2
SWP_NOMOVE = 0x0002
SWP_NOSIZE = 0x0001
SWP_SHOWWINDOW = 0x0040


def raise_browser_window(pid: int, timeout: float = 10.0) -> bool:
    """Bring the capture window to the foreground on Windows.

    A background process cannot simply call SetForegroundWindow -- Windows'
    foreground lock ignores it, which is why the window appeared behind the
    existing ones. The reliable trick is to toggle the window topmost and
    straight back again, which raises it without leaving it always-on-top.
    Returns True once the window actually holds the foreground.
    """
    if os.name != "nt":
        return False
    try:
        import ctypes
        from ctypes import wintypes
    except Exception:
        return False

    user32 = ctypes.windll.user32
    user32.SetWindowPos.argtypes = [wintypes.HWND, wintypes.HWND, ctypes.c_int,
                                    ctypes.c_int, ctypes.c_int, ctypes.c_int,
                                    ctypes.c_uint]
    enum_proc = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)

    def find_window():
        found = []

        def callback(hwnd, _):
            if not user32.IsWindowVisible(hwnd):
                return True
            owner = wintypes.DWORD()
            user32.GetWindowThreadProcessId(hwnd, ctypes.byref(owner))
            if owner.value != pid:
                return True
            if user32.GetWindowTextLengthW(hwnd) <= 0:
                return True        # skip Chrome's hidden helper windows
            found.append(hwnd)
            return False

        user32.EnumWindows(enum_proc(callback), 0)
        return found[0] if found else None

    deadline = time.time() + timeout
    while time.time() < deadline:
        hwnd = find_window()
        if hwnd:
            try:
                user32.ShowWindow(hwnd, SW_RESTORE)
                flags = SWP_NOMOVE | SWP_NOSIZE | SWP_SHOWWINDOW
                user32.SetWindowPos(hwnd, HWND_TOPMOST, 0, 0, 0, 0, flags)
                user32.SetWindowPos(hwnd, HWND_NOTOPMOST, 0, 0, 0, 0, flags)
                user32.SetForegroundWindow(hwnd)
            except Exception:
                return False
            fg = user32.GetForegroundWindow()
            fg_pid = wintypes.DWORD()
            user32.GetWindowThreadProcessId(fg, ctypes.byref(fg_pid))
            if fg_pid.value == pid:
                return True
        time.sleep(0.25)
    return False

# Only these are accepted as a capture result. These pages open ad popups that
# start their own downloads (installers, tracking .txt files), so an allowlist of
# real payload types is used -- blocking known-bad extensions is not enough,
# because ad URLs often carry no file extension at all.
_PAYLOAD_RE = re.compile(
    r"\.(?:" + "|".join(SUPPORTED_EXTENSIONS) + r"|\d{3})(?:$|[?#])", re.I)


# Per-host capture policy.
#   family  : domains that belong to the SAME service. Hosts that canonicalise
#             across domains (akirabox.com -> akirabox.to) would otherwise look
#             like an advert hijack the moment they redirect.
#   recover : how to rescue a tab an advert has hijacked --
#             "back"   use history.back(), preserving multi-click state
#             "reload" reload the landing page (only for hosts that refuse bfcache)
#             "none"   leave the tab alone
CAPTURE_POLICIES = (
    (re.compile(r"(?:^|\.)akirabox\.(?:to|com)$", re.I),
     {"family": re.compile(r"(?:^|\.)akirabox\.(?:to|com)$", re.I), "recover": "back"}),
    (re.compile(r"(?:^|\.)vik(?:ing|1ng)file\.[a-z]{2,}$", re.I),
     {"family": re.compile(r"(?:^|\.)vik(?:ing|1ng)file\.[a-z]{2,}$", re.I), "recover": "back"}),
    (re.compile(r"(?:^|\.)datanodes\.to$", re.I),
     {"family": re.compile(r"(?:^|\.)datanodes\.(?:to|com)$", re.I), "recover": "back"}),
    (re.compile(r"(?:^|\.)(?:buzzheavier\.com|bzzhr\.to)$", re.I),
     {"family": re.compile(r"(?:^|\.)(?:buzzheavier\.com|bzzhr\.to)$", re.I), "recover": "back"}),
    (re.compile(r"(?:^|\.)fileditch(?:files)?\.[a-z]{2,}$", re.I),
     {"family": re.compile(r"(?:^|\.)(?:new\.|files\.)?fileditch(?:files)?\.[a-z]{2,}$", re.I),
      "recover": "back"}),
)
DEFAULT_POLICY = {"family": None, "recover": "back"}


def _host_of(url: str) -> str:
    try:
        return (urllib.parse.urlsplit(url).hostname or "").lower()
    except Exception:
        return ""


def policy_for(url: str) -> dict:
    """Capture policy for a landing page URL."""
    host = _host_of(url)
    for pattern, policy in CAPTURE_POLICIES:
        if pattern.search(host):
            return policy
    return DEFAULT_POLICY


def same_service(url: str, origin_url: str) -> bool:
    """True if url belongs to the same service as the landing page.

    Covers plain same-site, plus known multi-domain families so a host's own
    canonical redirect is not mistaken for an advert hijack.
    """
    if not url or not origin_url:
        return False
    if site_of(url) and site_of(url) == site_of(origin_url):
        return True
    family = policy_for(origin_url).get("family")
    if family:
        return bool(family.search(_host_of(url)) and family.search(_host_of(origin_url)))
    return False


def site_of(url: str) -> str:
    """Return the registrable-ish site (last two labels) of a URL's host."""
    try:
        host = urllib.parse.urlsplit(url).hostname or ""
    except Exception:
        return ""
    parts = [p for p in host.lower().split(".") if p]
    return ".".join(parts[-2:]) if len(parts) >= 2 else host.lower()


def is_payload_name(name: str) -> bool:
    """True if a filename or URL points at a transferable payload."""
    if not name:
        return False
    candidate = name.strip()
    if candidate.startswith(("http://", "https://")):
        try:
            candidate = urllib.parse.urlsplit(candidate).path
        except Exception:
            pass
    candidate = urllib.parse.unquote(candidate)
    if is_installer_url(candidate) or is_installer_url(name):
        return False
    return bool(_PAYLOAD_RE.search(candidate))


# --------------------------------------------------------------------------
# Chrome discovery
# --------------------------------------------------------------------------

def find_browser() -> str | None:
    """Locate a Chrome/Chromium/Edge executable for the current platform."""
    env = os.environ.get("DIRECT_STREAM_BROWSER")
    if env and os.path.isfile(env):
        return env

    if sys.platform == "darwin":
        candidates = [
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Chromium.app/Contents/MacOS/Chromium",
            "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
            "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
        ]
    elif os.name == "nt":
        programs = [
            os.environ.get("PROGRAMFILES", r"C:\Program Files"),
            os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)"),
            os.environ.get("LOCALAPPDATA", ""),
        ]
        rel = [
            r"Google\Chrome\Application\chrome.exe",
            r"Microsoft\Edge\Application\msedge.exe",
            r"BraveSoftware\Brave-Browser\Application\brave.exe",
            r"Chromium\Application\chrome.exe",
        ]
        candidates = [os.path.join(p, r) for p in programs if p for r in rel]
    else:
        candidates = []
        for name in ("google-chrome", "google-chrome-stable", "chromium",
                     "chromium-browser", "microsoft-edge", "brave-browser"):
            found = shutil.which(name)
            if found:
                candidates.append(found)

    for path in candidates:
        if path and os.path.isfile(path):
            return path
    return None


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# --------------------------------------------------------------------------
# Minimal RFC 6455 websocket client (text frames only)
# --------------------------------------------------------------------------

class WebSocket:
    """Just enough of a websocket client to drive the DevTools protocol."""

    def __init__(self, url: str, timeout: float = 10.0):
        parsed = urllib.parse.urlsplit(url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 80
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query

        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._sock.settimeout(timeout)
        self._buf = b""
        self._lock = threading.Lock()

        key = base64.b64encode(os.urandom(16)).decode()
        handshake = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        )
        self._sock.sendall(handshake.encode())

        # Read response headers
        while b"\r\n\r\n" not in self._buf:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise ConnectionError("DevTools closed the connection during handshake.")
            self._buf += chunk
        head, _, rest = self._buf.partition(b"\r\n\r\n")
        if b"101" not in head.split(b"\r\n")[0]:
            raise ConnectionError(f"DevTools refused the websocket upgrade: {head[:120]!r}")
        self._buf = rest

    # -- low level ---------------------------------------------------------

    def _read_exactly(self, n: int) -> bytes:
        while len(self._buf) < n:
            chunk = self._sock.recv(65536)
            if not chunk:
                raise ConnectionError("DevTools connection closed.")
            self._buf += chunk
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def send(self, payload: dict) -> None:
        data = json.dumps(payload).encode()
        header = bytearray([0x81])  # FIN + text
        length = len(data)
        if length <= 125:
            header.append(0x80 | length)
        elif length <= 0xFFFF:
            header.append(0x80 | 126)
            header += struct.pack(">H", length)
        else:
            header.append(0x80 | 127)
            header += struct.pack(">Q", length)
        mask = os.urandom(4)
        header += mask
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
        with self._lock:
            self._sock.sendall(bytes(header) + masked)

    def recv(self) -> dict | None:
        """Return the next decoded JSON message, or None for control frames."""
        fragments = b""
        while True:
            b0, b1 = self._read_exactly(2)
            fin = b0 & 0x80
            opcode = b0 & 0x0F
            masked = b1 & 0x80
            length = b1 & 0x7F
            if length == 126:
                length = struct.unpack(">H", self._read_exactly(2))[0]
            elif length == 127:
                length = struct.unpack(">Q", self._read_exactly(8))[0]
            mask = self._read_exactly(4) if masked else b""
            payload = self._read_exactly(length) if length else b""
            if masked:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))

            if opcode == 0x8:                      # close
                raise ConnectionError("DevTools closed the websocket.")
            if opcode == 0x9:                      # ping -> pong
                self._send_control(0xA, payload)
                continue
            if opcode == 0xA:                      # pong
                continue

            fragments += payload
            if fin:
                try:
                    return json.loads(fragments.decode("utf-8", errors="replace"))
                except ValueError:
                    return None

    def _send_control(self, opcode: int, payload: bytes) -> None:
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        frame = bytes([0x80 | opcode, 0x80 | len(payload)]) + mask + masked
        with self._lock:
            self._sock.sendall(frame)

    def settimeout(self, t: float) -> None:
        self._sock.settimeout(t)

    def close(self) -> None:
        try:
            self._sock.close()
        except Exception:
            pass


# --------------------------------------------------------------------------
# Capture session
# --------------------------------------------------------------------------

class CaptureSession:
    """Opens a browser window and captures the first direct download URL."""

    def __init__(self, page_url: str, timeout: int = CAPTURE_TIMEOUT):
        self.page_url = page_url
        self.timeout = timeout
        self.state = "starting"        # starting|waiting|done|error|cancelled
        self.result_url = ""
        self.error = ""
        self.started_at = time.time()

        self._proc = None
        self._profile = None
        self._stop = threading.Event()
        self._thread = None
        self._attached = {}            # target id -> WebSocket
        self._navigated = False
        self._main_tid = None
        self._seen_at = {}
        self._offsite_since = None
        self._recover_attempts = 0
        # The page a host canonicalises to (akirabox.com -> akirabox.to) becomes
        # the reference origin once the tab first settles, so the host's own
        # redirect is never mistaken for an advert hijack.
        self._effective_origin = page_url
        self._origin_locked = False
        self.devtools_port = None
        self.skipped = []
        self.blocked_popups = []
        self._lock = threading.Lock()
        self._rlock = threading.Lock()
        self._rws = None
        self.headless = False
        self._xvfb = None

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        browser = find_browser()
        if not browser:
            self.state = "error"
            self.error = ("No Chrome, Edge, Brave or Chromium installation was found. "
                          "Install one, or set DIRECT_STREAM_BROWSER to its executable path.")
            return

        port = _free_port()
        self.devtools_port = port
        self._profile = tempfile.mkdtemp(prefix="ds-capture-")
        args = [
            browser,
            f"--remote-debugging-port={port}",
            f"--user-data-dir={self._profile}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-extensions",
            "--new-window",
            "--window-size=1100,860",
            # Start blank: we attach DevTools first, then navigate. Launching
            # straight at the URL can fire the download before we are listening.
            "about:blank",
        ]
        # A server with no screen (e.g. a Raspberry Pi) runs the browser headless;
        # the dashboard then shows it remotely through remote_frame()/remote_input().
        self.headless = (os.environ.get("DS5_CAPTURE_HEADLESS") == "1"
                         or (os.name != "nt" and sys.platform != "darwin"
                             and not os.environ.get("DISPLAY")
                             and not os.environ.get("WAYLAND_DISPLAY")))
        xvfb_env = None
        if self.headless:
            # A normal (non-headless) Chromium on a private virtual screen when
            # Xvfb exists: it is the same browser a person would use, just drawn
            # off-screen. Plain headless mode is only the fallback.
            xvfb_env = self._start_virtual_display()
            if xvfb_env is None:
                args[1:1] = ["--headless=new", "--disable-gpu", "--hide-scrollbars"]
            else:
                args[1:1] = ["--ozone-platform=x11"]
        try:
            kwargs = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
            if self.headless:
                # Under a systemd user service the session bus and accessibility
                # variables made Chromium stall on its first navigation, leaving
                # a blank page. It needs neither.
                env = dict(xvfb_env or os.environ)
                env.pop("DBUS_SESSION_BUS_ADDRESS", None)
                env.pop("QT_ACCESSIBILITY", None)
                kwargs["env"] = env
            if os.name == "nt":
                # The dashboard frequently runs as a background/console-less
                # process. On Windows a child inherits the parent's "show window"
                # state unless STARTUPINFO says otherwise, which made the capture
                # window come up minimised. Ask for a normal window explicitly.
                # (CREATE_NO_WINDOW is for console apps; Chrome is a GUI app, so it
                # bought nothing here and only propagated the hidden state.)
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                startupinfo.wShowWindow = SW_SHOWNORMAL
                kwargs["startupinfo"] = startupinfo
            self._proc = subprocess.Popen(args, **kwargs)
        except Exception as ex:
            self.state = "error"
            self.error = f"Could not launch the browser: {ex}"
            return

        self.state = "waiting"
        if os.name == "nt" and self._proc is not None:
            threading.Thread(target=raise_browser_window,
                             args=(self._proc.pid,), daemon=True).start()
        self._thread = threading.Thread(target=self._run, args=(port,), daemon=True)
        self._thread.start()

    def cancel(self) -> None:
        if self.state in ("done", "error"):
            return
        self.state = "cancelled"
        self._stop.set()

    def snapshot(self) -> dict:
        remaining = max(0, int(self.timeout - (time.time() - self.started_at)))
        return {
            "state": self.state,
            "url": self.result_url,
            "error": self.error,
            "remaining": remaining if self.state == "waiting" else 0,
            "skipped": list(self.skipped),
            "blocked_popups": list(self.blocked_popups),
        }

    # -- internals ---------------------------------------------------------

    def _devtools(self, port: int, path: str, timeout: float = 2.0):
        req = urllib.request.Request(f"http://127.0.0.1:{port}{path}",
                                     headers={"Host": f"127.0.0.1:{port}"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8", errors="replace"))

    def _start_virtual_display(self):
        """Start a private Xvfb screen; return the environment to use, or None."""
        exe = shutil.which("Xvfb")
        if not exe:
            return None
        for n in range(90, 190):
            if os.path.exists(f"/tmp/.X11-unix/X{n}") or os.path.exists(f"/tmp/.X{n}-lock"):
                continue
            try:
                proc = subprocess.Popen(
                    [exe, f":{n}", "-screen", "0", "1100x860x24", "-nolisten", "tcp"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except Exception:
                return None
            for _ in range(40):
                if os.path.exists(f"/tmp/.X11-unix/X{n}"):
                    self._xvfb = proc
                    env = dict(os.environ)
                    env["DISPLAY"] = f":{n}"
                    env.pop("WAYLAND_DISPLAY", None)
                    return env
                if proc.poll() is not None:
                    break
                time.sleep(0.1)
            try:
                proc.terminate()
            except Exception:
                pass
            if proc.poll() is None:
                return None  # started but never came up; don't burn through every display number
        return None

    def _plain_user_agent(self, port: int) -> str:
        try:
            ua = self._devtools(port, "/json/version").get("User-Agent", "")
        except Exception:
            return ""
        return ua.replace("HeadlessChrome", "Chrome")

    # -- remote view/control (for a browser nobody can see) ------------------

    def _remote_ws(self):
        """A dedicated DevTools socket to the capture tab, separate from the
        listener sockets so replies never mix with download events."""
        port = self.devtools_port
        if not port:
            raise RuntimeError("Browser not running.")
        pages = [t for t in self._devtools(port, "/json/list") if t.get("type") == "page"]
        target = next((t for t in pages if t.get("id") == self._main_tid), None) \
            or (pages[0] if pages else None)
        if not target:
            raise RuntimeError("No browser tab open yet.")
        # Do not poke the tab before it has left the blank start page: a
        # screenshot during the first navigation left it stuck on a blank page.
        if not self._navigated or (target.get("url") or "").startswith("about:blank"):
            raise RuntimeError("The page is still starting.")
        cur = getattr(self, "_rws", None)
        if cur and cur[0] == target["id"]:
            return cur[1]
        if cur:
            cur[1].close()
        ws = WebSocket(target["webSocketDebuggerUrl"], timeout=5)
        self._rws = (target["id"], ws)
        self._rid = 100
        return ws

    def _remote_call(self, method: str, params=None, want_result: bool = False):
        with self._rlock:
            ws = self._remote_ws()
            self._rid += 1
            rid = self._rid
            ws.send({"id": rid, "method": method, "params": params or {}})
            deadline = time.time() + 5
            while time.time() < deadline:
                try:
                    msg = ws.recv()
                except socket.timeout:
                    continue
                if msg and msg.get("id") == rid:
                    if "error" in msg:
                        raise RuntimeError(msg["error"].get("message", "DevTools error"))
                    return msg.get("result", {})
            raise TimeoutError("Browser did not answer.")

    def remote_frame(self) -> dict:
        """One JPEG of the capture tab, base64, with its viewport size."""
        if self.state != "waiting":
            raise RuntimeError("Capture is not active.")
        res = self._remote_call("Page.captureScreenshot",
                                {"format": "jpeg", "quality": 55})
        return {"frame": res.get("data", ""), "width": 1100, "height": 860}

    _SPECIAL_KEYS = {
        "Enter": 13, "Backspace": 8, "Tab": 9, "Escape": 27, "Delete": 46,
        "ArrowLeft": 37, "ArrowUp": 38, "ArrowRight": 39, "ArrowDown": 40,
        "Home": 36, "End": 35, "PageUp": 33, "PageDown": 34,
    }

    def remote_input(self, ev: dict) -> None:
        """Forward a click, scroll or keypress from the dashboard to the tab."""
        if self.state != "waiting":
            return
        kind = str(ev.get("type") or "")

        def num(name, lo=0, hi=5000):
            return max(lo, min(hi, float(ev.get(name) or 0)))

        if kind == "click":
            x, y = num("x"), num("y")
            base = {"x": x, "y": y, "button": "left", "clickCount": 1}
            self._remote_call("Input.dispatchMouseEvent", {"type": "mouseMoved", "x": x, "y": y})
            self._remote_call("Input.dispatchMouseEvent", {**base, "type": "mousePressed"})
            self._remote_call("Input.dispatchMouseEvent", {**base, "type": "mouseReleased"})
        elif kind == "wheel":
            self._remote_call("Input.dispatchMouseEvent", {
                "type": "mouseWheel", "x": num("x"), "y": num("y"),
                "deltaX": num("dx", -3000, 3000) if "dx" in ev else 0,
                "deltaY": max(-3000, min(3000, float(ev.get("dy") or 0)))})
        elif kind == "text":
            text = str(ev.get("text") or "")[:200]
            if text:
                self._remote_call("Input.insertText", {"text": text})
        elif kind == "key":
            key = str(ev.get("key") or "")
            code = self._SPECIAL_KEYS.get(key)
            if code:
                p = {"key": key, "windowsVirtualKeyCode": code, "nativeVirtualKeyCode": code}
                if key == "Enter":
                    p["text"] = "\r"
                self._remote_call("Input.dispatchKeyEvent", {**p, "type": "keyDown"})
                self._remote_call("Input.dispatchKeyEvent", {**p, "type": "keyUp"})

    def _run(self, port: int) -> None:
        try:
            self._wait_for_devtools(port)
            self._pump(port)
        except Exception as ex:
            if self.state == "waiting":
                self.state = "error"
                self.error = str(ex)
        finally:
            self._cleanup()

    def _wait_for_devtools(self, port: int) -> None:
        deadline = time.time() + 25
        while time.time() < deadline:
            if self._stop.is_set():
                return
            try:
                self._devtools(port, "/json/version")
                return
            except Exception:
                time.sleep(0.25)
        raise TimeoutError("The browser did not expose its DevTools port in time.")

    def _attach(self, port: int, target: dict) -> None:
        tid = target.get("id")
        ws_url = target.get("webSocketDebuggerUrl")
        if not tid or not ws_url or tid in self._attached:
            return
        try:
            ws = WebSocket(ws_url, timeout=5)
        except Exception:
            return
        with self._lock:
            self._attached[tid] = ws
        try:
            ws.send({"id": 1, "method": "Page.enable"})
            ws.send({"id": 2, "method": "Network.enable"})
            # Deny the actual download -- we only want the URL, not the bytes.
            # eventsEnabled is required for Browser.downloadWillBegin to fire.
            ws.send({"id": 3, "method": "Browser.setDownloadBehavior",
                     "params": {"behavior": "deny", "eventsEnabled": True}})
            ws.send({"id": 4, "method": "Page.setDownloadBehavior",
                     "params": {"behavior": "deny"}})
            if self.headless and self._xvfb is None:
                ua = self._plain_user_agent(port)
                if ua:
                    ws.send({"id": 7, "method": "Network.setUserAgentOverride",
                             "params": {"userAgent": ua}})
            # Now that we are listening, drive the window to the landing page.
            if not self._navigated:
                self._navigated = True
                self._main_tid = tid
                ws.send({"id": 5, "method": "Page.navigate",
                         "params": {"url": self.page_url}})
                # Bring the capture tab forward; a background launcher cannot
                # steal focus on Windows, so ask the browser to raise itself.
                ws.send({"id": 6, "method": "Page.bringToFront"})
        except Exception:
            pass
        threading.Thread(target=self._reader, args=(tid, ws), daemon=True).start()

    def _reader(self, tid: str, ws: WebSocket) -> None:
        ws.settimeout(1.0)
        while not self._stop.is_set() and self.state == "waiting":
            try:
                msg = ws.recv()
            except socket.timeout:
                continue
            except Exception:
                break
            if not msg:
                continue
            url = self._url_from_event(msg)
            if url:
                self._finish(url)
                break
        with self._lock:
            self._attached.pop(tid, None)
        ws.close()

    def _url_from_event(self, msg: dict) -> str:
        method = msg.get("method", "")
        params = msg.get("params") or {}

        # The definitive signal: the browser decided this navigation is a download.
        if method in ("Page.downloadWillBegin", "Browser.downloadWillBegin"):
            url = params.get("url", "")
            # suggestedFilename is authoritative: ad links frequently carry no
            # extension in the URL at all (e.g. Opera's installer endpoint).
            suggested = params.get("suggestedFilename") or ""
            if is_payload_name(suggested) or (not suggested and is_payload_name(url)):
                return url
            self._note_skipped(suggested or url)
            return ""

        # Fallback: a response that is explicitly an attachment or a binary payload
        # on a media-looking URL. Covers hosts that stream the file inline.
        if method == "Network.responseReceived":
            resp = params.get("response") or {}
            url = resp.get("url", "")
            if not url.startswith(("http://", "https://")):
                return ""
            headers = {k.lower(): v for k, v in (resp.get("headers") or {}).items()}
            disposition = headers.get("content-disposition", "")
            ctype = headers.get("content-type", "").lower()
            filename = ""
            m_name = re.search(r"filename\*?=(?:UTF-8''|utf-8'')?\"?([^\";]+)",
                               disposition, re.I)
            if m_name:
                filename = m_name.group(1).strip()
            if "attachment" in disposition.lower() and is_payload_name(filename):
                return url
            if is_payload_name(url) and (
                    "octet-stream" in ctype or "application/x-" in ctype
                    or ctype.startswith("application/zip")):
                return url
            if filename and "attachment" in disposition.lower():
                self._note_skipped(filename)
        return ""

    def _guard_main_tab(self, url: str, origin: str) -> None:
        """Recover the main tab when an advert script hijacks it.

        Recovery prefers history.back(): reloading the landing page restarts the
        host's multi-click download flow and caused an endless loop on AkiraBox.
        A reload is kept only as a last resort. The per-host policy decides the
        strategy; see CAPTURE_POLICIES.
        """
        broken = url.startswith("chrome-error://")
        is_page = url.startswith(("http://", "https://"))

        # The first page the tab settles on is the host's canonical URL. Only
        # adopt it shortly after launch: a hijack needs a human click, which
        # cannot happen before the initial navigation has been observed.
        if (not self._origin_locked and is_page and not broken
                and time.time() - self.started_at < 20):
            self._effective_origin = url
            self._origin_locked = True

        hijacked = broken or (is_page
                              and not same_service(url, self._effective_origin)
                              and not is_payload_name(url))
        if not hijacked:
            self._offsite_since = None
            self._recover_attempts = 0
            return

        policy = policy_for(self.page_url)
        if policy.get("recover") == "none":
            return

        # Grace period: a genuine download navigation announces itself first.
        now = time.time()
        if self._offsite_since is None:
            self._offsite_since = now
            return
        if now - self._offsite_since < 2.0:
            return
        self._offsite_since = None

        target_site = site_of(url)
        with self._lock:
            ws = self._attached.get(self._main_tid)
            if target_site and target_site not in self.blocked_popups                     and len(self.blocked_popups) < 10:
                self.blocked_popups.append(target_site)
        if not ws:
            return

        self._recover_attempts += 1
        force_reload = (policy.get("recover") == "reload" or self._recover_attempts > 2)
        try:
            if force_reload:
                ws.send({"id": 11, "method": "Page.navigate",
                         "params": {"url": self.page_url}})
                self._recover_attempts = 0
            else:
                ws.send({"id": 9, "method": "Page.navigateToHistoryEntry",
                         "params": {"entryId": -1}})
                ws.send({"id": 10, "method": "Runtime.evaluate",
                         "params": {"expression": "history.back()"}})
        except Exception:
            pass

    def _close_target(self, port: int, tid: str, site: str) -> None:
        """Close an off-site tab (ad popunder) so it cannot hijack the window."""
        # /json/close returns plain text ("Target is closing"), not JSON, so this
        # must not go through the JSON-decoding helper.
        try:
            req = urllib.request.Request(f"http://127.0.0.1:{port}/json/close/{tid}",
                                         headers={"Host": f"127.0.0.1:{port}"})
            with urllib.request.urlopen(req, timeout=2):
                pass
        except Exception:
            return
        with self._lock:
            ws = self._attached.pop(tid, None)
            if site and site not in self.blocked_popups and len(self.blocked_popups) < 10:
                self.blocked_popups.append(site)
        if ws:
            ws.close()

    def _note_skipped(self, name: str) -> None:
        """Record a decoy/ad download that was ignored, for user-visible feedback."""
        label = (name or "").strip()
        if label.startswith(("http://", "https://")):
            label = os.path.basename(urllib.parse.urlsplit(label).path) or label
        label = urllib.parse.unquote(label)[:60]
        if label:
            with self._lock:
                if label not in self.skipped and len(self.skipped) < 10:
                    self.skipped.append(label)

    def _finish(self, url: str) -> None:
        if self.state != "waiting":
            return
        self.result_url = url
        self.state = "done"
        self._stop.set()

    def _pump(self, port: int) -> None:
        """Keep every page target attached until a URL shows up or we time out."""
        deadline = time.time() + self.timeout
        while not self._stop.is_set() and self.state == "waiting":
            if time.time() > deadline:
                self.state = "error"
                self.error = ("Timed out waiting for a download link. Open the link, "
                              "complete the check, then press Download.")
                break
            if self._proc and self._proc.poll() is not None:
                if self.state == "waiting":
                    self.state = "error"
                    self.error = "The browser window was closed before a download link appeared."
                break
            try:
                origin = site_of(self.page_url)
                for target in self._devtools(port, "/json/list"):
                    if target.get("type") != "page":
                        continue
                    tid = target.get("id")
                    url = target.get("url", "") or ""
                    # Keep listening even to ad tabs: a legitimate download may open
                    # in a new tab on a CDN domain, and we must not miss it.
                    self._attach(port, target)
                    if not tid:
                        continue
                    if tid == self._main_tid:
                        self._guard_main_tab(url, origin)
                        continue
                    self._seen_at.setdefault(tid, time.time())
                    target_site = site_of(url)
                    offsite = (url.startswith(("http://", "https://"))
                               and not same_service(url, self._effective_origin))
                    # Grace period so a real download tab can announce itself first.
                    if offsite and time.time() - self._seen_at[tid] > 2.5:
                        self._close_target(port, tid, target_site)
            except Exception:
                pass
            time.sleep(0.6)

    def _cleanup(self) -> None:
        self._stop.set()
        if self._rws:
            try:
                self._rws[1].close()
            except Exception:
                pass
            self._rws = None
        with self._lock:
            sockets = list(self._attached.values())
            self._attached.clear()
        for ws in sockets:
            ws.close()
        if self._proc and self._proc.poll() is None:
            try:
                self._proc.terminate()
                self._proc.wait(timeout=5)
            except Exception:
                try:
                    self._proc.kill()
                except Exception:
                    pass
        if self._xvfb and self._xvfb.poll() is None:
            try:
                self._xvfb.terminate()
                self._xvfb.wait(timeout=5)
            except Exception:
                pass
        self._xvfb = None
        if self._profile:
            shutil.rmtree(self._profile, ignore_errors=True)
            self._profile = None
