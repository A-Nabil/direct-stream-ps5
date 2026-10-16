"""Tests for disk-buffer mode: downloading to this PC first, then sending to the PS5.

Covers the pieces that make a download possible while the console is off — the tail
reader that streams a still-growing cache file, cache resume/identity handling, the
progress channel for the second bar, and cache lifecycle.
"""

import io
import json
import logging
import os
import socket
import shutil
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import transfer_core  # noqa: E402
from transfer_core import (BLOCK, Cancelled, DownloadState, Meter,  # noqa: E402
                           StopToken, TailLocalReader, TransferError,
                           _disk_buffer_needs_full_download, _download_to_cache,
                           _ps5_reachable, cleanup_orphan_download_caches,
                           download_cache_root, make_reader, probe_source)
from transfer_core import transfer_disk_buffer  # noqa: E402
from ps5_streamer import Manager  # noqa: E402
from pyftpdlib.authorizers import DummyAuthorizer  # noqa: E402
from pyftpdlib.handlers import FTPHandler  # noqa: E402
from pyftpdlib.ioloop import IOLoop  # noqa: E402
from pyftpdlib.servers import FTPServer  # noqa: E402


class TailLocalReaderTests(unittest.TestCase):
    """The reader must never outrun flushed bytes, and must block rather than EOF."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="ds-tail-")
        self.path = os.path.join(self.tmp, "cache.bin")
        with open(self.path, "wb"):
            pass
        self.token = StopToken()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _append(self, state, payload):
        with open(self.path, "ab") as f:
            f.write(payload)
            f.flush()
        state.publish(len(payload))

    def test_reads_already_committed_bytes(self):
        state = DownloadState(total=4)
        self._append(state, b"abcd")
        reader = TailLocalReader(self.path, 0, state, self.token)
        self.addCleanup(reader.close)
        self.assertEqual(reader.read(BLOCK), b"abcd")

    def test_blocks_until_more_bytes_are_published(self):
        state = DownloadState(total=8)
        self._append(state, b"1234")
        reader = TailLocalReader(self.path, 0, state, self.token)
        self.addCleanup(reader.close)
        self.assertEqual(reader.read(BLOCK), b"1234")

        got = []

        def consume():
            got.append(reader.read(BLOCK))

        t = threading.Thread(target=consume, daemon=True)
        t.start()
        t.join(0.4)
        self.assertTrue(t.is_alive(), "reader returned EOF instead of waiting for the download")

        self._append(state, b"5678")
        t.join(3)
        self.assertEqual(got, [b"5678"])

    def test_eof_only_once_download_is_done_and_drained(self):
        state = DownloadState(total=2)
        self._append(state, b"ab")
        reader = TailLocalReader(self.path, 0, state, self.token)
        self.addCleanup(reader.close)
        self.assertEqual(reader.read(BLOCK), b"ab")
        state.finish()
        self.assertEqual(reader.read(BLOCK), b"")

    def test_start_offset_beyond_committed_waits_then_serves(self):
        # This is what FTP REST resume needs: upload resumes at a byte the download
        # has not reached yet.
        state = DownloadState(total=8)
        self._append(state, b"1234")
        reader = TailLocalReader(self.path, 6, state, self.token)
        self.addCleanup(reader.close)

        got = []
        t = threading.Thread(target=lambda: got.append(reader.read(BLOCK)), daemon=True)
        t.start()
        t.join(0.3)
        self.assertTrue(t.is_alive())

        self._append(state, b"5678")
        t.join(3)
        self.assertEqual(got, [b"78"])

    def test_cancel_unblocks_the_reader(self):
        state = DownloadState(total=99)
        reader = TailLocalReader(self.path, 0, state, self.token)
        self.addCleanup(reader.close)
        errors = []

        def consume():
            try:
                reader.read(BLOCK)
            except Cancelled as e:
                errors.append(e)

        t = threading.Thread(target=consume, daemon=True)
        t.start()
        t.join(0.3)
        self.token.event.set()
        t.join(3)
        self.assertEqual(len(errors), 1, "cancelling did not unblock the tail reader")

    def test_download_error_is_reraised_to_the_uploader(self):
        state = DownloadState(total=99)
        reader = TailLocalReader(self.path, 0, state, self.token)
        self.addCleanup(reader.close)
        state.finish(TransferError("source died"))
        with self.assertRaises(TransferError):
            reader.read(BLOCK)

    def test_make_reader_routes_tail_local(self):
        state = DownloadState(total=1)
        self._append(state, b"x")
        source = transfer_core.SourceInfo(kind="tail_local", location=self.path, size=1)
        reader = make_reader(source, 0, {"_dl_state": state, "buffer_mb": 16,
                                         "chunk_mb": 8, "streams": 1},
                             self.token, Meter())
        self.addCleanup(reader.close)
        self.assertIsInstance(reader, TailLocalReader)

    def test_tail_local_is_resumable(self):
        # stream_file_to_ftp refuses REST resume for non-resumable sources.
        self.assertTrue(transfer_core.SourceInfo(kind="tail_local", location="x",
                                                 size=10).resumable())


class CacheFixture(BaseHTTPRequestHandler):
    """Serves a known payload, honours Range, and records the ranges it was asked for."""

    payload = bytes(range(256)) * 24576  # 6 MiB: several 1 MiB reader blocks
    ranges_seen = []
    chunk_delay = 0.0

    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def handle(self):
        try:
            super().handle()
        except (ConnectionResetError, BrokenPipeError):
            pass

    def do_GET(self):
        rng = self.headers.get("Range")
        total = len(CacheFixture.payload)
        start, end = 0, total - 1
        if rng:
            CacheFixture.ranges_seen.append(rng)
            lo, _, hi = rng.replace("bytes=", "").partition("-")
            start = int(lo or 0)
            # The probe sends bytes=0-0 and validates the echoed end bound, so the
            # end of the requested range must be honoured exactly.
            if hi:
                end = min(int(hi), total - 1)
        body = CacheFixture.payload[start:end + 1]
        if rng:
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{total}")
        else:
            self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("ETag", '"cache-fixture"')
        self.end_headers()
        step = 64 * 1024
        for i in range(0, len(body), step):
            self.wfile.write(body[i:i + step])
            if CacheFixture.chunk_delay:
                time.sleep(CacheFixture.chunk_delay)


class CacheDownloadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.http = ThreadingHTTPServer(("127.0.0.1", 0), CacheFixture)
        cls.http.daemon_threads = True
        threading.Thread(target=cls.http.serve_forever, daemon=True).start()
        cls.url = "http://127.0.0.1:%d/file.pkg" % cls.http.server_port

    @classmethod
    def tearDownClass(cls):
        cls.http.shutdown()

    def setUp(self):
        CacheFixture.ranges_seen = []
        CacheFixture.chunk_delay = 0.0
        self.tmp = tempfile.mkdtemp(prefix="ds-cache-")
        self.token = StopToken()
        self.settings = {"buffer_mb": 16, "chunk_mb": 8, "streams": 1, "retries": 0,
                         "limit_mbps": 0}

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _run_download(self, cache_file, source=None):
        source = source or probe_source("url", self.url, self.token, decompress=False)
        state = DownloadState(total=source.size,
                              committed=os.path.getsize(cache_file)
                              if os.path.exists(cache_file) else 0)
        stop = threading.Event()
        _download_to_cache(source, cache_file, self.settings, self.token, stop,
                           state, Meter(), lambda: None)
        return state

    def test_downloads_full_payload_into_the_cache(self):
        cache_file = os.path.join(self.tmp, "file.pkg")
        state = self._run_download(cache_file)
        committed, total, done, error = state.snapshot()
        self.assertIsNone(error)
        self.assertTrue(done)
        self.assertEqual(committed, len(CacheFixture.payload))
        self.assertEqual(total, len(CacheFixture.payload))
        with open(cache_file, "rb") as f:
            self.assertEqual(f.read(), CacheFixture.payload)

    def test_resumes_a_partial_cache_with_a_range_request(self):
        cache_file = os.path.join(self.tmp, "file.pkg")
        with open(cache_file, "wb") as f:
            f.write(CacheFixture.payload[:100_000])
        CacheFixture.ranges_seen = []
        self._run_download(cache_file)
        self.assertTrue(any(r.startswith("bytes=100000-") for r in CacheFixture.ranges_seen),
                        "download did not resume from the cached bytes: %s"
                        % CacheFixture.ranges_seen)
        with open(cache_file, "rb") as f:
            self.assertEqual(f.read(), CacheFixture.payload)

    def test_complete_cache_finishes_without_refetching(self):
        cache_file = os.path.join(self.tmp, "file.pkg")
        with open(cache_file, "wb") as f:
            f.write(CacheFixture.payload)
        CacheFixture.ranges_seen = []
        state = self._run_download(cache_file)
        _c, _t, done, error = state.snapshot()
        self.assertTrue(done)
        self.assertIsNone(error)


class DiskBufferRoutingTests(unittest.TestCase):
    def test_plain_file_is_pipelined_archive_and_multipart_are_not(self):
        self.assertFalse(_disk_buffer_needs_full_download(
            {"kind": "url", "name": "Game.pkg", "decompress": True, "extract_mode": "ps5"}))
        self.assertTrue(_disk_buffer_needs_full_download(
            {"kind": "url", "name": "Game.rar", "decompress": True, "extract_mode": "ps5"}))
        self.assertTrue(_disk_buffer_needs_full_download({"kind": "multipart", "parts": []}))
        # Extraction disabled → the archive is just bytes, so it can pipeline.
        self.assertFalse(_disk_buffer_needs_full_download(
            {"kind": "url", "name": "Game.rar", "decompress": False, "extract_mode": "none"}))

    def test_reachability_probe_is_false_without_a_host(self):
        self.assertFalse(_ps5_reachable({"host": "", "port": 1337}))
        self.assertFalse(_ps5_reachable({"host": "127.0.0.1", "port": 1}, timeout=1))

    def test_cache_root_prefers_explicit_download_dir(self):
        self.assertEqual(download_cache_root({"download_dir": "/tmp/x"}),
                         os.path.join("/tmp/x", "downloads"))
        self.assertEqual(download_cache_root({"_data_dir": "/tmp/d"}),
                         os.path.join("/tmp/d", "downloads"))


class SequentialProgressTests(unittest.TestCase):
    """Archives download fully before sending, and that phase must still drive the
    Transfer monitor. Without a progress event it read "0 KB of Unknown size · — · —"."""

    def test_await_download_feeds_the_monitor(self):
        from transfer_core import _await_download

        token = StopToken()
        state = DownloadState(total=1000, committed=400)
        meter = Meter()
        events = []

        def report(event, value):
            events.append((event, value))

        def emit_dl():
            events.append(("dl_progress", {"transferred": 400, "total": 1000}))

        t = threading.Thread(
            target=lambda: _await_download(state, token, report, "Game.rar", emit_dl,
                                           meter=meter, settings={"buffer_mb": 16},
                                           total_hint=1000,
                                           after="sends to PS5 after extraction"),
            daemon=True)
        t.start()
        time.sleep(1.2)
        state.finish()
        t.join(5)

        progress = [v for e, v in events if e == "progress"]
        self.assertTrue(progress, "no progress event: the monitor stays blank and speeds show —")
        for value in progress:
            # Manager._report subscripts both of these without a default.
            self.assertIn("upload_bps", value)
            self.assertIn("download_bps", value)
            self.assertEqual(value["total"], 1000,
                             "headline size missing, monitor shows 'Unknown size'")
            self.assertEqual(value["transferred"], 0, "nothing has been uploaded yet")

        statuses = [str(v) for e, v in events if e == "status"]
        self.assertTrue(any("sends to PS5 after extraction" in s for s in statuses),
                        "status never explains that the send happens after the download")

    def test_await_download_still_returns_the_byte_count(self):
        from transfer_core import _await_download

        state = DownloadState(total=10, committed=10)
        state.finish()
        got = _await_download(state, StopToken(), lambda *_: None, "x", lambda: None)
        self.assertEqual(got, 10)


class CacheLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="ds-life-")
        self.root = os.path.join(self.tmp, "downloads")
        os.makedirs(self.root)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _mk(self, job_id):
        d = os.path.join(self.root, job_id)
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "blob"), "wb") as f:
            f.write(b"x")
        return d

    def test_orphan_caches_are_removed_live_ones_kept(self):
        live = self._mk("live1")
        orphan = self._mk("gone1")
        done_delete = self._mk("done1")
        done_keep = self._mk("done2")
        jobs = [
            {"id": "live1", "state": "queued"},
            {"id": "done1", "state": "completed", "keep_local": False},
            {"id": "done2", "state": "completed", "keep_local": True},
        ]
        cleanup_orphan_download_caches(self.tmp, jobs)
        self.assertTrue(os.path.isdir(live), "an active job's cache was deleted")
        self.assertFalse(os.path.isdir(orphan), "orphan cache survived")
        self.assertFalse(os.path.isdir(done_delete), "completed cache was not reclaimed")
        self.assertTrue(os.path.isdir(done_keep), "keep_local cache was deleted")

    def test_missing_root_is_tolerated(self):
        cleanup_orphan_download_caches(os.path.join(self.tmp, "nope"), [])


class ReportChannelTests(unittest.TestCase):
    """The second progress bar is fed by a dedicated event, not by detail-text scraping."""

    def test_dl_progress_sets_scalars_only(self):
        with tempfile.TemporaryDirectory() as d:
            m = Manager(d)
            job = {"id": "j1", "state": "running", "transferred": 0, "total": 100,
                   "dl_transferred": 0, "dl_total": None, "detail": "x"}
            m._report(job, "dl_progress", {"transferred": 4096, "total": 8192})
            self.assertEqual(job["dl_transferred"], 4096)
            self.assertEqual(job["dl_total"], 8192)
            # Upload fields and the global metrics must be untouched.
            self.assertEqual(job["transferred"], 0)
            self.assertEqual(job["total"], 100)
            self.assertEqual(m.metrics, {})
            self.assertEqual(m.history, [])

    def test_complete_deletes_the_cache_unless_keep_local(self):
        with tempfile.TemporaryDirectory() as d:
            m = Manager(d)
            cache = os.path.join(d, "downloads", "j2")
            os.makedirs(cache)
            with open(os.path.join(cache, "blob"), "wb") as f:
                f.write(b"x")
            job = {"id": "j2", "state": "running", "transferred": 10, "total": 10,
                   "kind": "url", "name": "a.pkg", "cache_dir": cache,
                   "keep_local": False, "folder": "/data"}
            m._report(job, "complete", {"size": 10, "verification": "size match"})
            self.assertFalse(os.path.isdir(cache), "cache kept after a successful send")
            self.assertEqual(job["cache_dir"], "")

    def test_complete_keeps_the_cache_when_asked(self):
        with tempfile.TemporaryDirectory() as d:
            m = Manager(d)
            cache = os.path.join(d, "downloads", "j3")
            os.makedirs(cache)
            job = {"id": "j3", "state": "running", "transferred": 10, "total": 10,
                   "kind": "url", "name": "a.pkg", "cache_dir": cache,
                   "keep_local": True, "folder": "/data"}
            m._report(job, "complete", {"size": 10, "verification": "size match"})
            self.assertTrue(os.path.isdir(cache), "keep_local cache was deleted")


class JobFlagTests(unittest.TestCase):
    def test_flags_round_trip_through_add_jobs(self):
        with tempfile.TemporaryDirectory() as d:
            m = Manager(d)
            m.add_jobs({"kind": "url", "folder": "/data/homebrew",
                        "items": [{"source": "http://example.com/Game.pkg",
                                   "name": "Game.pkg"}],
                        "disk_buffer": True, "keep_local": True,
                        "decompress": False, "extract_mode": "none"})
            job = m.jobs[-1]
            self.assertTrue(job["disk_buffer"])
            self.assertTrue(job["keep_local"])
            self.assertEqual(job["dl_transferred"], 0)
            self.assertIsNone(job["dl_total"])
            self.assertEqual(job["cache_dir"], "")

    def test_toggle_actions_flip_the_flags(self):
        with tempfile.TemporaryDirectory() as d:
            m = Manager(d)
            m.add_jobs({"kind": "url", "folder": "/data/homebrew",
                        "items": [{"source": "http://example.com/Game.pkg",
                                   "name": "Game.pkg"}],
                        "decompress": False, "extract_mode": "none"})
            job = m.jobs[-1]
            self.assertFalse(job["disk_buffer"])
            m.action("toggle_disk_buffer", job["id"])
            self.assertTrue(m.jobs[-1]["disk_buffer"])
            m.action("toggle_keep_local", job["id"])
            self.assertTrue(m.jobs[-1]["keep_local"])

    def test_remove_reclaims_the_cache(self):
        with tempfile.TemporaryDirectory() as d:
            m = Manager(d)
            m.add_jobs({"kind": "url", "folder": "/data/homebrew",
                        "items": [{"source": "http://example.com/Game.pkg",
                                   "name": "Game.pkg"}],
                        "disk_buffer": True, "decompress": False,
                        "extract_mode": "none"})
            job = m.jobs[-1]
            cache = os.path.join(d, "downloads", job["id"])
            os.makedirs(cache)
            job["cache_dir"] = cache
            m.action("remove", job["id"])
            self.assertFalse(os.path.isdir(cache), "cache leaked after removing the job")


class OfflineStartTests(unittest.TestCase):
    """The headline requirement: the download runs with the console off, and the upload
    picks up by itself once the console appears."""

    @classmethod
    def setUpClass(cls):
        logging.disable(logging.CRITICAL)
        cls.http = ThreadingHTTPServer(("127.0.0.1", 0), CacheFixture)
        cls.http.daemon_threads = True
        threading.Thread(target=cls.http.serve_forever, daemon=True).start()
        cls.url = "http://127.0.0.1:%d/Game.pkg" % cls.http.server_port

    @classmethod
    def tearDownClass(cls):
        cls.http.shutdown()
        logging.disable(logging.NOTSET)

    def setUp(self):
        CacheFixture.ranges_seen = []
        CacheFixture.chunk_delay = 0.0
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "ftproot"
        self.root.mkdir()
        self.data_dir = Path(self.tmp.name) / "data"
        self.data_dir.mkdir()
        # Reserve a port the console will later answer on.
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        self.port = probe.getsockname()[1]
        probe.close()
        self.ftp = None
        self.token = StopToken()

    def tearDown(self):
        self.token.event.set()
        if self.ftp:
            try:
                self.ftp.close_all()
            except Exception:
                pass
        self.tmp.cleanup()

    def _start_console(self):
        authorizer = DummyAuthorizer()
        authorizer.add_user("test", "pass", str(self.root), perm="elradfmwMT")

        class FTPFixture(FTPHandler):
            pass

        FTPFixture.authorizer = authorizer
        self.ftp = FTPServer(("127.0.0.1", self.port), FTPFixture, ioloop=IOLoop())
        threading.Thread(target=self.ftp.serve_forever,
                         kwargs={"timeout": 0.05, "handle_exit": False},
                         daemon=True).start()

    def _settings(self):
        return {"host": "127.0.0.1", "port": self.port, "folder": "/target",
                "username": "test", "password": "pass", "streams": 1,
                "buffer_mb": 16, "chunk_mb": 1, "retries": 0, "limit_mbps": 0,
                "_data_dir": str(self.data_dir), "_ftp_lock": threading.Lock()}

    def test_download_runs_offline_then_uploads_when_console_appears(self):
        job = {"id": "offline1", "name": "Game.pkg", "kind": "url", "source": self.url,
               "folder": "/target", "identity": None, "transferred": 0,
               "disk_buffer": True, "keep_local": False, "dl_transferred": 0,
               "dl_total": None, "cache_dir": "", "decompress": False,
               "extract_mode": "none"}
        events = []
        failures = []

        def report(event, value):
            events.append((event, value))
            # Mirror Manager._report's dl_progress branch: transfer_core only emits the
            # event, the manager is what writes the scalars onto the job.
            if event == "dl_progress":
                job["dl_transferred"] = value["transferred"]
                if value.get("total"):
                    job["dl_total"] = value["total"]

        def run():
            try:
                transfer_disk_buffer(job, self._settings(), self.token, report, lambda: None)
            except Exception as e:  # surfaced in the assertions below
                failures.append(e)

        worker = threading.Thread(target=run, daemon=True)
        worker.start()

        # 1. With no console listening, the download must still complete.
        deadline = time.time() + 60
        while time.time() < deadline:
            if job.get("dl_total") and job.get("dl_transferred") == job["dl_total"]:
                break
            if failures:
                self.fail("disk buffer raised before the console came up: %r" % failures[0])
            time.sleep(0.1)
        self.assertEqual(job["dl_transferred"], len(CacheFixture.payload),
                         "download did not finish while the PS5 was offline")
        self.assertTrue(worker.is_alive(), "job ended instead of waiting for the PS5")

        cache_file = os.path.join(job["cache_dir"], "Game.pkg")
        self.assertTrue(os.path.exists(cache_file))
        self.assertEqual(os.path.getsize(cache_file), len(CacheFixture.payload))

        deadline = time.time() + 15
        waiting = []
        while time.time() < deadline and not waiting:
            waiting = [v for e, v in events
                       if e == "status" and "waiting for PS5" in str(v)]
            if not waiting:
                time.sleep(0.1)
        self.assertTrue(waiting, "no status told the user it was waiting for the console")

        # Nothing has been uploaded yet, but the monitor is still being fed.
        progress = [v for e, v in events if e == "progress"]
        self.assertTrue(progress, "no progress events while waiting")
        for value in progress:
            self.assertIn("upload_bps", value)
            self.assertIn("download_bps", value)

        # 2. Console comes online — the upload should proceed on its own.
        self._start_console()
        worker.join(90)
        self.assertFalse(worker.is_alive(), "upload did not finish after the PS5 came up")
        self.assertFalse(failures, "disk buffer failed after the console came up: %r"
                         % (failures[0] if failures else None))

        dest = self.root / "target" / "Game.pkg"
        self.assertTrue(dest.exists(), "file never arrived on the console")
        self.assertEqual(dest.read_bytes(), CacheFixture.payload)
        self.assertTrue([e for e, _ in events if e == "complete"],
                        "no complete event was emitted")

    def test_pipelined_upload_starts_before_the_download_finishes(self):
        CacheFixture.chunk_delay = 0.012  # stretch the download so an overlap is observable
        self._start_console()
        job = {"id": "pipe1", "name": "Game.pkg", "kind": "url", "source": self.url,
               "folder": "/target", "identity": None, "transferred": 0,
               "disk_buffer": True, "keep_local": True, "dl_transferred": 0,
               "dl_total": None, "cache_dir": "", "decompress": False,
               "extract_mode": "none"}
        overlap = []
        done = threading.Event()

        def report(event, value):
            if event == "dl_progress":
                job["dl_transferred"] = value["transferred"]
                if value.get("total"):
                    job["dl_total"] = value["total"]
            if event == "progress" and value.get("transferred"):
                dl = job.get("dl_transferred") or 0
                total = job.get("dl_total") or 0
                if total and dl < total:
                    overlap.append((value["transferred"], dl, total))

        def run():
            try:
                transfer_disk_buffer(job, self._settings(), self.token, report, lambda: None)
            finally:
                done.set()

        threading.Thread(target=run, daemon=True).start()
        self.assertTrue(done.wait(120), "pipelined transfer did not finish")

        dest = self.root / "target" / "Game.pkg"
        self.assertTrue(dest.exists())
        self.assertEqual(dest.read_bytes(), CacheFixture.payload)
        self.assertTrue(overlap,
                        "upload never overlapped the download — it waited for completion")
        # keep_local=True: transfer_core must not remove the cache (Manager owns deletion).
        self.assertTrue(os.path.isdir(job["cache_dir"]))


if __name__ == "__main__":
    unittest.main()
