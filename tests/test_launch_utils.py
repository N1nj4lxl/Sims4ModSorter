"""Tests for update check and download helpers."""
from __future__ import annotations

import http.server
import io
import json
import shutil
import socketserver
import tempfile
import threading
import zipfile
from pathlib import Path
from typing import Optional
from unittest import TestCase, mock

from launch_utils import UpdateResult, check_for_update
from Sims4ModSorter import Sims4ModSorterApp


class _DummyResponse:
    def __init__(self, payload: bytes, headers: Optional[dict[str, str]] = None) -> None:
        self._payload = io.BytesIO(payload)
        self.headers = headers or {}

    def read(self, size: int = -1) -> bytes:
        return self._payload.read(size)

    def __enter__(self) -> "_DummyResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        return None


def _build_release_payload() -> dict[str, object]:
    return {
        "tag_name": "v2.0.0",
        "name": "v2.0.0",
        "html_url": "https://github.com/example/Example/releases/tag/v2.0.0",
        "assets": [
            {
                "browser_download_url": "https://github.com/example/Example/releases/download/v2/Example-mismatch.zip",
                "name": "Example-mismatch.zip",
                "size": 0,
                "content_type": "application/zip",
            },
            {
                "browser_download_url": "https://github.com/example/Example/releases/download/v2/Example-2.0.0.zip",
                "name": "Example-2.0.0.zip",
                "size": 1024,
                "content_type": "application/zip",
            },
        ],
    }


class CheckForUpdateTests(TestCase):
    def test_prefers_named_asset_and_returns_metadata(self) -> None:
        payload = json.dumps(_build_release_payload()).encode("utf-8")

        with mock.patch("launch_utils._load_update_config") as mock_config, mock.patch(
            "launch_utils.urllib.request.urlopen"
        ) as mock_urlopen:
            mock_config.return_value = {
                "repo_owner": "example",
                "repo_name": "Example",
                "download_asset_name": "Example-2.0.0.zip",
                "release_api_url": "https://api.github.com/repos/example/Example/releases/latest",
            }
            mock_urlopen.return_value = _DummyResponse(payload)

            result = check_for_update("app", "1.0.0", timeout=0.1)

        self.assertIsInstance(result, UpdateResult)
        self.assertEqual(result.download_url, "https://github.com/example/Example/releases/download/v2/Example-2.0.0.zip")
        self.assertEqual(result.asset_name, "Example-2.0.0.zip")
        self.assertEqual(result.asset_size, 1024)
        self.assertEqual(result.asset_content_type, "application/zip")
        self.assertEqual(result.latest_version, "2.0.0")


class _ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def _make_zip_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("file.txt", "payload")
    return buffer.getvalue()


class _DownloadHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # type: ignore[override]
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", f"{self.server.base_url}/good.zip")
            self.end_headers()
            return
        if self.path == "/good.zip":
            data = self.server.good_zip  # type: ignore[attr-defined]
            self.send_response(200)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if self.path == "/empty.zip":
            self.send_response(200)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args) -> None:  # type: ignore[override]
        return


class DownloadWorkerTests(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._zip_data = _make_zip_bytes()
        cls._server = _ThreadedHTTPServer(("127.0.0.1", 0), _DownloadHandler)
        cls._server.good_zip = cls._zip_data  # type: ignore[attr-defined]
        cls._server.base_url = f"http://127.0.0.1:{cls._server.server_address[1]}"  # type: ignore[attr-defined]
        cls._thread = threading.Thread(target=cls._server.serve_forever, daemon=True)
        cls._thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._server.shutdown()
        cls._server.server_close()
        cls._thread.join(timeout=1)

    def _make_dummy_downloader(self) -> object:
        class Dummy:
            def __init__(self) -> None:
                self.success: Optional[tuple[Path, bool]] = None
                self.failure: Optional[tuple[Path, BaseException, bool]] = None
                self.progress: list[tuple[int, int]] = []

            def _enqueue_ui(self, callback):
                callback()

            def _update_download_progress(self, written: int, total: int) -> None:
                self.progress.append((written, total))

            def _handle_update_download_success(self, target_path: Path, manual: bool) -> None:
                self.success = (target_path, manual)

            def _handle_update_download_failure(self, target_path: Path, error: BaseException, manual: bool) -> None:
                self.failure = (target_path, error, manual)

        return Dummy()

    def test_download_worker_follows_redirect_and_validates_zip(self) -> None:
        dummy = self._make_dummy_downloader()
        destination_dir = Path(tempfile.mkdtemp())
        destination = destination_dir / "update.zip"

        Sims4ModSorterApp._download_update_worker(  # type: ignore[arg-type]
            dummy,
            f"{self._server.base_url}/redirect",
            destination,
            manual=False,
        )

        self.assertIsNotNone(dummy.success)
        self.assertTrue(destination.exists())
        self.assertTrue(zipfile.is_zipfile(destination))

        shutil.rmtree(destination_dir)

    def test_download_worker_rejects_empty_payload(self) -> None:
        dummy = self._make_dummy_downloader()
        destination_dir = Path(tempfile.mkdtemp())
        destination = destination_dir / "empty.zip"

        Sims4ModSorterApp._download_update_worker(  # type: ignore[arg-type]
            dummy,
            f"{self._server.base_url}/empty.zip",
            destination,
            manual=True,
        )

        self.assertIsNone(dummy.success)
        self.assertIsNotNone(dummy.failure)
        self.assertFalse(destination.exists())
        self.assertIn("empty", str(dummy.failure[1]))

        shutil.rmtree(destination_dir)

