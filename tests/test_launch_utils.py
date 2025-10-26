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
        "body": "New features\n- Added windows installer\n- Improved stability",
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
        self.assertEqual(
            result.release_notes,
            "New features\n- Added windows installer\n- Improved stability",
        )

    def test_platform_heuristic_prefers_windows_asset(self) -> None:
        payload = {
            "tag_name": "v3.1.0",
            "name": "v3.1.0",
            "html_url": "https://github.com/example/Example/releases/tag/v3.1.0",
            "assets": [
                {
                    "browser_download_url": "https://github.com/example/Example/releases/download/v3/Example-linux.AppImage",
                    "name": "Example-linux.AppImage",
                    "size": 8192,
                    "content_type": "application/octet-stream",
                },
                {
                    "browser_download_url": "https://github.com/example/Example/releases/download/v3/Example-mac.zip",
                    "name": "Example-mac.zip",
                    "size": 16384,
                    "content_type": "application/zip",
                },
                {
                    "browser_download_url": "https://github.com/example/Example/releases/download/v3/Example-win.exe",
                    "name": "Example-win.exe",
                    "size": 4096,
                    "content_type": "application/x-msdownload",
                },
            ],
        }
        payload_bytes = json.dumps(payload).encode("utf-8")

        with mock.patch("launch_utils._load_update_config") as mock_config, mock.patch(
            "launch_utils.urllib.request.urlopen"
        ) as mock_urlopen, mock.patch("launch_utils.sys.platform", "win32"):
            mock_config.return_value = {
                "repo_owner": "example",
                "repo_name": "Example",
                "release_api_url": "https://api.github.com/repos/example/Example/releases/latest",
            }
            mock_urlopen.return_value = _DummyResponse(payload_bytes)

            result = check_for_update("app", "3.0.0", timeout=0.1)

        self.assertIsInstance(result, UpdateResult)
        self.assertEqual(
            result.download_url,
            "https://github.com/example/Example/releases/download/v3/Example-win.exe",
        )
        self.assertEqual(result.asset_name, "Example-win.exe")
        self.assertEqual(result.asset_size, 4096)
        self.assertEqual(result.asset_content_type, "application/x-msdownload")
        self.assertEqual(result.latest_version, "3.1.0")


class _ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def _make_zip_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("file.txt", "payload")
    return buffer.getvalue()


def _make_empty_zip_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w"):
        pass
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
        if self.path == "/empty_archive.zip":
            data = self.server.empty_zip  # type: ignore[attr-defined]
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
        cls._empty_zip = _make_empty_zip_bytes()
        cls._server = _ThreadedHTTPServer(("127.0.0.1", 0), _DownloadHandler)
        cls._server.good_zip = cls._zip_data  # type: ignore[attr-defined]
        cls._server.empty_zip = cls._empty_zip  # type: ignore[attr-defined]
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
                self.success: Optional[tuple[Path, str]] = None
                self.failure: Optional[tuple[Path, BaseException, str]] = None
                self.progress: list[tuple[int, int]] = []

            def _enqueue_ui(self, callback):
                callback()

            def _update_download_progress(self, written: int, total: int) -> None:
                self.progress.append((written, total))

            def _handle_update_download_success(self, target_path: Path, mode: str) -> None:
                self.success = (target_path, mode)

            def _handle_update_download_failure(
                self, target_path: Path, error: BaseException, mode: str
            ) -> None:
                self.failure = (target_path, error, mode)

        return Dummy()

    def test_download_worker_follows_redirect_and_validates_zip(self) -> None:
        dummy = self._make_dummy_downloader()
        destination_dir = Path(tempfile.mkdtemp())
        destination = destination_dir / "update.zip"

        Sims4ModSorterApp._download_update_worker(  # type: ignore[arg-type]
            dummy,
            f"{self._server.base_url}/redirect",
            destination,
            mode="auto-install",
        )

        self.assertIsNotNone(dummy.success)
        self.assertEqual(dummy.success[1], "auto-install")
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
            mode="manual-download",
        )

        self.assertIsNone(dummy.success)
        self.assertIsNotNone(dummy.failure)
        self.assertEqual(dummy.failure[2], "manual-download")
        self.assertFalse(destination.exists())
        self.assertIn("empty", str(dummy.failure[1]))

        shutil.rmtree(destination_dir)

    def test_download_worker_rejects_empty_archive(self) -> None:
        dummy = self._make_dummy_downloader()
        destination_dir = Path(tempfile.mkdtemp())
        destination = destination_dir / "empty_archive.zip"

        Sims4ModSorterApp._download_update_worker(  # type: ignore[arg-type]
            dummy,
            f"{self._server.base_url}/empty_archive.zip",
            destination,
            mode="auto-install",
        )

        self.assertIsNone(dummy.success)
        self.assertIsNotNone(dummy.failure)
        self.assertEqual(dummy.failure[2], "auto-install")
        self.assertFalse(destination.exists())
        self.assertIn("archive", str(dummy.failure[1]).lower())

        shutil.rmtree(destination_dir)


class _DummyButton:
    def __init__(self) -> None:
        self.state = "normal"

    def configure(self, **kwargs) -> None:
        if "state" in kwargs:
            self.state = kwargs["state"]

    def winfo_exists(self) -> bool:
        return True


class _DummyStringVar:
    def __init__(self) -> None:
        self.value = None

    def set(self, value) -> None:
        self.value = value


class _DummyProgressFrame:
    def __init__(self) -> None:
        self.grid_called = False
        self.grid_remove_called = False

    def grid(self, *args, **kwargs) -> None:
        self.grid_called = True

    def grid_remove(self) -> None:
        self.grid_remove_called = True


class _DummyOverlay:
    def place(self, *args, **kwargs) -> None:
        return None

    def tkraise(self) -> None:
        return None

    def grab_set(self) -> None:
        return None


class _DummyContainer:
    def winfo_exists(self) -> bool:
        return False

class UpdateOverlayWorkflowTests(TestCase):
    def _make_app(self) -> Sims4ModSorterApp:
        app = Sims4ModSorterApp.__new__(Sims4ModSorterApp)
        app._update_check_in_progress = False
        app.check_updates_button = _DummyButton()
        app._update_download_url = None
        app._update_release_page_url = None
        app._update_download_filename = None
        app._update_available = False
        app._latest_version = None
        app._update_overlay_origin = "general"
        app._update_overlay_visible = False
        app._update_overlay = None
        app._refresh_version_display = mock.Mock()
        app.log = mock.Mock()
        app._show_update_overlay = mock.Mock()
        app._show_info_overlay = mock.Mock()
        app._hide_update_overlay = mock.Mock()
        app._update_release_notes = None
        app.settings_sidebar = mock.Mock()
        app.settings_sidebar.winfo_exists.return_value = True
        app.settings_sidebar.focus_set = mock.Mock()
        return app

    def test_manual_settings_check_uses_overlay(self) -> None:
        app = self._make_app()
        result = UpdateResult(
            latest_version="2.0.0",
            is_newer=True,
            download_url="https://example.com/update.zip",
            message=None,
            release_page_url="https://example.com/release",
            asset_name="update.zip",
            release_notes="Fixed bugs\nAdded UI improvements",
        )

        with mock.patch("tkinter.messagebox.askyesno") as mock_ask, mock.patch(
            "tkinter.messagebox.showinfo"
        ) as mock_info:
            app._complete_update_check(result, manual=True, error_message=None, from_settings=True)

        mock_ask.assert_not_called()
        mock_info.assert_not_called()
        app._show_update_overlay.assert_called_once()
        args, kwargs = app._show_update_overlay.call_args
        self.assertIn("Auto Update", args[0])
        self.assertTrue(kwargs["enable_manual"])
        self.assertEqual(kwargs["origin"], "settings")
        self.assertEqual(app.check_updates_button.state, "normal")
        self.assertEqual(app._update_release_notes, "Fixed bugs\nAdded UI improvements")
        self.assertEqual(kwargs["changelog"], app._format_update_changelog())

    def test_manual_settings_check_displays_current_release_notes(self) -> None:
        app = self._make_app()
        app._update_release_notes = None

        result = UpdateResult(
            latest_version="1.2.3",
            is_newer=False,
            download_url=None,
            message=None,
            release_page_url="https://example.com/release",
            asset_name=None,
            release_notes="Bug fixes\nPerformance improvements",
        )

        app._complete_update_check(result, manual=True, error_message=None, from_settings=True)

        app._show_info_overlay.assert_not_called()
        app._show_update_overlay.assert_called_once()
        args, kwargs = app._show_update_overlay.call_args
        self.assertIn("You are using the latest version", args[0])
        self.assertFalse(kwargs["enable_download"])
        self.assertTrue(kwargs["enable_skip"])
        self.assertEqual(kwargs["origin"], "settings")
        self.assertEqual(kwargs["status_icon"], "✅")
        self.assertEqual(kwargs["skip_label"], "Close")
        self.assertEqual(kwargs["changelog"], app._format_update_changelog())
        self.assertEqual(app._update_release_notes, "Bug fixes\nPerformance improvements")

    def test_overlay_buttons_dispatch_modes(self) -> None:
        app = Sims4ModSorterApp.__new__(Sims4ModSorterApp)
        app._start_update_download = mock.Mock()
        app._update_download_url = "https://example.com/update.zip"
        app._update_release_page_url = "https://example.com/release"

        app._on_update_overlay_auto_update()
        app._start_update_download.assert_called_once_with(mode="auto-install")

        app._start_update_download.reset_mock()
        app._on_update_overlay_manual_download()
        app._start_update_download.assert_called_once_with(mode="manual-download")

    def test_manual_download_button_opens_release_page_when_no_download(self) -> None:
        app = Sims4ModSorterApp.__new__(Sims4ModSorterApp)
        app._start_update_download = mock.Mock()
        app._open_release_page = mock.Mock()
        app._update_download_url = None
        app._update_release_page_url = "https://example.com/release"

        app._on_update_overlay_manual_download()

        app._open_release_page.assert_called_once()
        app._start_update_download.assert_not_called()

    def test_auto_install_displays_release_notes(self) -> None:
        app = Sims4ModSorterApp.__new__(Sims4ModSorterApp)
        app._update_download_mode = mock.Mock()
        app._update_download_mode.get.return_value = "simple"
        app._install_update_package = mock.Mock(return_value=(Path("/new"), 4))
        app._latest_version = "2.0.0"
        app._update_available = True
        app._update_release_notes = "Feature A\nFeature B"
        app._update_release_page_url = "https://example.com/release"
        app._show_update_overlay = mock.Mock()
        app._show_info_overlay = mock.Mock()
        app._hide_update_overlay = mock.Mock()
        app._show_error_overlay = mock.Mock()
        app._show_warning_overlay = mock.Mock()
        app._launch_new_installation = mock.Mock(return_value=True)
        app._schedule_update_cleanup = mock.Mock()
        app.after = mock.Mock()
        app._shutdown_after_update = mock.Mock()
        app.log = mock.Mock()

        app._handle_update_download_success(Path("/download/update.zip"), mode="auto-install")

        self.assertGreaterEqual(len(app._show_update_overlay.call_args_list), 2)
        final_call = app._show_update_overlay.call_args_list[-1]
        self.assertEqual(final_call.kwargs.get("status_icon"), "✅")
        self.assertEqual(final_call.kwargs.get("changelog"), app._format_update_changelog())
        app._show_info_overlay.assert_not_called()

    def test_update_overlay_displays_changelog_without_progress(self) -> None:
        app = Sims4ModSorterApp.__new__(Sims4ModSorterApp)
        app._ensure_update_overlay = mock.Mock(return_value=_DummyOverlay())
        app._update_overlay_origin = "general"
        app._update_overlay_headline = _DummyStringVar()
        app._update_overlay_status_icon = _DummyStringVar()
        app._update_overlay_message = _DummyStringVar()
        app._update_overlay_progress_frame = _DummyProgressFrame()
        app._update_overlay_progress_title = _DummyStringVar()
        app._update_overlay_progress_detail = _DummyStringVar()
        app._update_overlay_progress_title_label = None
        app._update_overlay_progress_detail_label = None
        app._update_overlay_progress = None
        app._update_overlay_download_btn = None
        app._update_overlay_manual_btn = None
        app._update_overlay_details_btn = None
        app._update_overlay_skip_btn = None
        app._update_mode_simple_radio = None
        app._update_mode_advanced_radio = None
        app._update_overlay_set_changelog = mock.Mock()
        app._update_overlay_container = _DummyContainer()
        app.focus_set = mock.Mock()
        app._center_update_overlay = mock.Mock()
        app._update_overlay_visible = False
        app._update_download_url = None
        app._update_release_page_url = None

        app._show_update_overlay(
            "Update available",
            progress=False,
            enable_download=False,
            enable_skip=True,
            enable_details=False,
            enable_manual=False,
            progress_subtext=None,
            changelog="Patch notes",
        )

        self.assertTrue(app._update_overlay_progress_frame.grid_called)
        self.assertFalse(app._update_overlay_progress_frame.grid_remove_called)
        app._update_overlay_set_changelog.assert_called_once_with("Patch notes")

