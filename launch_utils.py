"""Shared helpers for launch diagnostics and update checks."""
from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

BASE_DIR = Path(__file__).resolve().parent
LAUNCH_LOG_PATH = BASE_DIR / "launch_diagnostics.jsonl"
VERSION_FILE = BASE_DIR / "VERSION"
UPDATE_CONFIG_FILE = BASE_DIR / "update_config.json"


@dataclass(slots=True)
class UpdateResult:
    latest_version: Optional[str]
    is_newer: bool
    download_url: Optional[str]
    message: Optional[str]


def log_launch_event(component: str, event: str, details: Optional[Dict[str, object]] = None) -> None:
    """Append a diagnostic event so debug tools can explain fallbacks."""
    entry = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "component": component,
        "event": event,
        "details": details or {},
    }
    try:
        with LAUNCH_LOG_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=False))
            handle.write("\n")
    except Exception:
        pass


def get_local_version(component: str | None = None) -> str:
    """Return the version string stored in VERSION."""
    try:
        text = VERSION_FILE.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return "0.0.0"
    return text or "0.0.0"


def _load_update_config() -> Dict[str, object]:
    if not UPDATE_CONFIG_FILE.exists():
        return {}
    try:
        with UPDATE_CONFIG_FILE.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            return data
    except Exception:
        return {}
    return {}


def _component_config(component: str, config: Dict[str, object]) -> Dict[str, object]:
    components = config.get("components")
    if isinstance(components, dict):
        specific = components.get(component)
        if isinstance(specific, dict):
            return specific
    return {}


def _detect_repo_from_git(base: Path) -> Optional[tuple[str, str]]:
    git_config = base / ".git" / "config"
    if not git_config.exists():
        return None
    try:
        text = git_config.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return None
    match = re.search(r"url\s*=\s*(.+)", text)
    if not match:
        return None
    url = match.group(1).strip()
    gh_match = re.search(r"github\.com[:/](?P<owner>[^/]+)/(?P<repo>[^/.]+)(?:\.git)?", url)
    if not gh_match:
        return None
    return gh_match.group("owner"), gh_match.group("repo")


def _build_version_url(
    component: str, config: Dict[str, object]
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    component_cfg = _component_config(component, config)
    version_url = component_cfg.get("version_url") if isinstance(component_cfg.get("version_url"), str) else None
    download_url = component_cfg.get("download_url") if isinstance(component_cfg.get("download_url"), str) else None
    release_api_url = (
        component_cfg.get("release_api_url")
        if isinstance(component_cfg.get("release_api_url"), str)
        else None
    )

    if version_url and version_url.strip():
        version_url = version_url.strip()

    if not download_url and isinstance(config.get("download_url"), str):
        download_url = config["download_url"].strip() or None

    if release_api_url and release_api_url.strip():
        release_api_url = release_api_url.strip()

    if not version_url:
        # Attempt to derive from repo info in config or .git.
        owner = None
        repo = None
        if isinstance(config.get("repo_owner"), str):
            owner = config["repo_owner"].strip() or None
        if isinstance(config.get("repo_name"), str):
            repo = config["repo_name"].strip() or None
        if not owner or not repo:
            detected = _detect_repo_from_git(BASE_DIR)
            if detected:
                owner, repo = detected
        branch = "main"
        if isinstance(config.get("branch"), str) and config["branch"].strip():
            branch = config["branch"].strip()
        if owner and repo:
            version_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/VERSION"
            if not download_url:
                download_url = f"https://github.com/{owner}/{repo}/releases/latest"
            if not release_api_url:
                release_api_url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
    return version_url, download_url, release_api_url


def _parse_version(value: str) -> tuple[int, ...]:
    parts = []
    for chunk in re.split(r"[.+-]", value):
        if chunk.isdigit():
            parts.append(int(chunk))
        else:
            # Non-numeric suffixes are treated as zero to keep ordering predictable.
            parts.append(0)
    return tuple(parts) if parts else (0,)


def _normalise_version(value: str) -> str:
    value = value.strip()
    if value.lower().startswith("v") and value[1:2].isdigit():
        return value[1:]
    return value


def _build_request(url: str) -> urllib.request.Request:
    return urllib.request.Request(url, headers={"User-Agent": "Sims4ModSorter Update Check"})


def check_for_update(component: str, current_version: str, timeout: float = 5.0) -> UpdateResult:
    """Fetch the latest version string and compare it with the current version."""
    config = _load_update_config()
    version_url, download_url, release_api_url = _build_version_url(component, config)
    remote_version: Optional[str] = None
    resolved_download = download_url
    failure_message: Optional[str] = None

    if not release_api_url and not version_url:
        return UpdateResult(None, False, resolved_download, "Update source not configured.")

    if release_api_url:
        try:
            with urllib.request.urlopen(_build_request(release_api_url), timeout=timeout) as response:
                payload = response.read().decode("utf-8", errors="ignore")
            data = json.loads(payload)
        except urllib.error.URLError as exc:
            failure_message = f"Update check failed: {exc.reason if hasattr(exc, 'reason') else exc}"
        except Exception as exc:
            failure_message = f"Update check failed: {exc}"
        else:
            if isinstance(data, dict):
                candidate = data.get("tag_name") or data.get("name")
                if isinstance(candidate, str) and candidate.strip():
                    remote_version = _normalise_version(candidate)
                html_url = data.get("html_url")
                if isinstance(html_url, str) and html_url.strip():
                    resolved_download = html_url.strip()
            else:
                failure_message = "Update check failed: unexpected release payload"

    if not remote_version and version_url:
        try:
            with urllib.request.urlopen(_build_request(version_url), timeout=timeout) as response:
                remote_version = response.read().decode("utf-8", errors="ignore").strip()
        except urllib.error.URLError as exc:
            if not failure_message:
                failure_message = f"Update check failed: {exc.reason if hasattr(exc, 'reason') else exc}"
        except Exception as exc:
            if not failure_message:
                failure_message = f"Update check failed: {exc}"
        else:
            if remote_version:
                remote_version = _normalise_version(remote_version)

    if not remote_version:
        if failure_message:
            return UpdateResult(None, False, resolved_download, failure_message)
        return UpdateResult(None, False, resolved_download, "Update check returned an empty version string.")

    try:
        remote_tuple = _parse_version(remote_version)
        current_tuple = _parse_version(current_version)
    except Exception:
        return UpdateResult(remote_version, False, resolved_download, "Unable to compare version strings.")

    is_newer = remote_tuple > current_tuple
    return UpdateResult(remote_version, is_newer, resolved_download, None)

