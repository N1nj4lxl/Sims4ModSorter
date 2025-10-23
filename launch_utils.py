"""Shared helpers for launch diagnostics and update checks."""
from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from urllib.parse import urlparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Pattern

BASE_DIR = Path(__file__).resolve().parent
LAUNCH_LOG_PATH = BASE_DIR / "launch_diagnostics.jsonl"
VERSION_FILE = BASE_DIR / "VERSION"
UPDATE_CONFIG_FILE = BASE_DIR / "update_config.json"


def _clean_str(value: object) -> Optional[str]:
    if isinstance(value, str):
        value = value.strip()
        if value:
            return value
    return None


def _extract_repo_from_url(url: str) -> tuple[Optional[str], Optional[str]]:
    try:
        parsed = urlparse(url)
    except Exception:
        return None, None
    host = parsed.netloc.lower()
    parts = [segment for segment in parsed.path.split("/") if segment]
    if not parts:
        return None, None
    if host == "api.github.com" and len(parts) >= 3 and parts[0] == "repos":
        return parts[1], parts[2]
    if host in {"github.com", "codeload.github.com"} and len(parts) >= 2:
        return parts[0], parts[1]
    return None, None


def _is_official_release_page(url: str, owner: Optional[str], repo: Optional[str]) -> bool:
    if not owner or not repo:
        return False
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme != "https":
        return False
    if parsed.netloc.lower() != "github.com":
        return False
    expected_prefix = f"/{owner}/{repo}/releases"
    return parsed.path.startswith(expected_prefix)


def _is_official_asset_url(url: str, owner: Optional[str], repo: Optional[str]) -> bool:
    if not owner or not repo:
        return False
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme != "https":
        return False
    if parsed.netloc.lower() != "github.com":
        return False
    expected_prefix = f"/{owner}/{repo}/releases/download/"
    return parsed.path.startswith(expected_prefix)


def _is_official_tarball_url(url: str, owner: Optional[str], repo: Optional[str]) -> bool:
    if not owner or not repo:
        return False
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme != "https":
        return False
    host = parsed.netloc.lower()
    path = parsed.path
    if host == "api.github.com":
        return path.startswith(f"/repos/{owner}/{repo}/tarball")
    if host == "codeload.github.com":
        return path.startswith(f"/{owner}/{repo}/")
    return False


@dataclass(slots=True)
class UpdateResult:
    latest_version: Optional[str]
    is_newer: bool
    download_url: Optional[str]
    message: Optional[str]
    release_page_url: Optional[str] = None
    asset_name: Optional[str] = None
    asset_size: Optional[int] = None
    asset_content_type: Optional[str] = None


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
) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    component_cfg = _component_config(component, config)
    version_url = _clean_str(component_cfg.get("version_url"))
    download_url = _clean_str(component_cfg.get("download_url"))
    release_api_url = _clean_str(component_cfg.get("release_api_url"))

    owner = _clean_str(component_cfg.get("repo_owner"))
    repo = _clean_str(component_cfg.get("repo_name"))
    if not download_url and isinstance(config.get("download_url"), str):
        download_url = _clean_str(config.get("download_url"))
    if not owner and isinstance(config.get("repo_owner"), str):
        owner = _clean_str(config.get("repo_owner"))
    if not repo and isinstance(config.get("repo_name"), str):
        repo = _clean_str(config.get("repo_name"))

    if release_api_url:
        api_owner, api_repo = _extract_repo_from_url(release_api_url)
        if api_owner and api_repo:
            if owner and owner.lower() != api_owner.lower():
                release_api_url = None
            elif repo and repo.lower() != api_repo.lower():
                release_api_url = None
            else:
                if not owner or owner.lower() == api_owner.lower():
                    owner = api_owner
                if not repo or repo.lower() == api_repo.lower():
                    repo = api_repo
        else:
            release_api_url = None

    if download_url and (not owner or not repo):
        dl_owner, dl_repo = _extract_repo_from_url(download_url)
        if dl_owner and dl_repo:
            if not owner or owner.lower() == dl_owner.lower():
                owner = dl_owner
            if not repo or repo.lower() == dl_repo.lower():
                repo = dl_repo

    if version_url and (not owner or not repo):
        v_owner, v_repo = _extract_repo_from_url(version_url)
        if v_owner and v_repo:
            if not owner or owner.lower() == v_owner.lower():
                owner = v_owner
            if not repo or repo.lower() == v_repo.lower():
                repo = v_repo

    if not owner or not repo:
        detected = _detect_repo_from_git(BASE_DIR)
        if detected:
            owner = owner or detected[0]
            repo = repo or detected[1]

    branch = _clean_str(component_cfg.get("branch")) or _clean_str(config.get("branch")) or "main"

    if not version_url and owner and repo:
        version_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/VERSION"
    if not download_url and owner and repo:
        download_url = f"https://github.com/{owner}/{repo}/releases/latest"
    if not release_api_url and owner and repo:
        release_api_url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"

    return version_url, download_url, release_api_url, owner, repo


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
    component_cfg = _component_config(component, config)
    version_url, config_download_url, release_api_url, owner, repo = _build_version_url(component, config)
    remote_version: Optional[str] = None
    resolved_download: Optional[str] = None
    failure_message: Optional[str] = None
    release_page_url: Optional[str] = None
    asset_name: Optional[str] = None
    asset_size: Optional[int] = None
    asset_content_type: Optional[str] = None

    asset_name_preference = _clean_str(component_cfg.get("download_asset_name"))
    if not asset_name_preference and isinstance(config.get("download_asset_name"), str):
        asset_name_preference = _clean_str(config.get("download_asset_name"))

    asset_pattern_text = _clean_str(component_cfg.get("download_asset_pattern"))
    if not asset_pattern_text and isinstance(config.get("download_asset_pattern"), str):
        asset_pattern_text = _clean_str(config.get("download_asset_pattern"))
    asset_pattern: Optional[Pattern[str]] = None
    if asset_pattern_text:
        try:
            asset_pattern = re.compile(asset_pattern_text, re.IGNORECASE)
        except re.error:
            asset_pattern = None

    if config_download_url:
        if _is_official_asset_url(config_download_url, owner, repo) or _is_official_tarball_url(
            config_download_url, owner, repo
        ):
            resolved_download = config_download_url
        elif _is_official_release_page(config_download_url, owner, repo):
            release_page_url = config_download_url

    if not release_api_url and not version_url:
        return UpdateResult(
            None,
            False,
            resolved_download,
            "Update source not configured.",
            release_page_url,
            asset_name,
            asset_size,
            asset_content_type,
        )

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
                candidates: list[str] = []
                name_value = data.get("name")
                if isinstance(name_value, str) and name_value.strip():
                    candidates.append(name_value.strip())
                tag_value = data.get("tag_name")
                if isinstance(tag_value, str) and tag_value.strip():
                    candidates.append(tag_value.strip())
                for candidate in candidates:
                    normalized = _normalise_version(candidate)
                    if normalized:
                        remote_version = normalized
                        break
                html_url = data.get("html_url")
                if isinstance(html_url, str) and html_url.strip():
                    html_url = html_url.strip()
                    html_owner, html_repo = _extract_repo_from_url(html_url)
                    if html_owner and html_repo:
                        if owner and owner.lower() != html_owner.lower():
                            pass
                        elif repo and repo.lower() != html_repo.lower():
                            pass
                        else:
                            if not owner or owner.lower() == html_owner.lower():
                                owner = html_owner
                            if not repo or repo.lower() == html_repo.lower():
                                repo = html_repo
                    if _is_official_release_page(html_url, owner, repo):
                        release_page_url = html_url
                assets = data.get("assets")
                if isinstance(assets, list):
                    best_score: tuple[int, int, int] | None = None
                    best_asset: Optional[dict[str, object]] = None
                    for asset in assets:
                        if not isinstance(asset, dict):
                            continue
                        browser_download = asset.get("browser_download_url")
                        if not (
                            isinstance(browser_download, str)
                            and browser_download.strip()
                            and _is_official_asset_url(browser_download.strip(), owner, repo)
                        ):
                            continue
                        candidate_name = _clean_str(asset.get("name"))
                        size_value = asset.get("size")
                        candidate_size = None
                        if isinstance(size_value, int):
                            candidate_size = max(size_value, 0)
                        candidate_content_type = _clean_str(asset.get("content_type"))
                        match_rank = 0
                        if candidate_name and asset_name_preference and candidate_name.lower() == asset_name_preference.lower():
                            match_rank = 2
                        elif candidate_name and asset_pattern and asset_pattern.search(candidate_name):
                            match_rank = 1
                        size_rank = 1 if candidate_size and candidate_size > 0 else 0
                        score = (match_rank, size_rank, candidate_size or 0)
                        if best_score is None or score > best_score:
                            best_score = score
                            best_asset = {
                                "url": browser_download.strip(),
                                "name": candidate_name,
                                "size": candidate_size,
                                "content_type": candidate_content_type,
                            }
                    if best_asset:
                        resolved_download = best_asset["url"]  # type: ignore[assignment]
                        asset_name = best_asset.get("name")  # type: ignore[assignment]
                        asset_size = best_asset.get("size")  # type: ignore[assignment]
                        asset_content_type = best_asset.get("content_type")  # type: ignore[assignment]
                tarball_url = data.get("tarball_url")
                if (
                    not resolved_download
                    and isinstance(tarball_url, str)
                    and tarball_url.strip()
                    and _is_official_tarball_url(tarball_url.strip(), owner, repo)
                ):
                    resolved_download = tarball_url.strip()
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
            return UpdateResult(
                None,
                False,
                resolved_download,
                failure_message,
                release_page_url,
                asset_name,
                asset_size,
                asset_content_type,
            )
        return UpdateResult(
            None,
            False,
            resolved_download,
            "Update check returned an empty version string.",
            release_page_url,
            asset_name,
            asset_size,
            asset_content_type,
        )

    try:
        remote_tuple = _parse_version(remote_version)
        current_tuple = _parse_version(current_version)
    except Exception:
        return UpdateResult(
            remote_version,
            False,
            resolved_download,
            "Unable to compare version strings.",
            release_page_url,
            asset_name,
            asset_size,
            asset_content_type,
        )

    is_newer = remote_tuple > current_tuple
    return UpdateResult(
        remote_version,
        is_newer,
        resolved_download,
        None,
        release_page_url,
        asset_name,
        asset_size,
        asset_content_type,
    )

