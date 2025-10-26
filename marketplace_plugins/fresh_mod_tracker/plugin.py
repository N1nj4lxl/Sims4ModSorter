from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, List

from scanner import FileItem

_WINDOW = timedelta(days=14)


def _is_recent(path) -> bool:
    try:
        stat = path.stat()
    except Exception:
        return False
    modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
    return modified >= datetime.now(tz=timezone.utc) - _WINDOW


def register(api) -> None:
    """Register the freshness tracker."""

    def _on_post_scan(items: List[FileItem], _context: Dict[str, object], _api) -> None:
        fresh = sum(1 for item in items if _is_recent(item.path))
        if fresh:
            api.log(f"[Fresh Mod Tracker] {fresh} mod(s) updated in the last {_WINDOW.days} days.")
        else:
            api.log("[Fresh Mod Tracker] No recently updated mods detected.")

    api.register_post_scan_hook(_on_post_scan)
