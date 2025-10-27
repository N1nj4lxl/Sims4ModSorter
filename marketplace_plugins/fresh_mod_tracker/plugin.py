from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Sequence, Tuple

from scanner import FileItem

_WINDOW = timedelta(days=14)


def _is_recent(path) -> bool:
    try:
        stat = path.stat()
    except Exception:
        return False
    modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
    return modified >= datetime.now(tz=timezone.utc) - _WINDOW


def _recent_files(items: Iterable[FileItem]) -> List[Tuple[FileItem, datetime]]:
    """Return recently updated files sorted by modified time descending."""

    results: List[Tuple[FileItem, datetime]] = []
    for item in items:
        try:
            stat = item.path.stat()
        except Exception:
            continue
        modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        if datetime.now(tz=timezone.utc) - modified <= _WINDOW:
            results.append((item, modified))
    results.sort(key=lambda pair: pair[1], reverse=True)
    return results


def _format_recent_summary(records: Sequence[Tuple[FileItem, datetime]]) -> str:
    if not records:
        return "[Fresh Mod Tracker] No recently updated mods detected."

    sample = ", ".join(
        f"{item.name} ({modified.strftime('%Y-%m-%d')})" for item, modified in records[:3]
    )
    if len(records) > 3:
        sample += ", …"
    return (
        "[Fresh Mod Tracker] "
        f"{len(records)} mod(s) updated in the last {_WINDOW.days} days. Latest: {sample}."
    )


def register(api) -> None:
    """Register the freshness tracker."""

    def _on_post_scan(items: List[FileItem], _context: Dict[str, object], _api) -> None:
        records = _recent_files(items)
        api.log(_format_recent_summary(records))

    api.register_post_scan_hook(_on_post_scan)


__all__ = ["_is_recent", "_recent_files", "_format_recent_summary", "register"]
