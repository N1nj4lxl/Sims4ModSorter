from __future__ import annotations

from typing import Dict, List

from scanner import DUPLICATE_EXTRA_KEY, FileItem


def register(api) -> None:
    """Register duplicate reporting hooks."""

    def _on_post_scan(items: List[FileItem], _context: Dict[str, object], _api) -> None:
        duplicates = [item for item in items if item.extras.get(DUPLICATE_EXTRA_KEY)]
        if duplicates:
            message = f"[Duplicate Spotlight] {len(duplicates)} potential duplicate mod(s) found."
        else:
            message = "[Duplicate Spotlight] No duplicates flagged in the latest scan."
        api.log(message)

    api.register_post_scan_hook(_on_post_scan)
