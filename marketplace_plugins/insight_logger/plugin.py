from __future__ import annotations

from collections import Counter
from typing import Dict, List

from scanner import FileItem


def _summarise_categories(items: List[FileItem]) -> str:
    counts = Counter(item.guess_type or "Unknown" for item in items)
    if not counts:
        return ""
    top = counts.most_common(3)
    fragments = [f"{name}: {count}" for name, count in top]
    return ", ".join(fragments)


def register(api) -> None:
    """Register the plugin with the Sims4 Mod Sorter."""

    def _on_post_scan(items: List[FileItem], _context: Dict[str, object], _api) -> None:
        total = len(items)
        summary = _summarise_categories(items)
        if summary:
            message = f"[Insight Logger] Scanned {total} file(s). Top categories: {summary}."
        else:
            message = f"[Insight Logger] Scanned {total} file(s)."
        api.log(message)

    api.register_post_scan_hook(_on_post_scan)
