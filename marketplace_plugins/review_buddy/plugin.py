"""Review Buddy marketplace plugin."""

from __future__ import annotations

from typing import Iterable, List, Sequence

from scanner import FileItem, UNKNOWN_DEFAULT_FOLDER


def _collect_unknowns(items: Iterable[FileItem]) -> List[FileItem]:
    return [
        item
        for item in items
        if item.target_folder == UNKNOWN_DEFAULT_FOLDER or item.guess_type == "Unknown"
    ]


def _format_summary(unknowns: Sequence[FileItem]) -> str:
    if not unknowns:
        return "[Review Buddy] All mods were classified with known destinations."
    sample = ", ".join(item.name for item in unknowns[:5])
    if len(unknowns) > 5:
        sample += ", …"
    return (
        "[Review Buddy] "
        f"{len(unknowns)} mod(s) need attention before sorting. Examples: {sample}."
    )


def register(api) -> None:
    """Register review helper hook."""

    def _on_post_scan(items: List[FileItem], _context, _api) -> None:
        unknowns = _collect_unknowns(items)
        api.log(_format_summary(unknowns))

    api.register_post_scan_hook(_on_post_scan)


__all__ = ["_collect_unknowns", "_format_summary", "register"]
