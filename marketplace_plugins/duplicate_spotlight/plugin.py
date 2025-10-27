"""Duplicate Spotlight marketplace plugin."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Dict, Iterable, List, Mapping, Sequence

from scanner import DUPLICATE_EXTRA_KEY, FileItem


_DUPLICATE_OF_RE = re.compile(r"Duplicate of (?P<primary>.+)")


def _group_duplicates(items: Iterable[FileItem]) -> Mapping[str, List[FileItem]]:
    """Return a mapping of primary file path to duplicate items."""

    groups: dict[str, List[FileItem]] = defaultdict(list)
    for item in items:
        marker = item.extras.get(DUPLICATE_EXTRA_KEY)
        if not marker:
            continue
        tooltip = item.tooltips.get(DUPLICATE_EXTRA_KEY, "")
        match = _DUPLICATE_OF_RE.search(tooltip)
        primary = match.group("primary") if match else item.relpath
        groups[primary].append(item)
    return {key: value for key, value in groups.items() if value}


def _build_summary(groups: Mapping[str, Sequence[FileItem]]) -> str:
    """Create a human friendly log message summarising duplicates."""

    total = sum(len(group) for group in groups.values())
    if not total:
        return "[Duplicate Spotlight] No duplicates flagged in the latest scan."

    ordered = sorted(groups.items(), key=lambda pair: (-len(pair[1]), pair[0]))
    top_primary, top_duplicates = ordered[0]
    sample = ", ".join(item.name for item in top_duplicates[:3])
    if len(top_duplicates) > 3:
        sample += ", …"
    return (
        "[Duplicate Spotlight] "
        f"{total} potential duplicate mod(s) found across {len(groups)} group(s). "
        f"Most duplicates stem from '{top_primary}' (examples: {sample})."
    )


def register(api) -> None:
    """Register duplicate reporting hooks."""

    def _on_post_scan(items: List[FileItem], _context: Dict[str, object], _api) -> None:
        groups = _group_duplicates(items)
        api.log(_build_summary(groups))

    api.register_post_scan_hook(_on_post_scan)


__all__ = ["_group_duplicates", "_build_summary", "register"]
