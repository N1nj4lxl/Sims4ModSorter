"""Category Snapshot marketplace plugin."""

from __future__ import annotations

from collections import Counter
from typing import Iterable, List, Sequence, Tuple

from scanner import CATEGORY_ORDER, FileItem


def _collect_counts(items: Iterable[FileItem]) -> List[Tuple[str, int]]:
    counter: Counter[str] = Counter()
    for item in items:
        category = item.guess_type or "Unknown"
        counter[category] += 1
    results = list(counter.items())
    if not results:
        return []
    order = {name: index for index, name in enumerate(CATEGORY_ORDER)}
    results.sort(key=lambda pair: (order.get(pair[0], len(order)), pair[0]))
    return results


def _format_summary(counts: Sequence[Tuple[str, int]]) -> str:
    if not counts:
        return "[Category Snapshot] No files processed in the latest scan."
    total = sum(count for _, count in counts)
    top = ", ".join(f"{name} ({count})" for name, count in counts[:3])
    if len(counts) > 3:
        top += ", …"
    return (
        "[Category Snapshot] "
        f"{total} file(s) scanned. Top categories: {top}."
    )


def register(api) -> None:
    """Register category summary hook."""

    def _on_post_scan(items: List[FileItem], _context, _api) -> None:
        counts = _collect_counts(items)
        api.log(_format_summary(counts))

    api.register_post_scan_hook(_on_post_scan)


__all__ = ["_collect_counts", "_format_summary", "register"]
