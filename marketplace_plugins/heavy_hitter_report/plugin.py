"""Heavy Hitter Report marketplace plugin."""

from __future__ import annotations

from typing import Iterable, List, Sequence

from scanner import FileItem

_DEFAULT_THRESHOLD_MB = 50.0


def _find_large_mods(items: Iterable[FileItem], threshold_mb: float = _DEFAULT_THRESHOLD_MB) -> List[FileItem]:
    large = [item for item in items if item.size_mb >= threshold_mb]
    large.sort(key=lambda item: item.size_mb, reverse=True)
    return large


def _format_summary(large: Sequence[FileItem], threshold_mb: float = _DEFAULT_THRESHOLD_MB) -> str:
    if not large:
        return f"[Heavy Hitter Report] No mods above {threshold_mb:.0f} MB detected."
    sample = ", ".join(f"{item.name} ({item.size_mb:.1f} MB)" for item in large[:3])
    if len(large) > 3:
        sample += ", …"
    return (
        "[Heavy Hitter Report] "
        f"{len(large)} file(s) exceed {threshold_mb:.0f} MB. Largest: {sample}."
    )


def register(api) -> None:
    """Register heavy file reporting hook."""

    def _on_post_scan(items: List[FileItem], _context, _api) -> None:
        large = _find_large_mods(items)
        api.log(_format_summary(large))

    api.register_post_scan_hook(_on_post_scan)


__all__ = ["_find_large_mods", "_format_summary", "register"]
