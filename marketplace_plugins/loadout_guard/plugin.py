from __future__ import annotations

from typing import Dict, Iterable, List, Mapping

from scanner import FileItem


def _loadout_stats(items: Iterable[FileItem]) -> Mapping[str, float]:
    total = 0
    included = 0
    disabled = 0
    for item in items:
        total += 1
        if item.disabled or not item.include:
            disabled += 1
            continue
        included += 1
    excluded = total - included
    ratio = (included / total) if total else 0.0
    return {
        "total": total,
        "included": included,
        "excluded": excluded,
        "disabled": disabled,
        "ratio": ratio,
    }


def _format_message(stats: Mapping[str, float]) -> Dict[str, str]:
    total = int(stats["total"])
    included = int(stats["included"])
    excluded = int(stats["excluded"])
    disabled = int(stats["disabled"])
    if total == 0:
        return {"message": "[Loadout Guard] No files processed in the latest scan.", "level": "info"}

    level = "info"
    if excluded > included:
        level = "warn"
        message = (
            "[Loadout Guard] "
            f"{excluded} file(s) excluded versus {included} included. "
            "Consider updating your loadout."
        )
    else:
        percent = stats["ratio"] * 100
        message = (
            "[Loadout Guard] Loadout is balanced "
            f"({included} included / {excluded} excluded, {percent:.1f}% active)."
        )

    if disabled:
        message += f" {disabled} file(s) are disabled regardless of loadout."
    return {"message": message, "level": level}


def register(api) -> None:
    """Register loadout balance monitoring."""

    def _on_post_scan(items: List[FileItem], _context: Dict[str, object], _api) -> None:
        stats = _loadout_stats(items)
        payload = _format_message(stats)
        api.log(payload["message"], level=payload["level"])

    api.register_post_scan_hook(_on_post_scan)


__all__ = ["_loadout_stats", "_format_message", "register"]
