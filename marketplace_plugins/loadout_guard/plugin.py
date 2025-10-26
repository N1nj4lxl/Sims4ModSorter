from __future__ import annotations

from typing import Dict, List

from scanner import FileItem


def register(api) -> None:
    """Register loadout balance monitoring."""

    def _on_post_scan(items: List[FileItem], _context: Dict[str, object], _api) -> None:
        included = sum(1 for item in items if item.include and not item.disabled)
        excluded = sum(1 for item in items if not item.include or item.disabled)
        if not items:
            api.log("[Loadout Guard] No files processed in the latest scan.")
            return
        if excluded > included:
            api.log(
                f"[Loadout Guard] {excluded} file(s) excluded versus {included} included. Consider updating your loadout.",
                level="warn",
            )
        else:
            api.log(
                f"[Loadout Guard] Loadout is balanced ({included} included / {excluded} excluded)."
            )

    api.register_post_scan_hook(_on_post_scan)
