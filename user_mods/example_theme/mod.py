"""Example mod that adds a theme and logs scan statistics."""
from __future__ import annotations

from typing import Dict


PALETTE: Dict[str, str] = {
    "bg": "#1f2933",
    "fg": "#f5f7fa",
    "alt": "#323f4b",
    "accent": "#f9703e",
    "sel": "#3e4c59",
}


def register(api):
    """Entrypoint expected by the sorter. Called with a ModAPI instance."""

    api.register_theme("Sunrise Demo", PALETTE)

    def post_scan(items, context, _api):
        package_count = sum(1 for item in items if getattr(item, "ext", "").lower() == ".package")
        if package_count:
            api.log(f"Sunrise Demo theme saw {package_count} package file(s) in the last scan.")

    api.register_post_scan_hook(post_scan)
    api.log("Sunrise Demo mod loaded. Switch to the theme from Settings → Themes.")
