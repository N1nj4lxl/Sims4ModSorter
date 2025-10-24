"""Example plugin for Sims4ModSorter demonstrating the plugin API."""

from __future__ import annotations

THEMES = {
    "Ocean Breeze": {
        "bg": "#0b1d26",
        "fg": "#e7f9ff",
        "alt": "#12313f",
        "accent": "#2bb3ff",
        "sel": "#1f4d63",
    },
    "Sunset Citrus": {
        "bg": "#34100f",
        "fg": "#fff6ec",
        "alt": "#4a1a17",
        "accent": "#ff8a3d",
        "sel": "#a64228",
    },
    "Moonlit Lavender": {
        "bg": "#1c1426",
        "fg": "#efe7ff",
        "alt": "#2a1f39",
        "accent": "#9c6bff",
        "sel": "#46346d",
    },
    "Verdant Grove": {
        "bg": "#112015",
        "fg": "#e3f5e9",
        "alt": "#1a3120",
        "accent": "#4ec27f",
        "sel": "#295a3a",
    },
}


def register(api):
    api.log("Example plugin loaded. Example themes available from settings.")
    for name, palette in THEMES.items():
        api.register_theme(name, palette)

    def pre_scan(context, _api):
        ignore_exts = set(str(ext).strip() for ext in context.get("ignore_exts", set()) if str(ext).strip())
        ignore_exts.update({".bak", ".tmp"})
        context["ignore_exts"] = ignore_exts
        _api.log("Example plugin: added .bak and .tmp to ignored extensions for scans.")

    def post_scan(items, context, _api):
        flagged = 0
        for item in items:
            name_lower = getattr(item, "name", "").lower()
            if name_lower.endswith(".package") and "preview" in name_lower:
                note = getattr(item, "notes", "")
                item.notes = (note + "; Marked by example plugin").strip("; ")
                flagged += 1
        if flagged:
            _api.log(f"Example plugin: flagged {flagged} preview package(s) for review.")

    api.register_pre_scan_hook(pre_scan)
    api.register_post_scan_hook(post_scan)
