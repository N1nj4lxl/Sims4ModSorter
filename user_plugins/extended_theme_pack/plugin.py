"""Extended theme pack plugin demonstrating the Sims4ModSorter API."""

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
    "Aurora Frost": {
        "bg": "#0f1a2a",
        "fg": "#e4f1ff",
        "alt": "#1b2c42",
        "accent": "#6ad5ff",
        "sel": "#2d4f73",
    },
    "Desert Mirage": {
        "bg": "#2c1a0f",
        "fg": "#f6ebde",
        "alt": "#3f2616",
        "accent": "#ffb45e",
        "sel": "#7a4b23",
    },
    "Retro Neon": {
        "bg": "#130f26",
        "fg": "#f1f0ff",
        "alt": "#231c3a",
        "accent": "#ff5fd2",
        "sel": "#503f7c",
    },
    "Woodland Campfire": {
        "bg": "#1a1109",
        "fg": "#f9efe2",
        "alt": "#2c1d12",
        "accent": "#ff7a3c",
        "sel": "#5b3520",
    },
}


def register(api):
    api.log("Extended theme pack loaded. Extra themes available from settings.")
    for name, palette in THEMES.items():
        api.register_theme(name, palette)

    def pre_scan(context, _api):
        ignore_exts = set(str(ext).strip() for ext in context.get("ignore_exts", set()) if str(ext).strip())
        ignore_exts.update({".bak", ".tmp"})
        context["ignore_exts"] = ignore_exts
        _api.log("Extended theme pack: added .bak and .tmp to ignored extensions for scans.")

    def post_scan(items, context, _api):
        flagged = 0
        for item in items:
            name_lower = getattr(item, "name", "").lower()
            if name_lower.endswith(".package") and "preview" in name_lower:
                note = getattr(item, "notes", "")
                item.notes = (note + "; Marked by extended theme pack").strip("; ")
                flagged += 1
        if flagged:
            _api.log(f"Extended theme pack: flagged {flagged} preview package(s) for review.")

    api.register_pre_scan_hook(pre_scan)
    api.register_post_scan_hook(post_scan)
