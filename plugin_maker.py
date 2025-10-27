"""Command line helper to scaffold Sims4 Mod Sorter plugins."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from textwrap import dedent

DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "user_plugins"


def _sanitize(value: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z_-]+", "-", value.strip())
    normalized = normalized.strip("-_")
    return normalized or "plugin"


def _write_file(path: Path, content: str, *, force: bool = False) -> None:
    if path.exists() and not force:
        raise FileExistsError(f"Refusing to overwrite existing file: {path}")
    path.write_text(content, encoding="utf-8")


def create_plugin(**kwargs) -> Path:
    name: str = kwargs["name"]
    description: str = kwargs.get("description") or "Custom Sims4 Mod Sorter plugin."
    version: str = kwargs.get("version") or "0.1.0"
    author: str = kwargs.get("author") or "Unknown Author"
    folder: str = kwargs.get("folder") or _sanitize(name)
    output_dir: Path = Path(kwargs.get("output_dir") or DEFAULT_OUTPUT_DIR)
    force: bool = kwargs.get("force", False)

    output_dir.mkdir(parents=True, exist_ok=True)
    plugin_dir = output_dir / folder
    plugin_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = plugin_dir / "plugin.json"
    manifest = {
        "name": name,
        "entry": "plugin.py",
        "version": version,
        "description": description,
        "author": author,
    }
    _write_file(manifest_path, json.dumps(manifest, indent=2, sort_keys=True) + "\n", force=force)

    message = f"[{name}] Plugin hook executed."
    plugin_template = dedent(
        f'''"""Auto-generated plugin for Sims4 Mod Sorter."""
from __future__ import annotations

from typing import Dict, List

from scanner import FileItem


def register(api) -> None:
    """Entry point for the plugin."""

    def _on_post_scan(items: List[FileItem], context: Dict[str, object], _api) -> None:
        _ = items, context  # unused placeholders for the default template
        api.log("{message}")

    api.register_post_scan_hook(_on_post_scan)
'''
    ).strip() + "\n"
    _write_file(plugin_dir / "plugin.py", plugin_template, force=force)

    return plugin_dir


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name", help="Display name for the plugin")
    parser.add_argument("--folder", help="Folder name for the plugin (defaults to a sanitised name)")
    parser.add_argument("--description", help="Plugin description for the manifest")
    parser.add_argument("--version", help="Initial plugin version", default="0.1.0")
    parser.add_argument("--author", help="Author name for the manifest")
    parser.add_argument(
        "--output-dir",
        help="Destination directory for the plugin (defaults to user_plugins/)",
        default=str(DEFAULT_OUTPUT_DIR),
    )
    parser.add_argument("--force", action="store_true", help="Overwrite existing files if they exist")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        plugin_dir = create_plugin(**vars(args))
    except FileExistsError as error:
        parser.error(str(error))
        return 2
    print(f"Plugin created at {plugin_dir}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
