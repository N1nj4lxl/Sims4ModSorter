"""Command line helper to scaffold Sims4 Mod Sorter plugins."""
from __future__ import annotations

import argparse
import json
import re
import sys
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


def _prompt(message: str, *, default: str | None = None) -> str:
    try:
        response = input(message)
    except (EOFError, KeyboardInterrupt):
        raise SystemExit(1)
    response = response.strip()
    if not response and default is not None:
        return default
    return response


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name", nargs="?", help="Display name for the plugin")
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


def _resolve_arguments(namespace: argparse.Namespace, parser: argparse.ArgumentParser) -> dict[str, object]:
    values = vars(namespace).copy()
    name = values.get("name")
    if name:
        return values

    if not sys.stdin.isatty():
        parser.error("Plugin name is required")

    print("Sims4 Mod Sorter Plugin Maker")
    print("==============================")

    name = _prompt("Plugin name: ")
    if not name:
        parser.error("Plugin name is required")
    values["name"] = name

    folder_default = _sanitize(name)
    folder_prompt = f"Plugin folder [{folder_default}]: "
    values["folder"] = _prompt(folder_prompt, default=folder_default)

    description_default = values.get("description") or "Custom Sims4 Mod Sorter plugin."
    values["description"] = _prompt(
        f"Description [{description_default}]: ", default=description_default
    )

    version_default = values.get("version") or "0.1.0"
    values["version"] = _prompt(f"Version [{version_default}]: ", default=version_default)

    author_default = values.get("author") or "Unknown Author"
    values["author"] = _prompt(f"Author [{author_default}]: ", default=author_default)

    output_default = values.get("output_dir") or str(DEFAULT_OUTPUT_DIR)
    values["output_dir"] = _prompt(
        f"Output directory [{output_default}]: ", default=output_default
    )

    return values


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    resolved_args = _resolve_arguments(args, parser)
    try:
        plugin_dir = create_plugin(**resolved_args)
    except FileExistsError as error:
        parser.error(str(error))
        return 2
    print(f"Plugin created at {plugin_dir}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
