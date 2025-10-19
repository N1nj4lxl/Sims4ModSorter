"""CLI utility to manage Sims4ModSorter user plugins."""
from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import zipfile
from pathlib import Path
from typing import Dict, Optional

USER_PLUGINS_DIR = Path(__file__).resolve().parent / "user_plugins"


def sanitize_name(name: str) -> str:
    safe = re.sub(r"[^0-9A-Za-z_-]+", "-", name.strip())
    safe = safe.strip("-_")
    return safe or "plugin"


def ensure_plugins_dir() -> None:
    USER_PLUGINS_DIR.mkdir(parents=True, exist_ok=True)


def load_manifest(plugin_dir: Path) -> Dict[str, object]:
    manifest_path = plugin_dir / "plugin.json"
    if manifest_path.exists():
        try:
            with open(manifest_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    return {
        "name": plugin_dir.name,
        "entry": "plugin.py",
        "enabled": True,
        "callable": "register",
    }


def write_manifest(plugin_dir: Path, manifest: Dict[str, object]) -> None:
    manifest_path = plugin_dir / "plugin.json"
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)


def copy_directory(src: Path, dest: Path) -> None:
    for item in src.iterdir():
        target = dest / item.name
        if item.is_dir():
            shutil.copytree(item, target, dirs_exist_ok=True)
        else:
            shutil.copy2(item, target)


def import_plugin(args: argparse.Namespace) -> int:
    source = Path(args.source).expanduser().resolve()
    if not source.exists():
        print(f"Source not found: {source}", file=sys.stderr)
        return 1

    ensure_plugins_dir()
    plugin_name = args.name or source.stem
    folder_name = sanitize_name(plugin_name)
    dest_dir = USER_PLUGINS_DIR / folder_name

    if dest_dir.exists():
        if args.overwrite:
            shutil.rmtree(dest_dir)
        else:
            print(f"Plugin '{folder_name}' already exists. Use --overwrite to replace it.", file=sys.stderr)
            return 1

    dest_dir.mkdir(parents=True, exist_ok=True)

    entry = args.entry
    callable_name = args.callable

    if source.is_file() and zipfile.is_zipfile(source):
        with zipfile.ZipFile(source, "r") as zf:
            zf.extractall(dest_dir)
        if not entry:
            potential = dest_dir / "plugin.py"
            if potential.exists():
                entry = "plugin.py"
    elif source.is_file():
        entry = entry or source.name
        target_path = dest_dir / entry
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target_path)
    elif source.is_dir():
        copy_directory(source, dest_dir)
        if not entry:
            potential = dest_dir / "plugin.py"
            if potential.exists():
                entry = "plugin.py"
    else:
        print(f"Unsupported source type: {source}", file=sys.stderr)
        shutil.rmtree(dest_dir, ignore_errors=True)
        return 1

    entry = entry or "plugin.py"
    if not (dest_dir / entry).exists():
        print(f"Entry file '{entry}' not found inside installed plugin. Adjust --entry.", file=sys.stderr)
        shutil.rmtree(dest_dir, ignore_errors=True)
        return 1

    manifest = {
        "name": plugin_name,
        "entry": entry,
        "enabled": not args.disable,
        "callable": callable_name,
    }
    write_manifest(dest_dir, manifest)
    status = "disabled" if args.disable else "enabled"
    print(f"Imported plugin '{plugin_name}' into {dest_dir.name} ({status}).")
    return 0


def list_plugins(_args: argparse.Namespace) -> int:
    ensure_plugins_dir()
    entries = [d for d in USER_PLUGINS_DIR.iterdir() if d.is_dir()]
    if not entries:
        print("No plugins installed.")
        return 0

    print(f"Plugins in {USER_PLUGINS_DIR}:")
    for plugin_dir in sorted(entries, key=lambda d: d.name.lower()):
        manifest = load_manifest(plugin_dir)
        status = "enabled" if manifest.get("enabled", True) else "disabled"
        entry = manifest.get("entry", "plugin.py")
        print(f"- {manifest.get('name', plugin_dir.name)} [{status}] -> {plugin_dir.name}/{entry}")
    return 0


def find_plugin(identifier: str) -> Optional[Path]:
    ensure_plugins_dir()
    candidate = USER_PLUGINS_DIR / sanitize_name(identifier)
    if candidate.exists():
        return candidate
    identifier_lower = identifier.lower()
    for plugin_dir in USER_PLUGINS_DIR.iterdir():
        if not plugin_dir.is_dir():
            continue
        manifest = load_manifest(plugin_dir)
        name = str(manifest.get("name", ""))
        if name.lower() == identifier_lower:
            return plugin_dir
    return None


def set_enabled(identifier: str, enabled: bool) -> int:
    plugin_dir = find_plugin(identifier)
    if not plugin_dir:
        print(f"Plugin '{identifier}' not found.", file=sys.stderr)
        return 1
    manifest = load_manifest(plugin_dir)
    manifest["enabled"] = enabled
    write_manifest(plugin_dir, manifest)
    state = "enabled" if enabled else "disabled"
    print(f"Plugin '{manifest.get('name', plugin_dir.name)}' is now {state}.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage Sims4ModSorter user plugins.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    imp = subparsers.add_parser("import", help="Import a plugin into the user_plugins directory")
    imp.add_argument("source", help="Path to a .py file, directory, or .zip archive")
    imp.add_argument("--name", help="Display name for the plugin")
    imp.add_argument("--entry", help="Relative entry file inside the plugin", default=None)
    imp.add_argument("--callable", help="Registration callable name", default="register")
    imp.add_argument("--disable", action="store_true", help="Import the plugin but leave it disabled")
    imp.add_argument("--overwrite", action="store_true", help="Replace an existing plugin with the same name")
    imp.set_defaults(func=import_plugin)

    ls = subparsers.add_parser("list", help="List installed plugins")
    ls.set_defaults(func=list_plugins)

    enable = subparsers.add_parser("enable", help="Enable a plugin")
    enable.add_argument("identifier", help="Folder name or plugin display name")
    enable.set_defaults(func=lambda args: set_enabled(args.identifier, True))

    disable = subparsers.add_parser("disable", help="Disable a plugin")
    disable.add_argument("identifier", help="Folder name or plugin display name")
    disable.set_defaults(func=lambda args: set_enabled(args.identifier, False))

    return parser


def interactive_main() -> int:
    ensure_plugins_dir()
    print("Sims4 Mod Sorter - Plugin Manager")
    print("Manage user plugins without needing command-line arguments.")
    while True:
        print("\nOptions:")
        print("  1) Import plugin")
        print("  2) List plugins")
        print("  3) Enable plugin")
        print("  4) Disable plugin")
        print("  5) Quit")
        try:
            choice = input("Select an option: ").strip()
        except EOFError:
            print()
            return 0
        if choice == "1":
            source = input("Path to .py/.zip/directory: ").strip()
            name = input("Display name (optional): ").strip() or None
            entry = input("Entry file (optional): ").strip() or None
            callable_name = input("Callable (default register): ").strip() or "register"
            overwrite = input("Overwrite existing? [y/N]: ").strip().lower() == "y"
            disable = input("Import disabled? [y/N]: ").strip().lower() == "y"
            args = argparse.Namespace(
                source=source,
                name=name,
                entry=entry,
                callable=callable_name,
                overwrite=overwrite,
                disable=disable,
            )
            import_plugin(args)
        elif choice == "2":
            list_plugins(argparse.Namespace())
        elif choice == "3":
            identifier = input("Folder name or display name: ").strip()
            if identifier:
                set_enabled(identifier, True)
        elif choice == "4":
            identifier = input("Folder name or display name: ").strip()
            if identifier:
                set_enabled(identifier, False)
        elif choice == "5" or choice.lower() in {"q", "quit", "exit"}:
            return 0
        else:
            print("Invalid selection. Choose 1-5.")


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        parser.print_help()
        print()
        try:
            return interactive_main()
        except KeyboardInterrupt:
            print()
            return 0
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
