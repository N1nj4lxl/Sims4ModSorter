"""CLI utility to manage Sims4ModSorter user mods."""
from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import zipfile
from pathlib import Path
from typing import Dict, Optional

USER_MODS_DIR = Path(__file__).resolve().parent / "user_mods"


def sanitize_name(name: str) -> str:
    safe = re.sub(r"[^0-9A-Za-z_-]+", "-", name.strip())
    safe = safe.strip("-_")
    return safe or "mod"


def ensure_mods_dir() -> None:
    USER_MODS_DIR.mkdir(parents=True, exist_ok=True)


def load_manifest(mod_dir: Path) -> Dict[str, object]:
    manifest_path = mod_dir / "mod.json"
    if manifest_path.exists():
        try:
            with open(manifest_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    return {
        "name": mod_dir.name,
        "entry": "mod.py",
        "enabled": True,
        "callable": "register",
    }


def write_manifest(mod_dir: Path, manifest: Dict[str, object]) -> None:
    manifest_path = mod_dir / "mod.json"
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)


def copy_directory(src: Path, dest: Path) -> None:
    for item in src.iterdir():
        target = dest / item.name
        if item.is_dir():
            shutil.copytree(item, target, dirs_exist_ok=True)
        else:
            shutil.copy2(item, target)


def import_mod(args: argparse.Namespace) -> int:
    source = Path(args.source).expanduser().resolve()
    if not source.exists():
        print(f"Source not found: {source}", file=sys.stderr)
        return 1

    ensure_mods_dir()
    mod_name = args.name or source.stem
    folder_name = sanitize_name(mod_name)
    dest_dir = USER_MODS_DIR / folder_name

    if dest_dir.exists():
        if args.overwrite:
            shutil.rmtree(dest_dir)
        else:
            print(f"Mod '{folder_name}' already exists. Use --overwrite to replace it.", file=sys.stderr)
            return 1

    dest_dir.mkdir(parents=True, exist_ok=True)

    entry = args.entry
    callable_name = args.callable

    if source.is_file() and zipfile.is_zipfile(source):
        with zipfile.ZipFile(source, "r") as zf:
            zf.extractall(dest_dir)
        if not entry:
            potential = dest_dir / "mod.py"
            if potential.exists():
                entry = "mod.py"
    elif source.is_file():
        entry = entry or source.name
        target_path = dest_dir / entry
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target_path)
    elif source.is_dir():
        copy_directory(source, dest_dir)
        if not entry:
            potential = dest_dir / "mod.py"
            if potential.exists():
                entry = "mod.py"
    else:
        print(f"Unsupported source type: {source}", file=sys.stderr)
        shutil.rmtree(dest_dir, ignore_errors=True)
        return 1

    entry = entry or "mod.py"
    if not (dest_dir / entry).exists():
        print(f"Entry file '{entry}' not found inside installed mod. Adjust --entry.", file=sys.stderr)
        shutil.rmtree(dest_dir, ignore_errors=True)
        return 1

    manifest = {
        "name": mod_name,
        "entry": entry,
        "enabled": not args.disable,
        "callable": callable_name,
    }
    write_manifest(dest_dir, manifest)
    status = "disabled" if args.disable else "enabled"
    print(f"Imported mod '{mod_name}' into {dest_dir.name} ({status}).")
    return 0


def list_mods(_args: argparse.Namespace) -> int:
    ensure_mods_dir()
    entries = [d for d in USER_MODS_DIR.iterdir() if d.is_dir()]
    if not entries:
        print("No mods installed.")
        return 0

    print(f"Mods in {USER_MODS_DIR}:")
    for mod_dir in sorted(entries, key=lambda d: d.name.lower()):
        manifest = load_manifest(mod_dir)
        status = "enabled" if manifest.get("enabled", True) else "disabled"
        entry = manifest.get("entry", "mod.py")
        print(f"- {manifest.get('name', mod_dir.name)} [{status}] -> {mod_dir.name}/{entry}")
    return 0


def find_mod(identifier: str) -> Optional[Path]:
    ensure_mods_dir()
    candidate = USER_MODS_DIR / sanitize_name(identifier)
    if candidate.exists():
        return candidate
    identifier_lower = identifier.lower()
    for mod_dir in USER_MODS_DIR.iterdir():
        if not mod_dir.is_dir():
            continue
        manifest = load_manifest(mod_dir)
        name = str(manifest.get("name", ""))
        if name.lower() == identifier_lower:
            return mod_dir
    return None


def set_enabled(identifier: str, enabled: bool) -> int:
    mod_dir = find_mod(identifier)
    if not mod_dir:
        print(f"Mod '{identifier}' not found.", file=sys.stderr)
        return 1
    manifest = load_manifest(mod_dir)
    manifest["enabled"] = enabled
    write_manifest(mod_dir, manifest)
    state = "enabled" if enabled else "disabled"
    print(f"Mod '{manifest.get('name', mod_dir.name)}' is now {state}.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage Sims4ModSorter user mods.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    imp = subparsers.add_parser("import", help="Import a mod into the user_mods directory")
    imp.add_argument("source", help="Path to a .py file, directory, or .zip archive")
    imp.add_argument("--name", help="Display name for the mod")
    imp.add_argument("--entry", help="Relative entry file inside the mod", default=None)
    imp.add_argument("--callable", help="Registration callable name", default="register")
    imp.add_argument("--disable", action="store_true", help="Import the mod but leave it disabled")
    imp.add_argument("--overwrite", action="store_true", help="Replace an existing mod with the same name")
    imp.set_defaults(func=import_mod)

    ls = subparsers.add_parser("list", help="List installed mods")
    ls.set_defaults(func=list_mods)

    enable = subparsers.add_parser("enable", help="Enable a mod")
    enable.add_argument("identifier", help="Folder name or mod display name")
    enable.set_defaults(func=lambda args: set_enabled(args.identifier, True))

    disable = subparsers.add_parser("disable", help="Disable a mod")
    disable.add_argument("identifier", help="Folder name or mod display name")
    disable.set_defaults(func=lambda args: set_enabled(args.identifier, False))

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
