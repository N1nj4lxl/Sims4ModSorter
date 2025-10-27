from __future__ import annotations

import json
from pathlib import Path

from plugin_maker import DEFAULT_OUTPUT_DIR, _sanitize, create_plugin


def test_sanitize_generates_safe_names() -> None:
    assert _sanitize("My Plugin!!") == "My-Plugin"
    assert _sanitize("   ###   ") == "plugin"


def test_create_plugin_scaffolds_files(tmp_path: Path) -> None:
    target_dir = tmp_path / "custom_plugins"
    plugin_dir = create_plugin(
        name="Fancy Plugin",
        description="Does fancy things",
        version="2.0.0",
        author="Mod Squad",
        output_dir=target_dir,
    )

    manifest_path = plugin_dir / "plugin.json"
    module_path = plugin_dir / "plugin.py"
    assert manifest_path.exists()
    assert module_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["name"] == "Fancy Plugin"
    assert manifest["author"] == "Mod Squad"
    assert "entry" in manifest

    source = module_path.read_text(encoding="utf-8")
    assert "api.log" in source
    assert "register" in source


def test_default_output_dir_points_to_user_plugins() -> None:
    assert DEFAULT_OUTPUT_DIR.name == "user_plugins"
