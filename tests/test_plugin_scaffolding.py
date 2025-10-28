from __future__ import annotations

import json
from pathlib import Path

import io

import pytest

import plugin_manager
from plugin_manager import DEFAULT_OUTPUT_DIR, create_plugin, sanitize_name


def test_sanitize_generates_safe_names() -> None:
    assert sanitize_name("My Plugin!!") == "My-Plugin"
    assert sanitize_name("   ###   ") == "plugin"


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


def test_resolve_arguments_requires_name_without_tty(monkeypatch) -> None:
    parser = plugin_manager._build_parser()
    namespace = parser.parse_args(["--scaffold"])

    dummy_stdin = io.StringIO()
    monkeypatch.setattr(dummy_stdin, "isatty", lambda: False)
    monkeypatch.setattr(plugin_manager.sys, "stdin", dummy_stdin)

    with pytest.raises(SystemExit):
        plugin_manager._resolve_arguments(namespace, parser)


def test_resolve_arguments_prompts_for_missing_fields(monkeypatch) -> None:
    parser = plugin_manager._build_parser()
    namespace = parser.parse_args(["--scaffold"])

    class DummyStdin:
        @staticmethod
        def isatty() -> bool:
            return True

    monkeypatch.setattr(plugin_manager.sys, "stdin", DummyStdin())

    defaults = {
        "Plugin name: ": "Ocean Breeze",
        "Plugin folder [Ocean-Breeze]: ": "__default__",
        "Description [Custom Sims4 Mod Sorter plugin.]: ": "Adds sea sounds",
        "Version [0.1.0]: ": "1.2.3",
        "Author [Unknown Author]: ": "Luna",
        f"Output directory [{DEFAULT_OUTPUT_DIR}]: ": "__default__",
    }

    def fake_prompt(message: str, *, default: str | None = None) -> str:
        value = defaults.get(message, "")
        if value == "__default__":
            return default or ""
        return value

    monkeypatch.setattr(plugin_manager, "_prompt", fake_prompt)

    resolved = plugin_manager._resolve_arguments(namespace, parser)

    assert resolved["name"] == "Ocean Breeze"
    assert resolved["folder"] == "Ocean-Breeze"
    assert resolved["description"] == "Adds sea sounds"
    assert resolved["version"] == "1.2.3"
    assert resolved["author"] == "Luna"
    assert resolved["output_dir"] == str(DEFAULT_OUTPUT_DIR)
