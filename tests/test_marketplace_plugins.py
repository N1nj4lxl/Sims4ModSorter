from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

import pytest

from marketplace_plugins.duplicate_spotlight import plugin as duplicate_plugin
from marketplace_plugins.fresh_mod_tracker import plugin as fresh_plugin
from marketplace_plugins.loadout_guard import plugin as loadout_plugin
from scanner import DUPLICATE_EXTRA_KEY, FileItem, UNKNOWN_DEFAULT_FOLDER


class DummyAPI:
    def __init__(self) -> None:
        self.logged: List[Dict[str, str]] = []
        self.hooks = []

    def log(self, message: str, level: str = "info") -> None:
        self.logged.append({"message": message, "level": level})

    def register_post_scan_hook(self, callback):
        self.hooks.append(callback)


@pytest.fixture
def make_file(tmp_path: Path):
    def _factory(name: str, *, mtime: datetime | None = None) -> Path:
        path = tmp_path / name
        path.write_text("data", encoding="utf-8")
        if mtime:
            os.utime(path, times=(mtime.timestamp(), mtime.timestamp()))
        return path

    return _factory


def _make_item(path: Path, **overrides) -> FileItem:
    defaults = dict(
        path=path,
        name=path.name,
        ext=path.suffix,
        size_mb=1.0,
        relpath=str(path.name),
        guess_type="Script Mod",
        confidence=1.0,
        notes="",
        include=True,
        target_folder=UNKNOWN_DEFAULT_FOLDER,
        bundle="",
        meta_tags="",
        dependency_status="",
        dependency_detail="",
        extras={},
        tooltips={},
        disabled=False,
        original_ext=path.suffix,
    )
    defaults.update(overrides)
    return FileItem(**defaults)


# ---------------------------------------------------------------------------
# Duplicate Spotlight
# ---------------------------------------------------------------------------


def test_duplicate_grouping_and_summary(tmp_path: Path) -> None:
    primary_path = tmp_path / "primary.package"
    duplicate_a_path = tmp_path / "dupe_a.package"
    duplicate_b_path = tmp_path / "dupe_b.package"
    for path in (primary_path, duplicate_a_path, duplicate_b_path):
        path.write_text("data", encoding="utf-8")

    duplicate_items = [
        _make_item(
            duplicate_a_path,
            relpath="Mods/dupe_a.package",
            extras={DUPLICATE_EXTRA_KEY: "⚠"},
            tooltips={DUPLICATE_EXTRA_KEY: "Duplicate of Mods/primary.package"},
        ),
        _make_item(
            duplicate_b_path,
            relpath="Mods/dupe_b.package",
            extras={DUPLICATE_EXTRA_KEY: "⚠"},
            tooltips={DUPLICATE_EXTRA_KEY: "Duplicate of Mods/primary.package"},
        ),
    ]
    groups = duplicate_plugin._group_duplicates(duplicate_items)
    assert "Mods/primary.package" in groups
    assert len(groups["Mods/primary.package"]) == 2

    summary = duplicate_plugin._build_summary(groups)
    assert "2 potential duplicate mod(s)" in summary
    assert "Mods/primary.package" in summary


# ---------------------------------------------------------------------------
# Fresh Mod Tracker
# ---------------------------------------------------------------------------


def test_recent_files_sorted(make_file) -> None:
    now = datetime.now(tz=timezone.utc)
    recent_path = make_file("recent.package", mtime=now)
    older_path = make_file("older.package", mtime=now - timedelta(days=1))
    stale_path = make_file("stale.package", mtime=now - timedelta(days=30))

    items = [
        _make_item(recent_path),
        _make_item(older_path),
        _make_item(stale_path),
    ]
    records = fresh_plugin._recent_files(items)
    assert [record[0].name for record in records] == ["recent.package", "older.package"]

    message = fresh_plugin._format_recent_summary(records)
    assert "2 mod(s) updated" in message
    assert "recent.package" in message


def test_recent_summary_when_empty() -> None:
    message = fresh_plugin._format_recent_summary([])
    assert message == "[Fresh Mod Tracker] No recently updated mods detected."


# ---------------------------------------------------------------------------
# Loadout Guard
# ---------------------------------------------------------------------------


def test_loadout_stats_and_messages(tmp_path: Path) -> None:
    included_path = tmp_path / "included.package"
    excluded_path = tmp_path / "excluded.package"
    disabled_path = tmp_path / "disabled.package"
    for path in (included_path, excluded_path, disabled_path):
        path.write_text("data", encoding="utf-8")

    items = [
        _make_item(included_path, include=True, disabled=False),
        _make_item(excluded_path, include=False, disabled=False),
        _make_item(disabled_path, include=True, disabled=True),
    ]
    stats = loadout_plugin._loadout_stats(items)
    assert stats["total"] == 3
    assert stats["included"] == 1
    assert stats["excluded"] == 2
    assert stats["disabled"] == 2

    payload = loadout_plugin._format_message(stats)
    assert payload["level"] == "warn"
    assert "Consider updating" in payload["message"]
    assert "disabled" in payload["message"]


def test_loadout_registers_hook() -> None:
    api = DummyAPI()
    loadout_plugin.register(api)
    assert api.hooks, "register should attach a post-scan hook"
