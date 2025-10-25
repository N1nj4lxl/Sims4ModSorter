from __future__ import annotations

from pathlib import Path
from typing import Dict

import pytest

import scanner


@pytest.fixture(autouse=True)
def _reset_routing(monkeypatch: pytest.MonkeyPatch) -> None:
    # Ensure tests start with clean routing map without caching surprises
    monkeypatch.setattr(scanner, "DEFAULT_FOLDER_MAP", scanner.load_routing())


def _make_dummy(path: Path) -> Path:
    path.write_bytes(b"x" * 2048)
    return path


def _classify(
    path: Path,
    monkeypatch: pytest.MonkeyPatch,
    manifest: Dict[int, int] | None,
) -> scanner.FileItem:
    if manifest is not None:
        monkeypatch.setattr(scanner, "read_package_manifest", lambda *_args, **_kwargs: manifest)
    rules = scanner.load_rules()
    budgets = scanner.load_budgets()
    routing = scanner.load_routing()
    return scanner.scan_file(path, rules, routing, budgets)


def test_ts4script_is_script(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "UI_Cheats_Extension.ts4script"
    path.write_bytes(b"zipdata")
    item = _classify(path, monkeypatch, manifest=None)
    assert item.guess_type == "Script Mod"
    assert item.tooltips.get("reason") == "script:ts4script"
    assert item.target_folder == scanner.DEFAULT_FOLDER_MAP["Script Mod"].rstrip("/") + "/"


def test_adult_strong_keyword(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _make_dummy(tmp_path / "WickedWhims_Example.package")
    item = _classify(path, monkeypatch, manifest={})
    assert item.guess_type == "Adult"
    assert item.tooltips.get("reason", "").startswith("adult:strong")


def test_buildbuy_from_header(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _make_dummy(tmp_path / "Sofa_Modern.package")
    item = _classify(path, monkeypatch, manifest={0x319E4F1D: 1})
    assert item.guess_type == "BuildBuy"
    assert item.tooltips.get("reason") in {"header:OBJD", "name:buildbuy"}
    assert item.target_folder == scanner.DEFAULT_FOLDER_MAP["BuildBuy"].rstrip("/") + "/"


def test_cas_from_header(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _make_dummy(tmp_path / "Hair_Long_Wave.package")
    item = _classify(path, monkeypatch, manifest={0x034AEECB: 1})
    assert item.guess_type == "CAS"
    assert item.tooltips.get("reason") in {"header:CASP", "name:cas"}


def test_pose_detection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _make_dummy(tmp_path / "A2O_Posepack.package")
    item = _classify(path, monkeypatch, manifest={0x6B20C4F3: 1})
    assert item.guess_type == "Pose or Animation"
    assert item.tooltips.get("reason") in {"header:CLIP", "name:pose"}


def test_disabled_suffix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _make_dummy(tmp_path / "SomeMod.package.off")
    item = _classify(path, monkeypatch, manifest={})
    assert item.guess_type == "Disabled"
    assert item.include is False
    assert item.tooltips.get("reason") == "disabled:ext"
    assert item.target_folder == scanner.DEFAULT_FOLDER_MAP["Disabled"].rstrip("/") + "/"


def test_header_nondeterministic(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _make_dummy(tmp_path / "Only_STBL.package")
    item = _classify(path, monkeypatch, manifest={0x220557DA: 2})
    assert item.guess_type == "Unknown"
    assert item.tooltips.get("reason") == "header:nondet"
    assert item.target_folder == scanner.DEFAULT_FOLDER_MAP["Unknown"].rstrip("/") + "/"


def test_mixed_decision(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _make_dummy(tmp_path / "CAS_and_OBJD_Mix.package")
    item = _classify(path, monkeypatch, manifest={0x034AEECB: 1, 0x319E4F1D: 1})
    assert item.guess_type == "Mixed"
    assert item.tooltips.get("reason") == "header:mixed"
    assert item.target_folder == scanner.DEFAULT_FOLDER_MAP["Mixed"].rstrip("/") + "/"


def test_archives_routed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rar_path = tmp_path / "Archive.rar"
    seven_path = tmp_path / "Archive.7z"
    rar_path.write_bytes(b"rar")
    seven_path.write_bytes(b"7z")
    rules = scanner.load_rules()
    budgets = scanner.load_budgets()
    routing = scanner.load_routing()
    rar_item = scanner.scan_file(rar_path, rules, routing, budgets)
    seven_item = scanner.scan_file(seven_path, rules, routing, budgets)
    assert rar_item.guess_type == "Archive"
    assert seven_item.guess_type == "Archive"
    assert rar_item.target_folder == scanner.DEFAULT_FOLDER_MAP["Archive"].rstrip("/") + "/"
    assert seven_item.target_folder == scanner.DEFAULT_FOLDER_MAP["Archive"].rstrip("/") + "/"
