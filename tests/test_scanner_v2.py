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
    assert item.tooltips.get("reason") == "name:script"
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
    assert item.guess_type != "Disabled"
    assert item.include is True
    assert item.disabled is False
    assert item.target_folder != scanner.DEFAULT_FOLDER_MAP["Disabled"].rstrip("/") + "/"


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


def test_script_package_pair_linking(tmp_path: Path) -> None:
    script_path = tmp_path / "UI_Cheats_Extension.ts4script"
    package_path = tmp_path / "UI_Cheats_Extension.package"
    script_path.write_bytes(b"zipdata")
    package_path.write_bytes(b"\x00" * 4096)
    result = scanner.scan_folder(tmp_path, recurse=False)
    assert not result.errors
    types = {item.name: item for item in result.items + result.disabled_items}
    script_item = types[script_path.name]
    package_item = types[package_path.name]
    assert script_item.guess_type == "Script Mod"
    assert script_item.tooltips.get("reason") == "name:script"
    assert package_item.guess_type == "Script Mod"
    assert package_item.tooltips.get("reason") == "Linked to script mod: UI_Cheats_Extension"
    assert package_item.notes == "link:script-pair"


def test_scan_folder_metrics(tmp_path: Path) -> None:
    (tmp_path / "Hair_Long_Wave.package").write_bytes(b"\x00" * 2048)
    result = scanner.scan_folder(tmp_path, recurse=False)
    assert result.metrics is not None
    assert result.metrics.total_files == 1
    assert result.metrics.category_counts.get("CAS", 0) == 1
