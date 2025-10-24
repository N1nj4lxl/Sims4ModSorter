from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

import scanner


def _clear_scan_cache() -> None:
    base = Path(scanner.__file__).resolve().parent
    for suffix in ("", "-wal", "-shm"):
        cache_path = base / f"scan_cache.db{suffix}"
        if cache_path.exists():
            cache_path.unlink()


@pytest.mark.parametrize(
    "filename",
    [
        "glam_hair.package",
        "modern_sofa.package",
        "nude_pose.package",
        "functional.ts4script",
    ],
)
def test_scan_folder_assigns_known_targets(tmp_path: Path, filename: str) -> None:
    _clear_scan_cache()
    file_path = tmp_path / filename
    if file_path.suffix == ".ts4script":
        with zipfile.ZipFile(file_path, "w") as archive:
            archive.writestr("module/__init__.pyc", b"pyc")
    else:
        file_path.write_text("placeholder content", encoding="utf-8")

    result = scanner.scan_folder(tmp_path)

    assert not result.errors
    assert len(result.items) == 1
    item = result.items[0]
    assert item.target_folder in scanner.DEFAULT_FOLDER_MAP.values()
    assert "Unsorted" not in item.target_folder


def test_scan_folder_routes_multiple_categories(tmp_path: Path) -> None:
    _clear_scan_cache()
    script_path = tmp_path / "functional.ts4script"
    with zipfile.ZipFile(script_path, "w") as archive:
        archive.writestr("module/__init__.pyc", b"pyc")

    cas_path = tmp_path / "glam_hair.package"
    cas_path.write_text("hair swatch", encoding="utf-8")

    build_path = tmp_path / "modern_sofa.package"
    build_path.write_text("sofa recolor", encoding="utf-8")

    adult_path = tmp_path / "nude_pose.package"
    adult_path.write_text("nude animation", encoding="utf-8")

    result = scanner.scan_folder(tmp_path)

    assert not result.errors
    targets = {item.name: item.target_folder for item in result.items}

    assert targets["functional.ts4script"] == scanner.DEFAULT_FOLDER_MAP["Script Mod"]
    assert targets["glam_hair.package"] == scanner.DEFAULT_FOLDER_MAP["CAS"]
    assert targets["modern_sofa.package"] == scanner.DEFAULT_FOLDER_MAP["BuildBuy"]
    assert targets["nude_pose.package"] == scanner.DEFAULT_FOLDER_MAP["Adult"]
    assert "Unsorted" not in targets.values()
