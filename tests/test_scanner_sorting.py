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
    base = item.guess_type.split(":", 1)[0]
    expected_prefix = scanner.DEFAULT_FOLDER_MAP.get(base, scanner.DEFAULT_FOLDER_MAP["Unknown"])
    assert item.target_folder.startswith(expected_prefix)
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

    result = scanner.scan_folder(tmp_path)

    assert not result.errors
    targets = {item.name: item.target_folder for item in result.items}

    assert targets["functional.ts4script"] == scanner.DEFAULT_FOLDER_MAP["Script Mod"]
    assert targets["glam_hair.package"] == scanner.DEFAULT_FOLDER_MAP["CAS"]
    assert targets["modern_sofa.package"] == scanner.DEFAULT_FOLDER_MAP["BuildBuy"]
    assert "Unsorted" not in targets.values()


def test_name_heuristics_scoring_configured(tmp_path: Path) -> None:
    ctx = scanner._build_context()
    try:
        signal = scanner.NameHeuristics.guess(tmp_path / "fancy_hair.package", ctx.rules)
        assert signal.category == "CAS"
        assert signal.score_map.get("CAS", 0) >= 3
        assert any(sig.startswith("name:") for sig in signal.signals)
    finally:
        ctx.pool.shutdown(wait=True)
        ctx.cache.close()


@pytest.mark.parametrize(
    "type_map, expected_category, decisive",
    [
        ({0x034AEECB: 1}, "CAS", True),
        ({0x319E4F1D: 1}, "BuildBuy", True),
        ({0x6B20C4F3: 1}, "Pose or Animation", False),
        ({0x03B33DDF: 1}, "Tuning", False),
        ({0x220557DA: 2}, "Tuning", False),
    ],
)
def test_dbpf_probe_scoring(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, type_map: dict[int, int], expected_category: str, decisive: bool
) -> None:
    dummy = tmp_path / "sample.package"
    dummy.write_text("", encoding="utf-8")

    def fake_scan(_: Path, limit: int = 10) -> dict[int, int]:  # noqa: ANN001
        return type_map

    monkeypatch.setattr(scanner, "dbpf_scan_types", fake_scan)
    signal = scanner.DbpfProbe.inspect(dummy)
    assert signal.category == expected_category
    assert signal.decisive is decisive
    assert signal.score >= (5 if decisive else 2)


def test_mixed_folder_keeps_needs_review_low(tmp_path: Path) -> None:
    _clear_scan_cache()
    files = {
        "stylish_hair.package": "hair swatch",
        "modern_sofa.package": "sofa recolor",
        "dance_pose.package": "pose animation",
        "gameplay_tuning.package": "tuning data",
    }
    for name, content in files.items():
        (tmp_path / name).write_text(content, encoding="utf-8")
    script_path = tmp_path / "functional.ts4script"
    with zipfile.ZipFile(script_path, "w") as archive:
        archive.writestr("module/__init__.pyc", b"pyc")

    result = scanner.scan_folder(tmp_path)
    assert not result.errors
    total = len(result.items)
    assert total == len(files) + 1
    needs_review = sum(1 for item in result.items if "NeedsReview" in item.target_folder)
    unknown = sum(1 for item in result.items if item.guess_type.startswith("Unknown"))
    assert needs_review / total < 0.2
    assert unknown / total < 0.1


def test_disabled_package_routed_to_disabled(tmp_path: Path) -> None:
    _clear_scan_cache()
    disabled = tmp_path / "broken.package.off"
    disabled.write_text("chair", encoding="utf-8")

    result = scanner.scan_folder(tmp_path)

    assert not result.errors
    assert not result.items
    assert len(result.disabled_items) == 1
    entry = result.disabled_items[0]
    assert entry.include is False
    assert entry.target_folder.endswith("Disabled/")
