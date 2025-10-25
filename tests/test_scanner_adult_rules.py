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


def _manifest_with(types: dict[int, int]) -> scanner.PackageManifest:
    return scanner.PackageManifest(type_counts=types)


def test_ts4script_remains_script(tmp_path: Path) -> None:
    _clear_scan_cache()
    script = tmp_path / "wickedwhims_module.ts4script"
    with zipfile.ZipFile(script, "w") as archive:
        archive.writestr("wickedwhims/__init__.pyc", b"pyc")

    result = scanner.scan_folder(tmp_path)
    assert not result.errors
    assert len(result.items) == 1
    item = result.items[0]
    assert item.guess_type.split(":", 1)[0] == "Script Mod"
    extras = item.extras if isinstance(item.extras, dict) else {}
    assert extras.get("is_adult") != "true"


def test_objd_package_buildbuy_not_adult(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_scan_cache()
    package = tmp_path / "chair.package"
    package.write_bytes(b"chair")

    monkeypatch.setattr(
        scanner,
        "read_sims4pkg_manifest",
        lambda path, limit=64: _manifest_with({0x319E4F1D: 1}),
    )
    monkeypatch.setattr(scanner.ContentPeek, "sample", staticmethod(lambda *args, **kwargs: None))

    result = scanner.scan_folder(tmp_path)
    assert not result.errors
    assert len(result.items) == 1
    item = result.items[0]
    assert item.guess_type.split(":", 1)[0] == "BuildBuy"
    extras = item.extras if isinstance(item.extras, dict) else {}
    assert extras.get("is_adult") != "true"
    assert item.target_folder.startswith(scanner.DEFAULT_FOLDER_MAP["BuildBuy"])


def test_clip_with_adult_hit_flags_animation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _clear_scan_cache()
    package = tmp_path / "pose.package"
    package.write_bytes(b"pose")

    monkeypatch.setattr(
        scanner,
        "read_sims4pkg_manifest",
        lambda path, limit=64: _manifest_with({0x6B20C4F3: 1}),
    )

    def _peek(*_args, **_kwargs):
        return scanner.PeekSignal(hits={"adult_strong": {"wickedwhims"}})

    monkeypatch.setattr(scanner.ContentPeek, "sample", staticmethod(_peek))

    result = scanner.scan_folder(tmp_path)
    assert not result.errors
    assert len(result.items) == 1
    item = result.items[0]
    assert item.guess_type.split(":", 1)[0] == "Pose or Animation"
    extras = item.extras if isinstance(item.extras, dict) else {}
    assert extras.get("is_adult") == "true"
    assert item.target_folder.startswith(scanner.DEFAULT_FOLDER_MAP["Adult"])


def test_xml_package_without_adult_strings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_scan_cache()
    package = tmp_path / "tuning.package"
    package.write_bytes(b"tuning")

    monkeypatch.setattr(
        scanner,
        "read_sims4pkg_manifest",
        lambda path, limit=64: _manifest_with({0x545AC67A: 1}),
    )
    monkeypatch.setattr(scanner.ContentPeek, "sample", staticmethod(lambda *args, **kwargs: None))

    result = scanner.scan_folder(tmp_path)
    assert not result.errors
    assert len(result.items) == 1
    item = result.items[0]
    assert item.guess_type.split(":", 1)[0] == "Tuning"
    extras = item.extras if isinstance(item.extras, dict) else {}
    assert extras.get("is_adult") != "true"
    assert item.target_folder.startswith(scanner.DEFAULT_FOLDER_MAP["Tuning"])

def test_dependency_marks_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_scan_cache()
    rule = scanner.DependencyRule(
        key="mod-a",
        requires=(("mod-b", "Mod B"),),
        aliases=tuple(),
        label="Mod A",
    )
    monkeypatch.setattr(scanner, "_parse_dependency_rules", lambda raw: (rule,))
    monkeypatch.setattr(
        scanner,
        "read_sims4pkg_manifest",
        lambda path, limit=64: _manifest_with({0x319E4F1D: 1}),
    )
    monkeypatch.setattr(scanner.ContentPeek, "sample", staticmethod(lambda *args, **kwargs: None))

    (tmp_path / "Mod_A.package").write_bytes(b"a")

    result = scanner.scan_folder(tmp_path)
    assert not result.errors
    assert len(result.items) == 1
    item = result.items[0]
    assert item.dependency_status == "missing"
    assert "Mod B" in (item.dependency_detail or "")


def test_dependency_marks_satisfied(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_scan_cache()
    rule = scanner.DependencyRule(
        key="mod-a",
        requires=(("mod-b", "Mod B"),),
        aliases=tuple(),
        label="Mod A",
    )
    monkeypatch.setattr(scanner, "_parse_dependency_rules", lambda raw: (rule,))
    monkeypatch.setattr(
        scanner,
        "read_sims4pkg_manifest",
        lambda path, limit=64: _manifest_with({0x319E4F1D: 1}),
    )
    monkeypatch.setattr(scanner.ContentPeek, "sample", staticmethod(lambda *args, **kwargs: None))

    (tmp_path / "Mod_A.package").write_bytes(b"a")
    (tmp_path / "Mod_B.package").write_bytes(b"b")

    result = scanner.scan_folder(tmp_path)
    assert not result.errors
    statuses = {item.name: item.dependency_status for item in result.items}
    assert statuses["Mod_A.package"] == "ok"
    details = {item.name: item.dependency_detail for item in result.items}
    assert "Requires: Mod B" in (details["Mod_A.package"] or "")
