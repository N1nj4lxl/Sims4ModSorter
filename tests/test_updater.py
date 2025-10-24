import sys
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import Sims4ModSorter  # type: ignore

Sims4ModSorterApp = Sims4ModSorter.Sims4ModSorterApp


def _make_app() -> Sims4ModSorterApp:
    app = object.__new__(Sims4ModSorterApp)
    app.log = lambda *args, **kwargs: None  # type: ignore[attr-defined]
    return app


def test_advanced_update_preserves_existing_files(tmp_path):
    app = _make_app()

    current_root = tmp_path / "current"
    current_root.mkdir()
    (current_root / "Sims4ModSorter.exe").write_text("old exe", encoding="utf-8")
    (current_root / "VERSION").write_text("1.0.0", encoding="utf-8")
    (current_root / "keep.txt").write_text("keep", encoding="utf-8")

    source = tmp_path / "update"
    source.mkdir()
    (source / "Sims4ModSorter.exe").write_text("new exe", encoding="utf-8")
    (source / "VERSION").write_text("2.0.0", encoding="utf-8")
    (source / "extra.txt").write_text("extra", encoding="utf-8")

    selected: set[PurePosixPath] = {PurePosixPath("extra.txt")}

    destination, copied = app._prepare_new_installation(
        source, current_root, selected, "2.0.0"
    )

    assert copied == 3
    assert (destination / "keep.txt").read_text(encoding="utf-8") == "keep"
    assert (destination / "extra.txt").read_text(encoding="utf-8") == "extra"
    assert (destination / "Sims4ModSorter.exe").read_text(encoding="utf-8") == "new exe"
    assert (destination / "VERSION").read_text(encoding="utf-8").strip() == "2.0.0"


def test_full_update_replaces_files_without_preserving_old(tmp_path):
    app = _make_app()

    current_root = tmp_path / "current"
    current_root.mkdir()
    (current_root / "Sims4ModSorter.exe").write_text("old exe", encoding="utf-8")
    (current_root / "VERSION").write_text("1.0.0", encoding="utf-8")
    (current_root / "old_only.txt").write_text("obsolete", encoding="utf-8")

    source = tmp_path / "update"
    source.mkdir()
    (source / "Sims4ModSorter.exe").write_text("new exe", encoding="utf-8")
    (source / "VERSION").write_text("2.0.0", encoding="utf-8")

    destination, copied = app._prepare_new_installation(
        source, current_root, None, "2.0.0"
    )

    assert copied == 2
    assert not (destination / "old_only.txt").exists()
    assert (destination / "Sims4ModSorter.exe").read_text(encoding="utf-8") == "new exe"
    assert (destination / "VERSION").read_text(encoding="utf-8").strip() == "2.0.0"
