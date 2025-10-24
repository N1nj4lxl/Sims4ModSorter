from __future__ import annotations

from pathlib import Path

import scanner


def _clear_scan_cache() -> None:
    base = Path(scanner.__file__).resolve().parent
    for suffix in ("", "-wal", "-shm"):
        cache_path = base / f"scan_cache.db{suffix}"
        if cache_path.exists():
            cache_path.unlink()


PNG_BYTES = (
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde"
    b"\x00\x00\x00\x0cIDAT\x08\xd7c\xf8\x0f\x00\x01\x05\x01\x02\x9a\x07\x9c\xba"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)


def test_scan_folder_handles_duplicate_pngs(tmp_path):
    _clear_scan_cache()
    (tmp_path / "first.png").write_bytes(PNG_BYTES)
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "second.png").write_bytes(PNG_BYTES)

    result = scanner.scan_folder(tmp_path)

    assert result.errors == []
    relpaths = sorted(item.relpath for item in result.items)
    assert relpaths == ["first.png", "nested/second.png"]
    duplicate_flags = [
        item.extras.get(scanner.DUPLICATE_EXTRA_KEY, "") for item in result.items
    ]
    assert duplicate_flags.count("⚠") == 1
