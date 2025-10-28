"""Tests for plugin_api module resilience."""
from __future__ import annotations

import json

import plugin_api


def test_load_history_index_handles_invalid_session_id(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin_api, "HISTORY_DIR", tmp_path)
    history_file = tmp_path / "scan-0001.json"
    history_file.write_text(json.dumps({"session_id": "corrupt"}), encoding="utf-8")

    metrics = plugin_api.ScanMetrics()

    assert metrics._session_counter == 0
