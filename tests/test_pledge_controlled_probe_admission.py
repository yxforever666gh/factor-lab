from __future__ import annotations

import importlib.util
import json
from pathlib import Path


SCRIPT = Path("scripts/admit_pledge_controlled_probe_task.py")


def _load_module():
    spec = importlib.util.spec_from_file_location("admit_pledge_controlled_probe_task", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_pledge_controlled_probe_admission_dry_run_allows_without_write():
    module = _load_module()
    result = module.admit_pledge_controlled_probe_task(write=False, db_path="artifacts/factor_lab.db")
    assert result["ok"] is True
    assert result["decision"] == "dry_run_allow"
    assert result["would_enqueue_count"] == 1
    assert result["enqueued_count"] == 0
    assert result["task"]["payload"]["route_id"] == "value_quality_high_pledge_record_count_confirmation"
    assert result["task"]["payload"]["source"] == "controlled_pledge_probe"
    assert result["admission"]["decision"] == "allow"


def test_pledge_controlled_probe_admission_write_enqueues_one(tmp_path):
    module = _load_module()
    db_path = tmp_path / "factor_lab.db"
    result = module.admit_pledge_controlled_probe_task(write=True, db_path=str(db_path), priority=0)
    assert result["ok"] is True
    assert result["decision"] == "enqueued"
    assert result["enqueued_count"] == 1
    assert len(result["task_ids"]) == 1

    import sqlite3

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT task_type, status, priority, payload_json, worker_note FROM research_tasks").fetchone()
    conn.close()
    assert row[0] == "workflow"
    assert row[1] == "pending"
    assert row[2] == 0
    payload = json.loads(row[3])
    assert payload["route_id"] == "value_quality_high_pledge_record_count_confirmation"
    assert payload["mechanism_id"] == "pledge_control_pressure"
    assert payload["source"] == "controlled_pledge_probe"
    assert row[4].startswith("controlled_pledge_probe")
