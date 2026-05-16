from __future__ import annotations

import json
import sqlite3

from factor_lab.bucket_aware_task_preparer import prepare_bucket_aware_tasks


def test_prepare_bucket_aware_tasks_can_force_new_task_after_existing_pending(tmp_path):
    db = tmp_path / "factor_lab.db"
    conn = sqlite3.connect(db)
    conn.execute("""
        CREATE TABLE research_tasks (
            task_id TEXT,
            task_type TEXT,
            status TEXT,
            priority INTEGER,
            fingerprint TEXT,
            payload_json TEXT,
            parent_task_id TEXT,
            attempt_count INTEGER,
            last_error TEXT,
            created_at_utc TEXT,
            started_at_utc TEXT,
            finished_at_utc TEXT,
            worker_note TEXT
        )
    """)
    payload = {
        "route_id": "industry_relative_value",
        "mechanism_id": "industry_relative_value",
        "required_data_fields": ["book_yield", "industry"],
        "factors": [{"name": "x", "expression": "book_yield"}],
        "portfolio_construction": {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 0},
    }
    from factor_lab.dedup import workflow_experiment_fingerprint
    cfg_fingerprint = workflow_experiment_fingerprint(payload)
    conn.execute(
        "INSERT INTO research_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("existing", "workflow", "pending", 0, f"bucket_aware::{cfg_fingerprint}", json.dumps(payload), None, 0, None, "2026", None, None, "bucket_aware｜industry_relative_value"),
    )
    conn.commit(); conn.close()
    cfg = tmp_path / "industry_relative_value_bucket_aware.json"
    cfg.write_text(json.dumps(payload), encoding="utf-8")

    first = prepare_bucket_aware_tasks(config_paths=[cfg], db_path=db, dry_run=False, priority=0)
    forced = prepare_bucket_aware_tasks(config_paths=[cfg], db_path=db, dry_run=False, priority=0, force_new=True)

    assert first["task_ids"] == ["existing"]
    assert forced["task_ids"] != ["existing"]
    assert forced["enqueued_count"] == 1


def test_prepare_bucket_aware_tasks_suppresses_recent_finished_equivalent_evidence(tmp_path):
    db = tmp_path / "factor_lab.db"
    conn = sqlite3.connect(db)
    conn.execute("""
        CREATE TABLE research_tasks (
            task_id TEXT,
            task_type TEXT,
            status TEXT,
            priority INTEGER,
            fingerprint TEXT,
            payload_json TEXT,
            parent_task_id TEXT,
            attempt_count INTEGER,
            last_error TEXT,
            created_at_utc TEXT,
            started_at_utc TEXT,
            finished_at_utc TEXT,
            worker_note TEXT
        )
    """)
    payload = {
        "route_id": "industry_relative_value",
        "mechanism_id": "industry_relative_value",
        "required_data_fields": ["book_yield", "industry"],
        "factors": [{"name": "x", "expression": "book_yield"}],
        "portfolio_construction": {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 0},
    }
    from factor_lab.dedup import workflow_experiment_fingerprint
    cfg_fingerprint = workflow_experiment_fingerprint(payload)
    conn.execute(
        "INSERT INTO research_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "finished-existing",
            "workflow",
            "finished",
            0,
            f"bucket_aware::{cfg_fingerprint}",
            json.dumps(payload),
            None,
            1,
            None,
            "2026-05-05T00:00:00+00:00",
            "2026-05-05T00:01:00+00:00",
            "2026-05-05T00:02:00+00:00",
            "bucket_aware｜industry_relative_value",
        ),
    )
    conn.commit(); conn.close()
    cfg = tmp_path / "industry_relative_value_bucket_aware.json"
    cfg.write_text(json.dumps(payload), encoding="utf-8")

    result = prepare_bucket_aware_tasks(config_paths=[cfg], db_path=db, dry_run=False, priority=0)

    assert result["would_enqueue_count"] == 0
    assert result["enqueued_count"] == 0
    assert result["tasks"][0]["admission"]["decision"] == "skip"
    assert result["tasks"][0]["admission"]["reasons"] == ["recent_equivalent_evidence_exists"]
