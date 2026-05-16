import sqlite3
from pathlib import Path

from factor_lab.controlled_restart_audit import dry_run_controlled_restart


def _init_db(path: Path):
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE research_tasks (
            task_id TEXT,
            task_type TEXT,
            status TEXT,
            priority INTEGER,
            payload_json TEXT,
            worker_note TEXT,
            created_at_utc TEXT
        )
    """)
    conn.commit()
    conn.close()


def test_controlled_restart_dry_run_blocks_legacy_and_caps_allowed(tmp_path):
    db = tmp_path / "factor_lab.db"
    _init_db(db)
    conn = sqlite3.connect(db)
    allowed_payload = '{"mechanism_id":"industry_relative_value","route_id":"industry_relative_value","required_data_fields":["book_yield","industry"],"factors":[{"name":"x","expression":"book_yield"}]}'
    legacy_payload = '{"factors":[{"name":"x","expression":"book_yield"}]}'
    conn.execute("INSERT INTO research_tasks VALUES (?,?,?,?,?,?,?)", ("a", "workflow", "pending", 10, allowed_payload, "value route", "2026-01-01"))
    conn.execute("INSERT INTO research_tasks VALUES (?,?,?,?,?,?,?)", ("b", "workflow", "pending", 9, legacy_payload, "candidate_earnings_yield_book_yield_recent_90d", "2026-01-01"))
    conn.commit(); conn.close()

    result = dry_run_controlled_restart(db_path=db, max_new_workflows=1)

    assert result["would_run_count"] == 1
    assert result["claimable_workflow_count"] == 1
    assert result["blocked_workflow_count"] == 1
    assert result["pending_non_workflow_count"] == 0
    assert result["pending_diagnostic_count"] == 0
    assert result["would_run"][0]["task_id"] == "a"
    assert result["blocked_count"] == 1
    assert result["blocked"][0]["task_id"] == "b"


def test_controlled_restart_dry_run_counts_diagnostics_and_non_workflows(tmp_path):
    db = tmp_path / "factor_lab.db"
    _init_db(db)
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO research_tasks VALUES (?,?,?,?,?,?,?)", ("d", "diagnostic", "pending", 10, '{}', "diag", "2026-01-01"))
    conn.execute("INSERT INTO research_tasks VALUES (?,?,?,?,?,?,?)", ("g", "generated_batch", "pending", 9, '{}', "batch", "2026-01-01"))
    conn.commit(); conn.close()

    result = dry_run_controlled_restart(db_path=db, max_new_workflows=1)

    assert result["pending_non_workflow_count"] == 2
    assert result["pending_diagnostic_count"] == 1
    assert result["claimable_workflow_count"] == 0
