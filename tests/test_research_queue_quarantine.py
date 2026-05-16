import sqlite3

from factor_lab.research_queue_quarantine import quarantine_legacy_pending_tasks
from factor_lab.runtime_takeover_policy import load_runtime_takeover_policy


def _init_db(path):
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE research_tasks (
            task_id TEXT, task_type TEXT, status TEXT, priority INTEGER, fingerprint TEXT,
            payload_json TEXT, parent_task_id TEXT, attempt_count INTEGER, last_error TEXT,
            created_at_utc TEXT, started_at_utc TEXT, finished_at_utc TEXT, worker_note TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO research_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("t1", "workflow", "pending", 1, "fp", '{"config_path":"x"}', None, 0, None, "2026-04-29T15:00:00+00:00", None, None, "candidate_earnings_yield_book_yield_recent_90d"),
    )
    conn.execute(
        "INSERT INTO research_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("t2", "workflow", "finished", 1, "fp2", '{"config_path":"y"}', None, 0, None, "2026-04-29T15:00:00+00:00", None, None, "candidate_earnings_yield_book_yield_recent_45d"),
    )
    conn.commit(); conn.close()


def test_quarantine_legacy_pending_tasks_dry_run_does_not_modify_db(tmp_path):
    db = tmp_path / "factor_lab.db"
    _init_db(db)
    policy = load_runtime_takeover_policy({"enabled": True})

    result = quarantine_legacy_pending_tasks(db_path=db, policy=policy, dry_run=True)

    assert result["dry_run"] is True
    assert result["would_quarantine_count"] == 1
    conn = sqlite3.connect(db)
    status = conn.execute("SELECT status FROM research_tasks WHERE task_id='t1'").fetchone()[0]
    conn.close()
    assert status == "pending"


def test_quarantine_legacy_pending_tasks_write_marks_only_pending_blocked_tasks(tmp_path):
    db = tmp_path / "factor_lab.db"
    _init_db(db)
    policy = load_runtime_takeover_policy({"enabled": True})

    result = quarantine_legacy_pending_tasks(db_path=db, policy=policy, dry_run=False)

    assert result["quarantined_count"] == 1
    conn = sqlite3.connect(db)
    rows = conn.execute("SELECT task_id,status,worker_note FROM research_tasks ORDER BY task_id").fetchall()
    conn.close()
    assert rows[0][1] == "failed"
    assert "runtime_takeover_quarantined" in rows[0][2]
    assert rows[1][1] == "finished"
