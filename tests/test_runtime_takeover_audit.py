import sqlite3

from factor_lab.runtime_takeover_audit import build_runtime_takeover_audit, write_runtime_takeover_audit
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
        """
        CREATE TABLE workflow_runs (
            run_id TEXT, created_at_utc TEXT, config_path TEXT, output_dir TEXT,
            data_source TEXT, start_date TEXT, end_date TEXT, universe_limit INTEGER,
            factor_count INTEGER, dataset_rows INTEGER, status TEXT, config_fingerprint TEXT,
            rerun_of_run_id TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE factor_evaluations (
            run_id TEXT, status TEXT, rejection_reason TEXT, created_at_utc TEXT
        )
        """
    )
    conn.commit()
    return conn


def test_runtime_takeover_audit_identifies_old_path_and_pending_blocks(tmp_path):
    db = tmp_path / "factor_lab.db"
    conn = _init_db(db)
    conn.execute(
        "INSERT INTO research_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("t1", "workflow", "pending", 1, "fp", '{"config_path":"x"}', None, 0, None, "2026-04-29T15:00:00+00:00", None, None, "candidate_earnings_yield_book_yield_recent_90d"),
    )
    conn.execute(
        "INSERT INTO workflow_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("r1", "2026-04-29T15:00:00+00:00", "artifacts/generated_configs/candidate_earnings_yield_book_yield_recent_90d.json", "out", "tushare", "2026-01-01", "2026-04-01", 80, 1, 100, "finished", "fp", None),
    )
    conn.execute(
        "INSERT INTO factor_evaluations VALUES (?,?,?,?)",
        ("r1", "rejected", "coverage_too_low; too_many_split_failures", "2026-04-29T15:01:00+00:00"),
    )
    conn.commit(); conn.close()

    audit = build_runtime_takeover_audit(db_path=db, policy=load_runtime_takeover_policy({"enabled": True}), now_utc="2026-04-29T16:00:00+00:00")

    assert audit["workflow_runs"]["last_24h"] == 1
    assert audit["factor_evaluations"]["coverage_too_low_after_full_run"] == 1
    assert audit["pending_policy_decisions"]["block"] == 1
    assert audit["recommendations"][0] in {"pause_daemon", "clear_or_quarantine_legacy_pending"}


def test_write_runtime_takeover_audit_writes_json_and_markdown(tmp_path):
    db = tmp_path / "empty.db"
    _init_db(db).close()
    json_path = tmp_path / "audit.json"
    md_path = tmp_path / "audit.md"

    payload = write_runtime_takeover_audit(db_path=db, json_path=json_path, markdown_path=md_path, now_utc="2026-04-29T16:00:00+00:00")

    assert json_path.exists()
    assert "Runtime Takeover Audit" in md_path.read_text(encoding="utf-8")
    assert payload["schema_version"] == 1


def test_runtime_takeover_audit_allows_controlled_only_after_acceptance_passes(tmp_path):
    db = tmp_path / "empty.db"
    _init_db(db).close()
    acceptance = tmp_path / "acceptance.json"
    acceptance.write_text('{"ok": true, "passed_round_count": 3, "requested_rounds": 3}', encoding="utf-8")

    audit = build_runtime_takeover_audit(
        db_path=db,
        policy=load_runtime_takeover_policy({"enabled": True}),
        now_utc="2026-04-29T16:00:00+00:00",
        acceptance_path=acceptance,
    )

    assert audit["daemon_decision"]["pause_broad_daemon"] is True
    assert audit["daemon_decision"]["allow_controlled_only_daemon"] is True
    assert audit["daemon_decision"]["controlled_only_reason"] == "post_h_three_round_acceptance_passed"
    assert "pause_broad_daemon" in audit["recommendations"]
    assert "allow_controlled_only_daemon" in audit["recommendations"]


def test_runtime_takeover_audit_keeps_pause_daemon_without_acceptance(tmp_path):
    db = tmp_path / "empty.db"
    _init_db(db).close()

    audit = build_runtime_takeover_audit(
        db_path=db,
        policy=load_runtime_takeover_policy({"enabled": True}),
        now_utc="2026-04-29T16:00:00+00:00",
        acceptance_path=tmp_path / "missing_acceptance.json",
    )

    assert audit["daemon_decision"]["pause_broad_daemon"] is True
    assert audit["daemon_decision"]["allow_controlled_only_daemon"] is False
    assert audit["recommendations"] == ["pause_daemon"]
