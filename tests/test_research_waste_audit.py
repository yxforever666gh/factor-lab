from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from factor_lab.research_waste_audit import build_research_waste_audit, write_research_waste_audit
from factor_lab.storage import ExperimentStore


def _insert_workflow(conn, *, run_id: str, created_at: datetime, config_path: str, start_date: str, end_date: str, universe_limit: int, fingerprint: str, status: str = "finished"):
    conn.execute(
        """
        INSERT INTO workflow_runs (
            run_id, created_at_utc, config_path, output_dir, data_source,
            start_date, end_date, universe_limit, factor_count, dataset_rows,
            status, config_fingerprint, rerun_of_run_id
        ) VALUES (?, ?, ?, ?, 'tushare', ?, ?, ?, 1, 100, ?, ?, NULL)
        """,
        (run_id, created_at.isoformat(), config_path, f"artifacts/{run_id}", start_date, end_date, universe_limit, status, fingerprint),
    )


def test_build_research_waste_audit_counts_duplicate_configs_and_windows(tmp_path):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)
    now = datetime.now(timezone.utc)
    _insert_workflow(store.conn, run_id="r1", created_at=now - timedelta(hours=1), config_path="configs/a.json", start_date="2024-01-01", end_date="2024-12-31", universe_limit=100, fingerprint="fp-a")
    _insert_workflow(store.conn, run_id="r2", created_at=now - timedelta(hours=2), config_path="configs/a.json", start_date="2024-01-01", end_date="2024-12-31", universe_limit=100, fingerprint="fp-a")
    _insert_workflow(store.conn, run_id="r3", created_at=now - timedelta(hours=3), config_path="configs/b.json", start_date="2024-06-01", end_date="2024-12-31", universe_limit=50, fingerprint="fp-b", status="failed")
    store.conn.commit()

    audit = build_research_waste_audit(db_path=db_path, now=now)

    assert audit["workflow_runs"]["last_24h"]["total"] == 3
    assert audit["workflow_runs"]["last_24h"]["finished"] == 2
    assert audit["workflow_runs"]["last_24h"]["non_finished"] == 1
    assert audit["duplicates"]["config_path_top"][0]["config_path"] == "configs/a.json"
    assert audit["duplicates"]["config_fingerprint_top"][0]["config_fingerprint"] == "fp-a"
    assert audit["duplicates"]["duplicate_config_fingerprint_run_count_24h"] == 2
    assert audit["duplicates"]["duplicate_config_fingerprint_ratio_24h"] > 0
    assert audit["duplicates"]["duplicate_config_fingerprint_run_count_7d"] == 2
    assert audit["low_value_repeat_indicators"]["top_repeated_config_path"] == "configs/a.json"
    assert audit["recommended_blockers"][0]["blocker"] == "workflow_equivalence_duplicate_control"


def test_build_research_waste_audit_includes_rejection_and_candidate_status(tmp_path):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)
    now = datetime.now(timezone.utc)
    store.conn.execute(
        """
        INSERT INTO factor_candidates (
            id, name, family, definition_json, expression, status,
            evaluation_count, avg_final_score, pass_rate, created_at_utc, updated_at_utc
        ) VALUES ('c1', 'factor_a', 'value', '{}', 'book_yield', 'fragile', 3, 1.2, 0.1, ?, ?)
        """,
        (now.isoformat(), now.isoformat()),
    )
    store.conn.execute(
        """
        INSERT INTO factor_evaluations (
            id, candidate_id, sample_size, rejection_reason, status, pass_flag, created_at_utc
        ) VALUES ('e1', 'c1', 100, 'coverage_too_low', 'rejected', 0, ?)
        """,
        (now.isoformat(),),
    )
    store.conn.commit()

    audit = build_research_waste_audit(db_path=db_path, now=now)

    assert audit["factor_evaluations"]["rejection_reasons_top"][0]["rejection_reason"] == "coverage_too_low"
    assert audit["factor_candidates"]["status_counts"][0]["status"] == "fragile"


def test_write_research_waste_audit_writes_json_and_markdown(tmp_path):
    db_path = tmp_path / "factor_lab.db"
    ExperimentStore(db_path)
    json_path = tmp_path / "audit.json"
    md_path = tmp_path / "audit.md"

    audit = write_research_waste_audit(db_path=db_path, json_path=json_path, markdown_path=md_path)

    assert json_path.exists()
    assert md_path.exists()
    assert json.loads(json_path.read_text(encoding="utf-8"))["schema_version"] == "research_waste_audit.v1"
    assert "Research Waste Audit" in md_path.read_text(encoding="utf-8")
    assert audit["schema_version"] == "research_waste_audit.v1"
