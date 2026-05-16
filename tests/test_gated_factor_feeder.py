import json
import sqlite3

from factor_lab.gated_factor_feeder import build_gated_factor_configs, write_gated_factor_configs
from tests.test_research_gate import valid_hypothesis


def passing_preflight():
    return {
        "mode": "sample",
        "no_factor_run": True,
        "no_queue_write": True,
        "no_daemon_start": True,
        "tushare": {"summary": {"ready_for_p0_value_trap_experiment": True}},
        "diemeng": {"summary": {"ready_for_p0_value_trap_experiment": False}},
    }


def test_gated_feeder_blocks_cashflow_variants_after_closure_policy():
    result = build_gated_factor_configs(valid_hypothesis(), preflight_report=passing_preflight())
    assert result["decision"] == "blocked"
    assert result["configs"] == []
    assert result["reasons"] == ["cashflow_conditioning_closed"]
    assert len(result["cashflow_closure_blocked"]) == 3


def test_gated_feeder_blocks_without_preflight_pass():
    result = build_gated_factor_configs(valid_hypothesis(), preflight_report={"mode": "sample"})
    assert result["decision"] == "blocked"
    assert "primary_tushare_pit_preflight_not_ready" in result["preflight"]["reasons"]


def test_gated_feeder_does_not_support_unknown_hypothesis():
    h = valid_hypothesis()
    h["hypothesis_id"] = "random"
    result = build_gated_factor_configs(h, preflight_report=passing_preflight())
    assert result["decision"] == "blocked"


def test_gated_configs_include_value_trap_features():
    result = build_gated_factor_configs(valid_hypothesis(), preflight_report=passing_preflight(), max_variants=1)
    assert result["decision"] == "blocked"
    assert result["configs"] == []
    assert result["cashflow_closure_blocked"][0]["variant_id"] == "industry_relative_value_plus_cashflow_quality"


def test_write_configs_requires_explicit_write(tmp_path):
    out_dir = tmp_path / "configs"
    dry = write_gated_factor_configs(valid_hypothesis(), preflight_report=passing_preflight(), output_dir=out_dir, write=False)
    assert dry["files"] == []
    assert not out_dir.exists()
    written = write_gated_factor_configs(valid_hypothesis(), preflight_report=passing_preflight(), output_dir=out_dir, write=True, max_variants=1)
    assert written["files"] == []
    assert written["queue_written"] is False


def test_dry_run_does_not_create_queue_rows(tmp_path):
    db = tmp_path / "factor_lab.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE research_tasks (id INTEGER PRIMARY KEY, task_type TEXT)")
    conn.commit()
    conn.close()
    write_gated_factor_configs(valid_hypothesis(), preflight_report=passing_preflight(), output_dir=tmp_path / "out", write=False, queue_db_path=db)
    conn = sqlite3.connect(db)
    count = conn.execute("SELECT COUNT(*) FROM research_tasks").fetchone()[0]
    conn.close()
    assert count == 0
