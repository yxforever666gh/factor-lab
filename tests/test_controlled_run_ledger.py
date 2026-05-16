from __future__ import annotations

import json
from pathlib import Path

from factor_lab.controlled_run_ledger import build_controlled_run_ledger, summarize_controlled_run_ledger, write_controlled_run_ledger


def _write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_build_controlled_run_ledger_extracts_bucket_aware_metrics(tmp_path):
    run = tmp_path / "runs" / "industry_relative_value_bucket_aware"
    _write_json(run / "task_state.json", {"task_id": "task-1", "status": "finished", "finished_at_utc": "2026-05-05T00:02:00+00:00", "config_path": "cfg.json", "output_dir": str(run)})
    _write_json(run / "experiment_ledger.json", {"config": {"route_id": "industry_relative_value", "mechanism_id": "industry_relative_value"}})
    _write_json(run / "results.json", [{"rank_ic_mean": 0.02, "top_bottom_spread_mean": -0.001, "pass_gate": False, "fail_reason": "coverage_too_low; too_many_split_failures"}])
    _write_json(run / "bucket_aware_portfolio_results.json", [{"spread_mean": 0.004, "pass_gate": True}])
    _write_json(run / "rolling_summary.json", [{"pass_rate": 0.1, "fail_count": 9, "pass_count": 1}])

    rows = build_controlled_run_ledger(runs_root=tmp_path / "runs")

    assert rows == [{
        "task_id": "task-1",
        "route_id": "industry_relative_value",
        "mechanism_id": "industry_relative_value",
        "config_path": "cfg.json",
        "output_dir": str(run),
        "status": "finished",
        "finished_at_utc": "2026-05-05T00:02:00+00:00",
        "rank_ic_mean": 0.02,
        "top_bottom_spread_mean": -0.001,
        "bucket_aware_spread_mean": 0.004,
        "pass_gate": True,
        "coverage_too_low": True,
        "too_many_split_failures": True,
        "rolling_pass_rate": 0.1,
        "bucket_aware_oos_pass_gate": False,
        "bucket_aware_rolling_pass_rate": None,
        "bucket_aware_positive_spread_ratio": None,
        "bucket_aware_avg_spread_mean": None,
        "bucket_aware_worst_spread_mean": None,
        "bucket_aware_oos_diagnostics_path": None,
    }]


def test_summarize_controlled_run_ledger_counts_routes_and_blockers():
    rows = [
        {"route_id": "a", "status": "finished", "pass_gate": True, "coverage_too_low": False, "too_many_split_failures": False},
        {"route_id": "a", "status": "finished", "pass_gate": False, "coverage_too_low": True, "too_many_split_failures": True},
        {"route_id": "b", "status": "failed", "pass_gate": False, "coverage_too_low": False, "too_many_split_failures": False},
    ]

    summary = summarize_controlled_run_ledger(rows)

    assert summary["total"] == 3
    assert summary["by_status"] == {"finished": 2, "failed": 1}
    assert summary["route_summary"]["a"]["run_count"] == 2
    assert summary["route_summary"]["a"]["pass_gate_count"] == 1
    assert summary["main_blockers"] == {"coverage_too_low": 1, "too_many_split_failures": 1}


def test_write_controlled_run_ledger_writes_jsonl_summary_and_markdown(tmp_path):
    run = tmp_path / "runs" / "value_quality_no_distress_bucket_aware"
    _write_json(run / "task_state.json", {"task_id": "task-2", "status": "finished", "finished_at_utc": "2026-05-05T00:02:00+00:00", "config_path": "cfg.json", "output_dir": str(run)})
    _write_json(run / "experiment_ledger.json", {"config": {"route_id": "value_quality_no_distress", "mechanism_id": "value_quality_no_distress"}})
    _write_json(run / "results.json", [{"rank_ic_mean": 0.03, "top_bottom_spread_mean": -0.001, "pass_gate": False}])
    _write_json(run / "bucket_aware_portfolio_results.json", [{"spread_mean": 0.006, "pass_gate": True}])

    result = write_controlled_run_ledger(runs_root=tmp_path / "runs", output_dir=tmp_path / "out")

    assert Path(result["jsonl_path"]).exists()
    assert Path(result["summary_json_path"]).exists()
    assert Path(result["summary_markdown_path"]).read_text().startswith("# Controlled Run Ledger Summary")
