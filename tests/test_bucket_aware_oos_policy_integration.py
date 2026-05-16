from __future__ import annotations

import json
from pathlib import Path

from factor_lab.controlled_route_policy import build_controlled_route_policy
from factor_lab.controlled_run_ledger import build_controlled_run_ledger, summarize_controlled_run_ledger


def _write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_ledger_uses_bucket_aware_oos_diagnostics_to_clear_raw_split_failure(tmp_path):
    run = tmp_path / "runs" / "value_quality_no_distress_bucket_aware"
    _write_json(run / "task_state.json", {"task_id": "task-1", "status": "finished"})
    _write_json(run / "experiment_ledger.json", {"config": {"route_id": "value_quality_no_distress", "mechanism_id": "value_quality_no_distress"}})
    _write_json(run / "results.json", [{"rank_ic_mean": 0.01, "top_bottom_spread_mean": -0.001, "pass_gate": False, "fail_reason": "too_many_split_failures"}])
    _write_json(run / "bucket_aware_portfolio_results.json", [{"spread_mean": 0.006, "pass_gate": True}])
    _write_json(run / "rolling_summary.json", [{"pass_rate": 0.1, "fail_count": 9, "pass_count": 1}])
    _write_json(
        tmp_path / "diagnostics" / "value_quality_no_distress" / "bucket_aware_rolling_summary.json",
        {
            "pass_rate": 0.9375,
            "positive_spread_ratio": 0.9375,
            "avg_spread_mean": 0.00618,
            "worst_spread_mean": -0.000874,
            "pass_count": 15,
            "window_count": 16,
        },
    )

    rows = build_controlled_run_ledger(runs_root=tmp_path / "runs", bucket_aware_diagnostics_root=tmp_path / "diagnostics")

    assert rows[0]["bucket_aware_oos_pass_gate"] is True
    assert rows[0]["bucket_aware_rolling_pass_rate"] == 0.9375
    assert rows[0]["too_many_split_failures"] is False


def test_route_policy_promotes_when_bucket_aware_oos_clears_raw_split_failures():
    summary = {
        "route_summary": {
            "value_quality_no_distress": {
                "run_count": 1,
                "pass_gate_count": 1,
                "coverage_too_low_count": 0,
                "too_many_split_failures_count": 0,
                "bucket_aware_oos_pass_gate_count": 1,
                "bucket_aware_rolling_pass_rate_max": 0.9375,
                "bucket_aware_positive_spread_ratio_max": 0.9375,
            }
        }
    }

    policy = build_controlled_route_policy(summary)

    assert policy["routes"]["value_quality_no_distress"]["decision"] == "promote"
    assert policy["routes"]["value_quality_no_distress"]["reason"] == "bucket_aware_oos_stable"


def test_summarize_controlled_run_ledger_counts_bucket_aware_oos_metrics():
    summary = summarize_controlled_run_ledger([
        {
            "route_id": "value_quality_no_distress",
            "status": "finished",
            "pass_gate": True,
            "coverage_too_low": False,
            "too_many_split_failures": False,
            "bucket_aware_oos_pass_gate": True,
            "bucket_aware_rolling_pass_rate": 0.9375,
            "bucket_aware_positive_spread_ratio": 0.9375,
        }
    ])

    row = summary["route_summary"]["value_quality_no_distress"]
    assert row["bucket_aware_oos_pass_gate_count"] == 1
    assert row["bucket_aware_rolling_pass_rate_max"] == 0.9375
    assert row["bucket_aware_positive_spread_ratio_max"] == 0.9375
