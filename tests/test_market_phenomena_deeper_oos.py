from __future__ import annotations

import json

import pandas as pd

from factor_lab.market_phenomena_deeper_oos import (
    build_deeper_oos_horizon_report,
    validate_deeper_oos_horizon_report,
    deeper_oos_horizon_report_to_markdown,
    write_deeper_oos_horizon_report,
)


def iteration_plan():
    return {
        "run_id": "plan_v2",
        "phenomenon_id": "p1",
        "target_group": "target",
        "risk_cost_constraints": {
            "liquidity_turnover_filter": {"rule": "exclude highest turnover/cost bucket"},
            "drawdown_guard": {"rule": "reject extreme worst_forward_return"},
        },
        "production_boundaries": {"live_trading_allowed": False, "queue_write_allowed": False, "timer_enable_allowed": False, "daemon_restore_allowed": False, "auto_promotion_allowed": False},
    }


def feature_frame():
    rows = []
    dates = pd.date_range("2018-01-01", periods=36, freq="MS")
    for ticker, group, base in [("A", "target", 10.0), ("B", "control", 20.0), ("C", "target", 12.0), ("D", "control", 22.0)]:
        for i, date in enumerate(dates):
            trend = 1 + i * (0.02 if group == "target" else 0.005)
            rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "close": base * trend,
                    "turnover_rate": 0.5 if i % 5 else 3.0,
                    "_phenomenon_group": group,
                    "industry": "x",
                    "total_mv": base * 100,
                }
            )
    return pd.DataFrame(rows)


def test_deeper_oos_report_runs_requested_horizons_and_splits():
    report = build_deeper_oos_horizon_report(
        run_id="deep",
        iteration_plan=iteration_plan(),
        feature_frame=feature_frame(),
        horizons=[1, 3],
        split_years={"train": [2018], "validation": [2019], "oos": [2020]},
    )
    assert report["mode"] == "deeper_oos_horizon_report"
    assert report["source_iteration_plan_run_id"] == "plan_v2"
    assert report["summary"]["horizon_count"] == 2
    assert report["summary"]["split_count"] == 3
    assert len(report["results"]) == 6
    assert {item["split"] for item in report["results"]} == {"train", "validation", "oos"}
    assert {item["horizon"] for item in report["results"]} == [1, 3] or {item["horizon"] for item in report["results"]} == {1, 3}


def test_deeper_oos_report_applies_constraints_before_metrics():
    report = build_deeper_oos_horizon_report(
        run_id="deep",
        iteration_plan=iteration_plan(),
        feature_frame=feature_frame(),
        horizons=[1],
        split_years={"oos": [2020]},
    )
    result = report["results"][0]
    assert result["constraint_application"]["constraints_applied"] == ["liquidity_turnover_filter", "drawdown_guard"]
    assert result["constraint_application"]["rows_after_constraints"] < result["constraint_application"]["rows_before_constraints"]
    assert result["cost_adjusted_spread_vs_control"] is not None


def test_validate_rejects_missing_oos_or_open_gates():
    report = build_deeper_oos_horizon_report(
        run_id="deep",
        iteration_plan=iteration_plan(),
        feature_frame=feature_frame(),
        horizons=[1],
        split_years={"train": [2018]},
    )
    report["queue_write_allowed"] = True
    validation = validate_deeper_oos_horizon_report(report)
    assert validation["decision"] == "reject"
    assert "missing_oos_result" in validation["reason_codes"]
    assert "gate_not_closed_queue_write_allowed" in validation["reason_codes"]


def test_deeper_oos_markdown_and_writes(tmp_path):
    report = build_deeper_oos_horizon_report(
        run_id="deep",
        iteration_plan=iteration_plan(),
        feature_frame=feature_frame(),
        horizons=[1, 3],
        split_years={"train": [2018], "validation": [2019], "oos": [2020]},
    )
    assert validate_deeper_oos_horizon_report(report)["decision"] == "keep"
    markdown = deeper_oos_horizon_report_to_markdown(report)
    assert "Deeper OOS / Holding Horizon Report" in markdown
    assert "oos" in markdown
    paths = write_deeper_oos_horizon_report(report, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["run_id"] == "deep"
