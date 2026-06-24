from __future__ import annotations

import json

import pandas as pd

from factor_lab.market_phenomena_experiment_runner import (
    build_minimal_verification_result,
    compute_future_return,
    derive_condition_fields,
    minimal_verification_result_to_markdown,
    run_conditional_distribution_experiment,
    write_minimal_verification_result,
)


def sample_frame() -> pd.DataFrame:
    rows = []
    for ticker, base in [("A", 10.0), ("B", 20.0), ("C", 30.0), ("D", 40.0)]:
        for i in range(8):
            rows.append(
                {
                    "date": f"2020-01-{i+1:02d}",
                    "ticker": ticker,
                    "close": base + i,
                    "profit_yoy": 10 + i if ticker in {"A", "B"} else -10 - i,
                    "roe": 0.2 if ticker in {"A", "B"} else 0.05,
                    "debt_to_asset": 20 if ticker in {"A", "B"} else 80,
                    "pb": 0.8 if ticker in {"A", "C"} else 2.0,
                    "industry_return_60d": 0.1,
                    "debt_to_asset_delta": -5 if ticker in {"A", "B"} else 5,
                    "operating_cashflow_to_profit": 1.2 if ticker in {"A", "B"} else 0.3,
                    "industry": "x",
                }
            )
    return pd.DataFrame(rows)


def experiment_plan(phenomenon_id="quality_repair_delayed_repricing_v1"):
    return {
        "phenomenon_id": phenomenon_id,
        "title": "test",
        "experiment_type": "conditional_distribution_test",
        "condition_variables": ["profit_yoy", "roe", "debt_to_asset", "pb"],
        "target_variables": ["future_2d_return", "future_2d_downside_risk", "future_2d_max_drawdown"],
        "comparison_groups": ["quality_repair_low_valuation", "low_quality_low_valuation", "quality_repair_not_low_valuation"],
        "success_criteria": {"minimum_usable_tickers": 2, "minimum_usable_rows": 4},
    }


def test_compute_future_return_adds_forward_column():
    df = compute_future_return(sample_frame(), horizon_days=2)
    assert "future_2d_return" in df.columns
    first = df[(df["ticker"] == "A")].iloc[0]
    assert round(first["future_2d_return"], 6) == round((12.0 / 10.0) - 1.0, 6)


def test_derive_condition_fields_adds_debt_delta_and_industry_return():
    raw = sample_frame().drop(columns=["debt_to_asset_delta", "industry_return_60d"])
    derived = derive_condition_fields(raw, required_fields=["debt_to_asset_delta", "industry_return_60d"])
    assert "debt_to_asset_delta" in derived.columns
    assert "industry_return_60d" in derived.columns
    assert derived["industry_return_60d"].notna().any()


def test_run_conditional_distribution_experiment_returns_group_metrics():
    result = run_conditional_distribution_experiment(experiment_plan(), sample_frame())
    assert result["phenomenon_id"] == "quality_repair_delayed_repricing_v1"
    assert result["result_status"] in {"pass", "fail", "insufficient_sample"}
    assert result["usable_row_count"] > 0
    assert "quality_repair_low_valuation" in result["groups"]
    assert "mean_return" in result["groups"]["quality_repair_low_valuation"]
    assert result["strategy_generation_allowed"] is False
    assert result["backtest_allowed"] is False
    assert result["queue_write_allowed"] is False


def test_run_conditional_distribution_experiment_blocks_missing_condition_column():
    plan = experiment_plan()
    plan["condition_variables"] = ["missing_x"]
    result = run_conditional_distribution_experiment(plan, sample_frame())
    assert result["result_status"] == "blocked_missing_columns"
    assert result["missing_columns"] == ["missing_x"]


def test_build_minimal_verification_result_runs_all_experiments():
    report = build_minimal_verification_result(run_id="r", plan_report={"experiments": [experiment_plan()]}, feature_frame=sample_frame())
    assert report["summary"]["experiment_count"] == 1
    assert report["strategy_generation_allowed"] is False
    assert report["backtest_allowed"] is False


def test_minimal_verification_result_markdown_and_write(tmp_path):
    report = build_minimal_verification_result(run_id="r", plan_report={"experiments": [experiment_plan()]}, feature_frame=sample_frame())
    markdown = minimal_verification_result_to_markdown(report)
    assert "Minimal Verification Result" in markdown
    paths = write_minimal_verification_result(report, tmp_path)
    assert paths["json"].exists()
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["summary"]["experiment_count"] == 1
