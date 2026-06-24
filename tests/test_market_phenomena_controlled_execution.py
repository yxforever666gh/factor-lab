from __future__ import annotations

import json

import pandas as pd

from factor_lab.market_phenomena_controlled_execution import (
    build_controlled_execution_plan,
    run_controlled_research_execution,
    validate_controlled_execution_plan,
    controlled_execution_result_to_markdown,
    write_controlled_execution_artifacts,
)


def execution_request():
    return {
        "run_id": "request",
        "mode": "controlled_research_execution_request",
        "phenomenon_id": "value_trap_escape_after_balance_sheet_repair_v1",
        "controlled_research_backtest_allowed": True,
        "production_execution_allowed": False,
        "live_trading_allowed": False,
        "queue_write_allowed": False,
        "requested_checks": [
            "industry_split_robustness",
            "size_split_robustness",
            "regime_split_robustness",
            "turnover_sensitivity",
            "drawdown_sensitivity",
            "cost_sensitivity_probe",
        ],
        "stop_conditions": ["negative spread versus matched controls after robustness splits"],
    }


def iteration_plan():
    return {
        "run_id": "iteration",
        "phenomenon_id": "value_trap_escape_after_balance_sheet_repair_v1",
        "target_group": "balance_sheet_repair_low_valuation",
        "production_boundaries": {"live_trading_allowed": False, "queue_write_allowed": False, "timer_enable_allowed": False, "daemon_restore_allowed": False, "auto_promotion_allowed": False},
        "observable_variables": {"primary": ["pb", "debt_to_asset_delta", "operating_cashflow_to_profit"], "controls": ["industry", "size_bucket", "turnover_bucket", "market_regime"]},
        "stop_conditions": ["negative spread versus matched controls after robustness splits"],
    }


def feature_frame():
    return pd.DataFrame(
        {
            "date": pd.date_range("2020-01-01", periods=8).tolist() * 2,
            "ticker": ["A"] * 8 + ["B"] * 8,
            "close": [10, 11, 10, 12, 13, 12, 14, 15, 20, 19, 18, 20, 21, 20, 22, 23],
            "industry": ["bank"] * 8 + ["tech"] * 8,
            "total_mv": [100, 101, 99, 102, 103, 102, 104, 105, 300, 301, 299, 302, 303, 302, 304, 305],
            "turnover_rate": [1.0, 1.1, 1.2, 1.0, 0.9, 1.3, 1.2, 1.1, 2.0, 2.1, 2.2, 2.0, 1.9, 2.3, 2.2, 2.1],
            "future_5d_return": [0.1, 0.09, 0.08, 0.07, 0.06, 0.05, 0.04, 0.03, -0.01, -0.02, -0.03, -0.01, 0.0, -0.02, -0.01, 0.01],
            "_phenomenon_group": ["balance_sheet_repair_low_valuation"] * 8 + ["low_valuation_no_repair"] * 8,
        }
    )


def test_build_controlled_execution_plan_maps_requested_checks_to_safe_steps():
    plan = build_controlled_execution_plan(run_id="exec_plan", execution_request=execution_request(), iteration_plan=iteration_plan())
    assert plan["mode"] == "controlled_research_execution_plan"
    assert plan["source_execution_request_run_id"] == "request"
    assert [step["check_name"] for step in plan["execution_steps"]] == execution_request()["requested_checks"]
    assert plan["production_boundaries"]["queue_write_allowed"] is False
    assert plan["live_trading_allowed"] is False


def test_validate_controlled_execution_plan_rejects_production_or_unapproved_execution():
    plan = build_controlled_execution_plan(run_id="exec_plan", execution_request=execution_request(), iteration_plan=iteration_plan())
    plan["production_boundaries"]["queue_write_allowed"] = True
    plan["controlled_research_backtest_allowed"] = False
    result = validate_controlled_execution_plan(plan)
    assert result["decision"] == "reject"
    assert "production_gate_not_closed_queue_write_allowed" in result["reason_codes"]
    assert "controlled_research_backtest_not_allowed" in result["reason_codes"]


def test_run_controlled_execution_executes_feasible_checks_with_real_metrics():
    plan = build_controlled_execution_plan(run_id="exec_plan", execution_request=execution_request(), iteration_plan=iteration_plan())
    result = run_controlled_research_execution(run_id="exec_result", execution_plan=plan, feature_frame=feature_frame())
    assert result["mode"] == "controlled_research_execution_result"
    assert result["summary"]["executed"] == 6
    assert result["summary"]["blocked"] == 0
    assert result["production_execution_allowed"] is False
    industry = next(item for item in result["check_results"] if item["check_name"] == "industry_split_robustness")
    assert industry["status"] == "executed"
    assert "spread_by_bucket" in industry["metrics"]


def test_run_controlled_execution_blocks_missing_data_without_opening_gates():
    plan = build_controlled_execution_plan(run_id="exec_plan", execution_request=execution_request(), iteration_plan=iteration_plan())
    sparse = feature_frame().drop(columns=["industry", "total_mv", "turnover_rate"])
    result = run_controlled_research_execution(run_id="exec_result", execution_plan=plan, feature_frame=sparse)
    assert result["summary"]["blocked"] >= 3
    assert result["queue_write_allowed"] is False
    blocked = [item for item in result["check_results"] if item["status"] == "blocked_missing_columns"]
    assert blocked
    assert all(item["missing_columns"] for item in blocked)


def test_constraint_aware_plan_carries_risk_cost_constraints():
    request = execution_request()
    request["risk_cost_constraints"] = {
        "liquidity_turnover_filter": {"rule": "exclude highest turnover/cost bucket"},
        "drawdown_guard": {"rule": "reject extreme worst_forward_return"},
    }
    plan = build_controlled_execution_plan(run_id="exec_plan", execution_request=request, iteration_plan=iteration_plan())
    assert "liquidity_turnover_filter" in plan["risk_cost_constraints"]
    assert "drawdown_guard" in plan["risk_cost_constraints"]


def test_constraint_aware_execution_filters_high_cost_and_tail_rows():
    request = execution_request()
    request["risk_cost_constraints"] = {
        "liquidity_turnover_filter": {"rule": "exclude highest turnover/cost bucket"},
        "drawdown_guard": {"rule": "reject extreme worst_forward_return"},
    }
    frame = feature_frame()
    frame.loc[frame.index[-4:], "turnover_rate"] = 99.0
    frame.loc[frame.index[-4:], "future_5d_return"] = -0.90
    plan = build_controlled_execution_plan(run_id="exec_plan", execution_request=request, iteration_plan=iteration_plan())
    result = run_controlled_research_execution(run_id="exec_result", execution_plan=plan, feature_frame=frame)
    application = result["constraint_application"]
    assert application["constraints_applied"] == ["liquidity_turnover_filter", "drawdown_guard"]
    assert application["rows_after_constraints"] < application["rows_before_constraints"]
    cost = next(item for item in result["check_results"] if item["check_name"] == "cost_sensitivity_probe")
    assert cost["metrics"]["constraint_adjusted"] is True
    assert cost["metrics"]["cost_adjusted_mean_return"] > -0.20


def test_controlled_execution_markdown_and_writes(tmp_path):
    plan = build_controlled_execution_plan(run_id="exec_plan", execution_request=execution_request(), iteration_plan=iteration_plan())
    result = run_controlled_research_execution(run_id="exec_result", execution_plan=plan, feature_frame=feature_frame())
    markdown = controlled_execution_result_to_markdown(result)
    assert "Controlled Research Execution Result" in markdown
    assert "production_execution_allowed: False" in markdown
    paths = write_controlled_execution_artifacts(plan, result, tmp_path)
    assert paths["plan_json"].exists()
    assert paths["result_json"].exists()
    payload = json.loads(paths["result_json"].read_text(encoding="utf-8"))
    assert payload["run_id"] == "exec_result"
