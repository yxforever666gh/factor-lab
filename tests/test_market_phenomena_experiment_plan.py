from __future__ import annotations

import json

from factor_lab.market_phenomena_experiment_plan import (
    FORBIDDEN_PLAN_FIELDS,
    build_minimal_verification_plan,
    minimal_verification_plan_to_markdown,
    plan_experiment_for_ready_phenomenon,
    validate_minimal_verification_plan,
    write_minimal_verification_plan,
)


def ready_item(phenomenon_id="quality_repair_delayed_repricing_v1"):
    return {
        "phenomenon_id": phenomenon_id,
        "title": "盈利质量修复后的延迟重估",
        "decision": "ready_for_minimal_verification",
        "observable_variables": ["profit_yoy", "roe", "debt_to_asset", "pb", "industry_return_60d"],
        "requested_target_horizons": ["60d"],
        "field_coverage": {"profit_yoy": 0.97, "roe": 1.0, "debt_to_asset": 0.97, "pb": 0.98},
        "row_count": 120866,
        "ticker_count": 93,
    }


def blocked_item():
    item = ready_item("blocked")
    item["decision"] = "blocked_missing_data"
    return item


def test_plan_experiment_for_ready_phenomenon_uses_distribution_not_strategy_rules():
    plan = plan_experiment_for_ready_phenomenon(ready_item())
    assert plan["phenomenon_id"] == "quality_repair_delayed_repricing_v1"
    assert plan["experiment_type"] == "conditional_distribution_test"
    assert plan["target_variables"] == ["future_60d_return", "future_60d_downside_risk", "future_60d_max_drawdown"]
    for forbidden in FORBIDDEN_PLAN_FIELDS:
        assert forbidden not in plan
    assert plan["strategy_generation_allowed"] is False
    assert plan["backtest_allowed"] is False
    assert plan["queue_write_allowed"] is False


def test_plan_experiment_for_balance_sheet_repair_has_repair_groups():
    item = ready_item("value_trap_escape_after_balance_sheet_repair_v1")
    item["title"] = "资产负债表修复后的价值陷阱脱离"
    item["observable_variables"] = ["debt_to_asset", "debt_to_asset_delta", "operating_cashflow_to_profit", "roe", "pb", "industry"]
    item["requested_target_horizons"] = ["120d"]
    plan = plan_experiment_for_ready_phenomenon(item)
    assert "balance_sheet_repair_low_valuation" in plan["comparison_groups"]
    assert plan["target_variables"] == ["future_120d_return", "future_120d_downside_risk", "future_120d_max_drawdown"]


def test_validate_minimal_verification_plan_rejects_strategy_fields():
    plan = plan_experiment_for_ready_phenomenon(ready_item())
    plan["buy_rule"] = "buy when condition group is true"
    result = validate_minimal_verification_plan(plan)
    assert result["decision"] == "reject"
    assert "forbidden_strategy_field_buy_rule" in result["reason_codes"]


def test_build_plan_only_includes_ready_items_and_preserves_safety_flags():
    report = build_minimal_verification_plan(
        run_id="m",
        data_feasibility_review={"reviewed_phenomena": [ready_item(), blocked_item()]},
    )
    assert len(report["experiments"]) == 1
    assert report["summary"]["planned"] == 1
    assert report["summary"]["skipped_not_ready"] == 1
    assert report["strategy_generation_allowed"] is False
    assert report["backtest_allowed"] is False
    assert report["queue_write_allowed"] is False


def test_minimal_verification_plan_markdown_and_write(tmp_path):
    report = build_minimal_verification_plan(run_id="m", data_feasibility_review={"reviewed_phenomena": [ready_item()]})
    markdown = minimal_verification_plan_to_markdown(report)
    assert "Minimal Verification Plan" in markdown
    assert "conditional_distribution_test" in markdown
    paths = write_minimal_verification_plan(report, tmp_path)
    assert paths["json"].exists()
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["summary"]["planned"] == 1
