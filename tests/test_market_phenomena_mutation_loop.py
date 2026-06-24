from __future__ import annotations

import json

from factor_lab.market_phenomena_mutation_loop import (
    build_next_iteration_from_mutation,
    validate_next_iteration_bundle,
    next_iteration_bundle_to_markdown,
    write_next_iteration_bundle,
)


def previous_plan():
    return {
        "run_id": "plan_v1",
        "phenomenon_id": "value_trap_escape_after_balance_sheet_repair_v1",
        "title": "资产负债表修复后的价值陷阱脱离",
        "target_group": "balance_sheet_repair_low_valuation",
        "mechanism_hypothesis": {"claim": "balance sheet repair delayed repricing", "mispricing_mechanism": "capital_constraint_and_slow_confirmation"},
        "participant_logic": {"participants": ["forced_seller", "risk_budgeted_institution"], "constraint": "capital_constraint"},
        "observable_variables": {"primary": ["pb", "debt_to_asset_delta"], "controls": ["industry", "size_bucket", "turnover_bucket", "market_regime"]},
        "controlled_research_backtest_design": {"minimum_checks": ["industry_split_robustness", "size_split_robustness", "regime_split_robustness", "turnover_sensitivity", "drawdown_sensitivity", "cost_sensitivity_probe"]},
        "production_boundaries": {"live_trading_allowed": False, "queue_write_allowed": False, "timer_enable_allowed": False, "daemon_restore_allowed": False, "auto_promotion_allowed": False},
        "stop_conditions": ["negative spread versus matched controls"],
    }


def mutation_request(action="mutate_risk_or_cost_model"):
    return {
        "run_id": "mutation",
        "phenomenon_id": "value_trap_escape_after_balance_sheet_repair_v1",
        "action": action,
        "reason_codes": ["cost_adjusted_return_non_positive"],
        "requested_actions": ["tighten_turnover_or_liquidity_filter", "add_drawdown_guard", "rerun_cost_sensitivity"],
        "support_summary": {
            "checks": {
                "industry_split_robustness": {"positive_rate": 0.8},
                "size_split_robustness": {"positive_rate": 1.0},
                "regime_split_robustness": {"positive_rate": 1.0},
                "turnover_sensitivity": {"positive_rate": 1.0},
            },
            "cost": {"cost_adjusted_mean_return": -0.0015},
            "drawdown": {"worst_forward_return": -0.59},
        },
        "queue_write_allowed": False,
        "live_trading_allowed": False,
    }


def test_mutation_loop_builds_v2_plan_with_risk_cost_constraints():
    bundle = build_next_iteration_from_mutation(run_id="v2", previous_iteration_plan=previous_plan(), mutation_request=mutation_request())
    plan = bundle["agent_iteration_plan_v2"]
    request = bundle["research_execution_request_v2"]
    assert bundle["mode"] == "next_iteration_bundle"
    assert plan["mutation_parent_run_id"] == "plan_v1"
    assert plan["mutation_reason_codes"] == ["cost_adjusted_return_non_positive"]
    assert "liquidity_turnover_filter" in plan["risk_cost_constraints"]
    assert "drawdown_guard" in plan["risk_cost_constraints"]
    assert "cost_sensitivity_probe" in request["requested_checks"]
    assert "drawdown_sensitivity" in request["requested_checks"]
    assert request["queue_write_allowed"] is False


def test_mutation_loop_preserves_supported_split_checks():
    bundle = build_next_iteration_from_mutation(run_id="v2", previous_iteration_plan=previous_plan(), mutation_request=mutation_request())
    checks = bundle["research_execution_request_v2"]["requested_checks"]
    assert "industry_split_robustness" in checks
    assert "size_split_robustness" in checks
    assert "regime_split_robustness" in checks
    assert "turnover_sensitivity" in checks


def test_mutation_loop_handles_request_more_data_without_execution_permission():
    req = mutation_request(action="request_more_data")
    req["missing_columns"] = ["industry"]
    bundle = build_next_iteration_from_mutation(run_id="v2", previous_iteration_plan=previous_plan(), mutation_request=req)
    assert bundle["research_execution_request_v2"]["controlled_research_backtest_allowed"] is False
    assert bundle["agent_iteration_plan_v2"]["data_feasibility_assumptions"]["blocked_if"]
    assert "industry" in json.dumps(bundle, ensure_ascii=False)


def test_validate_next_iteration_bundle_rejects_open_gates_and_missing_constraints():
    bundle = build_next_iteration_from_mutation(run_id="v2", previous_iteration_plan=previous_plan(), mutation_request=mutation_request())
    bundle["research_execution_request_v2"]["queue_write_allowed"] = True
    bundle["agent_iteration_plan_v2"]["risk_cost_constraints"] = {}
    validation = validate_next_iteration_bundle(bundle)
    assert validation["decision"] == "reject"
    assert "execution_request_gate_not_closed_queue_write_allowed" in validation["reason_codes"]
    assert "missing_risk_cost_constraints" in validation["reason_codes"]


def test_next_iteration_bundle_markdown_and_writes(tmp_path):
    bundle = build_next_iteration_from_mutation(run_id="v2", previous_iteration_plan=previous_plan(), mutation_request=mutation_request())
    assert validate_next_iteration_bundle(bundle)["decision"] == "keep"
    markdown = next_iteration_bundle_to_markdown(bundle)
    assert "Next Iteration Bundle" in markdown
    assert "mutate_risk_or_cost_model" in markdown
    paths = write_next_iteration_bundle(bundle, tmp_path)
    assert paths["plan_json"].exists()
    assert paths["request_json"].exists()
    payload = json.loads(paths["request_json"].read_text(encoding="utf-8"))
    assert payload["run_id"] == "v2_execution_request"
