from __future__ import annotations

from factor_lab.pit_cashflow_closure_policy import build_cashflow_closure_policy, evaluate_cashflow_closure


def test_cashflow_closure_blocks_value_trap_cashflow_config() -> None:
    policy = build_cashflow_closure_policy({"decision": "stop_cashflow_conditioning_non_incremental"})
    config = {
        "route_id": "value_trap_filter_quality_confirmation",
        "required_data_fields": ["industry_relative_book_yield", "operating_cashflow_to_profit"],
        "factors": [{"name": "cashflow_value", "expression": "industry_relative_book_yield + operating_cashflow_to_profit"}],
    }

    decision = evaluate_cashflow_closure(config, policy=policy)

    assert decision.decision == "block"
    assert "cashflow_value_trap_route_closed" in decision.reasons
    assert "cashflow_fields_monitor_only" in decision.reasons
    assert "operating_cashflow_to_profit" in decision.matched_fields


def test_cashflow_closure_allows_non_cashflow_primary_route() -> None:
    policy = build_cashflow_closure_policy({"decision": "stop_cashflow_conditioning_non_incremental"})
    config = {
        "route_id": "value_quality_no_distress",
        "required_data_fields": ["industry_relative_book_yield"],
        "factors": [{"name": "value_quality", "expression": "industry_relative_book_yield"}],
    }

    decision = evaluate_cashflow_closure(config, policy=policy)

    assert decision.decision == "allow"
    assert decision.reasons == ("no_cashflow_closure_match",)


def test_cashflow_closure_can_be_reopened_by_explicit_plan_id() -> None:
    policy = build_cashflow_closure_policy({"decision": "stop_cashflow_conditioning_non_incremental"})
    config = {
        "route_id": "value_trap_filter_quality_confirmation",
        "required_data_fields": ["operating_cashflow_to_profit"],
        "governance": {"cashflow_reopen_plan_id": "2026-05-xx-new-mechanism"},
    }

    decision = evaluate_cashflow_closure(config, policy=policy)

    assert decision.decision == "allow"
    assert decision.reasons == ("explicit_cashflow_reopen_plan",)
    assert decision.override == "2026-05-xx-new-mechanism"
