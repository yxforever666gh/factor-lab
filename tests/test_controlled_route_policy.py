from __future__ import annotations

from factor_lab.controlled_route_policy import build_controlled_route_policy, route_decision_rank


def test_controlled_route_policy_promotes_passed_stable_route():
    summary = {"route_summary": {"value_quality_no_distress": {"run_count": 3, "pass_gate_count": 2, "coverage_too_low_count": 0, "too_many_split_failures_count": 0}}}

    policy = build_controlled_route_policy(summary)

    assert policy["routes"]["value_quality_no_distress"]["decision"] == "promote"
    assert policy["routes"]["value_quality_no_distress"]["reason"] == "passes_gate_without_repeated_blockers"


def test_controlled_route_policy_demotes_repeated_coverage_low():
    summary = {"route_summary": {"industry_relative_value": {"run_count": 3, "pass_gate_count": 0, "coverage_too_low_count": 2, "too_many_split_failures_count": 0}}}

    policy = build_controlled_route_policy(summary)

    assert policy["routes"]["industry_relative_value"]["decision"] == "demote"
    assert policy["routes"]["industry_relative_value"]["reason"] == "repeated_coverage_too_low"


def test_controlled_route_policy_holds_split_unstable_route():
    summary = {"route_summary": {"value_momentum_confirmation": {"run_count": 3, "pass_gate_count": 1, "coverage_too_low_count": 0, "too_many_split_failures_count": 2}}}

    policy = build_controlled_route_policy(summary)

    assert policy["routes"]["value_momentum_confirmation"]["decision"] == "hold"
    assert policy["routes"]["value_momentum_confirmation"]["reason"] == "repeated_split_instability"


def test_route_decision_rank_orders_promote_before_neutral_and_demote():
    assert route_decision_rank("promote") < route_decision_rank("neutral") < route_decision_rank("hold") < route_decision_rank("demote")
