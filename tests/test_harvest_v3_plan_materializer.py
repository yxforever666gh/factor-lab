import pytest

from factor_lab.harvest_experiment_fingerprint import fingerprint_plan
from factor_lab.harvest_v3_plan_materializer import materialize_v3_next_plan


def test_materialize_cost_robustness_branch_actions():
    v3_plan = {
        "cycle_id": "cycle_0051",
        "based_on_cycle": "cycle_0050",
        "plan_status": "planned",
        "branch": "cost_robustness_branch",
        "rationale": ["zero_cost_only_best"],
        "expected_information_gain": "test costs",
        "experiments": [
            {"type": "action", "action": {"type": "restrict_costs", "cost_bps_values": [30, 60]}},
            {"type": "action", "action": {"type": "add_filter", "field": "turnover", "operator": ">=", "quantile": 0.4}},
            {"type": "action", "action": {"type": "prefer_cost_robust"}},
        ],
    }

    plan = materialize_v3_next_plan(v3_plan, next_cycle_id="cycle_0051", dataset_path="dataset.csv", mechanism_route={"mechanism_id": "industry_relative_value"})

    assert plan["cycle_id"] == "cycle_0051"
    assert plan["based_on_cycle"] == "cycle_0050"
    assert plan["executable"] is True
    assert plan["mechanism_route"]["mechanism_id"] == "industry_relative_value"
    assert {a["type"] for a in plan["actions"]} == {"restrict_costs", "add_filter", "prefer_cost_robust"}
    assert plan["research_decision"]["decision"] == "cost_robustness_branch"
    assert fingerprint_plan(plan)


def test_materialize_portfolio_branch_preserves_construction_config():
    v3_plan = {
        "plan_status": "planned",
        "branch": "portfolio_construction_branch",
        "experiments": [
            {"type": "portfolio_construction", "config": {"mode": "bucket_pair", "long_quantile": 3, "short_quantile": 0}},
        ],
    }

    plan = materialize_v3_next_plan(v3_plan, next_cycle_id="cycle_0002", dataset_path="dataset.csv")

    assert plan["portfolio_construction"] == [{"mode": "bucket_pair", "long_quantile": 3, "short_quantile": 0}]


def test_materialize_applies_safe_strategy_plan_constraints():
    v3_plan = {
        "plan_status": "planned",
        "branch": "portfolio_construction_branch",
        "experiments": [
            {"type": "action", "action": {"type": "restrict_costs", "cost_bps_values": [0, 30, 60]}},
            {"type": "action", "action": {"type": "set_holding_counts", "holding_counts": [50, 75, 100]}},
        ],
    }
    strategy_plan = {
        "strategy_run_id": "strategy_test",
        "allowed_branches": ["risk_reduction_branch"],
        "blocked_branches": ["portfolio_construction_branch"],
        "experiment_constraints": {
            "max_next_backtests": 120,
            "allowed_cost_bps": [30, 60],
            "allowed_holding_counts": [75, 100],
            "require_non_duplicate_semantic_hash": True,
        },
    }

    plan = materialize_v3_next_plan(v3_plan, next_cycle_id="cycle_0002", dataset_path="dataset.csv", strategy_plan=strategy_plan)

    assert plan["strategy_plan_id"] == "strategy_test"
    assert plan["controller_constraints"]["max_next_backtests"] == 120
    assert plan["controller_constraints"]["blocked_branches"] == ["portfolio_construction_branch"]
    assert plan["research_decision"]["decision"] == "risk_reduction_branch"
    assert plan["research_decision"]["source_v3_decision"] == "portfolio_construction_branch"
    assert {tuple(a.get("cost_bps_values", [])) for a in plan["actions"] if a["type"] == "restrict_costs"} == {(30, 60)}
    assert {tuple(a.get("holding_counts", [])) for a in plan["actions"] if a["type"] == "set_holding_counts"} == {(75, 100)}


@pytest.mark.parametrize("status", ["blocked", "stopped"])
def test_materialize_refuses_non_planned_v3_plan(status):
    with pytest.raises(ValueError, match="not planned"):
        materialize_v3_next_plan({"plan_status": status}, next_cycle_id="cycle_0002", dataset_path="dataset.csv")
