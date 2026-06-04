from factor_lab.harvest_agent_policy import DEFAULT_HARVEST_AGENT_POLICY
from factor_lab.harvest_planner import build_harvest_cycle_plan


def test_planner_chooses_one_mainline_and_budget():
    plan = build_harvest_cycle_plan({"promoted_bucket_aware_routes": ["value_quality_no_distress"]}, cycle_id="cycle_0001")
    assert plan["cycle_charter"]["mainline"] == "bucket_aware_oos_followup"
    assert plan["cycle_charter"]["research_budget"]["max_experiments"] == 2
    assert len(plan["proposals"]) <= 2


def test_planner_prefers_defensive_quality_when_drawdown_dominates():
    plan = build_harvest_cycle_plan({"current_blockers": ["drawdown_risk_too_high"]})
    assert plan["cycle_charter"]["mainline"] == "defensive_quality_risk_layer"


def test_planner_prefers_data_gap_analysis_for_blocked_fields():
    plan = build_harvest_cycle_plan({"data_blockers": {"blocked_fields": ["analyst_revision"]}})
    assert plan["cycle_charter"]["mainline"] == "mechanism_data_gap_analysis"
    assert plan["proposals"][0]["experiment_type"] == "data_gap_analysis"


def test_planner_never_generates_arbitrary_expression_variants_without_mechanism():
    plan = build_harvest_cycle_plan({})
    assert all(p.get("mechanism_id") for p in plan["proposals"])
    assert all("arbitrary" not in p["proposal_id"] for p in plan["proposals"])


def test_planner_rejects_policy_budget_overflow():
    policy = dict(DEFAULT_HARVEST_AGENT_POLICY)
    policy["max_experiments_per_cycle"] = 1
    plan = build_harvest_cycle_plan({"promoted_bucket_aware_routes": ["a", "b"]}, policy=policy)
    assert len(plan["proposals"]) == 1
