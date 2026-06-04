import json

from factor_lab.harvest_v3_plan_loader import classify_v3_next_plan, load_latest_v3_next_plan


def test_load_latest_v3_next_plan_returns_none_when_missing(tmp_path):
    assert load_latest_v3_next_plan(tmp_path) is None


def test_load_latest_v3_next_plan_reads_latest_cycle_plan(tmp_path):
    base = tmp_path / "artifacts/harvest_agent"
    cycle = base / "cycle_0050"
    cycle.mkdir(parents=True)
    (base / "latest_cycle.json").write_text(json.dumps({"cycle_id": "cycle_0050"}))
    (cycle / "v3_next_cycle_plan.json").write_text(json.dumps({"plan_status": "planned", "branch": "cost_robustness_branch"}))

    plan = load_latest_v3_next_plan(tmp_path)

    assert plan["branch"] == "cost_robustness_branch"
    assert plan["_source_cycle_id"] == "cycle_0050"
    assert plan["_source_path"].endswith("cycle_0050/v3_next_cycle_plan.json")


def test_classify_v3_next_plan_blocks_stops_and_manual_review():
    assert classify_v3_next_plan({"plan_status": "blocked"}) == {"decision": "block", "reason": "plan_status_blocked"}
    assert classify_v3_next_plan({"plan_status": "stopped"}) == {"decision": "stop", "reason": "plan_status_stopped"}
    assert classify_v3_next_plan({"plan_status": "planned", "manual_approval_required": True}) == {"decision": "manual_review", "reason": "manual_approval_required"}


def test_classify_v3_next_plan_allows_planned_cost_branch():
    result = classify_v3_next_plan({"plan_status": "planned", "branch": "cost_robustness_branch"})

    assert result == {"decision": "executable", "reason": "planned"}
