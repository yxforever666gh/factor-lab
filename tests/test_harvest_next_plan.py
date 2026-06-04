from factor_lab.harvest_next_plan import build_next_plan


def test_next_plan_stops_after_stop_verdict():
    plan = build_next_plan({"cycle_id": "cycle_0001", "decision": "stop_no_information_gain", "next_action": "stop"})
    assert plan["plan_status"] == "stop"
    assert plan["experiments"] == []


def test_next_plan_continues_bounded_same_mainline():
    plan = build_next_plan({"cycle_id": "cycle_0001", "decision": "continue_same_mainline", "next_action": "run stricter tail-risk validation"})
    assert plan["cycle_id"] == "cycle_0002"
    assert plan["research_budget"]["max_experiments"] == 1
    assert len(plan["proposals"]) == 1
    assert plan["manual_approval_required"] is False


def test_next_plan_manual_review_halts_execution():
    plan = build_next_plan({"cycle_id": "cycle_0001", "decision": "promote_to_manual_review", "manual_approval_required": True})
    assert plan["plan_status"] == "manual_review"
    assert plan["manual_approval_required"] is True
