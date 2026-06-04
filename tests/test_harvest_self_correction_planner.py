from factor_lab.harvest_self_correction_planner import build_correction_plan


def test_build_correction_plan_turns_diagnosis_into_executable_actions():
    plan = build_correction_plan(
        {"cycle_id": "cycle_0042", "best_signal_column": "industry_relative_book_yield"},
        {
            "failure_classes": [
                "drawdown_too_high",
                "weak_risk_adjusted_return",
                "zero_cost_best_only",
                "window_concentration",
            ]
        },
        next_cycle_id="cycle_0043",
    )
    assert plan["cycle_id"] == "cycle_0043"
    assert plan["based_on_cycle"] == "cycle_0042"
    assert plan["plan_status"] == "planned"
    assert {a["type"] for a in plan["actions"]} >= {"add_filter", "restrict_costs", "prefer_cost_robust", "set_signal_columns"}
    assert plan["success_criteria"]["max_drawdown_min"] == -0.35
    assert plan["success_criteria"]["positive_at_cost_bps"] == 30
    assert plan["mechanism_route"]["mechanism_id"] == "cost_robust_value_quality"
    assert set(next(a["signal_columns"] for a in plan["actions"] if a["type"] == "set_signal_columns")) <= set(plan["mechanism_route"]["allowed_signals"])


def test_build_correction_plan_avoids_exact_duplicate_with_attempt_index():
    plan1 = build_correction_plan({"cycle_id": "c1"}, {"failure_classes": ["drawdown_too_high"]}, next_cycle_id="c2", attempt_index=0)
    plan2 = build_correction_plan({"cycle_id": "c2"}, {"failure_classes": ["drawdown_too_high"]}, next_cycle_id="c3", attempt_index=1)
    assert plan1["actions"] != plan2["actions"]


def test_v3_stop_route_returns_stopped_non_executable_plan():
    plan = build_correction_plan(
        {"cycle_id": "c1"},
        {"failure_classes": ["drawdown_too_high"]},
        next_cycle_id="c2",
        research_decision={"decision": "stop_route"},
    )
    assert plan["plan_status"] == "stopped"
    assert plan["executable"] is False


def test_v3_cost_branch_excludes_zero_cost():
    plan = build_correction_plan(
        {"cycle_id": "c1"},
        {"failure_classes": []},
        next_cycle_id="c2",
        research_decision={"decision": "cost_robustness_branch"},
        portfolio_branch_plan={"actions": [{"type": "restrict_costs", "cost_bps_values": [30, 60]}]},
    )
    cost_actions = [a for a in plan["actions"] if a.get("type") == "restrict_costs"]
    assert cost_actions
    assert all(0 not in a["cost_bps_values"] for a in cost_actions)
