from factor_lab.harvest_strategy_policy import decide_strategy


def test_policy_stops_when_route_state_stop():
    decision = decide_strategy({"current_route_status": "stop", "latest_cycle_id": "cycle_0057"})

    assert decision["strategy_decision"] == "stop_route"
    assert decision["plan_status"] == "stopped"
    assert decision["manual_approval_required"] is True
    assert "route_stopped" in decision["reason_codes"]


def test_policy_requests_data_when_missing_required_fields():
    decision = decide_strategy({
        "latest_cycle_id": "cycle_0057",
        "missing_required_fields": ["cashflow_quality", "leverage"],
        "current_route_status": "active",
    })

    assert decision["strategy_decision"] == "request_data"
    assert decision["plan_status"] == "blocked"
    assert decision["manual_approval_required"] is True
    assert decision["data_request"]["missing_fields"] == ["cashflow_quality", "leverage"]


def test_policy_switches_route_after_repeated_oos_failures():
    decision = decide_strategy({
        "latest_cycle_id": "cycle_0057",
        "current_route_status": "active",
        "ready_alternative_routes": ["value_quality_no_distress"],
        "loop_analysis": {"reason_codes": ["repeated_oos_failures", "sharpe_not_improving"]},
        "cycles": [
            {"oos_class": "fail", "best_sharpe": 0.45, "max_drawdown": -0.49},
            {"oos_class": "fail", "best_sharpe": 0.31, "max_drawdown": -0.57},
        ],
    })

    assert decision["strategy_decision"] == "switch_mechanism_route"
    assert decision["route_action"]["mechanism_id"] == "value_quality_no_distress"


def test_policy_shrinks_search_space_on_branch_loop():
    decision = decide_strategy({
        "latest_cycle_id": "cycle_0057",
        "current_route_status": "active",
        "ready_alternative_routes": [],
        "loop_analysis": {
            "loop_detected": True,
            "reason_codes": ["branch_loop_detected", "drawdown_not_improving"],
            "blocked_branches": ["portfolio_construction_branch", "cost_robustness_branch"],
        },
        "cycles": [
            {"oos_class": "fail", "best_sharpe": 0.45, "max_drawdown": -0.49},
            {"oos_class": "fail", "best_sharpe": 0.31, "max_drawdown": -0.57},
        ],
    })

    assert decision["strategy_decision"] == "shrink_search_space"
    assert "risk_reduction_branch" in decision["allowed_branches"]
    assert "portfolio_construction_branch" in decision["blocked_branches"]


def test_policy_prefers_risk_reduction_when_drawdown_worsens():
    decision = decide_strategy({
        "loop_analysis": {"reason_codes": ["drawdown_not_improving"]},
        "cycles": [{"oos_class": "fail", "max_drawdown": -0.6}],
    })

    assert decision["strategy_decision"] == "shrink_search_space"
    assert decision["allowed_branches"] == ["risk_reduction_branch"]


def test_policy_continues_with_constraints_when_no_blocker():
    decision = decide_strategy({
        "latest_cycle_id": "cycle_0057",
        "current_route_status": "active",
        "loop_analysis": {"loop_detected": False, "reason_codes": []},
        "cycles": [{"oos_class": "pass", "best_sharpe": 0.8, "max_drawdown": -0.2}],
    })

    assert decision["strategy_decision"] == "continue_with_constraints"
    assert decision["plan_status"] == "planned"
    assert decision["manual_approval_required"] is False
    assert decision["safety"]["no_daemon"] is True
