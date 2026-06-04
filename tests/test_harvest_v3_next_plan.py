from factor_lab.harvest_v3_next_plan import build_v3_next_cycle_plan


def test_risk_branch_plan_includes_experiments():
    out = build_v3_next_cycle_plan(
        current_cycle_id="cycle_0001",
        diagnosis={}, oos_validation={}, failure_attribution={}, route_state={},
        research_decision={"decision":"risk_reduction_branch","expected_information_gain":"risk info","rationale":["r"]},
        portfolio_branch_plan={"actions":[{"type":"add_filter","field":"volatility_20"}]},
        data_request={"blocked":False},
    )
    assert out["branch"] == "risk_reduction_branch"
    assert out["plan_status"] == "planned"
    assert out["experiments"]


def test_data_request_branch_is_blocked():
    out = build_v3_next_cycle_plan(
        current_cycle_id="cycle_0001",
        diagnosis={}, oos_validation={}, failure_attribution={}, route_state={},
        research_decision={"decision":"data_request","expected_information_gain":"data info","rationale":[]},
        portfolio_branch_plan={},
        data_request={"blocked":True,"missing_required_fields":["cashflow"],"recommended_data":["debt_to_asset"]},
    )
    assert out["plan_status"] == "blocked"
    assert "cashflow" in out["data_request"]["missing_required_fields"]


def test_stop_route_branch_is_stopped():
    out = build_v3_next_cycle_plan(
        current_cycle_id="cycle_0001",
        diagnosis={}, oos_validation={}, failure_attribution={}, route_state={},
        research_decision={"decision":"stop_route","expected_information_gain":"stop","rationale":[]},
        portfolio_branch_plan={}, data_request={}
    )
    assert out["plan_status"] == "stopped"
