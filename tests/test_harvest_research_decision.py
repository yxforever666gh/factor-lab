from factor_lab.harvest_research_decision import decide_next_research_branch


def _base(**overrides):
    payload = {
        "diagnosis": {"failure_classes": ["drawdown_too_high"]},
        "oos_validation": {"oos_class": "fail", "best_total_return": 1.0, "reasons": ["drawdown_below_threshold"]},
        "failure_attribution": {"primary_blockers": ["zero_cost_only_best"]},
        "route_state": {"current_route_status": "active"},
        "mechanism_route": {"mechanism_id": "industry_relative_value", "required_fields": ["book_yield"]},
        "available_fields": {"book_yield"},
    }
    payload.update(overrides)
    return payload


def test_stop_state_returns_stop_route():
    out = decide_next_research_branch(**_base(route_state={"current_route_status": "stop"}))
    assert out["decision"] == "stop_route"
    assert out["blocked"] is True


def test_missing_required_fields_returns_data_request():
    out = decide_next_research_branch(**_base(available_fields=set()))
    assert out["decision"] == "data_request"
    assert out["blocked"] is True


def test_drawdown_positive_return_returns_risk_branch():
    out = decide_next_research_branch(**_base(failure_attribution={"primary_blockers": []}))
    assert out["decision"] == "risk_reduction_branch"


def test_zero_cost_best_returns_cost_branch():
    out = decide_next_research_branch(**_base())
    assert out["decision"] == "cost_robustness_branch"
