from factor_lab.harvest_portfolio_branch_planner import build_portfolio_branch_plan


def test_risk_branch_adds_stricter_volatility_and_positive_costs():
    out = build_portfolio_branch_plan(
        decision={"decision":"risk_reduction_branch"},
        current_plan={"actions": []},
        failure_attribution={},
        mechanism_route={"allowed_signals":["s1"]},
    )
    assert any(a.get("field") == "volatility_20" and a.get("quantile") <= 0.5 for a in out["actions"])
    assert any(a.get("type") == "restrict_costs" and 0 not in a.get("cost_bps_values", []) for a in out["actions"])


def test_portfolio_branch_emits_bucket_pair_configs():
    out = build_portfolio_branch_plan(
        decision={"decision":"portfolio_construction_branch"},
        current_plan={},
        failure_attribution={},
        mechanism_route={"allowed_signals":["s1"]},
    )
    assert out["portfolio_construction"][0]["mode"] == "bucket_pair"
    assert out["signal_columns"] == ["s1"]


def test_cost_branch_excludes_zero_cost():
    out = build_portfolio_branch_plan(
        decision={"decision":"cost_robustness_branch"},
        current_plan={},
        failure_attribution={},
        mechanism_route={},
    )
    costs = [a for a in out["actions"] if a.get("type") == "restrict_costs"][0]["cost_bps_values"]
    assert 0 not in costs
