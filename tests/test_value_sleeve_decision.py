
from factor_lab.value_sleeve_decision import decide_value_sleeve


def _scorecard():
    return {"routes":[{"route_id":"value_quality_no_distress","base_spread_mean":0.006,"recommended_role":"primary_candidate"},{"route_id":"value_momentum_confirmation","base_spread_mean":0.005,"recommended_role":"confirmation_candidate"},{"route_id":"industry_relative_value","base_spread_mean":0.003,"recommended_role":"low_weight_core_value_candidate"}]}


def test_high_duplicate_collapses_to_sleeve():
    d=decide_value_sleeve(_scorecard(), {"decision":"high_duplicate_risk"}, {"combinations":[{"status":"ok","spread_improvement_vs_quality":-0.001,"spread_std_reduction_vs_quality":0.001}]})
    assert d["decision"] == "collapse_to_value_sleeve_with_primary_route"
    assert d["primary_route"] == "value_quality_no_distress"


def test_missing_diagnostics_hold():
    d=decide_value_sleeve({}, {}, {})
    assert d["decision"] == "hold_portfolio_expansion_pending_data_enrichment"
