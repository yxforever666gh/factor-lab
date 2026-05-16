from factor_lab.alpha_sleeves import assign_sleeve, build_sleeve_summary


def test_assign_sleeve_uses_mechanism_and_family():
    assert assign_sleeve({"mechanism_id": "industry_relative_value"}) == "value_sleeve"
    assert assign_sleeve({"family": "momentum"}) == "momentum_sleeve"
    assert assign_sleeve({"name": "turnover_shock_5_20"}) == "liquidity_sleeve"


def test_build_sleeve_summary_groups_candidates_and_caps_high_correlation_duplicates():
    candidates = [
        {"name": "value_a", "mechanism_id": "industry_relative_value", "allocated_weight": 0.4},
        {"name": "value_b", "mechanism_id": "value_quality_no_distress", "allocated_weight": 0.4, "correlation_cluster": "value_cluster"},
        {"name": "value_c", "mechanism_id": "value_momentum_confirmation", "allocated_weight": 0.4, "correlation_cluster": "value_cluster"},
        {"name": "mom", "family": "momentum", "allocated_weight": 0.2},
    ]

    summary = build_sleeve_summary(candidates)

    assert summary["sleeves"]["value_sleeve"]["candidate_count"] == 3
    assert summary["sleeves"]["momentum_sleeve"]["candidate_count"] == 1
    assert summary["sleeves"]["value_sleeve"]["duplicate_cluster_count"] == 1
    assert summary["sleeves"]["value_sleeve"]["recommended_action"] == "cap_duplicate_cluster_weight"
