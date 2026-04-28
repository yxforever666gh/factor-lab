from factor_lab.registry import FactorRegistry


def test_registry_builds_research_stages(tmp_path):
    registry = FactorRegistry(tmp_path)
    raw_results = [
        {"factor_name": "alpha_core", "expression": "book_yield", "pass_gate": True, "rank_ic_mean": 0.04, "rank_ic_ir": 0.8, "fail_reason": ""},
        {"factor_name": "alpha_explore", "expression": "earnings_yield", "pass_gate": True, "rank_ic_mean": 0.02, "rank_ic_ir": 0.3, "fail_reason": ""},
        {"factor_name": "alpha_dead", "expression": "roe", "pass_gate": False, "rank_ic_mean": -0.01, "rank_ic_ir": -0.2, "fail_reason": "rank_ic_mean<0.02"},
    ]
    neutralized_results = [
        {"factor_name": "alpha_core", "pass_gate": True, "rank_ic_mean": 0.03, "fail_reason": ""},
        {"factor_name": "alpha_explore", "pass_gate": False, "rank_ic_mean": 0.0, "fail_reason": "neutral_fail"},
    ]
    split_results = [
        {"factor_name": "alpha_core", "pass_gate": True},
        {"factor_name": "alpha_core", "pass_gate": True},
        {"factor_name": "alpha_explore", "pass_gate": True},
        {"factor_name": "alpha_explore", "pass_gate": False},
        {"factor_name": "alpha_dead", "pass_gate": False},
    ]
    rolling_results = [
        {"factor_name": "alpha_core", "pass_gate": True, "rank_ic_mean": 0.03, "top_bottom_spread_mean": 0.002},
        {"factor_name": "alpha_core", "pass_gate": True, "rank_ic_mean": 0.025, "top_bottom_spread_mean": 0.0018},
        {"factor_name": "alpha_explore", "pass_gate": False, "rank_ic_mean": 0.015, "top_bottom_spread_mean": 0.0004},
        {"factor_name": "alpha_explore", "pass_gate": False, "rank_ic_mean": -0.01, "top_bottom_spread_mean": -0.0002},
    ]

    explore, watchlist, candidates, graveyard = registry.build_candidate_and_graveyard(
        raw_results=raw_results,
        neutralized_results=neutralized_results,
        split_results=split_results,
        rolling_results=rolling_results,
        correlation_lookup={"alpha_core": [], "alpha_explore": [], "alpha_dead": []},
        metadata_lookup={
            "alpha_core": {"role": "alpha_seed"},
            "alpha_explore": {"role": "alpha_seed"},
            "alpha_dead": {"role": "alpha_seed"},
        },
    )

    assert [row["factor_name"] for row in candidates] == ["alpha_core"]
    assert [row["factor_name"] for row in explore] == ["alpha_explore"]
    assert [row["factor_name"] for row in graveyard] == ["alpha_dead"]
    assert watchlist == []
