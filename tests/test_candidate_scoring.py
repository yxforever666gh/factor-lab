from factor_lab.registry import FactorRegistry
from factor_lab.scoring import score_factors


def test_score_factors_exposes_dimension_scores_and_penalties():
    scored = score_factors(
        raw_results=[{
            "factor_name": "value_ep", "expression": "earnings_yield", "pass_gate": True, "rank_ic_mean": 0.04, "rank_ic_ir": 0.3
        }],
        neutralized_results=[{
            "factor_name": "value_ep", "expression": "earnings_yield", "pass_gate": False, "rank_ic_mean": -0.01
        }],
        split_results=[{"factor_name": "value_ep", "pass_gate": False}],
        rolling_results=[{
            "factor_name": "value_ep", "pass_gate": True, "rank_ic_mean": 0.03, "top_bottom_spread_mean": 0.002
        }],
        correlation_lookup={"value_ep": ["value_bp"]},
        metadata_lookup={"value_ep": {"role": "exposure_probe"}},
    )
    row = scored[0]
    assert "raw_score" in row
    assert "neutral_score" in row
    assert "rolling_score" in row
    assert row["correlation_penalty"] > 0
    assert row["style_exposure_penalty"] > 0


def test_exposure_probe_stays_off_candidate_even_when_scores_are_good(tmp_path):
    registry = FactorRegistry(tmp_path)
    raw_results = [{"factor_name": "value_ep", "expression": "earnings_yield", "pass_gate": True, "rank_ic_mean": 0.05, "rank_ic_ir": 0.6, "fail_reason": ""}]
    neutralized_results = [{"factor_name": "value_ep", "pass_gate": True, "rank_ic_mean": 0.03, "fail_reason": ""}]
    split_results = [{"factor_name": "value_ep", "pass_gate": True}, {"factor_name": "value_ep", "pass_gate": True}]
    rolling_results = [
        {"factor_name": "value_ep", "pass_gate": True, "rank_ic_mean": 0.03, "top_bottom_spread_mean": 0.002},
        {"factor_name": "value_ep", "pass_gate": True, "rank_ic_mean": 0.025, "top_bottom_spread_mean": 0.0015},
    ]
    score_lookup = {
        "value_ep": {
            "raw_score": 0.8,
            "neutral_score": 0.4,
            "rolling_score": 0.5,
            "turnover_penalty": 0.0,
            "correlation_penalty": 0.0,
            "style_exposure_penalty": 0.35,
        }
    }

    explore, watchlist, candidates, graveyard = registry.build_candidate_and_graveyard(
        raw_results=raw_results,
        neutralized_results=neutralized_results,
        split_results=split_results,
        rolling_results=rolling_results,
        correlation_lookup={"value_ep": []},
        metadata_lookup={"value_ep": {"role": "exposure_probe"}},
        score_lookup=score_lookup,
    )

    assert candidates == []
    assert explore == []
    assert graveyard == []
    assert [row["factor_name"] for row in watchlist] == ["value_ep"]
    assert watchlist[0]["style_exposure_penalty"] > 0


def test_family_probe_gets_style_penalty_but_can_still_stay_in_watchlist(tmp_path):
    registry = FactorRegistry(tmp_path)
    raw_results = [{"factor_name": "mom_60_skip_5", "expression": "momentum_60_skip_5", "pass_gate": True, "rank_ic_mean": 0.03, "rank_ic_ir": 0.4, "fail_reason": ""}]
    neutralized_results = [{"factor_name": "mom_60_skip_5", "pass_gate": False, "rank_ic_mean": 0.0, "fail_reason": "neutral_fail"}]
    split_results = [{"factor_name": "mom_60_skip_5", "pass_gate": True}]
    rolling_results = [{"factor_name": "mom_60_skip_5", "pass_gate": True, "rank_ic_mean": 0.02, "top_bottom_spread_mean": 0.001}]
    score_lookup = {"mom_60_skip_5": {"raw_score": 0.5, "neutral_score": -0.1, "rolling_score": 0.3, "turnover_penalty": 0.0, "correlation_penalty": 0.0, "style_exposure_penalty": 0.1}}
    explore, watchlist, candidates, graveyard = registry.build_candidate_and_graveyard(
        raw_results=raw_results, neutralized_results=neutralized_results, split_results=split_results, rolling_results=rolling_results,
        correlation_lookup={"mom_60_skip_5": []}, metadata_lookup={"mom_60_skip_5": {"role": "family_probe"}}, score_lookup=score_lookup
    )
    assert candidates == []
    assert explore == []
    assert graveyard == []
    assert [row["factor_name"] for row in watchlist] == ["mom_60_skip_5"]
