import pytest

from factor_lab.harvest_research_proposal import validate_harvest_research_proposal


def valid_proposal():
    return {
        "proposal_id": "value_quality_cost_sensitivity_v1",
        "mechanism_id": "value_quality_no_distress",
        "hypothesis": "Quality filtered value survives realistic costs.",
        "required_fields": ["earnings_yield", "roe", "pb", "turnover", "return_1d"],
        "derived_fields": ["bucket_pair_spread", "cost_adjusted_return"],
        "experiment_type": "controlled_backtest",
        "expected_information_gain": "Tests cost sensitivity of a known bucket-aware route.",
        "falsification_criteria": ["net spread turns negative"],
        "duplicate_rationale": "Stricter cost follow-up is not duplicate-equivalent.",
    }


def test_valid_proposal_passes():
    result = validate_harvest_research_proposal(valid_proposal())
    assert result["valid"] is True
    assert result["reasons"] == []


def test_missing_required_machine_checkable_fields_fails():
    p = valid_proposal()
    del p["mechanism_id"]
    p["falsification_criteria"] = []
    result = validate_harvest_research_proposal(p)
    assert result["valid"] is False
    assert "missing_mechanism_id" in result["reasons"]
    assert "missing_falsification_criteria" in result["reasons"]


def test_live_trading_request_is_invalid():
    p = valid_proposal()
    p["experiment_type"] = "live_trading"
    result = validate_harvest_research_proposal(p)
    assert result["valid"] is False
    assert "live_trading_requested" in result["reasons"]


def test_legacy_broad_path_is_invalid():
    p = valid_proposal()
    p["output_path"] = "artifacts/generated/broad_search/run_1"
    result = validate_harvest_research_proposal(p)
    assert "legacy_broad_path_requested" in result["reasons"]
