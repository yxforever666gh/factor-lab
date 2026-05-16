from __future__ import annotations

from factor_lab.bucket_aware_coverage_preflight import evaluate_bucket_aware_preflight


def test_bucket_aware_preflight_blocks_missing_required_fields():
    result = evaluate_bucket_aware_preflight(
        {"required_data_fields": ["industry", "book_yield", "cashflow_quality"], "portfolio_construction": {"mode": "bucket_pair"}},
        available_fields={"industry", "book_yield"},
    )

    assert result["decision"] == "block"
    assert result["reasons"] == ["bucket_aware_coverage_preflight_failed"]
    assert result["missing_fields"] == ["cashflow_quality"]


def test_bucket_aware_preflight_holds_repeated_split_instability_from_route_policy():
    result = evaluate_bucket_aware_preflight(
        {"route_id": "value_momentum_confirmation", "required_data_fields": ["momentum_60_skip_5"], "portfolio_construction": {"mode": "bucket_pair"}},
        available_fields={"momentum_60_skip_5"},
        route_policy={"routes": {"value_momentum_confirmation": {"decision": "hold", "reason": "repeated_split_instability"}}},
    )

    assert result["decision"] == "hold"
    assert result["reasons"] == ["repeated_split_instability"]


def test_bucket_aware_preflight_allows_ready_bucket_pair():
    result = evaluate_bucket_aware_preflight(
        {"route_id": "value_quality_no_distress", "required_data_fields": ["industry", "book_yield"], "portfolio_construction": {"mode": "bucket_pair"}},
        available_fields={"industry", "book_yield"},
    )

    assert result["decision"] == "allow"
    assert result["reasons"] == []
