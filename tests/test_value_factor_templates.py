from factor_lab.value_factor_templates import load_value_research_routes, build_value_route_candidates


def test_value_research_routes_include_required_core_routes():
    routes = load_value_research_routes()
    route_ids = {route["route_id"] for route in routes}

    assert {"industry_relative_value", "value_quality_no_distress", "value_momentum_confirmation"}.issubset(route_ids)
    for route in routes:
        assert route["hypothesis"]
        assert route["required_data_fields"]
        assert route["expected_horizons"]
        assert route["falsification_criteria"]
        assert route["mechanism_id"]


def test_build_value_route_candidates_blocks_missing_fields_and_emits_ready_candidates():
    candidates = build_value_route_candidates(
        available_fields={"industry", "book_yield", "earnings_yield", "roe", "momentum_60_skip_5", "pb", "pe_ttm"}
    )

    ready = [row for row in candidates if row["status"] == "ready"]
    blocked = [row for row in candidates if row["status"] == "blocked_missing_fields"]

    assert any(row["route_id"] == "industry_relative_value" for row in ready)
    assert any(row["route_id"] == "value_quality_no_distress" for row in ready)
    assert any(row["route_id"] == "value_momentum_confirmation" for row in ready)
    assert any(row["route_id"] == "historical_valuation_percentile" for row in blocked)
    assert all(row["mechanism_id"] and row["hypothesis"] for row in ready)
    assert all("missing_fields" in row for row in blocked)
