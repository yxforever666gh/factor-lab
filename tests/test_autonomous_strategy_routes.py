from __future__ import annotations

import json
from pathlib import Path

from factor_lab.autonomous_strategy_routes import (
    build_route_registry_from_worker_response,
    route_registry_to_markdown,
    write_route_registry,
)


def worker_response() -> dict:
    return {
        "schema_version": 1,
        "worker_key": "factor_lab_mechanism_researcher",
        "decision_recommendation": "request_data",
        "route_proposals": [
            {
                "route_id": "historical_relative_valuation_repair",
                "mechanism_id": "historical_relative_valuation_repair",
                "economic_mechanism": "Historical cheapness route.",
                "required_fields": ["date", "ticker", "industry", "pb", "pb_history_756d"],
                "known_missing_or_blocked_fields": ["pb_history_756d"],
                "cheap_screens": ["coverage_preflight"],
                "falsification_criteria": ["coverage below 60%"],
                "recommended_next_step": "request_data",
            },
            {
                "route_id": "current_roe_quality_screen",
                "mechanism_id": "current_roe_quality_screen",
                "economic_mechanism": "Only currently available fields.",
                "required_fields": ["date", "ticker", "industry", "roe", "forward_return_5d"],
                "known_missing_or_blocked_fields": [],
                "cheap_screens": ["coverage_preflight"],
                "falsification_criteria": ["roe coverage too low"],
                "recommended_next_step": "cheap_screen_only_after_review",
            },
        ],
    }


def test_route_registry_blocks_missing_fields_and_keeps_execution_disabled():
    registry = build_route_registry_from_worker_response(
        worker_response(),
        available_fields={"date", "ticker", "industry", "pb", "roe", "forward_return_5d"},
        blocked_fields=set(),
        source_path="worker.json",
    )

    routes = {route["route_id"]: route for route in registry["routes"]}
    blocked = routes["historical_relative_valuation_repair"]
    ready = routes["current_roe_quality_screen"]

    assert registry["schema_version"] == 1
    assert registry["source_path"] == "worker.json"
    assert registry["queue_write_allowed"] is False
    assert registry["controlled_execution_allowed"] is False
    assert blocked["route_status"] == "blocked_missing_fields"
    assert blocked["missing_fields"] == ["pb_history_756d"]
    assert blocked["max_backtests_before_review"] == 0
    assert ready["route_status"] == "cheap_screen_candidate"
    assert ready["max_backtests_before_review"] == 0
    assert ready["requires_manual_review"] is True


def test_route_registry_treats_schema_blocked_fields_as_blockers():
    payload = worker_response()
    payload["route_proposals"][1]["required_fields"].append("debt_to_asset")

    registry = build_route_registry_from_worker_response(
        payload,
        available_fields={"date", "ticker", "industry", "roe", "forward_return_5d", "debt_to_asset"},
        blocked_fields={"debt_to_asset"},
    )

    route = next(row for row in registry["routes"] if row["route_id"] == "current_roe_quality_screen")
    assert route["route_status"] == "blocked_missing_fields"
    assert route["blocked_fields"] == ["debt_to_asset"]


def test_write_route_registry_outputs_json_and_markdown(tmp_path):
    registry = build_route_registry_from_worker_response(
        worker_response(),
        available_fields={"date", "ticker", "industry", "pb", "roe", "forward_return_5d"},
        blocked_fields=set(),
    )

    markdown = route_registry_to_markdown(registry)
    assert "Autonomous Strategy Route Registry" in markdown
    assert "historical_relative_valuation_repair" in markdown

    paths = write_route_registry(registry, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
    payload = json.loads(paths["json"].read_text())
    assert payload["routes"][0]["route_id"] == "historical_relative_valuation_repair"
