from __future__ import annotations

import json

from factor_lab.autonomous_strategy_cheap_screen_planner import (
    build_cheap_screen_plan,
    cheap_screen_plan_to_markdown,
    write_cheap_screen_plan,
)


def route_registry() -> dict:
    return {
        "schema_version": 1,
        "routes": [
            {
                "route_id": "historical_relative_valuation_repair",
                "mechanism_id": "historical_relative_valuation_repair",
                "route_status": "cheap_screen_candidate",
                "required_fields": ["pb_history_756d", "pe_ttm_history_756d", "forward_return_5d"],
                "cheap_screens": ["coverage_preflight", "information_screen", "risk_screen"],
                "falsification_criteria": ["coverage below 60%"],
            },
            {
                "route_id": "earnings_revision_valuation_repair",
                "route_status": "blocked_missing_fields",
                "required_fields": ["forecast_eps"],
                "cheap_screens": ["coverage_preflight"],
                "falsification_criteria": [],
            },
        ],
    }


def test_build_cheap_screen_plan_only_includes_candidates_and_keeps_execution_disabled():
    plan = build_cheap_screen_plan(run_id="x", route_registry=route_registry())

    assert plan["schema_version"] == 1
    assert plan["run_id"] == "x"
    assert plan["mode"] == "preview_only"
    assert plan["queue_write_allowed"] is False
    assert plan["controlled_execution_allowed"] is False
    assert plan["max_backtests_before_review"] == 0
    assert len(plan["cheap_screen_tasks"]) == 1
    task = plan["cheap_screen_tasks"][0]
    assert task["route_id"] == "historical_relative_valuation_repair"
    assert task["execution_status"] == "not_executed"
    assert task["requires_manual_review"] is True
    assert "coverage_preflight" in task["cheap_screens"]


def test_cheap_screen_plan_markdown_and_write(tmp_path):
    plan = build_cheap_screen_plan(run_id="x", route_registry=route_registry())

    markdown = cheap_screen_plan_to_markdown(plan)
    assert "Autonomous Strategy Cheap Screen Plan" in markdown
    assert "historical_relative_valuation_repair" in markdown
    assert "controlled_execution_allowed: False" in markdown

    paths = write_cheap_screen_plan(plan, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
    payload = json.loads(paths["json"].read_text())
    assert payload["cheap_screen_tasks"][0]["route_id"] == "historical_relative_valuation_repair"
