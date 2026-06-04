from __future__ import annotations

import json

from factor_lab.autonomous_strategy_execution_adapter import (
    build_controlled_execution_decision,
    execution_decision_to_markdown,
    write_execution_decision,
)


def passing_cheap_plan() -> dict:
    return {
        "mode": "preview_only",
        "task_count": 1,
        "controlled_execution_allowed": True,
        "queue_write_allowed": False,
        "cheap_screen_tasks": [
            {"route_id": "historical_relative_valuation_repair", "execution_status": "not_executed"}
        ],
    }


def passing_preflight() -> dict:
    return {
        "mode": "preflight_only",
        "route_id": "historical_relative_valuation_repair",
        "overall_status": "pass",
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
    }


def test_execution_adapter_blocks_without_explicit_allow_flag():
    decision = build_controlled_execution_decision(
        run_id="x",
        cheap_screen_plan=passing_cheap_plan(),
        coverage_preflight=passing_preflight(),
        allow_controlled_execution=False,
        max_backtests_requested=1,
        policy_cap=1,
    )

    assert decision["execution_status"] == "blocked"
    assert "missing_allow_controlled_execution_flag" in decision["reason_codes"]
    assert decision["controlled_execution_started"] is False
    assert decision["queue_write_allowed"] is False
    assert decision["timer_enable_allowed"] is False


def test_execution_adapter_blocks_when_coverage_preflight_is_blocked():
    preflight = passing_preflight() | {"overall_status": "blocked"}

    decision = build_controlled_execution_decision(
        run_id="x",
        cheap_screen_plan=passing_cheap_plan(),
        coverage_preflight=preflight,
        allow_controlled_execution=True,
        max_backtests_requested=1,
        policy_cap=1,
    )

    assert decision["execution_status"] == "blocked"
    assert "coverage_preflight_blocked" in decision["reason_codes"]
    assert decision["controlled_execution_started"] is False


def test_execution_adapter_blocks_when_request_exceeds_policy_cap():
    decision = build_controlled_execution_decision(
        run_id="x",
        cheap_screen_plan=passing_cheap_plan(),
        coverage_preflight=passing_preflight(),
        allow_controlled_execution=True,
        max_backtests_requested=2,
        policy_cap=1,
    )

    assert decision["execution_status"] == "blocked"
    assert "max_backtests_exceeds_policy_cap" in decision["reason_codes"]
    assert decision["max_backtests_allowed"] == 0


def test_execution_adapter_can_prepare_single_controlled_preview_only_when_all_gates_pass():
    decision = build_controlled_execution_decision(
        run_id="x",
        cheap_screen_plan=passing_cheap_plan(),
        coverage_preflight=passing_preflight(),
        allow_controlled_execution=True,
        max_backtests_requested=1,
        policy_cap=1,
    )

    assert decision["execution_status"] == "ready_for_manual_controlled_execution"
    assert decision["controlled_execution_started"] is False
    assert decision["max_backtests_allowed"] == 1
    assert decision["reason_codes"] == ["manual_controlled_execution_ready"]


def test_execution_adapter_markdown_and_write(tmp_path):
    decision = build_controlled_execution_decision(
        run_id="x",
        cheap_screen_plan=passing_cheap_plan(),
        coverage_preflight=passing_preflight(),
        allow_controlled_execution=False,
    )
    markdown = execution_decision_to_markdown(decision)
    assert "Autonomous Strategy Controlled Execution Decision" in markdown
    assert "controlled_execution_started: False" in markdown

    paths = write_execution_decision(decision, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
    payload = json.loads(paths["json"].read_text())
    assert payload["execution_status"] == "blocked"
