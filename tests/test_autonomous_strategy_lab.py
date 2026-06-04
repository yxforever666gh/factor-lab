from __future__ import annotations

import json

from factor_lab.autonomous_strategy_lab import (
    build_autonomous_strategy_report,
    autonomous_strategy_report_to_markdown,
    diagnose_failure_modes,
    score_strategy_routes,
    choose_strategy_decision,
    write_autonomous_strategy_report,
)


def sample_blocked_evidence() -> dict:
    return {
        "db": {
            "candidate_status": {"testing": 51, "fragile": 42, "rejected": 26},
            "research_tasks_by_status": {"pending": 4, "finished": 87144},
        },
        "risk": {
            "status": "blocked_no_drawdown_safe_candidate",
            "drawdown_limit": -0.35,
            "best_available_max_drawdown": -0.475431,
            "candidate_count": 0,
            "best_sharpe": 0.198329,
        },
        "harvest_v3_status": {
            "cycles": [
                {"cycle_id": "cycle_0058", "oos_class": "insufficient_data", "repeated_blockers": ["no_ok_rows"]},
                {"cycle_id": "cycle_0059", "oos_class": "insufficient_data", "repeated_blockers": ["no_ok_rows"]},
                {"cycle_id": "cycle_0060", "oos_class": "insufficient_data", "repeated_blockers": ["no_ok_rows"]},
            ]
        },
        "controlled_dry_run": {"pending_count": 4, "would_run_count": 0, "blocked_count": 0},
        "runtime_audit": {"recommendations": ["pause_broad_daemon", "allow_controlled_only_daemon"]},
    }


def test_diagnosis_blocks_same_route_when_drawdown_and_data_are_bad():
    diagnosis = diagnose_failure_modes(sample_blocked_evidence())

    assert diagnosis["severity"] == "blocker"
    assert "drawdown_blocker_no_safe_candidate" in diagnosis["reason_codes"]
    assert "best_drawdown_worse_than_limit" in diagnosis["reason_codes"]
    assert "repeated_insufficient_data" in diagnosis["reason_codes"]
    assert "same_route_full_backtest_batch" in diagnosis["blocked_actions"]
    assert "draft_new_mechanism_or_data_request" in diagnosis["allowed_actions"]


def test_route_scoring_prefers_new_mechanism_over_same_route_under_blocker():
    evidence = sample_blocked_evidence()
    diagnosis = diagnose_failure_modes(evidence)

    scores = score_strategy_routes(evidence, diagnosis)

    assert scores[0]["route_id"] == "new_mechanism_or_data_request"
    same_route = next(route for route in scores if route["route_id"] == "continue_industry_relative_value")
    assert same_route["score"] < 0
    assert "same_route_drawdown_blocked" in same_route["penalties"]


def test_decision_requests_data_when_drawdown_blocker_and_insufficient_data_repeat():
    evidence = sample_blocked_evidence()
    diagnosis = diagnose_failure_modes(evidence)
    scores = score_strategy_routes(evidence, diagnosis)

    decision, next_plan = choose_strategy_decision(diagnosis, scores)

    assert decision == "request_data"
    assert next_plan["selected_route_id"] == "new_mechanism_or_data_request"
    assert next_plan["max_backtests_before_review"] == 0
    assert next_plan["requires_human_review"] is True
    assert "queue_write" in next_plan["blocked_next_actions"]


def test_report_preserves_conservative_safety_flags_and_markdown_surfaces_decision():
    report = build_autonomous_strategy_report(
        run_id="strategy_lab_test",
        evidence_summary=sample_blocked_evidence(),
        created_at_utc="2026-06-01T00:00:00+00:00",
    )

    assert report["decision"] == "request_data"
    assert report["safety"]["queue_write_allowed"] is False
    assert report["safety"]["automation_allowed"] is False
    assert report["safety"]["live_trading_enabled"] is False

    markdown = autonomous_strategy_report_to_markdown(report)
    assert "Autonomous Strategy Lab Dry Run" in markdown
    assert "decision: request_data" in markdown
    assert "drawdown_blocker_no_safe_candidate" in markdown


def test_write_report_creates_latest_and_run_artifacts(tmp_path):
    report = build_autonomous_strategy_report(
        run_id="strategy_lab_test",
        evidence_summary=sample_blocked_evidence(),
        created_at_utc="2026-06-01T00:00:00+00:00",
    )

    paths = write_autonomous_strategy_report(report, tmp_path)

    assert paths["latest_json"].exists()
    assert paths["latest_markdown"].exists()
    assert paths["run_json"].exists()
    assert paths["run_markdown"].exists()
    payload = json.loads(paths["latest_json"].read_text())
    assert payload["run_id"] == "strategy_lab_test"
    assert payload["decision"] == "request_data"
