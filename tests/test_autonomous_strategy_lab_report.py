from __future__ import annotations

import json

from factor_lab.autonomous_strategy_lab_report import (
    autonomous_strategy_lab_report_to_markdown,
    build_autonomous_strategy_lab_report,
    write_autonomous_strategy_lab_report,
)


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_build_autonomous_strategy_lab_report_collects_artifact_status(tmp_path):
    _write_json(tmp_path / "configs/autonomous_strategy_routes.json", {
        "routes": [
            {"route_id": "historical_relative_valuation_repair", "route_status": "cheap_screen_candidate", "recommended_next_step": "request_data"}
        ]
    })
    base = tmp_path / "artifacts/autonomous_strategy_lab"
    _write_json(base / "field_derivation_specs.json", {"derived_fields": [{"field": "pb_history_756d"}]})
    _write_json(base / "cheap_screen_plan.json", {"task_count": 1, "blocked_actions": ["queue_write"]})
    _write_json(base / "historical_valuation_coverage_preflight.json", {
        "overall_status": "blocked",
        "field_coverage": [{"derived_field": "pb_history_756d", "status": "insufficient_history", "eligible_ticker_count": 50, "ticker_count": 97, "eligible_ticker_ratio": 0.515464}],
        "next_allowed_actions": ["request_data_or_extend_cache"],
        "blocked_actions": ["full_backtest"],
    })
    _write_json(base / "controlled_execution_decision.json", {
        "execution_status": "blocked",
        "reason_codes": ["coverage_preflight_blocked"],
        "controlled_execution_started": False,
        "controlled_execution_allowed": False,
        "blocked_actions": ["timer_enable"],
    })

    report = build_autonomous_strategy_lab_report(tmp_path)

    assert report["status"] == "blocked"
    assert report["decision"] == "request_data"
    assert report["derived_field_count"] == 1
    assert report["cheap_screen_task_count"] == 1
    assert report["controlled_execution_started"] is False
    assert report["queue_write_allowed"] is False
    assert "coverage_preflight_blocked" in report["execution_reason_codes"]
    assert "request_data_or_extend_cache" in report["allowed_actions"]
    assert "full_backtest" in report["blocked_actions"]


def test_autonomous_strategy_lab_report_markdown_and_write(tmp_path):
    report = {
        "status": "blocked",
        "decision": "request_data",
        "coverage_overall_status": "blocked",
        "execution_status": "blocked",
        "controlled_execution_started": False,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "execution_reason_codes": ["coverage_preflight_blocked"],
        "coverage_field_summary": [],
        "route_statuses": [],
        "allowed_actions": ["request_data_or_extend_cache"],
        "blocked_actions": ["full_backtest"],
    }
    markdown = autonomous_strategy_lab_report_to_markdown(report)
    assert "Autonomous Strategy Lab Status" in markdown
    assert "decision: request_data" in markdown
    assert "controlled_execution_started: False" in markdown

    paths = write_autonomous_strategy_lab_report(report, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
    payload = json.loads(paths["json"].read_text())
    assert payload["decision"] == "request_data"
