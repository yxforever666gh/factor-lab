from __future__ import annotations

import json

from factor_lab.autonomous_strategy_data_request_report import (
    build_data_request_report,
    data_request_report_to_markdown,
    write_data_request_report,
)


def route_registry() -> dict:
    return {
        "schema_version": 1,
        "queue_write_allowed": False,
        "controlled_execution_allowed": False,
        "routes": [
            {
                "route_id": "historical_relative_valuation_repair",
                "route_status": "blocked_missing_fields",
                "missing_fields": ["pb_history_756d", "pe_ttm_history_756d"],
                "blocked_fields": [],
                "required_fields": ["date", "ticker", "pb_history_756d"],
                "recommended_next_step": "request_data",
            },
            {
                "route_id": "quality_cashflow_distress_filter",
                "route_status": "blocked_missing_fields",
                "missing_fields": ["operating_cashflow_ttm"],
                "blocked_fields": ["debt_to_asset"],
                "required_fields": ["debt_to_asset", "operating_cashflow_ttm"],
                "recommended_next_step": "request_data",
            },
        ],
    }


def worker_verdict() -> dict:
    return {
        "schema_version": 1,
        "run_id": "worker_preview_test",
        "consensus_decision": "request_data",
        "valid_response_count": 4,
        "reason_codes": ["worker_consensus_request_data", "drawdown_blocker_no_safe_candidate"],
        "requested_actions": ["write_blocker_report", "draft_new_mechanism_or_data_request"],
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
    }


def test_build_data_request_report_summarizes_missing_and_blocked_fields():
    report = build_data_request_report(
        run_id="worker_preview_test",
        worker_verdict=worker_verdict(),
        route_registry=route_registry(),
    )

    assert report["schema_version"] == 1
    assert report["decision"] == "request_data"
    assert report["controlled_execution_allowed"] is False
    assert report["queue_write_allowed"] is False
    assert report["field_requests"] == [
        {"field": "debt_to_asset", "request_type": "blocked_field_provider_support", "routes": ["quality_cashflow_distress_filter"]},
        {"field": "operating_cashflow_ttm", "request_type": "missing_field", "routes": ["quality_cashflow_distress_filter"]},
        {"field": "pb_history_756d", "request_type": "missing_field", "routes": ["historical_relative_valuation_repair"]},
        {"field": "pe_ttm_history_756d", "request_type": "missing_field", "routes": ["historical_relative_valuation_repair"]},
    ]
    assert report["next_allowed_actions"] == [
        "write_blocker_report",
        "draft_new_mechanism_or_data_request",
        "resolve_field_availability",
        "rerun_route_registry_after_data_update",
    ]


def test_data_request_report_markdown_and_write(tmp_path):
    report = build_data_request_report(
        run_id="worker_preview_test",
        worker_verdict=worker_verdict(),
        route_registry=route_registry(),
    )

    markdown = data_request_report_to_markdown(report)
    assert "Autonomous Strategy Data Request Report" in markdown
    assert "decision: request_data" in markdown
    assert "pb_history_756d" in markdown
    assert "controlled_execution_allowed: False" in markdown

    paths = write_data_request_report(report, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
    payload = json.loads(paths["json"].read_text())
    assert payload["decision"] == "request_data"
