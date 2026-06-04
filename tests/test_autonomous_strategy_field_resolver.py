from __future__ import annotations

import json

from factor_lab.autonomous_strategy_field_resolver import (
    build_field_resolution_report,
    field_resolution_report_to_markdown,
    write_field_resolution_report,
)


def data_request_report() -> dict:
    return {
        "schema_version": 1,
        "decision": "request_data",
        "field_requests": [
            {"field": "debt_to_asset", "request_type": "blocked_field_provider_support", "routes": ["quality_cashflow_distress_filter"]},
            {"field": "pb_history_756d", "request_type": "missing_field", "routes": ["historical_relative_valuation_repair"]},
            {"field": "forecast_eps", "request_type": "missing_field", "routes": ["earnings_revision_valuation_repair"]},
            {"field": "quality_roe", "request_type": "missing_field", "routes": ["legacy_quality_route"]},
        ],
    }


def test_field_resolution_classifies_blocked_derivable_external_and_alias():
    report = build_field_resolution_report(
        run_id="x",
        data_request_report=data_request_report(),
        schema_fields={"pb", "roe", "debt_to_asset"},
        available_fields={"pb", "roe"},
        blocked_fields={"debt_to_asset"},
        aliases={"quality_roe": "roe"},
    )

    by_field = {row["field"]: row for row in report["field_resolutions"]}
    assert by_field["debt_to_asset"]["resolution_status"] == "blocked_provider_support_required"
    assert by_field["pb_history_756d"]["resolution_status"] == "derivable_from_available_history"
    assert by_field["pb_history_756d"]["source_field"] == "pb"
    assert by_field["forecast_eps"]["resolution_status"] == "external_data_required"
    assert by_field["quality_roe"]["resolution_status"] == "alias_available"
    assert by_field["quality_roe"]["source_field"] == "roe"
    assert report["ready_for_route_registry_rerun"] is False
    assert report["controlled_execution_allowed"] is False


def test_field_resolution_markdown_and_write(tmp_path):
    report = build_field_resolution_report(
        run_id="x",
        data_request_report=data_request_report(),
        schema_fields={"pb", "roe", "debt_to_asset"},
        available_fields={"pb", "roe"},
        blocked_fields={"debt_to_asset"},
        aliases={"quality_roe": "roe"},
    )

    markdown = field_resolution_report_to_markdown(report)
    assert "Autonomous Strategy Field Resolution Report" in markdown
    assert "pb_history_756d" in markdown
    assert "external_data_required" in markdown

    paths = write_field_resolution_report(report, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
    payload = json.loads(paths["json"].read_text())
    assert payload["field_resolutions"][0]["field"] == "debt_to_asset"
