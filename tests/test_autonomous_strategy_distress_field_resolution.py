from __future__ import annotations

import json

from factor_lab.autonomous_strategy_distress_field_resolution import (
    build_quality_cashflow_distress_field_resolution,
    distress_field_resolution_to_markdown,
    write_distress_field_resolution,
)


def route_registry() -> dict:
    return {
        "routes": [
            {
                "route_id": "quality_cashflow_distress_filter",
                "route_status": "next_mechanism_candidate",
                "required_fields": [
                    "operating_cashflow_ttm",
                    "net_profit_ttm",
                    "debt_to_asset",
                    "interest_coverage",
                    "roe",
                ],
            }
        ]
    }


def test_distress_field_resolution_marks_pit_available_proxy_and_missing_fields():
    report = build_quality_cashflow_distress_field_resolution(
        run_id="x",
        route_registry=route_registry(),
        feature_fields={"roe", "debt_to_asset", "operating_cashflow_to_profit", "profit_yoy"},
        pit_fields={"debt_to_asset", "operating_cashflow_to_profit", "profit_yoy"},
    )

    statuses = {row["field"]: row["resolution_status"] for row in report["field_resolutions"]}
    assert statuses["debt_to_asset"] == "pit_available"
    assert statuses["roe"] == "available"
    assert statuses["operating_cashflow_ttm"] == "proxy_available_requires_review"
    assert statuses["net_profit_ttm"] == "proxy_available_requires_review"
    assert statuses["interest_coverage"] == "missing_external_or_derivation_required"
    assert report["ready_for_distress_screen"] is False
    assert report["queue_write_allowed"] is False
    assert report["controlled_execution_allowed"] is False


def test_distress_field_resolution_ready_only_when_all_required_are_pit_or_available():
    report = build_quality_cashflow_distress_field_resolution(
        run_id="x",
        route_registry=route_registry(),
        feature_fields={"roe"},
        pit_fields={"debt_to_asset", "operating_cashflow_ttm", "net_profit_ttm", "interest_coverage"},
    )

    assert report["ready_for_distress_screen"] is True
    assert report["decision"] == "prepare_distress_cheap_screen"


def test_distress_field_resolution_markdown_and_write(tmp_path):
    report = build_quality_cashflow_distress_field_resolution(
        run_id="x",
        route_registry=route_registry(),
        feature_fields={"roe"},
        pit_fields={"debt_to_asset"},
    )
    markdown = distress_field_resolution_to_markdown(report)
    assert "Quality Cashflow Distress Field Resolution" in markdown
    assert "interest_coverage" in markdown

    paths = write_distress_field_resolution(report, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
    payload = json.loads(paths["json"].read_text())
    assert payload["route_id"] == "quality_cashflow_distress_filter"
