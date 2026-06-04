from __future__ import annotations

import json

from factor_lab.autonomous_strategy_field_derivations import (
    build_field_derivation_specs,
    field_derivation_specs_to_markdown,
    write_field_derivation_specs,
)


def field_resolution_report() -> dict:
    return {
        "schema_version": 1,
        "run_id": "x",
        "field_resolutions": [
            {"field": "pb_history_756d", "resolution_status": "derivable_from_available_history", "source_field": "pb", "derivation": "rolling_history_window:756d", "routes": ["historical_relative_valuation_repair"]},
            {"field": "forecast_eps", "resolution_status": "external_data_required", "source_field": None, "routes": ["earnings_revision_valuation_repair"]},
        ],
    }


def test_build_field_derivation_specs_only_includes_derivable_fields():
    specs = build_field_derivation_specs(field_resolution_report())

    assert specs["schema_version"] == 1
    assert specs["run_id"] == "x"
    assert specs["queue_write_allowed"] is False
    assert specs["controlled_execution_allowed"] is False
    assert specs["derived_fields"] == [
        {
            "field": "pb_history_756d",
            "source_field": "pb",
            "derivation": "rolling_history_window:756d",
            "routes": ["historical_relative_valuation_repair"],
            "implementation_status": "spec_only_not_materialized",
        }
    ]


def test_field_derivation_specs_markdown_and_write(tmp_path):
    specs = build_field_derivation_specs(field_resolution_report())

    markdown = field_derivation_specs_to_markdown(specs)
    assert "Autonomous Strategy Field Derivation Specs" in markdown
    assert "pb_history_756d" in markdown
    assert "spec_only_not_materialized" in markdown

    paths = write_field_derivation_specs(specs, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
    payload = json.loads(paths["json"].read_text())
    assert payload["derived_fields"][0]["field"] == "pb_history_756d"
