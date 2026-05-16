from __future__ import annotations

from factor_lab.pit_field_decision import build_field_decision


def test_field_decision_stops_low_coverage_cashflow() -> None:
    cashflow = {"coverage": 0.22}
    transforms = {
        "fields": [
            {"field": "operating_cashflow_to_profit", "variants": [{"variant": "reversed", "coverage": 0.22, "rank_ic_mean": 0.02}]},
            {"field": "debt_to_assets", "variants": [{"variant": "reversed_winsorized_zscore", "coverage": 0.9, "rank_ic_mean": 0.03}]},
        ]
    }
    missing = {
        "fields": [
            {"field": "operating_cashflow_to_profit", "fragility": "stable_or_consistently_weak"},
            {"field": "debt_to_assets", "fragility": "stable_or_consistently_weak"},
        ]
    }

    decision = build_field_decision(cashflow=cashflow, transforms=transforms, missing=missing, fields=["operating_cashflow_to_profit", "debt_to_assets"])

    by_field = {row["field"]: row for row in decision["fields"]}
    assert by_field["operating_cashflow_to_profit"]["action"] == "monitor_only"
    assert by_field["operating_cashflow_to_profit"]["eligible_for_repaired_combo"] is False
    assert by_field["debt_to_assets"]["action"] == "reverse"
    assert by_field["debt_to_assets"]["eligible_for_repaired_combo"] is True
    assert decision["decision"] == "allow_repaired_config_generation"


def test_field_decision_stops_when_no_fields_clear() -> None:
    cashflow = {"coverage": 0.22}
    transforms = {"fields": [{"field": "operating_cashflow_to_profit", "variants": [{"variant": "reversed", "coverage": 0.22, "rank_ic_mean": 0.02}]}]}
    missing = {"fields": [{"field": "operating_cashflow_to_profit", "fragility": "stable_or_consistently_weak"}]}

    decision = build_field_decision(cashflow=cashflow, transforms=transforms, missing=missing, fields=["operating_cashflow_to_profit"])

    assert decision["decision"] == "stop_value_trap_repair_no_fields_clear_gates"
