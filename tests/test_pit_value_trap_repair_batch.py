from __future__ import annotations

from factor_lab.pit_value_trap_repair_batch import build_repair_configs


def test_repair_batch_generates_at_most_three_standardized_configs() -> None:
    decision = {
        "fields": [
            {"field": "debt_to_assets", "action": "reverse", "eligible_for_repaired_combo": True},
            {"field": "netprofit_yoy", "action": "reverse", "eligible_for_repaired_combo": True},
            {"field": "tr_yoy", "action": "monitor_only", "eligible_for_repaired_combo": False},
            {"field": "operating_cashflow_to_profit", "action": "monitor_only", "eligible_for_repaired_combo": False},
        ]
    }

    manifest = build_repair_configs(decision)

    assert manifest["decision"] == "generated_repaired_configs"
    assert len(manifest["configs"]) <= 3
    first = manifest["configs"][0]
    assert first["standardized_pit_features"] is True
    assert "low_debt_to_assets_zscore_by_date_industry" in first["factors"][0]["expression"]
    assert "operating_cashflow_to_profit" not in first["factors"][0]["expression"]


def test_repair_batch_generates_zero_when_no_fields_clear() -> None:
    manifest = build_repair_configs({"fields": [{"field": "operating_cashflow_to_profit", "action": "monitor_only", "eligible_for_repaired_combo": False}]})
    assert manifest["decision"] == "no_configs_generated_no_fields_clear_gates"
    assert manifest["configs"] == []
