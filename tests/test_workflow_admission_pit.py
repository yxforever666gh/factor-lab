import json

from factor_lab.workflow_admission_adapter import enforce_workflow_admission


def test_pit_bucket_aware_workflow_blocks_closed_cashflow_value_trap_config(tmp_path):
    cfg = {
        "route_id": "value_trap_filter_quality_confirmation",
        "mechanism_id": "value_trap_filter_quality_confirmation",
        "required_data_fields": ["industry_relative_book_yield", "operating_cashflow_to_profit", "debt_to_asset", "profit_yoy"],
        "required_pit_features": ["operating_cashflow_to_profit", "debt_to_asset", "profit_yoy"],
        "pit_requirements": {"require_ann_date_asof": True, "forbid_end_date_only": True},
        "factors": [
            {
                "name": "pit_value_trap_quality",
                "expression": "industry_relative_book_yield + operating_cashflow_to_profit - debt_to_asset + profit_yoy",
            }
        ],
        "portfolio_construction": {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 0},
    }
    path = tmp_path / "pit_value_trap.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")

    result = enforce_workflow_admission({"task_type": "workflow", "payload": {"config_path": str(path), "output_dir": str(tmp_path / "out")}})

    assert result["decision"] == "block"
    assert "cashflow_fields_monitor_only" in result["reasons"]


def test_pit_bucket_aware_workflow_can_reopen_cashflow_with_explicit_plan(tmp_path):
    cfg = {
        "route_id": "value_trap_filter_quality_confirmation",
        "mechanism_id": "value_trap_filter_quality_confirmation",
        "required_data_fields": ["industry_relative_book_yield", "operating_cashflow_to_profit", "debt_to_asset", "profit_yoy"],
        "required_pit_features": ["operating_cashflow_to_profit", "debt_to_asset", "profit_yoy"],
        "pit_requirements": {"require_ann_date_asof": True, "forbid_end_date_only": True},
        "portfolio_construction": {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 0},
        "governance": {"cashflow_reopen_plan_id": "unit-test-explicit-plan"},
        "factors": [
            {
                "name": "pit_value_trap_quality",
                "expression": "industry_relative_book_yield + operating_cashflow_to_profit - debt_to_asset + profit_yoy",
            }
        ],
    }
    path = tmp_path / "pit_value_trap_reopen.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")

    result = enforce_workflow_admission({"task_type": "workflow", "payload": {"config_path": str(path), "output_dir": str(tmp_path / "out")}})

    assert result["decision"] == "allow"
    assert result["reasons"] == []


def test_non_pit_workflow_still_blocks_unpopulated_financial_fields(tmp_path):
    cfg = {
        "route_id": "value_quality_no_distress",
        "mechanism_id": "value_quality_no_distress",
        "required_data_fields": ["industry_relative_book_yield", "operating_cashflow_to_profit"],
        "factors": [{"name": "unsafe", "expression": "industry_relative_book_yield + operating_cashflow_to_profit"}],
        "portfolio_construction": {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 0},
    }
    path = tmp_path / "unsafe.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")

    result = enforce_workflow_admission({"task_type": "workflow", "payload": {"config_path": str(path), "output_dir": str(tmp_path / "out")}})

    assert result["decision"] == "block"
    assert "cashflow_fields_monitor_only" in result["reasons"]
