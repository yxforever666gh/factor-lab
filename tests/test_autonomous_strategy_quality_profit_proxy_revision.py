from factor_lab.autonomous_strategy_quality_profit_proxy_revision import build_quality_profit_proxy_revision


def blocked_resolution():
    return {
        "mechanism_id": "quality_cashflow_value_repair_v1",
        "decision": "request_data",
        "ready_for_cheap_screen": False,
        "missing_fields": ["gross_margin", "current_ratio", "quick_ratio", "interest_coverage"],
        "proxy_blocked_fields": ["ocfps", "operating_cashflow_yoy"],
    }


def test_proxy_revision_created_from_blocked_quality_cashflow_resolution():
    revision = build_quality_profit_proxy_revision(run_id="r", field_resolution=blocked_resolution())
    assert revision["mechanism_id"] == "quality_profit_proxy_value_repair_v1"
    assert revision["decision"] == "revise_to_proxy_mechanism"
    assert revision["revision_status"] == "ready_for_proxy_field_resolution"
    assert revision["recommended_next_step"] == "run_quality_profit_proxy_field_resolution"
    assert revision["controlled_execution_allowed"] is False
    assert revision["queue_write_allowed"] is False
    assert "operating_cashflow_to_profit" in revision["proxy_required_fields"]
    assert "controlled_backtest" in revision["blocked_actions"]
    assert "proxy_field_resolution" in revision["next_allowed_actions"]


def test_proxy_revision_blocks_when_source_resolution_not_blocked():
    revision = build_quality_profit_proxy_revision(
        run_id="r",
        field_resolution={
            "mechanism_id": "quality_cashflow_value_repair_v1",
            "decision": "prepare_quality_cashflow_cheap_screen",
            "ready_for_cheap_screen": True,
            "missing_fields": [],
            "proxy_blocked_fields": [],
        },
    )
    assert revision["decision"] == "blocked"
    assert revision["revision_status"] == "blocked_until_quality_cashflow_resolution_confirms_request_data"
    assert revision["recommended_next_step"] == "inspect_quality_cashflow_field_resolution"
    assert revision["controlled_execution_allowed"] is False
