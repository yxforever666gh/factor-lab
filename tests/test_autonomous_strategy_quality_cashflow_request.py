from factor_lab.autonomous_strategy_quality_cashflow_request import build_quality_cashflow_value_repair_request


def terminal_closure():
    return {
        "route_id": "industry_cycle_inflection_value_anchor_v1",
        "route_status": "stopped",
        "stop_reason": "industry_cycle_cheap_screen_risk_failed",
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "mechanism_lesson": "cheap + industry cycle momentum produced weak positive spread but did not control deep drawdown",
    }


def test_quality_cashflow_request_ready_after_terminal_route_closure():
    request = build_quality_cashflow_value_repair_request(run_id="r", route_closure=terminal_closure())
    assert request["mechanism_id"] == "quality_cashflow_value_repair_v1"
    assert request["decision"] == "request_new_mechanism"
    assert request["prerequisite_status"] == "ready_for_field_resolution"
    assert request["recommended_next_step"] == "run_quality_cashflow_field_resolution"
    assert request["controlled_execution_allowed"] is False
    assert request["queue_write_allowed"] is False
    assert "controlled_backtest" in request["blocked_actions"]
    assert "field_resolution" in request["next_allowed_actions"]
    assert "ocfps" in request["required_fields"]
    assert "debt_to_assets" in request["required_fields"]


def test_quality_cashflow_request_blocks_without_terminal_closure():
    request = build_quality_cashflow_value_repair_request(
        run_id="r",
        route_closure={
            "route_id": "industry_cycle_inflection_value_anchor_v1",
            "route_status": "open",
            "controlled_execution_allowed": True,
            "queue_write_allowed": True,
        },
    )
    assert request["decision"] == "blocked"
    assert request["prerequisite_status"] == "blocked_until_terminal_route_closure"
    assert request["recommended_next_step"] == "inspect_route_closure"
    assert request["controlled_execution_allowed"] is False
    assert request["queue_write_allowed"] is False
