from factor_lab.autonomous_strategy_industry_cycle_route_closure import build_industry_cycle_route_closure


def failed_screen():
    return {
        "overall_status": "fail",
        "recommended_next_step": "stop_route",
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "best_candidate": {
            "candidate": "industry_return_60d_positive",
            "mean_daily_spread": 0.0026,
            "max_drawdown": -1.69,
            "risk_pass": False,
        },
    }


def test_industry_cycle_closure_stops_terminal_risk_failure():
    closure = build_industry_cycle_route_closure(run_id="r", cheap_screen=failed_screen())
    assert closure["route_status"] == "stopped"
    assert closure["stop_reason"] == "industry_cycle_cheap_screen_risk_failed"
    assert closure["recommended_next_step"] == "request_new_mechanism"
    assert closure["controlled_execution_allowed"] is False
    assert closure["queue_write_allowed"] is False
    assert closure["timer_enable_allowed"] is False
    assert "broad_daemon_restore" in closure["blocked_actions"]
    assert "auto_promotion" in closure["blocked_actions"]


def test_industry_cycle_closure_blocks_unexpected_artifact_state():
    closure = build_industry_cycle_route_closure(
        run_id="r",
        cheap_screen={
            "overall_status": "pass",
            "recommended_next_step": "controlled_backtest",
            "controlled_execution_allowed": True,
            "queue_write_allowed": True,
            "best_candidate": {"risk_pass": True},
        },
    )
    assert closure["route_status"] == "blocked"
    assert closure["stop_reason"] == "industry_cycle_cheap_screen_not_terminal_failure"
    assert closure["controlled_execution_allowed"] is False
    assert closure["queue_write_allowed"] is False
