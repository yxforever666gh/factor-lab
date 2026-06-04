from factor_lab.autonomous_strategy_proxy_route_verdict import build_proxy_route_verdict


def test_proxy_route_verdict_stops_failed_screen():
    verdict = build_proxy_route_verdict(
        run_id="r",
        cheap_screen_result={
            "overall_status": "fail",
            "recommended_next_step": "stop_proxy_route",
            "best_candidate": {"candidate": "x", "max_drawdown": -1.2, "risk_pass": False},
        },
    )
    assert verdict["verdict"] == "stop_route"
    assert "proxy_cheap_screen_failed_risk_gate" in verdict["reason_codes"]
    assert verdict["recommended_next_step"] == "write_proxy_workstream_report"
    assert verdict["controlled_execution_allowed"] is False
    assert verdict["queue_write_allowed"] is False


def test_proxy_route_verdict_manual_review_for_passed_candidate():
    verdict = build_proxy_route_verdict(
        run_id="r",
        cheap_screen_result={
            "overall_status": "manual_review",
            "best_candidate": {"candidate": "x", "risk_pass": True},
        },
    )
    assert verdict["verdict"] == "manual_review_before_controlled_backtest"
    assert verdict["recommended_next_step"] == "manual_review_proxy_candidate"
    assert verdict["controlled_execution_allowed"] is False


def test_proxy_route_verdict_requests_revision_for_nonterminal_result():
    verdict = build_proxy_route_verdict(run_id="r", cheap_screen_result={"overall_status": "blocked"})
    assert verdict["verdict"] == "request_revision"
    assert verdict["recommended_next_step"] == "inspect_proxy_cheap_screen_result"
