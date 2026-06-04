from factor_lab.autonomous_strategy_proxy_workstream_report import build_proxy_workstream_report


def test_proxy_workstream_report_separates_engineering_and_failed_alpha():
    report = build_proxy_workstream_report(
        run_id="r",
        phase6_final_verdict={
            "phase_status": "completed",
            "pit_overlay_coverage_passed": True,
            "pit_alignment_passed": True,
            "pit_alignment_usable_coverage": 1.0,
        },
        proxy_cheap_screen_result={
            "overall_status": "fail",
            "best_candidate": {"candidate": "cheap", "risk_pass": False, "max_drawdown": -1.2},
        },
        proxy_route_verdict={"verdict": "stop_route", "reason_codes": ["risk_failed"]},
    )
    assert report["engineering_status"] == "completed"
    assert report["alpha_status"] == "failed"
    assert report["data_status"]["pit_alignment_passed"] is True
    assert report["route_verdict"] == "stop_route"
    assert report["next_recommended_workstream"] == "request_new_mechanism_or_revisit_risk_model"
    assert report["controlled_execution_allowed"] is False
    assert report["queue_write_allowed"] is False


def test_proxy_workstream_report_manual_review_when_verdict_passes():
    report = build_proxy_workstream_report(
        run_id="r",
        phase6_final_verdict={"phase_status": "completed"},
        proxy_cheap_screen_result={"overall_status": "manual_review", "best_candidate": {"risk_pass": True}},
        proxy_route_verdict={"verdict": "manual_review_before_controlled_backtest", "reason_codes": []},
    )
    assert report["alpha_status"] == "manual_review"
    assert report["next_recommended_workstream"] == "manual_review_before_any_execution"
    assert report["controlled_execution_allowed"] is False
