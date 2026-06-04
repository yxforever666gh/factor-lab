from factor_lab.autonomous_strategy_controller import build_autonomous_strategy_controller_state


def test_controller_prioritizes_proxy_workstream_report_failed():
    state = build_autonomous_strategy_controller_state(
        run_id="r",
        artifacts={
            "proxy_workstream_report.json": {
                "alpha_status": "failed",
                "route_verdict": "stop_route",
                "next_recommended_workstream": "request_new_mechanism_or_revisit_risk_model",
            },
            "proxy_route_verdict.json": {"verdict": "stop_route"},
        },
    )
    assert state["current_state"] == "proxy_workstream_completed_failed_alpha"
    assert state["recommended_next_step"] == "request_new_mechanism_or_revisit_risk_model"
    assert state["human_required"] is False
    assert state["queue_write_allowed"] is False


def test_controller_prioritizes_proxy_route_verdict_stopped():
    state = build_autonomous_strategy_controller_state(
        run_id="r",
        artifacts={
            "proxy_route_verdict.json": {"verdict": "stop_route"},
            "proxy_cheap_screen_result.json": {"overall_status": "fail"},
        },
    )
    assert state["current_state"] == "proxy_route_stopped"
    assert state["recommended_next_step"] == "write_proxy_workstream_report"
    assert state["human_required"] is False
    assert state["queue_write_allowed"] is False


def test_controller_prioritizes_proxy_cheap_screen_result_failed():
    state = build_autonomous_strategy_controller_state(
        run_id="r",
        artifacts={
            "proxy_cheap_screen_result.json": {"overall_status": "fail", "recommended_next_step": "stop_proxy_route"},
            "proxy_cheap_screen_plan.json": {"decision": "prepare_proxy_cheap_screen_execution"},
        },
    )
    assert state["current_state"] == "proxy_cheap_screen_failed"
    assert state["recommended_next_step"] == "write_proxy_route_verdict"
    assert state["human_required"] is False
    assert state["queue_write_allowed"] is False


def test_controller_prioritizes_proxy_cheap_screen_plan_ready():
    state = build_autonomous_strategy_controller_state(
        run_id="r",
        artifacts={
            "proxy_cheap_screen_plan.json": {"decision": "prepare_proxy_cheap_screen_execution"},
            "proxy_pit_alignment.json": {"decision": "prepare_proxy_cheap_screen_plan"},
        },
    )
    assert state["current_state"] == "proxy_cheap_screen_plan_ready"
    assert state["recommended_next_step"] == "run_proxy_cheap_screen_execution"
    assert state["human_required"] is False
    assert state["controlled_execution_allowed"] is False


def test_controller_prioritizes_proxy_pit_alignment_passed():
    state = build_autonomous_strategy_controller_state(
        run_id="r",
        artifacts={
            "proxy_pit_alignment.json": {"decision": "prepare_proxy_cheap_screen_plan"},
            "pit_overlay_diagnostic.json": {"decision": "prepare_proxy_pit_alignment_review"},
        },
    )
    assert state["current_state"] == "proxy_pit_alignment_passed"
    assert state["recommended_next_step"] == "write_proxy_cheap_screen_plan"
    assert state["human_required"] is False
    assert state["controlled_execution_allowed"] is False


def test_controller_prioritizes_passed_pit_overlay_over_chunk_run():
    state = build_autonomous_strategy_controller_state(
        run_id="r",
        artifacts={
            "pit_overlay_diagnostic.json": {"decision": "prepare_proxy_pit_alignment_review"},
            "pit_cache_extension_run.json": {
                "execution_status": "completed",
                "coverage_pass": True,
                "chunk_mode": {"enabled": True},
            },
        },
    )
    assert state["current_state"] == "pit_cache_extension_completed"
    assert state["recommended_next_step"] == "prove_proxy_report_date_alignment"
    assert state["human_required"] is False
    assert state["queue_write_allowed"] is False


def test_controller_prioritizes_pit_extension_run_chunk_completed():
    state = build_autonomous_strategy_controller_state(
        run_id="r",
        artifacts={
            "pit_cache_extension_run.json": {
                "execution_status": "completed",
                "coverage_pass": True,
                "chunk_mode": {"enabled": True, "max_tickers": 2},
                "recommended_next_step": "rerun_proxy_field_resolution_with_pit_overlay",
            },
            "pit_cache_extension_plan.json": {"decision": "await_human_approval_for_pit_cache_extension"},
        },
    )
    assert state["current_state"] == "pit_cache_extension_chunk_completed"
    assert state["recommended_next_step"] == "run_next_pit_cache_extension_chunk"
    assert state["human_required"] is False
    assert state["queue_write_allowed"] is False


def test_controller_prioritizes_pit_extension_plan_autonomous_data_work():
    state = build_autonomous_strategy_controller_state(
        run_id="r",
        artifacts={
            "pit_cache_extension_plan.json": {"decision": "await_human_approval_for_pit_cache_extension"},
            "pit_overlay_diagnostic.json": {"decision": "extend_pit_cache_coverage"},
        },
    )
    assert state["current_state"] == "ready_autonomous_pit_cache_extension"
    assert state["recommended_next_step"] == "run_autonomous_pit_cache_extension"
    assert state["human_required"] is False
    assert state["controlled_execution_allowed"] is False
    assert state["queue_write_allowed"] is False
    assert state["timer_enable_allowed"] is False


def test_controller_prioritizes_pit_overlay_human_decision():
    state = build_autonomous_strategy_controller_state(
        run_id="r",
        artifacts={
            "pit_overlay_diagnostic.json": {"decision": "extend_pit_cache_coverage"},
            "quality_profit_proxy_field_resolution.json": {"decision": "block_low_coverage"},
        },
    )
    assert state["current_state"] == "blocked_pit_overlay_coverage"
    assert state["recommended_next_step"] == "prepare_pit_cache_extension_plan"
    assert state["human_required"] is True
    assert state["controlled_execution_allowed"] is False
    assert state["queue_write_allowed"] is False
    assert state["timer_enable_allowed"] is False
    assert "controlled_backtest" in state["blocked_actions"]


def test_controller_runs_safe_pit_overlay_when_proxy_field_low_coverage():
    state = build_autonomous_strategy_controller_state(
        run_id="r",
        artifacts={"quality_profit_proxy_field_resolution.json": {"decision": "block_low_coverage"}},
    )
    assert state["current_state"] == "blocked_proxy_low_coverage"
    assert state["recommended_next_step"] == "run_pit_overlay_diagnostic"
    assert state["human_required"] is False
    assert state["next_allowed_actions"] == ["run_pit_overlay_diagnostic"]


def test_controller_runs_safe_proxy_field_resolution_when_revision_ready():
    state = build_autonomous_strategy_controller_state(
        run_id="r",
        artifacts={"quality_profit_proxy_value_repair_revision.json": {"revision_status": "ready_for_proxy_field_resolution", "decision": "revise_to_proxy_mechanism"}},
    )
    assert state["current_state"] == "ready_proxy_field_resolution"
    assert state["recommended_next_step"] == "run_quality_profit_proxy_field_resolution"
    assert state["human_required"] is False


def test_controller_requests_human_for_original_route_data_block():
    state = build_autonomous_strategy_controller_state(
        run_id="r",
        artifacts={"quality_cashflow_field_resolution.json": {"decision": "request_data"}},
    )
    assert state["current_state"] == "blocked_quality_cashflow_request_data"
    assert state["recommended_next_step"] == "create_proxy_revision_or_request_data"
    assert state["human_required"] is True


def test_controller_requests_new_mechanism_after_industry_closure():
    state = build_autonomous_strategy_controller_state(
        run_id="r",
        artifacts={"industry_cycle_route_closure.json": {"route_status": "stopped", "stop_reason": "risk_failed"}},
    )
    assert state["current_state"] == "industry_route_stopped"
    assert state["recommended_next_step"] == "request_new_mechanism"
    assert state["human_required"] is False


def test_controller_blocks_when_artifacts_missing():
    state = build_autonomous_strategy_controller_state(run_id="r", artifacts={})
    assert state["current_state"] == "insufficient_artifacts"
    assert state["recommended_next_step"] == "inspect_autonomous_strategy_lab_state"
    assert state["human_required"] is True
