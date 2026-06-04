from factor_lab.autonomous_strategy_proxy_cheap_screen_plan import build_proxy_cheap_screen_plan


def test_proxy_cheap_screen_plan_ready_when_phase6_and_pit_pass():
    plan = build_proxy_cheap_screen_plan(
        run_id="r",
        phase6_final_verdict={"phase_status": "completed"},
        proxy_pit_alignment={"decision": "prepare_proxy_cheap_screen_plan"},
    )
    assert plan["decision"] == "prepare_proxy_cheap_screen_execution"
    assert plan["recommended_next_step"] == "run_proxy_cheap_screen_execution"
    assert len(plan["candidate_screens"]) == 6
    assert "combined_quality_profit_proxy" in {s["screen_id"] for s in plan["candidate_screens"]}
    assert plan["controlled_execution_allowed"] is False
    assert plan["queue_write_allowed"] is False
    assert "controlled_backtest" in plan["blocked_actions"]


def test_proxy_cheap_screen_plan_blocks_when_phase6_not_complete():
    plan = build_proxy_cheap_screen_plan(
        run_id="r",
        phase6_final_verdict={"phase_status": "blocked"},
        proxy_pit_alignment={"decision": "prepare_proxy_cheap_screen_plan"},
    )
    assert plan["decision"] == "block_proxy_cheap_screen_plan"
    assert plan["next_allowed_actions"] == ["inspect_phase6_or_pit_alignment_state"]
    assert plan["queue_write_allowed"] is False


def test_proxy_cheap_screen_plan_blocks_when_pit_not_ready():
    plan = build_proxy_cheap_screen_plan(
        run_id="r",
        phase6_final_verdict={"phase_status": "completed"},
        proxy_pit_alignment={"decision": "block_proxy_pit_alignment"},
    )
    assert plan["decision"] == "block_proxy_cheap_screen_plan"
    assert plan["pit_alignment_ready"] is False
