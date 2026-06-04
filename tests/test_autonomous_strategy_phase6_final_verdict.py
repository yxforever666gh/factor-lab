from factor_lab.autonomous_strategy_phase6_final_verdict import build_phase6_final_verdict


def test_phase6_final_verdict_completed_when_controller_overlay_and_alignment_pass():
    verdict = build_phase6_final_verdict(
        run_id="r",
        controller_state={"current_state": "proxy_pit_alignment_passed"},
        pit_overlay_diagnostic={
            "decision": "prepare_proxy_pit_alignment_review",
            "low_after_overlay": [],
            "overlay_coverage": {"profit_yoy": 0.97},
        },
        proxy_pit_alignment={"decision": "prepare_proxy_cheap_screen_plan", "usable_coverage": 1.0},
    )
    assert verdict["phase_status"] == "completed"
    assert verdict["next_phase"] == "phase7_proxy_cheap_screen_plan"
    assert verdict["recommended_next_step"] == "write_proxy_cheap_screen_plan"
    assert verdict["controlled_execution_allowed"] is False
    assert verdict["queue_write_allowed"] is False
    assert verdict["remaining_phase6_items"] == []


def test_phase6_final_verdict_blocks_when_alignment_not_passed():
    verdict = build_phase6_final_verdict(
        run_id="r",
        controller_state={"current_state": "pit_cache_extension_completed"},
        pit_overlay_diagnostic={"decision": "prepare_proxy_pit_alignment_review", "low_after_overlay": []},
        proxy_pit_alignment={"decision": "block_proxy_pit_alignment", "usable_coverage": 0.0},
    )
    assert verdict["phase_status"] == "blocked"
    assert verdict["next_phase"] == "continue_phase6_blocker_resolution"
    assert verdict["controlled_execution_allowed"] is False
