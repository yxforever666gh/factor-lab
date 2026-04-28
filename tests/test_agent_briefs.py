from factor_lab.agent_briefs import build_planner_agent_brief, build_repair_agent_brief


def test_planner_agent_brief_includes_candidate_hypothesis_cards(tmp_path):
    snapshot = {
        "promotion_scorecard": {
            "rows": [
                {
                    "factor_name": "book_yield",
                    "family": "value",
                    "quality_summary": "需要继续验证中长窗稳定性",
                    "quality_classification": "needs-validation",
                    "quality_scores": {"incremental_value": 14, "cross_window_robustness": 18},
                    "quality_hard_flags": {"insufficient_window_evidence": True},
                }
            ]
        }
    }
    output = tmp_path / "planner_brief.json"
    payload = build_planner_agent_brief(snapshot, {"tasks": []}, {}, {}, {}, output)

    cards = payload["inputs"]["candidate_hypothesis_cards"]
    assert len(cards) == 1
    assert cards[0]["candidate_name"] == "book_yield"
    assert cards[0]["target_window"] in {"recent_extension", "medium_horizon"}
    assert "incremental_value_thesis" in cards[0]


def test_repair_agent_brief_includes_stale_running_candidates(tmp_path):
    runtime_snapshot = {
        "daemon_status": {"state": "running"},
        "queue_budget": {"baseline": 0, "validation": 0, "exploration": 0},
        "queue_counts": {"pending": 0, "running": 1, "finished": 10, "failed": 1},
        "failure_state": {"consecutive_failures": 0, "cooldown_active": False},
        "blocked_lane_status": {},
        "route_status": {"healthy": True},
        "resource_pressure": {"rss_mb": 128},
        "heartbeat_gap": {"available": True, "seconds_since_last": 12},
        "recent_research_tasks": [],
        "recent_failed_or_risky_tasks": [],
        "stale_running_candidates": [{"task_id": "t1", "outputs_complete": True}],
        "status_file_consistency": {"daemon_status_available": True},
        "open_incidents": [],
    }
    output = tmp_path / "repair_brief.json"
    payload = build_repair_agent_brief(runtime_snapshot, {"open_questions": []}, {"x": 1}, output)

    assert payload["schema_version"] == "factor_lab.repair_agent_brief.v1"
    assert payload["inputs"]["stale_running_candidates"][0]["task_id"] == "t1"
    assert payload["agent_role"] == "repair_agent"
