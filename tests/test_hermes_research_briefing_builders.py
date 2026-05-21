from factor_lab.hermes_research_briefing_builders import build_diagnostician_brief, build_researcher_profile_brief, build_repair_agent_brief


def test_researcher_profile_brief_includes_candidate_hypothesis_cards(tmp_path):
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
    payload = build_researcher_profile_brief(snapshot, {"tasks": []}, {}, {}, {}, output)

    cards = payload["inputs"]["candidate_hypothesis_cards"]
    assert len(cards) == 1
    assert cards[0]["candidate_name"] == "book_yield"
    assert cards[0]["target_window"] in {"recent_extension", "medium_horizon"}
    assert "incremental_value_thesis" in cards[0]
    proposal_requirements = payload["required_output_schema"]["experiment_proposal_requirements"]
    assert "mechanism_hypothesis" in proposal_requirements
    assert "required_data_fields" in proposal_requirements
    assert "falsification_criteria" in proposal_requirements
    assert "budget_justification" in proposal_requirements


def test_diagnostician_brief_requires_research_failure_taxonomy(tmp_path):
    output = tmp_path / "failure_brief.json"
    payload = build_diagnostician_brief({"recent_research_tasks": []}, {"open_questions": []}, {}, output)

    taxonomy = payload["required_output_schema"]["failure_taxonomy"]
    assert "data_insufficiency" in taxonomy
    assert "horizon_mismatch" in taxonomy
    assert "factor_direction_error" in taxonomy
    assert "neutralization_exposure_collapse" in taxonomy


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
    assert payload["hermes_profile"] == "repair_agent"
