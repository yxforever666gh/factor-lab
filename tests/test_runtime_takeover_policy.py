from factor_lab.runtime_takeover_policy import load_runtime_takeover_policy


def test_takeover_policy_blocks_legacy_recent_candidate():
    policy = load_runtime_takeover_policy(
        {
            "enabled": True,
            "blocked_worker_note_patterns": ["candidate_earnings_yield_book_yield_recent"],
        }
    )

    decision = policy.evaluate_task(
        {"task_type": "workflow", "worker_note": "candidate_earnings_yield_book_yield_recent_90d"}
    )

    assert decision["decision"] == "block"
    assert "blocked_worker_note_pattern" in decision["reasons"]


def test_takeover_policy_blocks_unmechanized_workflow_when_required():
    policy = load_runtime_takeover_policy({"enabled": True, "allow_unmechanized_workflow": False})

    decision = policy.evaluate_task({"task_type": "workflow", "payload": {"config_path": "x.json"}})

    assert decision["decision"] == "block"
    assert "missing_mechanism_id" in decision["reasons"]


def test_takeover_policy_allows_allowed_value_route_with_mechanism():
    policy = load_runtime_takeover_policy(
        {"enabled": True, "allowed_value_routes": ["industry_relative_value"]}
    )

    decision = policy.evaluate_task(
        {
            "task_type": "workflow",
            "payload": {"route_id": "industry_relative_value", "mechanism_id": "industry_relative_value"},
        }
    )

    assert decision["decision"] == "allow"
    assert decision["reasons"] == []


def test_takeover_policy_allows_configured_pledge_controlled_route():
    policy = load_runtime_takeover_policy(
        {"enabled": True, "allowed_controlled_routes": ["value_quality_high_pledge_record_count_confirmation"]}
    )

    decision = policy.evaluate_task(
        {
            "task_type": "workflow",
            "payload": {
                "route_id": "value_quality_high_pledge_record_count_confirmation",
                "mechanism_id": "pledge_control_pressure",
            },
        }
    )

    assert decision["decision"] == "allow"
    assert decision["reasons"] == []


def test_takeover_policy_disabled_allows_legacy_task():
    policy = load_runtime_takeover_policy({"enabled": False})

    decision = policy.evaluate_task({"task_type": "workflow", "worker_note": "candidate_earnings_yield_book_yield_recent_90d"})

    assert decision["decision"] == "allow"
