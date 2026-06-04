from __future__ import annotations

from factor_lab.autonomous_strategy_worker_responses import validate_worker_response


def test_validate_worker_response_accepts_schema_valid_safe_response():
    response = {
        "schema_version": 1,
        "worker_key": "factor_lab_diagnostician",
        "decision_recommendation": "request_data",
        "reason_codes": ["drawdown_blocker_no_safe_candidate"],
        "requested_actions": ["write_blocker_report"],
        "forbidden_actions_observed": [],
        "summary": "Route is blocked by drawdown and data insufficiency.",
    }

    result = validate_worker_response(
        response,
        expected_worker_key="factor_lab_diagnostician",
        forbidden_actions=["queue_write", "provider_model_change"],
    )

    assert result["valid"] is True
    assert result["errors"] == []


def test_validate_worker_response_rejects_wrong_worker_or_forbidden_action():
    response = {
        "schema_version": 1,
        "worker_key": "factor_lab_diagnostician",
        "decision_recommendation": "continue_route_with_constraints",
        "reason_codes": [],
        "requested_actions": ["queue_write", "provider_model_change"],
        "forbidden_actions_observed": [],
        "summary": "Unsafe.",
    }

    result = validate_worker_response(
        response,
        expected_worker_key="factor_lab_reviewer",
        forbidden_actions=["queue_write", "provider_model_change"],
    )

    assert result["valid"] is False
    assert "wrong_worker_key" in result["errors"]
    assert "forbidden_action_requested:queue_write" in result["errors"]
    assert "forbidden_action_requested:provider_model_change" in result["errors"]


def test_validate_worker_response_rejects_model_provider_fields_anywhere():
    response = {
        "schema_version": 1,
        "worker_key": "factor_lab_diagnostician",
        "decision_recommendation": "request_data",
        "reason_codes": [],
        "requested_actions": [],
        "forbidden_actions_observed": [],
        "summary": "Unsafe metadata.",
        "model": "pinned-model",
    }

    result = validate_worker_response(
        response,
        expected_worker_key="factor_lab_diagnostician",
        forbidden_actions=[],
    )

    assert result["valid"] is False
    assert "forbidden_field:model" in result["errors"]
