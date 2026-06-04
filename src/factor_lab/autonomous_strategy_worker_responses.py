from __future__ import annotations

from typing import Any

FORBIDDEN_RESPONSE_FIELDS = {
    "agent",
    "agent_id",
    "agent_role",
    "legacy_agent_id",
    "llm_fallback_order",
    "model",
    "provider",
    "base_url",
    "api_key",
    "profile",
}

REQUIRED_RESPONSE_FIELDS = {
    "schema_version",
    "worker_key",
    "decision_recommendation",
    "reason_codes",
    "requested_actions",
    "forbidden_actions_observed",
    "summary",
}


def validate_worker_response(
    response: dict[str, Any],
    *,
    expected_worker_key: str,
    forbidden_actions: list[str],
) -> dict[str, Any]:
    errors: list[str] = []

    missing = sorted(REQUIRED_RESPONSE_FIELDS - set(response))
    errors.extend(f"missing_field:{field}" for field in missing)

    if response.get("worker_key") != expected_worker_key:
        errors.append("wrong_worker_key")

    for field in sorted(FORBIDDEN_RESPONSE_FIELDS & set(response)):
        errors.append(f"forbidden_field:{field}")

    requested = set(str(item) for item in response.get("requested_actions") or [])
    observed = set(str(item) for item in response.get("forbidden_actions_observed") or [])
    for action in forbidden_actions:
        if action in requested:
            errors.append(f"forbidden_action_requested:{action}")
        if action in observed:
            errors.append(f"forbidden_action_observed:{action}")

    return {"valid": not errors, "errors": errors}
