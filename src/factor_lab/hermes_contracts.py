from __future__ import annotations

from typing import Any


def common_output_contract(profile_key: str, request_id: str) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "profile_key": profile_key,
        "summary": "string",
        "recommendation": "string",
        "confidence": 0.0,
        "risks": [],
        "next_actions": [],
    }


def validate_hermes_response(payload: dict[str, Any], *, request_id: str, profile_key: str) -> list[str]:
    errors: list[str] = []
    if payload.get("request_id") != request_id:
        errors.append("request_id_mismatch")
    if payload.get("profile_key") != profile_key:
        errors.append("profile_key_mismatch")
    for key in ["summary", "recommendation"]:
        if not isinstance(payload.get(key), str) or not payload.get(key):
            errors.append(f"missing_{key}")
    try:
        confidence = float(payload.get("confidence"))
        if not 0.0 <= confidence <= 1.0:
            errors.append("confidence_out_of_range")
    except Exception:
        errors.append("confidence_not_numeric")
    for key in ["risks", "next_actions"]:
        if not isinstance(payload.get(key), list):
            errors.append(f"{key}_not_list")
    return errors
