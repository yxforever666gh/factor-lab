from __future__ import annotations

from typing import Any

from factor_lab.hermes_decision_artifact_loader import (
    validate_diagnostician_response,
    validate_researcher_profile_response,
)


RESPONSE_TYPES = {"planner", "diagnostician", "reviewer", "data_steward"}


def _validate_reviewer_response(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if payload.get("schema_version") != "factor_lab.reviewer_agent_response.v1":
        errors.append("schema_version 必须是 factor_lab.reviewer_agent_response.v1")
    if not isinstance(payload.get("candidate_reviews"), list):
        errors.append("candidate_reviews 必须是列表")
    if not isinstance(payload.get("summary_markdown"), str) or not payload.get("summary_markdown", "").strip():
        errors.append("summary_markdown 不能为空字符串")
    return errors


def _validate_data_steward_response(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if payload.get("schema_version") != "factor_lab.data_steward_agent_response.v1":
        errors.append("schema_version 必须是 factor_lab.data_steward_agent_response.v1")
    if not isinstance(payload.get("data_steward_findings"), list):
        errors.append("data_steward_findings 必须是列表")
    if not isinstance(payload.get("dataset_health"), dict):
        errors.append("dataset_health 必须是对象")
    if not isinstance(payload.get("should_pause_research"), bool):
        errors.append("should_pause_research 必须是布尔值")
    if not isinstance(payload.get("summary_markdown"), str) or not payload.get("summary_markdown", "").strip():
        errors.append("summary_markdown 不能为空字符串")
    return errors



def validate_decision_payload(decision_type: str, payload: dict[str, Any]) -> list[str]:
    if decision_type not in RESPONSE_TYPES:
        return [f"unsupported decision_type: {decision_type}"]
    if decision_type == "planner":
        errors = validate_researcher_profile_response(payload)
    elif decision_type == "diagnostician":
        errors = validate_diagnostician_response(payload)
    elif decision_type == "reviewer":
        errors = _validate_reviewer_response(payload)
    else:
        errors = _validate_data_steward_response(payload)

    metadata = payload.get("decision_metadata") or {}
    if metadata and not isinstance(metadata, dict):
        errors.append("decision_metadata 必须是对象")
    elif isinstance(metadata, dict):
        if metadata.get("source") and metadata.get("source") not in {"direct_model", "hermes_native_gateway", "hermes_native_agent", "legacy_hermes_native_gateway", "legacy_hermes_native_agent", "heuristic", "mock"}:
            errors.append("decision_metadata.source 非法")
        if metadata.get("effective_source") and metadata.get("effective_source") not in {"direct_model", "hermes_native_gateway", "hermes_native_agent", "legacy_hermes_native_gateway", "legacy_hermes_native_agent", "heuristic", "mock"}:
            errors.append("decision_metadata.effective_source 非法")
        if metadata.get("schema_valid") not in {None, True, False}:
            errors.append("decision_metadata.schema_valid 必须是布尔值")
        if metadata.get("degraded_to_heuristic") not in {None, True, False}:
            errors.append("decision_metadata.degraded_to_heuristic 必须是布尔值")
    return errors
