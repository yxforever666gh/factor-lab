from __future__ import annotations

from typing import Any

from factor_lab.agent_responses import (
    validate_failure_analyst_response,
    validate_planner_agent_response,
)


RESPONSE_TYPES = {"planner", "failure_analyst", "reviewer", "data_quality"}


def _validate_reviewer_response(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if payload.get("schema_version") != "factor_lab.reviewer_agent_response.v1":
        errors.append("schema_version 必须是 factor_lab.reviewer_agent_response.v1")
    if not isinstance(payload.get("candidate_reviews"), list):
        errors.append("candidate_reviews 必须是列表")
    if not isinstance(payload.get("summary_markdown"), str) or not payload.get("summary_markdown", "").strip():
        errors.append("summary_markdown 不能为空字符串")
    return errors


def _validate_data_quality_response(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if payload.get("schema_version") != "factor_lab.data_quality_agent_response.v1":
        errors.append("schema_version 必须是 factor_lab.data_quality_agent_response.v1")
    if not isinstance(payload.get("data_quality_findings"), list):
        errors.append("data_quality_findings 必须是列表")
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
        errors = validate_planner_agent_response(payload)
    elif decision_type == "failure_analyst":
        errors = validate_failure_analyst_response(payload)
    elif decision_type == "reviewer":
        errors = _validate_reviewer_response(payload)
    else:
        errors = _validate_data_quality_response(payload)

    metadata = payload.get("decision_metadata") or {}
    if metadata and not isinstance(metadata, dict):
        errors.append("decision_metadata 必须是对象")
    elif isinstance(metadata, dict):
        if metadata.get("source") and metadata.get("source") not in {"real_llm", "openclaw_gateway", "openclaw_agent", "legacy_openclaw_gateway", "legacy_openclaw_agent", "heuristic", "mock"}:
            errors.append("decision_metadata.source 非法")
        if metadata.get("effective_source") and metadata.get("effective_source") not in {"real_llm", "openclaw_gateway", "openclaw_agent", "legacy_openclaw_gateway", "legacy_openclaw_agent", "heuristic", "mock"}:
            errors.append("decision_metadata.effective_source 非法")
        if metadata.get("schema_valid") not in {None, True, False}:
            errors.append("decision_metadata.schema_valid 必须是布尔值")
        if metadata.get("degraded_to_heuristic") not in {None, True, False}:
            errors.append("decision_metadata.degraded_to_heuristic 必须是布尔值")
    return errors
