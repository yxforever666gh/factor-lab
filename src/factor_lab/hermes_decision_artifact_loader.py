from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RESEARCHER_PROFILE_RESPONSE_SCHEMA_VERSION = "factor_lab.researcher_profile_response.v1"
DIAGNOSTICIAN_RESPONSE_SCHEMA_VERSION = "factor_lab.diagnostician_response.v1"



def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()



def _read_json(path: str | Path, default: Any) -> Any:
    p = Path(path)
    if not p.exists():
        return default
    return json.loads(p.read_text(encoding="utf-8"))



def _validate_metadata(payload: dict[str, Any], prefix: str) -> list[str]:
    errors: list[str] = []
    metadata = payload.get("decision_metadata") or {}
    if metadata and not isinstance(metadata, dict):
        errors.append(f"{prefix}.decision_metadata 必须是对象")
        return errors
    if not metadata:
        return errors
    source = metadata.get("source")
    if source is not None and source not in {"direct_model", "hermes_native_gateway", "hermes_native_agent", "legacy_hermes_native_gateway", "legacy_hermes_native_agent", "heuristic", "mock"}:
        errors.append(f"{prefix}.decision_metadata.source 非法")
    effective_source = metadata.get("effective_source")
    if effective_source is not None and effective_source not in {"direct_model", "hermes_native_gateway", "hermes_native_agent", "legacy_hermes_native_gateway", "legacy_hermes_native_agent", "heuristic", "mock"}:
        errors.append(f"{prefix}.decision_metadata.effective_source 非法")
    if metadata.get("schema_valid") not in {None, True, False}:
        errors.append(f"{prefix}.decision_metadata.schema_valid 必须是布尔值")
    if metadata.get("degraded_to_heuristic") not in {None, True, False}:
        errors.append(f"{prefix}.decision_metadata.degraded_to_heuristic 必须是布尔值")
    if metadata.get("latency_ms") is not None:
        try:
            latency = int(metadata.get("latency_ms"))
            if latency < 0:
                errors.append(f"{prefix}.decision_metadata.latency_ms 不能小于 0")
        except Exception:
            errors.append(f"{prefix}.decision_metadata.latency_ms 必须是整数")
    if metadata.get("provider_latency_ms") is not None:
        try:
            provider_latency = int(metadata.get("provider_latency_ms"))
            if provider_latency < 0:
                errors.append(f"{prefix}.decision_metadata.provider_latency_ms 不能小于 0")
        except Exception:
            errors.append(f"{prefix}.decision_metadata.provider_latency_ms 必须是整数")
    if metadata.get("validation_errors") is not None and not isinstance(metadata.get("validation_errors"), list):
        errors.append(f"{prefix}.decision_metadata.validation_errors 必须是列表")
    return errors



def validate_researcher_profile_response(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if payload.get("schema_version") != RESEARCHER_PROFILE_RESPONSE_SCHEMA_VERSION:
        errors.append("planner schema_version 不匹配")
    for key in ["mode", "task_mix", "priority_families", "suppress_families", "recommended_actions"]:
        if key not in payload:
            errors.append(f"planner 响应缺少字段: {key}")
    task_mix = payload.get("task_mix") or {}
    if not isinstance(task_mix, dict):
        errors.append("planner.task_mix 必须是对象")
    else:
        for key in ["baseline", "validation", "exploration"]:
            if key in task_mix:
                try:
                    int(task_mix[key])
                except Exception:
                    errors.append(f"planner.task_mix.{key} 必须是整数")
    for key in ["priority_families", "suppress_families", "recommended_actions", "hypothesis_cards", "challenger_queue"]:
        if key in payload and not isinstance(payload.get(key), list):
            errors.append(f"planner.{key} 必须是列表")
    if "confidence_score" in payload:
        try:
            score = float(payload.get("confidence_score"))
            if score < 0 or score > 1:
                errors.append("planner.confidence_score 必须在 0 到 1 之间")
        except Exception:
            errors.append("planner.confidence_score 必须是数值")
    errors.extend(_validate_metadata(payload, "planner"))
    return errors



def validate_diagnostician_response(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if payload.get("schema_version") != DIAGNOSTICIAN_RESPONSE_SCHEMA_VERSION:
        errors.append("failure analyst schema_version 不匹配")
    for key in ["failure_patterns", "should_stop", "should_probe", "should_reroute"]:
        if key not in payload:
            errors.append(f"failure analyst 响应缺少字段: {key}")
        elif not isinstance(payload.get(key), list):
            errors.append(f"failure analyst.{key} 必须是列表")
    errors.extend(_validate_metadata(payload, "failure analyst"))
    return errors



def load_validated_hermes_decision_artifacts(base_dir: str | Path) -> dict[str, Any]:
    base = Path(base_dir)
    planner = _read_json(base / "researcher_profile_response.json", {})
    failure = _read_json(base / "diagnostician_response.json", {})
    planner_errors = validate_researcher_profile_response(planner) if planner else []
    failure_errors = validate_diagnostician_response(failure) if failure else []
    return {
        "loaded_at_utc": _iso_now(),
        "planner": planner if planner and not planner_errors else {},
        "planner_errors": planner_errors,
        "diagnostician": failure if failure and not failure_errors else {},
        "diagnostician_errors": failure_errors,
    }
