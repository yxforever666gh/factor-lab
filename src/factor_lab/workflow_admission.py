from __future__ import annotations

import json
from typing import Any, Iterable

from factor_lab.coverage_preflight import evaluate_factor_coverage
from factor_lab.feature_schema import TUSHARE_AVAILABLE_FEATURE_COLUMNS
from factor_lab.runtime_takeover_policy import RuntimeTakeoverPolicy, load_runtime_takeover_policy


def _payload(task: dict[str, Any]) -> dict[str, Any]:
    payload = task.get("payload") or task.get("payload_json") or {}
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return {}
    return payload if isinstance(payload, dict) else {}


def _first_factor(payload: dict[str, Any]) -> dict[str, Any]:
    factors = payload.get("factors") or payload.get("factor_definitions") or []
    if isinstance(factors, list) and factors and isinstance(factors[0], dict):
        return dict(factors[0])
    return {"name": payload.get("factor_name"), "expression": payload.get("expression") or ""}


def evaluate_workflow_admission(
    task: dict[str, Any],
    *,
    available_fields: Iterable[str] | None = None,
    policy: RuntimeTakeoverPolicy | None = None,
) -> dict[str, Any]:
    policy = policy or load_runtime_takeover_policy()
    available = set(available_fields or TUSHARE_AVAILABLE_FEATURE_COLUMNS)
    payload = _payload(task)
    reasons: list[str] = []
    policy_decision = policy.evaluate_task(task)
    if policy_decision["decision"] == "block":
        reasons.extend(policy_decision.get("reasons") or [])

    mechanism_id = payload.get("mechanism_id")
    if not mechanism_id and not payload.get("baseline_reason"):
        reasons.append("missing_mechanism_id")

    required = list(payload.get("required_data_fields") or [])
    factor = _first_factor(payload)
    factor["required_data_fields"] = required
    coverage = evaluate_factor_coverage(factor=factor, available_fields=available)
    if coverage.get("missing_fields"):
        reasons.append("missing_required_fields")
    if coverage.get("recommendation") == "block":
        reasons.append("coverage_preflight_block")
    elif coverage.get("recommendation") == "cheap_screen":
        reasons.append("coverage_preflight_cheap_screen")

    unique_reasons = sorted(set(reasons))
    if "coverage_preflight_cheap_screen" in unique_reasons and not any(r.endswith("block") or r.startswith("missing_") for r in unique_reasons):
        decision = "cheap_screen_only"
    else:
        decision = "block" if unique_reasons else "allow"

    return {
        "decision": decision,
        "reasons": unique_reasons,
        "mechanism_id": mechanism_id,
        "route_id": payload.get("route_id"),
        "coverage_preflight": coverage,
        "policy_decision": policy_decision,
    }
