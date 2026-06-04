from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SAFE_FLAGS = {
    "queue_write_allowed": False,
    "broad_daemon_allowed": False,
    "automation_allowed": False,
    "automated_rerun_allowed": False,
    "live_trading_enabled": False,
}

APPROVAL_DECISIONS = {"approve_candidate", "approve_risk_relaxation"}
OBSERVATIONAL_DECISIONS = {"defer", "reject"}


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _flag_from(payload: dict[str, Any], flag: str) -> bool:
    if flag in payload:
        return bool(payload.get(flag))
    safety = payload.get("safety") or payload.get("safety_flags") or {}
    return bool(safety.get(flag, False)) if isinstance(safety, dict) else False


def _combine_safety(*payloads: dict[str, Any]) -> dict[str, bool]:
    # Handoff is strictly observational: any unsafe upstream truth is surfaced by
    # status/errors, but this artifact never grants permissions.
    return dict(SAFE_FLAGS)


def _decision_axis(manual_review: dict[str, Any], gate: dict[str, Any], consistency: dict[str, Any]) -> str | None:
    matched = consistency.get("matched_fields") or {}
    if matched.get("decision_axis"):
        return matched.get("decision_axis")
    decision = manual_review.get("recommended_manual_decision") or gate.get("required_approval") or {}
    dimension = decision.get("dimension")
    value = decision.get("value")
    if dimension is not None and value is not None:
        return f"{dimension}={value}"
    return None


def _handoff_status(intake_status: str | None, decision_type: str | None) -> str:
    if intake_status == "missing" or not intake_status:
        return "awaiting_operator_decision"
    if intake_status == "invalid":
        return "blocked_invalid_operator_decision"
    if intake_status == "valid" and decision_type in APPROVAL_DECISIONS:
        return "manual_decision_recorded_requires_separate_execution_plan"
    if intake_status == "valid" and decision_type in OBSERVATIONAL_DECISIONS:
        return "operator_decision_recorded_observational_only"
    return "blocked_invalid_operator_decision"


def build_operator_decision_handoff(
    *,
    intake_validation_path: str | Path,
    manual_approval_gate_path: str | Path,
    repair_blocker_review_path: str | Path,
    approval_consistency_path: str | Path,
) -> dict[str, Any]:
    intake = load_json(intake_validation_path)
    gate = load_json(manual_approval_gate_path)
    manual_review = load_json(repair_blocker_review_path)
    consistency = load_json(approval_consistency_path)

    intake_status = intake.get("intake_status")
    decision_type = intake.get("decision_type")
    handoff_status = _handoff_status(intake_status, decision_type)
    validation_errors = list(intake.get("validation_errors") or [])
    if consistency.get("inconsistencies"):
        validation_errors.extend(f"consistency:{item}" for item in consistency.get("inconsistencies") or [])

    safety = _combine_safety(intake, gate, manual_review, consistency)
    primary_blocker = manual_review.get("primary_issue") or (gate.get("required_approval") or {}).get("primary_issue")
    repair_status = manual_review.get("repair_status") or (gate.get("manual_review") or {}).get("repair_status")
    decision_axis = _decision_axis(manual_review, gate, consistency)
    approval_recorded = intake_status == "valid" and decision_type in APPROVAL_DECISIONS

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "handoff_status": handoff_status,
        "intake_status": intake_status or "missing",
        "decision_type": decision_type,
        "scope": intake.get("scope"),
        "reason": intake.get("reason"),
        "validation_errors": validation_errors,
        "primary_blocker": primary_blocker,
        "repair_status": repair_status,
        "candidate_count": int(manual_review.get("candidate_count") or (gate.get("manual_review") or {}).get("candidate_count") or 0),
        "decision_axis": decision_axis,
        "manual_gate_status": gate.get("gate_status"),
        "approval_consistency_status": consistency.get("consistency_status"),
        "non_mutating": True,
        "execution_allowed": False,
        "separate_execution_plan_required": bool(approval_recorded),
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
        "safety": safety,
        "source_artifacts": {
            "intake_validation_path": str(intake_validation_path),
            "manual_approval_gate_path": str(manual_approval_gate_path),
            "repair_blocker_review_path": str(repair_blocker_review_path),
            "approval_consistency_path": str(approval_consistency_path),
        },
    }


def operator_decision_handoff_to_markdown(payload: dict[str, Any]) -> str:
    safety = payload.get("safety") or {}
    errors = payload.get("validation_errors") or []
    lines = [
        "# Operator Decision Handoff",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Handoff status: {payload.get('handoff_status')}",
        f"Intake status: {payload.get('intake_status')}",
        f"Decision type: {payload.get('decision_type')}",
        f"Decision axis: {payload.get('decision_axis')}",
        f"Primary blocker: {payload.get('primary_blocker')}",
        f"Repair status: {payload.get('repair_status')}",
        f"Execution allowed: {payload.get('execution_allowed')}",
        f"Separate execution plan required: {payload.get('separate_execution_plan_required')}",
        f"Non-mutating: {payload.get('non_mutating')}",
        "",
        "## Safety flags",
        f"- queue_write_allowed: {safety.get('queue_write_allowed')}",
        f"- broad_daemon_allowed: {safety.get('broad_daemon_allowed')}",
        f"- automation_allowed: {safety.get('automation_allowed')}",
        f"- automated_rerun_allowed: {safety.get('automated_rerun_allowed')}",
        f"- live_trading_enabled: {safety.get('live_trading_enabled')}",
        "",
        "## Validation errors",
    ]
    if errors:
        lines.extend(f"- {error}" for error in errors)
    else:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def write_operator_decision_handoff(
    *,
    intake_validation_path: str | Path,
    manual_approval_gate_path: str | Path,
    repair_blocker_review_path: str | Path,
    approval_consistency_path: str | Path,
    json_path: str | Path,
    markdown_path: str | Path,
) -> dict[str, Any]:
    payload = build_operator_decision_handoff(
        intake_validation_path=intake_validation_path,
        manual_approval_gate_path=manual_approval_gate_path,
        repair_blocker_review_path=repair_blocker_review_path,
        approval_consistency_path=approval_consistency_path,
    )
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(operator_decision_handoff_to_markdown(payload), encoding="utf-8")
    return payload
