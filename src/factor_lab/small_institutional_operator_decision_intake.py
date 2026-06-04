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

VALID_DECISION_TYPES = {"approve_candidate", "approve_risk_relaxation", "reject", "defer"}


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"_decode_error": True}
    return payload if isinstance(payload, dict) else {"_type_error": True}


def _safe_flags_from(payload: dict[str, Any]) -> dict[str, bool]:
    safety = payload.get("safety") or {}
    return {flag: bool(safety.get(flag, False)) for flag in SAFE_FLAGS}


def validate_operator_decision_intake(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    generated_at = datetime.now(timezone.utc).isoformat()
    if not p.exists():
        return {
            "schema_version": 1,
            "generated_at_utc": generated_at,
            "intake_status": "missing",
            "intake_path": str(p),
            "decision_type": None,
            "scope": None,
            "reason": None,
            "validation_errors": [],
            "non_mutating": True,
            "safety": dict(SAFE_FLAGS),
        }

    payload = load_json(p)
    errors: list[str] = []
    if payload.get("_decode_error"):
        errors.append("invalid_json")
    if payload.get("_type_error"):
        errors.append("invalid_payload_type")

    decision_type = payload.get("decision_type")
    scope = payload.get("scope")
    reason = payload.get("reason")

    if decision_type not in VALID_DECISION_TYPES:
        errors.append("invalid_decision_type")
    if not scope:
        errors.append("missing_scope")
    if not reason:
        errors.append("missing_reason")

    raw_safety = payload.get("safety") or {}
    if not isinstance(raw_safety, dict):
        errors.append("missing_safety")
        raw_safety = {}
    for flag in SAFE_FLAGS:
        if flag not in raw_safety:
            errors.append(f"missing_{flag}")
        elif bool(raw_safety.get(flag)) is not False:
            errors.append(f"unsafe_{flag}")

    status = "invalid" if errors else "valid"
    return {
        "schema_version": 1,
        "generated_at_utc": generated_at,
        "intake_status": status,
        "intake_path": str(p),
        "decision_type": decision_type,
        "scope": scope,
        "reason": reason,
        "validation_errors": errors,
        "non_mutating": True,
        "safety": dict(SAFE_FLAGS),
    }


def operator_decision_intake_to_markdown(payload: dict[str, Any]) -> str:
    safety = payload.get("safety") or {}
    errors = payload.get("validation_errors") or []
    lines = [
        "# Operator Decision Intake Validation",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Intake status: {payload.get('intake_status')}",
        f"Decision type: {payload.get('decision_type')}",
        f"Scope: {payload.get('scope')}",
        f"Reason: {payload.get('reason')}",
        f"Non-mutating: {payload.get('non_mutating')}",
        "",
        "## Safety",
        f"- Queue write allowed: {safety.get('queue_write_allowed')}",
        f"- Broad daemon allowed: {safety.get('broad_daemon_allowed')}",
        f"- Automation allowed: {safety.get('automation_allowed')}",
        f"- Automated rerun allowed: {safety.get('automated_rerun_allowed')}",
        f"- Live trading enabled: {safety.get('live_trading_enabled')}",
        "",
        "## Validation errors",
    ]
    if errors:
        lines.extend(f"- {error}" for error in errors)
    else:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def write_operator_decision_intake_validation(
    *,
    intake_path: str | Path,
    json_path: str | Path,
    markdown_path: str | Path,
) -> dict[str, Any]:
    payload = validate_operator_decision_intake(intake_path)
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(operator_decision_intake_to_markdown(payload), encoding="utf-8")
    return payload
