from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_MANUAL_REVIEW_PATH = Path("artifacts/small_institutional_simulation/repair_blocker_manual_review.json")
DEFAULT_JSON_PATH = Path("artifacts/small_institutional_simulation/manual_approval_gate.json")
DEFAULT_MARKDOWN_PATH = Path("artifacts/small_institutional_simulation/manual_approval_gate.md")


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _base_safety() -> dict[str, bool]:
    return {
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
    }


def _required_approval(manual_review: dict[str, Any]) -> dict[str, Any]:
    decision = manual_review.get("recommended_manual_decision") or {}
    return {
        "decision_required": bool(decision.get("decision_required")),
        "dimension": decision.get("dimension"),
        "value": decision.get("value"),
        "primary_issue": manual_review.get("primary_issue"),
        "repair_status": manual_review.get("repair_status"),
        "best_available_max_drawdown": manual_review.get("best_available_max_drawdown"),
        "drawdown_gap_to_limit": manual_review.get("drawdown_gap_to_limit"),
    }


def _is_unsafe_approval(approval: dict[str, Any]) -> bool:
    return bool(
        approval.get("queue_write_allowed")
        or approval.get("broad_daemon_allowed")
        or approval.get("automation_allowed")
        or approval.get("automated_rerun_allowed")
        or approval.get("live_trading_enabled")
    )


def _approved_candidate(approval: dict[str, Any]) -> dict[str, Any]:
    candidate = approval.get("approved_candidate") or approval.get("approved_repaired_candidate") or {}
    return candidate if isinstance(candidate, dict) else {}


def _approved_risk_relaxation(approval: dict[str, Any]) -> dict[str, Any]:
    relaxation = approval.get("approved_risk_relaxation") or approval.get("risk_relaxation") or {}
    return relaxation if isinstance(relaxation, dict) else {}


def build_manual_approval_gate(
    manual_review_path: str | Path,
    approval_path: str | Path | None = None,
) -> dict[str, Any]:
    manual_review = load_json(manual_review_path)
    approval = load_json(approval_path) if approval_path is not None else {}
    safety = _base_safety()
    required = _required_approval(manual_review)

    base: dict[str, Any] = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_artifacts": {
            "manual_review_path": str(manual_review_path),
            "approval_path": str(approval_path) if approval_path is not None else None,
        },
        "manual_review": {
            "review_status": manual_review.get("review_status") or "missing",
            "primary_issue": manual_review.get("primary_issue"),
            "repair_status": manual_review.get("repair_status"),
            "candidate_count": int(manual_review.get("candidate_count") or 0),
        },
        "required_approval": required,
        "safety": safety,
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
    }

    candidate = _approved_candidate(approval)
    relaxation = _approved_risk_relaxation(approval)
    explicit_approval = bool(approval.get("human_approval_present")) and not _is_unsafe_approval(approval)

    if explicit_approval and candidate:
        return {
            **base,
            "gate_status": "approved_candidate_observational_only",
            "human_approval_present": True,
            "risk_relaxation_allowed": False,
            "approved_candidate": candidate,
            "approved_risk_relaxation": None,
        }

    if explicit_approval and relaxation:
        return {
            **base,
            "gate_status": "approved_risk_relaxation_observational_only",
            "human_approval_present": True,
            "risk_relaxation_allowed": True,
            "approved_candidate": None,
            "approved_risk_relaxation": relaxation,
        }

    return {
        **base,
        "gate_status": "blocked_pending_manual_approval",
        "human_approval_present": False,
        "risk_relaxation_allowed": False,
        "approved_candidate": None,
        "approved_risk_relaxation": None,
    }


def manual_approval_gate_to_markdown(payload: dict[str, Any]) -> str:
    safety = payload.get("safety") or {}
    required = payload.get("required_approval") or {}
    manual_review = payload.get("manual_review") or {}
    dimension = required.get("dimension")
    value = required.get("value")
    axis = f"{dimension}={value}" if dimension is not None and value is not None else "None"
    lines = [
        "# Manual Approval Gate",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Gate status: {payload.get('gate_status')}",
        f"Human approval present: {payload.get('human_approval_present')}",
        f"Risk relaxation allowed: {payload.get('risk_relaxation_allowed')}",
        f"automated_rerun_allowed: {payload.get('automated_rerun_allowed')}",
        "",
        "## Manual review source",
        f"- review_status: {manual_review.get('review_status')}",
        f"- primary_issue: {manual_review.get('primary_issue')}",
        f"- repair_status: {manual_review.get('repair_status')}",
        f"- candidate_count: {manual_review.get('candidate_count')}",
        "",
        "## Required approval",
        f"- decision_required: {required.get('decision_required')}",
        f"- axis: {axis}",
        f"- best_available_max_drawdown: {required.get('best_available_max_drawdown')}",
        f"- drawdown_gap_to_limit: {required.get('drawdown_gap_to_limit')}",
        "",
        "## Safety",
        f"- queue_write_allowed: {safety.get('queue_write_allowed')}",
        f"- broad_daemon_allowed: {safety.get('broad_daemon_allowed')}",
        f"- automation_allowed: {safety.get('automation_allowed')}",
        f"- live_trading_enabled: {payload.get('live_trading_enabled')}",
    ]
    return "\n".join(lines) + "\n"


def write_manual_approval_gate(
    *,
    manual_review_path: str | Path = DEFAULT_MANUAL_REVIEW_PATH,
    approval_path: str | Path | None = None,
    json_path: str | Path = DEFAULT_JSON_PATH,
    markdown_path: str | Path = DEFAULT_MARKDOWN_PATH,
) -> dict[str, Any]:
    payload = build_manual_approval_gate(manual_review_path, approval_path)
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(manual_approval_gate_to_markdown(payload), encoding="utf-8")
    return payload
