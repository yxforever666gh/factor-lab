from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _bool_flag(*values: Any) -> bool:
    return any(bool(value) for value in values)


def build_operator_approval_summary(
    *,
    gate_path: str | Path,
    manual_review_path: str | Path,
    status_path: str | Path,
) -> dict[str, Any]:
    gate = load_json(gate_path)
    manual_review = load_json(manual_review_path)
    status = load_json(status_path)

    decision = manual_review.get("recommended_manual_decision") or {}
    gate_safety = gate.get("safety") or {}
    review_safety = manual_review.get("safety") or {}
    promotion = status.get("paper_live_promotion_readiness") or {}

    dimension = decision.get("dimension")
    value = decision.get("value")
    required_axis = f"{dimension}={value}" if dimension is not None and value is not None else None

    live_trading_enabled = _bool_flag(gate.get("live_trading_enabled"), promotion.get("live_trading_enabled"))
    safety = {
        "queue_write_allowed": _bool_flag(gate.get("queue_write_allowed"), gate_safety.get("queue_write_allowed"), review_safety.get("queue_write_allowed")),
        "broad_daemon_allowed": _bool_flag(gate.get("broad_daemon_allowed"), gate_safety.get("broad_daemon_allowed"), review_safety.get("broad_daemon_allowed")),
        "automation_allowed": _bool_flag(gate.get("automation_allowed"), gate_safety.get("automation_allowed"), review_safety.get("automation_allowed")),
        "automated_rerun_allowed": _bool_flag(gate.get("automated_rerun_allowed"), decision.get("automated_rerun_allowed")),
        "live_trading_enabled": live_trading_enabled,
    }

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "summary_status": gate.get("gate_status") or "missing_manual_approval_gate",
        "approval_required": bool(decision.get("decision_required") or gate.get("gate_status") == "blocked_pending_manual_approval"),
        "human_approval_present": bool(gate.get("human_approval_present")),
        "required_decision_axis": required_axis,
        "primary_blocker": manual_review.get("primary_issue") or (gate.get("required_approval") or {}).get("primary_issue"),
        "repair_status": manual_review.get("repair_status") or (gate.get("manual_review") or {}).get("repair_status"),
        "candidate_count": int(manual_review.get("candidate_count") or (gate.get("manual_review") or {}).get("candidate_count") or 0),
        "best_available_max_drawdown": manual_review.get("best_available_max_drawdown") or (gate.get("required_approval") or {}).get("best_available_max_drawdown"),
        "drawdown_gap_to_limit": manual_review.get("drawdown_gap_to_limit") or (gate.get("required_approval") or {}).get("drawdown_gap_to_limit"),
        "next_action": status.get("next_action"),
        "safety": safety,
        "checklist": [
            "approved candidate id OR approved risk relaxation",
            "explicit scope",
            "explicit reason",
            "no queue/broad daemon/live trading permission",
        ],
    }


def operator_approval_summary_to_markdown(payload: dict[str, Any]) -> str:
    safety = payload.get("safety") or {}
    checklist = payload.get("checklist") or []
    lines = [
        "# Operator Approval Summary",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Summary status: {payload.get('summary_status')}",
        f"Approval required: {payload.get('approval_required')}",
        f"Required decision axis: {payload.get('required_decision_axis')}",
        f"Primary blocker: {payload.get('primary_blocker')}",
        f"Repair status: {payload.get('repair_status')}",
        f"Candidate count: {payload.get('candidate_count')}",
        f"Best available max drawdown: {payload.get('best_available_max_drawdown')}",
        f"Drawdown gap to limit: {payload.get('drawdown_gap_to_limit')}",
        f"Next action: {payload.get('next_action')}",
        "",
        "## Safety flags",
        f"- queue_write_allowed: {safety.get('queue_write_allowed')}",
        f"- broad_daemon_allowed: {safety.get('broad_daemon_allowed')}",
        f"- automation_allowed: {safety.get('automation_allowed')}",
        f"- automated_rerun_allowed: {safety.get('automated_rerun_allowed')}",
        f"- live_trading_enabled: {safety.get('live_trading_enabled')}",
        "",
        "## Required human approval checklist",
    ]
    lines.extend(f"- {item}" for item in checklist)
    return "\n".join(lines) + "\n"


def write_operator_approval_summary(
    *,
    gate_path: str | Path,
    manual_review_path: str | Path,
    status_path: str | Path,
    json_path: str | Path,
    markdown_path: str | Path,
) -> dict[str, Any]:
    payload = build_operator_approval_summary(gate_path=gate_path, manual_review_path=manual_review_path, status_path=status_path)
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(operator_approval_summary_to_markdown(payload), encoding="utf-8")
    return payload
