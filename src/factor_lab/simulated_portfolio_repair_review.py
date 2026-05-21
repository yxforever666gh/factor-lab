from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def load_repair_diagnostics(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def build_repair_blocker_review(repair_payload: dict[str, Any]) -> dict[str, Any]:
    if not repair_payload:
        return {
            "schema_version": 1,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "review_status": "missing_repair_diagnostics",
            "primary_blocker": "missing_repair_diagnostics",
            "candidate_count": 0,
            "recommended_candidate": None,
            "best_available_max_drawdown": None,
            "drawdown_gap_to_limit": None,
            "automation_allowed": False,
            "manual_decision_required": True,
            "recommended_action": "write_or_repair_simulated_portfolio_construction_diagnostics",
        }

    repair_status = str(repair_payload.get("repair_status") or "unknown")
    candidate_count = int(repair_payload.get("candidate_count") or 0)
    automation_allowed = bool(repair_payload.get("automation_allowed"))
    no_safe_candidate = repair_status == "blocked_no_drawdown_safe_candidate" or (
        candidate_count == 0 and not automation_allowed
    )

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "review_status": "manual_review_required" if no_safe_candidate else "review_ready",
        "primary_blocker": repair_status,
        "candidate_count": candidate_count,
        "recommended_candidate": repair_payload.get("recommended_candidate"),
        "best_available_max_drawdown": repair_payload.get("best_available_max_drawdown"),
        "drawdown_gap_to_limit": repair_payload.get("drawdown_gap_to_limit"),
        "automation_allowed": automation_allowed,
        "manual_decision_required": not automation_allowed or no_safe_candidate,
        "recommended_action": "manual_review_drawdown_tradeoff_before_any_automation"
        if no_safe_candidate
        else "review_candidate_before_controlled_simulation_rerun",
    }


def repair_review_to_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Simulated Portfolio Repair Blocker Review",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"review_status: {payload.get('review_status')}",
        f"primary_blocker: {payload.get('primary_blocker')}",
        f"candidate_count: {payload.get('candidate_count')}",
        f"recommended_candidate: {payload.get('recommended_candidate')}",
        f"best_available_max_drawdown: {payload.get('best_available_max_drawdown')}",
        f"drawdown_gap_to_limit: {payload.get('drawdown_gap_to_limit')}",
        f"automation_allowed: {payload.get('automation_allowed')}",
        f"manual_decision_required: {payload.get('manual_decision_required')}",
        f"recommended_action: {payload.get('recommended_action')}",
    ]
    return "\n".join(lines) + "\n"


def write_repair_blocker_review(
    *,
    repair_path: str | Path,
    json_path: str | Path,
    markdown_path: str | Path,
) -> dict[str, Any]:
    repair_payload = load_repair_diagnostics(repair_path)
    payload = build_repair_blocker_review(repair_payload)

    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(repair_review_to_markdown(payload), encoding="utf-8")
    return payload
