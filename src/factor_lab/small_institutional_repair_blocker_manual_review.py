from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_EVIDENCE_PATH = Path("artifacts/small_institutional_simulation/drawdown_blocker_evidence.json")
DEFAULT_REPAIR_PATH = Path("artifacts/small_institutional_simulation/portfolio_construction_repair.json")
DEFAULT_GROUP_DIAGNOSTIC_PATH = Path("artifacts/small_institutional_simulation/drawdown_group_diagnostic.json")
DEFAULT_JSON_PATH = Path("artifacts/small_institutional_simulation/repair_blocker_manual_review.json")
DEFAULT_MARKDOWN_PATH = Path("artifacts/small_institutional_simulation/repair_blocker_manual_review.md")


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _safe_bool(value: Any) -> bool:
    return bool(value) if value is not None else False


def _manual_axis(evidence: dict[str, Any], group_diagnostic: dict[str, Any]) -> dict[str, Any]:
    evidence_review = evidence.get("manual_review") or {}
    group_axis = group_diagnostic.get("recommended_manual_axis") or {}
    return {
        "dimension": evidence_review.get("dimension") or group_axis.get("dimension"),
        "value": evidence_review.get("value") or group_axis.get("value"),
        "best_max_drawdown": evidence_review.get("best_max_drawdown") or group_axis.get("best_max_drawdown"),
        "drawdown_gap_to_limit": evidence_review.get("drawdown_gap_to_limit") or group_axis.get("drawdown_gap_to_limit"),
    }


def build_repair_blocker_manual_review(
    evidence_path: str | Path,
    repair_path: str | Path,
    group_diagnostic_path: str | Path,
) -> dict[str, Any]:
    evidence = load_json(evidence_path)
    repair_artifact = load_json(repair_path)
    group_diagnostic = load_json(group_diagnostic_path)
    safety = {
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
    }
    base = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_artifacts": {
            "evidence_path": str(evidence_path),
            "repair_path": str(repair_path),
            "group_diagnostic_path": str(group_diagnostic_path),
        },
        "safety": safety,
    }
    if not evidence:
        return {
            **base,
            "review_status": "missing_evidence",
            "primary_issue": None,
            "repair_status": None,
            "candidate_count": 0,
            "recommended_manual_decision": {
                "decision_required": False,
                "dimension": None,
                "value": None,
                "automated_rerun_allowed": False,
                "notes": "Drawdown blocker evidence is missing; no automated action is allowed.",
            },
        }

    blocker = evidence.get("blocker") or {}
    evidence_repair = evidence.get("repair") or {}
    repair = {**repair_artifact, **evidence_repair}
    context = evidence.get("paper_portfolio_context") or {}
    axis = _manual_axis(evidence, group_diagnostic)
    return {
        **base,
        "review_status": "blocked_manual_review_required",
        "primary_issue": blocker.get("primary_issue"),
        "severity": blocker.get("severity"),
        "repair_status": repair.get("repair_status"),
        "candidate_count": int(repair.get("candidate_count") or 0),
        "best_available_max_drawdown": repair.get("best_available_max_drawdown") or axis.get("best_max_drawdown"),
        "drawdown_gap_to_limit": repair.get("drawdown_gap_to_limit") or axis.get("drawdown_gap_to_limit"),
        "paper_portfolio_context": context,
        "recommended_manual_decision": {
            "decision_required": True,
            "dimension": axis.get("dimension"),
            "value": axis.get("value"),
            "automated_rerun_allowed": False,
            "notes": "Manual approval is required before any repaired rerun or risk relaxation.",
        },
        "input_safety_flags": {
            "evidence_automation_allowed": _safe_bool((evidence.get("safety") or {}).get("automation_allowed")),
            "repair_automation_allowed": _safe_bool(repair.get("automation_allowed")),
            "group_automation_allowed": _safe_bool(group_diagnostic.get("automation_allowed")),
        },
    }


def manual_review_to_markdown(payload: dict[str, Any]) -> str:
    context = payload.get("paper_portfolio_context") or {}
    safety = payload.get("safety") or {}
    decision = payload.get("recommended_manual_decision") or {}
    dimension = decision.get("dimension")
    value = decision.get("value")
    axis = f"{dimension}={value}" if dimension is not None and value is not None else "None"
    lines = [
        "# Repair Blocker Manual Review",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Review status: {payload.get('review_status')}",
        f"Primary issue: {payload.get('primary_issue')}",
        f"Repair status: {payload.get('repair_status')}",
        f"Candidate count: {payload.get('candidate_count')}",
        f"Best available max drawdown: {payload.get('best_available_max_drawdown')}",
        f"Drawdown gap to limit: {payload.get('drawdown_gap_to_limit')}",
        "",
        "## Paper portfolio context",
        f"- benchmark_id: {context.get('benchmark_id')}",
        f"- benchmark_name: {context.get('benchmark_name')}",
        f"- tracking_mode: {context.get('tracking_mode')}",
        f"- turnover_one_way_estimate: {context.get('turnover_one_way_estimate')}",
        f"- estimated_round_trip_cost: {context.get('estimated_round_trip_cost')}",
        "",
        "## Safety",
        f"- queue_write_allowed: {safety.get('queue_write_allowed')}",
        f"- broad_daemon_allowed: {safety.get('broad_daemon_allowed')}",
        f"- automation_allowed: {safety.get('automation_allowed')}",
        "",
        "## Recommended manual decision",
        f"- decision_required: {decision.get('decision_required')}",
        f"- axis: {axis}",
        f"- automated_rerun_allowed: {decision.get('automated_rerun_allowed')}",
        f"- notes: {decision.get('notes')}",
    ]
    return "\n".join(lines) + "\n"


def write_repair_blocker_manual_review(
    *,
    evidence_path: str | Path = DEFAULT_EVIDENCE_PATH,
    repair_path: str | Path = DEFAULT_REPAIR_PATH,
    group_diagnostic_path: str | Path = DEFAULT_GROUP_DIAGNOSTIC_PATH,
    json_path: str | Path = DEFAULT_JSON_PATH,
    markdown_path: str | Path = DEFAULT_MARKDOWN_PATH,
) -> dict[str, Any]:
    payload = build_repair_blocker_manual_review(evidence_path, repair_path, group_diagnostic_path)
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(manual_review_to_markdown(payload), encoding="utf-8")
    return payload
