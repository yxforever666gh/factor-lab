from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SAFETY_FLAGS = {
    "queue_write_allowed": False,
    "broad_daemon_allowed": False,
    "automation_allowed": False,
    "automated_rerun_allowed": False,
    "live_trading_enabled": False,
    "execution_allowed": False,
}


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _missing_artifacts(named_payloads: dict[str, dict[str, Any]]) -> list[str]:
    return sorted(name for name, payload in named_payloads.items() if not payload)


def _decision_axis(blocker: dict[str, Any]) -> str | None:
    manual_review = blocker.get("manual_review") or {}
    dimension = blocker.get("manual_review_dimension") or manual_review.get("dimension")
    value = blocker.get("manual_review_value") or manual_review.get("value")
    if dimension is None or value is None:
        return None
    return f"{dimension}={value}"


def _blocker_value(payload: dict[str, Any], section: str, key: str) -> Any:
    nested = payload.get(section) or {}
    if isinstance(nested, dict) and key in nested:
        return nested.get(key)
    return payload.get(key)


def build_operator_approval_packet(
    *,
    status_path: str | Path,
    diagnostics_path: str | Path,
    drawdown_blocker_evidence_path: str | Path,
    manual_review_path: str | Path,
    intake_validation_path: str | Path,
    handoff_path: str | Path,
) -> dict[str, Any]:
    status = load_json(status_path)
    diagnostics = load_json(diagnostics_path)
    blocker = load_json(drawdown_blocker_evidence_path)
    manual_review = load_json(manual_review_path)
    intake = load_json(intake_validation_path)
    handoff = load_json(handoff_path)

    missing = _missing_artifacts(
        {
            "status": status,
            "diagnostics": diagnostics,
            "drawdown_blocker_evidence": blocker,
            "manual_review": manual_review,
            "intake_validation": intake,
            "handoff": handoff,
        }
    )
    paper = status.get("paper_portfolio") or {}
    benchmark = diagnostics.get("benchmark") or {}
    turnover = diagnostics.get("turnover") or {}
    cost = diagnostics.get("cost") or {}

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "packet_status": "ready" if not missing else "incomplete",
        "missing_artifacts": missing,
        "portfolio": {
            "strategy_name": paper.get("strategy_name") or status.get("strategy_name"),
            "as_of_date": paper.get("as_of_date"),
            "position_count": paper.get("position_count"),
            "next_action": status.get("next_action"),
        },
        "benchmark": {
            "benchmark_id": benchmark.get("benchmark_id") or paper.get("benchmark_id"),
            "benchmark_name": benchmark.get("benchmark_name"),
            "tracking_mode": benchmark.get("tracking_mode"),
        },
        "turnover_cost": {
            "added_count": turnover.get("added_count"),
            "removed_count": turnover.get("removed_count"),
            "overlap_count": turnover.get("overlap_count"),
            "turnover_one_way_estimate": turnover.get("turnover_one_way_estimate") or paper.get("turnover_one_way_estimate"),
            "cost_bps": cost.get("cost_bps"),
            "estimated_one_way_cost": cost.get("estimated_one_way_cost"),
            "estimated_round_trip_cost": cost.get("estimated_round_trip_cost") or paper.get("estimated_round_trip_cost"),
        },
        "paper_monitoring": status.get("paper_monitoring") or {},
        "drawdown_blocker": {
            "primary_issue": _blocker_value(blocker, "blocker", "primary_issue"),
            "repair_status": _blocker_value(blocker, "repair", "repair_status"),
            "candidate_count": int(_blocker_value(blocker, "repair", "candidate_count") or 0),
            "best_available_max_drawdown": manual_review.get("best_available_max_drawdown") or _blocker_value(blocker, "repair", "best_available_max_drawdown"),
            "drawdown_gap_to_limit": manual_review.get("drawdown_gap_to_limit") or _blocker_value(blocker, "repair", "drawdown_gap_to_limit"),
            "decision_axis": _decision_axis(blocker),
        },
        "operator_state": {
            "intake_status": intake.get("intake_status") or "missing",
            "validation_errors": intake.get("validation_errors") or [],
            "non_mutating": bool(intake.get("non_mutating", True)),
            "handoff_status": handoff.get("handoff_status") or "awaiting_operator_decision",
            "execution_allowed": bool(handoff.get("execution_allowed")),
            "separate_execution_plan_required": bool(handoff.get("separate_execution_plan_required")),
        },
        "safety": dict(SAFETY_FLAGS),
        "source_artifacts": {
            "status_path": str(status_path),
            "diagnostics_path": str(diagnostics_path),
            "drawdown_blocker_evidence_path": str(drawdown_blocker_evidence_path),
            "manual_review_path": str(manual_review_path),
            "intake_validation_path": str(intake_validation_path),
            "handoff_path": str(handoff_path),
        },
    }


def operator_approval_packet_to_markdown(payload: dict[str, Any]) -> str:
    portfolio = payload.get("portfolio") or {}
    benchmark = payload.get("benchmark") or {}
    turnover_cost = payload.get("turnover_cost") or {}
    monitoring = payload.get("paper_monitoring") or {}
    blocker = payload.get("drawdown_blocker") or {}
    operator_state = payload.get("operator_state") or {}
    safety = payload.get("safety") or {}
    missing = payload.get("missing_artifacts") or []
    errors = operator_state.get("validation_errors") or []

    lines = [
        "# Operator Approval Packet",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Packet status: {payload.get('packet_status')}",
        "",
        "## Portfolio",
        f"- strategy_name: {portfolio.get('strategy_name')}",
        f"- as_of_date: {portfolio.get('as_of_date')}",
        f"- position_count: {portfolio.get('position_count')}",
        f"- next_action: {portfolio.get('next_action')}",
        "",
        "## Benchmark",
        f"- benchmark_id: {benchmark.get('benchmark_id')}",
        f"- benchmark_name: {benchmark.get('benchmark_name')}",
        f"- tracking_mode: {benchmark.get('tracking_mode')}",
        "",
        "## Turnover and cost",
        f"- added_count: {turnover_cost.get('added_count')}",
        f"- removed_count: {turnover_cost.get('removed_count')}",
        f"- overlap_count: {turnover_cost.get('overlap_count')}",
        f"- turnover_one_way_estimate: {turnover_cost.get('turnover_one_way_estimate')}",
        f"- cost_bps: {turnover_cost.get('cost_bps')}",
        f"- estimated_one_way_cost: {turnover_cost.get('estimated_one_way_cost')}",
        f"- estimated_round_trip_cost: {turnover_cost.get('estimated_round_trip_cost')}",
        "",
        "## Paper monitoring",
        f"- weekly_report_status: {monitoring.get('weekly_report_status')}",
        f"- missing_artifacts: {monitoring.get('missing_artifacts')}",
        f"- runtime_safe: {monitoring.get('runtime_safe')}",
        "",
        "## Current blocker",
        f"- primary_issue: {blocker.get('primary_issue')}",
        f"- repair_status: {blocker.get('repair_status')}",
        f"- candidate_count: {blocker.get('candidate_count')}",
        f"- best_available_max_drawdown: {blocker.get('best_available_max_drawdown')}",
        f"- drawdown_gap_to_limit: {blocker.get('drawdown_gap_to_limit')}",
        f"- decision_axis: {blocker.get('decision_axis')}",
        "",
        "## Operator state",
        f"- intake_status: {operator_state.get('intake_status')}",
        f"- handoff_status: {operator_state.get('handoff_status')}",
        f"- non_mutating: {operator_state.get('non_mutating')}",
        f"- execution_allowed: {operator_state.get('execution_allowed')}",
        "",
        "## Safety flags",
        f"- queue_write_allowed: {safety.get('queue_write_allowed')}",
        f"- broad_daemon_allowed: {safety.get('broad_daemon_allowed')}",
        f"- automation_allowed: {safety.get('automation_allowed')}",
        f"- automated_rerun_allowed: {safety.get('automated_rerun_allowed')}",
        f"- live_trading_enabled: {safety.get('live_trading_enabled')}",
        f"- execution_allowed: {safety.get('execution_allowed')}",
        "",
        "## Missing artifacts",
    ]
    if missing:
        lines.extend(f"- {item}" for item in missing)
    else:
        lines.append("- none")
    lines.extend(["", "## Validation errors"])
    if errors:
        lines.extend(f"- {error}" for error in errors)
    else:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def write_operator_approval_packet(
    *,
    status_path: str | Path,
    diagnostics_path: str | Path,
    drawdown_blocker_evidence_path: str | Path,
    manual_review_path: str | Path,
    intake_validation_path: str | Path,
    handoff_path: str | Path,
    json_path: str | Path,
    markdown_path: str | Path,
) -> dict[str, Any]:
    payload = build_operator_approval_packet(
        status_path=status_path,
        diagnostics_path=diagnostics_path,
        drawdown_blocker_evidence_path=drawdown_blocker_evidence_path,
        manual_review_path=manual_review_path,
        intake_validation_path=intake_validation_path,
        handoff_path=handoff_path,
    )
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(operator_approval_packet_to_markdown(payload), encoding="utf-8")
    return payload
