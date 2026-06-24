from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _portfolio_summary(portfolio: dict[str, Any]) -> dict[str, Any]:
    positions = portfolio.get("positions") or []
    return {
        "strategy_name": portfolio.get("strategy_name"),
        "as_of_date": portfolio.get("as_of_date"),
        "position_count": portfolio.get("position_count", len(positions) if isinstance(positions, list) else None),
    }


def _top_position_preview(portfolio: dict[str, Any], limit: int = 10) -> list[dict[str, Any]]:
    positions = portfolio.get("positions") or []
    if not isinstance(positions, list):
        return []
    preview: list[dict[str, Any]] = []
    for row in positions[:limit]:
        if not isinstance(row, dict):
            continue
        preview.append(
            {
                "ticker": row.get("ticker"),
                "weight": row.get("weight"),
                "signal": row.get("signal"),
            }
        )
    return preview


def _change_counts(turnover: dict[str, Any]) -> dict[str, Any]:
    if not turnover:
        return {}
    return {
        "history_status": turnover.get("history_status"),
        "added_count": turnover.get("added_count"),
        "removed_count": turnover.get("removed_count"),
        "overlap_count": turnover.get("overlap_count"),
    }


def _blocker_context(status: dict[str, Any]) -> dict[str, Any]:
    if not status:
        return {}
    simulation = status.get("small_institutional_simulation") or status.get("simulation_self_diagnosis") or {}
    manual_gate = status.get("manual_approval_gate") or {}
    operator_summary = status.get("operator_approval_summary") or {}
    return {
        "decision": status.get("decision"),
        "next_action": status.get("next_action"),
        "primary_issue": simulation.get("primary_issue"),
        "manual_approval_gate_status": manual_gate.get("gate_status"),
        "human_approval_present": manual_gate.get("human_approval_present"),
        "approval_required": operator_summary.get("approval_required"),
        "required_decision_axis": operator_summary.get("required_decision_axis"),
    }


def _runtime_flags(status: dict[str, Any]) -> dict[str, Any]:
    runtime_safety = status.get("runtime_safety") or {}
    return {
        "safe": runtime_safety.get("safe", True),
        "would_run_count": int(runtime_safety.get("would_run_count") or 0),
        "recommendations": runtime_safety.get("recommendations") or [],
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
    }


def _operator_pending_observation_summary(observation: dict[str, Any]) -> dict[str, Any]:
    if not observation:
        return {"observation_status": "missing"}
    benchmark_raw = observation.get("benchmark")
    turnover_raw = observation.get("turnover")
    cost_raw = observation.get("cost")
    blocker_raw = observation.get("blocker")
    benchmark: dict[str, Any] = benchmark_raw if isinstance(benchmark_raw, dict) else {}
    turnover: dict[str, Any] = turnover_raw if isinstance(turnover_raw, dict) else {}
    cost: dict[str, Any] = cost_raw if isinstance(cost_raw, dict) else {}
    blocker: dict[str, Any] = blocker_raw if isinstance(blocker_raw, dict) else {}
    return {
        "observation_status": observation.get("observation_status"),
        "primary_issue": observation.get("primary_issue", blocker.get("primary_issue")),
        "manual_approval_status": observation.get("manual_approval_status", blocker.get("manual_approval_status")),
        "benchmark_id": observation.get("benchmark_id", benchmark.get("benchmark_id")),
        "benchmark_name": observation.get("benchmark_name", benchmark.get("benchmark_name")),
        "tracking_mode": observation.get("tracking_mode", benchmark.get("tracking_mode")),
        "turnover_one_way_estimate": observation.get(
            "turnover_one_way_estimate", turnover.get("turnover_one_way_estimate")
        ),
        "estimated_round_trip_cost": observation.get("estimated_round_trip_cost", cost.get("estimated_round_trip_cost")),
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
    }


def build_weekly_paper_report(
    current_portfolio_path: str | Path,
    diagnostics_path: str | Path,
    status_path: str | Path,
    operator_pending_observation_path: str | Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    portfolio = _load_json(current_portfolio_path)
    diagnostics = _load_json(diagnostics_path)
    status = _load_json(status_path)
    operator_pending_observation = _load_json(operator_pending_observation_path) if operator_pending_observation_path else {}

    missing_artifacts: list[str] = []
    if not portfolio:
        missing_artifacts.append("current_portfolio")
    if not diagnostics:
        missing_artifacts.append("portfolio_diagnostics")
    if not status:
        missing_artifacts.append("small_institutionalization_status")
    if operator_pending_observation_path and not operator_pending_observation:
        missing_artifacts.append("operator_pending_observation")

    benchmark = diagnostics.get("benchmark") or {}
    turnover = diagnostics.get("turnover") or {}
    cost = diagnostics.get("cost") or {}

    return {
        "schema_version": 1,
        "generated_at_utc": generated_at or datetime.now(timezone.utc).isoformat(),
        "cadence": "weekly",
        "portfolio": _portfolio_summary(portfolio),
        "benchmark": benchmark,
        "turnover": turnover,
        "cost": cost,
        "changes": _change_counts(turnover),
        "top_positions": _top_position_preview(portfolio),
        "blockers": _blocker_context(status),
        "operator_pending_observation": _operator_pending_observation_summary(operator_pending_observation),
        "runtime": _runtime_flags(status),
        "missing_artifacts": missing_artifacts,
        "next_observation_window": "next_weekly_paper_review",
    }


def weekly_report_to_markdown(payload: dict[str, Any]) -> str:
    portfolio = payload.get("portfolio") or {}
    benchmark = payload.get("benchmark") or {}
    turnover = payload.get("turnover") or {}
    cost = payload.get("cost") or {}
    changes = payload.get("changes") or {}
    blockers = payload.get("blockers") or {}
    operator_pending_observation = payload.get("operator_pending_observation") or {}
    runtime = payload.get("runtime") or {}
    missing = payload.get("missing_artifacts") or []
    top_positions = payload.get("top_positions") or []

    lines = [
        "# Weekly Paper Monitoring Report",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Cadence: {payload.get('cadence')}",
        "",
        "## Portfolio",
        f"- Strategy: {portfolio.get('strategy_name')}",
        f"- As-of date: {portfolio.get('as_of_date')}",
        f"- Position count: {portfolio.get('position_count')}",
        "",
        "## Benchmark",
        f"- benchmark_id: {benchmark.get('benchmark_id')}",
        f"- benchmark_name: {benchmark.get('benchmark_name')}",
        f"- tracking_mode: {benchmark.get('tracking_mode')}",
        "",
        "## Turnover and cost",
        f"- turnover_one_way_estimate: {turnover.get('turnover_one_way_estimate')}",
        f"- estimated_one_way_cost: {cost.get('estimated_one_way_cost')}",
        f"- estimated_round_trip_cost: {cost.get('estimated_round_trip_cost')}",
        "",
        "## Changes",
        f"- history_status: {changes.get('history_status')}",
        f"- added_count: {changes.get('added_count')}",
        f"- removed_count: {changes.get('removed_count')}",
        f"- overlap_count: {changes.get('overlap_count')}",
        "",
        "## Top positions",
    ]
    if top_positions:
        for row in top_positions:
            if not isinstance(row, dict):
                continue
            lines.append(f"- {row.get('ticker')}: weight={row.get('weight')}, signal={row.get('signal')}")
    else:
        lines.append("- None")

    lines.extend(
        [
            "",
            "## Blockers",
            f"- decision: {blockers.get('decision')}",
            f"- primary_issue: {blockers.get('primary_issue')}",
            f"- manual_approval_gate_status: {blockers.get('manual_approval_gate_status')}",
            f"- required_decision_axis: {blockers.get('required_decision_axis')}",
            f"- next_action: {blockers.get('next_action')}",
            "",
            "## Operator-pending observation",
            f"- observation_status: {operator_pending_observation.get('observation_status')}",
            f"- primary_issue: {operator_pending_observation.get('primary_issue')}",
            f"- manual_approval_status: {operator_pending_observation.get('manual_approval_status')}",
            f"- benchmark_id: {operator_pending_observation.get('benchmark_id')}",
            f"- benchmark_name: {operator_pending_observation.get('benchmark_name')}",
            f"- tracking_mode: {operator_pending_observation.get('tracking_mode')}",
            f"- turnover_one_way_estimate: {operator_pending_observation.get('turnover_one_way_estimate')}",
            f"- estimated_round_trip_cost: {operator_pending_observation.get('estimated_round_trip_cost')}",
            f"- queue_write_allowed: {operator_pending_observation.get('queue_write_allowed')}",
            f"- broad_daemon_allowed: {operator_pending_observation.get('broad_daemon_allowed')}",
            f"- automation_allowed: {operator_pending_observation.get('automation_allowed')}",
            f"- automated_rerun_allowed: {operator_pending_observation.get('automated_rerun_allowed')}",
            f"- live_trading_enabled: {operator_pending_observation.get('live_trading_enabled')}",
            "",
            "## Runtime safety",
            f"- Safe: {runtime.get('safe')}",
            f"- Would run count: {runtime.get('would_run_count')}",
            f"- Recommendations: {', '.join(runtime.get('recommendations') or [])}",
            f"- Queue write allowed: {runtime.get('queue_write_allowed')}",
            f"- Broad daemon allowed: {runtime.get('broad_daemon_allowed')}",
            f"- Automation allowed: {runtime.get('automation_allowed')}",
            f"- Automated rerun allowed: {runtime.get('automated_rerun_allowed')}",
            f"- Live trading enabled: {runtime.get('live_trading_enabled')}",
            "",
            "## Missing artifacts",
            *(f"- {item}" for item in missing),
        ]
    )
    if not missing:
        lines.append("- None")
    lines.extend(["", f"Next observation window: {payload.get('next_observation_window')}"])
    return "\n".join(lines) + "\n"


def write_weekly_paper_report(
    *,
    current_portfolio_path: str | Path,
    diagnostics_path: str | Path,
    status_path: str | Path,
    json_path: str | Path,
    markdown_path: str | Path,
    operator_pending_observation_path: str | Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    payload = build_weekly_paper_report(
        current_portfolio_path=current_portfolio_path,
        diagnostics_path=diagnostics_path,
        status_path=status_path,
        operator_pending_observation_path=operator_pending_observation_path,
        generated_at=generated_at,
    )
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(weekly_report_to_markdown(payload), encoding="utf-8")
    return payload
