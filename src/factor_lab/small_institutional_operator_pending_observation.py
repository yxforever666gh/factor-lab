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


def _blocker_context(status: dict[str, Any]) -> dict[str, Any]:
    if not status:
        return {}
    simulation = status.get("small_institutional_simulation") or status.get("simulation_self_diagnosis") or {}
    repair = status.get("simulated_portfolio_construction_repair") or {}
    manual_gate = status.get("manual_approval_gate") or {}
    operator_summary = status.get("operator_approval_summary") or {}
    return {
        "primary_issue": simulation.get("primary_issue"),
        "repair_status": repair.get("repair_status"),
        "decision_axis": operator_summary.get("required_decision_axis"),
        "manual_approval_status": manual_gate.get("gate_status"),
        "human_approval_present": manual_gate.get("human_approval_present"),
        "approval_required": operator_summary.get("approval_required"),
        "status_decision": status.get("decision"),
        "status_next_action": status.get("next_action"),
    }


def _runtime_flags(status: dict[str, Any], weekly_report: dict[str, Any]) -> dict[str, Any]:
    weekly_runtime = weekly_report.get("runtime") or {}
    runtime_safety = status.get("runtime_safety") or {}
    manual_gate = status.get("manual_approval_gate") or {}
    operator_summary = status.get("operator_approval_summary") or {}
    handoff = status.get("operator_decision_handoff") or {}
    return {
        "safe": bool(weekly_runtime.get("safe", runtime_safety.get("safe", True))),
        "queue_write_allowed": bool(
            weekly_runtime.get("queue_write_allowed")
            or manual_gate.get("queue_write_allowed")
            or operator_summary.get("queue_write_allowed")
            or handoff.get("queue_write_allowed")
        ),
        "broad_daemon_allowed": bool(
            weekly_runtime.get("broad_daemon_allowed")
            or manual_gate.get("broad_daemon_allowed")
            or operator_summary.get("broad_daemon_allowed")
            or handoff.get("broad_daemon_allowed")
        ),
        "automated_rerun_allowed": bool(
            weekly_runtime.get("automated_rerun_allowed")
            or manual_gate.get("automated_rerun_allowed")
            or operator_summary.get("automated_rerun_allowed")
            or handoff.get("automated_rerun_allowed")
        ),
        "automation_allowed": bool(
            manual_gate.get("automation_allowed")
            or operator_summary.get("automation_allowed")
            or handoff.get("automation_allowed")
        ),
        "live_trading_enabled": bool(
            weekly_runtime.get("live_trading_enabled")
            or manual_gate.get("live_trading_enabled")
            or operator_summary.get("live_trading_enabled")
            or handoff.get("live_trading_enabled")
        ),
    }


def build_operator_pending_observation(
    weekly_report_path: str | Path,
    status_path: str | Path,
    generated_at: str | None = None,
) -> dict[str, Any]:
    weekly_report = _load_json(weekly_report_path)
    status = _load_json(status_path)

    missing_artifacts: list[str] = []
    if not weekly_report:
        missing_artifacts.append("weekly_monitoring_report")
    if not status:
        missing_artifacts.append("small_institutionalization_status")

    observation_status = "missing_artifacts" if missing_artifacts else "operator_pending"
    return {
        "schema_version": 1,
        "generated_at_utc": generated_at or datetime.now(timezone.utc).isoformat(),
        "observation_status": observation_status,
        "portfolio": weekly_report.get("portfolio") or {},
        "weekly_report": {
            "cadence": weekly_report.get("cadence"),
            "generated_at_utc": weekly_report.get("generated_at_utc"),
            "next_observation_window": weekly_report.get("next_observation_window"),
        }
        if weekly_report
        else {},
        "benchmark": weekly_report.get("benchmark") or {},
        "turnover": {
            "history_status": (weekly_report.get("turnover") or {}).get("history_status"),
            "turnover_one_way_estimate": (weekly_report.get("turnover") or {}).get("turnover_one_way_estimate"),
        }
        if weekly_report.get("turnover")
        else {},
        "cost": {
            "cost_bps": (weekly_report.get("cost") or {}).get("cost_bps"),
            "estimated_round_trip_cost": (weekly_report.get("cost") or {}).get("estimated_round_trip_cost"),
        }
        if weekly_report.get("cost")
        else {},
        "blocker": _blocker_context(status),
        "runtime": _runtime_flags(status, weekly_report),
        "missing_artifacts": missing_artifacts,
        "next_action": "await_operator_decision_no_automation",
    }


def operator_pending_observation_to_markdown(payload: dict[str, Any]) -> str:
    portfolio = payload.get("portfolio") or {}
    weekly = payload.get("weekly_report") or {}
    benchmark = payload.get("benchmark") or {}
    turnover = payload.get("turnover") or {}
    cost = payload.get("cost") or {}
    blocker = payload.get("blocker") or {}
    runtime = payload.get("runtime") or {}
    missing = payload.get("missing_artifacts") or []

    lines = [
        "# Operator-Pending Observation",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Observation status: {payload.get('observation_status')}",
        "",
        "## Portfolio",
        f"- Strategy: {portfolio.get('strategy_name')}",
        f"- As-of date: {portfolio.get('as_of_date')}",
        f"- Position count: {portfolio.get('position_count')}",
        "",
        "## Weekly report freshness",
        f"- Cadence: {weekly.get('cadence')}",
        f"- Weekly generated_at_utc: {weekly.get('generated_at_utc')}",
        f"- Next observation window: {weekly.get('next_observation_window')}",
        "",
        "## Benchmark / turnover / cost",
        f"- benchmark_id: {benchmark.get('benchmark_id')}",
        f"- benchmark_name: {benchmark.get('benchmark_name')}",
        f"- tracking_mode: {benchmark.get('tracking_mode')}",
        f"- turnover_one_way_estimate: {turnover.get('turnover_one_way_estimate')}",
        f"- estimated_round_trip_cost: {cost.get('estimated_round_trip_cost')}",
        "",
        "## Blocker",
        f"- primary_issue: {blocker.get('primary_issue')}",
        f"- repair_status: {blocker.get('repair_status')}",
        f"- decision_axis: {blocker.get('decision_axis')}",
        f"- manual_approval_status: {blocker.get('manual_approval_status')}",
        f"- human_approval_present: {blocker.get('human_approval_present')}",
        "",
        "## Runtime safety",
        f"- Safe: {runtime.get('safe')}",
        f"- Queue write allowed: {runtime.get('queue_write_allowed')}",
        f"- Broad daemon allowed: {runtime.get('broad_daemon_allowed')}",
        f"- Automated rerun allowed: {runtime.get('automated_rerun_allowed')}",
        f"- Automation allowed: {runtime.get('automation_allowed')}",
        f"- Live trading enabled: {runtime.get('live_trading_enabled')}",
        "",
        "## Missing artifacts",
    ]
    if missing:
        lines.extend(f"- {item}" for item in missing)
    else:
        lines.append("- None")
    lines.extend(["", f"Next action: {payload.get('next_action')}"])
    return "\n".join(lines) + "\n"


def write_operator_pending_observation(
    *,
    weekly_report_path: str | Path,
    status_path: str | Path,
    json_path: str | Path,
    markdown_path: str | Path,
    generated_at: str | None = None,
) -> dict[str, Any]:
    payload = build_operator_pending_observation(
        weekly_report_path=weekly_report_path,
        status_path=status_path,
        generated_at=generated_at,
    )
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(operator_pending_observation_to_markdown(payload), encoding="utf-8")
    return payload
