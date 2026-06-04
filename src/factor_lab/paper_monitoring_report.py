from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CURRENT_PORTFOLIO_PATH = ROOT / "artifacts" / "paper_portfolio" / "current_portfolio.json"
DEFAULT_DIAGNOSTICS_PATH = ROOT / "artifacts" / "paper_portfolio" / "portfolio_diagnostics.json"
DEFAULT_STATUS_PATH = ROOT / "artifacts" / "small_institutionalization" / "status.json"
DEFAULT_DRY_RUN_PATH = ROOT / "artifacts" / "controlled_restart_dry_run.json"
DEFAULT_RUNTIME_AUDIT_PATH = ROOT / "artifacts" / "runtime_takeover_audit.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "paper_portfolio" / "weekly_monitoring_report.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "paper_portfolio" / "weekly_monitoring_report.md"


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _missing_artifacts(named_paths: dict[str, str | Path]) -> list[str]:
    missing: list[str] = []
    for name, path in named_paths.items():
        if not Path(path).exists():
            missing.append(name)
    return missing


def _status_blockers(status: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    for item in status.get("blockers") or []:
        if item and str(item) not in blockers:
            blockers.append(str(item))

    for section_name in ("small_institutional_simulation", "simulation_self_diagnosis", "drawdown_blocker_evidence"):
        section = status.get(section_name) or {}
        primary_issue = section.get("primary_issue")
        if primary_issue and str(primary_issue) not in blockers:
            blockers.append(str(primary_issue))

    manual_gate = status.get("manual_approval_gate") or {}
    if manual_gate.get("gate_status") == "blocked_pending_manual_approval" or manual_gate.get("human_approval_present") is False:
        if "manual_approval_pending" not in blockers:
            blockers.append("manual_approval_pending")

    return blockers


def _operator_decision(status: dict[str, Any]) -> dict[str, Any]:
    summary = status.get("operator_approval_summary") or {}
    gate = status.get("manual_approval_gate") or {}
    handoff = status.get("operator_decision_handoff") or {}
    return {
        "status": summary.get("summary_status") or gate.get("gate_status") or handoff.get("handoff_status"),
        "required_decision_axis": summary.get("required_decision_axis") or handoff.get("decision_axis"),
        "human_approval_present": gate.get("human_approval_present"),
        "live_trading_enabled": bool(gate.get("live_trading_enabled", False)),
    }


def build_paper_monitoring_report(
    *,
    current_portfolio_path: str | Path = DEFAULT_CURRENT_PORTFOLIO_PATH,
    diagnostics_path: str | Path = DEFAULT_DIAGNOSTICS_PATH,
    status_path: str | Path = DEFAULT_STATUS_PATH,
    dry_run_path: str | Path = DEFAULT_DRY_RUN_PATH,
    runtime_audit_path: str | Path = DEFAULT_RUNTIME_AUDIT_PATH,
) -> dict[str, Any]:
    current = load_json(current_portfolio_path)
    diagnostics = load_json(diagnostics_path)
    status = load_json(status_path)
    dry_run = load_json(dry_run_path)
    runtime_audit = load_json(runtime_audit_path)

    benchmark = diagnostics.get("benchmark") or {}
    turnover = diagnostics.get("turnover") or {}
    cost = diagnostics.get("cost") or {}
    status_runtime = status.get("runtime_safety") or {}

    missing = _missing_artifacts(
        {
            "current_portfolio": current_portfolio_path,
            "portfolio_diagnostics": diagnostics_path,
            "small_institutionalization_status": status_path,
            "controlled_restart_dry_run": dry_run_path,
            "runtime_takeover_audit": runtime_audit_path,
        }
    )

    safe = bool(status_runtime.get("safe")) and "runtime_takeover_audit" not in missing and "controlled_restart_dry_run" not in missing
    operator_decision = _operator_decision(status)
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "cadence": "weekly",
        "portfolio": {
            "strategy_name": current.get("strategy_name"),
            "as_of_date": current.get("as_of_date"),
            "position_count": current.get("position_count"),
        },
        "benchmark": {
            "benchmark_id": benchmark.get("benchmark_id"),
            "benchmark_name": benchmark.get("benchmark_name"),
            "tracking_mode": benchmark.get("tracking_mode", "metadata_only"),
        },
        "trading_friction": {
            "turnover_one_way_estimate": turnover.get("turnover_one_way_estimate"),
            "estimated_one_way_cost": cost.get("estimated_one_way_cost"),
            "estimated_round_trip_cost": cost.get("estimated_round_trip_cost"),
            "cost_bps": cost.get("cost_bps"),
        },
        "runtime": {
            "safe": safe,
            "would_run_count": int(dry_run.get("would_run_count") or 0),
            "blocked_count": int(dry_run.get("blocked_count") or 0),
            "recommendations": runtime_audit.get("recommendations") or status_runtime.get("recommendations") or [],
        },
        "blockers": _status_blockers(status),
        "operator_decision": operator_decision,
        "next_observation_window": {
            "trading_days": 5,
            "calendar": "one_week_placeholder",
            "note": "Paper monitoring skeleton only; realized-return tracking comes next.",
        },
        "missing_artifacts": missing,
    }


def paper_monitoring_report_to_markdown(payload: dict[str, Any]) -> str:
    portfolio = payload.get("portfolio") or {}
    benchmark = payload.get("benchmark") or {}
    friction = payload.get("trading_friction") or {}
    runtime = payload.get("runtime") or {}
    window = payload.get("next_observation_window") or {}
    blockers = payload.get("blockers") or []
    operator_decision = payload.get("operator_decision") or {}
    missing = payload.get("missing_artifacts") or []

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
        f"- Benchmark ID: {benchmark.get('benchmark_id')}",
        f"- Benchmark name: {benchmark.get('benchmark_name')}",
        f"- Tracking mode: {benchmark.get('tracking_mode')}",
        "",
        "## Trading friction",
        f"- One-way turnover estimate: {friction.get('turnover_one_way_estimate')}",
        f"- Estimated one-way cost: {friction.get('estimated_one_way_cost')}",
        f"- Estimated round-trip cost: {friction.get('estimated_round_trip_cost')}",
        f"- Cost bps: {friction.get('cost_bps')}",
        "",
        "## Runtime",
        f"- Safe: {runtime.get('safe')}",
        f"- Would-run count: {runtime.get('would_run_count')}",
        f"- Blocked count: {runtime.get('blocked_count')}",
        f"- Recommendations: {runtime.get('recommendations')}",
        "",
        "## Blockers",
    ]
    lines.extend(f"- {item}" for item in blockers) if blockers else lines.append("- none")
    lines.extend(
        [
            "",
            "## Operator decision",
            f"- Status: {operator_decision.get('status')}",
            f"- Required decision axis: {operator_decision.get('required_decision_axis')}",
            f"- Human approval present: {operator_decision.get('human_approval_present')}",
            f"- Live trading enabled: {operator_decision.get('live_trading_enabled')}",
        ]
    )
    lines.extend(["", "## Missing artifacts"])
    lines.extend(f"- {item}" for item in missing) if missing else lines.append("- none")
    lines.extend(
        [
            "",
            "## Next observation window",
            f"- Trading days: {window.get('trading_days')}",
            f"- Calendar: {window.get('calendar')}",
            f"- Note: {window.get('note')}",
        ]
    )
    return "\n".join(lines) + "\n"


def write_paper_monitoring_report(
    *,
    current_portfolio_path: str | Path = DEFAULT_CURRENT_PORTFOLIO_PATH,
    diagnostics_path: str | Path = DEFAULT_DIAGNOSTICS_PATH,
    status_path: str | Path = DEFAULT_STATUS_PATH,
    dry_run_path: str | Path = DEFAULT_DRY_RUN_PATH,
    runtime_audit_path: str | Path = DEFAULT_RUNTIME_AUDIT_PATH,
    json_path: str | Path = DEFAULT_JSON_PATH,
    markdown_path: str | Path = DEFAULT_MARKDOWN_PATH,
) -> dict[str, Any]:
    payload = build_paper_monitoring_report(
        current_portfolio_path=current_portfolio_path,
        diagnostics_path=diagnostics_path,
        status_path=status_path,
        dry_run_path=dry_run_path,
        runtime_audit_path=runtime_audit_path,
    )
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(paper_monitoring_report_to_markdown(payload), encoding="utf-8")
    return payload
