from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_STATUS_PATH = ROOT / "artifacts" / "small_institutionalization" / "status.json"
DEFAULT_REPAIR_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "portfolio_construction_repair.json"
DEFAULT_GROUP_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "drawdown_group_diagnostic.json"
DEFAULT_DIAGNOSTICS_PATH = ROOT / "artifacts" / "paper_portfolio" / "portfolio_diagnostics.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "drawdown_blocker_evidence.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "drawdown_blocker_evidence.md"


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def build_drawdown_blocker_evidence(
    *,
    status_path: str | Path = DEFAULT_STATUS_PATH,
    repair_path: str | Path = DEFAULT_REPAIR_PATH,
    group_path: str | Path = DEFAULT_GROUP_PATH,
    diagnostics_path: str | Path = DEFAULT_DIAGNOSTICS_PATH,
) -> dict[str, Any]:
    status = load_json(status_path)
    repair = load_json(repair_path)
    group = load_json(group_path)
    diagnostics = load_json(diagnostics_path)

    simulation = status.get("small_institutional_simulation") or status.get("simulation_self_diagnosis") or {}
    manual_axis = group.get("recommended_manual_axis") or {}
    benchmark = diagnostics.get("benchmark") or {}
    turnover = diagnostics.get("turnover") or {}
    cost = diagnostics.get("cost") or {}
    repair_automation_allowed = bool(repair.get("automation_allowed"))
    group_automation_allowed = bool(group.get("automation_allowed"))
    status_automation_allowed = bool(simulation.get("automation_allowed"))

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_artifacts": {
            "status_path": str(status_path),
            "repair_path": str(repair_path),
            "group_path": str(group_path),
            "diagnostics_path": str(diagnostics_path),
        },
        "blocker": {
            "diagnosis_status": simulation.get("diagnosis_status"),
            "primary_issue": simulation.get("primary_issue"),
            "severity": simulation.get("severity"),
        },
        "repair": {
            "repair_status": repair.get("repair_status"),
            "candidate_count": int(repair.get("candidate_count") or 0),
            "best_available_max_drawdown": repair.get("best_available_max_drawdown"),
            "drawdown_gap_to_limit": repair.get("drawdown_gap_to_limit"),
            "automation_allowed": repair_automation_allowed,
        },
        "manual_review": {
            "diagnostic_status": group.get("diagnostic_status"),
            "dimension": manual_axis.get("dimension"),
            "value": manual_axis.get("value"),
            "best_max_drawdown": manual_axis.get("best_max_drawdown"),
            "drawdown_gap_to_limit": manual_axis.get("drawdown_gap_to_limit"),
        },
        "paper_portfolio_context": {
            "benchmark_id": benchmark.get("benchmark_id"),
            "benchmark_name": benchmark.get("benchmark_name"),
            "tracking_mode": benchmark.get("tracking_mode"),
            "turnover_one_way_estimate": turnover.get("turnover_one_way_estimate"),
            "estimated_round_trip_cost": cost.get("estimated_round_trip_cost"),
        },
        "safety": {
            "automation_allowed": repair_automation_allowed and group_automation_allowed and status_automation_allowed,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
        },
        "next_action": status.get("next_action"),
    }


def evidence_to_markdown(payload: dict[str, Any]) -> str:
    blocker = payload.get("blocker") or {}
    repair = payload.get("repair") or {}
    manual = payload.get("manual_review") or {}
    paper = payload.get("paper_portfolio_context") or {}
    safety = payload.get("safety") or {}
    manual_axis = f"{manual.get('dimension')}={manual.get('value')}"
    lines = [
        "# Drawdown Blocker Evidence",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Next action: {payload.get('next_action')}",
        "",
        "## Blocker",
        f"- diagnosis_status: {blocker.get('diagnosis_status')}",
        f"- primary_issue: {blocker.get('primary_issue')}",
        f"- severity: {blocker.get('severity')}",
        "",
        "## Repair status",
        f"- repair_status: {repair.get('repair_status')}",
        f"- candidate_count: {repair.get('candidate_count')}",
        f"- best_available_max_drawdown: {repair.get('best_available_max_drawdown')}",
        f"- drawdown_gap_to_limit: {repair.get('drawdown_gap_to_limit')}",
        f"- automation_allowed: {repair.get('automation_allowed')}",
        "",
        "## Manual review axis",
        f"- diagnostic_status: {manual.get('diagnostic_status')}",
        f"- recommended_axis: {manual_axis}",
        f"- best_max_drawdown: {manual.get('best_max_drawdown')}",
        f"- drawdown_gap_to_limit: {manual.get('drawdown_gap_to_limit')}",
        "",
        "## Paper portfolio context",
        f"- benchmark_id: {paper.get('benchmark_id')}",
        f"- benchmark_name: {paper.get('benchmark_name')}",
        f"- tracking_mode: {paper.get('tracking_mode')}",
        f"- turnover_one_way_estimate: {paper.get('turnover_one_way_estimate')}",
        f"- estimated_round_trip_cost: {paper.get('estimated_round_trip_cost')}",
        "",
        "## Safety guardrails",
        f"- automation_allowed: {safety.get('automation_allowed')}",
        f"- queue_write_allowed: {safety.get('queue_write_allowed')}",
        f"- broad_daemon_allowed: {safety.get('broad_daemon_allowed')}",
    ]
    return "\n".join(lines) + "\n"


def write_drawdown_blocker_evidence(
    *,
    status_path: str | Path = DEFAULT_STATUS_PATH,
    repair_path: str | Path = DEFAULT_REPAIR_PATH,
    group_path: str | Path = DEFAULT_GROUP_PATH,
    diagnostics_path: str | Path = DEFAULT_DIAGNOSTICS_PATH,
    json_path: str | Path = DEFAULT_JSON_PATH,
    markdown_path: str | Path = DEFAULT_MARKDOWN_PATH,
) -> dict[str, Any]:
    payload = build_drawdown_blocker_evidence(status_path=status_path, repair_path=repair_path, group_path=group_path, diagnostics_path=diagnostics_path)
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(evidence_to_markdown(payload), encoding="utf-8")
    return payload
