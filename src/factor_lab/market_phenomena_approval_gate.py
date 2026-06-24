from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

GATE_KEYS = ["production_execution_allowed", "live_trading_allowed", "queue_write_allowed", "timer_enable_allowed", "daemon_restore_allowed", "auto_promotion_allowed"]


def _closed_gates() -> dict[str, bool]:
    return {key: False for key in GATE_KEYS}


def _normalize_horizons(values: list[Any] | None) -> list[int]:
    out: list[int] = []
    for value in values or []:
        if isinstance(value, str) and value.endswith("d"):
            value = value[:-1]
        out.append(int(value))
    return out


def build_strategy_design_approval_stub(
    *,
    run_id: str,
    strategy_design_spec: dict[str, Any],
    approval_status: str = "pending_human_review",
    approved_horizons: list[int] | None = None,
    reviewer: str | None = None,
) -> dict[str, Any]:
    requested = _normalize_horizons(strategy_design_spec.get("supported_horizons") or strategy_design_spec.get("review_scope") or [])
    approved = _normalize_horizons(approved_horizons or [])
    review_allowed = strategy_design_spec.get("strategy_design_review_allowed") is True
    is_approved = approval_status == "approved" and bool(approved) and review_allowed
    block_reasons: list[str] = []
    if not review_allowed:
        block_reasons.append("strategy_design_review_not_allowed")
    if approval_status != "approved":
        block_reasons.append("approval_pending")
    if approval_status == "approved" and not approved:
        block_reasons.append("approved_without_horizons")
    if any(h not in requested for h in approved):
        block_reasons.append("approved_horizon_not_requested")
    stub = {
        "schema_version": 1,
        "run_id": run_id + "_approval_stub",
        "mode": "strategy_design_approval_stub",
        "phenomenon_id": strategy_design_spec.get("phenomenon_id"),
        "source_strategy_design_spec_run_id": strategy_design_spec.get("run_id"),
        "approval_status": approval_status,
        "reviewer": reviewer,
        "requested_horizons": requested,
        "approved_horizons": approved,
        "human_approval_required": True,
        "approval_required_before_prototype_generation": True,
        **_closed_gates(),
    }
    gate = {
        "schema_version": 1,
        "run_id": run_id + "_prototype_gate",
        "mode": "strategy_design_prototype_gate",
        "phenomenon_id": strategy_design_spec.get("phenomenon_id"),
        "source_approval_stub_run_id": stub["run_id"],
        "approval_status": approval_status,
        "requested_horizons": requested,
        "approved_horizons": approved if is_approved and all(h in requested for h in approved) else [],
        "prototype_generation_allowed": bool(is_approved and all(h in requested for h in approved)),
        "block_reasons": [] if bool(is_approved and all(h in requested for h in approved)) else block_reasons,
        "strategy_generation_allowed": False,
        "human_approval_required": True,
        **_closed_gates(),
    }
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "strategy_design_approval_gate",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "phenomenon_id": strategy_design_spec.get("phenomenon_id"),
        "strategy_design_approval_stub": stub,
        "strategy_design_prototype_gate": gate,
        **_closed_gates(),
    }


def validate_strategy_design_approval_stub(report: dict[str, Any]) -> dict[str, Any]:
    reason_codes: list[str] = []
    stub = report.get("strategy_design_approval_stub") or {}
    gate = report.get("strategy_design_prototype_gate") or {}
    requested = set(stub.get("requested_horizons") or [])
    for horizon in gate.get("approved_horizons") or stub.get("approved_horizons") or []:
        if horizon not in requested:
            reason_codes.append(f"approved_horizon_not_requested_{horizon}")
    for key in GATE_KEYS:
        if report.get(key) is not False:
            reason_codes.append(f"report_gate_not_closed_{key}")
        if stub.get(key) is not False:
            reason_codes.append(f"approval_stub_not_closed_{key}")
        if gate.get(key) is not False:
            reason_codes.append(f"prototype_gate_not_closed_{key}")
    if gate.get("strategy_generation_allowed") is not False:
        reason_codes.append("strategy_generation_not_closed")
    if gate.get("prototype_generation_allowed") and stub.get("approval_status") != "approved":
        reason_codes.append("prototype_allowed_without_approval")
    if gate.get("prototype_generation_allowed") and not gate.get("approved_horizons"):
        reason_codes.append("prototype_allowed_without_horizons")
    return {"decision": "keep" if not reason_codes else "reject", "reason_codes": reason_codes}


def strategy_design_approval_stub_to_markdown(report: dict[str, Any]) -> str:
    stub = report.get("strategy_design_approval_stub") or {}
    gate = report.get("strategy_design_prototype_gate") or {}
    lines = [
        "# Strategy Design Approval Gate",
        "",
        f"run_id: {report.get('run_id')}",
        f"phenomenon_id: {report.get('phenomenon_id')}",
        f"approval_status: {stub.get('approval_status')}",
        f"requested_horizons: {', '.join(str(h) + 'd' for h in stub.get('requested_horizons') or [])}",
        f"approved_horizons: {', '.join(str(h) + 'd' for h in stub.get('approved_horizons') or []) or 'none'}",
        f"prototype_generation_allowed: {gate.get('prototype_generation_allowed')}",
        f"production_execution_allowed: {gate.get('production_execution_allowed')}",
        f"queue_write_allowed: {gate.get('queue_write_allowed')}",
        f"live_trading_allowed: {gate.get('live_trading_allowed')}",
        "",
        "## Block reasons",
    ]
    if gate.get("block_reasons"):
        lines.extend(f"- {reason}" for reason in gate.get("block_reasons") or [])
    else:
        lines.append("- none")
    return "\n".join(lines).rstrip() + "\n"


def write_strategy_design_approval_stub(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "approval_report_json": out / "strategy_design_approval_gate.json",
        "approval_report_markdown": out / "strategy_design_approval_gate.md",
        "approval_stub_json": out / "strategy_design_approval_stub.json",
        "prototype_gate_json": out / "strategy_design_prototype_gate.json",
    }
    paths["approval_report_json"].write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    paths["approval_report_markdown"].write_text(strategy_design_approval_stub_to_markdown(report), encoding="utf-8")
    paths["approval_stub_json"].write_text(json.dumps(report.get("strategy_design_approval_stub") or {}, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    paths["prototype_gate_json"].write_text(json.dumps(report.get("strategy_design_prototype_gate") or {}, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return paths
