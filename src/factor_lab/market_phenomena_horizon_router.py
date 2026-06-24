from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

GATE_KEYS = ["production_execution_allowed", "live_trading_allowed", "queue_write_allowed", "timer_enable_allowed", "daemon_restore_allowed", "auto_promotion_allowed"]


def _closed_gates() -> dict[str, bool]:
    return {key: False for key in GATE_KEYS}


def _by_horizon(results: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for item in results:
        grouped.setdefault(int(item.get("horizon")), []).append(item)
    return grouped


def _split_item(items: list[dict[str, Any]], split: str) -> dict[str, Any] | None:
    for item in items:
        if item.get("split") == split:
            return item
    return None


def _failed_negative(item: dict[str, Any] | None) -> bool:
    if not item:
        return False
    spread = item.get("cost_adjusted_spread_vs_control")
    if spread is None:
        spread = item.get("spread_vs_control")
    return spread is not None and float(spread) <= 0


def _horizon_rejection_reason(items: list[dict[str, Any]]) -> str:
    oos = _split_item(items, "oos")
    validation = _split_item(items, "validation")
    if not oos:
        return "missing_oos_split"
    if _failed_negative(oos):
        return "oos_failed_negative_spread"
    if oos.get("status") != "pass":
        return "risk_gate_failed"
    if not validation:
        return "missing_validation_split"
    if _failed_negative(validation):
        return "validation_failed_negative_spread"
    if validation.get("status") != "pass":
        return "risk_gate_failed"
    return "not_supported"


def _horizon_detail(horizon: int, items: list[dict[str, Any]]) -> dict[str, Any]:
    detail = {"horizon": horizon, "splits": {}}
    for item in items:
        detail["splits"][item.get("split")] = {
            "status": item.get("status"),
            "spread_vs_control": item.get("spread_vs_control"),
            "cost_adjusted_spread_vs_control": item.get("cost_adjusted_spread_vs_control"),
            "target_worst_forward_return": item.get("target_worst_forward_return"),
            "usable_row_count": item.get("usable_row_count"),
        }
    return detail


def build_supported_horizon_router(*, run_id: str, deeper_oos_report: dict[str, Any]) -> dict[str, Any]:
    grouped = _by_horizon(list(deeper_oos_report.get("results") or []))
    supported: list[int] = []
    rejected: list[dict[str, Any]] = []
    horizon_details: list[dict[str, Any]] = []
    for horizon in sorted(grouped):
        items = grouped[horizon]
        horizon_details.append(_horizon_detail(horizon, items))
        validation = _split_item(items, "validation")
        oos = _split_item(items, "oos")
        if validation and oos and validation.get("status") == "pass" and oos.get("status") == "pass":
            supported.append(horizon)
        else:
            rejected.append({"horizon": horizon, "reason": _horizon_rejection_reason(items)})
    review_allowed = bool(supported)
    gate = {
        "schema_version": 1,
        "run_id": run_id + "_strategy_design_review_gate",
        "mode": "strategy_design_review_gate",
        "phenomenon_id": deeper_oos_report.get("phenomenon_id"),
        "supported_horizons": supported,
        "rejected_horizons": rejected,
        "strategy_design_review_allowed": review_allowed,
        "strategy_generation_allowed": False,
        "human_approval_required": True,
        "auto_promotion_allowed": False,
        "production_execution_allowed": False,
        "next_action": "human_strategy_design_review" if review_allowed else "mutate_or_reject_horizons",
        "review_scope": [f"{h}d" for h in supported],
        **{k: False for k in ["live_trading_allowed", "queue_write_allowed", "timer_enable_allowed", "daemon_restore_allowed"]},
    }
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "supported_horizon_router",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_deeper_oos_run_id": deeper_oos_report.get("run_id"),
        "phenomenon_id": deeper_oos_report.get("phenomenon_id"),
        "supported_horizons": supported,
        "rejected_horizons": rejected,
        "horizon_details": horizon_details,
        "strategy_design_review_gate": gate,
        **_closed_gates(),
    }


def validate_supported_horizon_router(report: dict[str, Any]) -> dict[str, Any]:
    reason_codes: list[str] = []
    for key in GATE_KEYS:
        if report.get(key) is not False:
            reason_codes.append(f"gate_not_closed_{key}")
    gate = report.get("strategy_design_review_gate") or {}
    for key in ["production_execution_allowed", "live_trading_allowed", "queue_write_allowed", "timer_enable_allowed", "daemon_restore_allowed", "auto_promotion_allowed"]:
        if gate.get(key) is not False:
            reason_codes.append(f"review_gate_not_closed_{key}")
    if gate.get("strategy_generation_allowed") is not False:
        reason_codes.append("strategy_generation_not_closed")
    if gate.get("human_approval_required") is not True:
        reason_codes.append("human_approval_not_required")
    if set(report.get("supported_horizons") or []) != set(gate.get("supported_horizons") or []):
        reason_codes.append("supported_horizon_mismatch")
    return {"decision": "keep" if not reason_codes else "reject", "reason_codes": reason_codes}


def supported_horizon_router_to_markdown(report: dict[str, Any]) -> str:
    gate = report.get("strategy_design_review_gate") or {}
    lines = [
        "# Supported Horizon Router",
        "",
        f"run_id: {report.get('run_id')}",
        f"phenomenon_id: {report.get('phenomenon_id')}",
        f"strategy_design_review_allowed: {gate.get('strategy_design_review_allowed')}",
        f"human_approval_required: {gate.get('human_approval_required')}",
        f"auto_promotion_allowed: {gate.get('auto_promotion_allowed')}",
        f"production_execution_allowed: {gate.get('production_execution_allowed')}",
        "",
        "## Supported horizons",
    ]
    if report.get("supported_horizons"):
        lines.extend(f"- {horizon}d" for horizon in report.get("supported_horizons") or [])
    else:
        lines.append("- none")
    lines.extend(["", "## Rejected horizons"])
    for item in report.get("rejected_horizons") or []:
        lines.append(f"- {item.get('horizon')}d: {item.get('reason')}")
    lines.extend(["", "## Next action", str(gate.get("next_action"))])
    return "\n".join(lines).rstrip() + "\n"


def write_supported_horizon_router(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "router_json": out / "supported_horizon_router.json",
        "router_markdown": out / "supported_horizon_router.md",
        "gate_json": out / "strategy_design_review_gate.json",
    }
    paths["router_json"].write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    paths["router_markdown"].write_text(supported_horizon_router_to_markdown(report), encoding="utf-8")
    paths["gate_json"].write_text(json.dumps(report.get("strategy_design_review_gate") or {}, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return paths
