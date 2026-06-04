from __future__ import annotations

import json
from pathlib import Path
from typing import Any

DEFAULT_SAFETY = {
    "no_timer": True,
    "no_daemon": True,
    "no_live_trading": True,
    "no_automatic_promotion": True,
}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_md(path: Path, title: str, payload: dict[str, Any]) -> None:
    lines = [f"# {title}", ""]
    for key in ["strategy_run_id", "strategy_decision", "plan_status", "based_on_cycle_id", "based_on_controller_run_id", "reason_codes", "manual_approval_required"]:
        if key in payload:
            lines.append(f"- {key}: `{payload.get(key)}`")
    lines.extend(["", "```json", json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), "```", ""])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def build_strategy_plan(
    *,
    strategy_run_id: str,
    evidence: dict[str, Any],
    decision: dict[str, Any],
    max_next_backtests: int = 120,
) -> dict[str, Any]:
    constraints = dict(decision.get("experiment_constraints") or {})
    constraints["max_next_backtests"] = int(max_next_backtests)
    constraints.setdefault("require_non_duplicate_semantic_hash", True)
    constraints.setdefault("require_expected_information_gain_min", "medium")
    plan = {
        "schema_version": 1,
        "strategy_run_id": strategy_run_id,
        "based_on_controller_run_id": evidence.get("latest_controller_run_id"),
        "based_on_cycle_id": evidence.get("latest_cycle_id"),
        "plan_status": decision.get("plan_status", "planned"),
        "strategy_decision": decision.get("strategy_decision", "manual_review"),
        "reason_codes": list(decision.get("reason_codes") or []),
        "allowed_branches": list(decision.get("allowed_branches") or []),
        "blocked_branches": list(decision.get("blocked_branches") or []),
        "route_action": dict(decision.get("route_action") or {}),
        "experiment_constraints": constraints,
        "stop_conditions": [
            "next_cycle_oos_class_fail_without_sharpe_improvement",
            "max_drawdown_worse_than_-0.55",
            "semantic_duplicate_detected",
        ],
        "manual_approval_required": bool(decision.get("manual_approval_required")),
        "safety": dict(DEFAULT_SAFETY | dict(decision.get("safety") or {})),
    }
    if "data_request" in decision:
        plan["data_request"] = decision["data_request"]
    return plan


def write_strategy_plan(run_dir: str | Path, plan: dict[str, Any]) -> None:
    run_dir = Path(run_dir)
    _write_json(run_dir / "v5_strategy_plan.json", plan)
    _write_md(run_dir / "v5_strategy_plan.md", "Harvest v5 Strategy Plan", plan)


def write_strategy_summary(
    run_dir: str | Path,
    *,
    evidence: dict[str, Any],
    decision: dict[str, Any],
    plan: dict[str, Any],
) -> dict[str, Any]:
    run_dir = Path(run_dir)
    summary = {
        "schema_version": 1,
        "strategy_run_id": plan.get("strategy_run_id"),
        "strategy_decision": decision.get("strategy_decision"),
        "plan_status": decision.get("plan_status") or plan.get("plan_status"),
        "based_on_cycle_id": plan.get("based_on_cycle_id") or evidence.get("latest_cycle_id"),
        "based_on_controller_run_id": plan.get("based_on_controller_run_id") or evidence.get("latest_controller_run_id"),
        "reason_codes": decision.get("reason_codes") or [],
        "loop_reason_codes": (evidence.get("loop_analysis") or {}).get("reason_codes") or [],
        "manual_approval_required": bool(decision.get("manual_approval_required")),
        "artifacts_dir": str(run_dir),
        "safety": plan.get("safety") or DEFAULT_SAFETY,
    }
    _write_json(run_dir / "strategy_summary.json", summary)
    _write_md(run_dir / "strategy_summary.md", "Harvest v5 Strategy Summary", summary)
    return summary
