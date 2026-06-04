from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def build_controlled_execution_decision(
    *,
    run_id: str,
    cheap_screen_plan: dict[str, Any],
    coverage_preflight: dict[str, Any],
    allow_controlled_execution: bool = False,
    max_backtests_requested: int = 1,
    policy_cap: int = 1,
) -> dict[str, Any]:
    reason_codes: list[str] = []
    route_id = coverage_preflight.get("route_id")

    if not allow_controlled_execution:
        reason_codes.append("missing_allow_controlled_execution_flag")
    if cheap_screen_plan.get("controlled_execution_allowed") is not True:
        reason_codes.append("cheap_screen_plan_disallows_controlled_execution")
    if coverage_preflight.get("overall_status") != "pass":
        reason_codes.append("coverage_preflight_blocked")
    if int(max_backtests_requested) > int(policy_cap):
        reason_codes.append("max_backtests_exceeds_policy_cap")
    if cheap_screen_plan.get("queue_write_allowed") is True or coverage_preflight.get("queue_write_allowed") is True:
        reason_codes.append("upstream_queue_write_flag_unexpectedly_true")

    if reason_codes:
        execution_status = "blocked"
        max_backtests_allowed = 0
    else:
        execution_status = "ready_for_manual_controlled_execution"
        max_backtests_allowed = int(max_backtests_requested)
        reason_codes = ["manual_controlled_execution_ready"]

    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "controlled_execution_adapter",
        "route_id": route_id,
        "execution_status": execution_status,
        "reason_codes": reason_codes,
        "allow_controlled_execution_flag": bool(allow_controlled_execution),
        "max_backtests_requested": int(max_backtests_requested),
        "policy_cap": int(policy_cap),
        "max_backtests_allowed": max_backtests_allowed,
        "controlled_execution_started": False,
        "controlled_execution_allowed": execution_status == "ready_for_manual_controlled_execution",
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "broad_daemon_restore_allowed": False,
        "auto_promotion_allowed": False,
        "blocked_actions": [
            "queue_write",
            "timer_enable",
            "broad_daemon_restore",
            "auto_promotion",
            "live_trading",
        ],
        "next_allowed_actions": [
            "manual_review_execution_decision",
            "run_one_controlled_backtest_with_explicit_flag",
        ] if execution_status == "ready_for_manual_controlled_execution" else [
            "manual_review_execution_decision",
            "resolve_blocking_reason_codes",
        ],
    }


def execution_decision_to_markdown(decision: dict[str, Any]) -> str:
    lines = [
        "# Autonomous Strategy Controlled Execution Decision",
        "",
        f"run_id: {decision.get('run_id')}",
        f"route_id: {decision.get('route_id')}",
        f"mode: {decision.get('mode')}",
        f"execution_status: {decision.get('execution_status')}",
        f"controlled_execution_started: {decision.get('controlled_execution_started')}",
        f"controlled_execution_allowed: {decision.get('controlled_execution_allowed')}",
        f"max_backtests_allowed: {decision.get('max_backtests_allowed')}",
        f"queue_write_allowed: {decision.get('queue_write_allowed')}",
        f"timer_enable_allowed: {decision.get('timer_enable_allowed')}",
        "",
        "## Reason codes",
    ]
    lines.extend(f"- {reason}" for reason in decision.get("reason_codes") or [])
    lines.append("")
    lines.append("## Next allowed actions")
    lines.extend(f"- {action}" for action in decision.get("next_allowed_actions") or [])
    lines.append("")
    lines.append("## Blocked actions")
    lines.extend(f"- {action}" for action in decision.get("blocked_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_execution_decision(decision: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "controlled_execution_decision.json"
    md_path = out / "controlled_execution_decision.md"
    json_path.write_text(json.dumps(decision, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(execution_decision_to_markdown(decision), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
