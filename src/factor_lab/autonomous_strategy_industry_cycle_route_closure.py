from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BLOCKED_ACTIONS = [
    "controlled_backtest",
    "queue_write",
    "timer_enable",
    "broad_daemon_restore",
    "auto_promotion",
    "live_trading",
]


def build_industry_cycle_route_closure(*, run_id: str, cheap_screen: dict[str, Any]) -> dict[str, Any]:
    best = cheap_screen.get("best_candidate") or {}
    screen_failed_risk = (
        cheap_screen.get("overall_status") == "fail"
        and cheap_screen.get("recommended_next_step") == "stop_route"
        and cheap_screen.get("controlled_execution_allowed") is False
        and cheap_screen.get("queue_write_allowed") is False
        and best.get("risk_pass") is False
    )
    if screen_failed_risk:
        route_status = "stopped"
        stop_reason = "industry_cycle_cheap_screen_risk_failed"
        recommended_next_step = "request_new_mechanism"
        reason_codes = [
            "industry_cycle_condition_improved_spread_but_failed_drawdown_guard",
            "best_candidate_risk_pass_false",
            "do_not_restore_queue_or_daemon",
        ]
    else:
        route_status = "blocked"
        stop_reason = "industry_cycle_cheap_screen_not_terminal_failure"
        recommended_next_step = "inspect_industry_cycle_screen_artifact"
        reason_codes = ["cheap_screen_artifact_not_in_expected_fail_stop_state"]

    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "industry_cycle_route_closure",
        "route_id": "industry_cycle_inflection_value_anchor_v1",
        "route_status": route_status,
        "stop_reason": stop_reason,
        "reason_codes": reason_codes,
        "cheap_screen_status": cheap_screen.get("overall_status"),
        "cheap_screen_recommended_next_step": cheap_screen.get("recommended_next_step"),
        "best_candidate": best,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": BLOCKED_ACTIONS,
        "recommended_next_step": recommended_next_step,
        "mechanism_lesson": (
            "cheap + industry cycle momentum produced weak positive spread but did not control deep drawdown; "
            "future value-repair routes need stronger cashflow, quality, balance-sheet, or earnings-confirmation filters before any controlled execution."
        ),
    }


def write_industry_cycle_route_closure(closure: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "industry_cycle_route_closure.json"
    markdown_path = out / "industry_cycle_route_closure.md"
    json_path.write_text(json.dumps(closure, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    best = closure.get("best_candidate") or {}
    markdown_path.write_text(
        "# Industry Cycle Route Closure\n\n"
        f"route_id: {closure.get('route_id')}\n"
        f"route_status: {closure.get('route_status')}\n"
        f"stop_reason: {closure.get('stop_reason')}\n"
        f"recommended_next_step: {closure.get('recommended_next_step')}\n"
        f"controlled_execution_allowed: {closure.get('controlled_execution_allowed')}\n"
        f"queue_write_allowed: {closure.get('queue_write_allowed')}\n"
        f"timer_enable_allowed: {closure.get('timer_enable_allowed')}\n\n"
        "## Best Candidate\n"
        f"- candidate: {best.get('candidate')}\n"
        f"- mean_daily_spread: {best.get('mean_daily_spread')}\n"
        f"- rank_ic: {best.get('rank_ic')}\n"
        f"- max_drawdown: {best.get('max_drawdown')}\n"
        f"- risk_pass: {best.get('risk_pass')}\n\n"
        "## Blocked Actions\n"
        + "\n".join(f"- {action}" for action in closure.get("blocked_actions", []))
        + "\n\n## Mechanism Lesson\n"
        + str(closure.get("mechanism_lesson"))
        + "\n",
        encoding="utf-8",
    )
    return {"json": json_path, "markdown": markdown_path}
