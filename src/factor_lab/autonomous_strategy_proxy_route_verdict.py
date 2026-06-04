from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BLOCKED_ACTIONS = ["controlled_backtest", "queue_write", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"]


def build_proxy_route_verdict(*, run_id: str, cheap_screen_result: dict[str, Any]) -> dict[str, Any]:
    status = cheap_screen_result.get("overall_status")
    best = cheap_screen_result.get("best_candidate") or {}
    if status == "manual_review" and best.get("risk_pass") is True:
        verdict = "manual_review_before_controlled_backtest"
        reason_codes = ["proxy_cheap_screen_candidate_passed", "controlled_backtest_requires_later_manual_approval"]
        recommended_next_step = "manual_review_proxy_candidate"
    elif status == "fail":
        verdict = "stop_route"
        reason_codes = ["proxy_cheap_screen_failed_risk_gate", "max_drawdown_below_limit", "do_not_restore_queue_or_daemon"]
        recommended_next_step = "write_proxy_workstream_report"
    else:
        verdict = "request_revision"
        reason_codes = ["proxy_cheap_screen_not_terminal_or_missing", "inspect_proxy_cheap_screen_result"]
        recommended_next_step = "inspect_proxy_cheap_screen_result"
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "proxy_route_verdict",
        "mechanism_id": "quality_profit_proxy_value_repair_v1",
        "verdict": verdict,
        "reason_codes": reason_codes,
        "recommended_next_step": recommended_next_step,
        "cheap_screen_status": status,
        "cheap_screen_recommended_next_step": cheap_screen_result.get("recommended_next_step"),
        "best_candidate": best,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": BLOCKED_ACTIONS,
    }


def proxy_route_verdict_to_markdown(verdict: dict[str, Any]) -> str:
    best = verdict.get("best_candidate") or {}
    lines = [
        "# Proxy Route Verdict",
        "",
        f"mechanism_id: {verdict.get('mechanism_id')}",
        f"verdict: {verdict.get('verdict')}",
        f"recommended_next_step: {verdict.get('recommended_next_step')}",
        f"controlled_execution_allowed: {verdict.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {verdict.get('queue_write_allowed')}",
        "",
        "## Reason codes",
    ]
    lines.extend(f"- {code}" for code in verdict.get("reason_codes") or [])
    lines += ["", "## Best candidate"]
    for key in ["candidate", "mean_daily_spread", "rank_ic", "max_drawdown", "risk_pass", "usable_row_count", "usable_ticker_count", "daily_count"]:
        lines.append(f"- {key}: {best.get(key)}")
    lines += ["", "## Blocked actions"]
    lines.extend(f"- {action}" for action in verdict.get("blocked_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_proxy_route_verdict(verdict: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "proxy_route_verdict.json"
    markdown_path = out / "proxy_route_verdict.md"
    json_path.write_text(json.dumps(verdict, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(proxy_route_verdict_to_markdown(verdict), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
