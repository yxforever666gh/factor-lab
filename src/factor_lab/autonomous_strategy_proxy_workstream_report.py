from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BLOCKED_ACTIONS = ["controlled_backtest", "queue_write", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"]


def build_proxy_workstream_report(
    *,
    run_id: str,
    phase6_final_verdict: dict[str, Any],
    proxy_cheap_screen_result: dict[str, Any],
    proxy_route_verdict: dict[str, Any],
) -> dict[str, Any]:
    route_verdict = proxy_route_verdict.get("verdict")
    alpha_status = "failed" if route_verdict == "stop_route" else "manual_review" if route_verdict == "manual_review_before_controlled_backtest" else "needs_revision"
    best = proxy_cheap_screen_result.get("best_candidate") or {}
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "proxy_workstream_report",
        "mechanism_id": "quality_profit_proxy_value_repair_v1",
        "engineering_status": "completed",
        "data_status": {
            "phase6_status": phase6_final_verdict.get("phase_status"),
            "pit_overlay_coverage_passed": phase6_final_verdict.get("pit_overlay_coverage_passed"),
            "pit_alignment_passed": phase6_final_verdict.get("pit_alignment_passed"),
            "pit_alignment_usable_coverage": phase6_final_verdict.get("pit_alignment_usable_coverage"),
        },
        "cheap_screen_status": proxy_cheap_screen_result.get("overall_status"),
        "route_verdict": route_verdict,
        "alpha_status": alpha_status,
        "best_candidate": best,
        "key_failure_reasons": proxy_route_verdict.get("reason_codes") or [],
        "completed_artifacts": [
            "artifacts/autonomous_strategy_lab/phase6_final_verdict.json",
            "artifacts/autonomous_strategy_lab/proxy_cheap_screen_plan.json",
            "artifacts/autonomous_strategy_lab/proxy_cheap_screen_result.json",
            "artifacts/autonomous_strategy_lab/proxy_route_verdict.json",
        ],
        "next_recommended_workstream": "request_new_mechanism_or_revisit_risk_model" if alpha_status == "failed" else "manual_review_before_any_execution",
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": BLOCKED_ACTIONS,
    }


def proxy_workstream_report_to_markdown(report: dict[str, Any]) -> str:
    best = report.get("best_candidate") or {}
    data = report.get("data_status") or {}
    lines = [
        "# Proxy Workstream Report",
        "",
        f"mechanism_id: {report.get('mechanism_id')}",
        f"engineering_status: {report.get('engineering_status')}",
        f"alpha_status: {report.get('alpha_status')}",
        f"cheap_screen_status: {report.get('cheap_screen_status')}",
        f"route_verdict: {report.get('route_verdict')}",
        f"next_recommended_workstream: {report.get('next_recommended_workstream')}",
        f"controlled_execution_allowed: {report.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "## Data status",
        f"- phase6_status: {data.get('phase6_status')}",
        f"- pit_overlay_coverage_passed: {data.get('pit_overlay_coverage_passed')}",
        f"- pit_alignment_passed: {data.get('pit_alignment_passed')}",
        f"- pit_alignment_usable_coverage: {data.get('pit_alignment_usable_coverage')}",
        "",
        "## Best candidate",
    ]
    for key in ["candidate", "mean_daily_spread", "rank_ic", "max_drawdown", "risk_pass", "usable_row_count", "usable_ticker_count", "daily_count"]:
        lines.append(f"- {key}: {best.get(key)}")
    lines += ["", "## Key failure reasons"]
    lines.extend(f"- {reason}" for reason in report.get("key_failure_reasons") or [])
    lines += ["", "## Completed artifacts"]
    lines.extend(f"- {artifact}" for artifact in report.get("completed_artifacts") or [])
    lines += ["", "## Blocked actions"]
    lines.extend(f"- {action}" for action in report.get("blocked_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_proxy_workstream_report(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "proxy_workstream_report.json"
    markdown_path = out / "proxy_workstream_report.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(proxy_workstream_report_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
