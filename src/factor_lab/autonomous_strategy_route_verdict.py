from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def build_autonomous_strategy_route_verdict(
    *,
    run_id: str,
    coverage_preflight: dict[str, Any],
    cheap_screen_result: dict[str, Any],
    risk_diagnostic: dict[str, Any],
    max_additional_risk_filter_probes: int = 1,
) -> dict[str, Any]:
    reason_codes: list[str] = []
    route_id = cheap_screen_result.get("route_id") or risk_diagnostic.get("route_id") or "historical_relative_valuation_repair"

    if coverage_preflight.get("overall_status") != "pass":
        verdict = "request_data"
        reason_codes.append("coverage_preflight_not_passed")
        max_next_probes = 0
    elif cheap_screen_result.get("information_screen_status") != "pass":
        verdict = "stop_route"
        reason_codes.append("information_screen_failed")
        max_next_probes = 0
    elif cheap_screen_result.get("risk_screen_status") == "pass":
        verdict = "allow_one_controlled_backtest"
        reason_codes.append("information_and_risk_screens_passed")
        max_next_probes = 0
    elif risk_diagnostic.get("overall_status") == "fail":
        spread = cheap_screen_result.get("cheap_expensive_spread")
        rank_ic = cheap_screen_result.get("rank_ic")
        if spread is not None and spread > 0 and rank_ic is not None and rank_ic > 0 and max_additional_risk_filter_probes > 0:
            verdict = "design_risk_filter_one_probe"
            reason_codes.extend([
                "information_screen_passed",
                "risk_screen_failed",
                "simple_repair_failed",
                "weak_positive_signal_allows_one_more_risk_filter_probe",
            ])
            max_next_probes = int(max_additional_risk_filter_probes)
        else:
            verdict = "stop_route"
            reason_codes.extend(["risk_screen_failed", "no_positive_information_edge_for_repair"])
            max_next_probes = 0
    else:
        verdict = "manual_review"
        reason_codes.append("ambiguous_risk_diagnostic")
        max_next_probes = 0

    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "route_verdict",
        "route_id": route_id,
        "verdict": verdict,
        "reason_codes": reason_codes,
        "coverage_overall_status": coverage_preflight.get("overall_status"),
        "information_screen_status": cheap_screen_result.get("information_screen_status"),
        "risk_screen_status": cheap_screen_result.get("risk_screen_status"),
        "cheap_expensive_spread": cheap_screen_result.get("cheap_expensive_spread"),
        "rank_ic": cheap_screen_result.get("rank_ic"),
        "drawdown_proxy": cheap_screen_result.get("drawdown_proxy"),
        "risk_diagnostic_status": risk_diagnostic.get("overall_status"),
        "risk_diagnostic_recommended_next_step": risk_diagnostic.get("recommended_next_step"),
        "max_next_risk_filter_probes": max_next_probes,
        "next_allowed_actions": ["run_one_value_trap_risk_filter_probe"] if verdict == "design_risk_filter_one_probe" else ["write_stop_route_state"] if verdict == "stop_route" else ["manual_review"],
        "controlled_execution_allowed": verdict == "allow_one_controlled_backtest",
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "auto_promotion_allowed": False,
        "blocked_actions": ["queue_write", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"],
    }


def route_verdict_to_markdown(verdict: dict[str, Any]) -> str:
    lines = [
        "# Autonomous Strategy Route Verdict",
        "",
        f"run_id: {verdict.get('run_id')}",
        f"route_id: {verdict.get('route_id')}",
        f"verdict: {verdict.get('verdict')}",
        f"coverage_overall_status: {verdict.get('coverage_overall_status')}",
        f"information_screen_status: {verdict.get('information_screen_status')}",
        f"risk_screen_status: {verdict.get('risk_screen_status')}",
        f"drawdown_proxy: {verdict.get('drawdown_proxy')}",
        f"max_next_risk_filter_probes: {verdict.get('max_next_risk_filter_probes')}",
        f"controlled_execution_allowed: {verdict.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {verdict.get('queue_write_allowed')}",
        "",
        "## Reason codes",
    ]
    lines.extend(f"- {reason}" for reason in verdict.get("reason_codes") or [])
    lines.append("")
    lines.append("## Next allowed actions")
    lines.extend(f"- {action}" for action in verdict.get("next_allowed_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_route_verdict(verdict: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "route_verdict.json"
    md_path = out / "route_verdict.md"
    json_path.write_text(json.dumps(verdict, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(route_verdict_to_markdown(verdict), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
