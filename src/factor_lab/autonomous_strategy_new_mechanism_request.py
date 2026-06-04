from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def build_new_mechanism_request(
    *,
    run_id: str,
    route_registry: dict[str, Any],
    stop_route_state: dict[str, Any],
    distress_route_verdict: dict[str, Any],
) -> dict[str, Any]:
    stopped_routes = [
        {
            "route_id": route.get("route_id"),
            "route_status": route.get("route_status"),
            "final_verdict": route.get("final_verdict"),
            "stop_reason": route.get("stop_reason"),
            "recommended_next_step": route.get("recommended_next_step"),
        }
        for route in route_registry.get("routes") or []
        if route.get("route_status") == "stopped"
    ]
    blocked_missing = [
        {
            "route_id": route.get("route_id"),
            "missing_fields": route.get("missing_fields") or [],
            "recommended_next_step": route.get("recommended_next_step"),
        }
        for route in route_registry.get("routes") or []
        if route.get("route_status") == "blocked_missing_fields"
    ]
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "new_mechanism_request",
        "decision": "request_new_mechanism_or_external_distress_data",
        "stopped_routes": stopped_routes,
        "blocked_missing_field_routes": blocked_missing,
        "failed_route_evidence": {
            "historical_relative_valuation_repair": {
                "final_decision": stop_route_state.get("final_decision"),
                "stop_reason": stop_route_state.get("stop_reason"),
                "information_screen_status": stop_route_state.get("information_screen_status"),
                "risk_screen_status": stop_route_state.get("risk_screen_status"),
                "drawdown_proxy": stop_route_state.get("drawdown_proxy"),
            },
            "quality_cashflow_distress_filter": {
                "verdict": distress_route_verdict.get("verdict"),
                "reason_codes": distress_route_verdict.get("reason_codes") or [],
                "best_candidate": distress_route_verdict.get("best_candidate"),
            },
        },
        "do_not_repeat": [
            "do not propose pure valuation cheapness without an independent risk or catalyst mechanism",
            "do not rely on industry exclusion alone as a drawdown repair",
            "do not run controlled backtests until cheap/risk screens pass",
            "do not treat proxy fields as true TTM fields without PIT safety validation",
        ],
        "required_new_mechanism_properties": [
            "economic catalyst distinct from static cheapness",
            "explicit value-trap or distress separation mechanism",
            "PIT-safe data availability or explicit request_data path",
            "cheap-screen metric with positive spread and drawdown proxy >= -0.35 before controlled execution",
        ],
        "external_data_requests": [
            "interest_coverage or finance_cost + EBIT/EBITDA fields",
            "true operating_cashflow_ttm with announcement/report-date alignment",
            "true net_profit_ttm with announcement/report-date alignment",
            "earnings forecast/revision fields if switching to earnings_revision_valuation_repair",
        ],
        "candidate_next_mechanism_families": [
            "earnings_revision_valuation_repair",
            "balance_sheet_improvement_recovery",
            "cashflow_acceleration_quality_value",
            "industry_cycle_inflection_with_value_anchor",
        ],
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": ["controlled_backtest", "queue_write", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"],
        "next_allowed_actions": ["send_to_mechanism_researcher", "request_external_distress_data"],
    }


def new_mechanism_request_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# New Mechanism / External Data Request",
        "",
        f"decision: {report.get('decision')}",
        f"controlled_execution_allowed: {report.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "## Stopped routes",
    ]
    for route in report.get("stopped_routes") or []:
        lines.append(f"- {route.get('route_id')}: {route.get('stop_reason')} -> {route.get('recommended_next_step')}")
    lines.append("")
    lines.append("## Do not repeat")
    lines.extend(f"- {item}" for item in report.get("do_not_repeat") or [])
    lines.append("")
    lines.append("## Required new mechanism properties")
    lines.extend(f"- {item}" for item in report.get("required_new_mechanism_properties") or [])
    lines.append("")
    lines.append("## External data requests")
    lines.extend(f"- {item}" for item in report.get("external_data_requests") or [])
    lines.append("")
    lines.append("## Candidate next mechanism families")
    lines.extend(f"- {item}" for item in report.get("candidate_next_mechanism_families") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_new_mechanism_request(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir); out.mkdir(parents=True, exist_ok=True)
    jp = out / "new_mechanism_request.json"
    mp = out / "new_mechanism_request.md"
    jp.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    mp.write_text(new_mechanism_request_to_markdown(report), encoding="utf-8")
    return {"json": jp, "markdown": mp}
