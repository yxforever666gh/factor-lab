from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ALLOWED_DECISIONS = {
    "continue_route_with_constraints",
    "repair_portfolio_construction",
    "switch_mechanism_route",
    "request_data",
    "stop_route",
    "manual_review",
}

SAFETY_DEFAULTS = {
    "queue_write_allowed": False,
    "automation_allowed": False,
    "live_trading_enabled": False,
    "timer_enable_allowed": False,
    "systemd_change_allowed": False,
    "auto_promotion_allowed": False,
}

DEFAULT_ROUTES = [
    {
        "route_id": "portfolio_risk_first_repair",
        "description": "Change portfolio/risk construction before new factor variants.",
        "data_need": "existing",
        "expected_information_gain": 0.55,
        "risk_fit": 0.35,
    },
    {
        "route_id": "new_mechanism_or_data_request",
        "description": "Request or design a new economically distinct mechanism/data source.",
        "data_need": "new_or_manual",
        "expected_information_gain": 0.80,
        "risk_fit": 0.60,
    },
    {
        "route_id": "continue_industry_relative_value",
        "description": "Keep testing current industry-relative value family.",
        "data_need": "existing",
        "expected_information_gain": 0.15,
        "risk_fit": 0.20,
    },
]


def diagnose_failure_modes(evidence: dict[str, Any]) -> dict[str, Any]:
    reason_codes: list[str] = []
    severity = "info"

    risk = evidence.get("risk") or {}
    if risk.get("status") == "blocked_no_drawdown_safe_candidate":
        reason_codes.append("drawdown_blocker_no_safe_candidate")
        severity = "blocker"
    if _drawdown_worse_than_limit(risk):
        reason_codes.append("best_drawdown_worse_than_limit")
        severity = "blocker"

    cycles = (evidence.get("harvest_v3_status") or {}).get("cycles") or []
    recent_cycles = cycles[-5:]
    recent_oos_classes = [cycle.get("oos_class") for cycle in recent_cycles]
    if recent_oos_classes.count("insufficient_data") >= 3:
        reason_codes.append("repeated_insufficient_data")
        severity = "blocker"
    if any("no_ok_rows" in (cycle.get("repeated_blockers") or []) for cycle in recent_cycles[-3:]):
        reason_codes.append("recent_no_ok_rows")

    candidate_status = (evidence.get("db") or {}).get("candidate_status") or {}
    fragile_or_rejected = int(candidate_status.get("fragile", 0) or 0) + int(candidate_status.get("rejected", 0) or 0)
    testing = int(candidate_status.get("testing", 0) or 0)
    if fragile_or_rejected >= testing and fragile_or_rejected > 0:
        reason_codes.append("candidate_pool_fragile_or_rejected_heavy")

    controlled_dry_run = evidence.get("controlled_dry_run") or {}
    if controlled_dry_run.get("would_run_count") == 0:
        reason_codes.append("no_claimable_controlled_workflow")

    recommendations = (evidence.get("runtime_audit") or {}).get("recommendations") or []
    if "pause_broad_daemon" in recommendations:
        reason_codes.append("broad_daemon_should_remain_paused")

    return {
        "severity": severity,
        "reason_codes": reason_codes,
        "summary": _summarize_reasons(reason_codes),
        "allowed_actions": _allowed_actions_for(reason_codes),
        "blocked_actions": _blocked_actions_for(reason_codes),
    }


def score_strategy_routes(
    evidence: dict[str, Any],
    diagnosis: dict[str, Any],
    routes: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    reason_codes = set(diagnosis.get("reason_codes") or [])
    scored_routes: list[dict[str, Any]] = []
    for route in routes or DEFAULT_ROUTES:
        scored = dict(route)
        score = float(scored.get("expected_information_gain", 0.0)) + float(scored.get("risk_fit", 0.0))
        penalties: list[str] = []
        bonuses: list[str] = []

        if scored["route_id"] == "continue_industry_relative_value":
            if "drawdown_blocker_no_safe_candidate" in reason_codes:
                score -= 0.6
                penalties.append("same_route_drawdown_blocked")
            if "repeated_insufficient_data" in reason_codes:
                score -= 0.3
                penalties.append("same_route_recent_insufficient_data")
        if scored["route_id"] == "portfolio_risk_first_repair":
            if "best_drawdown_worse_than_limit" in reason_codes:
                score -= 0.15
                penalties.append("prior_risk_repair_failed_limit")
        if scored["route_id"] == "new_mechanism_or_data_request":
            if "repeated_insufficient_data" in reason_codes:
                score += 0.2
                bonuses.append("addresses_repeated_insufficient_data")
            if "drawdown_blocker_no_safe_candidate" in reason_codes:
                score += 0.0

        scored["score"] = round(score, 4)
        scored["penalties"] = penalties
        scored["bonuses"] = bonuses
        scored_routes.append(scored)

    return sorted(scored_routes, key=lambda item: item["score"], reverse=True)


def choose_strategy_decision(
    diagnosis: dict[str, Any],
    route_scores: list[dict[str, Any]],
) -> tuple[str, dict[str, Any]]:
    reason_codes = set(diagnosis.get("reason_codes") or [])
    top_route = route_scores[0] if route_scores else {"route_id": "manual_review"}

    if "drawdown_blocker_no_safe_candidate" in reason_codes and "repeated_insufficient_data" in reason_codes:
        decision = "request_data"
    elif "drawdown_blocker_no_safe_candidate" in reason_codes:
        decision = "manual_review"
    elif "repeated_insufficient_data" in reason_codes:
        decision = "request_data"
    elif top_route.get("route_id") == "new_mechanism_or_data_request":
        decision = "switch_mechanism_route"
    elif top_route.get("route_id") == "portfolio_risk_first_repair":
        decision = "repair_portfolio_construction"
    else:
        decision = "continue_route_with_constraints"

    if decision not in ALLOWED_DECISIONS:
        raise ValueError(f"Unsupported autonomous strategy decision: {decision}")

    next_plan = {
        "selected_route_id": top_route.get("route_id"),
        "allowed_next_actions": _allowed_actions_for(list(reason_codes)),
        "blocked_next_actions": [
            "same_route_full_backtest_batch",
            "queue_write",
            "timer_enable",
            "broad_daemon_restore",
            "auto_promotion",
            "drawdown_limit_relaxation",
        ],
        "max_backtests_before_review": 0 if decision in {"request_data", "manual_review", "stop_route"} else 5,
        "requires_human_review": True,
    }
    return decision, next_plan


def build_autonomous_strategy_report(
    *,
    run_id: str,
    evidence_summary: dict[str, Any],
    created_at_utc: str,
    mode: str = "dry_run",
) -> dict[str, Any]:
    diagnosis = diagnose_failure_modes(evidence_summary)
    route_scores = score_strategy_routes(evidence_summary, diagnosis)
    decision, next_plan = choose_strategy_decision(diagnosis, route_scores)
    return {
        "schema_version": 1,
        "run_id": run_id,
        "created_at_utc": created_at_utc,
        "mode": mode,
        "evidence_summary": evidence_summary,
        "diagnosis": diagnosis,
        "route_scores": route_scores,
        "decision": decision,
        "reason_codes": diagnosis["reason_codes"],
        "next_plan": next_plan,
        "safety": dict(SAFETY_DEFAULTS),
    }


def autonomous_strategy_report_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Autonomous Strategy Lab Dry Run",
        "",
        f"run_id: {report['run_id']}",
        f"mode: {report['mode']}",
        f"decision: {report['decision']}",
        "",
        "## Diagnosis",
        f"- severity: {report['diagnosis']['severity']}",
        f"- summary: {report['diagnosis']['summary']}",
        "- reason_codes:",
    ]
    lines.extend(f"  - {code}" for code in report["diagnosis"].get("reason_codes", []))
    risk = report.get("evidence_summary", {}).get("risk", {})
    lines.extend(["", "## Risk evidence"])
    for key in ["status", "drawdown_limit", "best_available_max_drawdown", "candidate_count", "best_sharpe"]:
        lines.append(f"- {key}: {risk.get(key)}")
    lines.extend(["", "## Route scores"])
    for route in report.get("route_scores", []):
        lines.append(
            f"- {route['route_id']}: score={route['score']} "
            f"info_gain={route['expected_information_gain']} risk_fit={route['risk_fit']} "
            f"penalties={route['penalties']}"
        )
    lines.extend([
        "",
        "## Next plan",
        f"- selected_route_id: {report['next_plan']['selected_route_id']}",
        f"- max_backtests_before_review: {report['next_plan']['max_backtests_before_review']}",
        f"- requires_human_review: {report['next_plan']['requires_human_review']}",
        "- allowed_next_actions:",
    ])
    lines.extend(f"  - {action}" for action in report["next_plan"].get("allowed_next_actions", []))
    lines.append("- blocked_next_actions:")
    lines.extend(f"  - {action}" for action in report["next_plan"].get("blocked_next_actions", []))
    lines.extend(["", "## Safety"])
    for key, value in report.get("safety", {}).items():
        lines.append(f"- {key}: {value}")
    lines.append("")
    return "\n".join(lines)


def write_autonomous_strategy_report(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    base = Path(output_dir)
    run_dir = base / "runs" / report["run_id"]
    run_dir.mkdir(parents=True, exist_ok=True)
    base.mkdir(parents=True, exist_ok=True)

    latest_json = base / "latest_decision.json"
    latest_markdown = base / "latest_decision.md"
    run_json = run_dir / "decision.json"
    run_markdown = run_dir / "summary.md"
    markdown = autonomous_strategy_report_to_markdown(report)
    payload = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)

    latest_json.write_text(payload, encoding="utf-8")
    run_json.write_text(payload, encoding="utf-8")
    latest_markdown.write_text(markdown, encoding="utf-8")
    run_markdown.write_text(markdown, encoding="utf-8")
    return {
        "latest_json": latest_json,
        "latest_markdown": latest_markdown,
        "run_json": run_json,
        "run_markdown": run_markdown,
    }


def _drawdown_worse_than_limit(risk: dict[str, Any]) -> bool:
    best_drawdown = risk.get("best_available_max_drawdown")
    limit = risk.get("drawdown_limit")
    return best_drawdown is not None and limit is not None and float(best_drawdown) < float(limit)


def _summarize_reasons(reason_codes: list[str]) -> str:
    if "drawdown_blocker_no_safe_candidate" in reason_codes:
        return "Current route is blocked by portfolio drawdown; more same-route backtests are low value."
    if "repeated_insufficient_data" in reason_codes:
        return "Recent autonomous cycles lack enough valid evidence; request data or switch mechanism."
    return "Evidence does not justify broad automation; keep dry-run and choose a bounded next experiment."


def _allowed_actions_for(reason_codes: list[str]) -> list[str]:
    actions = ["write_blocker_report", "draft_new_mechanism_or_data_request", "run_cheap_screen_only_after_review"]
    if "drawdown_blocker_no_safe_candidate" not in reason_codes and "repeated_insufficient_data" not in reason_codes:
        actions.append("prepare_bounded_controlled_backtest_plan")
    return actions


def _blocked_actions_for(reason_codes: list[str]) -> list[str]:
    blocked = [
        "queue_write",
        "timer_enable",
        "broad_daemon_restore",
        "auto_promotion",
        "drawdown_limit_relaxation",
    ]
    if "drawdown_blocker_no_safe_candidate" in reason_codes or "repeated_insufficient_data" in reason_codes:
        blocked.append("same_route_full_backtest_batch")
    return blocked
