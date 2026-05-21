from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]

DEFAULT_POLICY_PATH = ROOT / "configs" / "small_institutionalization_policy.json"
DEFAULT_RESEARCH_QUALITY_PATH = ROOT / "artifacts" / "research_quality_summary.json"
DEFAULT_DRY_RUN_PATH = ROOT / "artifacts" / "controlled_restart_dry_run.json"
DEFAULT_RUNTIME_AUDIT_PATH = ROOT / "artifacts" / "runtime_takeover_audit.json"
DEFAULT_VALUE_SLEEVE_POLICY_PATH = ROOT / "artifacts" / "value_sleeve_validation" / "value_sleeve_policy.json"
DEFAULT_PAPER_PORTFOLIO_PATH = ROOT / "artifacts" / "paper_portfolio" / "current_portfolio.json"
DEFAULT_PORTFOLIO_DIAGNOSTICS_PATH = ROOT / "artifacts" / "paper_portfolio" / "portfolio_diagnostics.json"
DEFAULT_WEEKLY_MONITORING_REPORT_PATH = ROOT / "artifacts" / "paper_portfolio" / "weekly_monitoring_report.json"
DEFAULT_RETROSPECTIVE_TRACKING_PATH = ROOT / "artifacts" / "paper_portfolio" / "retrospective_return_tracking.json"
DEFAULT_CONSTRAINT_HARDENING_PATH = ROOT / "artifacts" / "paper_portfolio" / "portfolio_constraint_hardening.json"
DEFAULT_PROMOTION_READINESS_PATH = ROOT / "artifacts" / "paper_portfolio" / "paper_live_promotion_readiness.json"
DEFAULT_SIMULATION_SELF_DIAGNOSIS_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "self_diagnosis.json"
DEFAULT_SIMULATED_PORTFOLIO_CONSTRUCTION_REPAIR_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "portfolio_construction_repair.json"
DEFAULT_STATUS_JSON_PATH = ROOT / "artifacts" / "small_institutionalization" / "status.json"
DEFAULT_STATUS_MD_PATH = ROOT / "artifacts" / "small_institutionalization" / "status.md"
DEFAULT_KNOWLEDGE_PATH = ROOT / "knowledge" / "small_institutionalization.md"


def load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _value_sleeve_section(value_sleeve_policy: dict[str, Any], research_quality: dict[str, Any]) -> dict[str, Any]:
    policy = value_sleeve_policy or research_quality.get("value_sleeve_policy") or {}
    decision = str(policy.get("decision") or "missing")
    primary_route = policy.get("primary_route")
    ready = decision == "collapse_to_value_sleeve_with_primary_route" and bool(primary_route)
    return {
        "ready": ready,
        "decision": decision,
        "primary_route": primary_route,
        "confirmation_route": policy.get("confirmation_route"),
        "route_count": len(policy.get("routes") or {}),
    }


def _runtime_safety_section(policy: dict[str, Any], dry_run: dict[str, Any], runtime_audit: dict[str, Any]) -> dict[str, Any]:
    recommendations = runtime_audit.get("recommendations") or []
    would_run_count = int(dry_run.get("would_run_count") or 0)
    blocked_count = int(dry_run.get("blocked_count") or 0)
    claimable_workflow_count = int(dry_run.get("claimable_workflow_count") or dry_run.get("allowed_workflow_count") or 0)
    max_unapproved = int((policy.get("runtime_constraints") or {}).get("max_unapproved_workflows", 0))

    reasons: list[str] = []
    if "pause_broad_daemon" not in recommendations:
        reasons.append("broad_daemon_not_paused")
    if "allow_controlled_only_daemon" not in recommendations:
        reasons.append("controlled_only_not_explicitly_allowed")
    if would_run_count > max_unapproved and claimable_workflow_count > max_unapproved:
        reasons.append("unexpected_claimable_workflows")

    return {
        "safe": not reasons,
        "reasons": reasons,
        "recommendations": recommendations,
        "would_run_count": would_run_count,
        "blocked_count": blocked_count,
        "claimable_workflow_count": claimable_workflow_count,
    }


def _paper_portfolio_section(paper_portfolio: dict[str, Any], portfolio_diagnostics: dict[str, Any] | None = None) -> dict[str, Any]:
    if not paper_portfolio:
        return {"ready": False, "reason": "missing_paper_portfolio_baseline", "position_count": None}
    has_count = "position_count" in paper_portfolio
    position_count = paper_portfolio.get("position_count")
    try:
        count = int(position_count) if position_count is not None else None
    except (TypeError, ValueError):
        count = None
    if not has_count:
        ready = False
        reason = "missing_position_count"
    elif count is None or count <= 0:
        ready = False
        reason = "empty_paper_portfolio_baseline"
    else:
        ready = True
        reason = paper_portfolio.get("reason")
    section = {
        "ready": ready,
        "strategy_name": paper_portfolio.get("strategy_name"),
        "as_of_date": paper_portfolio.get("as_of_date"),
        "position_count": position_count,
        "reason": reason,
    }
    diagnostics = portfolio_diagnostics or {}
    if diagnostics:
        benchmark = diagnostics.get("benchmark") or {}
        turnover = diagnostics.get("turnover") or {}
        cost = diagnostics.get("cost") or {}
        section.update(
            {
                "benchmark_id": benchmark.get("benchmark_id"),
                "turnover_one_way_estimate": turnover.get("turnover_one_way_estimate"),
                "estimated_round_trip_cost": cost.get("estimated_round_trip_cost"),
            }
        )
    return section


def _paper_monitoring_section(weekly_monitoring_report: dict[str, Any]) -> dict[str, Any]:
    if not weekly_monitoring_report:
        return {
            "weekly_report_status": "missing",
            "cadence": None,
            "missing_artifacts": [],
            "runtime_safe": None,
        }
    missing = weekly_monitoring_report.get("missing_artifacts") or []
    runtime = weekly_monitoring_report.get("runtime") or {}
    return {
        "weekly_report_status": "ready" if not missing else "incomplete",
        "cadence": weekly_monitoring_report.get("cadence"),
        "missing_artifacts": missing,
        "runtime_safe": runtime.get("safe"),
    }


def build_small_institutionalization_status(
    *,
    policy_path: str | Path = DEFAULT_POLICY_PATH,
    research_quality_path: str | Path = DEFAULT_RESEARCH_QUALITY_PATH,
    dry_run_path: str | Path = DEFAULT_DRY_RUN_PATH,
    runtime_audit_path: str | Path = DEFAULT_RUNTIME_AUDIT_PATH,
    value_sleeve_policy_path: str | Path = DEFAULT_VALUE_SLEEVE_POLICY_PATH,
    paper_portfolio_path: str | Path = DEFAULT_PAPER_PORTFOLIO_PATH,
    portfolio_diagnostics_path: str | Path = DEFAULT_PORTFOLIO_DIAGNOSTICS_PATH,
    weekly_monitoring_report_path: str | Path = DEFAULT_WEEKLY_MONITORING_REPORT_PATH,
    retrospective_tracking_path: str | Path = DEFAULT_RETROSPECTIVE_TRACKING_PATH,
    constraint_hardening_path: str | Path = DEFAULT_CONSTRAINT_HARDENING_PATH,
    promotion_readiness_path: str | Path = DEFAULT_PROMOTION_READINESS_PATH,
    simulation_self_diagnosis_path: str | Path = DEFAULT_SIMULATION_SELF_DIAGNOSIS_PATH,
    simulated_portfolio_construction_repair_path: str | Path = DEFAULT_SIMULATED_PORTFOLIO_CONSTRUCTION_REPAIR_PATH,
    self_diagnosis_path: str | Path | None = None,
) -> dict[str, Any]:
    policy = load_json(policy_path)
    research_quality = load_json(research_quality_path)
    dry_run = load_json(dry_run_path)
    runtime_audit = load_json(runtime_audit_path)
    value_sleeve_policy = load_json(value_sleeve_policy_path)
    paper_portfolio = load_json(paper_portfolio_path)
    portfolio_diagnostics = load_json(portfolio_diagnostics_path)
    weekly_monitoring_report = load_json(weekly_monitoring_report_path)
    retrospective_tracking = load_json(retrospective_tracking_path)
    constraint_hardening = load_json(constraint_hardening_path)
    promotion_readiness = load_json(promotion_readiness_path)
    simulation_self_diagnosis = load_json(self_diagnosis_path or simulation_self_diagnosis_path)
    simulated_portfolio_construction_repair = load_json(simulated_portfolio_construction_repair_path)

    phase = str(policy.get("phase") or "A_baseline")
    runtime_safety = _runtime_safety_section(policy, dry_run, runtime_audit)
    value_sleeve = _value_sleeve_section(value_sleeve_policy, research_quality)
    paper = _paper_portfolio_section(paper_portfolio, portfolio_diagnostics)

    blockers: list[str] = []
    blockers.extend(runtime_safety["reasons"])
    if not value_sleeve["ready"]:
        blockers.append("missing_primary_value_sleeve")
    if not paper["ready"]:
        blockers.append(str(paper.get("reason") or "missing_paper_portfolio_baseline"))

    if not runtime_safety["safe"]:
        decision = "blocked_runtime_safety"
    elif not value_sleeve["ready"]:
        decision = "blocked_no_primary_sleeve"
    elif not paper["ready"]:
        decision = "needs_paper_portfolio_baseline"
    else:
        decision = "ready_for_portfolio_mvp"

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "phase": phase,
        "strategy_mode": policy.get("strategy_mode") or "long_only_equity_enhancement",
        "decision": decision,
        "blockers": blockers,
        "policy": {
            "target_holdings_min": policy.get("target_holdings_min"),
            "target_holdings_max": policy.get("target_holdings_max"),
            "benchmark_candidates": policy.get("benchmark_candidates") or [],
            "rebalance_candidates": policy.get("rebalance_candidates") or [],
            "portfolio_constraints_next_phase": policy.get("portfolio_constraints_next_phase") or {},
        },
        "runtime_safety": runtime_safety,
        "value_sleeve": value_sleeve,
        "paper_portfolio": paper,
        "paper_monitoring": _paper_monitoring_section(weekly_monitoring_report),
        "retrospective_tracking": _retrospective_tracking_section(retrospective_tracking),
        "portfolio_constraint_hardening": _constraint_hardening_section(constraint_hardening),
        "paper_live_promotion_readiness": _promotion_readiness_section(promotion_readiness),
        "small_institutional_simulation": _simulation_self_diagnosis_section(simulation_self_diagnosis),
        "simulation_self_diagnosis": _simulation_self_diagnosis_section(simulation_self_diagnosis),
        "simulated_portfolio_construction_repair": _simulated_portfolio_construction_repair_section(simulated_portfolio_construction_repair),
        "next_action": _next_action(
            decision,
            has_portfolio_diagnostics=bool(portfolio_diagnostics),
            has_weekly_monitoring_report=bool(weekly_monitoring_report),
            retrospective_tracking=retrospective_tracking,
            constraint_hardening=constraint_hardening,
            promotion_readiness=promotion_readiness,
            simulation_self_diagnosis=simulation_self_diagnosis,
            simulated_portfolio_construction_repair=simulated_portfolio_construction_repair,
        ),
    }


def _retrospective_tracking_section(retrospective_tracking: dict[str, Any]) -> dict[str, Any]:
    if not retrospective_tracking:
        return {"tracking_status": "missing", "portfolio_forward_return": None}
    portfolio_return = retrospective_tracking.get("portfolio_return") or {}
    return {
        "tracking_status": retrospective_tracking.get("tracking_status") or "missing",
        "portfolio_forward_return": portfolio_return.get("portfolio_forward_return"),
        "matched_position_count": portfolio_return.get("matched_position_count"),
        "missing_position_count": portfolio_return.get("missing_position_count"),
    }


def _constraint_hardening_section(constraint_hardening: dict[str, Any]) -> dict[str, Any]:
    if not constraint_hardening:
        return {"constraint_status": "missing", "violations": [], "warnings": []}
    return {
        "constraint_status": constraint_hardening.get("constraint_status") or "missing",
        "violations": constraint_hardening.get("violations") or [],
        "warnings": constraint_hardening.get("warnings") or [],
    }


def _promotion_readiness_section(promotion_readiness: dict[str, Any]) -> dict[str, Any]:
    if not promotion_readiness:
        return {"readiness_status": "missing", "blockers": [], "warnings": []}
    return {
        "readiness_status": promotion_readiness.get("readiness_status") or "missing",
        "blockers": promotion_readiness.get("blockers") or [],
        "warnings": promotion_readiness.get("warnings") or [],
        "manual_approval_required": promotion_readiness.get("manual_approval_required"),
        "live_trading_enabled": promotion_readiness.get("live_trading_enabled"),
    }


def _simulation_self_diagnosis_section(simulation_self_diagnosis: dict[str, Any]) -> dict[str, Any]:
    if not simulation_self_diagnosis:
        return {
            "diagnosis_status": "missing",
            "primary_issue": None,
            "severity": None,
            "recommended_run_mode": None,
            "automation_allowed": False,
        }
    return {
        "diagnosis_status": simulation_self_diagnosis.get("diagnosis_status") or "missing",
        "primary_issue": simulation_self_diagnosis.get("primary_issue"),
        "severity": simulation_self_diagnosis.get("severity"),
        "recommended_run_mode": simulation_self_diagnosis.get("recommended_run_mode"),
        "automation_allowed": bool(simulation_self_diagnosis.get("automation_allowed")),
    }


def _simulated_portfolio_construction_repair_section(repair: dict[str, Any]) -> dict[str, Any]:
    if not repair:
        return {
            "repair_status": "missing",
            "candidate_count": 0,
            "recommended_candidate": None,
            "automation_allowed": False,
        }
    return {
        "repair_status": repair.get("repair_status") or "missing",
        "candidate_count": int(repair.get("candidate_count") or 0),
        "recommended_candidate": repair.get("recommended_candidate"),
        "automation_allowed": bool(repair.get("automation_allowed")),
        "best_available_max_drawdown": repair.get("best_available_max_drawdown"),
        "drawdown_gap_to_limit": repair.get("drawdown_gap_to_limit"),
    }


def _simulation_next_action(
    simulation_self_diagnosis: dict[str, Any] | None,
    simulated_portfolio_construction_repair: dict[str, Any] | None = None,
) -> str:
    diagnosis = simulation_self_diagnosis or {}
    if not diagnosis:
        return "run_small_institutional_self_diagnosis"
    primary_issue = diagnosis.get("primary_issue")
    if primary_issue == "data_coverage_gap":
        return "extend_backtest_dataset"
    if primary_issue == "drawdown_risk_too_high":
        repair = simulated_portfolio_construction_repair or {}
        if repair.get("repair_status") == "candidate_found" and int(repair.get("candidate_count") or 0) > 0:
            return "rerun_bounded_matrix_with_repaired_construction"
        return "repair_simulated_portfolio_construction"
    if diagnosis.get("diagnosis_status") == "ready":
        return "run_bounded_large_scale_simulation"
    return str(diagnosis.get("next_action") or "continue_simulated_research")


def _next_action(
    decision: str,
    *,
    has_portfolio_diagnostics: bool = False,
    has_weekly_monitoring_report: bool = False,
    retrospective_tracking: dict[str, Any] | None = None,
    constraint_hardening: dict[str, Any] | None = None,
    promotion_readiness: dict[str, Any] | None = None,
    simulation_self_diagnosis: dict[str, Any] | None = None,
    simulated_portfolio_construction_repair: dict[str, Any] | None = None,
) -> str:
    if decision == "ready_for_portfolio_mvp":
        if not has_portfolio_diagnostics:
            return "write_benchmark_cost_turnover_diagnostics"
        if not has_weekly_monitoring_report:
            return "paper_monitoring_weekly_report"
        return _simulation_next_action(simulation_self_diagnosis, simulated_portfolio_construction_repair)
    if decision == "needs_paper_portfolio_baseline":
        return "build_paper_portfolio_baseline_without_queue_or_daemon"
    if decision == "blocked_no_primary_sleeve":
        return "write_or_repair_value_sleeve_policy_before_portfolio_work"
    return "stop_and_repair_runtime_safety_before_any_portfolio_work"


def status_to_markdown(status: dict[str, Any]) -> str:
    lines = [
        "# Factor Lab Small Institutionalization Status",
        "",
        f"Generated: {status.get('generated_at_utc')}",
        f"Phase: {status.get('phase')}",
        f"Strategy mode: {status.get('strategy_mode')}",
        f"Decision: {status.get('decision')}",
        f"Next action: {status.get('next_action')}",
        "",
        "## Blockers",
    ]
    blockers = status.get("blockers") or []
    if blockers:
        lines.extend(f"- {item}" for item in blockers)
    else:
        lines.append("- none")

    runtime = status.get("runtime_safety") or {}
    sleeve = status.get("value_sleeve") or {}
    paper = status.get("paper_portfolio") or {}
    monitoring = status.get("paper_monitoring") or {}
    retrospective = status.get("retrospective_tracking") or {}
    constraint = status.get("portfolio_constraint_hardening") or {}
    readiness = status.get("paper_live_promotion_readiness") or {}
    simulation = status.get("small_institutional_simulation") or {}
    repair = status.get("simulated_portfolio_construction_repair") or {}
    policy = status.get("policy") or {}
    lines.extend(
        [
            "",
            "## Runtime safety",
            f"- Safe: {runtime.get('safe')}",
            f"- Recommendations: {runtime.get('recommendations')}",
            f"- Would-run count: {runtime.get('would_run_count')}",
            "",
            "## Value sleeve",
            f"- Decision: {sleeve.get('decision')}",
            f"- Primary route: {sleeve.get('primary_route')}",
            f"- Confirmation route: {sleeve.get('confirmation_route')}",
            "",
            "## Paper portfolio",
            f"- Ready: {paper.get('ready')}",
            f"- Strategy: {paper.get('strategy_name')}",
            f"- As-of date: {paper.get('as_of_date')}",
            f"- Position count: {paper.get('position_count')}",
            f"- Benchmark ID: {paper.get('benchmark_id')}",
            f"- One-way turnover estimate: {paper.get('turnover_one_way_estimate')}",
            f"- Estimated round-trip cost: {paper.get('estimated_round_trip_cost')}",
            "",
            "## Paper monitoring",
            f"- Weekly report status: {monitoring.get('weekly_report_status')}",
            f"- Cadence: {monitoring.get('cadence')}",
            f"- Runtime safe: {monitoring.get('runtime_safe')}",
            f"- Missing artifacts: {monitoring.get('missing_artifacts')}",
            "",
            "## Retrospective tracking",
            f"- Tracking status: {retrospective.get('tracking_status')}",
            f"- Portfolio forward return: {retrospective.get('portfolio_forward_return')}",
            f"- Matched position count: {retrospective.get('matched_position_count')}",
            f"- Missing position count: {retrospective.get('missing_position_count')}",
            "",
            "## Portfolio constraint hardening",
            f"- Constraint status: {constraint.get('constraint_status')}",
            f"- Violations: {constraint.get('violations')}",
            f"- Warnings: {constraint.get('warnings')}",
            "",
            "## Paper/live promotion readiness",
            f"- Readiness status: {readiness.get('readiness_status')}",
            f"- Blockers: {readiness.get('blockers')}",
            f"- Warnings: {readiness.get('warnings')}",
            f"- Manual approval required: {readiness.get('manual_approval_required')}",
            f"- Live trading enabled: {readiness.get('live_trading_enabled')}",
            "",
            "## Small institutional simulation",
            f"- Diagnosis status: {simulation.get('diagnosis_status')}",
            f"- Primary issue: {simulation.get('primary_issue')}",
            f"- Severity: {simulation.get('severity')}",
            f"- Recommended run mode: {simulation.get('recommended_run_mode')}",
            f"- Automation allowed: {simulation.get('automation_allowed')}",
            "",
            "## Simulated portfolio construction repair",
            f"- Repair status: {repair.get('repair_status')}",
            f"- Candidate count: {repair.get('candidate_count')}",
            f"- Recommended candidate: {repair.get('recommended_candidate')}",
            f"- Best available max drawdown: {repair.get('best_available_max_drawdown')}",
            f"- Drawdown gap to limit: {repair.get('drawdown_gap_to_limit')}",
            f"- Automation allowed: {repair.get('automation_allowed')}",
            "",
            "## Next phase policy",
            f"- Target holdings: {policy.get('target_holdings_min')}-{policy.get('target_holdings_max')}",
            f"- Benchmark candidates: {policy.get('benchmark_candidates')}",
            f"- Rebalance candidates: {policy.get('rebalance_candidates')}",
        ]
    )
    return "\n".join(lines) + "\n"


def write_small_institutionalization_status(
    *,
    json_path: str | Path = DEFAULT_STATUS_JSON_PATH,
    markdown_path: str | Path = DEFAULT_STATUS_MD_PATH,
    knowledge_path: str | Path = DEFAULT_KNOWLEDGE_PATH,
    **kwargs: Any,
) -> dict[str, Any]:
    status = build_small_institutionalization_status(**kwargs)
    json_out = Path(json_path)
    md_out = Path(markdown_path)
    knowledge_out = Path(knowledge_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.parent.mkdir(parents=True, exist_ok=True)
    knowledge_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    text = status_to_markdown(status)
    md_out.write_text(text, encoding="utf-8")
    knowledge_out.write_text(text, encoding="utf-8")
    return status
