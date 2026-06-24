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
DEFAULT_DRAWDOWN_GROUP_DIAGNOSTIC_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "drawdown_group_diagnostic.json"
DEFAULT_DRAWDOWN_BLOCKER_EVIDENCE_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "drawdown_blocker_evidence.json"
DEFAULT_REPAIR_BLOCKER_MANUAL_REVIEW_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "repair_blocker_manual_review.json"
DEFAULT_MANUAL_APPROVAL_GATE_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "manual_approval_gate.json"
DEFAULT_OPERATOR_APPROVAL_SUMMARY_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "operator_approval_summary.json"
DEFAULT_APPROVAL_ARTIFACT_CONSISTENCY_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "approval_artifact_consistency.json"
DEFAULT_OPERATOR_DECISION_INTAKE_VALIDATION_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "operator_decision_intake_validation.json"
DEFAULT_OPERATOR_DECISION_HANDOFF_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "operator_decision_handoff.json"
DEFAULT_OPERATOR_PENDING_OBSERVATION_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "operator_pending_observation.json"
DEFAULT_OPERATOR_PENDING_CONSISTENCY_SNAPSHOT_PATH = ROOT / "artifacts" / "small_institutionalization" / "operator_pending_consistency_snapshot.json"
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


OPERATOR_PENDING_CONSISTENCY_FIELDS = (
    "observation_status",
    "primary_issue",
    "manual_approval_status",
    "benchmark_id",
    "turnover_one_way_estimate",
    "estimated_round_trip_cost",
    "queue_write_allowed",
    "broad_daemon_allowed",
    "automation_allowed",
    "automated_rerun_allowed",
    "live_trading_enabled",
)


def _operator_pending_consistency_section(
    weekly_operator_pending: dict[str, Any],
    canonical_operator_pending: dict[str, Any],
) -> dict[str, Any] | None:
    if not weekly_operator_pending or not canonical_operator_pending:
        return None
    if canonical_operator_pending.get("observation_status") == "missing":
        return None
    mismatches = [
        field
        for field in OPERATOR_PENDING_CONSISTENCY_FIELDS
        if weekly_operator_pending.get(field) != canonical_operator_pending.get(field)
    ]
    return {
        "consistency_status": "ok" if not mismatches else "mismatch",
        "mismatches": mismatches,
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
    }


def _operator_pending_consistency_snapshot_section(snapshot: dict[str, Any]) -> dict[str, Any] | None:
    if not snapshot:
        return None
    runtime = snapshot.get("runtime") or {}
    source_status_generated_at = snapshot.get("source_status_generated_at_utc")
    return {
        "snapshot_status": snapshot.get("snapshot_status") or "missing",
        "source_status_generated_at_utc": source_status_generated_at,
        "consistency_status": snapshot.get("consistency_status") or "missing",
        "mismatches": snapshot.get("mismatches") or [],
        "benchmark_id": snapshot.get("benchmark_id"),
        "turnover_one_way_estimate": snapshot.get("turnover_one_way_estimate"),
        "estimated_round_trip_cost": snapshot.get("estimated_round_trip_cost"),
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
    }


def _snapshot_freshness_status(source_status_generated_at: Any, latest_status_generated_at: Any) -> str:
    if not source_status_generated_at:
        return "source_metadata_missing"
    if not latest_status_generated_at:
        return "latest_status_metadata_missing"
    if source_status_generated_at == latest_status_generated_at:
        return "fresh"
    return "stale"


def _paper_monitoring_section(
    weekly_monitoring_report: dict[str, Any],
    canonical_operator_pending: dict[str, Any] | None = None,
    operator_pending_consistency_snapshot: dict[str, Any] | None = None,
    latest_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot_section = _operator_pending_consistency_snapshot_section(operator_pending_consistency_snapshot or {})
    if snapshot_section:
        latest_status_generated_at = (latest_status or {}).get("generated_at_utc")
        snapshot_section["latest_status_generated_at_utc"] = latest_status_generated_at
        snapshot_section["snapshot_freshness_status"] = _snapshot_freshness_status(
            snapshot_section.get("source_status_generated_at_utc"),
            latest_status_generated_at,
        )
    if not weekly_monitoring_report:
        section = {
            "weekly_report_status": "missing",
            "cadence": None,
            "missing_artifacts": [],
            "runtime_safe": None,
        }
        if snapshot_section:
            section["operator_pending_consistency_snapshot"] = snapshot_section
        return section
    missing = weekly_monitoring_report.get("missing_artifacts") or []
    runtime = weekly_monitoring_report.get("runtime") or {}
    section = {
        "weekly_report_status": "ready" if not missing else "incomplete",
        "cadence": weekly_monitoring_report.get("cadence"),
        "missing_artifacts": missing,
        "runtime_safe": runtime.get("safe"),
        "would_run_count": runtime.get("would_run_count"),
        "recommendations": runtime.get("recommendations") or [],
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
        "next_observation_window": weekly_monitoring_report.get("next_observation_window"),
    }
    blockers = weekly_monitoring_report.get("blockers") or {}
    if isinstance(blockers, dict) and blockers:
        section["blockers"] = {
            "decision": blockers.get("decision"),
            "next_action": blockers.get("next_action"),
            "primary_issue": blockers.get("primary_issue"),
            "manual_approval_gate_status": blockers.get("manual_approval_gate_status"),
            "human_approval_present": blockers.get("human_approval_present"),
            "approval_required": blockers.get("approval_required"),
            "required_decision_axis": blockers.get("required_decision_axis"),
        }
    operator_pending = weekly_monitoring_report.get("operator_pending_observation") or {}
    if operator_pending:
        section["operator_pending_observation"] = {
            "observation_status": operator_pending.get("observation_status"),
            "primary_issue": operator_pending.get("primary_issue"),
            "manual_approval_status": operator_pending.get("manual_approval_status"),
            "benchmark_id": operator_pending.get("benchmark_id"),
            "turnover_one_way_estimate": operator_pending.get("turnover_one_way_estimate"),
            "estimated_round_trip_cost": operator_pending.get("estimated_round_trip_cost"),
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automation_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        }
        consistency = _operator_pending_consistency_section(
            section["operator_pending_observation"],
            canonical_operator_pending or {},
        )
        if consistency:
            section["operator_pending_consistency"] = consistency
    if snapshot_section:
        section["operator_pending_consistency_snapshot"] = snapshot_section
    return section


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
    drawdown_group_diagnostic_path: str | Path = DEFAULT_DRAWDOWN_GROUP_DIAGNOSTIC_PATH,
    drawdown_blocker_evidence_path: str | Path = DEFAULT_DRAWDOWN_BLOCKER_EVIDENCE_PATH,
    repair_blocker_manual_review_path: str | Path = DEFAULT_REPAIR_BLOCKER_MANUAL_REVIEW_PATH,
    manual_approval_gate_path: str | Path = DEFAULT_MANUAL_APPROVAL_GATE_PATH,
    operator_approval_summary_path: str | Path = DEFAULT_OPERATOR_APPROVAL_SUMMARY_PATH,
    approval_artifact_consistency_path: str | Path = DEFAULT_APPROVAL_ARTIFACT_CONSISTENCY_PATH,
    operator_decision_intake_validation_path: str | Path = DEFAULT_OPERATOR_DECISION_INTAKE_VALIDATION_PATH,
    operator_decision_handoff_path: str | Path = DEFAULT_OPERATOR_DECISION_HANDOFF_PATH,
    operator_pending_observation_path: str | Path = DEFAULT_OPERATOR_PENDING_OBSERVATION_PATH,
    operator_pending_consistency_snapshot_path: str | Path = DEFAULT_OPERATOR_PENDING_CONSISTENCY_SNAPSHOT_PATH,
    status_json_path: str | Path = DEFAULT_STATUS_JSON_PATH,
    self_diagnosis_path: str | Path | None = None,
    generated_at: str | None = None,
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
    drawdown_group_diagnostic = load_json(drawdown_group_diagnostic_path)
    drawdown_blocker_evidence = load_json(drawdown_blocker_evidence_path)
    repair_blocker_manual_review = load_json(repair_blocker_manual_review_path)
    manual_approval_gate = load_json(manual_approval_gate_path)
    operator_approval_summary = load_json(operator_approval_summary_path)
    approval_artifact_consistency = load_json(approval_artifact_consistency_path)
    operator_decision_intake_validation = load_json(operator_decision_intake_validation_path)
    operator_decision_handoff = load_json(operator_decision_handoff_path)
    operator_pending_observation = load_json(operator_pending_observation_path)
    operator_pending_consistency_snapshot = load_json(operator_pending_consistency_snapshot_path)
    latest_status = load_json(status_json_path)

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

    canonical_operator_pending = _operator_pending_observation_section(operator_pending_observation)
    manual_approval_gate_section = _manual_approval_gate_section(manual_approval_gate)
    operator_approval_summary_section = _operator_approval_summary_section(operator_approval_summary)
    operator_decision_intake_validation_section = _operator_decision_intake_validation_section(operator_decision_intake_validation)
    operator_decision_handoff_section = _operator_decision_handoff_section(operator_decision_handoff)
    operator_decision_wait_state = _operator_decision_wait_state_section(
        manual_approval_gate_section,
        operator_approval_summary_section,
        operator_decision_intake_validation_section,
        operator_decision_handoff_section,
    )
    paper_monitoring_section = _paper_monitoring_section(
        weekly_monitoring_report,
        canonical_operator_pending,
        operator_pending_consistency_snapshot,
        latest_status,
    )

    return {
        "schema_version": 1,
        "generated_at_utc": generated_at or datetime.now(timezone.utc).isoformat(),
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
        "paper_monitoring": paper_monitoring_section,
        "retrospective_tracking": _retrospective_tracking_section(retrospective_tracking),
        "portfolio_constraint_hardening": _constraint_hardening_section(constraint_hardening),
        "paper_live_promotion_readiness": _promotion_readiness_section(promotion_readiness),
        "small_institutional_simulation": _simulation_self_diagnosis_section(simulation_self_diagnosis),
        "simulation_self_diagnosis": _simulation_self_diagnosis_section(simulation_self_diagnosis),
        "simulated_portfolio_construction_repair": _simulated_portfolio_construction_repair_section(simulated_portfolio_construction_repair),
        "drawdown_group_diagnostic": _drawdown_group_diagnostic_section(drawdown_group_diagnostic),
        "drawdown_blocker_evidence": _drawdown_blocker_evidence_section(drawdown_blocker_evidence),
        "repair_blocker_manual_review": _repair_blocker_manual_review_section(repair_blocker_manual_review),
        "manual_approval_gate": manual_approval_gate_section,
        "operator_approval_summary": operator_approval_summary_section,
        "approval_artifact_consistency": _approval_artifact_consistency_section(approval_artifact_consistency),
        "operator_decision_intake_validation": operator_decision_intake_validation_section,
        "operator_decision_handoff": operator_decision_handoff_section,
        "operator_decision_wait_state": operator_decision_wait_state,
        "operator_pending_observation": canonical_operator_pending,
        "operator_pending_consistency_snapshot": paper_monitoring_section.get("operator_pending_consistency_snapshot"),
        "next_action": _next_action(
            decision,
            has_portfolio_diagnostics=bool(portfolio_diagnostics),
            has_weekly_monitoring_report=bool(weekly_monitoring_report),
            retrospective_tracking=retrospective_tracking,
            constraint_hardening=constraint_hardening,
            promotion_readiness=promotion_readiness,
            simulation_self_diagnosis=simulation_self_diagnosis,
            simulated_portfolio_construction_repair=simulated_portfolio_construction_repair,
            manual_approval_gate=manual_approval_gate,
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
        return {"readiness_status": "missing", "blockers": [], "warnings": [], "live_trading_enabled": False}
    return {
        "readiness_status": promotion_readiness.get("readiness_status") or "missing",
        "blockers": promotion_readiness.get("blockers") or [],
        "warnings": promotion_readiness.get("warnings") or [],
        "manual_approval_required": promotion_readiness.get("manual_approval_required"),
        "live_trading_enabled": False,
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
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        }
    return {
        "repair_status": repair.get("repair_status") or "missing",
        "candidate_count": int(repair.get("candidate_count") or 0),
        "recommended_candidate": repair.get("recommended_candidate"),
        "automation_allowed": bool(repair.get("automation_allowed")),
        "queue_write_allowed": bool(repair.get("queue_write_allowed")),
        "broad_daemon_allowed": bool(repair.get("broad_daemon_allowed")),
        "automated_rerun_allowed": bool(repair.get("automated_rerun_allowed")),
        "live_trading_enabled": bool(repair.get("live_trading_enabled")),
        "best_available_max_drawdown": repair.get("best_available_max_drawdown"),
        "drawdown_gap_to_limit": repair.get("drawdown_gap_to_limit"),
    }


def _drawdown_group_diagnostic_section(diagnostic: dict[str, Any]) -> dict[str, Any]:
    if not diagnostic:
        return {
            "diagnostic_status": "missing",
            "recommended_dimension": None,
            "recommended_value": None,
            "best_max_drawdown": None,
            "drawdown_gap_to_limit": None,
            "automation_allowed": False,
        }
    axis = diagnostic.get("recommended_manual_axis") or {}
    return {
        "diagnostic_status": diagnostic.get("diagnostic_status") or "missing",
        "recommended_dimension": axis.get("dimension"),
        "recommended_value": axis.get("value"),
        "best_max_drawdown": axis.get("best_max_drawdown"),
        "drawdown_gap_to_limit": axis.get("drawdown_gap_to_limit"),
        "automation_allowed": bool(diagnostic.get("automation_allowed")),
    }


def _drawdown_blocker_evidence_section(evidence: dict[str, Any]) -> dict[str, Any]:
    if not evidence:
        return {"evidence_status": "missing"}
    blocker = evidence.get("blocker") or {}
    repair = evidence.get("repair") or {}
    manual_review = evidence.get("manual_review") or {}
    context = evidence.get("paper_portfolio_context") or {}
    safety = evidence.get("safety") or {}
    return {
        "evidence_status": "ready",
        "primary_issue": blocker.get("primary_issue"),
        "repair_status": repair.get("repair_status"),
        "candidate_count": int(repair.get("candidate_count") or 0),
        "manual_review_dimension": manual_review.get("dimension"),
        "manual_review_value": manual_review.get("value"),
        "queue_write_allowed": bool(safety.get("queue_write_allowed")),
        "broad_daemon_allowed": bool(safety.get("broad_daemon_allowed")),
        "benchmark_id": context.get("benchmark_id"),
        "benchmark_name": context.get("benchmark_name"),
        "tracking_mode": context.get("tracking_mode"),
        "turnover_one_way_estimate": context.get("turnover_one_way_estimate"),
        "estimated_round_trip_cost": context.get("estimated_round_trip_cost"),
    }


def _repair_blocker_manual_review_section(review: dict[str, Any]) -> dict[str, Any]:
    if not review:
        return {"review_status": "missing"}
    safety = review.get("safety") or {}
    decision = review.get("recommended_manual_decision") or {}
    return {
        "review_status": review.get("review_status") or "missing",
        "primary_issue": review.get("primary_issue"),
        "repair_status": review.get("repair_status"),
        "candidate_count": int(review.get("candidate_count") or 0),
        "best_available_max_drawdown": review.get("best_available_max_drawdown"),
        "drawdown_gap_to_limit": review.get("drawdown_gap_to_limit"),
        "queue_write_allowed": bool(safety.get("queue_write_allowed")),
        "broad_daemon_allowed": bool(safety.get("broad_daemon_allowed")),
        "automation_allowed": bool(safety.get("automation_allowed")),
        "manual_decision_required": bool(decision.get("decision_required")),
        "manual_decision_dimension": decision.get("dimension"),
        "manual_decision_value": decision.get("value"),
        "automated_rerun_allowed": bool(decision.get("automated_rerun_allowed")),
    }


def _manual_approval_gate_section(gate: dict[str, Any]) -> dict[str, Any]:
    if not gate:
        return {"gate_status": "missing"}
    safety = gate.get("safety") or {}
    return {
        "gate_status": gate.get("gate_status") or "missing",
        "human_approval_present": bool(gate.get("human_approval_present")),
        "risk_relaxation_allowed": bool(gate.get("risk_relaxation_allowed")),
        "automated_rerun_allowed": bool(gate.get("automated_rerun_allowed")),
        "queue_write_allowed": bool(gate.get("queue_write_allowed") or safety.get("queue_write_allowed")),
        "broad_daemon_allowed": bool(gate.get("broad_daemon_allowed") or safety.get("broad_daemon_allowed")),
        "automation_allowed": bool(gate.get("automation_allowed") or safety.get("automation_allowed")),
        "live_trading_enabled": bool(gate.get("live_trading_enabled")),
    }


def _operator_approval_summary_section(summary: dict[str, Any]) -> dict[str, Any]:
    if not summary:
        return {"summary_status": "missing"}
    safety = summary.get("safety") or {}
    return {
        "summary_status": summary.get("summary_status") or "missing",
        "approval_required": bool(summary.get("approval_required")),
        "human_approval_present": bool(summary.get("human_approval_present")),
        "required_decision_axis": summary.get("required_decision_axis"),
        "primary_blocker": summary.get("primary_blocker"),
        "repair_status": summary.get("repair_status"),
        "candidate_count": int(summary.get("candidate_count") or 0),
        "best_available_max_drawdown": summary.get("best_available_max_drawdown"),
        "drawdown_gap_to_limit": summary.get("drawdown_gap_to_limit"),
        "queue_write_allowed": bool(safety.get("queue_write_allowed")),
        "broad_daemon_allowed": bool(safety.get("broad_daemon_allowed")),
        "automation_allowed": bool(safety.get("automation_allowed")),
        "automated_rerun_allowed": bool(safety.get("automated_rerun_allowed")),
        "live_trading_enabled": bool(safety.get("live_trading_enabled")),
    }


def _approval_artifact_consistency_section(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {"consistency_status": "missing"}
    matched = payload.get("matched_fields") or {}
    safety = payload.get("safety_flags") or {}
    return {
        "consistency_status": payload.get("consistency_status") or "missing",
        "primary_blocker": matched.get("primary_blocker"),
        "decision_axis": matched.get("decision_axis"),
        "best_available_max_drawdown": matched.get("best_available_max_drawdown"),
        "drawdown_gap_to_limit": matched.get("drawdown_gap_to_limit"),
        "queue_write_allowed": bool(safety.get("queue_write_allowed")),
        "broad_daemon_allowed": bool(safety.get("broad_daemon_allowed")),
        "automation_allowed": bool(safety.get("automation_allowed")),
        "automated_rerun_allowed": bool(safety.get("automated_rerun_allowed")),
        "live_trading_enabled": bool(safety.get("live_trading_enabled")),
        "inconsistencies": payload.get("inconsistencies") or [],
        "staleness_warnings": payload.get("staleness_warnings") or [],
    }

def _operator_decision_intake_validation_section(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {
            "intake_status": "missing",
            "decision_type": None,
            "scope": None,
            "reason": None,
            "validation_errors": [],
            "non_mutating": True,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automation_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        }
    safety = payload.get("safety") or {}
    return {
        "intake_status": payload.get("intake_status") or "missing",
        "decision_type": payload.get("decision_type"),
        "scope": payload.get("scope"),
        "reason": payload.get("reason"),
        "validation_errors": payload.get("validation_errors") or [],
        "non_mutating": bool(payload.get("non_mutating")),
        "queue_write_allowed": bool(safety.get("queue_write_allowed")),
        "broad_daemon_allowed": bool(safety.get("broad_daemon_allowed")),
        "automation_allowed": bool(safety.get("automation_allowed")),
        "automated_rerun_allowed": bool(safety.get("automated_rerun_allowed")),
        "live_trading_enabled": bool(safety.get("live_trading_enabled")),
    }


def _operator_decision_handoff_section(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {"handoff_status": "missing", "validation_errors": [], "non_mutating": True}
    safety = payload.get("safety") or {}
    return {
        "handoff_status": payload.get("handoff_status") or "missing",
        "intake_status": payload.get("intake_status"),
        "decision_type": payload.get("decision_type"),
        "primary_blocker": payload.get("primary_blocker"),
        "repair_status": payload.get("repair_status"),
        "candidate_count": int(payload.get("candidate_count") or 0),
        "decision_axis": payload.get("decision_axis"),
        "validation_errors": payload.get("validation_errors") or [],
        "non_mutating": bool(payload.get("non_mutating")),
        "execution_allowed": bool(payload.get("execution_allowed")),
        "separate_execution_plan_required": bool(payload.get("separate_execution_plan_required")),
        "queue_write_allowed": bool(payload.get("queue_write_allowed") or safety.get("queue_write_allowed")),
        "broad_daemon_allowed": bool(payload.get("broad_daemon_allowed") or safety.get("broad_daemon_allowed")),
        "automation_allowed": bool(payload.get("automation_allowed") or safety.get("automation_allowed")),
        "automated_rerun_allowed": bool(payload.get("automated_rerun_allowed") or safety.get("automated_rerun_allowed")),
        "live_trading_enabled": bool(payload.get("live_trading_enabled") or safety.get("live_trading_enabled")),
    }


def _operator_decision_wait_state_section(
    manual_gate: dict[str, Any],
    operator_summary: dict[str, Any],
    operator_intake: dict[str, Any],
    operator_handoff: dict[str, Any],
) -> dict[str, Any]:
    gate_pending = manual_gate.get("gate_status") == "blocked_pending_manual_approval"
    summary_pending = operator_summary.get("summary_status") == "blocked_pending_manual_approval"
    intake_status = operator_intake.get("intake_status")
    intake_missing_or_pending = intake_status in {"missing", "pending", None}
    human_approval_present = bool(manual_gate.get("human_approval_present") or operator_summary.get("human_approval_present"))
    intake_valid_without_human_approval = intake_status == "valid" and not human_approval_present
    handoff_waiting = operator_handoff.get("handoff_status") == "awaiting_operator_decision"
    waiting = gate_pending and summary_pending and handoff_waiting and (intake_missing_or_pending or intake_valid_without_human_approval)
    executable_evidence_present = intake_status == "valid" and not operator_intake.get("validation_errors") and human_approval_present

    return {
        "wait_state_status": "awaiting_operator_decision" if waiting else "not_waiting_on_operator_decision",
        "primary_blocker": operator_handoff.get("primary_blocker") or operator_summary.get("primary_blocker"),
        "decision_axis": operator_handoff.get("decision_axis") or operator_summary.get("required_decision_axis"),
        "human_approval_present": human_approval_present,
        "approval_required": bool(operator_summary.get("approval_required")),
        "intake_status": intake_status,
        "handoff_status": operator_handoff.get("handoff_status"),
        "validation_errors": operator_intake.get("validation_errors") or operator_handoff.get("validation_errors") or [],
        "execution_allowed": bool(executable_evidence_present and operator_handoff.get("execution_allowed")),
        "separate_execution_plan_required": bool(executable_evidence_present and operator_handoff.get("separate_execution_plan_required")),
        "queue_write_allowed": bool(executable_evidence_present and (manual_gate.get("queue_write_allowed") or operator_summary.get("queue_write_allowed") or operator_intake.get("queue_write_allowed") or operator_handoff.get("queue_write_allowed"))),
        "broad_daemon_allowed": bool(executable_evidence_present and (manual_gate.get("broad_daemon_allowed") or operator_summary.get("broad_daemon_allowed") or operator_intake.get("broad_daemon_allowed") or operator_handoff.get("broad_daemon_allowed"))),
        "automation_allowed": bool(executable_evidence_present and (manual_gate.get("automation_allowed") or operator_summary.get("automation_allowed") or operator_intake.get("automation_allowed") or operator_handoff.get("automation_allowed"))),
        "automated_rerun_allowed": bool(executable_evidence_present and (manual_gate.get("automated_rerun_allowed") or operator_summary.get("automated_rerun_allowed") or operator_intake.get("automated_rerun_allowed") or operator_handoff.get("automated_rerun_allowed"))),
        "live_trading_enabled": bool(executable_evidence_present and (manual_gate.get("live_trading_enabled") or operator_summary.get("live_trading_enabled") or operator_intake.get("live_trading_enabled") or operator_handoff.get("live_trading_enabled"))),
    }



def _operator_pending_observation_section(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {"observation_status": "missing"}
    benchmark = payload.get("benchmark") or {}
    turnover = payload.get("turnover") or {}
    cost = payload.get("cost") or {}
    blocker = payload.get("blocker") or {}
    runtime = payload.get("runtime") or {}
    return {
        "observation_status": payload.get("observation_status") or "missing",
        "primary_issue": blocker.get("primary_issue"),
        "manual_approval_status": blocker.get("manual_approval_status"),
        "human_approval_present": bool(blocker.get("human_approval_present")),
        "approval_required": bool(blocker.get("approval_required")),
        "benchmark_id": benchmark.get("benchmark_id"),
        "benchmark_name": benchmark.get("benchmark_name"),
        "tracking_mode": benchmark.get("tracking_mode"),
        "turnover_one_way_estimate": turnover.get("turnover_one_way_estimate"),
        "estimated_round_trip_cost": cost.get("estimated_round_trip_cost"),
        "queue_write_allowed": bool(runtime.get("queue_write_allowed")),
        "broad_daemon_allowed": bool(runtime.get("broad_daemon_allowed")),
        "automated_rerun_allowed": bool(runtime.get("automated_rerun_allowed")),
        "automation_allowed": bool(runtime.get("automation_allowed")),
        "live_trading_enabled": bool(runtime.get("live_trading_enabled")),
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
    manual_approval_gate: dict[str, Any] | None = None,
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


def _operator_pending_snapshot_markdown_section(snapshot: dict[str, Any]) -> list[str]:
    if not snapshot:
        return []
    reporting_only_snapshot = {
        **snapshot,
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
    }
    return [
        "- Operator-pending consistency snapshot:",
        f"  - Snapshot status: {reporting_only_snapshot.get('snapshot_status')}",
        f"  - Snapshot freshness status: {reporting_only_snapshot.get('snapshot_freshness_status')}",
        f"  - Source status generated: {reporting_only_snapshot.get('source_status_generated_at_utc')}",
        f"  - Latest status generated: {reporting_only_snapshot.get('latest_status_generated_at_utc')}",
        f"  - Consistency status: {reporting_only_snapshot.get('consistency_status')}",
        f"  - Mismatches: {reporting_only_snapshot.get('mismatches')}",
        f"  - Benchmark ID: {reporting_only_snapshot.get('benchmark_id')}",
        f"  - One-way turnover estimate: {reporting_only_snapshot.get('turnover_one_way_estimate')}",
        f"  - Estimated round-trip cost: {reporting_only_snapshot.get('estimated_round_trip_cost')}",
        f"  - Queue write allowed: {reporting_only_snapshot.get('queue_write_allowed')}",
        f"  - Broad daemon allowed: {reporting_only_snapshot.get('broad_daemon_allowed')}",
        f"  - Automation allowed: {reporting_only_snapshot.get('automation_allowed')}",
        f"  - Automated rerun allowed: {reporting_only_snapshot.get('automated_rerun_allowed')}",
        f"  - Live trading enabled: {reporting_only_snapshot.get('live_trading_enabled')}",
    ]


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
    drawdown_group = status.get("drawdown_group_diagnostic") or {}
    evidence = status.get("drawdown_blocker_evidence") or {}
    manual_review = status.get("repair_blocker_manual_review") or {}
    manual_gate = status.get("manual_approval_gate") or {}
    operator_summary = status.get("operator_approval_summary") or {}
    approval_consistency = status.get("approval_artifact_consistency") or {}
    operator_intake = status.get("operator_decision_intake_validation") or {}
    operator_handoff = status.get("operator_decision_handoff") or {}
    operator_wait_state = status.get("operator_decision_wait_state") or {}
    operator_pending = status.get("operator_pending_observation") or {}
    operator_pending_snapshot = status.get("operator_pending_consistency_snapshot") or monitoring.get("operator_pending_consistency_snapshot") or {}
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
            f"- Weekly would-run count: {monitoring.get('would_run_count')}",
            f"- Weekly runtime recommendations: {monitoring.get('recommendations')}",
            f"- Queue write allowed: {monitoring.get('queue_write_allowed')}",
            f"- Broad daemon allowed: {monitoring.get('broad_daemon_allowed')}",
            f"- Automation allowed: {monitoring.get('automation_allowed')}",
            f"- Automated rerun allowed: {monitoring.get('automated_rerun_allowed')}",
            f"- Live trading enabled: {monitoring.get('live_trading_enabled')}",
        ]
    )
    missing_artifacts = monitoring.get("missing_artifacts") or []
    if missing_artifacts:
        lines.append("- Missing artifacts:")
        lines.extend(f"  - {artifact}" for artifact in missing_artifacts)
    else:
        lines.append(f"- Missing artifacts: {monitoring.get('missing_artifacts')}")
    lines.append(f"- Next observation window: {monitoring.get('next_observation_window')}")
    weekly_blockers = monitoring.get("blockers") or {}
    if weekly_blockers:
        lines.extend(
            [
                "- Weekly blocker context:",
                f"  - Decision: {weekly_blockers.get('decision')}",
                f"  - Next action: {weekly_blockers.get('next_action')}",
                f"  - Primary issue: {weekly_blockers.get('primary_issue')}",
                f"  - Manual approval gate status: {weekly_blockers.get('manual_approval_gate_status')}",
                f"  - Human approval present: {weekly_blockers.get('human_approval_present')}",
                f"  - Approval required: {weekly_blockers.get('approval_required')}",
                f"  - Required decision axis: {weekly_blockers.get('required_decision_axis')}",
            ]
        )
    weekly_operator_pending = monitoring.get("operator_pending_observation") or {}
    if weekly_operator_pending:
        lines.extend(
            [
                "- Weekly operator-pending observation:",
                f"  - Observation status: {weekly_operator_pending.get('observation_status')}",
                f"  - Primary issue: {weekly_operator_pending.get('primary_issue')}",
                f"  - Manual approval status: {weekly_operator_pending.get('manual_approval_status')}",
                f"  - Benchmark ID: {weekly_operator_pending.get('benchmark_id')}",
                f"  - One-way turnover estimate: {weekly_operator_pending.get('turnover_one_way_estimate')}",
                f"  - Estimated round-trip cost: {weekly_operator_pending.get('estimated_round_trip_cost')}",
                f"  - Queue write allowed: {weekly_operator_pending.get('queue_write_allowed')}",
                f"  - Broad daemon allowed: {weekly_operator_pending.get('broad_daemon_allowed')}",
                f"  - Automation allowed: {weekly_operator_pending.get('automation_allowed')}",
                f"  - Automated rerun allowed: {weekly_operator_pending.get('automated_rerun_allowed')}",
                f"  - Live trading enabled: {weekly_operator_pending.get('live_trading_enabled')}",
            ]
        )
    operator_pending_consistency = monitoring.get("operator_pending_consistency") or {}
    if operator_pending_consistency:
        lines.extend(
            [
                "- Weekly/canonical operator-pending consistency:",
                f"  - Consistency status: {operator_pending_consistency.get('consistency_status')}",
                f"  - Mismatches: {operator_pending_consistency.get('mismatches')}",
                f"  - Queue write allowed: {operator_pending_consistency.get('queue_write_allowed')}",
                f"  - Broad daemon allowed: {operator_pending_consistency.get('broad_daemon_allowed')}",
                f"  - Automation allowed: {operator_pending_consistency.get('automation_allowed')}",
                f"  - Automated rerun allowed: {operator_pending_consistency.get('automated_rerun_allowed')}",
                f"  - Live trading enabled: {operator_pending_consistency.get('live_trading_enabled')}",
            ]
        )
    if operator_pending_snapshot:
        lines.extend(_operator_pending_snapshot_markdown_section(operator_pending_snapshot))
    lines.extend(
        [
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
            f"- Queue write allowed: {repair.get('queue_write_allowed')}",
            f"- Broad daemon allowed: {repair.get('broad_daemon_allowed')}",
            f"- Automated rerun allowed: {repair.get('automated_rerun_allowed')}",
            f"- Live trading enabled: {repair.get('live_trading_enabled')}",
            "",
            "## Drawdown group diagnostic",
            f"- Diagnostic status: {drawdown_group.get('diagnostic_status')}",
            f"- Recommended dimension: {drawdown_group.get('recommended_dimension')}",
            f"- Recommended value: {drawdown_group.get('recommended_value')}",
            f"- Best max drawdown: {drawdown_group.get('best_max_drawdown')}",
            f"- Drawdown gap to limit: {drawdown_group.get('drawdown_gap_to_limit')}",
            f"- Automation allowed: {drawdown_group.get('automation_allowed')}",
            "",
            "## Drawdown blocker evidence",
            f"- Evidence status: {evidence.get('evidence_status')}",
            f"- Primary issue: {evidence.get('primary_issue')}",
            f"- Repair status: {evidence.get('repair_status')}",
            f"- Candidate count: {evidence.get('candidate_count')}",
            f"- Manual review dimension: {evidence.get('manual_review_dimension')}",
            f"- Manual review value: {evidence.get('manual_review_value')}",
            f"- Queue write allowed: {evidence.get('queue_write_allowed')}",
            f"- Broad daemon allowed: {evidence.get('broad_daemon_allowed')}",
            f"- Benchmark ID: {evidence.get('benchmark_id')}",
            f"- Benchmark name: {evidence.get('benchmark_name')}",
            f"- Tracking mode: {evidence.get('tracking_mode')}",
            f"- One-way turnover estimate: {evidence.get('turnover_one_way_estimate')}",
            f"- Estimated round-trip cost: {evidence.get('estimated_round_trip_cost')}",
            "",
            "## Repair blocker manual review",
            f"- Review status: {manual_review.get('review_status')}",
            f"- Primary issue: {manual_review.get('primary_issue')}",
            f"- Repair status: {manual_review.get('repair_status')}",
            f"- Candidate count: {manual_review.get('candidate_count')}",
            f"- Best available max drawdown: {manual_review.get('best_available_max_drawdown')}",
            f"- Drawdown gap to limit: {manual_review.get('drawdown_gap_to_limit')}",
            f"- Queue write allowed: {manual_review.get('queue_write_allowed')}",
            f"- Broad daemon allowed: {manual_review.get('broad_daemon_allowed')}",
            f"- Automation allowed: {manual_review.get('automation_allowed')}",
            f"- Manual decision: {manual_review.get('manual_decision_dimension')}={manual_review.get('manual_decision_value')}",
            f"- Automated rerun allowed: {manual_review.get('automated_rerun_allowed')}",
            "",
            "## Manual approval gate",
            f"- Gate status: {manual_gate.get('gate_status')}",
            f"- Human approval present: {manual_gate.get('human_approval_present')}",
            f"- Risk relaxation allowed: {manual_gate.get('risk_relaxation_allowed')}",
            f"- Queue write allowed: {manual_gate.get('queue_write_allowed')}",
            f"- Broad daemon allowed: {manual_gate.get('broad_daemon_allowed')}",
            f"- Automation allowed: {manual_gate.get('automation_allowed')}",
            f"- Automated rerun allowed: {manual_gate.get('automated_rerun_allowed')}",
            f"- Live trading enabled: {manual_gate.get('live_trading_enabled')}",
            "",
            "## Operator approval summary",
            f"- Summary status: {operator_summary.get('summary_status')}",
            f"- Approval required: {operator_summary.get('approval_required')}",
            f"- Human approval present: {operator_summary.get('human_approval_present')}",
            f"- Required decision axis: {operator_summary.get('required_decision_axis')}",
            f"- Primary blocker: {operator_summary.get('primary_blocker')}",
            f"- Repair status: {operator_summary.get('repair_status')}",
            f"- Candidate count: {operator_summary.get('candidate_count')}",
            f"- Queue write allowed: {operator_summary.get('queue_write_allowed')}",
            f"- Broad daemon allowed: {operator_summary.get('broad_daemon_allowed')}",
            f"- Automation allowed: {operator_summary.get('automation_allowed')}",
            f"- Automated rerun allowed: {operator_summary.get('automated_rerun_allowed')}",
            f"- Live trading enabled: {operator_summary.get('live_trading_enabled')}",
            "",
            "## Approval artifact consistency",
            f"- Consistency status: {approval_consistency.get('consistency_status')}",
            f"- Primary blocker: {approval_consistency.get('primary_blocker')}",
            f"- Decision axis: {approval_consistency.get('decision_axis')}",
            f"- Queue write allowed: {approval_consistency.get('queue_write_allowed')}",
            f"- Broad daemon allowed: {approval_consistency.get('broad_daemon_allowed')}",
            f"- Automation allowed: {approval_consistency.get('automation_allowed')}",
            f"- Automated rerun allowed: {approval_consistency.get('automated_rerun_allowed')}",
            f"- Live trading enabled: {approval_consistency.get('live_trading_enabled')}",
            f"- Inconsistencies: {approval_consistency.get('inconsistencies')}",
            f"- Staleness warnings: {approval_consistency.get('staleness_warnings')}",
            "",
            "## Operator decision intake validation",
            f"- Intake status: {operator_intake.get('intake_status')}",
            f"- Decision type: {operator_intake.get('decision_type')}",
            f"- Non-mutating: {operator_intake.get('non_mutating')}",
            f"- Validation errors: {operator_intake.get('validation_errors')}",
            f"- Queue write allowed: {operator_intake.get('queue_write_allowed')}",
            f"- Broad daemon allowed: {operator_intake.get('broad_daemon_allowed')}",
            f"- Automation allowed: {operator_intake.get('automation_allowed')}",
            f"- Automated rerun allowed: {operator_intake.get('automated_rerun_allowed')}",
            f"- Live trading enabled: {operator_intake.get('live_trading_enabled')}",
            "",
            "## Operator decision handoff",
            f"- Handoff status: {operator_handoff.get('handoff_status')}",
            f"- Intake status: {operator_handoff.get('intake_status')}",
            f"- Decision type: {operator_handoff.get('decision_type')}",
            f"- Decision axis: {operator_handoff.get('decision_axis')}",
            f"- Primary blocker: {operator_handoff.get('primary_blocker')}",
            f"- Execution allowed: {operator_handoff.get('execution_allowed')}",
            f"- Separate execution plan required: {operator_handoff.get('separate_execution_plan_required')}",
            f"- Validation errors: {operator_handoff.get('validation_errors')}",
            f"- Queue write allowed: {operator_handoff.get('queue_write_allowed')}",
            f"- Broad daemon allowed: {operator_handoff.get('broad_daemon_allowed')}",
            f"- Automation allowed: {operator_handoff.get('automation_allowed')}",
            f"- Automated rerun allowed: {operator_handoff.get('automated_rerun_allowed')}",
            f"- Live trading enabled: {operator_handoff.get('live_trading_enabled')}",
            "",
            "## Operator decision wait state",
            f"- Wait-state status: {operator_wait_state.get('wait_state_status')}",
            f"- Primary blocker: {operator_wait_state.get('primary_blocker')}",
            f"- Decision axis: {operator_wait_state.get('decision_axis')}",
            f"- Human approval present: {operator_wait_state.get('human_approval_present')}",
            f"- Approval required: {operator_wait_state.get('approval_required')}",
            f"- Intake status: {operator_wait_state.get('intake_status')}",
            f"- Handoff status: {operator_wait_state.get('handoff_status')}",
            f"- Validation errors: {operator_wait_state.get('validation_errors')}",
            f"- Execution allowed: {operator_wait_state.get('execution_allowed')}",
            f"- Separate execution plan required: {operator_wait_state.get('separate_execution_plan_required')}",
            f"- Queue write allowed: {operator_wait_state.get('queue_write_allowed')}",
            f"- Broad daemon allowed: {operator_wait_state.get('broad_daemon_allowed')}",
            f"- Automation allowed: {operator_wait_state.get('automation_allowed')}",
            f"- Automated rerun allowed: {operator_wait_state.get('automated_rerun_allowed')}",
            f"- Live trading enabled: {operator_wait_state.get('live_trading_enabled')}",
            "",
            "## Operator pending observation",
            f"- Observation status: {operator_pending.get('observation_status')}",
        ]
    )
    if operator_pending.get("observation_status") != "missing":
        lines.extend(
            [
                f"- Primary issue: {operator_pending.get('primary_issue')}",
                f"- Manual approval status: {operator_pending.get('manual_approval_status')}",
                f"- Benchmark ID: {operator_pending.get('benchmark_id')}",
                f"- One-way turnover estimate: {operator_pending.get('turnover_one_way_estimate')}",
                f"- Estimated round-trip cost: {operator_pending.get('estimated_round_trip_cost')}",
                f"- Queue write allowed: {operator_pending.get('queue_write_allowed')}",
                f"- Broad daemon allowed: {operator_pending.get('broad_daemon_allowed')}",
                f"- Automation allowed: {operator_pending.get('automation_allowed')}",
                f"- Automated rerun allowed: {operator_pending.get('automated_rerun_allowed')}",
                f"- Live trading enabled: {operator_pending.get('live_trading_enabled')}",
            ]
        )
    lines.extend(
        [
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
    kwargs.setdefault("status_json_path", json_path)
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
