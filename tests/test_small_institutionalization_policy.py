import json
from pathlib import Path

from factor_lab.small_institutional_operator_pending_consistency_snapshot import (
    write_operator_pending_consistency_snapshot,
)
from factor_lab.small_institutionalization_policy import (
    build_small_institutionalization_status,
    status_to_markdown,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _baseline_files(tmp_path: Path) -> dict[str, Path]:
    policy = _write_json(
        tmp_path / "configs" / "small_institutionalization_policy.json",
        {
            "strategy_mode": "long_only_equity_enhancement",
            "target_holdings_min": 50,
            "target_holdings_max": 100,
            "benchmark_candidates": ["CSI500", "CSI1000"],
        },
    )
    research_quality = _write_json(
        tmp_path / "artifacts" / "research_quality_summary.json",
        {
            "value_sleeve_policy": {
                "decision": "collapse_to_value_sleeve_with_primary_route",
                "primary_route": "value_quality_no_distress",
            }
        },
    )
    dry_run = _write_json(
        tmp_path / "artifacts" / "controlled_restart_dry_run.json",
        {"would_run_count": 0, "blocked_count": 0, "pending_non_workflow_count": 4},
    )
    runtime_audit = _write_json(
        tmp_path / "artifacts" / "runtime_takeover_audit.json",
        {"recommendations": ["pause_broad_daemon", "allow_controlled_only_daemon"]},
    )
    value_sleeve_policy = _write_json(
        tmp_path / "artifacts" / "value_sleeve_validation" / "value_sleeve_policy.json",
        {
            "decision": "collapse_to_value_sleeve_with_primary_route",
            "primary_route": "value_quality_no_distress",
            "confirmation_route": "value_momentum_confirmation",
            "routes": {"value_quality_no_distress": {"role": "primary"}},
        },
    )
    paper_portfolio = _write_json(
        tmp_path / "artifacts" / "paper_portfolio" / "current_portfolio.json",
        {"strategy_name": "paper_candidates_only", "position_count": 60, "as_of_date": "2023-12-29"},
    )
    diagnostics_path = tmp_path / "artifacts" / "paper_portfolio" / "portfolio_diagnostics.json"
    weekly_monitoring_report_path = tmp_path / "artifacts" / "paper_portfolio" / "weekly_monitoring_report.json"
    retrospective_tracking_path = tmp_path / "artifacts" / "paper_portfolio" / "retrospective_return_tracking.json"
    constraint_hardening_path = tmp_path / "artifacts" / "paper_portfolio" / "portfolio_constraint_hardening.json"
    promotion_readiness_path = tmp_path / "artifacts" / "paper_portfolio" / "paper_live_promotion_readiness.json"
    simulation_self_diagnosis_path = tmp_path / "artifacts" / "small_institutional_simulation" / "self_diagnosis.json"
    simulated_portfolio_construction_repair_path = tmp_path / "artifacts" / "small_institutional_simulation" / "portfolio_construction_repair.json"
    drawdown_group_diagnostic_path = tmp_path / "artifacts" / "small_institutional_simulation" / "drawdown_group_diagnostic.json"
    drawdown_blocker_evidence_path = tmp_path / "artifacts" / "small_institutional_simulation" / "drawdown_blocker_evidence.json"
    repair_blocker_manual_review_path = tmp_path / "artifacts" / "small_institutional_simulation" / "repair_blocker_manual_review.json"
    manual_approval_gate_path = tmp_path / "artifacts" / "small_institutional_simulation" / "manual_approval_gate.json"
    operator_approval_summary_path = tmp_path / "artifacts" / "small_institutional_simulation" / "operator_approval_summary.json"
    approval_artifact_consistency_path = tmp_path / "artifacts" / "small_institutional_simulation" / "approval_artifact_consistency.json"
    operator_decision_intake_validation_path = tmp_path / "artifacts" / "small_institutional_simulation" / "operator_decision_intake_validation.json"
    operator_decision_handoff_path = tmp_path / "artifacts" / "small_institutional_simulation" / "operator_decision_handoff.json"
    operator_pending_observation_path = tmp_path / "artifacts" / "small_institutional_simulation" / "operator_pending_observation.json"
    operator_pending_consistency_snapshot_path = tmp_path / "artifacts" / "small_institutionalization" / "operator_pending_consistency_snapshot.json"
    status_json_path = tmp_path / "artifacts" / "small_institutionalization" / "status.json"
    return {
        "policy_path": policy,
        "research_quality_path": research_quality,
        "dry_run_path": dry_run,
        "runtime_audit_path": runtime_audit,
        "value_sleeve_policy_path": value_sleeve_policy,
        "paper_portfolio_path": paper_portfolio,
        "portfolio_diagnostics_path": diagnostics_path,
        "weekly_monitoring_report_path": weekly_monitoring_report_path,
        "retrospective_tracking_path": retrospective_tracking_path,
        "constraint_hardening_path": constraint_hardening_path,
        "promotion_readiness_path": promotion_readiness_path,
        "simulation_self_diagnosis_path": simulation_self_diagnosis_path,
        "simulated_portfolio_construction_repair_path": simulated_portfolio_construction_repair_path,
        "drawdown_group_diagnostic_path": drawdown_group_diagnostic_path,
        "drawdown_blocker_evidence_path": drawdown_blocker_evidence_path,
        "repair_blocker_manual_review_path": repair_blocker_manual_review_path,
        "manual_approval_gate_path": manual_approval_gate_path,
        "operator_approval_summary_path": operator_approval_summary_path,
        "approval_artifact_consistency_path": approval_artifact_consistency_path,
        "operator_decision_intake_validation_path": operator_decision_intake_validation_path,
        "operator_decision_handoff_path": operator_decision_handoff_path,
        "operator_pending_observation_path": operator_pending_observation_path,
        "operator_pending_consistency_snapshot_path": operator_pending_consistency_snapshot_path,
        "status_json_path": status_json_path,
    }


def _complete_paper_path(paths: dict[str, Path]) -> None:
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(paths["weekly_monitoring_report_path"], {"cadence": "weekly"})
    _write_json(paths["retrospective_tracking_path"], {"tracking_status": "ok", "portfolio_return": {"portfolio_forward_return": 0.0123}})
    _write_json(paths["constraint_hardening_path"], {"constraint_status": "pass", "violations": [], "warnings": []})
    _write_json(paths["promotion_readiness_path"], {"readiness_status": "ready_for_manual_approval", "blockers": [], "warnings": [], "manual_approval_required": True, "live_trading_enabled": False})


def test_status_ready_for_portfolio_mvp_when_runtime_sleeve_and_paper_are_ready(tmp_path):
    paths = _baseline_files(tmp_path)

    status = build_small_institutionalization_status(**paths)

    assert status["phase"] == "A_baseline"
    assert status["decision"] == "ready_for_portfolio_mvp"
    assert status["runtime_safety"]["safe"] is True
    assert status["value_sleeve"]["primary_route"] == "value_quality_no_distress"
    assert status["paper_portfolio"]["position_count"] == 60
    assert not status["blockers"]
    assert status["next_action"] == "write_benchmark_cost_turnover_diagnostics"


def test_status_blocks_when_broad_daemon_runtime_is_not_paused(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["runtime_audit_path"], {"recommendations": ["restore_broad_daemon"]})

    status = build_small_institutionalization_status(**paths)

    assert status["decision"] == "blocked_runtime_safety"
    assert "broad_daemon_not_paused" in status["blockers"]


def test_status_requires_paper_portfolio_baseline_without_runtime_block(tmp_path):
    paths = _baseline_files(tmp_path)
    paths["paper_portfolio_path"].unlink()

    status = build_small_institutionalization_status(**paths)

    assert status["decision"] == "needs_paper_portfolio_baseline"
    assert "missing_paper_portfolio_baseline" in status["blockers"]
    assert status["runtime_safety"]["safe"] is True


def test_status_requires_non_empty_paper_portfolio_baseline(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["paper_portfolio_path"], {"strategy_name": "paper_candidates_only", "position_count": 0})

    status = build_small_institutionalization_status(**paths)

    assert status["decision"] == "needs_paper_portfolio_baseline"
    assert "empty_paper_portfolio_baseline" in status["blockers"]


def test_status_markdown_includes_decision_and_next_action(tmp_path):
    paths = _baseline_files(tmp_path)
    status = build_small_institutionalization_status(**paths)

    markdown = status_to_markdown(status)

    assert "ready_for_portfolio_mvp" in markdown
    assert "value_quality_no_distress" in markdown
    assert "Next action" in markdown


def test_status_includes_drawdown_group_diagnostic_without_advancing_next_action(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(
        paths["simulation_self_diagnosis_path"],
        {
            "diagnosis_status": "blocked",
            "primary_issue": "drawdown_risk_too_high",
            "severity": "high",
            "recommended_run_mode": "bounded_matrix",
            "automation_allowed": False,
        },
    )
    _write_json(
        paths["simulated_portfolio_construction_repair_path"],
        {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False},
    )
    _write_json(
        paths["drawdown_group_diagnostic_path"],
        {
            "diagnostic_status": "blocked_no_group_under_drawdown_limit",
            "recommended_manual_axis": {
                "dimension": "holding_count",
                "value": "50",
                "best_max_drawdown": -0.478256,
                "drawdown_gap_to_limit": 0.128256,
            },
            "automation_allowed": False,
        },
    )

    status = build_small_institutionalization_status(**paths)

    diagnostic = status["drawdown_group_diagnostic"]
    assert diagnostic["diagnostic_status"] == "blocked_no_group_under_drawdown_limit"
    assert diagnostic["recommended_dimension"] == "holding_count"
    assert diagnostic["recommended_value"] == "50"
    assert diagnostic["best_max_drawdown"] == -0.478256
    assert diagnostic["drawdown_gap_to_limit"] == 0.128256
    assert diagnostic["automation_allowed"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"

    markdown = status_to_markdown(status)
    assert "Drawdown group diagnostic" in markdown
    assert "blocked_no_group_under_drawdown_limit" in markdown


def test_status_markdown_surfaces_repair_non_mutating_safety_flags(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(
        paths["simulation_self_diagnosis_path"],
        {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False},
    )
    _write_json(
        paths["simulated_portfolio_construction_repair_path"],
        {
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "recommended_candidate": None,
            "automation_allowed": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        },
    )

    status = build_small_institutionalization_status(**paths)
    markdown = status_to_markdown(status)

    repair_section = markdown.split("## Simulated portfolio construction repair", maxsplit=1)[1].split(
        "## Drawdown group diagnostic", maxsplit=1
    )[0]
    assert "- Queue write allowed: False" in repair_section
    assert "- Broad daemon allowed: False" in repair_section
    assert "- Automation allowed: False" in repair_section
    assert "- Automated rerun allowed: False" in repair_section
    assert "- Live trading enabled: False" in repair_section


def test_status_surfaces_drawdown_blocker_evidence_without_advancing_next_action(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(
        paths["simulation_self_diagnosis_path"],
        {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False},
    )
    _write_json(
        paths["simulated_portfolio_construction_repair_path"],
        {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False},
    )
    _write_json(
        paths["drawdown_blocker_evidence_path"],
        {
            "blocker": {"primary_issue": "drawdown_risk_too_high"},
            "repair": {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0},
            "manual_review": {"dimension": "holding_count", "value": "50"},
            "paper_portfolio_context": {
                "benchmark_id": "CSI1000",
                "benchmark_name": "中证1000",
                "tracking_mode": "metadata_only",
                "turnover_one_way_estimate": 0.791672,
                "estimated_round_trip_cost": 0.00475,
            },
            "safety": {"queue_write_allowed": False, "broad_daemon_allowed": False},
        },
    )

    status = build_small_institutionalization_status(**paths)

    evidence = status["drawdown_blocker_evidence"]
    assert evidence == {
        "evidence_status": "ready",
        "primary_issue": "drawdown_risk_too_high",
        "repair_status": "blocked_no_drawdown_safe_candidate",
        "candidate_count": 0,
        "manual_review_dimension": "holding_count",
        "manual_review_value": "50",
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "benchmark_id": "CSI1000",
        "benchmark_name": "中证1000",
        "tracking_mode": "metadata_only",
        "turnover_one_way_estimate": 0.791672,
        "estimated_round_trip_cost": 0.00475,
    }
    assert status["next_action"] == "repair_simulated_portfolio_construction"

    markdown = status_to_markdown(status)
    assert "Drawdown blocker evidence" in markdown
    assert "Queue write allowed: False" in markdown


def test_status_marks_drawdown_blocker_evidence_missing_without_adding_blockers(tmp_path):
    paths = _baseline_files(tmp_path)

    status = build_small_institutionalization_status(**paths)

    assert status["drawdown_blocker_evidence"] == {"evidence_status": "missing"}
    assert status["repair_blocker_manual_review"] == {"review_status": "missing"}
    assert status["blockers"] == []
    assert status["next_action"] == "write_benchmark_cost_turnover_diagnostics"


def test_status_surfaces_repair_blocker_manual_review_without_advancing_next_action(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(
        paths["simulation_self_diagnosis_path"],
        {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False},
    )
    _write_json(
        paths["simulated_portfolio_construction_repair_path"],
        {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False},
    )
    _write_json(
        paths["repair_blocker_manual_review_path"],
        {
            "review_status": "blocked_manual_review_required",
            "primary_issue": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "best_available_max_drawdown": -0.478256,
            "drawdown_gap_to_limit": 0.128256,
            "safety": {"queue_write_allowed": False, "broad_daemon_allowed": False, "automation_allowed": False},
            "recommended_manual_decision": {
                "decision_required": True,
                "dimension": "holding_count",
                "value": "50",
                "automated_rerun_allowed": False,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    review = status["repair_blocker_manual_review"]
    assert review["review_status"] == "blocked_manual_review_required"
    assert review["queue_write_allowed"] is False
    assert review["broad_daemon_allowed"] is False
    assert review["automation_allowed"] is False
    assert review["manual_decision_required"] is True
    assert review["manual_decision_dimension"] == "holding_count"
    assert review["manual_decision_value"] == "50"
    assert review["automated_rerun_allowed"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"

    markdown = status_to_markdown(status)
    assert "Repair blocker manual review" in markdown
    assert "Manual decision: holding_count=50" in markdown


def test_status_preserves_repair_blocker_manual_review_continuity_at_operator_wait_state(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(
        paths["simulation_self_diagnosis_path"],
        {
            "diagnosis_status": "blocked",
            "primary_issue": "drawdown_risk_too_high",
            "severity": "high",
            "recommended_run_mode": "bounded_matrix",
            "automation_allowed": False,
        },
    )
    _write_json(
        paths["simulated_portfolio_construction_repair_path"],
        {
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "recommended_candidate": None,
            "automation_allowed": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        },
    )
    _write_json(
        paths["repair_blocker_manual_review_path"],
        {
            "review_status": "blocked_manual_review_required",
            "primary_issue": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "best_available_max_drawdown": -0.478256,
            "drawdown_gap_to_limit": 0.128256,
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
            },
            "recommended_manual_decision": {
                "decision_required": True,
                "dimension": "holding_count",
                "value": "50",
                "automated_rerun_allowed": False,
            },
        },
    )
    _write_json(
        paths["manual_approval_gate_path"],
        {
            "gate_status": "blocked_pending_manual_approval",
            "human_approval_present": False,
            "risk_relaxation_allowed": False,
            "automated_rerun_allowed": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automation_allowed": False,
            "live_trading_enabled": False,
        },
    )
    _write_json(
        paths["operator_approval_summary_path"],
        {
            "summary_status": "blocked_pending_manual_approval",
            "approval_required": True,
            "human_approval_present": False,
            "required_decision_axis": "holding_count=50",
            "primary_blocker": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "best_available_max_drawdown": -0.478256,
            "drawdown_gap_to_limit": 0.128256,
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )
    _write_json(
        paths["operator_decision_handoff_path"],
        {
            "handoff_status": "awaiting_operator_decision",
            "intake_status": "missing",
            "primary_blocker": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "decision_axis": "holding_count=50",
            "validation_errors": [],
            "non_mutating": True,
            "execution_allowed": False,
            "separate_execution_plan_required": False,
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    manual_review = status["repair_blocker_manual_review"]
    assert manual_review["review_status"] == "blocked_manual_review_required"
    assert manual_review["repair_status"] == "blocked_no_drawdown_safe_candidate"
    assert manual_review["manual_decision_required"] is True
    assert manual_review["manual_decision_dimension"] == "holding_count"
    assert manual_review["manual_decision_value"] == "50"
    assert manual_review["queue_write_allowed"] is False
    assert manual_review["broad_daemon_allowed"] is False
    assert manual_review["automation_allowed"] is False
    assert manual_review["automated_rerun_allowed"] is False
    assert status["manual_approval_gate"]["gate_status"] == "blocked_pending_manual_approval"
    wait_state = status["operator_decision_wait_state"]
    assert wait_state["wait_state_status"] == "awaiting_operator_decision"
    assert wait_state["execution_allowed"] is False
    assert wait_state["queue_write_allowed"] is False
    assert wait_state["broad_daemon_allowed"] is False
    assert wait_state["automation_allowed"] is False
    assert wait_state["automated_rerun_allowed"] is False
    assert wait_state["live_trading_enabled"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"

    markdown = status_to_markdown(status)
    manual_review_section = markdown.split("## Repair blocker manual review", maxsplit=1)[1].split(
        "## Manual approval gate", maxsplit=1
    )[0]
    assert "- Queue write allowed: False" in manual_review_section
    assert "- Broad daemon allowed: False" in manual_review_section
    assert "- Automation allowed: False" in manual_review_section
    assert "- Manual decision: holding_count=50" in manual_review_section
    assert "- Automated rerun allowed: False" in manual_review_section
    assert "- Automated rerun allowed: True" not in manual_review_section
    assert "- Queue write allowed: True" not in manual_review_section


def test_status_surfaces_manual_approval_gate_without_advancing_next_action(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(
        paths["simulation_self_diagnosis_path"],
        {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False},
    )
    _write_json(
        paths["simulated_portfolio_construction_repair_path"],
        {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False},
    )
    _write_json(
        paths["manual_approval_gate_path"],
        {
            "gate_status": "blocked_pending_manual_approval",
            "human_approval_present": False,
            "risk_relaxation_allowed": False,
            "automated_rerun_allowed": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automation_allowed": False,
            "safety": {"queue_write_allowed": False, "broad_daemon_allowed": False, "automation_allowed": False},
        },
    )

    status = build_small_institutionalization_status(**paths)

    gate = status["manual_approval_gate"]
    assert gate["gate_status"] == "blocked_pending_manual_approval"
    assert gate["human_approval_present"] is False
    assert gate["risk_relaxation_allowed"] is False
    assert gate["automated_rerun_allowed"] is False
    assert gate["queue_write_allowed"] is False
    assert gate["broad_daemon_allowed"] is False
    assert gate["automation_allowed"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"

    markdown = status_to_markdown(status)
    assert "Manual approval gate" in markdown
    assert "Gate status: blocked_pending_manual_approval" in markdown


def test_status_includes_portfolio_diagnostics_when_present_and_advances_next_action(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}, "turnover": {"turnover_one_way_estimate": 0.25}, "cost": {"estimated_round_trip_cost": 0.0015}})

    status = build_small_institutionalization_status(**paths)

    assert status["paper_portfolio"]["benchmark_id"] == "CSI1000"
    assert status["paper_portfolio"]["turnover_one_way_estimate"] == 0.25
    assert status["paper_portfolio"]["estimated_round_trip_cost"] == 0.0015
    assert status["next_action"] == "paper_monitoring_weekly_report"


def test_status_advances_to_retrospective_tracking_after_weekly_monitoring_report_exists(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(
        paths["weekly_monitoring_report_path"],
        {
            "cadence": "weekly",
            "missing_artifacts": [],
            "runtime": {"safe": True},
            "next_observation_window": "next_weekly_paper_review",
        },
    )

    status = build_small_institutionalization_status(**paths)

    assert status["paper_monitoring"]["weekly_report_status"] == "ready"
    assert status["paper_monitoring"]["cadence"] == "weekly"
    assert status["paper_monitoring"]["missing_artifacts"] == []
    assert status["paper_monitoring"]["runtime_safe"] is True
    assert status["paper_monitoring"]["next_observation_window"] == "next_weekly_paper_review"
    assert status["next_action"] == "run_small_institutional_self_diagnosis"


def test_status_tolerates_legacy_list_shaped_weekly_blockers_without_mutation(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(
        paths["weekly_monitoring_report_path"],
        {
            "cadence": "weekly",
            "missing_artifacts": [],
            "runtime": {
                "safe": True,
                "would_run_count": 0,
                "queue_write_allowed": True,
                "broad_daemon_allowed": True,
                "automation_allowed": True,
                "automated_rerun_allowed": True,
                "live_trading_enabled": True,
            },
            "blockers": ["legacy_drawdown_risk_too_high"],
            "next_observation_window": "next_weekly_paper_review",
        },
    )

    status = build_small_institutionalization_status(**paths)
    monitoring = status["paper_monitoring"]

    assert monitoring["weekly_report_status"] == "ready"
    assert monitoring["missing_artifacts"] == []
    assert "blockers" not in monitoring
    assert monitoring["queue_write_allowed"] is False
    assert monitoring["broad_daemon_allowed"] is False
    assert monitoring["automation_allowed"] is False
    assert monitoring["automated_rerun_allowed"] is False
    assert monitoring["live_trading_enabled"] is False
    assert status["next_action"] == "run_small_institutional_self_diagnosis"

    markdown = status_to_markdown(status)
    paper_monitoring_section = markdown.split("## Paper monitoring", 1)[1].split("\n## ", 1)[0]
    assert "- Weekly report status: ready" in paper_monitoring_section
    assert "- Queue write allowed: False" in paper_monitoring_section
    assert "- Broad daemon allowed: False" in paper_monitoring_section
    assert "- Automation allowed: False" in paper_monitoring_section
    assert "- Automated rerun allowed: False" in paper_monitoring_section
    assert "- Live trading enabled: False" in paper_monitoring_section
    assert "- Queue write allowed: True" not in paper_monitoring_section
    assert "- Broad daemon allowed: True" not in paper_monitoring_section
    assert "- Automation allowed: True" not in paper_monitoring_section
    assert "- Automated rerun allowed: True" not in paper_monitoring_section
    assert "- Live trading enabled: True" not in paper_monitoring_section


def test_status_hardens_weekly_operator_pending_runtime_flags_in_status_and_markdown(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(
        paths["weekly_monitoring_report_path"],
        {
            "cadence": "weekly",
            "missing_artifacts": [],
            "runtime": {"safe": True, "would_run_count": 0},
            "next_observation_window": "next_weekly_paper_review",
            "operator_pending_observation": {
                "observation_status": "operator_pending",
                "primary_issue": "drawdown_risk_too_high",
                "manual_approval_status": "blocked_pending_manual_approval",
                "queue_write_allowed": True,
                "broad_daemon_allowed": True,
                "automation_allowed": True,
                "automated_rerun_allowed": True,
                "live_trading_enabled": True,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    assert status["paper_monitoring"]["weekly_report_status"] == "ready"
    assert status["next_action"] == "run_small_institutional_self_diagnosis"
    weekly_operator_pending = status["paper_monitoring"]["operator_pending_observation"]
    assert weekly_operator_pending["queue_write_allowed"] is False
    assert weekly_operator_pending["broad_daemon_allowed"] is False
    assert weekly_operator_pending["automation_allowed"] is False
    assert weekly_operator_pending["automated_rerun_allowed"] is False
    assert weekly_operator_pending["live_trading_enabled"] is False

    markdown = status_to_markdown(status)
    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    weekly_operator_pending_section = paper_monitoring_section.split(
        "- Weekly operator-pending observation:", maxsplit=1
    )[1].split("- Weekly/canonical operator-pending consistency:", maxsplit=1)[0]
    assert "Queue write allowed: False" in weekly_operator_pending_section
    assert "Broad daemon allowed: False" in weekly_operator_pending_section
    assert "Automation allowed: False" in weekly_operator_pending_section
    assert "Automated rerun allowed: False" in weekly_operator_pending_section
    assert "Live trading enabled: False" in weekly_operator_pending_section
    assert "Queue write allowed: True" not in weekly_operator_pending_section
    assert "Broad daemon allowed: True" not in weekly_operator_pending_section
    assert "Automation allowed: True" not in weekly_operator_pending_section
    assert "Automated rerun allowed: True" not in weekly_operator_pending_section
    assert "Live trading enabled: True" not in weekly_operator_pending_section


def test_status_markdown_renders_paper_monitoring_next_observation_window(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(
        paths["weekly_monitoring_report_path"],
        {
            "cadence": "weekly",
            "missing_artifacts": [],
            "runtime": {"safe": True},
            "next_observation_window": "next_weekly_paper_review",
            "operator_pending_observation": {
                "observation_status": "operator_pending",
                "primary_issue": "drawdown_risk_too_high",
                "manual_approval_status": "blocked_pending_manual_approval",
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)
    markdown = status_to_markdown(status)

    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    assert "- Next observation window: next_weekly_paper_review" in paper_monitoring_section
    assert "- Queue write allowed: False" in paper_monitoring_section
    assert "- Broad daemon allowed: False" in paper_monitoring_section
    assert "- Automation allowed: False" in paper_monitoring_section
    assert "- Automated rerun allowed: False" in paper_monitoring_section
    assert "- Live trading enabled: False" in paper_monitoring_section
    assert status["next_action"] == "run_small_institutional_self_diagnosis"


def test_status_preserves_weekly_missing_artifacts_in_status_and_markdown(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(
        paths["weekly_monitoring_report_path"],
        {
            "cadence": "weekly",
            "missing_artifacts": ["operator_pending_observation"],
            "runtime": {
                "safe": True,
                "would_run_count": 0,
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
            "next_observation_window": "next_weekly_paper_review",
        },
    )

    status = build_small_institutionalization_status(**paths)
    markdown = status_to_markdown(status)

    monitoring = status["paper_monitoring"]
    assert monitoring["weekly_report_status"] == "incomplete"
    assert monitoring["missing_artifacts"] == ["operator_pending_observation"]
    assert monitoring["queue_write_allowed"] is False
    assert monitoring["broad_daemon_allowed"] is False
    assert monitoring["automation_allowed"] is False
    assert monitoring["automated_rerun_allowed"] is False
    assert monitoring["live_trading_enabled"] is False

    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    assert "- Missing artifacts:" in paper_monitoring_section
    assert "  - operator_pending_observation" in paper_monitoring_section
    assert "- Missing artifacts: ['operator_pending_observation']" not in paper_monitoring_section
    assert "- Queue write allowed: False" in paper_monitoring_section
    assert "- Broad daemon allowed: False" in paper_monitoring_section
    assert "- Automation allowed: False" in paper_monitoring_section
    assert "- Automated rerun allowed: False" in paper_monitoring_section
    assert "- Live trading enabled: False" in paper_monitoring_section


def test_status_keeps_empty_or_missing_weekly_missing_artifacts_markdown_compact(tmp_path):
    for missing_artifacts_key_present in (True, False):
        paths = _baseline_files(tmp_path / str(missing_artifacts_key_present))
        _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
        weekly_report = {
            "cadence": "weekly",
            "runtime": {
                "safe": True,
                "would_run_count": 0,
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
            "next_observation_window": "next_weekly_paper_review",
        }
        if missing_artifacts_key_present:
            weekly_report["missing_artifacts"] = []
        _write_json(paths["weekly_monitoring_report_path"], weekly_report)

        status = build_small_institutionalization_status(**paths)
        monitoring = status["paper_monitoring"]
        assert monitoring["weekly_report_status"] == "ready"
        assert monitoring["missing_artifacts"] == []
        assert monitoring["queue_write_allowed"] is False
        assert monitoring["broad_daemon_allowed"] is False
        assert monitoring["automation_allowed"] is False
        assert monitoring["automated_rerun_allowed"] is False
        assert monitoring["live_trading_enabled"] is False

        markdown = status_to_markdown(status)
        paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
            "## Retrospective tracking", maxsplit=1
        )[0]
        assert "- Missing artifacts: []" in paper_monitoring_section
        assert "- Missing artifacts:\n  -" not in paper_monitoring_section
        assert "- Queue write allowed: False" in paper_monitoring_section
        assert "- Broad daemon allowed: False" in paper_monitoring_section
        assert "- Automation allowed: False" in paper_monitoring_section
        assert "- Automated rerun allowed: False" in paper_monitoring_section
        assert "- Live trading enabled: False" in paper_monitoring_section


def test_status_defaults_missing_weekly_runtime_to_non_mutating_flags(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(
        paths["weekly_monitoring_report_path"],
        {
            "cadence": "weekly",
            "missing_artifacts": [],
            "next_observation_window": "next_weekly_paper_review",
        },
    )

    status = build_small_institutionalization_status(**paths)
    monitoring = status["paper_monitoring"]
    assert monitoring["weekly_report_status"] == "ready"
    assert monitoring["missing_artifacts"] == []
    assert monitoring["queue_write_allowed"] is False
    assert monitoring["broad_daemon_allowed"] is False
    assert monitoring["automation_allowed"] is False
    assert monitoring["automated_rerun_allowed"] is False
    assert monitoring["live_trading_enabled"] is False
    assert status["next_action"] == "run_small_institutional_self_diagnosis"

    markdown = status_to_markdown(status)
    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    assert "- Missing artifacts: []" in paper_monitoring_section
    assert "- Missing artifacts:\n  -" not in paper_monitoring_section
    assert "- Queue write allowed: False" in paper_monitoring_section
    assert "- Broad daemon allowed: False" in paper_monitoring_section
    assert "- Automation allowed: False" in paper_monitoring_section
    assert "- Automated rerun allowed: False" in paper_monitoring_section
    assert "- Live trading enabled: False" in paper_monitoring_section


def test_status_markdown_renders_paper_monitoring_runtime_context(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(
        paths["weekly_monitoring_report_path"],
        {
            "cadence": "weekly",
            "missing_artifacts": [],
            "runtime": {
                "safe": True,
                "would_run_count": 0,
                "recommendations": ["pause_broad_daemon", "allow_controlled_only_daemon"],
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
            "next_observation_window": "next_weekly_paper_review",
        },
    )

    status = build_small_institutionalization_status(**paths)
    markdown = status_to_markdown(status)

    monitoring = status["paper_monitoring"]
    assert monitoring["runtime_safe"] is True
    assert monitoring["would_run_count"] == 0
    assert monitoring["recommendations"] == ["pause_broad_daemon", "allow_controlled_only_daemon"]
    assert monitoring["queue_write_allowed"] is False
    assert monitoring["broad_daemon_allowed"] is False
    assert monitoring["automation_allowed"] is False
    assert monitoring["automated_rerun_allowed"] is False
    assert monitoring["live_trading_enabled"] is False

    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    assert "- Weekly would-run count: 0" in paper_monitoring_section
    assert "- Weekly runtime recommendations: ['pause_broad_daemon', 'allow_controlled_only_daemon']" in paper_monitoring_section
    assert "- Queue write allowed: False" in paper_monitoring_section
    assert "- Broad daemon allowed: False" in paper_monitoring_section
    assert "- Automation allowed: False" in paper_monitoring_section
    assert "- Automated rerun allowed: False" in paper_monitoring_section
    assert "- Live trading enabled: False" in paper_monitoring_section
    assert status["next_action"] == "run_small_institutional_self_diagnosis"


def test_status_markdown_renders_paper_monitoring_blocker_context(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(
        paths["weekly_monitoring_report_path"],
        {
            "cadence": "weekly",
            "missing_artifacts": [],
            "runtime": {
                "safe": True,
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
            "blockers": {
                "decision": "ready_for_portfolio_mvp",
                "next_action": "repair_simulated_portfolio_construction",
                "primary_issue": "drawdown_risk_too_high",
                "manual_approval_gate_status": "blocked_pending_manual_approval",
                "human_approval_present": False,
                "approval_required": True,
                "required_decision_axis": "holding_count=50",
            },
        },
    )

    status = build_small_institutionalization_status(**paths)
    markdown = status_to_markdown(status)

    monitoring = status["paper_monitoring"]
    assert monitoring["blockers"] == {
        "decision": "ready_for_portfolio_mvp",
        "next_action": "repair_simulated_portfolio_construction",
        "primary_issue": "drawdown_risk_too_high",
        "manual_approval_gate_status": "blocked_pending_manual_approval",
        "human_approval_present": False,
        "approval_required": True,
        "required_decision_axis": "holding_count=50",
    }
    assert monitoring["queue_write_allowed"] is False
    assert monitoring["broad_daemon_allowed"] is False
    assert monitoring["automation_allowed"] is False
    assert monitoring["automated_rerun_allowed"] is False
    assert monitoring["live_trading_enabled"] is False

    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    assert "- Weekly blocker context:" in paper_monitoring_section
    assert "  - Decision: ready_for_portfolio_mvp" in paper_monitoring_section
    assert "  - Next action: repair_simulated_portfolio_construction" in paper_monitoring_section
    assert "  - Primary issue: drawdown_risk_too_high" in paper_monitoring_section
    assert "  - Manual approval gate status: blocked_pending_manual_approval" in paper_monitoring_section
    assert "  - Human approval present: False" in paper_monitoring_section
    assert "  - Approval required: True" in paper_monitoring_section
    assert "  - Required decision axis: holding_count=50" in paper_monitoring_section
    assert "- Queue write allowed: False" in paper_monitoring_section
    assert "- Broad daemon allowed: False" in paper_monitoring_section
    assert "- Automation allowed: False" in paper_monitoring_section
    assert "- Automated rerun allowed: False" in paper_monitoring_section
    assert "- Live trading enabled: False" in paper_monitoring_section
    assert status["next_action"] == "run_small_institutional_self_diagnosis"


def test_status_markdown_aligns_weekly_operator_pending_labels_with_canonical_section(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(
        paths["weekly_monitoring_report_path"],
        {
            "cadence": "weekly",
            "missing_artifacts": [],
            "runtime": {
                "safe": True,
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
            "operator_pending_observation": {
                "observation_status": "operator_pending",
                "primary_issue": "drawdown_risk_too_high",
                "manual_approval_status": "blocked_pending_manual_approval",
                "benchmark_id": "CSI1000",
                "turnover_one_way_estimate": 0.791672,
                "estimated_round_trip_cost": 0.00475,
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)
    markdown = status_to_markdown(status)

    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    assert "- Weekly operator-pending observation:" in paper_monitoring_section
    assert "  - Observation status: operator_pending" in paper_monitoring_section
    assert "  - Weekly observation status:" not in paper_monitoring_section
    assert "  - Primary issue: drawdown_risk_too_high" in paper_monitoring_section
    assert "  - Manual approval status: blocked_pending_manual_approval" in paper_monitoring_section
    assert "  - Queue write allowed: False" in paper_monitoring_section
    assert "  - Broad daemon allowed: False" in paper_monitoring_section
    assert "  - Automation allowed: False" in paper_monitoring_section
    assert "  - Automated rerun allowed: False" in paper_monitoring_section
    assert "  - Live trading enabled: False" in paper_monitoring_section
    assert status["next_action"] == "run_small_institutional_self_diagnosis"


def test_status_marks_weekly_monitoring_report_missing_until_report_exists(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})

    status = build_small_institutionalization_status(**paths)

    assert status["paper_monitoring"]["weekly_report_status"] == "missing"
    assert status["next_action"] == "paper_monitoring_weekly_report"


def test_status_links_weekly_report_operator_pending_observation_metadata(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(
        paths["weekly_monitoring_report_path"],
        {
            "cadence": "weekly",
            "missing_artifacts": [],
            "runtime": {"safe": True},
            "operator_pending_observation": {
                "observation_status": "operator_pending",
                "primary_issue": "drawdown_risk_too_high",
                "manual_approval_status": "blocked_pending_manual_approval",
                "benchmark_id": "CSI1000",
                "turnover_one_way_estimate": 0.791672,
                "estimated_round_trip_cost": 0.00475,
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    linked = status["paper_monitoring"]["operator_pending_observation"]
    assert linked["observation_status"] == "operator_pending"
    assert linked["primary_issue"] == "drawdown_risk_too_high"
    assert linked["manual_approval_status"] == "blocked_pending_manual_approval"
    assert linked["benchmark_id"] == "CSI1000"
    assert linked["turnover_one_way_estimate"] == 0.791672
    assert linked["estimated_round_trip_cost"] == 0.00475
    assert linked["queue_write_allowed"] is False
    assert linked["broad_daemon_allowed"] is False
    assert linked["automation_allowed"] is False
    assert linked["automated_rerun_allowed"] is False
    assert linked["live_trading_enabled"] is False
    assert status["paper_monitoring"]["runtime_safe"] is True
    assert status["next_action"] == "run_small_institutional_self_diagnosis"
    markdown = status_to_markdown(status)
    assert "Weekly operator-pending observation" in markdown
    assert "Observation status: operator_pending" in markdown
    assert "Weekly observation status: operator_pending" not in markdown


def _operator_pending_observation_payload(*, primary_issue: str = "drawdown_risk_too_high") -> dict:
    return {
        "observation_status": "operator_pending",
        "benchmark": {"benchmark_id": "CSI1000", "benchmark_name": "中证1000", "tracking_mode": "metadata_only"},
        "turnover": {"turnover_one_way_estimate": 0.791672},
        "cost": {"estimated_round_trip_cost": 0.00475},
        "blocker": {
            "primary_issue": primary_issue,
            "manual_approval_status": "blocked_pending_manual_approval",
            "human_approval_present": False,
            "approval_required": True,
        },
        "runtime": {
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automated_rerun_allowed": False,
            "automation_allowed": False,
            "live_trading_enabled": False,
        },
    }


def _weekly_operator_pending_payload(*, primary_issue: str = "drawdown_risk_too_high") -> dict:
    return {
        "cadence": "weekly",
        "missing_artifacts": [],
        "runtime": {"safe": True},
        "operator_pending_observation": {
            "observation_status": "operator_pending",
            "primary_issue": primary_issue,
            "manual_approval_status": "blocked_pending_manual_approval",
            "benchmark_id": "CSI1000",
            "turnover_one_way_estimate": 0.791672,
            "estimated_round_trip_cost": 0.00475,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automation_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        },
    }


def test_status_marks_weekly_operator_pending_consistency_ok_when_canonical_matches(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(paths["weekly_monitoring_report_path"], _weekly_operator_pending_payload())
    _write_json(paths["operator_pending_observation_path"], _operator_pending_observation_payload())

    status = build_small_institutionalization_status(**paths)

    consistency = status["paper_monitoring"]["operator_pending_consistency"]
    assert consistency["consistency_status"] == "ok"
    assert consistency["mismatches"] == []
    assert consistency["queue_write_allowed"] is False
    assert consistency["broad_daemon_allowed"] is False
    assert consistency["automation_allowed"] is False
    assert consistency["automated_rerun_allowed"] is False
    assert consistency["live_trading_enabled"] is False
    markdown = status_to_markdown(status)
    assert "Weekly/canonical operator-pending consistency" in markdown
    assert "Consistency status: ok" in markdown


def test_status_markdown_aligns_weekly_canonical_consistency_labels_with_canonical_sections(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(paths["weekly_monitoring_report_path"], _weekly_operator_pending_payload())
    _write_json(paths["operator_pending_observation_path"], _operator_pending_observation_payload())

    status = build_small_institutionalization_status(**paths)
    markdown = status_to_markdown(status)
    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    consistency_section = paper_monitoring_section.split(
        "- Weekly/canonical operator-pending consistency:", maxsplit=1
    )[1].split("- Operator-pending consistency snapshot:", maxsplit=1)[0]

    assert "  - Consistency status: ok" in consistency_section
    assert "  - Mismatches: []" in consistency_section
    assert "  - Queue write allowed: False" in consistency_section
    assert "  - Broad daemon allowed: False" in consistency_section
    assert "  - Automation allowed: False" in consistency_section
    assert "  - Automated rerun allowed: False" in consistency_section
    assert "  - Live trading enabled: False" in consistency_section
    assert "Weekly consistency status" not in consistency_section
    assert "Weekly mismatches" not in consistency_section


def test_status_marks_weekly_operator_pending_consistency_mismatch_by_field(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(paths["weekly_monitoring_report_path"], _weekly_operator_pending_payload(primary_issue="stale_weekly_issue"))
    _write_json(paths["operator_pending_observation_path"], _operator_pending_observation_payload())

    status = build_small_institutionalization_status(**paths)

    consistency = status["paper_monitoring"]["operator_pending_consistency"]
    assert consistency["consistency_status"] == "mismatch"
    assert consistency["mismatches"] == ["primary_issue"]
    assert status["next_action"] == "run_small_institutional_self_diagnosis"
    assert status["runtime_safety"]["safe"] is True


def test_status_keeps_missing_weekly_operator_pending_consistency_non_blocking(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(paths["weekly_monitoring_report_path"], {"cadence": "weekly", "runtime": {"safe": True}})
    _write_json(paths["operator_pending_observation_path"], _operator_pending_observation_payload())

    status = build_small_institutionalization_status(**paths)

    assert "operator_pending_consistency" not in status["paper_monitoring"]
    assert status["next_action"] == "run_small_institutional_self_diagnosis"
    assert status["runtime_safety"]["safe"] is True


def test_status_omits_weekly_operator_pending_consistency_when_canonical_missing(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(paths["weekly_monitoring_report_path"], _weekly_operator_pending_payload())

    status = build_small_institutionalization_status(**paths)

    paper_monitoring = status["paper_monitoring"]
    assert "operator_pending_consistency" not in paper_monitoring
    assert paper_monitoring["operator_pending_observation"] == {
        "observation_status": "operator_pending",
        "primary_issue": "drawdown_risk_too_high",
        "manual_approval_status": "blocked_pending_manual_approval",
        "benchmark_id": "CSI1000",
        "turnover_one_way_estimate": 0.791672,
        "estimated_round_trip_cost": 0.00475,
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
    }
    assert status["operator_pending_observation"] == {"observation_status": "missing"}
    assert status["next_action"] == "run_small_institutional_self_diagnosis"
    assert status["runtime_safety"]["safe"] is True
    assert paper_monitoring["queue_write_allowed"] is False
    assert paper_monitoring["broad_daemon_allowed"] is False
    assert paper_monitoring["automation_allowed"] is False
    assert paper_monitoring["automated_rerun_allowed"] is False
    assert paper_monitoring["live_trading_enabled"] is False

    markdown = status_to_markdown(status)
    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    canonical_section = markdown.split("## Operator pending observation", maxsplit=1)[1].split("\n## ", maxsplit=1)[0]

    assert "- Weekly operator-pending observation:" in paper_monitoring_section
    assert "  - Observation status: operator_pending" in paper_monitoring_section
    assert "  - Primary issue: drawdown_risk_too_high" in paper_monitoring_section
    assert "Weekly/canonical operator-pending consistency" not in paper_monitoring_section
    assert "- Observation status: missing" in canonical_section
    assert "Primary issue" not in canonical_section
    assert "Manual approval status" not in canonical_section
    assert "Benchmark ID" not in canonical_section
    assert "Queue write allowed" not in canonical_section


def test_status_omits_operator_pending_consistency_and_snapshot_when_canonical_missing_and_snapshot_absent(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(paths["weekly_monitoring_report_path"], _weekly_operator_pending_payload())

    status = build_small_institutionalization_status(**paths)

    paper_monitoring = status["paper_monitoring"]
    assert paper_monitoring["operator_pending_observation"]["observation_status"] == "operator_pending"
    assert "operator_pending_consistency" not in paper_monitoring
    assert "operator_pending_consistency_snapshot" not in paper_monitoring
    assert status["operator_pending_observation"] == {"observation_status": "missing"}
    assert status["next_action"] == "run_small_institutional_self_diagnosis"
    assert status["runtime_safety"]["safe"] is True
    assert paper_monitoring["queue_write_allowed"] is False
    assert paper_monitoring["broad_daemon_allowed"] is False
    assert paper_monitoring["automation_allowed"] is False
    assert paper_monitoring["automated_rerun_allowed"] is False
    assert paper_monitoring["live_trading_enabled"] is False

    markdown = status_to_markdown(status)
    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    canonical_section = markdown.split("## Operator pending observation", maxsplit=1)[1].split("\n## ", maxsplit=1)[0]

    assert "- Weekly operator-pending observation:" in paper_monitoring_section
    assert "  - Observation status: operator_pending" in paper_monitoring_section
    assert "  - Primary issue: drawdown_risk_too_high" in paper_monitoring_section
    assert "Weekly/canonical operator-pending consistency" not in paper_monitoring_section
    assert "Operator-pending consistency snapshot" not in paper_monitoring_section
    assert canonical_section == "\n- Observation status: missing\n"


def test_status_waits_when_retrospective_tracking_has_insufficient_forward_window(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(paths["weekly_monitoring_report_path"], {"cadence": "weekly"})
    _write_json(paths["retrospective_tracking_path"], {"tracking_status": "insufficient_forward_window"})

    status = build_small_institutionalization_status(**paths)

    assert status["next_action"] == "run_small_institutional_self_diagnosis"
    assert status["retrospective_tracking"]["tracking_status"] == "insufficient_forward_window"


def test_status_advances_to_constraint_hardening_when_retrospective_tracking_ok(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(paths["weekly_monitoring_report_path"], {"cadence": "weekly"})
    _write_json(paths["retrospective_tracking_path"], {"tracking_status": "ok", "portfolio_return": {"portfolio_forward_return": 0.0123}})

    status = build_small_institutionalization_status(**paths)

    assert status["next_action"] == "run_small_institutional_self_diagnosis"
    assert status["retrospective_tracking"]["portfolio_forward_return"] == 0.0123


def test_status_waits_when_constraint_hardening_waits_for_inputs(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(paths["weekly_monitoring_report_path"], {"cadence": "weekly"})
    _write_json(paths["retrospective_tracking_path"], {"tracking_status": "ok", "portfolio_return": {"portfolio_forward_return": 0.0123}})
    _write_json(paths["constraint_hardening_path"], {"constraint_status": "wait", "warnings": ["retrospective_tracking_not_ready"]})

    status = build_small_institutionalization_status(**paths)

    assert status["next_action"] == "run_small_institutional_self_diagnosis"
    assert status["portfolio_constraint_hardening"]["constraint_status"] == "wait"


def test_status_requests_constraint_repair_when_constraint_hardening_fails(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(paths["weekly_monitoring_report_path"], {"cadence": "weekly"})
    _write_json(paths["retrospective_tracking_path"], {"tracking_status": "ok", "portfolio_return": {"portfolio_forward_return": 0.0123}})
    _write_json(paths["constraint_hardening_path"], {"constraint_status": "fail", "violations": ["single_name_weight_cap_breached"]})

    status = build_small_institutionalization_status(**paths)

    assert status["next_action"] == "run_small_institutional_self_diagnosis"
    assert status["portfolio_constraint_hardening"]["violations"] == ["single_name_weight_cap_breached"]


def test_status_advances_to_live_promotion_review_when_constraint_hardening_passes(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(paths["weekly_monitoring_report_path"], {"cadence": "weekly"})
    _write_json(paths["retrospective_tracking_path"], {"tracking_status": "ok", "portfolio_return": {"portfolio_forward_return": 0.0123}})
    _write_json(paths["constraint_hardening_path"], {"constraint_status": "pass", "violations": [], "warnings": []})

    status = build_small_institutionalization_status(**paths)

    assert status["next_action"] == "run_small_institutional_self_diagnosis"
    assert status["portfolio_constraint_hardening"]["constraint_status"] == "pass"


def test_status_requests_promotion_readiness_repair_when_readiness_blocks(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["promotion_readiness_path"], {"readiness_status": "blocked", "blockers": ["runtime_not_controlled_safe"]})

    status = build_small_institutionalization_status(**paths)

    assert status["next_action"] == "run_small_institutional_self_diagnosis"
    assert status["paper_live_promotion_readiness"]["blockers"] == ["runtime_not_controlled_safe"]


def test_status_continues_paper_observation_when_promotion_readiness_waits(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["promotion_readiness_path"], {"readiness_status": "wait", "warnings": ["insufficient_paper_observations"]})

    status = build_small_institutionalization_status(**paths)

    assert status["next_action"] == "run_small_institutional_self_diagnosis"
    assert status["paper_live_promotion_readiness"]["readiness_status"] == "wait"


def test_status_requires_self_diagnosis_after_live_stop_marker_is_ready(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)

    status = build_small_institutionalization_status(**paths)

    assert status["next_action"] == "run_small_institutional_self_diagnosis"
    assert status["paper_live_promotion_readiness"]["live_trading_enabled"] is False
    assert status["small_institutional_simulation"]["diagnosis_status"] == "missing"


def test_status_treats_promotion_readiness_as_reporting_only_when_upstream_enables_live(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(
        paths["promotion_readiness_path"],
        {
            "readiness_status": "ready_for_manual_approval",
            "blockers": [],
            "warnings": [],
            "manual_approval_required": True,
            "live_trading_enabled": True,
        },
    )

    status = build_small_institutionalization_status(**paths)

    readiness = status["paper_live_promotion_readiness"]
    assert readiness["readiness_status"] == "ready_for_manual_approval"
    assert readiness["manual_approval_required"] is True
    assert readiness["live_trading_enabled"] is False
    assert status["next_action"] == "run_small_institutional_self_diagnosis"

    markdown = status_to_markdown(status)
    readiness_section = markdown.split("## Paper/live promotion readiness", maxsplit=1)[1].split(
        "\n## ", maxsplit=1
    )[0]
    assert "- Manual approval required: True" in readiness_section
    assert "- Live trading enabled: False" in readiness_section
    assert "- Live trading enabled: True" not in readiness_section


def test_status_follows_self_diagnosis_data_gap_next_action(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "data_coverage_gap", "next_action": "extend_backtest_dataset", "automation_allowed": False})

    status = build_small_institutionalization_status(**paths)

    assert status["next_action"] == "extend_backtest_dataset"
    assert status["small_institutional_simulation"]["primary_issue"] == "data_coverage_gap"


def test_status_follows_self_diagnosis_drawdown_repair_next_action(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "next_action": "repair_simulated_portfolio_construction", "automation_allowed": False})

    status = build_small_institutionalization_status(**paths)

    assert status["next_action"] == "repair_simulated_portfolio_construction"


def test_status_surfaces_blocked_simulated_portfolio_construction_repair(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "recommended_candidate": None, "automation_allowed": False, "best_available_max_drawdown": -0.478256, "drawdown_gap_to_limit": 0.128256})

    status = build_small_institutionalization_status(**paths)

    assert status["simulated_portfolio_construction_repair"] == {
        "repair_status": "blocked_no_drawdown_safe_candidate",
        "candidate_count": 0,
        "recommended_candidate": None,
        "automation_allowed": False,
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
        "best_available_max_drawdown": -0.478256,
        "drawdown_gap_to_limit": 0.128256,
    }
    assert status["next_action"] == "repair_simulated_portfolio_construction"


def test_status_requests_repaired_bounded_matrix_when_repair_candidate_exists(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    candidate = {"combo_id": "safe", "holding_count": 50, "max_drawdown": -0.30, "sharpe": 0.8}
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(
        paths["simulated_portfolio_construction_repair_path"],
        {
            "repair_status": "candidate_found",
            "candidate_count": 1,
            "recommended_candidate": candidate,
            "automation_allowed": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        },
    )

    status = build_small_institutionalization_status(**paths)

    repair = status["simulated_portfolio_construction_repair"]
    assert repair["recommended_candidate"] == candidate
    assert repair["automation_allowed"] is False
    assert repair["queue_write_allowed"] is False
    assert repair["broad_daemon_allowed"] is False
    assert repair["automated_rerun_allowed"] is False
    assert repair["live_trading_enabled"] is False
    assert status["next_action"] == "rerun_bounded_matrix_with_repaired_construction"


def test_status_allows_bounded_large_scale_simulation_when_self_diagnosis_ready(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "ready", "primary_issue": "ready_for_broader_simulation", "next_action": "run_bounded_large_scale_simulation", "automation_allowed": True})

    status = build_small_institutionalization_status(**paths)

    assert status["next_action"] == "run_bounded_large_scale_simulation"
    assert status["small_institutional_simulation"]["automation_allowed"] is True


def test_status_surfaces_operator_approval_summary_without_advancing_next_action(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    _write_json(
        paths["operator_approval_summary_path"],
        {
            "summary_status": "blocked_pending_manual_approval",
            "approval_required": True,
            "required_decision_axis": "holding_count=50",
            "primary_blocker": "drawdown_risk_too_high",
            "best_available_max_drawdown": -0.478256,
            "drawdown_gap_to_limit": 0.128256,
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    summary = status["operator_approval_summary"]
    assert summary["summary_status"] == "blocked_pending_manual_approval"
    assert summary["required_decision_axis"] == "holding_count=50"
    assert summary["queue_write_allowed"] is False
    assert summary["broad_daemon_allowed"] is False
    assert summary["automation_allowed"] is False
    assert summary["automated_rerun_allowed"] is False
    assert summary["live_trading_enabled"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"


def test_status_surfaces_approval_artifact_consistency_without_advancing_next_action(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    _write_json(
        paths["manual_approval_gate_path"],
        {
            "gate_status": "blocked_pending_manual_approval",
            "human_approval_present": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automation_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        },
    )
    _write_json(
        paths["approval_artifact_consistency_path"],
        {
            "consistency_status": "ok",
            "matched_fields": {"primary_blocker": "drawdown_risk_too_high", "decision_axis": "holding_count=50"},
            "safety_flags": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
            "inconsistencies": [],
            "staleness_warnings": [],
        },
    )

    status = build_small_institutionalization_status(**paths)

    consistency = status["approval_artifact_consistency"]
    assert consistency["consistency_status"] == "ok"
    assert consistency["primary_blocker"] == "drawdown_risk_too_high"
    assert consistency["decision_axis"] == "holding_count=50"
    assert consistency["queue_write_allowed"] is False
    assert consistency["broad_daemon_allowed"] is False
    assert consistency["automation_allowed"] is False
    assert consistency["automated_rerun_allowed"] is False
    assert consistency["live_trading_enabled"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"
    assert "Approval artifact consistency" in status_to_markdown(status)


def test_status_surfaces_operator_decision_intake_validation_without_advancing_next_action(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    _write_json(
        paths["operator_decision_intake_validation_path"],
        {
            "intake_status": "valid",
            "decision_type": "defer",
            "scope": "small institutional manual review",
            "reason": "No operator approval yet.",
            "validation_errors": [],
            "non_mutating": True,
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    intake = status["operator_decision_intake_validation"]
    assert intake["intake_status"] == "valid"
    assert intake["decision_type"] == "defer"
    assert intake["non_mutating"] is True
    assert intake["validation_errors"] == []
    assert intake["queue_write_allowed"] is False
    assert intake["broad_daemon_allowed"] is False
    assert intake["automation_allowed"] is False
    assert intake["automated_rerun_allowed"] is False
    assert intake["live_trading_enabled"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"
    markdown = status_to_markdown(status)
    assert "Operator decision intake validation" in markdown
    assert "Intake status: valid" in markdown


def test_status_missing_operator_decision_intake_validation_keeps_disabled_runtime_flags(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})

    status = build_small_institutionalization_status(**paths)

    intake = status["operator_decision_intake_validation"]
    assert intake["intake_status"] == "missing"
    assert intake["decision_type"] is None
    assert intake["scope"] is None
    assert intake["reason"] is None
    assert intake["validation_errors"] == []
    assert intake["non_mutating"] is True
    assert intake["queue_write_allowed"] is False
    assert intake["broad_daemon_allowed"] is False
    assert intake["automation_allowed"] is False
    assert intake["automated_rerun_allowed"] is False
    assert intake["live_trading_enabled"] is False

    wait_state = status["operator_decision_wait_state"]
    assert wait_state["queue_write_allowed"] is False
    assert wait_state["broad_daemon_allowed"] is False
    assert wait_state["automation_allowed"] is False
    assert wait_state["automated_rerun_allowed"] is False
    assert wait_state["live_trading_enabled"] is False


def test_status_valid_operator_decision_intake_without_human_approval_stays_non_executable(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    _write_json(
        paths["manual_approval_gate_path"],
        {
            "gate_status": "blocked_pending_manual_approval",
            "human_approval_present": False,
            "risk_relaxation_allowed": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automation_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        },
    )
    _write_json(
        paths["operator_approval_summary_path"],
        {
            "summary_status": "blocked_pending_manual_approval",
            "approval_required": True,
            "human_approval_present": False,
            "required_decision_axis": "holding_count=50",
            "primary_blocker": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )
    _write_json(
        paths["operator_decision_intake_validation_path"],
        {
            "intake_status": "valid",
            "decision_type": "approve_risk_relaxation",
            "scope": "small_institutional_value_sleeve_mvp",
            "reason": "syntactically valid intake but still lacks human approval evidence",
            "validation_errors": [],
            "non_mutating": True,
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )
    _write_json(
        paths["operator_decision_handoff_path"],
        {
            "handoff_status": "awaiting_operator_decision",
            "intake_status": "valid",
            "primary_blocker": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "decision_axis": "holding_count=50",
            "validation_errors": [],
            "non_mutating": True,
            "execution_allowed": True,
            "separate_execution_plan_required": True,
            "queue_write_allowed": True,
            "broad_daemon_allowed": True,
            "automation_allowed": True,
            "automated_rerun_allowed": True,
            "live_trading_enabled": True,
            "safety": {
                "queue_write_allowed": True,
                "broad_daemon_allowed": True,
                "automation_allowed": True,
                "automated_rerun_allowed": True,
                "live_trading_enabled": True,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    wait_state = status["operator_decision_wait_state"]
    assert wait_state["wait_state_status"] == "awaiting_operator_decision"
    assert wait_state["human_approval_present"] is False
    assert wait_state["approval_required"] is True
    assert wait_state["intake_status"] == "valid"
    assert wait_state["validation_errors"] == []
    assert wait_state["execution_allowed"] is False
    assert wait_state["separate_execution_plan_required"] is False
    assert wait_state["queue_write_allowed"] is False
    assert wait_state["broad_daemon_allowed"] is False
    assert wait_state["automation_allowed"] is False
    assert wait_state["automated_rerun_allowed"] is False
    assert wait_state["live_trading_enabled"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"



def test_status_markdown_valid_operator_decision_intake_without_human_approval_shows_disabled_runtime_flags(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    _write_json(
        paths["manual_approval_gate_path"],
        {
            "gate_status": "blocked_pending_manual_approval",
            "human_approval_present": False,
            "risk_relaxation_allowed": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automation_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        },
    )
    _write_json(
        paths["operator_approval_summary_path"],
        {
            "summary_status": "blocked_pending_manual_approval",
            "approval_required": True,
            "human_approval_present": False,
            "required_decision_axis": "holding_count=50",
            "primary_blocker": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )
    _write_json(
        paths["operator_decision_intake_validation_path"],
        {
            "intake_status": "valid",
            "decision_type": "approve_risk_relaxation",
            "scope": "small_institutional_value_sleeve_mvp",
            "reason": "syntactically valid intake but still lacks human approval evidence",
            "validation_errors": [],
            "non_mutating": True,
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )
    _write_json(
        paths["operator_decision_handoff_path"],
        {
            "handoff_status": "awaiting_operator_decision",
            "intake_status": "valid",
            "primary_blocker": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "decision_axis": "holding_count=50",
            "validation_errors": [],
            "non_mutating": True,
            "execution_allowed": True,
            "separate_execution_plan_required": True,
            "queue_write_allowed": True,
            "broad_daemon_allowed": True,
            "automation_allowed": True,
            "automated_rerun_allowed": True,
            "live_trading_enabled": True,
            "safety": {
                "queue_write_allowed": True,
                "broad_daemon_allowed": True,
                "automation_allowed": True,
                "automated_rerun_allowed": True,
                "live_trading_enabled": True,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)
    markdown = status_to_markdown(status)
    wait_state_section = markdown.split("## Operator decision wait state", 1)[1].split("\n## ", 1)[0]

    assert "- Human approval present: False" in wait_state_section
    assert "- Approval required: True" in wait_state_section
    assert "- Intake status: valid" in wait_state_section
    assert "- Execution allowed: False" in wait_state_section
    assert "- Separate execution plan required: False" in wait_state_section
    assert "- Queue write allowed: False" in wait_state_section
    assert "- Broad daemon allowed: False" in wait_state_section
    assert "- Automation allowed: False" in wait_state_section
    assert "- Automated rerun allowed: False" in wait_state_section
    assert "- Live trading enabled: False" in wait_state_section
    assert "- Execution allowed: True" not in wait_state_section
    assert "- Separate execution plan required: True" not in wait_state_section
    assert "- Queue write allowed: True" not in wait_state_section
    assert "- Broad daemon allowed: True" not in wait_state_section
    assert "- Automation allowed: True" not in wait_state_section
    assert "- Automated rerun allowed: True" not in wait_state_section
    assert "- Live trading enabled: True" not in wait_state_section



def test_status_invalid_operator_decision_intake_forces_wait_state_non_executable(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    _write_json(
        paths["operator_decision_intake_validation_path"],
        {
            "intake_status": "invalid",
            "decision_type": "approve_risk_relaxation",
            "scope": "small_institutional_value_sleeve_mvp",
            "reason": "missing required human approval signature",
            "validation_errors": ["missing_human_approval_signature"],
            "non_mutating": True,
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )
    _write_json(
        paths["operator_decision_handoff_path"],
        {
            "handoff_status": "awaiting_operator_decision",
            "intake_status": "invalid",
            "primary_blocker": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "decision_axis": "holding_count=50",
            "validation_errors": ["missing_human_approval_signature"],
            "non_mutating": True,
            "execution_allowed": True,
            "separate_execution_plan_required": True,
            "queue_write_allowed": True,
            "broad_daemon_allowed": True,
            "automation_allowed": True,
            "automated_rerun_allowed": True,
            "live_trading_enabled": True,
            "safety": {
                "queue_write_allowed": True,
                "broad_daemon_allowed": True,
                "automation_allowed": True,
                "automated_rerun_allowed": True,
                "live_trading_enabled": True,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    wait_state = status["operator_decision_wait_state"]
    assert wait_state["wait_state_status"] == "not_waiting_on_operator_decision"
    assert wait_state["intake_status"] == "invalid"
    assert wait_state["handoff_status"] == "awaiting_operator_decision"
    assert wait_state["validation_errors"] == ["missing_human_approval_signature"]
    assert wait_state["execution_allowed"] is False
    assert wait_state["separate_execution_plan_required"] is False
    assert wait_state["queue_write_allowed"] is False
    assert wait_state["broad_daemon_allowed"] is False
    assert wait_state["automation_allowed"] is False
    assert wait_state["automated_rerun_allowed"] is False
    assert wait_state["live_trading_enabled"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"


def test_status_markdown_invalid_operator_decision_wait_state_shows_disabled_runtime_flags(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    _write_json(
        paths["operator_decision_intake_validation_path"],
        {
            "intake_status": "invalid",
            "decision_type": "approve_risk_relaxation",
            "scope": "small_institutional_value_sleeve_mvp",
            "reason": "missing required human approval signature",
            "validation_errors": ["missing_human_approval_signature"],
            "non_mutating": True,
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )
    _write_json(
        paths["operator_decision_handoff_path"],
        {
            "handoff_status": "awaiting_operator_decision",
            "intake_status": "invalid",
            "primary_blocker": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "decision_axis": "holding_count=50",
            "validation_errors": ["missing_human_approval_signature"],
            "non_mutating": True,
            "execution_allowed": True,
            "separate_execution_plan_required": True,
            "queue_write_allowed": True,
            "broad_daemon_allowed": True,
            "automation_allowed": True,
            "automated_rerun_allowed": True,
            "live_trading_enabled": True,
            "safety": {
                "queue_write_allowed": True,
                "broad_daemon_allowed": True,
                "automation_allowed": True,
                "automated_rerun_allowed": True,
                "live_trading_enabled": True,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)
    markdown = status_to_markdown(status)
    wait_state_section = markdown.split("## Operator decision wait state", 1)[1].split("\n## ", 1)[0]

    assert "- Intake status: invalid" in wait_state_section
    assert "- Validation errors: ['missing_human_approval_signature']" in wait_state_section
    assert "- Execution allowed: False" in wait_state_section
    assert "- Separate execution plan required: False" in wait_state_section
    assert "- Queue write allowed: False" in wait_state_section
    assert "- Broad daemon allowed: False" in wait_state_section
    assert "- Automation allowed: False" in wait_state_section
    assert "- Automated rerun allowed: False" in wait_state_section
    assert "- Live trading enabled: False" in wait_state_section
    assert "- Execution allowed: True" not in wait_state_section
    assert "- Separate execution plan required: True" not in wait_state_section
    assert "- Queue write allowed: True" not in wait_state_section
    assert "- Broad daemon allowed: True" not in wait_state_section
    assert "- Automation allowed: True" not in wait_state_section
    assert "- Automated rerun allowed: True" not in wait_state_section
    assert "- Live trading enabled: True" not in wait_state_section


def test_status_surfaces_operator_decision_handoff_without_advancing_next_action(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    _write_json(
        paths["operator_decision_handoff_path"],
        {
            "handoff_status": "awaiting_operator_decision",
            "intake_status": "missing",
            "decision_type": None,
            "primary_blocker": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "decision_axis": "holding_count=50",
            "non_mutating": True,
            "execution_allowed": False,
            "separate_execution_plan_required": False,
            "validation_errors": [],
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    handoff = status["operator_decision_handoff"]
    assert handoff["handoff_status"] == "awaiting_operator_decision"
    assert handoff["decision_axis"] == "holding_count=50"
    assert handoff["primary_blocker"] == "drawdown_risk_too_high"
    assert handoff["execution_allowed"] is False
    assert handoff["separate_execution_plan_required"] is False
    assert handoff["queue_write_allowed"] is False
    assert handoff["broad_daemon_allowed"] is False
    assert handoff["automation_allowed"] is False
    assert handoff["automated_rerun_allowed"] is False
    assert handoff["live_trading_enabled"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"
    markdown = status_to_markdown(status)
    assert "Operator decision handoff" in markdown
    assert "Handoff status: awaiting_operator_decision" in markdown


def test_status_surfaces_operator_decision_wait_state_without_advancing_next_action(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    _write_json(
        paths["manual_approval_gate_path"],
        {
            "gate_status": "blocked_pending_manual_approval",
            "human_approval_present": False,
            "risk_relaxation_allowed": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automation_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        },
    )
    _write_json(
        paths["operator_approval_summary_path"],
        {
            "summary_status": "blocked_pending_manual_approval",
            "approval_required": True,
            "human_approval_present": False,
            "required_decision_axis": "holding_count=50",
            "primary_blocker": "drawdown_risk_too_high",
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )
    _write_json(
        paths["operator_decision_intake_validation_path"],
        {"intake_status": "missing", "validation_errors": [], "non_mutating": True},
    )
    _write_json(
        paths["operator_decision_handoff_path"],
        {
            "handoff_status": "awaiting_operator_decision",
            "intake_status": "missing",
            "primary_blocker": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "decision_axis": "holding_count=50",
            "non_mutating": True,
            "execution_allowed": False,
            "separate_execution_plan_required": False,
            "validation_errors": [],
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    wait_state = status["operator_decision_wait_state"]
    assert wait_state["wait_state_status"] == "awaiting_operator_decision"
    assert wait_state["primary_blocker"] == "drawdown_risk_too_high"
    assert wait_state["decision_axis"] == "holding_count=50"
    assert wait_state["execution_allowed"] is False
    assert wait_state["separate_execution_plan_required"] is False
    assert wait_state["queue_write_allowed"] is False
    assert wait_state["broad_daemon_allowed"] is False
    assert wait_state["automation_allowed"] is False
    assert wait_state["automated_rerun_allowed"] is False
    assert wait_state["live_trading_enabled"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"
    markdown = status_to_markdown(status)
    wait_state_section = markdown.split("## Operator decision wait state", maxsplit=1)[1].split(
        "## Operator pending observation", maxsplit=1
    )[0]
    assert "- Wait-state status: awaiting_operator_decision" in wait_state_section
    assert "- Primary blocker: drawdown_risk_too_high" in wait_state_section
    assert "- Decision axis: holding_count=50" in wait_state_section
    assert "- Execution allowed: False" in wait_state_section
    assert "- Separate execution plan required: False" in wait_state_section
    assert "- Queue write allowed: False" in wait_state_section
    assert "- Broad daemon allowed: False" in wait_state_section
    assert "- Automation allowed: False" in wait_state_section
    assert "- Automated rerun allowed: False" in wait_state_section
    assert "- Live trading enabled: False" in wait_state_section
    assert "- Execution allowed: True" not in wait_state_section
    assert "- Queue write allowed: True" not in wait_state_section
    assert "- Broad daemon allowed: True" not in wait_state_section
    assert "- Automation allowed: True" not in wait_state_section
    assert "- Automated rerun allowed: True" not in wait_state_section
    assert "- Live trading enabled: True" not in wait_state_section



def test_status_surfaces_operator_pending_observation_without_advancing_next_action(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    _write_json(
        paths["operator_pending_observation_path"],
        {
            "observation_status": "operator_pending",
            "benchmark": {"benchmark_id": "CSI1000", "benchmark_name": "中证1000", "tracking_mode": "metadata_only"},
            "turnover": {"turnover_one_way_estimate": 0.791672},
            "cost": {"estimated_round_trip_cost": 0.00475},
            "blocker": {
                "primary_issue": "drawdown_risk_too_high",
                "manual_approval_status": "blocked_pending_manual_approval",
                "human_approval_present": False,
                "approval_required": True,
            },
            "runtime": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automated_rerun_allowed": False,
                "automation_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    observation = status["operator_pending_observation"]
    assert observation["observation_status"] == "operator_pending"
    assert observation["primary_issue"] == "drawdown_risk_too_high"
    assert observation["manual_approval_status"] == "blocked_pending_manual_approval"
    assert observation["benchmark_id"] == "CSI1000"
    assert observation["turnover_one_way_estimate"] == 0.791672
    assert observation["estimated_round_trip_cost"] == 0.00475
    assert observation["queue_write_allowed"] is False
    assert observation["broad_daemon_allowed"] is False
    assert observation["automated_rerun_allowed"] is False
    assert observation["automation_allowed"] is False
    assert observation["live_trading_enabled"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"
    markdown = status_to_markdown(status)
    assert "Operator pending observation" in markdown
    assert "Observation status: operator_pending" in markdown


def test_status_links_operator_pending_consistency_snapshot_without_advancing_next_action(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    _write_json(
        paths["operator_pending_consistency_snapshot_path"],
        {
            "snapshot_status": "ready",
            "consistency_status": "ok",
            "mismatches": [],
            "benchmark_id": "CSI1000",
            "turnover_one_way_estimate": 0.791672,
            "estimated_round_trip_cost": 0.00475,
            "runtime": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    snapshot = status["paper_monitoring"]["operator_pending_consistency_snapshot"]
    assert snapshot["snapshot_status"] == "ready"
    assert snapshot["consistency_status"] == "ok"
    assert snapshot["mismatches"] == []
    assert snapshot["benchmark_id"] == "CSI1000"
    assert snapshot["turnover_one_way_estimate"] == 0.791672
    assert snapshot["estimated_round_trip_cost"] == 0.00475
    assert snapshot["queue_write_allowed"] is False
    assert snapshot["broad_daemon_allowed"] is False
    assert snapshot["automation_allowed"] is False
    assert snapshot["automated_rerun_allowed"] is False
    assert snapshot["live_trading_enabled"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"
    markdown = status_to_markdown(status)
    assert "Operator-pending consistency snapshot" in markdown
    assert "Snapshot status: ready" in markdown


def test_status_markdown_aligns_operator_pending_consistency_snapshot_labels_with_canonical_sections(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    _write_json(
        paths["operator_pending_consistency_snapshot_path"],
        {
            "snapshot_status": "ready",
            "source_status_generated_at_utc": "2026-06-05T08:58:55.301305+00:00",
            "consistency_status": "ok",
            "mismatches": [],
            "benchmark_id": "CSI1000",
            "turnover_one_way_estimate": 0.791672,
            "estimated_round_trip_cost": 0.00475,
            "runtime": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)
    markdown = status_to_markdown(status)
    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    snapshot_section = paper_monitoring_section.split("- Operator-pending consistency snapshot:", maxsplit=1)[1]

    assert "  - Snapshot status: ready" in snapshot_section
    assert "  - Snapshot freshness status:" in snapshot_section
    assert "  - Source status generated: 2026-06-05T08:58:55.301305+00:00" in snapshot_section
    assert "  - Latest status generated:" in snapshot_section
    assert "  - Consistency status: ok" in snapshot_section
    assert "  - Mismatches: []" in snapshot_section
    assert "  - Queue write allowed: False" in snapshot_section
    assert "  - Broad daemon allowed: False" in snapshot_section
    assert "  - Automation allowed: False" in snapshot_section
    assert "  - Automated rerun allowed: False" in snapshot_section
    assert "  - Live trading enabled: False" in snapshot_section
    assert "Snapshot consistency status" not in snapshot_section
    assert "Snapshot mismatches" not in snapshot_section


def test_status_markdown_renders_top_level_operator_pending_consistency_snapshot_surface():
    safe_snapshot = {
        "snapshot_status": "ready",
        "snapshot_freshness_status": "fresh",
        "source_status_generated_at_utc": "2026-06-05T08:58:55.301305+00:00",
        "latest_status_generated_at_utc": "2026-06-05T08:58:55.301305+00:00",
        "consistency_status": "ok",
        "mismatches": [],
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
    }
    status = {
        "paper_monitoring": {
            "weekly_report_status": "ready",
            "operator_pending_consistency_snapshot": {
                **safe_snapshot,
                "queue_write_allowed": True,
                "broad_daemon_allowed": True,
                "automation_allowed": True,
                "automated_rerun_allowed": True,
                "live_trading_enabled": True,
            },
        },
        "operator_pending_consistency_snapshot": safe_snapshot,
    }

    markdown = status_to_markdown(status)
    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    snapshot_section = paper_monitoring_section.split("- Operator-pending consistency snapshot:", maxsplit=1)[1]

    assert "  - Snapshot freshness status: fresh" in snapshot_section
    assert "  - Queue write allowed: False" in snapshot_section
    assert "  - Broad daemon allowed: False" in snapshot_section
    assert "  - Automation allowed: False" in snapshot_section
    assert "  - Automated rerun allowed: False" in snapshot_section
    assert "  - Live trading enabled: False" in snapshot_section
    assert "  - Queue write allowed: True" not in snapshot_section
    assert "  - Broad daemon allowed: True" not in snapshot_section
    assert "  - Automation allowed: True" not in snapshot_section
    assert "  - Automated rerun allowed: True" not in snapshot_section
    assert "  - Live trading enabled: True" not in snapshot_section


def test_status_markdown_falls_back_to_nested_operator_pending_snapshot_reporting_only():
    status = {
        "paper_monitoring": {
            "weekly_report_status": "ready",
            "operator_pending_consistency_snapshot": {
                "snapshot_status": "ready",
                "snapshot_freshness_status": "fresh",
                "source_status_generated_at_utc": "2026-06-05T08:58:55.301305+00:00",
                "latest_status_generated_at_utc": "2026-06-05T08:58:55.301305+00:00",
                "consistency_status": "ok",
                "mismatches": [],
                "benchmark_id": "CSI1000",
                "turnover_one_way_estimate": 0.791672,
                "estimated_round_trip_cost": 0.00475,
                "queue_write_allowed": True,
                "broad_daemon_allowed": True,
                "automation_allowed": True,
                "automated_rerun_allowed": True,
                "live_trading_enabled": True,
            },
        },
    }

    markdown = status_to_markdown(status)
    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    snapshot_section = paper_monitoring_section.split("- Operator-pending consistency snapshot:", maxsplit=1)[1]

    assert "  - Snapshot status: ready" in snapshot_section
    assert "  - Snapshot freshness status: fresh" in snapshot_section
    assert "  - Source status generated: 2026-06-05T08:58:55.301305+00:00" in snapshot_section
    assert "  - Latest status generated: 2026-06-05T08:58:55.301305+00:00" in snapshot_section
    assert "  - Consistency status: ok" in snapshot_section
    assert "  - Mismatches: []" in snapshot_section
    assert "  - Queue write allowed: False" in snapshot_section
    assert "  - Broad daemon allowed: False" in snapshot_section
    assert "  - Automation allowed: False" in snapshot_section
    assert "  - Automated rerun allowed: False" in snapshot_section
    assert "  - Live trading enabled: False" in snapshot_section
    assert "  - Queue write allowed: True" not in snapshot_section
    assert "  - Broad daemon allowed: True" not in snapshot_section
    assert "  - Automation allowed: True" not in snapshot_section
    assert "  - Automated rerun allowed: True" not in snapshot_section
    assert "  - Live trading enabled: True" not in snapshot_section
    assert "execution allowed" not in snapshot_section.lower()


def test_status_markdown_operator_pending_snapshot_top_level_and_nested_render_identically_reporting_only():
    mutating_snapshot = {
        "snapshot_status": "ready",
        "snapshot_freshness_status": "fresh",
        "source_status_generated_at_utc": "2026-06-05T08:58:55.301305+00:00",
        "latest_status_generated_at_utc": "2026-06-05T08:58:55.301305+00:00",
        "consistency_status": "ok",
        "mismatches": [],
        "benchmark_id": "CSI1000",
        "turnover_one_way_estimate": 0.791672,
        "estimated_round_trip_cost": 0.00475,
        "queue_write_allowed": True,
        "broad_daemon_allowed": True,
        "automation_allowed": True,
        "automated_rerun_allowed": True,
        "live_trading_enabled": True,
    }
    top_level_status = {
        "paper_monitoring": {
            "weekly_report_status": "ready",
            "operator_pending_consistency_snapshot": {**mutating_snapshot, "snapshot_status": "nested_ignored"},
        },
        "operator_pending_consistency_snapshot": mutating_snapshot,
    }
    nested_only_status = {
        "paper_monitoring": {
            "weekly_report_status": "ready",
            "operator_pending_consistency_snapshot": mutating_snapshot,
        },
    }

    def snapshot_block(status: dict) -> str:
        markdown = status_to_markdown(status)
        paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
            "## Retrospective tracking", maxsplit=1
        )[0]
        return paper_monitoring_section.split("- Operator-pending consistency snapshot:", maxsplit=1)[1]

    top_level_block = snapshot_block(top_level_status)
    nested_only_block = snapshot_block(nested_only_status)

    assert top_level_block == nested_only_block
    for expected in (
        "  - Snapshot status: ready",
        "  - Snapshot freshness status: fresh",
        "  - Source status generated: 2026-06-05T08:58:55.301305+00:00",
        "  - Latest status generated: 2026-06-05T08:58:55.301305+00:00",
        "  - Consistency status: ok",
        "  - Mismatches: []",
        "  - Benchmark ID: CSI1000",
        "  - One-way turnover estimate: 0.791672",
        "  - Estimated round-trip cost: 0.00475",
        "  - Queue write allowed: False",
        "  - Broad daemon allowed: False",
        "  - Automation allowed: False",
        "  - Automated rerun allowed: False",
        "  - Live trading enabled: False",
    ):
        assert expected in top_level_block
    for forbidden in (
        "  - Queue write allowed: True",
        "  - Broad daemon allowed: True",
        "  - Automation allowed: True",
        "  - Automated rerun allowed: True",
        "  - Live trading enabled: True",
        "execution allowed",
    ):
        assert forbidden.lower() not in top_level_block.lower()


def test_status_treats_operator_pending_consistency_snapshot_runtime_as_reporting_only(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    source_generated_at = "2026-06-05T08:58:55.301305+00:00"
    _write_json(paths["status_json_path"], {"generated_at_utc": source_generated_at})
    _write_json(
        paths["operator_pending_consistency_snapshot_path"],
        {
            "snapshot_status": "ready",
            "source_status_generated_at_utc": source_generated_at,
            "consistency_status": "ok",
            "mismatches": [],
            "benchmark_id": "CSI1000",
            "turnover_one_way_estimate": 0.791672,
            "estimated_round_trip_cost": 0.00475,
            "runtime": {
                "queue_write_allowed": True,
                "broad_daemon_allowed": True,
                "automation_allowed": True,
                "automated_rerun_allowed": True,
                "live_trading_enabled": True,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    monitoring_snapshot = status["paper_monitoring"]["operator_pending_consistency_snapshot"]
    top_level_snapshot = status["operator_pending_consistency_snapshot"]
    assert monitoring_snapshot == top_level_snapshot
    assert top_level_snapshot["snapshot_freshness_status"] == "fresh"
    assert top_level_snapshot["source_status_generated_at_utc"] == source_generated_at
    assert top_level_snapshot["latest_status_generated_at_utc"] == source_generated_at
    for flag in (
        "queue_write_allowed",
        "broad_daemon_allowed",
        "automation_allowed",
        "automated_rerun_allowed",
        "live_trading_enabled",
    ):
        assert monitoring_snapshot[flag] is False
        assert top_level_snapshot[flag] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"

    markdown = status_to_markdown(status)
    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    snapshot_section = paper_monitoring_section.split("- Operator-pending consistency snapshot:", maxsplit=1)[1]
    assert "  - Snapshot freshness status: fresh" in snapshot_section
    assert "  - Queue write allowed: False" in snapshot_section
    assert "  - Broad daemon allowed: False" in snapshot_section
    assert "  - Automation allowed: False" in snapshot_section
    assert "  - Automated rerun allowed: False" in snapshot_section
    assert "  - Live trading enabled: False" in snapshot_section
    assert "  - Queue write allowed: True" not in snapshot_section
    assert "  - Broad daemon allowed: True" not in snapshot_section
    assert "  - Automation allowed: True" not in snapshot_section
    assert "  - Automated rerun allowed: True" not in snapshot_section
    assert "  - Live trading enabled: True" not in snapshot_section
    assert "execution allowed" not in snapshot_section.lower()


def test_status_marks_operator_pending_consistency_snapshot_fresh_when_source_status_matches(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    source_generated_at = "2026-06-05T08:58:55.301305+00:00"
    _write_json(paths["status_json_path"], {"generated_at_utc": source_generated_at})
    _write_json(
        paths["operator_pending_consistency_snapshot_path"],
        {
            "snapshot_status": "ready",
            "source_status_generated_at_utc": source_generated_at,
            "consistency_status": "ok",
            "mismatches": [],
            "runtime": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    snapshot = status["paper_monitoring"]["operator_pending_consistency_snapshot"]
    assert snapshot["source_status_generated_at_utc"] == source_generated_at
    assert snapshot["latest_status_generated_at_utc"] == source_generated_at
    assert snapshot["snapshot_freshness_status"] == "fresh"
    assert snapshot["queue_write_allowed"] is False
    assert snapshot["broad_daemon_allowed"] is False
    assert snapshot["automation_allowed"] is False
    assert snapshot["automated_rerun_allowed"] is False
    assert snapshot["live_trading_enabled"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"
    markdown = status_to_markdown(status)
    assert "Snapshot freshness status: fresh" in markdown


def test_status_keeps_weekly_reporting_ready_fresh_and_blocked_without_runtime_mutation(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    weekly_report = _weekly_operator_pending_payload()
    weekly_report["next_observation_window"] = "next_weekly_paper_review"
    weekly_report["blockers"] = {
        "decision": "ready_for_portfolio_mvp",
        "next_action": "repair_simulated_portfolio_construction",
        "primary_issue": "drawdown_risk_too_high",
        "manual_approval_gate_status": "blocked_pending_manual_approval",
        "human_approval_present": False,
        "approval_required": True,
        "required_decision_axis": "holding_count=50",
    }
    _write_json(paths["weekly_monitoring_report_path"], weekly_report)
    _write_json(paths["operator_pending_observation_path"], _operator_pending_observation_payload())
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    _write_json(
        paths["manual_approval_gate_path"],
        {
            "gate_status": "blocked_pending_manual_approval",
            "human_approval_present": False,
            "risk_relaxation_allowed": False,
            "automated_rerun_allowed": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automation_allowed": False,
            "live_trading_enabled": False,
        },
    )

    current_status = build_small_institutionalization_status(**paths)
    _write_json(paths["status_json_path"], current_status)
    write_operator_pending_consistency_snapshot(
        status_path=paths["status_json_path"],
        json_path=paths["operator_pending_consistency_snapshot_path"],
        markdown_path=tmp_path / "artifacts" / "small_institutionalization" / "operator_pending_consistency_snapshot.md",
        generated_at="2026-06-23T00:45:00+00:00",
    )

    reloaded_status = build_small_institutionalization_status(**paths)

    monitoring = reloaded_status["paper_monitoring"]
    assert monitoring["weekly_report_status"] == "ready"
    assert monitoring["missing_artifacts"] == []
    assert monitoring["next_observation_window"] == "next_weekly_paper_review"
    assert monitoring["operator_pending_consistency"]["consistency_status"] == "ok"
    for flag in (
        "queue_write_allowed",
        "broad_daemon_allowed",
        "automation_allowed",
        "automated_rerun_allowed",
        "live_trading_enabled",
    ):
        assert monitoring[flag] is False
    snapshot = monitoring["operator_pending_consistency_snapshot"]
    assert snapshot["snapshot_freshness_status"] == "fresh"
    assert snapshot["source_status_generated_at_utc"] == current_status["generated_at_utc"]
    assert snapshot["latest_status_generated_at_utc"] == current_status["generated_at_utc"]
    assert reloaded_status["next_action"] == "repair_simulated_portfolio_construction"

    markdown = status_to_markdown(reloaded_status)
    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    assert "- Weekly report status: ready" in paper_monitoring_section
    assert "- Missing artifacts: []" in paper_monitoring_section
    assert "- Next observation window: next_weekly_paper_review" in paper_monitoring_section
    assert "- Queue write allowed: False" in paper_monitoring_section
    assert "- Broad daemon allowed: False" in paper_monitoring_section
    assert "- Automation allowed: False" in paper_monitoring_section
    assert "- Automated rerun allowed: False" in paper_monitoring_section
    assert "- Live trading enabled: False" in paper_monitoring_section
    assert "  - Consistency status: ok" in paper_monitoring_section
    assert "  - Snapshot freshness status: fresh" in paper_monitoring_section
    assert "  - Primary issue: drawdown_risk_too_high" in paper_monitoring_section
    assert "  - Human approval present: False" in paper_monitoring_section
    assert "repair_status: blocked_no_drawdown_safe_candidate" not in paper_monitoring_section
    assert "execution allowed" not in paper_monitoring_section.lower()
    assert "automated rerun allowed: True" not in paper_monitoring_section


def test_status_marks_refreshed_operator_pending_consistency_snapshot_fresh_after_reload(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})
    _write_json(paths["weekly_monitoring_report_path"], _weekly_operator_pending_payload())
    _write_json(paths["operator_pending_observation_path"], _operator_pending_observation_payload())
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})

    current_status = build_small_institutionalization_status(**paths)
    _write_json(paths["status_json_path"], current_status)
    write_operator_pending_consistency_snapshot(
        status_path=paths["status_json_path"],
        json_path=paths["operator_pending_consistency_snapshot_path"],
        markdown_path=tmp_path / "artifacts" / "small_institutionalization" / "operator_pending_consistency_snapshot.md",
        generated_at="2026-06-05T09:00:00+00:00",
    )

    reloaded_status = build_small_institutionalization_status(**paths)

    snapshot = reloaded_status["paper_monitoring"]["operator_pending_consistency_snapshot"]
    assert snapshot["snapshot_status"] == "ready"
    assert snapshot["source_status_generated_at_utc"] == current_status["generated_at_utc"]
    assert snapshot["latest_status_generated_at_utc"] == current_status["generated_at_utc"]
    assert snapshot["snapshot_freshness_status"] == "fresh"
    assert snapshot["consistency_status"] == "ok"
    assert snapshot["mismatches"] == []
    assert snapshot["benchmark_id"] == "CSI1000"
    assert snapshot["turnover_one_way_estimate"] == 0.791672
    assert snapshot["estimated_round_trip_cost"] == 0.00475
    assert snapshot["queue_write_allowed"] is False
    assert snapshot["broad_daemon_allowed"] is False
    assert snapshot["automation_allowed"] is False
    assert snapshot["automated_rerun_allowed"] is False
    assert snapshot["live_trading_enabled"] is False
    assert reloaded_status["next_action"] == "repair_simulated_portfolio_construction"


def test_status_marks_operator_pending_consistency_snapshot_stale_without_runtime_side_effects(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    source_generated_at = "2026-06-05T08:58:55.301305+00:00"
    latest_generated_at = "2026-06-05T13:13:55.838386+00:00"
    _write_json(paths["status_json_path"], {"generated_at_utc": latest_generated_at})
    _write_json(
        paths["operator_pending_consistency_snapshot_path"],
        {
            "snapshot_status": "ready",
            "source_status_generated_at_utc": source_generated_at,
            "consistency_status": "ok",
            "mismatches": [],
            "runtime": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    snapshot = status["paper_monitoring"]["operator_pending_consistency_snapshot"]
    assert snapshot["source_status_generated_at_utc"] == source_generated_at
    assert snapshot["latest_status_generated_at_utc"] == latest_generated_at
    assert snapshot["snapshot_freshness_status"] == "stale"
    assert snapshot["queue_write_allowed"] is False
    assert snapshot["broad_daemon_allowed"] is False
    assert snapshot["automation_allowed"] is False
    assert snapshot["automated_rerun_allowed"] is False
    assert snapshot["live_trading_enabled"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"
    markdown = status_to_markdown(status)
    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    snapshot_section = paper_monitoring_section.split("- Operator-pending consistency snapshot:", maxsplit=1)[1]

    assert "  - Snapshot freshness status: stale" in snapshot_section
    assert f"  - Source status generated: {source_generated_at}" in snapshot_section
    assert f"  - Latest status generated: {latest_generated_at}" in snapshot_section
    assert "  - Consistency status: ok" in snapshot_section
    assert "  - Mismatches: []" in snapshot_section
    assert "  - Queue write allowed: False" in snapshot_section
    assert "  - Broad daemon allowed: False" in snapshot_section
    assert "  - Automation allowed: False" in snapshot_section
    assert "  - Automated rerun allowed: False" in snapshot_section
    assert "  - Live trading enabled: False" in snapshot_section
    assert "Snapshot consistency status" not in snapshot_section
    assert "Snapshot mismatches" not in snapshot_section


def test_status_markdown_marks_operator_pending_consistency_snapshot_source_metadata_missing(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    latest_generated_at = "2026-06-05T08:58:55.301305+00:00"
    _write_json(paths["status_json_path"], {"generated_at_utc": latest_generated_at})
    _write_json(
        paths["operator_pending_consistency_snapshot_path"],
        {
            "snapshot_status": "ready",
            "consistency_status": "ok",
            "mismatches": [],
            "runtime": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    snapshot = status["paper_monitoring"]["operator_pending_consistency_snapshot"]
    assert snapshot["source_status_generated_at_utc"] is None
    assert snapshot["latest_status_generated_at_utc"] == latest_generated_at
    assert snapshot["snapshot_freshness_status"] == "source_metadata_missing"
    assert snapshot["queue_write_allowed"] is False
    assert snapshot["broad_daemon_allowed"] is False
    assert snapshot["automation_allowed"] is False
    assert snapshot["automated_rerun_allowed"] is False
    assert snapshot["live_trading_enabled"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"

    markdown = status_to_markdown(status)
    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    snapshot_section = paper_monitoring_section.split("- Operator-pending consistency snapshot:", maxsplit=1)[1]
    assert "  - Snapshot freshness status: source_metadata_missing" in snapshot_section
    assert "  - Source status generated: None" in snapshot_section
    assert f"  - Latest status generated: {latest_generated_at}" in snapshot_section
    assert "  - Consistency status: ok" in snapshot_section
    assert "  - Mismatches: []" in snapshot_section
    assert "  - Queue write allowed: False" in snapshot_section
    assert "  - Broad daemon allowed: False" in snapshot_section
    assert "  - Automation allowed: False" in snapshot_section
    assert "  - Automated rerun allowed: False" in snapshot_section
    assert "  - Live trading enabled: False" in snapshot_section
    assert "Snapshot consistency status" not in snapshot_section
    assert "Snapshot mismatches" not in snapshot_section


def test_status_markdown_marks_operator_pending_consistency_snapshot_latest_metadata_missing(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})
    source_generated_at = "2026-06-05T08:58:55.301305+00:00"
    _write_json(paths["status_json_path"], {})
    _write_json(
        paths["operator_pending_consistency_snapshot_path"],
        {
            "snapshot_status": "ready",
            "source_status_generated_at_utc": source_generated_at,
            "consistency_status": "ok",
            "mismatches": [],
            "runtime": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    snapshot = status["paper_monitoring"]["operator_pending_consistency_snapshot"]
    assert snapshot["source_status_generated_at_utc"] == source_generated_at
    assert snapshot["latest_status_generated_at_utc"] is None
    assert snapshot["snapshot_freshness_status"] == "latest_status_metadata_missing"
    assert snapshot["queue_write_allowed"] is False
    assert snapshot["broad_daemon_allowed"] is False
    assert snapshot["automation_allowed"] is False
    assert snapshot["automated_rerun_allowed"] is False
    assert snapshot["live_trading_enabled"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"

    markdown = status_to_markdown(status)
    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    snapshot_section = paper_monitoring_section.split("- Operator-pending consistency snapshot:", maxsplit=1)[1]
    assert "  - Snapshot freshness status: latest_status_metadata_missing" in snapshot_section
    assert f"  - Source status generated: {source_generated_at}" in snapshot_section
    assert "  - Latest status generated: None" in snapshot_section
    assert "  - Consistency status: ok" in snapshot_section
    assert "  - Mismatches: []" in snapshot_section
    assert "  - Queue write allowed: False" in snapshot_section
    assert "  - Broad daemon allowed: False" in snapshot_section
    assert "  - Automation allowed: False" in snapshot_section
    assert "  - Automated rerun allowed: False" in snapshot_section
    assert "  - Live trading enabled: False" in snapshot_section
    assert "Snapshot consistency status" not in snapshot_section
    assert "Snapshot mismatches" not in snapshot_section


def test_status_tolerates_missing_operator_pending_consistency_snapshot(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(
        paths["weekly_monitoring_report_path"],
        {
            "cadence": "weekly",
            "missing_artifacts": [],
            "runtime": {
                "safe": True,
                "would_run_count": 0,
                "recommendations": ["pause_broad_daemon", "allow_controlled_only_daemon"],
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
            "blockers": {
                "decision": "ready_for_portfolio_mvp",
                "next_action": "repair_simulated_portfolio_construction",
                "primary_issue": "drawdown_risk_too_high",
                "manual_approval_gate_status": "blocked_pending_manual_approval",
                "human_approval_present": False,
                "approval_required": True,
                "required_decision_axis": "holding_count=50",
            },
            "next_observation_window": "next_weekly_paper_review",
        },
    )
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "automation_allowed": False})

    status = build_small_institutionalization_status(**paths)

    paper_monitoring = status["paper_monitoring"]
    assert "operator_pending_consistency_snapshot" not in paper_monitoring
    assert paper_monitoring["weekly_report_status"] == "ready"
    assert paper_monitoring["runtime_safe"] is True
    assert paper_monitoring["would_run_count"] == 0
    assert paper_monitoring["blockers"]["primary_issue"] == "drawdown_risk_too_high"
    assert paper_monitoring["next_observation_window"] == "next_weekly_paper_review"
    assert paper_monitoring["queue_write_allowed"] is False
    assert paper_monitoring["broad_daemon_allowed"] is False
    assert paper_monitoring["automation_allowed"] is False
    assert paper_monitoring["automated_rerun_allowed"] is False
    assert paper_monitoring["live_trading_enabled"] is False
    assert status["next_action"] == "repair_simulated_portfolio_construction"

    markdown = status_to_markdown(status)
    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    assert "Operator-pending consistency snapshot" not in paper_monitoring_section
    assert "Snapshot freshness status" not in paper_monitoring_section
    assert "Source status generated" not in paper_monitoring_section
    assert "Latest status generated" not in paper_monitoring_section
    assert "- Weekly report status: ready" in paper_monitoring_section
    assert "- Runtime safe: True" in paper_monitoring_section
    assert "- Weekly would-run count: 0" in paper_monitoring_section
    assert "- Next observation window: next_weekly_paper_review" in paper_monitoring_section
    assert "- Weekly blocker context:" in paper_monitoring_section
    assert "  - Primary issue: drawdown_risk_too_high" in paper_monitoring_section
    assert "  - Manual approval gate status: blocked_pending_manual_approval" in paper_monitoring_section
    assert "- Queue write allowed: False" in paper_monitoring_section
    assert "- Broad daemon allowed: False" in paper_monitoring_section
    assert "- Automation allowed: False" in paper_monitoring_section
    assert "- Automated rerun allowed: False" in paper_monitoring_section
    assert "- Live trading enabled: False" in paper_monitoring_section


def test_status_marks_operator_pending_observation_missing_without_runtime_side_effects(tmp_path):
    paths = _baseline_files(tmp_path)

    status = build_small_institutionalization_status(**paths)

    assert status["operator_pending_observation"] == {"observation_status": "missing"}
    assert status["runtime_safety"]["safe"] is True
    assert status["runtime_safety"]["would_run_count"] == 0
    assert status["next_action"] == "write_benchmark_cost_turnover_diagnostics"

    markdown = status_to_markdown(status)
    operator_pending_section = markdown.split("## Operator pending observation", 1)[1].split("\n## ", 1)[0]
    assert "- Observation status: missing" in operator_pending_section
    assert "Primary issue" not in operator_pending_section
    assert "Manual approval status" not in operator_pending_section
    assert "Benchmark ID" not in operator_pending_section
    assert "One-way turnover estimate" not in operator_pending_section
    assert "Estimated round-trip cost" not in operator_pending_section


def test_status_preserves_benchmark_cost_turnover_visibility_at_operator_wait_state(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(
        paths["portfolio_diagnostics_path"],
        {
            "benchmark": {"benchmark_id": "CSI1000", "benchmark_name": "中证1000", "tracking_mode": "metadata_only"},
            "turnover": {"turnover_one_way_estimate": 0.791672},
            "cost": {"estimated_round_trip_cost": 0.00475},
        },
    )
    malicious_weekly_payload = _weekly_operator_pending_payload()
    malicious_weekly_payload["operator_pending_observation"].update(
        {
            "queue_write_allowed": True,
            "broad_daemon_allowed": True,
            "automation_allowed": True,
            "automated_rerun_allowed": True,
            "live_trading_enabled": True,
        }
    )
    _write_json(paths["weekly_monitoring_report_path"], malicious_weekly_payload)
    _write_json(paths["operator_pending_observation_path"], _operator_pending_observation_payload())
    _write_json(
        paths["simulation_self_diagnosis_path"],
        {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False},
    )
    _write_json(
        paths["simulated_portfolio_construction_repair_path"],
        {
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "automation_allowed": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        },
    )
    _write_json(
        paths["repair_blocker_manual_review_path"],
        {
            "review_status": "blocked_manual_review_required",
            "primary_issue": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "safety": {"queue_write_allowed": False, "broad_daemon_allowed": False, "automation_allowed": False},
            "recommended_manual_decision": {
                "decision_required": True,
                "dimension": "holding_count",
                "value": "50",
                "automated_rerun_allowed": False,
            },
        },
    )
    _write_json(
        paths["manual_approval_gate_path"],
        {
            "gate_status": "blocked_pending_manual_approval",
            "human_approval_present": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automation_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        },
    )
    _write_json(
        paths["operator_approval_summary_path"],
        {
            "summary_status": "blocked_pending_manual_approval",
            "approval_required": True,
            "human_approval_present": False,
            "required_decision_axis": "holding_count=50",
            "primary_blocker": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )
    _write_json(
        paths["operator_decision_handoff_path"],
        {
            "handoff_status": "awaiting_operator_decision",
            "intake_status": "missing",
            "primary_blocker": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "decision_axis": "holding_count=50",
            "validation_errors": [],
            "non_mutating": True,
            "execution_allowed": False,
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )

    status = build_small_institutionalization_status(**paths)

    assert status["next_action"] == "repair_simulated_portfolio_construction"
    assert status["operator_decision_wait_state"]["wait_state_status"] == "awaiting_operator_decision"
    assert status["paper_portfolio"]["benchmark_id"] == "CSI1000"
    assert status["paper_portfolio"]["turnover_one_way_estimate"] == 0.791672
    assert status["paper_portfolio"]["estimated_round_trip_cost"] == 0.00475
    weekly_operator_pending = status["paper_monitoring"]["operator_pending_observation"]
    assert weekly_operator_pending["benchmark_id"] == "CSI1000"
    assert weekly_operator_pending["turnover_one_way_estimate"] == 0.791672
    assert weekly_operator_pending["estimated_round_trip_cost"] == 0.00475
    for section in (
        status["paper_monitoring"],
        weekly_operator_pending,
        status["operator_pending_observation"],
        status["operator_decision_wait_state"],
    ):
        assert section["queue_write_allowed"] is False
        assert section["broad_daemon_allowed"] is False
        assert section["automation_allowed"] is False
        assert section["automated_rerun_allowed"] is False
        assert section["live_trading_enabled"] is False

    markdown = status_to_markdown(status)
    paper_portfolio_section = markdown.split("## Paper portfolio", maxsplit=1)[1].split(
        "## Paper monitoring", maxsplit=1
    )[0]
    paper_monitoring_section = markdown.split("## Paper monitoring", maxsplit=1)[1].split(
        "## Retrospective tracking", maxsplit=1
    )[0]
    weekly_operator_pending_section = paper_monitoring_section.split(
        "- Weekly operator-pending observation:", maxsplit=1
    )[1].split("- Weekly/canonical operator-pending consistency:", maxsplit=1)[0]
    operator_pending_section = markdown.split("## Operator pending observation", maxsplit=1)[1].split(
        "\n## ", maxsplit=1
    )[0]
    wait_state_section = markdown.split("## Operator decision wait state", maxsplit=1)[1].split(
        "## Operator pending observation", maxsplit=1
    )[0]

    assert "- Benchmark ID: CSI1000" in paper_portfolio_section
    assert "- One-way turnover estimate: 0.791672" in paper_portfolio_section
    assert "- Estimated round-trip cost: 0.00475" in paper_portfolio_section

    for rendered_section in (weekly_operator_pending_section, operator_pending_section):
        assert "Benchmark ID: CSI1000" in rendered_section
        assert "One-way turnover estimate: 0.791672" in rendered_section
        assert "Estimated round-trip cost: 0.00475" in rendered_section
        assert "Queue write allowed: False" in rendered_section
        assert "Broad daemon allowed: False" in rendered_section
        assert "Automation allowed: False" in rendered_section
        assert "Automated rerun allowed: False" in rendered_section
        assert "Live trading enabled: False" in rendered_section
        assert "Queue write allowed: True" not in rendered_section
        assert "Broad daemon allowed: True" not in rendered_section
        assert "Automation allowed: True" not in rendered_section
        assert "Automated rerun allowed: True" not in rendered_section
        assert "Live trading enabled: True" not in rendered_section

    assert "- Wait-state status: awaiting_operator_decision" in wait_state_section
    assert "- Queue write allowed: False" in wait_state_section
    assert "- Broad daemon allowed: False" in wait_state_section
    assert "- Automation allowed: False" in wait_state_section
    assert "- Automated rerun allowed: False" in wait_state_section
    assert "- Live trading enabled: False" in wait_state_section
