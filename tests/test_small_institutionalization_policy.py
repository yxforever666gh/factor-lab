import json
from pathlib import Path

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
    _write_json(paths["weekly_monitoring_report_path"], {"cadence": "weekly", "missing_artifacts": [], "runtime": {"safe": True}})

    status = build_small_institutionalization_status(**paths)

    assert status["paper_monitoring"]["weekly_report_status"] == "ready"
    assert status["paper_monitoring"]["cadence"] == "weekly"
    assert status["paper_monitoring"]["missing_artifacts"] == []
    assert status["paper_monitoring"]["runtime_safe"] is True
    assert status["next_action"] == "run_small_institutional_self_diagnosis"


def test_status_marks_weekly_monitoring_report_missing_until_report_exists(tmp_path):
    paths = _baseline_files(tmp_path)
    _write_json(paths["portfolio_diagnostics_path"], {"benchmark": {"benchmark_id": "CSI1000"}})

    status = build_small_institutionalization_status(**paths)

    assert status["paper_monitoring"]["weekly_report_status"] == "missing"
    assert status["next_action"] == "paper_monitoring_weekly_report"


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
        "best_available_max_drawdown": -0.478256,
        "drawdown_gap_to_limit": 0.128256,
    }
    assert status["next_action"] == "repair_simulated_portfolio_construction"


def test_status_requests_repaired_bounded_matrix_when_repair_candidate_exists(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    candidate = {"combo_id": "safe", "holding_count": 50, "max_drawdown": -0.30, "sharpe": 0.8}
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "automation_allowed": False})
    _write_json(paths["simulated_portfolio_construction_repair_path"], {"repair_status": "candidate_found", "candidate_count": 1, "recommended_candidate": candidate, "automation_allowed": False})

    status = build_small_institutionalization_status(**paths)

    assert status["simulated_portfolio_construction_repair"]["recommended_candidate"] == candidate
    assert status["next_action"] == "rerun_bounded_matrix_with_repaired_construction"


def test_status_allows_bounded_large_scale_simulation_when_self_diagnosis_ready(tmp_path):
    paths = _baseline_files(tmp_path)
    _complete_paper_path(paths)
    _write_json(paths["simulation_self_diagnosis_path"], {"diagnosis_status": "ready", "primary_issue": "ready_for_broader_simulation", "next_action": "run_bounded_large_scale_simulation", "automation_allowed": True})

    status = build_small_institutionalization_status(**paths)

    assert status["next_action"] == "run_bounded_large_scale_simulation"
    assert status["small_institutional_simulation"]["automation_allowed"] is True
