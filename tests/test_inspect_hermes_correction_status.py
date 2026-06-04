from __future__ import annotations

import json
from pathlib import Path

from scripts.inspect_hermes_correction_status import inspect_hermes_correction_status


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _base_blocked_repair(root: Path) -> None:
    _write_json(
        root / "artifacts/small_institutional_simulation/portfolio_construction_repair.json",
        {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "recommended_candidate": None},
    )


def test_inspector_ready_for_executor_when_plan_exists_without_results(tmp_path):
    _base_blocked_repair(tmp_path)
    _write_json(tmp_path / "artifacts/small_institutional_simulation/risk_reduction_plan.json", {"plan_status": "candidate_plan_ready"})

    status = inspect_hermes_correction_status(root=tmp_path)

    assert status["correction_status"] == "ready_for_executor_agent"
    assert status["failure_target"] == "portfolio_simulation_drawdown_blocker"
    assert status["portfolio_status"] == "unproven_until_executor_runs"
    assert status["next_action"] == "run_risk_reduction_controlled_executor"
    assert status["latest_agent_role"] == "implementer"


def test_inspector_requests_plan_when_missing(tmp_path):
    _base_blocked_repair(tmp_path)

    status = inspect_hermes_correction_status(root=tmp_path)

    assert status["next_action"] == "write_risk_reduction_plan"
    assert status["correction_status"] == "needs_risk_reduction_plan"


def test_inspector_scores_results_before_repair_scoring_exists(tmp_path):
    _base_blocked_repair(tmp_path)
    _write_json(tmp_path / "artifacts/small_institutional_simulation/risk_reduction_plan.json", {"plan_status": "candidate_plan_ready"})
    _write_json(tmp_path / "artifacts/small_institutional_simulation/risk_reduction_results.json", {"results": [{"max_drawdown": -0.31}]})

    status = inspect_hermes_correction_status(root=tmp_path)

    assert status["next_action"] == "score_risk_reduction_results"
    assert status["correction_status"] == "ready_for_verifier_agent"


def test_inspector_reports_blocker_when_risk_reduction_repair_scoring_still_blocked(tmp_path):
    _base_blocked_repair(tmp_path)
    _write_json(tmp_path / "artifacts/small_institutional_simulation/risk_reduction_plan.json", {"plan_status": "candidate_plan_ready"})
    _write_json(tmp_path / "artifacts/small_institutional_simulation/risk_reduction_results.json", {"results": []})
    _write_json(
        tmp_path / "artifacts/small_institutional_simulation/risk_reduction_repair.json",
        {
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "best_available_max_drawdown": -0.47,
        },
    )

    status = inspect_hermes_correction_status(root=tmp_path)

    assert status["next_action"] == "write_blocker_report_or_request_new_mechanism"
    assert status["correction_status"] == "blocked_after_scoring"
    assert status["portfolio_status"] == "blocked_no_drawdown_safe_candidate_after_scoring"


def test_inspector_sends_risk_reduction_safe_candidate_to_manual_review(tmp_path):
    _base_blocked_repair(tmp_path)
    _write_json(tmp_path / "artifacts/small_institutional_simulation/risk_reduction_results.json", {"results": []})
    _write_json(
        tmp_path / "artifacts/small_institutional_simulation/risk_reduction_repair.json",
        {"repair_status": "candidate_ready_for_manual_review", "candidate_count": 1, "recommended_candidate": {"combo_id": "safe"}},
    )

    status = inspect_hermes_correction_status(root=tmp_path)

    assert status["next_action"] == "manual_review_before_admission"
    assert status["correction_status"] == "manual_review_required"
    assert status["portfolio_status"] == "safe_candidate_found_pending_manual_review"


def test_inspector_cli_json_is_deterministic(tmp_path, capsys):
    from scripts.inspect_hermes_correction_status import main

    _base_blocked_repair(tmp_path)
    assert main(["--root", str(tmp_path)]) == 0
    captured = capsys.readouterr().out
    payload = json.loads(captured)
    assert payload["next_action"] == "write_risk_reduction_plan"
