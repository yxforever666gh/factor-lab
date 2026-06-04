from __future__ import annotations

import json
from pathlib import Path

from scripts.write_hermes_correction_state import write_hermes_correction_state


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_writer_reads_known_artifacts_and_writes_json_and_markdown(tmp_path):
    root = tmp_path
    _write_json(root / "artifacts/small_institutionalization/status.json", {"status": "blocked"})
    _write_json(
        root / "artifacts/small_institutional_simulation/portfolio_construction_repair.json",
        {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0},
    )
    _write_json(root / "artifacts/small_institutional_simulation/risk_reduction_plan.json", {"plan_status": "candidate_plan_ready"})
    _write_json(root / "artifacts/controlled_restart_dry_run.json", {"claimable_workflow_count": 0})
    _write_json(root / "artifacts/runtime_takeover_audit.json", {"runtime_safety": "safe"})

    state = write_hermes_correction_state(root=root)

    json_path = root / "artifacts/hermes_correction/current_state.json"
    md_path = root / "artifacts/hermes_correction/current_state.md"
    written = json.loads(json_path.read_text(encoding="utf-8"))
    markdown = md_path.read_text(encoding="utf-8")
    assert state == written
    assert written["failure_target"] == "portfolio_simulation_drawdown_blocker"
    assert written["diagnosis"]["next_action"] == "run_risk_reduction_controlled_executor"
    assert written["next_agent_role"] == "implementer"
    assert written["manual_review_required"] is True
    assert written["queue_write_allowed"] is False
    assert written["automation_allowed"] is False
    assert any(item["path"].endswith("risk_reduction_results.json") for item in written["source_artifacts"])
    assert any(item["present"] is False and item["path"].endswith("risk_reduction_results.json") for item in written["source_artifacts"])
    assert "Hermes Correction State" in markdown
    assert "run_risk_reduction_controlled_executor" in markdown


def test_writer_sets_plan_authoring_next_action_when_plan_missing(tmp_path):
    _write_json(
        tmp_path / "artifacts/small_institutional_simulation/portfolio_construction_repair.json",
        {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0},
    )

    state = write_hermes_correction_state(root=tmp_path)

    assert state["diagnosis"]["next_action"] == "write_risk_reduction_plan"
    assert state["next_agent_role"] == "implementer"
