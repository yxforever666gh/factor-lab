import json
from pathlib import Path

from factor_lab.simulation_risk_constraint_diagnostics import (
    build_simulation_risk_constraint_diagnostics,
    diagnostics_to_markdown,
    write_simulation_risk_constraint_diagnostics,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def test_build_risk_constraint_diagnostics_reports_drawdown_gap_and_safe_next_step():
    matrix = {
        "matrix_status": "partial",
        "summary": {"ok_count": 2, "result_count": 2},
        "best_result": {"max_drawdown": -0.49, "sharpe": 0.45},
        "results": [
            {"status": "ok", "max_drawdown": -0.62, "holding_count": 50},
            {"status": "ok", "max_drawdown": -0.48, "holding_count": 75},
        ],
    }
    self_diagnosis = {
        "primary_issue": "drawdown_risk_too_high",
        "thresholds": {"max_drawdown_limit": -0.35},
        "automation_allowed": False,
    }
    repair = {
        "repair_status": "blocked_no_drawdown_safe_candidate",
        "candidate_count": 0,
        "automation_allowed": False,
    }

    payload = build_simulation_risk_constraint_diagnostics(matrix, self_diagnosis, repair)

    assert payload["diagnostic_status"] == "blocked_drawdown_gap"
    assert payload["best_available_drawdown"] == -0.48
    assert payload["drawdown_threshold"] == -0.35
    assert payload["drawdown_gap"] == 0.13
    assert payload["candidate_count"] == 0
    assert payload["recommended_safe_next_step"] == "tighten_simulation_risk_constraints_before_rerun"
    assert payload["automation_allowed"] is False
    assert payload["safety"]["queue_write_allowed"] is False
    assert payload["safety"]["daemon_change_allowed"] is False
    assert payload["safety"]["live_trading_enabled"] is False


def test_write_risk_constraint_diagnostics_reads_artifacts_and_writes_json_markdown(tmp_path):
    matrix_path = _write_json(
        tmp_path / "matrix.json",
        {
            "matrix_status": "partial",
            "summary": {"ok_count": 1, "result_count": 1},
            "results": [{"status": "ok", "max_drawdown": -0.41}],
        },
    )
    self_diagnosis_path = _write_json(
        tmp_path / "self_diagnosis.json",
        {"primary_issue": "drawdown_risk_too_high", "thresholds": {"max_drawdown_limit": -0.35}},
    )
    repair_path = _write_json(
        tmp_path / "portfolio_construction_repair.json",
        {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0},
    )
    json_path = tmp_path / "risk_constraint_diagnostics.json"
    markdown_path = tmp_path / "risk_constraint_diagnostics.md"

    payload = write_simulation_risk_constraint_diagnostics(
        matrix_path=matrix_path,
        self_diagnosis_path=self_diagnosis_path,
        repair_path=repair_path,
        json_path=json_path,
        markdown_path=markdown_path,
    )

    assert payload["drawdown_gap"] == 0.06
    assert json.loads(json_path.read_text(encoding="utf-8"))["automation_allowed"] is False
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Simulation Risk Constraint Diagnostics" in markdown
    assert "tighten_simulation_risk_constraints_before_rerun" in markdown


def test_markdown_includes_threshold_gap_and_safety_flags():
    markdown = diagnostics_to_markdown(
        {
            "generated_at_utc": "2026-05-15T00:00:00+00:00",
            "diagnostic_status": "blocked_drawdown_gap",
            "best_available_drawdown": -0.48,
            "drawdown_threshold": -0.35,
            "drawdown_gap": 0.13,
            "candidate_count": 0,
            "recommended_safe_next_step": "tighten_simulation_risk_constraints_before_rerun",
            "automation_allowed": False,
            "safety": {
                "queue_write_allowed": False,
                "daemon_change_allowed": False,
                "live_trading_enabled": False,
            },
        }
    )

    assert "drawdown_gap" in markdown
    assert "queue_write_allowed: False" in markdown
    assert "automation_allowed: False" in markdown
