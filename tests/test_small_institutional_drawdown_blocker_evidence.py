import json
from pathlib import Path

from factor_lab.small_institutional_drawdown_blocker_evidence import (
    build_drawdown_blocker_evidence,
    evidence_to_markdown,
    write_drawdown_blocker_evidence,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _artifact_paths(tmp_path: Path) -> dict[str, Path]:
    status_path = _write_json(
        tmp_path / "artifacts" / "small_institutionalization" / "status.json",
        {
            "decision": "ready_for_portfolio_mvp",
            "runtime_safety": {"safe": True, "would_run_count": 0},
            "small_institutional_simulation": {
                "diagnosis_status": "blocked",
                "primary_issue": "drawdown_risk_too_high",
                "severity": "high",
                "automation_allowed": False,
            },
            "next_action": "repair_simulated_portfolio_construction",
        },
    )
    repair_path = _write_json(
        tmp_path / "artifacts" / "small_institutional_simulation" / "portfolio_construction_repair.json",
        {
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "best_available_max_drawdown": -0.478256,
            "drawdown_gap_to_limit": 0.128256,
            "automation_allowed": False,
        },
    )
    group_path = _write_json(
        tmp_path / "artifacts" / "small_institutional_simulation" / "drawdown_group_diagnostic.json",
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
    diagnostics_path = _write_json(
        tmp_path / "artifacts" / "paper_portfolio" / "portfolio_diagnostics.json",
        {
            "benchmark": {"benchmark_id": "CSI1000", "benchmark_name": "中证1000", "tracking_mode": "metadata_only"},
            "turnover": {"turnover_one_way_estimate": 0.791672},
            "cost": {"estimated_round_trip_cost": 0.00475},
        },
    )
    return {"status_path": status_path, "repair_path": repair_path, "group_path": group_path, "diagnostics_path": diagnostics_path}


def test_build_drawdown_blocker_evidence_summarizes_current_blocker_without_enabling_automation(tmp_path):
    paths = _artifact_paths(tmp_path)

    payload = build_drawdown_blocker_evidence(**paths)

    assert payload["blocker"]["primary_issue"] == "drawdown_risk_too_high"
    assert payload["blocker"]["severity"] == "high"
    assert payload["repair"]["repair_status"] == "blocked_no_drawdown_safe_candidate"
    assert payload["repair"]["candidate_count"] == 0
    assert payload["repair"]["best_available_max_drawdown"] == -0.478256
    assert payload["repair"]["drawdown_gap_to_limit"] == 0.128256
    assert payload["manual_review"]["dimension"] == "holding_count"
    assert payload["manual_review"]["value"] == "50"
    assert payload["paper_portfolio_context"] == {
        "benchmark_id": "CSI1000",
        "benchmark_name": "中证1000",
        "tracking_mode": "metadata_only",
        "turnover_one_way_estimate": 0.791672,
        "estimated_round_trip_cost": 0.00475,
    }
    assert payload["safety"]["automation_allowed"] is False
    assert payload["safety"]["queue_write_allowed"] is False
    assert payload["safety"]["broad_daemon_allowed"] is False
    assert payload["next_action"] == "repair_simulated_portfolio_construction"


def test_write_drawdown_blocker_evidence_writes_json_and_markdown(tmp_path):
    paths = _artifact_paths(tmp_path)
    json_path = tmp_path / "artifacts" / "small_institutional_simulation" / "drawdown_blocker_evidence.json"
    markdown_path = tmp_path / "artifacts" / "small_institutional_simulation" / "drawdown_blocker_evidence.md"

    payload = write_drawdown_blocker_evidence(json_path=json_path, markdown_path=markdown_path, **paths)

    written = json.loads(json_path.read_text(encoding="utf-8"))
    assert written["repair"]["drawdown_gap_to_limit"] == 0.128256
    assert payload["safety"]["queue_write_allowed"] is False
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Drawdown Blocker Evidence" in markdown
    assert "queue_write_allowed: False" in markdown
    assert "holding_count=50" in markdown
    assert "CSI1000" in markdown
    assert "turnover_one_way_estimate: 0.791672" in markdown


def test_evidence_markdown_includes_blocker_repair_manual_review_and_safety():
    markdown = evidence_to_markdown(
        {
            "generated_at_utc": "2026-06-01T00:00:00+00:00",
            "blocker": {"diagnosis_status": "blocked", "primary_issue": "drawdown_risk_too_high", "severity": "high"},
            "repair": {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0, "best_available_max_drawdown": -0.478256, "drawdown_gap_to_limit": 0.128256},
            "manual_review": {"dimension": "holding_count", "value": "50"},
            "safety": {"automation_allowed": False, "queue_write_allowed": False, "broad_daemon_allowed": False},
            "next_action": "repair_simulated_portfolio_construction",
        }
    )

    assert "drawdown_risk_too_high" in markdown
    assert "blocked_no_drawdown_safe_candidate" in markdown
    assert "holding_count=50" in markdown
    assert "broad_daemon_allowed: False" in markdown
