import json
from pathlib import Path

from factor_lab.small_institutional_repair_blocker_manual_review import (
    build_repair_blocker_manual_review,
    manual_review_to_markdown,
    write_repair_blocker_manual_review,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _evidence_payload() -> dict:
    return {
        "blocker": {"primary_issue": "drawdown_risk_too_high", "severity": "high"},
        "repair": {
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "best_available_max_drawdown": -0.478256,
            "drawdown_gap_to_limit": 0.128256,
            "automation_allowed": False,
        },
        "manual_review": {
            "diagnostic_status": "blocked_no_group_under_drawdown_limit",
            "dimension": "holding_count",
            "value": "50",
            "best_max_drawdown": -0.478256,
            "drawdown_gap_to_limit": 0.128256,
        },
        "paper_portfolio_context": {
            "benchmark_id": "CSI1000",
            "benchmark_name": "中证1000",
            "tracking_mode": "metadata_only",
            "turnover_one_way_estimate": 0.791672,
            "estimated_round_trip_cost": 0.00475,
        },
        "safety": {
            "automation_allowed": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
        },
    }


def test_manual_review_payload_surfaces_blocker_context_and_forbids_automation(tmp_path):
    evidence_path = _write_json(tmp_path / "drawdown_blocker_evidence.json", _evidence_payload())
    repair_path = _write_json(
        tmp_path / "portfolio_construction_repair.json",
        {
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "best_available_max_drawdown": -0.478256,
            "drawdown_gap_to_limit": 0.128256,
            "automation_allowed": False,
        },
    )
    group_path = _write_json(
        tmp_path / "drawdown_group_diagnostic.json",
        {
            "diagnostic_status": "blocked_no_group_under_drawdown_limit",
            "recommended_manual_axis": {"dimension": "holding_count", "value": "50"},
            "automation_allowed": False,
        },
    )

    payload = build_repair_blocker_manual_review(evidence_path, repair_path, group_path)

    assert payload["review_status"] == "blocked_manual_review_required"
    assert payload["primary_issue"] == "drawdown_risk_too_high"
    assert payload["repair_status"] == "blocked_no_drawdown_safe_candidate"
    assert payload["candidate_count"] == 0
    assert payload["best_available_max_drawdown"] == -0.478256
    assert payload["drawdown_gap_to_limit"] == 0.128256
    assert payload["paper_portfolio_context"]["benchmark_id"] == "CSI1000"
    assert payload["paper_portfolio_context"]["turnover_one_way_estimate"] == 0.791672
    assert payload["safety"] == {
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
    }
    assert payload["recommended_manual_decision"] == {
        "decision_required": True,
        "dimension": "holding_count",
        "value": "50",
        "automated_rerun_allowed": False,
        "notes": "Manual approval is required before any repaired rerun or risk relaxation.",
    }


def test_manual_review_reports_missing_evidence_without_error(tmp_path):
    payload = build_repair_blocker_manual_review(
        tmp_path / "missing_evidence.json",
        tmp_path / "missing_repair.json",
        tmp_path / "missing_group.json",
    )

    assert payload["review_status"] == "missing_evidence"
    assert payload["safety"] == {
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
    }


def test_write_manual_review_writes_json_and_markdown(tmp_path):
    evidence_path = _write_json(tmp_path / "drawdown_blocker_evidence.json", _evidence_payload())
    repair_path = _write_json(tmp_path / "portfolio_construction_repair.json", {})
    group_path = _write_json(tmp_path / "drawdown_group_diagnostic.json", {})
    json_path = tmp_path / "repair_blocker_manual_review.json"
    markdown_path = tmp_path / "repair_blocker_manual_review.md"

    payload = write_repair_blocker_manual_review(
        evidence_path=evidence_path,
        repair_path=repair_path,
        group_diagnostic_path=group_path,
        json_path=json_path,
        markdown_path=markdown_path,
    )

    assert payload["review_status"] == "blocked_manual_review_required"
    assert json.loads(json_path.read_text(encoding="utf-8"))["review_status"] == "blocked_manual_review_required"
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Repair Blocker Manual Review" in markdown
    assert "automated_rerun_allowed: False" in markdown


def test_manual_review_markdown_includes_safety_and_context():
    markdown = manual_review_to_markdown(
        {
            "generated_at_utc": "2026-06-02T00:00:00+00:00",
            "review_status": "blocked_manual_review_required",
            "primary_issue": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "best_available_max_drawdown": -0.478256,
            "drawdown_gap_to_limit": 0.128256,
            "paper_portfolio_context": {"benchmark_id": "CSI1000", "estimated_round_trip_cost": 0.00475},
            "safety": {"queue_write_allowed": False, "broad_daemon_allowed": False, "automation_allowed": False},
            "recommended_manual_decision": {"dimension": "holding_count", "value": "50", "automated_rerun_allowed": False},
        }
    )

    assert "CSI1000" in markdown
    assert "queue_write_allowed: False" in markdown
    assert "holding_count=50" in markdown
