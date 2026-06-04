import json
from pathlib import Path

from factor_lab.small_institutional_approval_artifact_consistency import (
    approval_artifact_consistency_to_markdown,
    build_approval_artifact_consistency,
    write_approval_artifact_consistency,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _artifact_paths(tmp_path: Path) -> dict[str, Path]:
    manual_review_path = _write_json(
        tmp_path / "repair_blocker_manual_review.json",
        {
            "generated_at_utc": "2026-06-02T10:00:00+00:00",
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
                "dimension": "holding_count",
                "value": "50",
                "automated_rerun_allowed": False,
            },
        },
    )
    gate_path = _write_json(
        tmp_path / "manual_approval_gate.json",
        {
            "generated_at_utc": "2026-06-02T11:00:00+00:00",
            "gate_status": "blocked_pending_manual_approval",
            "human_approval_present": False,
            "required_approval": {
                "dimension": "holding_count",
                "value": "50",
                "primary_issue": "drawdown_risk_too_high",
                "repair_status": "blocked_no_drawdown_safe_candidate",
                "best_available_max_drawdown": -0.478256,
                "drawdown_gap_to_limit": 0.128256,
            },
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automation_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
            "safety": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
            },
        },
    )
    operator_summary_path = _write_json(
        tmp_path / "operator_approval_summary.json",
        {
            "generated_at_utc": "2026-06-02T12:00:00+00:00",
            "summary_status": "blocked_pending_manual_approval",
            "required_decision_axis": "holding_count=50",
            "primary_blocker": "drawdown_risk_too_high",
            "repair_status": "blocked_no_drawdown_safe_candidate",
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
    status_path = _write_json(
        tmp_path / "status.json",
        {
            "generated_at_utc": "2026-06-02T13:00:00+00:00",
            "next_action": "repair_simulated_portfolio_construction",
            "repair_blocker_manual_review": {
                "primary_issue": "drawdown_risk_too_high",
                "repair_status": "blocked_no_drawdown_safe_candidate",
                "best_available_max_drawdown": -0.478256,
                "drawdown_gap_to_limit": 0.128256,
                "manual_decision_dimension": "holding_count",
                "manual_decision_value": "50",
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
            },
            "manual_approval_gate": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
            "operator_approval_summary": {
                "required_decision_axis": "holding_count=50",
                "primary_blocker": "drawdown_risk_too_high",
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )
    return {
        "manual_review_path": manual_review_path,
        "gate_path": gate_path,
        "operator_summary_path": operator_summary_path,
        "status_path": status_path,
    }


def test_build_approval_artifact_consistency_reports_ok_when_key_fields_match(tmp_path):
    payload = build_approval_artifact_consistency(**_artifact_paths(tmp_path))

    assert payload["consistency_status"] == "ok"
    assert payload["matched_fields"]["primary_blocker"] == "drawdown_risk_too_high"
    assert payload["matched_fields"]["decision_axis"] == "holding_count=50"
    assert payload["matched_fields"]["best_available_max_drawdown"] == -0.478256
    assert payload["matched_fields"]["drawdown_gap_to_limit"] == 0.128256
    assert payload["safety_flags"] == {
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
    }
    assert payload["inconsistencies"] == []
    assert payload["staleness_warnings"] == []


def test_build_approval_artifact_consistency_reports_mismatches_without_mutating_state(tmp_path):
    paths = _artifact_paths(tmp_path)
    operator = json.loads(paths["operator_summary_path"].read_text(encoding="utf-8"))
    operator["required_decision_axis"] = "holding_count=72"
    operator["safety"]["automation_allowed"] = True
    paths["operator_summary_path"].write_text(json.dumps(operator), encoding="utf-8")

    payload = build_approval_artifact_consistency(**paths)

    assert payload["consistency_status"] == "inconsistent"
    assert any("decision_axis" in item for item in payload["inconsistencies"])
    assert any("automation_allowed" in item for item in payload["inconsistencies"])
    assert payload["non_mutating"] is True


def test_build_approval_artifact_consistency_warns_on_missing_or_out_of_order_timestamps(tmp_path):
    paths = _artifact_paths(tmp_path)
    manual = json.loads(paths["manual_review_path"].read_text(encoding="utf-8"))
    manual.pop("generated_at_utc")
    paths["manual_review_path"].write_text(json.dumps(manual), encoding="utf-8")
    status = json.loads(paths["status_path"].read_text(encoding="utf-8"))
    status["generated_at_utc"] = "2026-06-02T09:00:00+00:00"
    paths["status_path"].write_text(json.dumps(status), encoding="utf-8")

    payload = build_approval_artifact_consistency(**paths)

    assert any("manual_review:missing_generated_at_utc" in item for item in payload["staleness_warnings"])
    assert any("status_older_than_operator_summary" in item for item in payload["staleness_warnings"])


def test_write_approval_artifact_consistency_writes_json_and_markdown(tmp_path):
    paths = _artifact_paths(tmp_path)
    json_path = tmp_path / "approval_artifact_consistency.json"
    markdown_path = tmp_path / "approval_artifact_consistency.md"

    payload = write_approval_artifact_consistency(json_path=json_path, markdown_path=markdown_path, **paths)

    assert payload["consistency_status"] == "ok"
    assert json.loads(json_path.read_text(encoding="utf-8"))["consistency_status"] == "ok"
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Approval Artifact Consistency" in markdown
    assert "queue_write_allowed" in markdown


def test_approval_artifact_consistency_markdown_lists_mismatches_and_warnings(tmp_path):
    payload = build_approval_artifact_consistency(**_artifact_paths(tmp_path))
    payload["inconsistencies"] = ["decision_axis mismatch"]
    payload["staleness_warnings"] = ["status_older_than_operator_summary"]

    markdown = approval_artifact_consistency_to_markdown(payload)

    assert "decision_axis mismatch" in markdown
    assert "status_older_than_operator_summary" in markdown
