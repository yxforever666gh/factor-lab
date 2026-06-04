import json
from pathlib import Path

from factor_lab.small_institutional_manual_approval_gate import (
    build_manual_approval_gate,
    manual_approval_gate_to_markdown,
    write_manual_approval_gate,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _manual_review_payload() -> dict:
    return {
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
    }


def test_gate_blocks_without_explicit_approval_and_forbids_automation(tmp_path):
    manual_review_path = _write_json(tmp_path / "repair_blocker_manual_review.json", _manual_review_payload())

    payload = build_manual_approval_gate(manual_review_path)

    assert payload["gate_status"] == "blocked_pending_manual_approval"
    assert payload["human_approval_present"] is False
    assert payload["risk_relaxation_allowed"] is False
    assert payload["automated_rerun_allowed"] is False
    assert payload["safety"] == {
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
    }
    assert payload["manual_review"]["review_status"] == "blocked_manual_review_required"
    assert payload["required_approval"]["dimension"] == "holding_count"
    assert payload["required_approval"]["value"] == "50"


def test_gate_permits_observational_progress_with_explicit_risk_relaxation_approval(tmp_path):
    manual_review_path = _write_json(tmp_path / "repair_blocker_manual_review.json", _manual_review_payload())
    approval_path = _write_json(
        tmp_path / "approval.json",
        {
            "human_approval_present": True,
            "approved_risk_relaxation": {
                "dimension": "max_drawdown_limit",
                "from": -0.35,
                "to": -0.48,
                "reason": "manual observational override only",
            },
        },
    )

    payload = build_manual_approval_gate(manual_review_path, approval_path)

    assert payload["gate_status"] == "approved_risk_relaxation_observational_only"
    assert payload["human_approval_present"] is True
    assert payload["risk_relaxation_allowed"] is True
    assert payload["automated_rerun_allowed"] is False
    assert payload["safety"]["queue_write_allowed"] is False
    assert payload["safety"]["broad_daemon_allowed"] is False
    assert payload["safety"]["automation_allowed"] is False
    assert payload["approved_risk_relaxation"]["dimension"] == "max_drawdown_limit"


def test_gate_permits_observational_progress_with_explicit_candidate_approval(tmp_path):
    manual_review_path = _write_json(tmp_path / "repair_blocker_manual_review.json", _manual_review_payload())
    approval_path = _write_json(
        tmp_path / "approval.json",
        {
            "human_approval_present": True,
            "approved_candidate": {"combo_id": "manual_safe_candidate", "holding_count": 50},
        },
    )

    payload = build_manual_approval_gate(manual_review_path, approval_path)

    assert payload["gate_status"] == "approved_candidate_observational_only"
    assert payload["approved_candidate"]["combo_id"] == "manual_safe_candidate"
    assert payload["risk_relaxation_allowed"] is False
    assert payload["automated_rerun_allowed"] is False


def test_gate_ignores_empty_or_unsafe_approval(tmp_path):
    manual_review_path = _write_json(tmp_path / "repair_blocker_manual_review.json", _manual_review_payload())
    approval_path = _write_json(
        tmp_path / "approval.json",
        {"human_approval_present": True, "queue_write_allowed": True, "broad_daemon_allowed": True},
    )

    payload = build_manual_approval_gate(manual_review_path, approval_path)

    assert payload["gate_status"] == "blocked_pending_manual_approval"
    assert payload["human_approval_present"] is False
    assert payload["queue_write_allowed"] is False
    assert payload["broad_daemon_allowed"] is False


def test_write_gate_writes_json_and_markdown(tmp_path):
    manual_review_path = _write_json(tmp_path / "repair_blocker_manual_review.json", _manual_review_payload())
    json_path = tmp_path / "manual_approval_gate.json"
    markdown_path = tmp_path / "manual_approval_gate.md"

    payload = write_manual_approval_gate(
        manual_review_path=manual_review_path,
        json_path=json_path,
        markdown_path=markdown_path,
    )

    assert payload["gate_status"] == "blocked_pending_manual_approval"
    assert json.loads(json_path.read_text(encoding="utf-8"))["gate_status"] == "blocked_pending_manual_approval"
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Manual Approval Gate" in markdown
    assert "queue_write_allowed: False" in markdown


def test_manual_approval_gate_markdown_includes_required_approval():
    markdown = manual_approval_gate_to_markdown(
        {
            "generated_at_utc": "2026-06-02T00:00:00+00:00",
            "gate_status": "blocked_pending_manual_approval",
            "human_approval_present": False,
            "risk_relaxation_allowed": False,
            "automated_rerun_allowed": False,
            "required_approval": {"dimension": "holding_count", "value": "50"},
            "safety": {"queue_write_allowed": False, "broad_daemon_allowed": False, "automation_allowed": False},
        }
    )

    assert "blocked_pending_manual_approval" in markdown
    assert "holding_count=50" in markdown
    assert "automated_rerun_allowed: False" in markdown
