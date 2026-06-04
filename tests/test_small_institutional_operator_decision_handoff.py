import json
from pathlib import Path

from factor_lab.small_institutional_operator_decision_handoff import (
    build_operator_decision_handoff,
    operator_decision_handoff_to_markdown,
    write_operator_decision_handoff,
)

SAFE_FLAGS = {
    "queue_write_allowed": False,
    "broad_daemon_allowed": False,
    "automation_allowed": False,
    "automated_rerun_allowed": False,
    "live_trading_enabled": False,
}


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _intake(status: str, decision_type=None, errors=None) -> dict:
    return {
        "schema_version": 1,
        "intake_status": status,
        "decision_type": decision_type,
        "scope": "small institutional manual review",
        "reason": "operator decision recorded for handoff only",
        "validation_errors": errors or [],
        "non_mutating": True,
        "safety": dict(SAFE_FLAGS),
    }


def _gate() -> dict:
    return {
        "gate_status": "blocked_pending_manual_approval",
        "human_approval_present": False,
        "required_approval": {"dimension": "holding_count", "value": "50"},
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
    }


def _manual_review() -> dict:
    return {
        "review_status": "blocked_manual_review_required",
        "primary_issue": "drawdown_risk_too_high",
        "repair_status": "blocked_no_drawdown_safe_candidate",
        "candidate_count": 0,
        "recommended_manual_decision": {"dimension": "holding_count", "value": "50"},
        "safety": {"queue_write_allowed": False, "broad_daemon_allowed": False, "automation_allowed": False},
    }


def _consistency() -> dict:
    return {
        "consistency_status": "ok",
        "non_mutating": True,
        "matched_fields": {"decision_axis": "holding_count=50"},
        "safety_flags": dict(SAFE_FLAGS),
        "inconsistencies": [],
        "staleness_warnings": [],
    }


def _paths(tmp_path: Path, intake_payload: dict):
    return {
        "intake_validation_path": _write_json(tmp_path / "operator_decision_intake_validation.json", intake_payload),
        "manual_approval_gate_path": _write_json(tmp_path / "manual_approval_gate.json", _gate()),
        "repair_blocker_review_path": _write_json(tmp_path / "repair_blocker_manual_review.json", _manual_review()),
        "approval_consistency_path": _write_json(tmp_path / "approval_artifact_consistency.json", _consistency()),
    }


def test_missing_intake_validation_awaits_operator_decision(tmp_path):
    result = build_operator_decision_handoff(**_paths(tmp_path, _intake("missing")))

    assert result["handoff_status"] == "awaiting_operator_decision"
    assert result["decision_type"] is None
    assert result["primary_blocker"] == "drawdown_risk_too_high"
    assert result["decision_axis"] == "holding_count=50"
    assert result["non_mutating"] is True
    assert result["safety"] == SAFE_FLAGS


def test_valid_defer_or_reject_remains_observational(tmp_path):
    for decision_type in ("defer", "reject"):
        result = build_operator_decision_handoff(**_paths(tmp_path / decision_type, _intake("valid", decision_type)))

        assert result["handoff_status"] == "operator_decision_recorded_observational_only"
        assert result["decision_type"] == decision_type
        assert result["execution_allowed"] is False
        assert result["safety"] == SAFE_FLAGS


def test_invalid_validation_blocks_with_errors(tmp_path):
    result = build_operator_decision_handoff(**_paths(tmp_path, _intake("invalid", "approve_candidate", ["unsafe_queue_write_allowed"])))

    assert result["handoff_status"] == "blocked_invalid_operator_decision"
    assert result["validation_errors"] == ["unsafe_queue_write_allowed"]
    assert result["execution_allowed"] is False
    assert result["safety"] == SAFE_FLAGS


def test_valid_approval_decisions_require_separate_execution_plan(tmp_path):
    for decision_type in ("approve_candidate", "approve_risk_relaxation"):
        result = build_operator_decision_handoff(**_paths(tmp_path / decision_type, _intake("valid", decision_type)))

        assert result["handoff_status"] == "manual_decision_recorded_requires_separate_execution_plan"
        assert result["decision_type"] == decision_type
        assert result["separate_execution_plan_required"] is True
        assert result["execution_allowed"] is False
        assert result["safety"] == SAFE_FLAGS


def test_write_operator_decision_handoff_writes_json_and_markdown(tmp_path):
    json_path = tmp_path / "operator_decision_handoff.json"
    markdown_path = tmp_path / "operator_decision_handoff.md"

    result = write_operator_decision_handoff(
        **_paths(tmp_path, _intake("missing")),
        json_path=json_path,
        markdown_path=markdown_path,
    )

    assert result["handoff_status"] == "awaiting_operator_decision"
    assert json.loads(json_path.read_text(encoding="utf-8"))["handoff_status"] == "awaiting_operator_decision"
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Operator Decision Handoff" in markdown
    assert "queue_write_allowed: False" in markdown


def test_operator_decision_handoff_markdown_includes_errors_and_safety():
    markdown = operator_decision_handoff_to_markdown(
        {
            "generated_at_utc": "2026-06-03T00:00:00+00:00",
            "handoff_status": "blocked_invalid_operator_decision",
            "decision_type": "approve_candidate",
            "primary_blocker": "drawdown_risk_too_high",
            "decision_axis": "holding_count=50",
            "validation_errors": ["unsafe_queue_write_allowed"],
            "safety": dict(SAFE_FLAGS),
        }
    )

    assert "blocked_invalid_operator_decision" in markdown
    assert "unsafe_queue_write_allowed" in markdown
    assert "queue_write_allowed: False" in markdown
