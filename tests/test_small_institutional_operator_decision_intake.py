import json
from pathlib import Path

from factor_lab.small_institutional_operator_decision_intake import (
    operator_decision_intake_to_markdown,
    validate_operator_decision_intake,
    write_operator_decision_intake_validation,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _valid_intake() -> dict:
    return {
        "decision_type": "defer",
        "scope": "small_institutional_simulation holding_count=50 manual review",
        "reason": "Awaiting explicit operator approval; keep observational only.",
        "safety": {
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automation_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        },
    }


def test_validate_operator_decision_intake_missing_artifact_is_non_mutating(tmp_path):
    result = validate_operator_decision_intake(tmp_path / "operator_decision_intake.json")

    assert result["intake_status"] == "missing"
    assert result["validation_errors"] == []
    assert result["non_mutating"] is True
    assert result["safety"] == {
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
    }


def test_validate_operator_decision_intake_accepts_valid_observational_input(tmp_path):
    path = _write_json(tmp_path / "operator_decision_intake.json", _valid_intake())

    result = validate_operator_decision_intake(path)

    assert result["intake_status"] == "valid"
    assert result["decision_type"] == "defer"
    assert result["validation_errors"] == []
    assert result["non_mutating"] is True
    assert result["safety"]["queue_write_allowed"] is False
    assert result["safety"]["broad_daemon_allowed"] is False
    assert result["safety"]["automation_allowed"] is False
    assert result["safety"]["automated_rerun_allowed"] is False
    assert result["safety"]["live_trading_enabled"] is False


def test_validate_operator_decision_intake_rejects_missing_required_fields(tmp_path):
    path = _write_json(
        tmp_path / "operator_decision_intake.json",
        {"decision_type": "defer", "safety": _valid_intake()["safety"]},
    )

    result = validate_operator_decision_intake(path)

    assert result["intake_status"] == "invalid"
    assert "missing_scope" in result["validation_errors"]
    assert "missing_reason" in result["validation_errors"]
    assert result["non_mutating"] is True


def test_validate_operator_decision_intake_rejects_unsafe_flags(tmp_path):
    payload = _valid_intake()
    payload["safety"]["queue_write_allowed"] = True
    payload["safety"]["automation_allowed"] = True
    path = _write_json(tmp_path / "operator_decision_intake.json", payload)

    result = validate_operator_decision_intake(path)

    assert result["intake_status"] == "invalid"
    assert "unsafe_queue_write_allowed" in result["validation_errors"]
    assert "unsafe_automation_allowed" in result["validation_errors"]
    assert result["safety"]["queue_write_allowed"] is False
    assert result["safety"]["automation_allowed"] is False


def test_validate_operator_decision_intake_rejects_unknown_decision_type(tmp_path):
    payload = _valid_intake()
    payload["decision_type"] = "approve_live_trading"
    path = _write_json(tmp_path / "operator_decision_intake.json", payload)

    result = validate_operator_decision_intake(path)

    assert result["intake_status"] == "invalid"
    assert "invalid_decision_type" in result["validation_errors"]


def test_write_operator_decision_intake_validation_writes_json_and_markdown(tmp_path):
    intake_path = _write_json(tmp_path / "operator_decision_intake.json", _valid_intake())
    json_path = tmp_path / "operator_decision_intake_validation.json"
    markdown_path = tmp_path / "operator_decision_intake_validation.md"

    result = write_operator_decision_intake_validation(
        intake_path=intake_path,
        json_path=json_path,
        markdown_path=markdown_path,
    )

    assert result["intake_status"] == "valid"
    assert json.loads(json_path.read_text(encoding="utf-8"))["intake_status"] == "valid"
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Operator Decision Intake Validation" in markdown
    assert "Queue write allowed: False" in markdown


def test_operator_decision_intake_markdown_includes_errors_and_non_mutating():
    markdown = operator_decision_intake_to_markdown(
        {
            "generated_at_utc": "2026-06-03T00:00:00+00:00",
            "intake_status": "invalid",
            "decision_type": "approve_candidate",
            "validation_errors": ["unsafe_queue_write_allowed"],
            "non_mutating": True,
            "safety": {"queue_write_allowed": False, "broad_daemon_allowed": False, "automation_allowed": False, "automated_rerun_allowed": False, "live_trading_enabled": False},
        }
    )

    assert "invalid" in markdown
    assert "unsafe_queue_write_allowed" in markdown
    assert "Non-mutating: True" in markdown
