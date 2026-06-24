import json
from pathlib import Path

from factor_lab.small_institutional_operator_pending_observation import (
    build_operator_pending_observation,
    operator_pending_observation_to_markdown,
    write_operator_pending_observation,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _weekly_report() -> dict:
    return {
        "schema_version": 1,
        "generated_at_utc": "2026-06-04T20:26:32+00:00",
        "cadence": "weekly",
        "portfolio": {
            "strategy_name": "small_institutional_value_sleeve_mvp",
            "as_of_date": "2021-12-28",
            "position_count": 72,
        },
        "benchmark": {"benchmark_id": "CSI1000", "benchmark_name": "中证1000", "tracking_mode": "metadata_only"},
        "turnover": {"history_status": "ok", "turnover_one_way_estimate": 0.791672},
        "cost": {"cost_bps": 30.0, "estimated_round_trip_cost": 0.00475},
        "runtime": {
            "safe": True,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        },
        "missing_artifacts": [],
        "next_observation_window": "next_weekly_paper_review",
    }


def _status() -> dict:
    return {
        "decision": "ready_for_portfolio_mvp",
        "next_action": "repair_simulated_portfolio_construction",
        "small_institutional_simulation": {"primary_issue": "drawdown_risk_too_high"},
        "simulated_portfolio_construction_repair": {"repair_status": "blocked_no_drawdown_safe_candidate"},
        "manual_approval_gate": {
            "gate_status": "blocked_pending_manual_approval",
            "human_approval_present": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        },
        "operator_approval_summary": {
            "approval_required": True,
            "required_decision_axis": "holding_count=50",
            "automation_allowed": False,
        },
    }


def test_operator_pending_observation_includes_portfolio_weekly_freshness_and_blockers(tmp_path):
    weekly_path = _write_json(tmp_path / "weekly_monitoring_report.json", _weekly_report())
    status_path = _write_json(tmp_path / "status.json", _status())

    payload = build_operator_pending_observation(
        weekly_report_path=weekly_path,
        status_path=status_path,
        generated_at="2026-06-05T00:00:00+00:00",
    )

    assert payload["schema_version"] == 1
    assert payload["observation_status"] == "operator_pending"
    assert payload["portfolio"] == {
        "strategy_name": "small_institutional_value_sleeve_mvp",
        "as_of_date": "2021-12-28",
        "position_count": 72,
    }
    assert payload["weekly_report"] == {
        "cadence": "weekly",
        "generated_at_utc": "2026-06-04T20:26:32+00:00",
        "next_observation_window": "next_weekly_paper_review",
    }
    assert payload["blocker"]["primary_issue"] == "drawdown_risk_too_high"
    assert payload["blocker"]["repair_status"] == "blocked_no_drawdown_safe_candidate"
    assert payload["blocker"]["decision_axis"] == "holding_count=50"
    assert payload["blocker"]["manual_approval_status"] == "blocked_pending_manual_approval"
    assert payload["blocker"]["human_approval_present"] is False
    assert payload["missing_artifacts"] == []


def test_operator_pending_observation_includes_benchmark_turnover_cost_context(tmp_path):
    weekly_path = _write_json(tmp_path / "weekly_monitoring_report.json", _weekly_report())
    status_path = _write_json(tmp_path / "status.json", _status())

    payload = build_operator_pending_observation(weekly_path, status_path)

    assert payload["benchmark"]["benchmark_id"] == "CSI1000"
    assert payload["benchmark"]["benchmark_name"] == "中证1000"
    assert payload["benchmark"]["tracking_mode"] == "metadata_only"
    assert payload["turnover"]["turnover_one_way_estimate"] == 0.791672
    assert payload["cost"]["estimated_round_trip_cost"] == 0.00475


def test_operator_pending_observation_marks_missing_inputs_without_crashing(tmp_path):
    payload = build_operator_pending_observation(
        weekly_report_path=tmp_path / "missing_weekly.json",
        status_path=tmp_path / "missing_status.json",
        generated_at="2026-06-05T00:00:00+00:00",
    )

    assert payload["observation_status"] == "missing_artifacts"
    assert payload["portfolio"] == {}
    assert payload["benchmark"] == {}
    assert payload["missing_artifacts"] == ["weekly_monitoring_report", "small_institutionalization_status"]
    assert payload["runtime"]["queue_write_allowed"] is False
    assert payload["runtime"]["broad_daemon_allowed"] is False
    assert payload["runtime"]["automated_rerun_allowed"] is False
    assert payload["runtime"]["live_trading_enabled"] is False


def test_operator_pending_observation_runtime_flags_stay_non_mutating(tmp_path):
    weekly_path = _write_json(tmp_path / "weekly_monitoring_report.json", _weekly_report())
    status_path = _write_json(tmp_path / "status.json", _status())

    payload = build_operator_pending_observation(weekly_path, status_path)

    assert payload["runtime"] == {
        "safe": True,
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automated_rerun_allowed": False,
        "automation_allowed": False,
        "live_trading_enabled": False,
    }
    assert payload["next_action"] == "await_operator_decision_no_automation"


def test_write_operator_pending_observation_outputs_json_and_markdown(tmp_path):
    weekly_path = _write_json(tmp_path / "weekly_monitoring_report.json", _weekly_report())
    status_path = _write_json(tmp_path / "status.json", _status())
    json_path = tmp_path / "operator_pending_observation.json"
    markdown_path = tmp_path / "operator_pending_observation.md"

    payload = write_operator_pending_observation(
        weekly_report_path=weekly_path,
        status_path=status_path,
        json_path=json_path,
        markdown_path=markdown_path,
        generated_at="2026-06-05T00:00:00+00:00",
    )

    assert payload["observation_status"] == "operator_pending"
    assert json.loads(json_path.read_text(encoding="utf-8"))["blocker"]["decision_axis"] == "holding_count=50"
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Operator-Pending Observation" in markdown
    assert "drawdown_risk_too_high" in markdown
    assert "Queue write allowed: False" in markdown


def test_operator_pending_observation_markdown_summarizes_missing_artifacts():
    markdown = operator_pending_observation_to_markdown(
        {
            "generated_at_utc": "2026-06-05T00:00:00+00:00",
            "observation_status": "missing_artifacts",
            "portfolio": {},
            "weekly_report": {},
            "benchmark": {},
            "turnover": {},
            "cost": {},
            "blocker": {},
            "runtime": {"queue_write_allowed": False, "broad_daemon_allowed": False, "automated_rerun_allowed": False, "automation_allowed": False, "live_trading_enabled": False},
            "missing_artifacts": ["weekly_monitoring_report"],
            "next_action": "await_operator_decision_no_automation",
        }
    )

    assert "weekly_monitoring_report" in markdown
    assert "await_operator_decision_no_automation" in markdown
