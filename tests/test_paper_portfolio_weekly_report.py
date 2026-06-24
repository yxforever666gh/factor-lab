import json
from pathlib import Path

from factor_lab.paper_portfolio_weekly_report import (
    build_weekly_paper_report,
    weekly_report_to_markdown,
    write_weekly_paper_report,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _portfolio() -> dict:
    return {
        "strategy_name": "small_institutional_value_sleeve_mvp",
        "as_of_date": "2021-12-28",
        "position_count": 3,
        "positions": [
            {"ticker": "AAA", "signal": 1.5, "weight": 0.02},
            {"ticker": "BBB", "signal": 1.0, "weight": 0.015},
            {"ticker": "CCC", "signal": 0.5, "weight": 0.01},
        ],
    }


def _diagnostics() -> dict:
    return {
        "benchmark": {"benchmark_id": "CSI1000", "benchmark_name": "中证1000", "tracking_mode": "metadata_only"},
        "turnover": {
            "history_status": "ok",
            "added_count": 2,
            "removed_count": 1,
            "overlap_count": 1,
            "turnover_one_way_estimate": 0.25,
        },
        "cost": {"cost_bps": 30.0, "estimated_one_way_cost": 0.00075, "estimated_round_trip_cost": 0.0015},
    }


def _status() -> dict:
    return {
        "decision": "ready_for_portfolio_mvp",
        "next_action": "repair_simulated_portfolio_construction",
        "runtime_safety": {
            "safe": True,
            "would_run_count": 0,
            "recommendations": ["pause_broad_daemon", "allow_controlled_only_daemon"],
        },
        "small_institutional_simulation": {"primary_issue": "drawdown_risk_too_high"},
        "manual_approval_gate": {"gate_status": "blocked_pending_manual_approval", "human_approval_present": False},
        "operator_approval_summary": {"approval_required": True, "required_decision_axis": "holding_count=50"},
    }


def _operator_pending_observation() -> dict:
    return {
        "observation_status": "operator_pending",
        "benchmark": {
            "benchmark_id": "CSI1000",
            "benchmark_name": "中证1000",
            "tracking_mode": "metadata_only",
        },
        "turnover": {"turnover_one_way_estimate": 0.791672},
        "cost": {"estimated_round_trip_cost": 0.00475},
        "blocker": {
            "primary_issue": "drawdown_risk_too_high",
            "manual_approval_status": "blocked_pending_manual_approval",
        },
        "runtime": {
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automation_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        },
    }


def test_weekly_report_includes_portfolio_benchmark_turnover_cost_changes_and_blockers(tmp_path):
    current_path = _write_json(tmp_path / "current_portfolio.json", _portfolio())
    diagnostics_path = _write_json(tmp_path / "portfolio_diagnostics.json", _diagnostics())
    status_path = _write_json(tmp_path / "status.json", _status())

    payload = build_weekly_paper_report(
        current_portfolio_path=current_path,
        diagnostics_path=diagnostics_path,
        status_path=status_path,
        generated_at="2026-06-04T00:00:00+00:00",
    )

    assert payload["schema_version"] == 1
    assert payload["cadence"] == "weekly"
    assert payload["portfolio"] == {
        "strategy_name": "small_institutional_value_sleeve_mvp",
        "as_of_date": "2021-12-28",
        "position_count": 3,
    }
    assert payload["benchmark"]["benchmark_id"] == "CSI1000"
    assert payload["benchmark"]["benchmark_name"] == "中证1000"
    assert payload["benchmark"]["tracking_mode"] == "metadata_only"
    assert payload["turnover"]["turnover_one_way_estimate"] == 0.25
    assert payload["cost"]["estimated_round_trip_cost"] == 0.0015
    assert payload["changes"] == {"history_status": "ok", "added_count": 2, "removed_count": 1, "overlap_count": 1}
    assert payload["top_positions"] == [
        {"ticker": "AAA", "weight": 0.02, "signal": 1.5},
        {"ticker": "BBB", "weight": 0.015, "signal": 1.0},
        {"ticker": "CCC", "weight": 0.01, "signal": 0.5},
    ]
    assert payload["blockers"]["primary_issue"] == "drawdown_risk_too_high"
    assert payload["blockers"]["manual_approval_gate_status"] == "blocked_pending_manual_approval"
    assert payload["blockers"]["required_decision_axis"] == "holding_count=50"
    assert payload["missing_artifacts"] == []


def test_weekly_report_includes_operator_pending_observation_metadata(tmp_path):
    current_path = _write_json(tmp_path / "current_portfolio.json", _portfolio())
    diagnostics_path = _write_json(tmp_path / "portfolio_diagnostics.json", _diagnostics())
    status_path = _write_json(tmp_path / "status.json", _status())
    observation_path = _write_json(tmp_path / "operator_pending_observation.json", _operator_pending_observation())

    payload = build_weekly_paper_report(
        current_portfolio_path=current_path,
        diagnostics_path=diagnostics_path,
        status_path=status_path,
        operator_pending_observation_path=observation_path,
        generated_at="2026-06-04T00:00:00+00:00",
    )

    assert payload["operator_pending_observation"] == {
        "observation_status": "operator_pending",
        "primary_issue": "drawdown_risk_too_high",
        "manual_approval_status": "blocked_pending_manual_approval",
        "benchmark_id": "CSI1000",
        "benchmark_name": "中证1000",
        "tracking_mode": "metadata_only",
        "turnover_one_way_estimate": 0.791672,
        "estimated_round_trip_cost": 0.00475,
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
    }
    assert payload["runtime"]["queue_write_allowed"] is False
    assert payload["runtime"]["broad_daemon_allowed"] is False
    assert payload["runtime"]["automation_allowed"] is False
    assert payload["runtime"]["automated_rerun_allowed"] is False
    assert payload["runtime"]["live_trading_enabled"] is False


def test_weekly_report_operator_pending_observation_runtime_flags_remain_false_for_mutating_actions(tmp_path):
    current_path = _write_json(tmp_path / "current_portfolio.json", _portfolio())
    diagnostics_path = _write_json(tmp_path / "portfolio_diagnostics.json", _diagnostics())
    status_path = _write_json(tmp_path / "status.json", _status())
    observation = _operator_pending_observation()
    observation.update(
        {
            "queue_write_allowed": True,
            "broad_daemon_allowed": True,
            "automation_allowed": True,
            "automated_rerun_allowed": True,
            "live_trading_enabled": True,
        }
    )
    observation["runtime"] = {
        "queue_write_allowed": True,
        "broad_daemon_allowed": True,
        "automation_allowed": True,
        "automated_rerun_allowed": True,
        "live_trading_enabled": True,
    }
    observation_path = _write_json(tmp_path / "operator_pending_observation.json", observation)

    payload = build_weekly_paper_report(
        current_portfolio_path=current_path,
        diagnostics_path=diagnostics_path,
        status_path=status_path,
        operator_pending_observation_path=observation_path,
        generated_at="2026-06-04T00:00:00+00:00",
    )

    operator_pending = payload["operator_pending_observation"]
    assert operator_pending["queue_write_allowed"] is False
    assert operator_pending["broad_daemon_allowed"] is False
    assert operator_pending["automation_allowed"] is False
    assert operator_pending["automated_rerun_allowed"] is False
    assert operator_pending["live_trading_enabled"] is False

    markdown = weekly_report_to_markdown(payload)
    section = markdown.split("## Operator-pending observation", maxsplit=1)[1].split("## Runtime safety", maxsplit=1)[0]
    assert "- queue_write_allowed: False" in section
    assert "- broad_daemon_allowed: False" in section
    assert "- automation_allowed: False" in section
    assert "- automated_rerun_allowed: False" in section
    assert "- live_trading_enabled: False" in section
    assert "- benchmark_name: 中证1000" in section
    assert "- tracking_mode: metadata_only" in section
    assert "- queue_write_allowed: True" not in section
    assert "- broad_daemon_allowed: True" not in section
    assert "- automation_allowed: True" not in section
    assert "- automated_rerun_allowed: True" not in section
    assert "- live_trading_enabled: True" not in section


def test_weekly_report_marks_missing_artifacts_without_crashing(tmp_path):
    current_path = _write_json(tmp_path / "current_portfolio.json", _portfolio())

    payload = build_weekly_paper_report(
        current_portfolio_path=current_path,
        diagnostics_path=tmp_path / "missing_diagnostics.json",
        status_path=tmp_path / "missing_status.json",
        operator_pending_observation_path=tmp_path / "missing_operator_pending_observation.json",
        generated_at="2026-06-04T00:00:00+00:00",
    )

    assert payload["portfolio"]["position_count"] == 3
    assert payload["benchmark"] == {}
    assert payload["turnover"] == {}
    assert payload["cost"] == {}
    assert payload["operator_pending_observation"] == {"observation_status": "missing"}
    assert payload["missing_artifacts"] == [
        "portfolio_diagnostics",
        "small_institutionalization_status",
        "operator_pending_observation",
    ]

    markdown = weekly_report_to_markdown(payload)
    missing_section = markdown.split("## Missing artifacts", maxsplit=1)[1].split(
        "Next observation window", maxsplit=1
    )[0]
    assert "- portfolio_diagnostics" in missing_section
    assert "- small_institutionalization_status" in missing_section
    assert "- operator_pending_observation" in missing_section
    assert "- None" not in missing_section
    runtime_section = markdown.split("## Runtime safety", maxsplit=1)[1].split("## Missing artifacts", maxsplit=1)[0]
    assert "- Queue write allowed: False" in runtime_section
    assert "- Broad daemon allowed: False" in runtime_section
    assert "- Automation allowed: False" in runtime_section
    assert "- Automated rerun allowed: False" in runtime_section
    assert "- Live trading enabled: False" in runtime_section


def test_weekly_report_runtime_flags_remain_false_for_mutating_actions(tmp_path):
    current_path = _write_json(tmp_path / "current_portfolio.json", _portfolio())
    diagnostics_path = _write_json(tmp_path / "portfolio_diagnostics.json", _diagnostics())
    status = _status()
    status["manual_approval_gate"].update(
        {
            "queue_write_allowed": True,
            "broad_daemon_allowed": True,
            "automation_allowed": True,
            "automated_rerun_allowed": True,
            "live_trading_enabled": True,
        }
    )
    status["operator_approval_summary"].update(
        {
            "queue_write_allowed": True,
            "broad_daemon_allowed": True,
            "automation_allowed": True,
            "automated_rerun_allowed": True,
            "live_trading_enabled": True,
        }
    )
    status["operator_decision_handoff"] = {
        "queue_write_allowed": True,
        "broad_daemon_allowed": True,
        "automation_allowed": True,
        "automated_rerun_allowed": True,
        "live_trading_enabled": True,
    }
    status_path = _write_json(tmp_path / "status.json", status)

    payload = build_weekly_paper_report(current_path, diagnostics_path, status_path)

    assert payload["runtime"] == {
        "safe": True,
        "would_run_count": 0,
        "recommendations": ["pause_broad_daemon", "allow_controlled_only_daemon"],
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
    }

    markdown = weekly_report_to_markdown(payload)
    runtime_section = markdown.split("## Runtime safety", maxsplit=1)[1].split("## Missing artifacts", maxsplit=1)[0]
    assert "- Queue write allowed: False" in runtime_section
    assert "- Broad daemon allowed: False" in runtime_section
    assert "- Automation allowed: False" in runtime_section
    assert "- Automated rerun allowed: False" in runtime_section
    assert "- Live trading enabled: False" in runtime_section
    assert "- Queue write allowed: True" not in runtime_section
    assert "- Broad daemon allowed: True" not in runtime_section
    assert "- Automation allowed: True" not in runtime_section
    assert "- Automated rerun allowed: True" not in runtime_section
    assert "- Live trading enabled: True" not in runtime_section



def test_write_weekly_paper_report_writes_json_and_markdown(tmp_path):
    current_path = _write_json(tmp_path / "current_portfolio.json", _portfolio())
    diagnostics_path = _write_json(tmp_path / "portfolio_diagnostics.json", _diagnostics())
    status_path = _write_json(tmp_path / "status.json", _status())
    json_path = tmp_path / "weekly_monitoring_report.json"
    markdown_path = tmp_path / "weekly_monitoring_report.md"

    payload = write_weekly_paper_report(
        current_portfolio_path=current_path,
        diagnostics_path=diagnostics_path,
        status_path=status_path,
        json_path=json_path,
        markdown_path=markdown_path,
        generated_at="2026-06-04T00:00:00+00:00",
    )

    assert payload["cadence"] == "weekly"
    assert json.loads(json_path.read_text(encoding="utf-8"))["benchmark"]["benchmark_id"] == "CSI1000"
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Weekly Paper Monitoring Report" in markdown
    assert "drawdown_risk_too_high" in markdown


def test_weekly_report_markdown_includes_next_observation_window():
    markdown = weekly_report_to_markdown(
        {
            "generated_at_utc": "2026-06-04T00:00:00+00:00",
            "cadence": "weekly",
            "portfolio": {"strategy_name": "s", "as_of_date": "d", "position_count": 1},
            "benchmark": {"benchmark_id": "CSI1000", "benchmark_name": "中证1000", "tracking_mode": "metadata_only"},
            "turnover": {"turnover_one_way_estimate": 0.25},
            "cost": {"estimated_round_trip_cost": 0.0015},
            "changes": {"added_count": 2, "removed_count": 1, "overlap_count": 1},
            "top_positions": [{"ticker": "AAA", "weight": 0.02, "signal": 1.5}],
            "blockers": {"primary_issue": "drawdown_risk_too_high", "next_action": "repair_simulated_portfolio_construction"},
            "runtime": {"queue_write_allowed": False, "broad_daemon_allowed": False, "automation_allowed": False, "automated_rerun_allowed": False, "live_trading_enabled": False},
            "missing_artifacts": [],
            "next_observation_window": "next_weekly_paper_review",
        }
    )

    assert "next_weekly_paper_review" in markdown
    assert "Queue write allowed: False" in markdown
    assert "Automation allowed: False" in markdown
