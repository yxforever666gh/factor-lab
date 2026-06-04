import json

from factor_lab.paper_monitoring_report import (
    build_paper_monitoring_report,
    paper_monitoring_report_to_markdown,
    write_paper_monitoring_report,
)


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def test_build_paper_monitoring_report_combines_portfolio_diagnostics_status_and_runtime(tmp_path):
    current = _write_json(
        tmp_path / "current_portfolio.json",
        {"strategy_name": "small_institutional_value_sleeve_mvp", "as_of_date": "2021-12-28", "position_count": 72},
    )
    diagnostics = _write_json(
        tmp_path / "portfolio_diagnostics.json",
        {
            "benchmark": {"benchmark_id": "CSI1000", "benchmark_name": "中证1000"},
            "turnover": {"turnover_one_way_estimate": 0.12},
            "cost": {"cost_bps": 30.0, "estimated_one_way_cost": 0.00036, "estimated_round_trip_cost": 0.00072},
        },
    )
    status = _write_json(
        tmp_path / "status.json",
        {"decision": "ready_for_portfolio_mvp", "blockers": [], "runtime_safety": {"safe": True}},
    )
    dry_run = _write_json(tmp_path / "dry_run.json", {"would_run_count": 0})
    runtime_audit = _write_json(tmp_path / "runtime_audit.json", {"recommendations": ["pause_broad_daemon", "allow_controlled_only_daemon"]})

    report = build_paper_monitoring_report(
        current_portfolio_path=current,
        diagnostics_path=diagnostics,
        status_path=status,
        dry_run_path=dry_run,
        runtime_audit_path=runtime_audit,
    )

    assert report["cadence"] == "weekly"
    assert report["portfolio"]["strategy_name"] == "small_institutional_value_sleeve_mvp"
    assert report["portfolio"]["position_count"] == 72
    assert report["benchmark"]["benchmark_id"] == "CSI1000"
    assert report["trading_friction"]["turnover_one_way_estimate"] == 0.12
    assert report["trading_friction"]["estimated_one_way_cost"] == 0.00036
    assert report["trading_friction"]["estimated_round_trip_cost"] == 0.00072
    assert report["trading_friction"]["cost_bps"] == 30.0
    assert report["runtime"]["safe"] is True
    assert report["runtime"]["would_run_count"] == 0
    assert report["next_observation_window"]["calendar"] == "one_week_placeholder"
    assert report["missing_artifacts"] == []


def test_build_paper_monitoring_report_surfaces_drawdown_and_manual_approval_blockers_from_status(tmp_path):
    current = _write_json(
        tmp_path / "current_portfolio.json",
        {"strategy_name": "small_institutional_value_sleeve_mvp", "as_of_date": "2021-12-28", "position_count": 72},
    )
    diagnostics = _write_json(
        tmp_path / "portfolio_diagnostics.json",
        {
            "benchmark": {"benchmark_id": "CSI1000", "benchmark_name": "中证1000", "tracking_mode": "metadata_only"},
            "turnover": {"turnover_one_way_estimate": 0.791672},
            "cost": {"cost_bps": 30.0, "estimated_one_way_cost": 0.002375, "estimated_round_trip_cost": 0.00475},
        },
    )
    status = _write_json(
        tmp_path / "status.json",
        {
            "blockers": [],
            "runtime_safety": {"safe": True},
            "small_institutional_simulation": {"primary_issue": "drawdown_risk_too_high"},
            "manual_approval_gate": {"gate_status": "blocked_pending_manual_approval", "human_approval_present": False},
            "operator_approval_summary": {"summary_status": "blocked_pending_manual_approval", "required_decision_axis": "holding_count=50"},
        },
    )
    dry_run = _write_json(tmp_path / "dry_run.json", {"would_run_count": 0})
    runtime_audit = _write_json(tmp_path / "runtime_audit.json", {"recommendations": ["pause_broad_daemon"]})

    report = build_paper_monitoring_report(
        current_portfolio_path=current,
        diagnostics_path=diagnostics,
        status_path=status,
        dry_run_path=dry_run,
        runtime_audit_path=runtime_audit,
    )

    assert "drawdown_risk_too_high" in report["blockers"]
    assert "manual_approval_pending" in report["blockers"]
    assert report["operator_decision"]["status"] == "blocked_pending_manual_approval"
    assert report["operator_decision"]["required_decision_axis"] == "holding_count=50"


def test_build_paper_monitoring_report_reports_missing_optional_artifacts(tmp_path):
    current = _write_json(tmp_path / "current_portfolio.json", {"strategy_name": "x", "position_count": 1})

    report = build_paper_monitoring_report(
        current_portfolio_path=current,
        diagnostics_path=tmp_path / "missing_diagnostics.json",
        status_path=tmp_path / "missing_status.json",
        dry_run_path=tmp_path / "missing_dry_run.json",
        runtime_audit_path=tmp_path / "missing_runtime_audit.json",
    )

    assert "portfolio_diagnostics" in report["missing_artifacts"]
    assert "small_institutionalization_status" in report["missing_artifacts"]
    assert "controlled_restart_dry_run" in report["missing_artifacts"]
    assert "runtime_takeover_audit" in report["missing_artifacts"]
    assert report["runtime"]["safe"] is False


def test_write_paper_monitoring_report_writes_json_and_markdown(tmp_path):
    current = _write_json(tmp_path / "current_portfolio.json", {"strategy_name": "x", "as_of_date": "2021-12-28", "position_count": 72})
    diagnostics = _write_json(tmp_path / "portfolio_diagnostics.json", {"benchmark": {"benchmark_id": "CSI1000"}, "turnover": {}, "cost": {}})
    status = _write_json(tmp_path / "status.json", {"decision": "ready_for_portfolio_mvp", "blockers": [], "runtime_safety": {"safe": True}})
    dry_run = _write_json(tmp_path / "dry_run.json", {"would_run_count": 0})
    runtime_audit = _write_json(tmp_path / "runtime_audit.json", {"recommendations": ["pause_broad_daemon", "allow_controlled_only_daemon"]})
    json_path = tmp_path / "weekly_monitoring_report.json"
    markdown_path = tmp_path / "weekly_monitoring_report.md"

    payload = write_paper_monitoring_report(
        current_portfolio_path=current,
        diagnostics_path=diagnostics,
        status_path=status,
        dry_run_path=dry_run,
        runtime_audit_path=runtime_audit,
        json_path=json_path,
        markdown_path=markdown_path,
    )

    assert payload["benchmark"]["benchmark_id"] == "CSI1000"
    assert json.loads(json_path.read_text(encoding="utf-8"))["cadence"] == "weekly"
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Weekly Paper Monitoring Report" in markdown
    assert "CSI1000" in markdown


def test_paper_monitoring_report_markdown_includes_runtime_and_next_window():
    markdown = paper_monitoring_report_to_markdown(
        {
            "generated_at_utc": "2026-05-11T00:00:00+00:00",
            "cadence": "weekly",
            "portfolio": {"strategy_name": "x", "as_of_date": "2021-12-28", "position_count": 72},
            "benchmark": {"benchmark_id": "CSI1000", "benchmark_name": "中证1000"},
            "trading_friction": {
                "turnover_one_way_estimate": 0.1,
                "estimated_one_way_cost": 0.0003,
                "estimated_round_trip_cost": 0.0006,
                "cost_bps": 30.0,
            },
            "runtime": {"safe": True, "would_run_count": 0, "recommendations": ["pause_broad_daemon"]},
            "blockers": [],
            "next_observation_window": {"trading_days": 5, "calendar": "one_week_placeholder"},
            "missing_artifacts": [],
        }
    )

    assert "Runtime" in markdown
    assert "Next observation window" in markdown
    assert "Estimated one-way cost: 0.0003" in markdown
    assert "one_week_placeholder" in markdown
