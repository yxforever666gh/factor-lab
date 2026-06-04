import json
from pathlib import Path

from factor_lab.small_institutional_operator_approval_packet import (
    build_operator_approval_packet,
    operator_approval_packet_to_markdown,
    write_operator_approval_packet,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _artifact_paths(tmp_path: Path) -> dict[str, Path]:
    status_path = _write_json(
        tmp_path / "status.json",
        {
            "paper_portfolio": {
                "strategy_name": "small_institutional_value_sleeve_mvp",
                "as_of_date": "2021-12-28",
                "position_count": 72,
                "benchmark_id": "CSI1000",
                "turnover_one_way_estimate": 0.791672,
                "estimated_round_trip_cost": 0.00475,
            },
            "paper_monitoring": {"weekly_report_status": "ready", "missing_artifacts": [], "runtime_safe": True},
            "next_action": "repair_simulated_portfolio_construction",
        },
    )
    diagnostics_path = _write_json(
        tmp_path / "portfolio_diagnostics.json",
        {
            "benchmark": {"benchmark_id": "CSI1000", "benchmark_name": "中证1000", "tracking_mode": "metadata_only"},
            "turnover": {"added_count": 57, "removed_count": 0, "overlap_count": 15, "turnover_one_way_estimate": 0.791672},
            "cost": {"cost_bps": 30.0, "estimated_one_way_cost": 0.002375, "estimated_round_trip_cost": 0.00475},
        },
    )
    blocker_path = _write_json(
        tmp_path / "drawdown_blocker_evidence.json",
        {
            "blocker": {"primary_issue": "drawdown_risk_too_high"},
            "repair": {"repair_status": "blocked_no_drawdown_safe_candidate", "candidate_count": 0},
            "manual_review": {"dimension": "holding_count", "value": "50"},
            "safety": {"queue_write_allowed": False, "broad_daemon_allowed": False},
        },
    )
    manual_review_path = _write_json(
        tmp_path / "repair_blocker_manual_review.json",
        {
            "best_available_max_drawdown": -0.478256,
            "drawdown_gap_to_limit": 0.128256,
            "automation_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        },
    )
    intake_path = _write_json(
        tmp_path / "operator_decision_intake_validation.json",
        {"intake_status": "missing", "validation_errors": [], "non_mutating": True},
    )
    handoff_path = _write_json(
        tmp_path / "operator_decision_handoff.json",
        {"handoff_status": "awaiting_operator_decision", "execution_allowed": False, "separate_execution_plan_required": False},
    )
    return {
        "status_path": status_path,
        "diagnostics_path": diagnostics_path,
        "drawdown_blocker_evidence_path": blocker_path,
        "manual_review_path": manual_review_path,
        "intake_validation_path": intake_path,
        "handoff_path": handoff_path,
    }


def test_build_operator_approval_packet_compacts_portfolio_blocker_intake_and_safety(tmp_path):
    payload = build_operator_approval_packet(**_artifact_paths(tmp_path))

    assert payload["packet_status"] == "ready"
    assert payload["missing_artifacts"] == []
    assert payload["portfolio"]["strategy_name"] == "small_institutional_value_sleeve_mvp"
    assert payload["portfolio"]["position_count"] == 72
    assert payload["benchmark"]["benchmark_id"] == "CSI1000"
    assert payload["benchmark"]["benchmark_name"] == "中证1000"
    assert payload["turnover_cost"]["turnover_one_way_estimate"] == 0.791672
    assert payload["turnover_cost"]["estimated_one_way_cost"] == 0.002375
    assert payload["turnover_cost"]["estimated_round_trip_cost"] == 0.00475
    assert payload["paper_monitoring"]["weekly_report_status"] == "ready"
    assert payload["drawdown_blocker"]["primary_issue"] == "drawdown_risk_too_high"
    assert payload["drawdown_blocker"]["repair_status"] == "blocked_no_drawdown_safe_candidate"
    assert payload["drawdown_blocker"]["candidate_count"] == 0
    assert payload["drawdown_blocker"]["best_available_max_drawdown"] == -0.478256
    assert payload["drawdown_blocker"]["drawdown_gap_to_limit"] == 0.128256
    assert payload["drawdown_blocker"]["decision_axis"] == "holding_count=50"
    assert payload["operator_state"]["intake_status"] == "missing"
    assert payload["operator_state"]["handoff_status"] == "awaiting_operator_decision"
    assert payload["operator_state"]["non_mutating"] is True
    assert payload["safety"] == {
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
        "execution_allowed": False,
    }


def test_build_operator_approval_packet_marks_missing_optional_artifacts_incomplete(tmp_path):
    paths = _artifact_paths(tmp_path)
    paths["diagnostics_path"].unlink()

    payload = build_operator_approval_packet(**paths)

    assert payload["packet_status"] == "incomplete"
    assert "diagnostics" in payload["missing_artifacts"]
    assert payload["operator_state"]["non_mutating"] is True
    assert payload["safety"]["queue_write_allowed"] is False
    assert payload["safety"]["execution_allowed"] is False


def test_operator_approval_packet_markdown_includes_required_sections(tmp_path):
    markdown = operator_approval_packet_to_markdown(build_operator_approval_packet(**_artifact_paths(tmp_path)))

    assert "Operator Approval Packet" in markdown
    assert "Portfolio" in markdown
    assert "Benchmark" in markdown
    assert "Turnover and cost" in markdown
    assert "Paper monitoring" in markdown
    assert "Current blocker" in markdown
    assert "Operator state" in markdown
    assert "Safety flags" in markdown
    assert "holding_count=50" in markdown
    assert "drawdown_risk_too_high" in markdown


def test_write_operator_approval_packet_writes_json_and_markdown(tmp_path):
    paths = _artifact_paths(tmp_path)
    json_path = tmp_path / "operator_approval_packet.json"
    markdown_path = tmp_path / "operator_approval_packet.md"

    payload = write_operator_approval_packet(json_path=json_path, markdown_path=markdown_path, **paths)

    assert payload["packet_status"] == "ready"
    assert json.loads(json_path.read_text(encoding="utf-8"))["drawdown_blocker"]["decision_axis"] == "holding_count=50"
    assert "Safety flags" in markdown_path.read_text(encoding="utf-8")
