import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from numbers import Number
from pathlib import Path

import pytest

import scripts.write_small_institutionalization_status as status_cli

from factor_lab.small_institutional_operator_pending_consistency_snapshot import (
    build_operator_pending_consistency_snapshot,
    operator_pending_consistency_snapshot_to_markdown,
    write_operator_pending_consistency_snapshot,
    write_status_then_operator_pending_consistency_snapshot,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _status_payload() -> dict:
    return {
        "generated_at_utc": "2026-06-05T06:54:39+00:00",
        "paper_monitoring": {
            "weekly_report_status": "ready",
            "operator_pending_observation": {
                "observation_status": "operator_pending",
                "primary_issue": "drawdown_risk_too_high",
                "manual_approval_status": "blocked_pending_manual_approval",
                "benchmark_id": "CSI1000",
                "turnover_one_way_estimate": 0.791672,
                "estimated_round_trip_cost": 0.00475,
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
            "operator_pending_consistency": {
                "consistency_status": "ok",
                "mismatches": [],
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
        "operator_pending_observation": {
            "observation_status": "operator_pending",
            "primary_issue": "drawdown_risk_too_high",
            "manual_approval_status": "blocked_pending_manual_approval",
            "benchmark_id": "CSI1000",
            "turnover_one_way_estimate": 0.791672,
            "estimated_round_trip_cost": 0.00475,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automation_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
        },
        "simulated_portfolio_construction_repair": {
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "recommended_candidate": None,
            "automation_allowed": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
            "best_available_max_drawdown": -0.478256,
            "drawdown_gap_to_limit": 0.128256,
        },
    }


def test_snapshot_copies_consistency_context_and_runtime_flags(tmp_path):
    status_path = _write_json(tmp_path / "status.json", _status_payload())

    payload = build_operator_pending_consistency_snapshot(
        status_path=status_path,
        generated_at="2026-06-05T08:00:00+00:00",
    )

    assert payload["schema_version"] == 1
    assert payload["snapshot_status"] == "ready"
    assert payload["source_status_generated_at_utc"] == "2026-06-05T06:54:39+00:00"
    assert payload["consistency_status"] == "ok"
    assert payload["mismatches"] == []
    assert payload["weekly_operator_pending"]["observation_status"] == "operator_pending"
    assert payload["canonical_operator_pending"]["observation_status"] == "operator_pending"
    assert payload["benchmark_id"] == "CSI1000"
    assert payload["turnover_one_way_estimate"] == 0.791672
    assert payload["estimated_round_trip_cost"] == 0.00475
    assert payload["runtime"] == {
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automation_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
    }


def test_snapshot_marks_missing_consistency_without_crashing(tmp_path):
    status = _status_payload()
    del status["paper_monitoring"]["operator_pending_consistency"]
    status_path = _write_json(tmp_path / "status.json", status)

    payload = build_operator_pending_consistency_snapshot(status_path=status_path)

    assert payload["snapshot_status"] == "missing_consistency"
    assert payload["consistency_status"] is None
    assert payload["mismatches"] == []
    assert payload["runtime"]["queue_write_allowed"] is False
    assert payload["runtime"]["broad_daemon_allowed"] is False
    assert payload["runtime"]["automation_allowed"] is False
    assert payload["runtime"]["automated_rerun_allowed"] is False
    assert payload["runtime"]["live_trading_enabled"] is False


def test_write_snapshot_outputs_json_and_markdown(tmp_path):
    status_path = _write_json(tmp_path / "status.json", _status_payload())
    json_path = tmp_path / "operator_pending_consistency_snapshot.json"
    markdown_path = tmp_path / "operator_pending_consistency_snapshot.md"

    payload = write_operator_pending_consistency_snapshot(
        status_path=status_path,
        json_path=json_path,
        markdown_path=markdown_path,
        generated_at="2026-06-05T08:00:00+00:00",
    )

    assert payload["snapshot_status"] == "ready"
    assert json.loads(json_path.read_text(encoding="utf-8"))["consistency_status"] == "ok"
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Operator-Pending Consistency Snapshot" in markdown
    assert "Consistency status: ok" in markdown
    assert "Queue write allowed: False" in markdown


def test_snapshot_markdown_lists_mismatches():
    markdown = operator_pending_consistency_snapshot_to_markdown(
        {
            "generated_at_utc": "2026-06-05T08:00:00+00:00",
            "snapshot_status": "ready",
            "consistency_status": "mismatch",
            "mismatches": ["benchmark_id"],
            "weekly_operator_pending": {"observation_status": "operator_pending"},
            "canonical_operator_pending": {"observation_status": "operator_pending"},
            "benchmark_id": "CSI1000",
            "turnover_one_way_estimate": 0.791672,
            "estimated_round_trip_cost": 0.00475,
            "runtime": {"queue_write_allowed": False, "broad_daemon_allowed": False, "automation_allowed": False, "automated_rerun_allowed": False, "live_trading_enabled": False},
        }
    )

    assert "benchmark_id" in markdown
    assert "mismatch" in markdown


def test_guarded_status_snapshot_refresh_order_leaves_final_status_fresh(tmp_path):
    """A standalone status rewrite after a snapshot can stale the next read; this helper documents the safe write order."""
    status_path = _write_json(tmp_path / "status.json", _status_payload())
    json_path = tmp_path / "operator_pending_consistency_snapshot.json"
    markdown_path = tmp_path / "operator_pending_consistency_snapshot.md"
    status_markdown_path = tmp_path / "status.md"
    knowledge_path = tmp_path / "knowledge.md"

    result = write_status_then_operator_pending_consistency_snapshot(
        status_json_path=status_path,
        status_markdown_path=status_markdown_path,
        status_knowledge_path=knowledge_path,
        snapshot_json_path=json_path,
        snapshot_markdown_path=markdown_path,
        status_kwargs={
            "policy_path": tmp_path / "missing_policy.json",
            "dry_run_path": tmp_path / "missing_dry_run.json",
            "runtime_audit_path": tmp_path / "missing_runtime_audit.json",
            "portfolio_diagnostics_path": tmp_path / "missing_portfolio_diagnostics.json",
            "weekly_monitoring_report_path": status_path,
            "operator_pending_observation_path": status_path,
        },
    )

    snapshot = result["status"]["paper_monitoring"]["operator_pending_consistency_snapshot"]
    assert snapshot["snapshot_freshness_status"] == "fresh"
    assert snapshot["source_status_generated_at_utc"] == result["snapshot"]["source_status_generated_at_utc"]
    assert snapshot["latest_status_generated_at_utc"] == result["snapshot"]["source_status_generated_at_utc"]
    assert snapshot["mismatches"] == []
    assert snapshot["benchmark_id"] == "CSI1000"
    assert snapshot["turnover_one_way_estimate"] == 0.791672
    assert snapshot["estimated_round_trip_cost"] == 0.00475
    assert snapshot["queue_write_allowed"] is False
    assert snapshot["broad_daemon_allowed"] is False
    assert snapshot["automation_allowed"] is False
    assert snapshot["automated_rerun_allowed"] is False
    assert snapshot["live_trading_enabled"] is False


def test_two_consecutive_guarded_status_refreshes_remain_metadata_only_and_fresh(tmp_path):
    status_path = _write_json(tmp_path / "status.json", _status_payload())
    operator_pending = _status_payload()["operator_pending_observation"]
    weekly_path = _write_json(
        tmp_path / "weekly_report.json",
        {
            "cadence": "weekly",
            "missing_artifacts": [],
            "runtime": {"safe": True},
            "operator_pending_observation": operator_pending,
        },
    )
    canonical_path = _write_json(
        tmp_path / "operator_pending_observation.json",
        {
            "observation_status": "operator_pending",
            "blocker": {
                "primary_issue": operator_pending["primary_issue"],
                "manual_approval_status": operator_pending["manual_approval_status"],
            },
            "benchmark": {"benchmark_id": operator_pending["benchmark_id"]},
            "turnover": {"turnover_one_way_estimate": operator_pending["turnover_one_way_estimate"]},
            "cost": {"estimated_round_trip_cost": operator_pending["estimated_round_trip_cost"]},
            "runtime": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )
    json_path = tmp_path / "operator_pending_consistency_snapshot.json"
    markdown_path = tmp_path / "operator_pending_consistency_snapshot.md"
    status_markdown_path = tmp_path / "status.md"
    knowledge_path = tmp_path / "knowledge.md"
    status_kwargs = {
        "policy_path": tmp_path / "missing_policy.json",
        "dry_run_path": tmp_path / "missing_dry_run.json",
        "runtime_audit_path": tmp_path / "missing_runtime_audit.json",
        "portfolio_diagnostics_path": tmp_path / "missing_portfolio_diagnostics.json",
        "simulated_portfolio_construction_repair_path": tmp_path / "missing_repair.json",
        "weekly_monitoring_report_path": weekly_path,
        "operator_pending_observation_path": canonical_path,
    }

    first = write_status_then_operator_pending_consistency_snapshot(
        status_json_path=status_path,
        status_markdown_path=status_markdown_path,
        status_knowledge_path=knowledge_path,
        snapshot_json_path=json_path,
        snapshot_markdown_path=markdown_path,
        status_kwargs=status_kwargs,
    )
    second = write_status_then_operator_pending_consistency_snapshot(
        status_json_path=status_path,
        status_markdown_path=status_markdown_path,
        status_knowledge_path=knowledge_path,
        snapshot_json_path=json_path,
        snapshot_markdown_path=markdown_path,
        status_kwargs=status_kwargs,
    )

    first_snapshot = first["status"]["paper_monitoring"]["operator_pending_consistency_snapshot"]
    second_snapshot = second["status"]["paper_monitoring"]["operator_pending_consistency_snapshot"]
    assert first_snapshot["snapshot_freshness_status"] == "fresh"
    assert second_snapshot["snapshot_freshness_status"] == "fresh"
    assert second_snapshot["source_status_generated_at_utc"] == second["snapshot"]["source_status_generated_at_utc"]
    assert second_snapshot["latest_status_generated_at_utc"] == second["snapshot"]["source_status_generated_at_utc"]
    assert json.loads(status_path.read_text(encoding="utf-8"))["paper_monitoring"][
        "operator_pending_consistency_snapshot"
    ]["snapshot_freshness_status"] == "fresh"
    assert second_snapshot["mismatches"] == []
    assert second_snapshot["benchmark_id"] == "CSI1000"
    assert second_snapshot["turnover_one_way_estimate"] == 0.791672
    assert second_snapshot["estimated_round_trip_cost"] == 0.00475
    assert second_snapshot["queue_write_allowed"] is False
    assert second_snapshot["broad_daemon_allowed"] is False
    assert second_snapshot["automation_allowed"] is False
    assert second_snapshot["automated_rerun_allowed"] is False
    assert second_snapshot["live_trading_enabled"] is False


def test_status_writer_cli_main_repeated_refreshes_keep_snapshot_fresh(monkeypatch, tmp_path, capsys):
    """Exercise the script's default entry point with patched default paths, twice."""
    status_path = _write_json(tmp_path / "status.json", _status_payload())
    operator_pending = _status_payload()["operator_pending_observation"]
    weekly_path = _write_json(
        tmp_path / "weekly_report.json",
        {
            "cadence": "weekly",
            "missing_artifacts": [],
            "runtime": {"safe": True},
            "operator_pending_observation": operator_pending,
        },
    )
    canonical_path = _write_json(
        tmp_path / "operator_pending_observation.json",
        {
            "observation_status": "operator_pending",
            "blocker": {
                "primary_issue": operator_pending["primary_issue"],
                "manual_approval_status": operator_pending["manual_approval_status"],
            },
            "benchmark": {"benchmark_id": operator_pending["benchmark_id"]},
            "turnover": {"turnover_one_way_estimate": operator_pending["turnover_one_way_estimate"]},
            "cost": {"estimated_round_trip_cost": operator_pending["estimated_round_trip_cost"]},
            "runtime": {
                "queue_write_allowed": False,
                "broad_daemon_allowed": False,
                "automation_allowed": False,
                "automated_rerun_allowed": False,
                "live_trading_enabled": False,
            },
        },
    )

    monkeypatch.setattr(status_cli, "DEFAULT_STATUS_JSON_PATH", status_path)
    monkeypatch.setattr(status_cli, "DEFAULT_STATUS_MD_PATH", tmp_path / "status.md")
    monkeypatch.setattr(status_cli, "DEFAULT_KNOWLEDGE_PATH", tmp_path / "knowledge.md")
    monkeypatch.setattr(
        status_cli,
        "DEFAULT_OPERATOR_PENDING_CONSISTENCY_SNAPSHOT_PATH",
        tmp_path / "operator_pending_consistency_snapshot.json",
    )
    monkeypatch.setattr(
        status_cli,
        "DEFAULT_OPERATOR_PENDING_CONSISTENCY_SNAPSHOT_MD_PATH",
        tmp_path / "operator_pending_consistency_snapshot.md",
    )

    status_kwargs = {
        "policy_path": tmp_path / "missing_policy.json",
        "dry_run_path": tmp_path / "missing_dry_run.json",
        "runtime_audit_path": tmp_path / "missing_runtime_audit.json",
        "portfolio_diagnostics_path": tmp_path / "missing_portfolio_diagnostics.json",
        "weekly_monitoring_report_path": weekly_path,
        "operator_pending_observation_path": canonical_path,
    }

    snapshot_path = status_cli.DEFAULT_OPERATOR_PENDING_CONSISTENCY_SNAPSHOT_PATH

    status_cli.main(status_kwargs=status_kwargs)
    first_stdout = capsys.readouterr().out
    first_raw_snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    first_artifact_status = json.loads(status_path.read_text(encoding="utf-8"))
    status_cli.main(status_kwargs=status_kwargs)
    second_stdout = capsys.readouterr().out
    second_raw_snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    second_artifact_status = json.loads(status_path.read_text(encoding="utf-8"))

    expected_key_order = [
        "decision",
        "phase",
        "blockers",
        "next_action",
        "status_generated_at_utc",
        "snapshot_freshness_status",
        "operator_pending_consistency_status",
        "mismatches",
        "benchmark_id",
        "turnover_one_way_estimate",
        "estimated_round_trip_cost",
        "queue_write_allowed",
        "broad_daemon_allowed",
        "automation_allowed",
        "automated_rerun_allowed",
        "live_trading_enabled",
        "repair_status",
        "candidate_count",
        "best_available_max_drawdown",
        "drawdown_gap_to_limit",
    ]

    first_summary = json.loads(first_stdout)
    second_summary = json.loads(second_stdout)
    first_artifact_snapshot = first_artifact_status["paper_monitoring"]["operator_pending_consistency_snapshot"]
    second_artifact_snapshot = second_artifact_status["paper_monitoring"]["operator_pending_consistency_snapshot"]

    first_status_generated_at = first_summary["status_generated_at_utc"]
    second_status_generated_at = second_summary["status_generated_at_utc"]
    assert first_status_generated_at == first_artifact_status["generated_at_utc"]
    assert second_status_generated_at == second_artifact_status["generated_at_utc"]

    first_parsed_status_generated_at = datetime.fromisoformat(
        first_status_generated_at.replace("Z", "+00:00")
    )
    second_parsed_status_generated_at = datetime.fromisoformat(
        second_status_generated_at.replace("Z", "+00:00")
    )

    for parsed_generated_at in (
        first_parsed_status_generated_at,
        second_parsed_status_generated_at,
    ):
        assert parsed_generated_at.tzinfo is not None
        assert parsed_generated_at.utcoffset() == timezone.utc.utcoffset(parsed_generated_at)

    for summary, artifact_status, artifact_snapshot, raw_snapshot in (
        (first_summary, first_artifact_status, first_artifact_snapshot, first_raw_snapshot),
        (second_summary, second_artifact_status, second_artifact_snapshot, second_raw_snapshot),
    ):
        raw_runtime = raw_snapshot["runtime"]
        assert raw_snapshot["source_status_generated_at_utc"] == artifact_status["generated_at_utc"]
        assert raw_snapshot["source_status_generated_at_utc"] == summary["status_generated_at_utc"]
        assert artifact_snapshot["source_status_generated_at_utc"] == artifact_snapshot["latest_status_generated_at_utc"]
        assert artifact_snapshot["source_status_generated_at_utc"] == artifact_status["generated_at_utc"]
        assert artifact_snapshot["source_status_generated_at_utc"] == summary["status_generated_at_utc"]
        assert artifact_snapshot["latest_status_generated_at_utc"] == artifact_status["generated_at_utc"]
        assert artifact_snapshot["latest_status_generated_at_utc"] == summary["status_generated_at_utc"]
        assert list(summary.keys()) == expected_key_order
        for key in (
            "decision",
            "phase",
            "next_action",
            "status_generated_at_utc",
            "snapshot_freshness_status",
            "operator_pending_consistency_status",
            "benchmark_id",
        ):
            assert isinstance(summary[key], str)
            assert summary[key]
        assert isinstance(summary["blockers"], list)
        assert isinstance(summary["mismatches"], list)
        assert isinstance(summary["turnover_one_way_estimate"], (float, int))
        assert isinstance(summary["estimated_round_trip_cost"], (float, int))
        assert summary["decision"] == artifact_status["decision"]
        assert summary["phase"] == artifact_status["phase"]
        assert summary["blockers"] == artifact_status["blockers"]
        assert summary["next_action"] == artifact_status["next_action"]
        assert summary["status_generated_at_utc"] == artifact_status["generated_at_utc"]
        assert summary["snapshot_freshness_status"] == artifact_snapshot["snapshot_freshness_status"]
        assert summary["operator_pending_consistency_status"] == artifact_snapshot["consistency_status"]
        assert summary["mismatches"] == artifact_snapshot["mismatches"]
        assert summary["benchmark_id"] == artifact_snapshot["benchmark_id"]
        assert summary["turnover_one_way_estimate"] == artifact_snapshot["turnover_one_way_estimate"]
        assert summary["estimated_round_trip_cost"] == artifact_snapshot["estimated_round_trip_cost"]
        assert summary["snapshot_freshness_status"] == "fresh"
        assert summary["operator_pending_consistency_status"] == "ok"
        assert summary["mismatches"] == []
        assert summary["benchmark_id"] == "CSI1000"
        assert summary["turnover_one_way_estimate"] == 0.791672
        assert summary["estimated_round_trip_cost"] == 0.00475
        repair = artifact_status["simulated_portfolio_construction_repair"]
        assert summary["repair_status"] == repair["repair_status"]
        assert summary["candidate_count"] == repair["candidate_count"]
        assert summary["best_available_max_drawdown"] == repair["best_available_max_drawdown"]
        assert summary["drawdown_gap_to_limit"] == repair["drawdown_gap_to_limit"]
        assert summary["repair_status"] == "blocked_missing_repair_evidence"
        assert summary["candidate_count"] == 0
        assert summary["best_available_max_drawdown"] is None
        assert summary["drawdown_gap_to_limit"] is None
        for key in (
            "queue_write_allowed",
            "broad_daemon_allowed",
            "automation_allowed",
            "automated_rerun_allowed",
            "live_trading_enabled",
        ):
            assert isinstance(summary[key], bool)
            assert summary[key] is False
            assert artifact_snapshot[key] == raw_runtime[key]
            assert summary[key] == artifact_snapshot[key]
            assert summary[key] == raw_runtime[key]
    assert second_parsed_status_generated_at >= first_parsed_status_generated_at
    final_status = json.loads(status_path.read_text(encoding="utf-8"))
    snapshot = final_status["paper_monitoring"]["operator_pending_consistency_snapshot"]
    assert snapshot["snapshot_freshness_status"] == "fresh"
    assert snapshot["source_status_generated_at_utc"] == snapshot["latest_status_generated_at_utc"]
    assert snapshot["mismatches"] == []
    assert snapshot["benchmark_id"] == "CSI1000"
    assert snapshot["turnover_one_way_estimate"] == 0.791672
    assert snapshot["estimated_round_trip_cost"] == 0.00475
    assert snapshot["queue_write_allowed"] is False
    assert snapshot["broad_daemon_allowed"] is False
    assert snapshot["automation_allowed"] is False
    assert snapshot["automated_rerun_allowed"] is False
    assert snapshot["live_trading_enabled"] is False


def test_status_writer_subprocess_stdout_full_schema_matches_final_artifact():
    repo_root = Path(__file__).resolve().parents[1]
    status_path = repo_root / "artifacts" / "small_institutionalization" / "status.json"
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"

    completed = subprocess.run(
        [sys.executable, "scripts/write_small_institutionalization_status.py"],
        cwd=repo_root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    summary = json.loads(completed.stdout)
    final_status = json.loads(status_path.read_text(encoding="utf-8"))
    repair = final_status["simulated_portfolio_construction_repair"]
    snapshot = final_status["paper_monitoring"]["operator_pending_consistency_snapshot"]

    expected_key_order = [
        "decision",
        "phase",
        "blockers",
        "next_action",
        "status_generated_at_utc",
        "snapshot_freshness_status",
        "operator_pending_consistency_status",
        "mismatches",
        "benchmark_id",
        "turnover_one_way_estimate",
        "estimated_round_trip_cost",
        "queue_write_allowed",
        "broad_daemon_allowed",
        "automation_allowed",
        "automated_rerun_allowed",
        "live_trading_enabled",
        "repair_status",
        "candidate_count",
        "best_available_max_drawdown",
        "drawdown_gap_to_limit",
    ]
    assert list(summary.keys()) == expected_key_order

    for key in (
        "decision",
        "phase",
        "next_action",
        "status_generated_at_utc",
        "snapshot_freshness_status",
        "operator_pending_consistency_status",
        "repair_status",
    ):
        assert isinstance(summary[key], str)
        assert summary[key]
    assert isinstance(summary["blockers"], list)
    assert isinstance(summary["mismatches"], list)
    assert isinstance(summary["candidate_count"], int)
    assert not isinstance(summary["candidate_count"], bool)
    for key in (
        "turnover_one_way_estimate",
        "estimated_round_trip_cost",
        "best_available_max_drawdown",
        "drawdown_gap_to_limit",
    ):
        assert summary[key] is None or isinstance(summary[key], Number)
        assert not isinstance(summary[key], bool)
    parsed_status_generated_at = datetime.fromisoformat(summary["status_generated_at_utc"])
    assert parsed_status_generated_at.tzinfo == timezone.utc
    assert parsed_status_generated_at.utcoffset() == timezone.utc.utcoffset(None)
    assert summary["snapshot_freshness_status"] == "fresh"
    assert summary["operator_pending_consistency_status"] == "missing"
    assert summary["mismatches"] == []
    assert summary["candidate_count"] == 0

    assert summary["decision"] == final_status["decision"]
    assert summary["phase"] == final_status["phase"]
    assert summary["blockers"] == final_status["blockers"]
    assert summary["next_action"] == final_status["next_action"]
    assert summary["status_generated_at_utc"] == final_status["generated_at_utc"]
    assert summary["snapshot_freshness_status"] == snapshot["snapshot_freshness_status"]
    assert summary["operator_pending_consistency_status"] == snapshot["consistency_status"]
    assert summary["mismatches"] == snapshot["mismatches"]
    assert summary["benchmark_id"] == snapshot["benchmark_id"]
    assert summary["turnover_one_way_estimate"] == snapshot["turnover_one_way_estimate"]
    assert summary["estimated_round_trip_cost"] == snapshot["estimated_round_trip_cost"]
    assert summary["repair_status"] == repair["repair_status"]
    assert summary["candidate_count"] == repair["candidate_count"]
    assert summary["best_available_max_drawdown"] == repair["best_available_max_drawdown"]
    assert summary["drawdown_gap_to_limit"] == repair["drawdown_gap_to_limit"]
    for key in (
        "queue_write_allowed",
        "broad_daemon_allowed",
        "automation_allowed",
        "automated_rerun_allowed",
        "live_trading_enabled",
    ):
        assert summary[key] is False
        assert summary[key] == snapshot[key]


def test_status_writer_subprocess_fails_closed_without_operator_evidence():
    repo_root = Path(__file__).resolve().parents[1]
    status_path = repo_root / "artifacts" / "small_institutionalization" / "status.json"
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"

    subprocess.run(
        [sys.executable, "scripts/write_small_institutionalization_status.py"],
        cwd=repo_root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    status = json.loads(status_path.read_text(encoding="utf-8"))
    gate = status["manual_approval_gate"]
    summary = status["operator_approval_summary"]
    consistency = status["approval_artifact_consistency"]
    intake = status["operator_decision_intake_validation"]
    handoff = status["operator_decision_handoff"]
    wait_state = status["operator_decision_wait_state"]

    assert gate["gate_status"] == "blocked_missing_manual_approval_evidence"
    assert gate["human_approval_present"] is False
    assert gate["risk_relaxation_allowed"] is False
    assert summary["summary_status"] == "blocked_missing_operator_approval_summary"
    assert summary["approval_required"] is True
    assert summary["human_approval_present"] is False
    assert summary["primary_blocker"] == "missing_operator_approval_evidence"
    assert summary["repair_status"] == "blocked_missing_repair_evidence"
    assert summary["candidate_count"] == 0
    assert summary["required_decision_axis"] is None

    assert consistency["consistency_status"] == "not_evaluated_missing_approval_evidence"
    assert consistency["primary_blocker"] == summary["primary_blocker"]
    assert consistency["decision_axis"] == summary["required_decision_axis"]
    assert consistency["inconsistencies"] == []
    assert consistency["staleness_warnings"] == []

    assert intake["intake_status"] == "missing"
    assert intake["decision_type"] is None
    assert intake["scope"] is None
    assert intake["reason"] is None
    assert intake["validation_errors"] == []
    assert intake["non_mutating"] is True

    assert handoff["handoff_status"] == "blocked_missing_operator_handoff"
    assert handoff["intake_status"] == intake["intake_status"]
    assert handoff["primary_blocker"] == summary["primary_blocker"]
    assert handoff["repair_status"] == summary["repair_status"]
    assert handoff["candidate_count"] == summary["candidate_count"]
    assert handoff["decision_axis"] == summary["required_decision_axis"]
    assert handoff["validation_errors"] == intake["validation_errors"]
    assert handoff["non_mutating"] is True
    assert handoff["execution_allowed"] is False
    assert handoff["separate_execution_plan_required"] is False

    assert wait_state["wait_state_status"] == "not_waiting_on_operator_decision"
    assert wait_state["primary_blocker"] == handoff["primary_blocker"]
    assert wait_state["decision_axis"] == handoff["decision_axis"]
    assert wait_state["human_approval_present"] == gate["human_approval_present"]
    assert wait_state["approval_required"] == summary["approval_required"]
    assert wait_state["intake_status"] == intake["intake_status"]
    assert wait_state["handoff_status"] == handoff["handoff_status"]
    assert wait_state["execution_allowed"] is False
    assert wait_state["separate_execution_plan_required"] is False

    for payload in (gate, summary, consistency, intake, handoff, wait_state):
        assert payload["queue_write_allowed"] is False
        assert payload["broad_daemon_allowed"] is False
        assert payload["automation_allowed"] is False
        assert payload["automated_rerun_allowed"] is False
        if "live_trading_enabled" in payload:
            assert payload["live_trading_enabled"] is False


def _parse_markdown_label_value_section(markdown: str, heading: str) -> dict[str, str]:
    lines = markdown.splitlines()

    def _fence_marker(line: str) -> tuple[str, int] | None:
        stripped = line.strip()
        if not stripped or stripped[0] not in ("`", "~"):
            return None
        marker_char = stripped[0]
        marker_length = len(stripped) - len(stripped.lstrip(marker_char))
        if marker_length < 3:
            return None
        return marker_char, marker_length

    heading_line_indexes = [index for index, line in enumerate(lines) if line.rstrip() == heading]
    if not heading_line_indexes:
        raise ValueError(f"Markdown heading not found: {heading}")
    if len(heading_line_indexes) > 1:
        raise ValueError(f"Markdown heading is duplicated: {heading}")

    start_index = heading_line_indexes[0]
    next_heading_index = len(lines)
    in_fenced_code = False
    fenced_code_marker: tuple[str, int] | None = None
    for index, line in enumerate(lines[start_index + 1 :], start=start_index + 1):
        marker = _fence_marker(line)
        if marker is not None:
            marker_char, marker_length = marker
            if (
                in_fenced_code
                and fenced_code_marker is not None
                and marker_char == fenced_code_marker[0]
                and marker_length >= fenced_code_marker[1]
            ):
                in_fenced_code = False
                fenced_code_marker = None
            elif not in_fenced_code:
                in_fenced_code = True
                fenced_code_marker = marker
            continue
        if not in_fenced_code and line.rstrip().startswith("## "):
            next_heading_index = index
            break
    markdown_section_lines = lines[start_index:next_heading_index]

    parsed: dict[str, str] = {}
    in_fenced_code = False
    fenced_code_marker = None
    for raw_line in markdown_section_lines:
        line = raw_line.strip()
        marker = _fence_marker(raw_line)
        if marker is not None:
            marker_char, marker_length = marker
            if (
                in_fenced_code
                and fenced_code_marker is not None
                and marker_char == fenced_code_marker[0]
                and marker_length >= fenced_code_marker[1]
            ):
                in_fenced_code = False
                fenced_code_marker = None
            elif not in_fenced_code:
                in_fenced_code = True
                fenced_code_marker = marker
            continue
        if in_fenced_code or not line.startswith("- ") or ":" not in line:
            continue
        label, value = line[2:].split(":", 1)
        label = label.strip()
        if not label:
            continue
        if label in parsed:
            raise ValueError(f"Markdown label is duplicated in {heading}: {label}")
        parsed[label] = value.strip()
    return parsed


def test_parse_markdown_label_value_section_requires_same_or_longer_fence_close():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Earlier approval section",
            "- Summary status: earlier outside requested section ignored",
            "",
            "## Operator approval summary",
            "- Summary status: blocked_pending_manual_approval",
            "````markdown",
            "## Operator quoted note",
            "```",
            "- Queue write allowed: True",
            "## Operator decision wait state",
            "```",
            "- Automation allowed: True",
            "````",
            "- Required decision axis: holding_count=50:manual-review",
            "- Queue write allowed: False",
            "- Primary blocker: drawdown_risk_too_high",
            "",
            "## Operator decision wait state",
            "- Wait-state status: awaiting_operator_decision",
            "- Automation allowed: True",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Required decision axis": "holding_count=50:manual-review",
        "Queue write allowed": "False",
        "Primary blocker": "drawdown_risk_too_high",
    }
    assert "Wait-state status" not in parsed
    assert "Automation allowed" not in parsed
    assert "True" not in parsed.values()


def test_parse_markdown_label_value_section_requires_same_or_longer_tilde_fence_close():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Earlier approval section",
            "- Summary status: earlier outside requested section ignored",
            "",
            "## Operator approval summary",
            "- Summary status: blocked_pending_manual_approval",
            "~~~~markdown",
            "## Operator quoted note",
            "~~~",
            "- Queue write allowed: True",
            "## Operator decision wait state",
            "~~~",
            "- Automation allowed: True",
            "~~~~",
            "- Required decision axis: holding_count=50:manual-review",
            "- Queue write allowed: False",
            "- Primary blocker: drawdown_risk_too_high",
            "",
            "## Operator decision wait state",
            "- Wait-state status: awaiting_operator_decision",
            "- Automation allowed: True",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Required decision axis": "holding_count=50:manual-review",
        "Queue write allowed": "False",
        "Primary blocker": "drawdown_risk_too_high",
    }
    assert "Wait-state status" not in parsed
    assert "Automation allowed" not in parsed
    assert "True" not in parsed.values()


def test_parse_markdown_label_value_section_fails_clearly_when_heading_missing():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Present section",
            "- Shared label: present value",
        ]
    )

    missing_heading = "## Missing approval section"
    with pytest.raises(ValueError, match="Missing approval section"):
        _parse_markdown_label_value_section(markdown, missing_heading)


def test_parse_markdown_label_value_section_fails_clearly_when_heading_duplicated():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Duplicated approval section",
            "- Shared label: first value",
            "",
            "## Other section",
            "- Shared label: other value",
            "",
            "## Duplicated approval section",
            "- Shared label: second value",
        ]
    )

    duplicate_heading = "## Duplicated approval section"
    with pytest.raises(ValueError, match="Duplicated approval section"):
        _parse_markdown_label_value_section(markdown, duplicate_heading)


def test_parse_markdown_label_value_section_ignores_heading_mentions_inside_content():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Approval artifact consistency",
            "- Consistency status: ok",
            "- Note: prose mentions ## Approval artifact consistency but is not a heading",
            "- Path: artifacts/foo:bar/status.json",
            "",
            "## Operator decision wait state",
            "- Wait-state status: awaiting_operator_decision",
        ]
    )

    parsed = _parse_markdown_label_value_section(
        markdown,
        "## Approval artifact consistency",
    )

    assert parsed == {
        "Consistency status": "ok",
        "Note": "prose mentions ## Approval artifact consistency but is not a heading",
        "Path": "artifacts/foo:bar/status.json",
    }


def test_parse_markdown_label_value_section_accepts_heading_line_trailing_whitespace():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Approval artifact consistency   ",
            "- Consistency status: ok",
            "- Note: prose mentions ## Approval artifact consistency but is not a heading",
            "- Path: artifacts/foo:bar/status.json",
            "",
            "## Operator decision wait state\t",
            "- Wait-state status: awaiting_operator_decision",
        ]
    )

    parsed = _parse_markdown_label_value_section(
        markdown,
        "## Approval artifact consistency",
    )

    assert parsed == {
        "Consistency status": "ok",
        "Note": "prose mentions ## Approval artifact consistency but is not a heading",
        "Path": "artifacts/foo:bar/status.json",
    }


def test_parse_markdown_label_value_section_fails_clearly_when_duplicate_heading_has_trailing_whitespace():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Approval artifact consistency   ",
            "- Consistency status: first value",
            "- Note: prose mentions ## Approval artifact consistency but is not a heading",
            "",
            "## Operator decision wait state",
            "- Wait-state status: awaiting_operator_decision",
            "",
            "## Approval artifact consistency\t",
            "- Consistency status: second value",
        ]
    )

    duplicate_heading = "## Approval artifact consistency"
    with pytest.raises(ValueError, match="Approval artifact consistency"):
        _parse_markdown_label_value_section(markdown, duplicate_heading)


def test_parse_markdown_label_value_section_is_section_local_and_preserves_colons():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## First section",
            "- Shared label: first value",
            "- Path: artifacts/foo:bar/status.json",
            "- Decision axis: holding_count=50:manual-review",
            "not a bullet: ignored",
            "- Missing colon ignored",
            "",
            "## Second section",
            "- Shared label: second value",
            "- Other: keep me out of first",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## First section")

    assert parsed == {
        "Shared label": "first value",
        "Path": "artifacts/foo:bar/status.json",
        "Decision axis": "holding_count=50:manual-review",
    }


def test_parse_markdown_label_value_section_returns_empty_dict_for_empty_requested_section():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Empty approval section",
            "",
            "Plain prose without bullet label/value rows.",
            "- Missing colon ignored",
            "",
            "## Following approval section",
            "- Shared label: following value",
            "- Queue write allowed: False",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Empty approval section")

    assert parsed == {}


def test_parse_markdown_label_value_section_ignores_non_bullet_colon_lines():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Operator approval summary",
            "Narrative before rows: this colon-bearing prose is not a label/value bullet.",
            "Indented prose: also ignored even though it has a colon.",
            "- Summary status: blocked_pending_manual_approval",
            "- Required decision axis: holding_count=50:manual-review",
            "A final narrative note: still ignored after valid rows.",
            "",
            "## Operator decision wait state",
            "- Wait-state status: awaiting_operator_decision",
            "- Queue write allowed: False",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Required decision axis": "holding_count=50:manual-review",
    }


def test_parse_markdown_label_value_section_ignores_empty_label_bullets():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Operator approval summary",
            "- : malformed empty label before valid row",
            "-    : malformed whitespace-only label",
            "- Summary status: blocked_pending_manual_approval",
            "- Required decision axis: holding_count=50:manual-review",
            "- : malformed empty label after valid rows",
            "",
            "## Operator decision wait state",
            "- Wait-state status: awaiting_operator_decision",
            "- Queue write allowed: False",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Required decision axis": "holding_count=50:manual-review",
    }
    assert "" not in parsed
    assert "Wait-state status" not in parsed


def test_parse_markdown_label_value_section_fails_clearly_when_requested_section_has_duplicate_label():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Other approval section",
            "- Summary status: other first value",
            "- Summary status: other second value",
            "",
            "## Operator approval summary",
            "- Summary status: blocked_pending_manual_approval",
            "- Required decision axis: holding_count=50:manual-review",
            "- Summary status: silently-overriding-value",
            "",
            "## Operator decision wait state",
            "- Summary status: duplicate outside requested section is ignored",
            "- Summary status: duplicate outside requested section stays ignored",
        ]
    )

    with pytest.raises(ValueError, match="Summary status"):
        _parse_markdown_label_value_section(markdown, "## Operator approval summary")


def test_parse_markdown_label_value_section_ignores_duplicate_labels_outside_requested_section():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Earlier approval section",
            "- Summary status: earlier first duplicate",
            "- Summary status: earlier second duplicate",
            "- Required decision axis: earlier-only row",
            "",
            "## Operator approval summary",
            "- Summary status: blocked_pending_manual_approval",
            "- Required decision axis: holding_count=50:manual-review",
            "- Queue write allowed: False",
            "",
            "## Operator decision wait state",
            "- Summary status: later first duplicate",
            "- Summary status: later second duplicate",
            "- Queue write allowed: True",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Required decision axis": "holding_count=50:manual-review",
        "Queue write allowed": "False",
    }
    assert "earlier-only row" not in parsed.values()
    assert "True" not in parsed.values()


def test_parse_markdown_label_value_section_normalizes_label_whitespace_and_duplicate_detection():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Earlier approval section",
            "- Summary status: earlier duplicate outside requested section",
            "- Summary status: earlier duplicate outside requested section ignored",
            "",
            "## Operator approval summary",
            "-   Summary status   : blocked_pending_manual_approval",
            "-   Required decision axis   : holding_count=50:manual-review",
            "-   Summary status: normalized-duplicate-value",
            "",
            "## Operator decision wait state",
            "- Summary status: later duplicate outside requested section",
            "- Summary status: later duplicate outside requested section ignored",
        ]
    )

    with pytest.raises(ValueError, match="Summary status"):
        _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    non_duplicate_markdown = markdown.replace(
        "-   Summary status: normalized-duplicate-value",
        "-   Queue write allowed   : False",
    )
    parsed = _parse_markdown_label_value_section(
        non_duplicate_markdown,
        "## Operator approval summary",
    )

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Required decision axis": "holding_count=50:manual-review",
        "Queue write allowed": "False",
    }


def test_parse_markdown_label_value_section_strips_surrounding_value_whitespace():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Earlier approval section",
            "- Summary status: earlier outside value",
            "",
            "## Operator approval summary",
            "- Summary status:   blocked_pending_manual_approval   ",
            "- Required decision axis:\t holding_count=50:manual-review \t",
            "- Primary blocker:  drawdown   risk:too_high  ",
            "",
            "## Operator decision wait state",
            "- Summary status: later outside value",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Required decision axis": "holding_count=50:manual-review",
        "Primary blocker": "drawdown   risk:too_high",
    }
    assert "later outside value" not in parsed.values()


def test_parse_markdown_label_value_section_ignores_compact_malformed_bullets():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Earlier approval section",
            "-Summary status: compact outside requested section ignored",
            "- Summary status: earlier canonical outside value",
            "",
            "## Operator approval summary",
            "-Summary status: compact malformed row must be ignored",
            "-Queue write allowed: True",
            "- Summary status: blocked_pending_manual_approval",
            "- Required decision axis: holding_count=50:manual-review",
            "- Queue write allowed: False",
            "-Primary blocker: compact malformed blocker ignored",
            "",
            "## Operator decision wait state",
            "-Summary status: compact following section ignored",
            "- Queue write allowed: True",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Required decision axis": "holding_count=50:manual-review",
        "Queue write allowed": "False",
    }
    assert "compact malformed row must be ignored" not in parsed.values()
    assert "True" not in parsed.values()
    assert "Primary blocker" not in parsed


def test_parse_markdown_label_value_section_accepts_indented_canonical_bullets():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Earlier approval section",
            "  - Summary status: earlier indented outside requested section ignored",
            "\t- Queue write allowed: True",
            "",
            "## Operator approval summary",
            "  -Summary status: compact malformed row must be ignored",
            "\t-Queue write allowed: True",
            "  - Summary status: blocked_pending_manual_approval",
            "\t- Required decision axis: holding_count=50:manual-review",
            "    - Queue write allowed: False",
            "\t- Primary blocker: drawdown_risk_too_high",
            "",
            "## Operator decision wait state",
            "  - Summary status: following indented outside requested section ignored",
            "\t- Queue write allowed: True",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Required decision axis": "holding_count=50:manual-review",
        "Queue write allowed": "False",
        "Primary blocker": "drawdown_risk_too_high",
    }
    assert "compact malformed row must be ignored" not in parsed.values()
    assert "True" not in parsed.values()


def test_parse_markdown_label_value_section_ignores_indented_heading_looking_lines():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Earlier approval section",
            "- Summary status: earlier outside requested section ignored",
            "  ## Operator approval summary",
            "  - Summary status: indented heading-looking earlier content ignored",
            "",
            "## Operator approval summary   ",
            "  ## Operator decision wait state",
            "  - Summary status: blocked_pending_manual_approval",
            "\t- Required decision axis: holding_count=50:manual-review",
            "    - Queue write allowed: False",
            "  ## Later pseudo-heading remains inside requested section",
            "\t- Primary blocker: drawdown_risk_too_high",
            "",
            "## Operator decision wait state",
            "- Summary status: following outside requested section ignored",
            "- Queue write allowed: True",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Required decision axis": "holding_count=50:manual-review",
        "Queue write allowed": "False",
        "Primary blocker": "drawdown_risk_too_high",
    }
    assert "indented heading-looking earlier content ignored" not in parsed.values()
    assert "following outside requested section ignored" not in parsed.values()
    assert "True" not in parsed.values()


def test_parse_markdown_label_value_section_preserves_rows_after_nested_subheading():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Earlier approval section",
            "- Summary status: earlier outside requested section ignored",
            "",
            "## Operator approval summary",
            "- Summary status: blocked_pending_manual_approval",
            "### Nested note",
            "Subheading-like prose: ignored because it is not a bullet row.",
            "- Required decision axis: holding_count=50:manual-review",
            "### Runtime flags remain local",
            "- Queue write allowed: False",
            "- Primary blocker: drawdown_risk_too_high",
            "",
            "## Operator decision wait state",
            "- Wait-state status: awaiting_operator_decision",
            "- Queue write allowed: True",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Required decision axis": "holding_count=50:manual-review",
        "Queue write allowed": "False",
        "Primary blocker": "drawdown_risk_too_high",
    }
    assert "Wait-state status" not in parsed
    assert "True" not in parsed.values()


def test_parse_markdown_label_value_section_ignores_hash_prefixed_non_boundary_lines():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Earlier approval section",
            "- Summary status: earlier outside requested section ignored",
            "",
            "## Operator approval summary",
            "- Summary status: blocked_pending_manual_approval",
            "##Operator malformed adjacent hash text",
            "- Required decision axis: holding_count=50:manual-review",
            "#### Deep nested note",
            "Deep-note prose: ignored because it is not a bullet row.",
            "- Queue write allowed: False",
            "- Primary blocker: drawdown_risk_too_high",
            "",
            "## Operator decision wait state",
            "- Wait-state status: awaiting_operator_decision",
            "- Queue write allowed: True",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Required decision axis": "holding_count=50:manual-review",
        "Queue write allowed": "False",
        "Primary blocker": "drawdown_risk_too_high",
    }
    assert "Wait-state status" not in parsed
    assert "True" not in parsed.values()


def test_parse_markdown_label_value_section_ignores_single_hash_non_boundary_lines():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Earlier approval section",
            "- Summary status: earlier outside requested section ignored",
            "",
            "## Operator approval summary",
            "- Summary status: blocked_pending_manual_approval",
            "# Operator note",
            "Operator-note prose: ignored because it is not a bullet row.",
            "- Required decision axis: holding_count=50:manual-review",
            "# Manual risk note",
            "- Queue write allowed: False",
            "- Primary blocker: drawdown_risk_too_high",
            "",
            "## Operator decision wait state",
            "- Wait-state status: awaiting_operator_decision",
            "- Queue write allowed: True",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Required decision axis": "holding_count=50:manual-review",
        "Queue write allowed": "False",
        "Primary blocker": "drawdown_risk_too_high",
    }
    assert "Wait-state status" not in parsed
    assert "True" not in parsed.values()


def test_parse_markdown_label_value_section_ignores_blockquote_heading_non_boundary_lines():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Earlier approval section",
            "- Summary status: earlier outside requested section ignored",
            "",
            "## Operator approval summary",
            "- Summary status: blocked_pending_manual_approval",
            "> ## Operator quoted note",
            "> Quoted note prose: ignored because it is not a bullet row.",
            "- Required decision axis: holding_count=50:manual-review",
            "> ## Manual risk quoted note",
            "- Queue write allowed: False",
            "- Primary blocker: drawdown_risk_too_high",
            "",
            "## Operator decision wait state",
            "- Wait-state status: awaiting_operator_decision",
            "- Queue write allowed: True",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Required decision axis": "holding_count=50:manual-review",
        "Queue write allowed": "False",
        "Primary blocker": "drawdown_risk_too_high",
    }
    assert "Wait-state status" not in parsed
    assert "True" not in parsed.values()


def test_parse_markdown_label_value_section_ignores_fenced_code_heading_non_boundary_lines():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Earlier approval section",
            "- Summary status: earlier outside requested section ignored",
            "",
            "## Operator approval summary",
            "- Summary status: blocked_pending_manual_approval",
            "```markdown",
            "## Operator quoted note",
            "- Queue write allowed: True",
            "## Operator decision wait state",
            "```",
            "- Required decision axis: holding_count=50:manual-review",
            "- Queue write allowed: False",
            "- Primary blocker: drawdown_risk_too_high",
            "",
            "## Operator decision wait state",
            "- Wait-state status: awaiting_operator_decision",
            "- Automation allowed: True",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Queue write allowed": "False",
        "Required decision axis": "holding_count=50:manual-review",
        "Primary blocker": "drawdown_risk_too_high",
    }
    assert "Wait-state status" not in parsed
    assert "Automation allowed" not in parsed
    assert "True" not in parsed.values()


def test_parse_markdown_label_value_section_ignores_tilde_fenced_code_heading_non_boundary_lines():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Earlier approval section",
            "- Summary status: earlier outside requested section ignored",
            "",
            "## Operator approval summary",
            "- Summary status: blocked_pending_manual_approval",
            "~~~markdown",
            "## Operator quoted note",
            "- Queue write allowed: True",
            "## Operator decision wait state",
            "~~~",
            "- Required decision axis: holding_count=50:manual-review",
            "- Queue write allowed: False",
            "- Primary blocker: drawdown_risk_too_high",
            "",
            "## Operator decision wait state",
            "- Wait-state status: awaiting_operator_decision",
            "- Automation allowed: True",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Queue write allowed": "False",
        "Required decision axis": "holding_count=50:manual-review",
        "Primary blocker": "drawdown_risk_too_high",
    }
    assert "Wait-state status" not in parsed
    assert "Automation allowed" not in parsed
    assert "True" not in parsed.values()


def test_parse_markdown_label_value_section_honors_same_marker_fenced_code_close():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Earlier approval section",
            "- Summary status: earlier outside requested section ignored",
            "",
            "## Operator approval summary",
            "- Summary status: blocked_pending_manual_approval",
            "```markdown",
            "## Operator quoted note",
            "~~~",
            "- Queue write allowed: True",
            "## Operator decision wait state",
            "~~~",
            "- Automation allowed: True",
            "```",
            "- Required decision axis: holding_count=50:manual-review",
            "- Queue write allowed: False",
            "- Primary blocker: drawdown_risk_too_high",
            "",
            "## Operator decision wait state",
            "- Wait-state status: awaiting_operator_decision",
            "- Automation allowed: True",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Required decision axis": "holding_count=50:manual-review",
        "Queue write allowed": "False",
        "Primary blocker": "drawdown_risk_too_high",
    }
    assert "Wait-state status" not in parsed
    assert "Automation allowed" not in parsed
    assert "True" not in parsed.values()


def test_parse_markdown_label_value_section_honors_same_marker_fenced_code_close_reverse():
    markdown = "\n".join(
        [
            "# Synthetic approval markdown",
            "",
            "## Earlier approval section",
            "- Summary status: earlier outside requested section ignored",
            "",
            "## Operator approval summary",
            "- Summary status: blocked_pending_manual_approval",
            "~~~markdown",
            "## Operator quoted note",
            "```",
            "- Queue write allowed: True",
            "## Operator decision wait state",
            "```",
            "- Automation allowed: True",
            "~~~",
            "- Required decision axis: holding_count=50:manual-review",
            "- Queue write allowed: False",
            "- Primary blocker: drawdown_risk_too_high",
            "",
            "## Operator decision wait state",
            "- Wait-state status: awaiting_operator_decision",
            "- Automation allowed: True",
        ]
    )

    parsed = _parse_markdown_label_value_section(markdown, "## Operator approval summary")

    assert parsed == {
        "Summary status": "blocked_pending_manual_approval",
        "Required decision axis": "holding_count=50:manual-review",
        "Queue write allowed": "False",
        "Primary blocker": "drawdown_risk_too_high",
    }
    assert "Wait-state status" not in parsed
    assert "Automation allowed" not in parsed
    assert "True" not in parsed.values()


def test_status_writer_subprocess_pins_operator_approval_wait_state_markdown_contract():
    repo_root = Path(__file__).resolve().parents[1]
    status_path = repo_root / "artifacts" / "small_institutionalization" / "status.json"
    status_markdown_path = repo_root / "artifacts" / "small_institutionalization" / "status.md"
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"

    subprocess.run(
        [sys.executable, "scripts/write_small_institutionalization_status.py"],
        cwd=repo_root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    status = json.loads(status_path.read_text(encoding="utf-8"))
    markdown = status_markdown_path.read_text(encoding="utf-8")

    expected_sections = {
        "## Manual approval gate": (
            status["manual_approval_gate"],
            {
                "gate_status": "Gate status",
                "human_approval_present": "Human approval present",
                "risk_relaxation_allowed": "Risk relaxation allowed",
                "queue_write_allowed": "Queue write allowed",
                "broad_daemon_allowed": "Broad daemon allowed",
                "automation_allowed": "Automation allowed",
                "automated_rerun_allowed": "Automated rerun allowed",
                "live_trading_enabled": "Live trading enabled",
            },
        ),
        "## Operator approval summary": (
            status["operator_approval_summary"],
            {
                "summary_status": "Summary status",
                "approval_required": "Approval required",
                "human_approval_present": "Human approval present",
                "required_decision_axis": "Required decision axis",
                "primary_blocker": "Primary blocker",
                "repair_status": "Repair status",
                "candidate_count": "Candidate count",
                "queue_write_allowed": "Queue write allowed",
                "broad_daemon_allowed": "Broad daemon allowed",
                "automation_allowed": "Automation allowed",
                "automated_rerun_allowed": "Automated rerun allowed",
                "live_trading_enabled": "Live trading enabled",
            },
        ),
        "## Approval artifact consistency": (
            status["approval_artifact_consistency"],
            {
                "consistency_status": "Consistency status",
                "primary_blocker": "Primary blocker",
                "decision_axis": "Decision axis",
                "queue_write_allowed": "Queue write allowed",
                "broad_daemon_allowed": "Broad daemon allowed",
                "automation_allowed": "Automation allowed",
                "automated_rerun_allowed": "Automated rerun allowed",
                "live_trading_enabled": "Live trading enabled",
                "inconsistencies": "Inconsistencies",
                "staleness_warnings": "Staleness warnings",
            },
        ),
        "## Operator decision intake validation": (
            status["operator_decision_intake_validation"],
            {
                "intake_status": "Intake status",
                "decision_type": "Decision type",
                "non_mutating": "Non-mutating",
                "validation_errors": "Validation errors",
                "queue_write_allowed": "Queue write allowed",
                "broad_daemon_allowed": "Broad daemon allowed",
                "automation_allowed": "Automation allowed",
                "automated_rerun_allowed": "Automated rerun allowed",
                "live_trading_enabled": "Live trading enabled",
            },
        ),
        "## Operator decision handoff": (
            status["operator_decision_handoff"],
            {
                "handoff_status": "Handoff status",
                "intake_status": "Intake status",
                "decision_type": "Decision type",
                "decision_axis": "Decision axis",
                "primary_blocker": "Primary blocker",
                "execution_allowed": "Execution allowed",
                "separate_execution_plan_required": "Separate execution plan required",
                "validation_errors": "Validation errors",
                "queue_write_allowed": "Queue write allowed",
                "broad_daemon_allowed": "Broad daemon allowed",
                "automation_allowed": "Automation allowed",
                "automated_rerun_allowed": "Automated rerun allowed",
                "live_trading_enabled": "Live trading enabled",
            },
        ),
        "## Operator decision wait state": (
            status["operator_decision_wait_state"],
            {
                "wait_state_status": "Wait-state status",
                "primary_blocker": "Primary blocker",
                "decision_axis": "Decision axis",
                "human_approval_present": "Human approval present",
                "approval_required": "Approval required",
                "intake_status": "Intake status",
                "handoff_status": "Handoff status",
                "execution_allowed": "Execution allowed",
                "separate_execution_plan_required": "Separate execution plan required",
                "queue_write_allowed": "Queue write allowed",
                "broad_daemon_allowed": "Broad daemon allowed",
                "automation_allowed": "Automation allowed",
                "automated_rerun_allowed": "Automated rerun allowed",
                "live_trading_enabled": "Live trading enabled",
            },
        ),
    }

    for heading, (payload, label_by_key) in expected_sections.items():
        parsed_section = _parse_markdown_label_value_section(markdown, heading)
        for key, label in label_by_key.items():
            assert parsed_section[label] == str(payload.get(key))
        for key in (
            "queue_write_allowed",
            "broad_daemon_allowed",
            "automation_allowed",
            "automated_rerun_allowed",
            "live_trading_enabled",
        ):
            assert payload.get(key) is False
            assert parsed_section[label_by_key[key]] == "False"
