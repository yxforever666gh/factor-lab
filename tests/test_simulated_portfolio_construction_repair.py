import json
import subprocess
import sys

from scripts.write_simulated_portfolio_construction_repair import build_cli_summary
from factor_lab.simulated_portfolio_construction_repair import (
    build_simulated_portfolio_construction_repair,
    group_matrix_results,
    repair_to_markdown,
    write_simulated_portfolio_construction_repair,
)


def _row(signal="book", holding=50, freq="monthly", cost=0, sharpe=0.1, drawdown=-0.5, status="ok", turnover=0.2):
    return {
        "status": status,
        "signal_column": signal,
        "holding_count": holding,
        "rebalance_frequency": freq,
        "cost_bps": float(cost),
        "sharpe": sharpe,
        "max_drawdown": drawdown,
        "turnover_mean": turnover,
        "combo_id": f"{signal}-{holding}-{freq}-{cost}-{sharpe}",
    }


def test_group_matrix_results_summarizes_ok_rows_by_repair_dimensions():
    matrix = {
        "results": [
            _row(signal="book", holding=50, freq="monthly", cost=0, sharpe=0.2, drawdown=-0.42),
            _row(signal="book", holding=50, freq="monthly", cost=30, sharpe=0.5, drawdown=-0.30),
            _row(signal="roe", holding=75, freq="biweekly", cost=0, sharpe=0.1, drawdown=-0.60),
            _row(status="insufficient_data"),
        ]
    }

    grouped = group_matrix_results(matrix, drawdown_limit=-0.35)

    assert grouped["ok_result_count"] == 3
    assert grouped["dimension_summaries"]["signal_column"]["book"]["result_count"] == 2
    assert grouped["dimension_summaries"]["signal_column"]["book"]["best_sharpe"] == 0.5
    assert grouped["dimension_summaries"]["signal_column"]["book"]["best_max_drawdown"] == -0.30
    assert grouped["dimension_summaries"]["signal_column"]["book"]["median_max_drawdown"] == -0.36
    assert grouped["dimension_summaries"]["signal_column"]["book"]["pass_count_under_drawdown_limit"] == 1
    assert grouped["dimension_summaries"]["holding_count"]["50"]["pass_count_under_drawdown_limit"] == 1
    assert grouped["dimension_summaries"]["rebalance_frequency"]["monthly"]["result_count"] == 2
    assert grouped["dimension_summaries"]["cost_bps"]["30.0"]["pass_count_under_drawdown_limit"] == 1


def test_build_repair_recommends_highest_sharpe_drawdown_safe_candidate_when_present(tmp_path):
    matrix_path = tmp_path / "matrix.json"
    policy_path = tmp_path / "policy.json"
    matrix_path.write_text(
        json.dumps(
            {
                "results": [
                    _row(sharpe=0.4, drawdown=-0.30),
                    _row(sharpe=0.8, drawdown=-0.34),
                    _row(sharpe=1.0, drawdown=-0.50),
                ]
            }
        ),
        encoding="utf-8",
    )
    policy_path.write_text(json.dumps({"diagnosis_thresholds": {"max_drawdown_limit": -0.35}}), encoding="utf-8")

    payload = build_simulated_portfolio_construction_repair(matrix_path, policy_path)

    assert payload["repair_status"] == "candidate_found"
    assert payload["candidate_count"] == 2
    assert payload["automation_allowed"] is False
    assert payload["recommended_candidate"]["sharpe"] == 0.8
    assert payload["recommended_candidate"]["max_drawdown"] == -0.34
    assert payload["queue_write_allowed"] is False
    assert payload["broad_daemon_allowed"] is False
    assert payload["automated_rerun_allowed"] is False
    assert payload["live_trading_enabled"] is False
    assert "queue_write_allowed" not in payload["recommended_candidate"]
    assert "live_trading_enabled" not in payload["recommended_candidate"]
    assert {
        "repair_status",
        "candidate_count",
        "recommended_candidate",
        "best_available_max_drawdown",
        "drawdown_gap_to_limit",
        "automation_allowed",
        "queue_write_allowed",
        "broad_daemon_allowed",
        "automated_rerun_allowed",
        "live_trading_enabled",
    }.issubset(payload)


def test_build_repair_uses_strict_drawdown_gate_from_helper(tmp_path):
    matrix_path = tmp_path / "matrix.json"
    policy_path = tmp_path / "policy.json"
    matrix_path.write_text(json.dumps({"results": [_row(sharpe=1.0, drawdown=-0.35), _row(sharpe=0.4, drawdown=-0.36)]}), encoding="utf-8")
    policy_path.write_text(json.dumps({"diagnosis_thresholds": {"max_drawdown_limit": -0.35}}), encoding="utf-8")

    payload = build_simulated_portfolio_construction_repair(matrix_path, policy_path)

    assert payload["repair_status"] == "blocked_no_drawdown_safe_candidate"
    assert payload["candidate_count"] == 0
    assert payload["recommended_candidate"] is None
    assert payload["best_available_max_drawdown"] == -0.35
    assert payload["drawdown_gap_to_limit"] == 0.0


def test_build_repair_uses_sharpe_then_turnover_and_cost_after_safe_gate(tmp_path):
    matrix_path = tmp_path / "matrix.json"
    policy_path = tmp_path / "policy.json"
    rows = [
        {**_row(sharpe=0.6, drawdown=-0.30, turnover=0.10), "combo_id": "lower-sharpe"},
        {**_row(sharpe=0.9, drawdown=-0.30, turnover=0.30, cost=0), "combo_id": "higher-sharpe-higher-turnover"},
        {**_row(sharpe=0.9, drawdown=-0.31, turnover=0.20, cost=30), "combo_id": "unsafe-cost"},
        {**_row(sharpe=0.9, drawdown=-0.32, turnover=0.20, cost=0), "combo_id": "same-sharpe-lower-turnover-cost"},
    ]
    matrix_path.write_text(json.dumps({"results": rows}), encoding="utf-8")
    policy_path.write_text(json.dumps({"diagnosis_thresholds": {"max_drawdown_limit": -0.35}}), encoding="utf-8")

    payload = build_simulated_portfolio_construction_repair(matrix_path, policy_path)

    assert payload["recommended_candidate"]["combo_id"] == "same-sharpe-lower-turnover-cost"
    assert payload["candidate_count"] == 3


def test_build_repair_blocks_without_drawdown_safe_candidate(tmp_path):
    matrix_path = tmp_path / "matrix.json"
    policy_path = tmp_path / "policy.json"
    matrix_path.write_text(json.dumps({"results": [_row(sharpe=1.0, drawdown=-0.50), _row(sharpe=0.4, drawdown=-0.36)]}), encoding="utf-8")
    policy_path.write_text(json.dumps({"diagnosis_thresholds": {"max_drawdown_limit": -0.35}}), encoding="utf-8")

    payload = build_simulated_portfolio_construction_repair(matrix_path, policy_path)

    assert payload["repair_status"] == "blocked_no_drawdown_safe_candidate"
    assert payload["candidate_count"] == 0
    assert payload["recommended_candidate"] is None
    assert payload["automation_allowed"] is False
    assert payload["queue_write_allowed"] is False
    assert payload["broad_daemon_allowed"] is False
    assert payload["automated_rerun_allowed"] is False
    assert payload["live_trading_enabled"] is False
    assert payload["best_available_max_drawdown"] == -0.36
    assert payload["drawdown_gap_to_limit"] == 0.01


def test_write_repair_outputs_json_and_markdown(tmp_path):
    matrix_path = tmp_path / "matrix.json"
    policy_path = tmp_path / "policy.json"
    json_path = tmp_path / "repair.json"
    markdown_path = tmp_path / "repair.md"
    matrix_path.write_text(json.dumps({"results": [_row(sharpe=0.5, drawdown=-0.30)]}), encoding="utf-8")
    policy_path.write_text(json.dumps({"diagnosis_thresholds": {"max_drawdown_limit": -0.35}}), encoding="utf-8")

    payload = write_simulated_portfolio_construction_repair(
        matrix_path=matrix_path,
        policy_path=policy_path,
        json_path=json_path,
        markdown_path=markdown_path,
    )

    assert payload["repair_status"] == "candidate_found"
    assert json.loads(json_path.read_text(encoding="utf-8"))["candidate_count"] == 1
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Simulated Portfolio Construction Repair" in markdown
    assert "candidate_found" in markdown


def test_repair_markdown_includes_status_candidate_and_dimension_summary():
    markdown = repair_to_markdown(
        {
            "generated_at_utc": "2026-05-12T00:00:00+00:00",
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "drawdown_limit": -0.35,
            "candidate_count": 0,
            "recommended_candidate": None,
            "automation_allowed": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
            "grouped_results": {
                "ok_result_count": 1,
                "dimension_summaries": {"signal_column": {"book": {"result_count": 1, "best_sharpe": 0.1, "best_max_drawdown": -0.5, "median_max_drawdown": -0.5, "pass_count_under_drawdown_limit": 0}}},
            },
        }
    )

    assert "blocked_no_drawdown_safe_candidate" in markdown
    assert "automation_allowed: False" in markdown
    assert "queue_write_allowed: False" in markdown
    assert "broad_daemon_allowed: False" in markdown
    assert "automated_rerun_allowed: False" in markdown
    assert "live_trading_enabled: False" in markdown
    assert "drawdown_gap_to_limit" in markdown
    assert "signal_column" in markdown


def test_repair_cli_summary_surfaces_non_mutating_safety_flags():
    summary = build_cli_summary(
        {
            "repair_status": "blocked_no_drawdown_safe_candidate",
            "candidate_count": 0,
            "recommended_candidate": None,
            "automation_allowed": False,
            "queue_write_allowed": False,
            "broad_daemon_allowed": False,
            "automated_rerun_allowed": False,
            "live_trading_enabled": False,
            "best_available_max_drawdown": -0.36,
            "drawdown_gap_to_limit": 0.01,
        }
    )

    assert summary == {
        "repair_status": "blocked_no_drawdown_safe_candidate",
        "candidate_count": 0,
        "recommended_candidate": None,
        "automation_allowed": False,
        "queue_write_allowed": False,
        "broad_daemon_allowed": False,
        "automated_rerun_allowed": False,
        "live_trading_enabled": False,
        "best_available_max_drawdown": -0.36,
        "drawdown_gap_to_limit": 0.01,
    }


def test_repair_cli_stdout_surfaces_blocker_fields_from_same_written_artifact(tmp_path):
    matrix_path = tmp_path / "matrix.json"
    policy_path = tmp_path / "policy.json"
    json_path = tmp_path / "repair.json"
    markdown_path = tmp_path / "repair.md"
    matrix_path.write_text(
        json.dumps({"results": [_row(sharpe=1.0, drawdown=-0.50), _row(sharpe=0.4, drawdown=-0.36)]}),
        encoding="utf-8",
    )
    policy_path.write_text(json.dumps({"diagnosis_thresholds": {"max_drawdown_limit": -0.35}}), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/write_simulated_portfolio_construction_repair.py",
            "--matrix-path",
            str(matrix_path),
            "--policy-path",
            str(policy_path),
            "--json-path",
            str(json_path),
            "--markdown-path",
            str(markdown_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    stdout_summary = json.loads(result.stdout)
    artifact = json.loads(json_path.read_text(encoding="utf-8"))
    expected_key_order = [
        "repair_status",
        "candidate_count",
        "recommended_candidate",
        "automation_allowed",
        "queue_write_allowed",
        "broad_daemon_allowed",
        "automated_rerun_allowed",
        "live_trading_enabled",
        "best_available_max_drawdown",
        "drawdown_gap_to_limit",
    ]
    assert list(stdout_summary.keys()) == expected_key_order
    assert stdout_summary == {key: artifact[key] for key in expected_key_order}
    assert stdout_summary["repair_status"] == "blocked_no_drawdown_safe_candidate"
    assert stdout_summary["candidate_count"] == 0
    assert stdout_summary["recommended_candidate"] is None
    assert stdout_summary["queue_write_allowed"] is False
    assert stdout_summary["broad_daemon_allowed"] is False
    assert stdout_summary["automation_allowed"] is False
    assert stdout_summary["automated_rerun_allowed"] is False
    assert stdout_summary["live_trading_enabled"] is False
    assert stdout_summary["best_available_max_drawdown"] == -0.36
    assert stdout_summary["drawdown_gap_to_limit"] == 0.01


def test_repair_cli_stdout_matches_same_written_artifact(tmp_path):
    matrix_path = tmp_path / "matrix.json"
    policy_path = tmp_path / "policy.json"
    json_path = tmp_path / "repair.json"
    markdown_path = tmp_path / "repair.md"
    matrix_path.write_text(
        json.dumps({"results": [_row(sharpe=0.5, drawdown=-0.30), _row(sharpe=0.8, drawdown=-0.48)]}),
        encoding="utf-8",
    )
    policy_path.write_text(json.dumps({"diagnosis_thresholds": {"max_drawdown_limit": -0.35}}), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/write_simulated_portfolio_construction_repair.py",
            "--matrix-path",
            str(matrix_path),
            "--policy-path",
            str(policy_path),
            "--json-path",
            str(json_path),
            "--markdown-path",
            str(markdown_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    stdout_summary = json.loads(result.stdout)
    artifact = json.loads(json_path.read_text(encoding="utf-8"))
    expected_key_order = [
        "repair_status",
        "candidate_count",
        "recommended_candidate",
        "automation_allowed",
        "queue_write_allowed",
        "broad_daemon_allowed",
        "automated_rerun_allowed",
        "live_trading_enabled",
        "best_available_max_drawdown",
        "drawdown_gap_to_limit",
    ]
    assert list(stdout_summary.keys()) == expected_key_order
    assert stdout_summary == {key: artifact[key] for key in expected_key_order}
    assert stdout_summary["repair_status"] == "candidate_found"
    assert stdout_summary["queue_write_allowed"] is False
    assert stdout_summary["broad_daemon_allowed"] is False
    assert stdout_summary["automation_allowed"] is False
    assert stdout_summary["automated_rerun_allowed"] is False
    assert stdout_summary["live_trading_enabled"] is False
