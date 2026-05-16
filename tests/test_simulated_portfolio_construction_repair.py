import json

from factor_lab.simulated_portfolio_construction_repair import (
    build_simulated_portfolio_construction_repair,
    group_matrix_results,
    repair_to_markdown,
    write_simulated_portfolio_construction_repair,
)


def _row(signal="book", holding=50, freq="monthly", cost=0, sharpe=0.1, drawdown=-0.5, status="ok"):
    return {
        "status": status,
        "signal_column": signal,
        "holding_count": holding,
        "rebalance_frequency": freq,
        "cost_bps": float(cost),
        "sharpe": sharpe,
        "max_drawdown": drawdown,
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


def test_build_repair_recommends_best_drawdown_safe_candidate_when_present(tmp_path):
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
    assert payload["recommended_candidate"]["sharpe"] == 0.4
    assert payload["recommended_candidate"]["max_drawdown"] == -0.30


def test_build_repair_uses_sharpe_then_return_as_tie_breakers_after_drawdown(tmp_path):
    matrix_path = tmp_path / "matrix.json"
    policy_path = tmp_path / "policy.json"
    rows = [
        {**_row(sharpe=0.6, drawdown=-0.30), "total_return": 0.30, "combo_id": "lower-sharpe"},
        {**_row(sharpe=0.9, drawdown=-0.30), "total_return": 0.10, "combo_id": "higher-sharpe"},
        {**_row(sharpe=0.9, drawdown=-0.30), "total_return": 0.25, "combo_id": "higher-return"},
    ]
    matrix_path.write_text(json.dumps({"results": rows}), encoding="utf-8")
    policy_path.write_text(json.dumps({"diagnosis_thresholds": {"max_drawdown_limit": -0.35}}), encoding="utf-8")

    payload = build_simulated_portfolio_construction_repair(matrix_path, policy_path)

    assert payload["recommended_candidate"]["combo_id"] == "higher-return"


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
            "grouped_results": {
                "ok_result_count": 1,
                "dimension_summaries": {"signal_column": {"book": {"result_count": 1, "best_sharpe": 0.1, "best_max_drawdown": -0.5, "median_max_drawdown": -0.5, "pass_count_under_drawdown_limit": 0}}},
            },
        }
    )

    assert "blocked_no_drawdown_safe_candidate" in markdown
    assert "automation_allowed: False" in markdown
    assert "signal_column" in markdown
