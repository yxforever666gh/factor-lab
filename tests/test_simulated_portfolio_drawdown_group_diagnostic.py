import json

from factor_lab.simulated_portfolio_drawdown_group_diagnostic import (
    build_simulated_portfolio_drawdown_group_diagnostic,
    drawdown_group_diagnostic_to_markdown,
    summarize_drawdown_groups,
    write_simulated_portfolio_drawdown_group_diagnostic,
)


def _row(holding=50, freq="monthly", cost=0, sharpe=0.1, drawdown=-0.5, status="ok"):
    return {
        "status": status,
        "holding_count": holding,
        "rebalance_frequency": freq,
        "cost_bps": float(cost),
        "sharpe": sharpe,
        "max_drawdown": drawdown,
        "combo_id": f"{holding}-{freq}-{cost}-{sharpe}",
    }


def test_summarize_drawdown_groups_by_holding_frequency_and_cost():
    matrix = {
        "results": [
            _row(holding=50, freq="monthly", cost=0, sharpe=0.2, drawdown=-0.42),
            _row(holding=50, freq="monthly", cost=30, sharpe=0.5, drawdown=-0.30),
            _row(holding=75, freq="biweekly", cost=0, sharpe=0.1, drawdown=-0.60),
            _row(status="insufficient_data"),
        ]
    }

    grouped = summarize_drawdown_groups(matrix, drawdown_limit=-0.35)

    assert grouped["ok_result_count"] == 3
    holding_50 = grouped["groups"]["holding_count"]["50"]
    assert holding_50["result_count"] == 2
    assert holding_50["best_max_drawdown"] == -0.3
    assert holding_50["median_max_drawdown"] == -0.36
    assert holding_50["best_sharpe"] == 0.5
    assert holding_50["pass_count_under_drawdown_limit"] == 1
    assert holding_50["drawdown_gap_to_limit"] == -0.05
    assert grouped["groups"]["rebalance_frequency"]["monthly"]["result_count"] == 2
    assert grouped["groups"]["cost_bps"]["0.0"]["result_count"] == 2


def test_build_diagnostic_recommends_least_bad_manual_axis_without_automation(tmp_path):
    matrix_path = tmp_path / "matrix.json"
    policy_path = tmp_path / "policy.json"
    matrix_path.write_text(
        json.dumps(
            {
                "matrix_status": "partial",
                "execution": {"result_count": 3},
                "results": [
                    _row(holding=50, freq="monthly", cost=0, sharpe=0.3, drawdown=-0.55),
                    _row(holding=75, freq="monthly", cost=0, sharpe=0.2, drawdown=-0.40),
                    _row(holding=100, freq="biweekly", cost=30, sharpe=0.1, drawdown=-0.50),
                ],
            }
        ),
        encoding="utf-8",
    )
    policy_path.write_text(json.dumps({"diagnosis_thresholds": {"max_drawdown_limit": -0.35}}), encoding="utf-8")

    payload = build_simulated_portfolio_drawdown_group_diagnostic(matrix_path, policy_path)

    assert payload["diagnostic_status"] == "blocked_no_group_under_drawdown_limit"
    assert payload["automation_allowed"] is False
    assert payload["recommended_manual_axis"]["dimension"] == "holding_count"
    assert payload["recommended_manual_axis"]["value"] == "75"
    assert payload["recommended_manual_axis"]["best_max_drawdown"] == -0.4
    assert payload["recommended_manual_axis"]["drawdown_gap_to_limit"] == 0.05


def test_build_diagnostic_marks_review_ready_when_group_has_drawdown_pass(tmp_path):
    matrix_path = tmp_path / "matrix.json"
    policy_path = tmp_path / "policy.json"
    matrix_path.write_text(json.dumps({"results": [_row(holding=50, sharpe=0.6, drawdown=-0.30), _row(holding=75, sharpe=0.4, drawdown=-0.50)]}), encoding="utf-8")
    policy_path.write_text(json.dumps({"diagnosis_thresholds": {"max_drawdown_limit": -0.35}}), encoding="utf-8")

    payload = build_simulated_portfolio_drawdown_group_diagnostic(matrix_path, policy_path)

    assert payload["diagnostic_status"] == "manual_axis_review_ready"
    assert payload["automation_allowed"] is False
    assert payload["recommended_manual_axis"]["value"] == "50"


def test_write_drawdown_group_diagnostic_outputs_json_and_markdown(tmp_path):
    matrix_path = tmp_path / "matrix.json"
    policy_path = tmp_path / "policy.json"
    json_path = tmp_path / "drawdown_group_diagnostic.json"
    markdown_path = tmp_path / "drawdown_group_diagnostic.md"
    matrix_path.write_text(json.dumps({"results": [_row(holding=50, drawdown=-0.40)]}), encoding="utf-8")
    policy_path.write_text(json.dumps({"diagnosis_thresholds": {"max_drawdown_limit": -0.35}}), encoding="utf-8")

    payload = write_simulated_portfolio_drawdown_group_diagnostic(
        matrix_path=matrix_path,
        policy_path=policy_path,
        json_path=json_path,
        markdown_path=markdown_path,
    )

    assert json.loads(json_path.read_text(encoding="utf-8"))["automation_allowed"] is False
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Simulated Portfolio Drawdown Group Diagnostic" in markdown
    assert payload["recommended_manual_axis"]["dimension"] == "holding_count"


def test_markdown_includes_group_fields_and_recommendation():
    markdown = drawdown_group_diagnostic_to_markdown(
        {
            "generated_at_utc": "2026-06-01T00:00:00+00:00",
            "diagnostic_status": "blocked_no_group_under_drawdown_limit",
            "drawdown_limit": -0.35,
            "automation_allowed": False,
            "recommended_manual_axis": {"dimension": "holding_count", "value": "75", "drawdown_gap_to_limit": 0.05},
            "grouped_results": {
                "ok_result_count": 1,
                "groups": {
                    "holding_count": {
                        "75": {
                            "result_count": 1,
                            "best_max_drawdown": -0.4,
                            "median_max_drawdown": -0.4,
                            "best_sharpe": 0.2,
                            "pass_count_under_drawdown_limit": 0,
                            "drawdown_gap_to_limit": 0.05,
                        }
                    }
                },
            },
        }
    )

    assert "automation_allowed: False" in markdown
    assert "recommended_manual_axis" in markdown
    assert "drawdown_gap_to_limit" in markdown
