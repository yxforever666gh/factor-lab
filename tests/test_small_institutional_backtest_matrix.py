import json

import pandas as pd

from factor_lab.small_institutional_backtest_matrix import (
    build_small_institutional_backtest_matrix,
    deterministic_combo_id,
    run_long_only_backtest,
    small_institutional_backtest_matrix_to_markdown,
    write_small_institutional_backtest_matrix,
)


def _dataset():
    rows = []
    for date in pd.date_range("2020-01-31", periods=6, freq="ME"):
        for idx in range(4):
            rows.append(
                {
                    "date": str(date.date()),
                    "ticker": f"00000{idx}.SZ",
                    "signal": 10 - idx,
                    "signal_alt": 20 - idx,
                    "forward_return_5d": 0.04 - idx * 0.01,
                    "industry": "A" if idx < 2 else "B",
                }
            )
    return pd.DataFrame(rows)


def test_run_long_only_backtest_selects_top_n_and_computes_metrics():
    result = run_long_only_backtest(
        _dataset(),
        signal_column="signal",
        start_date="2020-01-01",
        end_date="2020-12-31",
        holding_count=2,
        rebalance_frequency="monthly",
        cost_bps=0,
    )

    assert result["status"] == "ok"
    assert result["holding_count"] == 2
    assert result["rebalance_count"] == 6
    assert result["period_return_mean"] == 0.035
    assert result["total_return"] > 0
    assert result["max_drawdown"] == 0


def test_run_long_only_backtest_applies_turnover_costs():
    no_cost = run_long_only_backtest(_dataset(), signal_column="signal", start_date="2020-01-01", end_date="2020-12-31", holding_count=2, rebalance_frequency="monthly", cost_bps=0)
    with_cost = run_long_only_backtest(_dataset(), signal_column="signal", start_date="2020-01-01", end_date="2020-12-31", holding_count=2, rebalance_frequency="monthly", cost_bps=60)

    assert with_cost["total_return"] < no_cost["total_return"]
    assert with_cost["cost_bps"] == 60


def test_run_long_only_backtest_reports_insufficient_data_for_missing_window():
    result = run_long_only_backtest(
        _dataset(),
        signal_column="signal",
        start_date="2022-01-01",
        end_date="2022-12-31",
        holding_count=2,
        rebalance_frequency="monthly",
        cost_bps=0,
    )

    assert result["status"] == "insufficient_data"
    assert result["rebalance_count"] == 0


def test_build_small_institutional_backtest_matrix_runs_parameter_grid(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    _dataset().to_csv(dataset_path, index=False)

    payload = build_small_institutional_backtest_matrix(
        dataset_path=dataset_path,
        signal_column="signal",
        year_windows=[{"label": "2020", "start_date": "2020-01-01", "end_date": "2020-12-31"}],
        holding_counts=[2, 3],
        rebalance_frequencies=["monthly", "biweekly"],
        cost_bps_values=[0, 30],
    )

    assert payload["matrix_status"] == "ok"
    assert payload["parameter_grid"]["combination_count"] == 8
    assert len(payload["results"]) == 8
    assert payload["summary"]["ok_count"] == 8
    assert payload["best_result"]["status"] == "ok"


def test_write_small_institutional_backtest_matrix_writes_json_and_markdown(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    _dataset().to_csv(dataset_path, index=False)
    json_path = tmp_path / "matrix.json"
    markdown_path = tmp_path / "matrix.md"

    payload = write_small_institutional_backtest_matrix(
        dataset_path=dataset_path,
        signal_column="signal",
        year_windows=[{"label": "2020", "start_date": "2020-01-01", "end_date": "2020-12-31"}],
        holding_counts=[2],
        rebalance_frequencies=["monthly"],
        cost_bps_values=[0],
        json_path=json_path,
        markdown_path=markdown_path,
    )

    assert payload["matrix_status"] == "ok"
    assert json.loads(json_path.read_text(encoding="utf-8"))["matrix_status"] == "ok"
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Small Institutional Backtest Matrix" in markdown
    assert "Best result" in markdown


def test_markdown_includes_insufficient_data_counts():
    markdown = small_institutional_backtest_matrix_to_markdown(
        {
            "generated_at_utc": "2026-05-12T00:00:00+00:00",
            "matrix_status": "partial",
            "summary": {"ok_count": 1, "insufficient_data_count": 2},
            "parameter_grid": {"combination_count": 3},
            "best_result": {"label": "2020", "total_return": 0.1, "sharpe": 1.2},
            "results": [],
        }
    )

    assert "insufficient_data_count" in markdown
    assert "2020" in markdown


def test_deterministic_combo_id_is_stable_and_distinguishes_signal():
    combo = {
        "signal_column": "signal",
        "label": "2020",
        "start_date": "2020-01-01",
        "end_date": "2020-12-31",
        "holding_count": 2,
        "rebalance_frequency": "monthly",
        "cost_bps": 0,
    }

    first = deterministic_combo_id(combo)
    assert first == deterministic_combo_id(dict(reversed(list(combo.items()))))
    changed = dict(combo)
    changed["signal_column"] = "signal_alt"
    assert first != deterministic_combo_id(changed)


def test_build_matrix_supports_multiple_signals_and_max_combination_cap(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    _dataset().to_csv(dataset_path, index=False)

    payload = build_small_institutional_backtest_matrix(
        dataset_path=dataset_path,
        signal_columns=["signal", "signal_alt"],
        year_windows=[{"label": "2020", "start_date": "2020-01-01", "end_date": "2020-12-31"}],
        holding_counts=[2, 3],
        rebalance_frequencies=["monthly"],
        cost_bps_values=[0],
        max_combinations=3,
    )

    assert payload["execution"]["planned_count"] == 4
    assert payload["execution"]["executed_count"] == 3
    assert payload["execution"]["capped"] is True
    assert len(payload["results"]) == 3
    assert {row["signal_column"] for row in payload["results"]} <= {"signal", "signal_alt"}
    assert all(row.get("combo_id") for row in payload["results"])


def test_build_matrix_dry_run_lists_combos_without_execution(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    _dataset().to_csv(dataset_path, index=False)

    payload = build_small_institutional_backtest_matrix(
        dataset_path=dataset_path,
        signal_columns=["signal"],
        year_windows=[{"label": "2020", "start_date": "2020-01-01", "end_date": "2020-12-31"}],
        holding_counts=[2],
        rebalance_frequencies=["monthly"],
        cost_bps_values=[0],
        dry_run=True,
    )

    assert payload["matrix_status"] == "dry_run"
    assert payload["execution"]["planned_count"] == 1
    assert payload["execution"]["executed_count"] == 0
    assert payload["planned_combinations"][0]["combo_id"]
    assert payload["results"] == []


def test_build_matrix_resumes_existing_completed_combo(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    _dataset().to_csv(dataset_path, index=False)
    combo = {
        "signal_column": "signal",
        "label": "2020",
        "start_date": "2020-01-01",
        "end_date": "2020-12-31",
        "holding_count": 2,
        "rebalance_frequency": "monthly",
        "cost_bps": 0.0,
    }
    combo_id = deterministic_combo_id(combo)
    existing_path = tmp_path / "existing.json"
    existing_path.write_text(
        json.dumps({"results": [{"combo_id": combo_id, "status": "ok", "total_return": 0.1, **combo}]}),
        encoding="utf-8",
    )

    payload = build_small_institutional_backtest_matrix(
        dataset_path=dataset_path,
        signal_columns=["signal"],
        year_windows=[{"label": "2020", "start_date": "2020-01-01", "end_date": "2020-12-31"}],
        holding_counts=[2],
        rebalance_frequencies=["monthly"],
        cost_bps_values=[0],
        resume_from_path=existing_path,
    )

    assert payload["execution"]["skipped_existing_count"] == 1
    assert payload["execution"]["executed_count"] == 0
    assert payload["results"][0]["combo_id"] == combo_id
