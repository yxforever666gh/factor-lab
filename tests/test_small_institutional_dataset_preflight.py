import json

import pandas as pd

from factor_lab.small_institutional_dataset_preflight import (
    build_small_institutional_dataset_preflight,
    small_institutional_dataset_preflight_to_markdown,
    write_small_institutional_dataset_preflight,
)


def _dataset(path):
    rows = []
    for date in pd.date_range("2020-01-31", periods=3, freq="ME"):
        for idx in range(3):
            rows.append(
                {
                    "date": str(date.date()),
                    "ticker": f"00000{idx}.SZ",
                    "signal": idx,
                    "forward_return_5d": 0.01,
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)


def test_dataset_preflight_reports_missing_dataset(tmp_path):
    payload = build_small_institutional_dataset_preflight(
        dataset_path=tmp_path / "missing.csv",
        signal_columns=["signal"],
        year_windows=[{"label": "2020", "start_date": "2020-01-01", "end_date": "2020-12-31"}],
        holding_counts=[2],
        rebalance_frequencies=["monthly"],
        cost_bps_values=[0],
    )

    assert payload["preflight_status"] == "blocked"
    assert payload["dataset"]["exists"] is False
    assert payload["next_action"] == "provide_backtest_dataset"


def test_dataset_preflight_classifies_window_coverage_and_combinations(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    _dataset(dataset_path)

    payload = build_small_institutional_dataset_preflight(
        dataset_path=dataset_path,
        signal_columns=["signal"],
        year_windows=[
            {"label": "2020", "start_date": "2020-01-01", "end_date": "2020-12-31"},
            {"label": "2021", "start_date": "2021-01-01", "end_date": "2021-12-31"},
        ],
        holding_counts=[2, 3],
        rebalance_frequencies=["monthly"],
        cost_bps_values=[0, 30],
    )

    assert payload["preflight_status"] == "partial"
    assert payload["dataset"]["row_count"] == 9
    assert payload["dataset"]["ticker_count"] == 3
    assert payload["dataset"]["min_date"] == "2020-01-31"
    assert payload["windows"][0]["status"] == "ready"
    assert payload["windows"][1]["status"] == "insufficient_date_coverage"
    assert payload["estimated_combinations"]["total"] == 8
    assert payload["estimated_combinations"]["ready"] == 4


def test_dataset_preflight_blocks_missing_signal_columns(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    _dataset(dataset_path)

    payload = build_small_institutional_dataset_preflight(
        dataset_path=dataset_path,
        signal_columns=["signal", "missing_signal"],
        year_windows=[{"label": "2020", "start_date": "2020-01-01", "end_date": "2020-12-31"}],
        holding_counts=[2],
        rebalance_frequencies=["monthly"],
        cost_bps_values=[0],
    )

    assert payload["preflight_status"] == "blocked"
    assert "missing_signal" in payload["signals"]["missing_columns"]
    assert payload["next_action"] == "repair_dataset_columns"


def test_write_dataset_preflight_writes_json_and_markdown(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    _dataset(dataset_path)
    json_path = tmp_path / "preflight.json"
    markdown_path = tmp_path / "preflight.md"

    payload = write_small_institutional_dataset_preflight(
        dataset_path=dataset_path,
        signal_columns=["signal"],
        year_windows=[{"label": "2020", "start_date": "2020-01-01", "end_date": "2020-12-31"}],
        holding_counts=[2],
        rebalance_frequencies=["monthly"],
        cost_bps_values=[0],
        json_path=json_path,
        markdown_path=markdown_path,
    )

    assert payload["preflight_status"] == "ready"
    assert json.loads(json_path.read_text(encoding="utf-8"))["preflight_status"] == "ready"
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Small Institutional Dataset Preflight" in markdown
    assert "ready" in markdown


def test_dataset_preflight_markdown_includes_next_action():
    markdown = small_institutional_dataset_preflight_to_markdown(
        {
            "generated_at_utc": "2026-05-12T00:00:00+00:00",
            "preflight_status": "partial",
            "next_action": "extend_backtest_dataset",
            "dataset": {"exists": True, "row_count": 10, "ticker_count": 3, "min_date": "2020-01-01", "max_date": "2020-02-01"},
            "signals": {"missing_columns": []},
            "estimated_combinations": {"total": 2, "ready": 1},
            "windows": [{"label": "2021", "status": "insufficient_date_coverage"}],
        }
    )

    assert "extend_backtest_dataset" in markdown
    assert "insufficient_date_coverage" in markdown
