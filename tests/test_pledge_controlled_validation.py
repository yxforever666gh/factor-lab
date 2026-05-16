from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_lab.pledge_controlled_validation import (
    PledgeControlledValidationConfig,
    build_pledge_controlled_validation,
)


def _write_run_dir(tmp_path: Path, *, weak_second_split: bool = False) -> Path:
    run = tmp_path / "run"
    run.mkdir()
    rows = []
    for year in [2020, 2021, 2022, 2023]:
        for month in range(1, 4):
            date = f"{year}{month:02d}28"
            for i in range(30):
                signal = i / 29
                ret = signal * 0.05 - 0.01
                if weak_second_split and year >= 2022:
                    ret = -ret
                rows.append(
                    {
                        "date": date,
                        "ticker": f"{i:06d}.SZ",
                        "forward_return_5d": ret,
                        "high_pledge_record_count": signal,
                        "industry_relative_book_yield": (i % 3) * 0.01,
                        "roe": (i % 5) * 0.01,
                        "turnover": ((i * 7) % 30) / 30,
                    }
                )
    pd.DataFrame(rows).to_csv(run / "dataset.csv", index=False)
    (run / "results.json").write_text(json.dumps([{"factor_name": "high_pledge_record_count"}]), encoding="utf-8")
    (run / "bucket_aware_portfolio_results.json").write_text(json.dumps([{"spread_mean": 0.01, "observations": 12}]), encoding="utf-8")
    (run / "task_state.json").write_text(json.dumps({"status": "finished", "task_id": "x"}), encoding="utf-8")
    (run / "timing.json").write_text(json.dumps({}), encoding="utf-8")
    return run


def test_pledge_controlled_validation_passes_when_spread_splits_and_coverage_are_good(tmp_path: Path) -> None:
    run = _write_run_dir(tmp_path)
    report = build_pledge_controlled_validation(
        run,
        db_path=tmp_path / "missing.db",
        source_report_path=tmp_path / "missing_source.json",
        config=PledgeControlledValidationConfig(min_observations=10, min_nonnull_rows=100, min_nonnull_tickers=20),
    )
    assert report["decision"]["decision"] == "pledge_validation_pass_prepare_single_followup_plan"
    assert report["recomputed_bucket_aware_result"]["spread_mean"] > report["benchmark"]["value_quality_no_distress_bucket_spread"]
    assert report["split_diagnostics"]["2020_2021"]["spread_mean"] > 0
    assert report["split_diagnostics"]["2022_2023"]["spread_mean"] > 0


def test_pledge_controlled_validation_stops_when_recent_split_breaks(tmp_path: Path) -> None:
    run = _write_run_dir(tmp_path, weak_second_split=True)
    report = build_pledge_controlled_validation(
        run,
        db_path=tmp_path / "missing.db",
        source_report_path=tmp_path / "missing_source.json",
        config=PledgeControlledValidationConfig(min_observations=10, min_nonnull_rows=100, min_nonnull_tickers=20),
    )
    assert report["decision"]["decision"] == "stop_pledge_not_robust"
    assert "split_2022_2023_not_positive" in report["decision"]["reasons"]
