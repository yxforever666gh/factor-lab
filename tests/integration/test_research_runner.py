from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from factor_lab.research.runner import run_research


def _frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.DatetimeIndex(
        [
            *pd.bdate_range("2017-01-03", periods=25),
            *pd.bdate_range("2023-01-03", periods=25),
            *pd.bdate_range("2025-01-03", periods=31),
        ]
    )
    tickers = [f"{index:06d}.SZ" for index in range(1, 13)]
    rows: list[dict[str, object]] = []
    for day_index, date in enumerate(dates):
        for ticker_index, ticker in enumerate(tickers):
            value = (ticker_index + 1) / len(tickers)
            rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "book_yield": value,
                    "earnings_yield": value * 0.8 + 0.01,
                    "pb": 1.0 + value,
                    "roe": value * 0.2,
                    "volatility_20": 0.3 - value * 0.1,
                    "turnover_rate": 0.2 - value * 0.05,
                    "forward_return_5d_open": value * 0.02 + day_index * 1e-8,
                    "st_filter_status": "verified",
                }
            )
    features = pd.DataFrame(rows)
    execution_dates = dates.append(pd.bdate_range(dates[-1] + pd.Timedelta(days=1), periods=6))
    execution_rows: list[dict[str, object]] = []
    for day_index, date in enumerate(execution_dates):
        for ticker_index, ticker in enumerate(tickers):
            execution_rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "open_adj": 10.0 + day_index * 0.01 + ticker_index * 0.001,
                    "adv_20": 1_000_000_000.0,
                    "volatility_20": 0.02,
                    "eligible": True,
                    "universe_member": True,
                    "is_one_price_limit_up": False,
                    "is_one_price_limit_down": False,
                }
            )
    return features, pd.DataFrame(execution_rows)


def test_full_runner_writes_resumable_two_stage_outputs(tmp_path: Path) -> None:
    features, execution = _frames()
    feature_path = tmp_path / "features.parquet"
    execution_path = tmp_path / "execution.parquet"
    features.to_parquet(feature_path, index=False)
    execution.to_parquet(execution_path, index=False)
    repository = Path(__file__).resolve().parents[2]

    result = run_research(
        project_root=tmp_path,
        suite="next",
        mode="full",
        feature_path=feature_path,
        execution_path=execution_path,
        factors_path=repository / "configs" / "factors.json",
        research_config_path=repository / "configs" / "research.json",
        run_robustness=False,
    )

    assert result["status"] == "completed"
    assert result["control_factor"] == "earnings_yield_over_pb"
    assert 1 <= len(result["stage_b_selected"]) <= 4
    assert len(result["stage_a"]) == 5
    run_dir = tmp_path / "runtime" / "runs" / result["run_id"]
    assert (run_dir / "summary.json").is_file()
    assert (run_dir / "report.md").is_file()
    assert (run_dir / "manifest.json").is_file()
    assert json.loads((tmp_path / "runtime" / "runs" / "latest.json").read_text())["run_id"] == result["run_id"]

    resumed = run_research(
        project_root=tmp_path,
        suite="next",
        mode="full",
        feature_path=feature_path,
        execution_path=execution_path,
        factors_path=repository / "configs" / "factors.json",
        research_config_path=repository / "configs" / "research.json",
        run_robustness=False,
    )
    assert resumed["run_fingerprint"] == result["run_fingerprint"]

