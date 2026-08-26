from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd
import pytest

from factor_lab.research_lite import _ranking_value, run_research_lite


PORTFOLIO_METRICS = {
    "benchmark_return",
    "excess_return",
    "gross_annual_return",
    "net_annual_return",
    "net_sharpe",
    "max_drawdown",
    "actual_turnover",
    "total_cost",
}


def _write_feature_frame(path: Path) -> None:
    """Write a small panel with the same columns as the real A-share store."""

    dates = pd.bdate_range("2024-01-02", periods=32)
    rows: list[dict[str, Any]] = []
    for ticker_index in range(50):
        ticker = f"{ticker_index + 1:06d}.SZ"
        ticker_score = (ticker_index - 24.5) / 50.0
        daily_return = (
            0.0003
            + ticker_score * 0.0008
            + 0.002 * np.sin(np.arange(len(dates), dtype=float) / 4.0)
        )
        opens = 10.0 * np.cumprod(1.0 + daily_return)
        for date_index, (trade_date, opening) in enumerate(zip(dates, opens)):
            rows.append(
                {
                    "date": trade_date,
                    "ticker": ticker,
                    "open_adj": float(opening),
                    "close_adj": float(opening * (1.0 + daily_return[date_index] / 3.0)),
                    "close": float(opening),
                    "adv_20": 200_000_000.0 + ticker_index * 1_000_000.0,
                    "volatility_20": 0.015 + ticker_index / 100_000.0,
                    "eligible": True,
                    "universe_member": True,
                    "is_one_price_limit_up": False,
                    "is_one_price_limit_down": False,
                    "momentum_20": ticker_score + date_index / 1_000.0,
                    "book_yield": (50 - ticker_index) / 100.0,
                    "earnings_yield": (ticker_index + 1) / 200.0,
                }
            )

    frame = pd.DataFrame(rows).sort_values(["ticker", "date"]).reset_index(drop=True)
    grouped_open = frame.groupby("ticker", sort=False)["open_adj"]
    frame["forward_return_5d_open"] = grouped_open.shift(-6) / grouped_open.shift(-1) - 1.0
    frame["forward_return_5d"] = frame["forward_return_5d_open"]
    frame = frame.sort_values(["date", "ticker"]).reset_index(drop=True)
    frame.to_parquet(path, index=False)


def _config() -> Mapping[str, Any]:
    return {
        "portfolio": {
            "mode": "long_only",
            "capital": 50_000_000,
            "holding_days": 5,
            "rebalance_every_days": 5,
            "position_count": 10,
            "target_weight": 0.10,
            "max_adv_participation": 0.05,
            "open_column": "open_adj",
            "adv_column": "adv_20",
            "volatility_column": "volatility_20",
        },
        "costs": {
            "commission_rate": 0.0003,
            "slippage_bps_per_side": 5.0,
            "stamp_duty_before_2023_08_28": 0.001,
            "stamp_duty_from_2023_08_28": 0.0005,
            "exchange_handling_rate": 0.0000341,
            "transfer_fee_rate": 0.00001,
            "impact_coefficient": 0.5,
        },
    }


def _factors() -> list[Mapping[str, Any]]:
    return [
        {
            "name": "momentum_20",
            "family": "momentum",
            "expression": "momentum_20",
            "direction": 1,
            "allow_in_long_only": True,
        },
        {
            "name": "book_yield",
            "family": "value",
            "expression": "book_yield",
            "direction": 1,
            "allow_in_long_only": True,
        },
        {
            "name": "earnings_yield_diagnostic",
            "family": "value",
            "expression": "earnings_yield",
            "direction": 1,
            "allow_in_long_only": False,
        },
    ]


def test_research_lite_writes_diagnostic_outputs_without_promotion(tmp_path: Path) -> None:
    feature_path = tmp_path / "features.parquet"
    output_dir = tmp_path / "research-lite"
    _write_feature_frame(feature_path)

    result = run_research_lite(
        feature_path,
        _config(),
        _factors(),
        output_dir,
        execution_path=None,
        resume=True,
    )

    assert isinstance(result, Mapping)
    assert result["evidence_class"] == "historical_diagnostic"
    assert result["promotion_triggered"] is False
    assert result["candidate_written"] is False

    output_paths = {
        name: Path(result[f"{name}_path"])
        for name in ("summary", "report", "manifest")
    }
    assert all(path.is_file() for path in output_paths.values())
    assert all(path.parent == output_dir for path in output_paths.values())

    summary = json.loads(output_paths["summary"].read_text(encoding="utf-8"))
    assert summary["evidence_class"] == "historical_diagnostic"
    assert summary["promotion_triggered"] is False
    assert summary["candidate_written"] is False
    assert summary["investment_claim_allowed"] is False

    by_name = {row["name"]: row for row in summary["results"]}
    assert set(by_name) == {
        "momentum_20",
        "book_yield",
        "earnings_yield_diagnostic",
    }

    tradable = [row for row in by_name.values() if row["allow_in_long_only"]]
    assert tradable
    for row in tradable:
        assert {"rank_ic_mean", "top_bottom_spread_mean"} <= set(row["diagnostics"])
        assert row["portfolio"]["status"] == "ok"
        assert PORTFOLIO_METRICS <= set(row["portfolio"])

    diagnostic = by_name["earnings_yield_diagnostic"]
    assert diagnostic["allow_in_long_only"] is False
    assert diagnostic["portfolio"]["status"] == "diagnostic_only"
    assert not (PORTFOLIO_METRICS & set(diagnostic["portfolio"]))

    report = output_paths["report"].read_text(encoding="utf-8")
    assert "historical_diagnostic" in report
    assert "promotion" in report.lower()

    manifest = json.loads(output_paths["manifest"].read_text(encoding="utf-8"))
    assert manifest["algorithm"] == "sha256"
    manifest_files = {Path(row["path"]).name: row for row in manifest["files"]}
    assert output_paths["summary"].name in manifest_files
    assert output_paths["report"].name in manifest_files
    for row in manifest_files.values():
        assert len(row["sha256"]) == 64

    summary_before_resume = output_paths["summary"].read_bytes()
    resumed = run_research_lite(
        feature_path,
        _config(),
        _factors(),
        output_dir,
        execution_path=None,
        resume=True,
    )
    assert Path(resumed["summary_path"]).read_bytes() == summary_before_resume
    assert resumed["evidence_class"] == "historical_diagnostic"
    assert resumed["promotion_triggered"] is False


def test_research_lite_rejects_label_entry_open_as_a_factor_input(tmp_path: Path) -> None:
    feature_path = tmp_path / "features.parquet"
    _write_feature_frame(feature_path)

    with pytest.raises(ValueError, match="forbidden future/label fields.*label_entry_open"):
        run_research_lite(
            feature_path,
            _config(),
            [
                {
                    "name": "leaking_factor",
                    "family": "invalid",
                    "expression": "label_entry_open / open_adj",
                    "direction": 1,
                    "allow_in_long_only": True,
                }
            ],
            tmp_path / "leaking-output",
        )


def test_research_lite_rejects_execution_store_missing_a_feature_date(
    tmp_path: Path,
) -> None:
    feature_path = tmp_path / "features.parquet"
    execution_path = tmp_path / "execution.parquet"
    _write_feature_frame(feature_path)
    frame = pd.read_parquet(feature_path)
    first_date = pd.to_datetime(frame["date"]).min()
    execution = frame.loc[
        pd.to_datetime(frame["date"]) != first_date,
        [
            "date",
            "ticker",
            "open_adj",
            "adv_20",
            "volatility_20",
            "eligible",
            "universe_member",
            "is_one_price_limit_up",
            "is_one_price_limit_down",
        ],
    ]
    execution.to_parquet(execution_path, index=False)

    with pytest.raises(ValueError, match="execution store does not cover 1 feature dates"):
        run_research_lite(
            feature_path,
            _config(),
            [_factors()[0]],
            tmp_path / "missing-execution-date-output",
            execution_path=execution_path,
        )


def test_research_lite_strategy_files_do_not_collide_after_name_sanitizing(
    tmp_path: Path,
) -> None:
    feature_path = tmp_path / "features.parquet"
    output_dir = tmp_path / "name-collision-output"
    _write_feature_frame(feature_path)
    factors = [
        {
            "name": "a/b",
            "family": "diagnostic",
            "expression": "momentum_20",
            "allow_in_long_only": False,
        },
        {
            "name": "a?b",
            "family": "diagnostic",
            "expression": "book_yield",
            "allow_in_long_only": False,
        },
    ]

    result = run_research_lite(
        feature_path,
        _config(),
        factors,
        output_dir,
        resume=True,
    )

    strategy_paths = sorted((output_dir / "strategies").glob("*.json"))
    assert len(strategy_paths) == 2
    assert strategy_paths[0].name != strategy_paths[1].name
    persisted_names = {
        json.loads(path.read_text(encoding="utf-8"))["factor"]["name"]
        for path in strategy_paths
    }
    assert persisted_names == {"a/b", "a?b"}
    assert {row["name"] for row in result["results"]} == persisted_names


def test_research_lite_zero_ranking_value_is_not_treated_as_missing() -> None:
    assert _ranking_value(0) == 0.0
    assert _ranking_value(0.0) == 0.0
    assert _ranking_value("0") == 0.0
    assert _ranking_value(None) == -999.0
    assert _ranking_value(float("nan")) == -999.0
