from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from factor_lab.research import runner as runner_module
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


def _period(
    signal_date: str,
    start_date: str,
    end_date: str,
    net_return: float,
    benchmark_return: float = 0.0,
) -> dict[str, object]:
    return {
        "signal_date": signal_date,
        "start_date": start_date,
        "end_date": end_date,
        "net_return": net_return,
        "gross_return": net_return,
        "benchmark_return": benchmark_return,
        "holding_count": 50,
        "turnover": 0.1,
        "capacity_violation_count": 0,
        "blocked_trade_count": 0,
        "costs": {"total": 0.0},
    }


def test_window_metrics_require_the_whole_period_to_stay_inside_split() -> None:
    periods = [
        _period("2022-12-20", "2022-12-21", "2022-12-28", 0.01),
        _period("2022-12-27", "2022-12-28", "2023-01-05", 9.0),
        _period("2023-01-04", "2023-01-05", "2023-01-12", 0.02),
        _period("2024-12-27", "2024-12-30", "2025-01-07", 9.0),
    ]

    train = runner_module._window_metrics(
        periods, start="2017-01-01", end="2022-12-31", periods_per_year=252 / 5
    )
    validation = runner_module._window_metrics(
        periods, start="2023-01-01", end="2024-12-31", periods_per_year=252 / 5
    )

    assert train["observations"] == 1
    assert train["net_return"] == 0.01
    assert validation["observations"] == 1
    assert validation["net_return"] == 0.02


def test_final_factor_state_manifest_recovery_and_robustness_fingerprint(
    tmp_path: Path, monkeypatch
) -> None:
    features, execution = _frames()
    feature_path = tmp_path / "features.parquet"
    execution_path = tmp_path / "execution.parquet"
    features.to_parquet(feature_path, index=False)
    execution.to_parquet(execution_path, index=False)
    repository = Path(__file__).resolve().parents[2]
    calls: list[str] = []

    def fake_portfolio_result(factor, validation, *args, **kwargs):
        calls.append(factor.name)
        is_control = factor.name == "earnings_yield_over_pb"
        metrics = {
            "observations": 10,
            "net_return": 0.1,
            "gross_return": 0.1,
            "benchmark_return": 0.0,
            "net_annual_return": 0.12 if is_control else 0.15,
            "benchmark_annual_return": 0.0,
            "net_excess_annual_return": 0.10 if is_control else 0.12,
            "net_sharpe": 1.0 if is_control else 1.2,
            "information_ratio": 0.8,
            "max_drawdown": -0.10,
            "positive_half_year_ratio": 0.75,
            "average_holding_count": 50.0,
            "actual_turnover": 0.1,
            "capacity_violation_count": 0,
            "blocked_trade_count": 0,
            "total_cost": 0.0,
        }
        return {
            "factor_name": factor.name,
            "family": factor.family,
            "factor": factor.to_dict(),
            "frozen_direction": validation.frozen_direction,
            "stage_a": validation.to_dict(),
            "portfolio": {"status": "ok"},
            "windows": {"train": metrics, "validation": metrics, "audit": metrics},
            "gate_passed": True,
            "gate_blockers": [],
            "beats_control": False,
            "validated": False,
        }

    monkeypatch.setattr(runner_module, "_portfolio_result", fake_portfolio_result)
    arguments = {
        "project_root": tmp_path,
        "suite": "next",
        "mode": "full",
        "feature_path": feature_path,
        "execution_path": execution_path,
        "factors_path": repository / "configs" / "factors.json",
        "research_config_path": repository / "configs" / "research.json",
        "run_robustness": False,
    }
    first = run_research(**arguments)
    run_dir = tmp_path / "runtime" / "runs" / first["run_id"]
    challenger = next(row for row in first["stage_b"] if row["factor_name"] != first["control_factor"])
    factor_path = run_dir / "factors" / f"{challenger['factor_name']}.json"
    factor_payload = json.loads(factor_path.read_text(encoding="utf-8"))
    assert factor_payload["result"]["beats_control"] is True
    assert factor_payload["result"]["validated"] is True
    assert runner_module._completed_run_valid(
        run_dir / "summary.json", run_dir, first["run_fingerprint"]
    )

    # A completed checkpoint with a modified artifact must not be trusted or
    # re-blessed from the corrupted per-factor cache.
    factor_payload["result"]["validated"] = False
    factor_path.write_text(json.dumps(factor_payload), encoding="utf-8")
    calls.clear()
    repaired = run_research(**arguments)
    assert repaired["run_id"] == first["run_id"]
    assert len(calls) == len(first["stage_b_selected"])
    repaired_factor = json.loads(factor_path.read_text(encoding="utf-8"))
    assert repaired_factor["result"]["validated"] is True
    assert runner_module._completed_run_valid(
        run_dir / "summary.json", run_dir, repaired["run_fingerprint"]
    )

    with_robustness = run_research(**{**arguments, "run_robustness": True})
    assert with_robustness["run_id"] != first["run_id"]
