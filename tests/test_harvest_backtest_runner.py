import pandas as pd

from factor_lab.harvest_backtest_runner import apply_plan_filters, run_plan_backtest


def _df():
    return pd.DataFrame({
        "date": ["2021-01-01"] * 4 + ["2021-01-08"] * 4,
        "ticker": ["a", "b", "c", "d"] * 2,
        "industry_relative_book_yield": [4, 3, 2, 1, 4, 3, 2, 1],
        "industry_relative_earnings_yield": [1, 2, 3, 4, 1, 2, 3, 4],
        "earnings_yield": [1, 1, 1, 1, 1, 1, 1, 1],
        "forward_return_5d": [0.1, 0.05, -0.01, -0.02, 0.08, 0.04, -0.01, -0.02],
        "volatility_20": [0.1, 0.2, 0.9, 1.0, 0.1, 0.2, 0.9, 1.0],
        "turnover": [10, 8, 1, 1, 10, 8, 1, 1],
    })


def test_apply_plan_filters_applies_quantile_filters():
    filtered = apply_plan_filters(_df(), [{"type": "add_filter", "field": "volatility_20", "operator": "<=", "quantile": 0.6}])
    assert set(filtered["ticker"]) == {"a", "b"}


def test_run_plan_backtest_returns_real_metrics(tmp_path):
    dataset = tmp_path / "dataset.csv"
    _df().to_csv(dataset, index=False)
    plan = {
        "dataset_path": str(dataset),
        "actions": [
            {"type": "add_filter", "field": "volatility_20", "operator": "<=", "quantile": 0.6},
            {"type": "set_signal_columns", "signal_columns": ["industry_relative_book_yield"]},
            {"type": "restrict_costs", "cost_bps_values": [30]},
            {"type": "set_holding_counts", "holding_counts": [1, 2]},
        ],
    }
    payload = run_plan_backtest(plan, output_dir=tmp_path / "out")
    assert payload["status"] == "ok"
    assert payload["execution"]["executed_count"] > 0
    assert (tmp_path / "out/result.json").exists()
