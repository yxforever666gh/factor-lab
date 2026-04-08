import pandas as pd

from factor_lab.analytics import factor_correlation_matrix, evaluate_time_splits, evaluate_rolling_windows
from factor_lab.evaluation import evaluate_factor
from factor_lab.factors import FactorDefinition


def _sample_frame():
    rows = []
    for d in range(12):
        for i in range(10):
            rows.append({
                "date": pd.Timestamp("2026-01-01") + pd.Timedelta(days=d),
                "ticker": f"T{i}",
                "forward_return_5d": float(i - 5) / 100,
                "momentum_20": float(i + d),
            })
    return pd.DataFrame(rows)


def test_factor_correlation_matrix_accepts_factor_value_cache():
    frame = _sample_frame()
    cache = {"mom_20": frame["momentum_20"]}

    corr = factor_correlation_matrix(frame, factor_value_cache=cache)

    assert list(corr.columns) == ["mom_20"]
    assert corr.loc["mom_20", "mom_20"] == 1.0


def test_split_and_rolling_accept_precomputed_factor_values():
    frame = _sample_frame()
    definition = FactorDefinition(name="mom_20", expression="momentum_20")
    values = frame["momentum_20"]
    thresholds = {"min_rank_ic": -1.0, "min_top_bottom_spread": -1.0}

    split_rows = evaluate_time_splits(frame, definition, thresholds, evaluate_factor, factor_values=values)
    rolling_rows = evaluate_rolling_windows(frame, definition, thresholds, evaluate_factor, factor_values=values, config={"window_size": 8, "step_size": 4})

    assert len(split_rows) == 2
    assert len(rolling_rows) >= 1
    assert all(row["factor_name"] == "mom_20" for row in split_rows + rolling_rows)
