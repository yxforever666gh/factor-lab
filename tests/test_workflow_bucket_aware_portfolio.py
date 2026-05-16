import json
from pathlib import Path

import pandas as pd

from factor_lab.factors import FactorDefinition
from factor_lab.workflow import evaluate_bucket_aware_portfolios


def test_evaluate_bucket_aware_portfolios_returns_empty_for_default_config():
    frame = pd.DataFrame({"date": ["2021-01-01"], "ticker": ["a"], "forward_return_5d": [0.0]})

    assert evaluate_bucket_aware_portfolios(frame, [FactorDefinition("x", "forward_return_5d")], {}, {}) == []


def test_evaluate_bucket_aware_portfolios_uses_factor_cache_and_configured_buckets():
    rows = []
    returns = {0: -0.002, 1: 0.001, 2: 0.003, 3: 0.004, 4: 0.001}
    values = []
    for date in ["2021-01-01", "2021-01-08"]:
        for idx in range(50):
            bucket = idx // 10
            rows.append({"date": date, "ticker": f"s{idx:02d}", "forward_return_5d": returns[bucket]})
            values.append(idx)
    frame = pd.DataFrame(rows)
    definition = FactorDefinition("value_quality", "book_yield + roe")

    result = evaluate_bucket_aware_portfolios(
        frame,
        [definition],
        {"value_quality": pd.Series(values)},
        {"portfolio_construction": {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 0}, "thresholds": {"min_bucket_spread": 0.001}},
    )

    assert result[0]["factor_name"] == "value_quality"
    assert result[0]["spread_mean"] == 0.006
    assert result[0]["pass_gate"] is True
