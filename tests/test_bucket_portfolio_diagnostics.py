import pandas as pd

from factor_lab.bucket_portfolio_diagnostics import best_bucket_pair_spread


def test_best_bucket_pair_spread_finds_upper_middle_long_vs_low_short():
    profile = []
    means = {0: -0.002, 1: 0.001, 2: 0.003, 3: 0.004, 4: 0.001}
    for q, value in means.items():
        profile.append({"date": "2021-01-01", "quantile": q, "mean_forward_return_5d": value})
        profile.append({"date": "2021-01-08", "quantile": q, "mean_forward_return_5d": value})

    result = best_bucket_pair_spread(profile)

    assert result["long_quantile"] == 3
    assert result["short_quantile"] == 0
    assert result["spread_mean"] == 0.006
    assert result["recommendation"] == "long_best_bucket_short_worst_bucket"
