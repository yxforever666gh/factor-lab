import pandas as pd

from factor_lab.portfolio_mechanism_diagnostics import (
    build_quantile_return_profile,
    diagnose_ic_spread_alignment,
    summarize_quantile_profile,
)


def test_quantile_profile_shows_top_minus_bottom_when_signal_monotonic():
    rows = []
    for date in ["2021-01-01", "2021-01-08"]:
        for idx in range(20):
            rows.append({"date": date, "ticker": f"s{idx:02d}", "factor_value": idx, "forward_return_5d": idx / 1000})
    frame = pd.DataFrame(rows)

    profile = build_quantile_return_profile(frame, quantiles=5)
    summary = summarize_quantile_profile(profile)

    assert summary["top_quantile"] == 4
    assert summary["bottom_quantile"] == 0
    assert summary["top_minus_bottom_mean"] > 0
    assert summary["monotonic_slope"] > 0


def test_quantile_profile_exposes_middle_hump_not_monotonic():
    rows = []
    returns_by_bucket = {0: -0.001, 1: 0.003, 2: 0.006, 3: 0.003, 4: -0.001}
    for date in ["2021-01-01", "2021-01-08"]:
        for idx in range(50):
            bucket = idx // 10
            rows.append({"date": date, "ticker": f"s{idx:02d}", "factor_value": idx, "forward_return_5d": returns_by_bucket[bucket]})
    frame = pd.DataFrame(rows)

    summary = summarize_quantile_profile(build_quantile_return_profile(frame, quantiles=5))

    assert summary["best_quantile"] == 2
    assert summary["shape"] == "middle_hump"


def test_ic_spread_alignment_classifies_positive_ic_negative_spread():
    result = diagnose_ic_spread_alignment({"rank_ic_mean": 0.03, "top_bottom_spread_mean": -0.001})

    assert result["classification"] == "positive_ic_negative_spread"
    assert "portfolio_monetization_gap" in result["reasons"]
