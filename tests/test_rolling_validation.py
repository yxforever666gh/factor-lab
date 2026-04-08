from factor_lab.analytics import summarize_rolling_windows


def test_summarize_rolling_windows_requires_consistent_signal_direction():
    rolling_results = [
        {"factor_name": "mom_20", "pass_gate": True, "rank_ic_mean": 0.03, "top_bottom_spread_mean": 0.002},
        {"factor_name": "mom_20", "pass_gate": True, "rank_ic_mean": 0.02, "top_bottom_spread_mean": 0.0015},
        {"factor_name": "mom_20", "pass_gate": False, "rank_ic_mean": -0.01, "top_bottom_spread_mean": -0.0004},
    ]

    summary = summarize_rolling_windows(rolling_results, {"min_pass_rate": 0.5, "max_sign_flips": 1, "min_rank_ic": 0.01})

    assert summary["window_count"] == 3
    assert summary["pass_count"] == 2
    assert summary["sign_flip_count"] == 1
    assert summary["rank_ic_std"] is not None
    assert summary["spread_std"] is not None
    assert 0.0 <= summary["stability_score"] <= 1.0
    assert summary["pass_gate"] is True
