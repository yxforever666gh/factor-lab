import pandas as pd
import pytest

from factor_lab.bucket_aware_portfolio import evaluate_bucket_pair_portfolio
from factor_lab.evaluation import evaluate_factor


def _middle_hump_frame():
    rows = []
    returns = {0: -0.002, 1: 0.001, 2: 0.003, 3: 0.004, 4: 0.001}
    for date in ["2021-01-01", "2021-01-08", "2021-01-15"]:
        for idx in range(50):
            bucket = idx // 10
            rows.append({
                "date": date,
                "ticker": f"s{idx:02d}",
                "factor_value": idx,
                "forward_return_5d": returns[bucket],
            })
    return pd.DataFrame(rows)


def test_bucket_pair_portfolio_uses_configured_long_and_short_quantiles():
    frame = _middle_hump_frame()

    q3_q0 = evaluate_bucket_pair_portfolio(frame, quantiles=5, long_quantile=3, short_quantile=0)
    q4_q0 = evaluate_bucket_pair_portfolio(frame, quantiles=5, long_quantile=4, short_quantile=0)

    assert q3_q0.spread_mean == 0.006
    assert q4_q0.spread_mean == 0.003
    assert q3_q0.spread_mean > q4_q0.spread_mean
    assert q3_q0.pass_gate is True


def test_bucket_pair_portfolio_rejects_invalid_quantile_config():
    with pytest.raises(ValueError, match="out of range"):
        evaluate_bucket_pair_portfolio(_middle_hump_frame(), quantiles=5, long_quantile=5, short_quantile=0)


def test_default_extreme_top_bottom_evaluation_is_unchanged():
    frame = _middle_hump_frame()
    result = evaluate_factor(frame, factor_name="x", expression="factor_value", thresholds={"min_rank_ic": -1, "min_top_bottom_spread": -1, "min_sharpe_net": -999})

    assert result.top_bottom_spread_mean == 0.0003
