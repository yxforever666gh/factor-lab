import pandas as pd

from factor_lab.portfolio import evaluate_long_short_portfolio


def test_long_short_portfolio_top_minus_bottom_direction_is_positive_when_high_signal_outperforms():
    rows = []
    for date in ["2021-01-01", "2021-01-08"]:
        for idx in range(20):
            rows.append({
                "date": date,
                "ticker": f"s{idx:02d}",
                "forward_return_5d": idx / 1000.0,
            })
    frame = pd.DataFrame(rows)
    signal = pd.Series([row["forward_return_5d"] for row in rows])

    result = evaluate_long_short_portfolio(frame, signal, top_q=0.2, bottom_q=0.2, cost_bps_per_turnover=0.0)

    assert result.strategy_name == "long_short_top_bottom"
    assert result.annual_return > 0
    assert result.cost_adjusted_annual_return > 0
