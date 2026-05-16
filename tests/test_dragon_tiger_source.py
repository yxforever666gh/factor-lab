from __future__ import annotations

import pandas as pd

from factor_lab.dragon_tiger_source import (
    asof_merge_dragon_tiger_features,
    build_dragon_tiger_daily_events,
    build_dragon_tiger_readonly_features,
    build_dragon_tiger_report,
    dragon_tiger_preflight,
)


def test_dragon_tiger_preflight_requires_event_date_and_code() -> None:
    report = dragon_tiger_preflight(pd.DataFrame({"ts_code": ["000001.SZ"], "net_amount": [10.0]}))
    assert report["required_fields_present"] is False
    assert "trade_date" in report["missing_required_fields"]
    assert report["signal_fields_present"] is True


def test_build_dragon_tiger_daily_events_derives_net_amount_and_imbalance() -> None:
    raw = pd.DataFrame(
        {
            "trade_date": ["20200102", "20200102"],
            "ts_code": ["000001.SZ", "000001.SZ"],
            "l_buy": [100.0, 50.0],
            "l_sell": [40.0, 10.0],
            "amount": [1000.0, 500.0],
        }
    )
    daily = build_dragon_tiger_daily_events(raw)
    assert len(daily) == 1
    assert float(daily.iloc[0]["dt_net_amount_sum"]) == 100.0
    assert float(daily.iloc[0]["dt_event_count"]) == 2.0
    assert round(float(daily.iloc[0]["dt_net_amount_to_amount"]), 6) == round(100.0 / 1500.0, 6)


def test_asof_merge_dragon_tiger_features_uses_trade_date_not_future_events() -> None:
    cache = pd.DataFrame(
        {
            "date": ["20200101", "20200103", "20200110"],
            "ticker": ["000001.SZ"] * 3,
            "forward_return_5d": [0.01, 0.02, 0.03],
            "industry_relative_book_yield": [1.0, 1.0, 1.0],
            "roe": [0.1, 0.1, 0.1],
        }
    )
    events = build_dragon_tiger_daily_events(
        pd.DataFrame(
            {
                "trade_date": ["20200102", "20200111"],
                "ts_code": ["000001.SZ", "000001.SZ"],
                "net_amount": [10.0, 100.0],
                "amount": [100.0, 100.0],
            }
        )
    )
    merged = asof_merge_dragon_tiger_features(cache, events, windows=(5, 20))
    assert float(merged.loc[merged["date"] == "20200101", "dt_event_count_20d"].iloc[0]) == 0.0
    assert float(merged.loc[merged["date"] == "20200103", "dt_net_amount_sum_5d"].iloc[0]) == 10.0
    assert float(merged.loc[merged["date"] == "20200110", "dt_net_amount_sum_20d"].iloc[0]) == 10.0


def test_build_dragon_tiger_report_can_pass_when_signal_beats_benchmark() -> None:
    rows = []
    for date in ["20200131", "20200228", "20200331", "20200430", "20200529"]:
        for i in range(20):
            signal = i / 19
            rows.append(
                {
                    "date": date,
                    "ticker": f"{i:06d}.SZ",
                    "forward_return_5d": signal * 0.05 - 0.01,
                    "industry_relative_book_yield": (i % 3) * 0.01,
                    "roe": (i % 5) * 0.01,
                    "dt_net_amount_sum_20d": signal,
                    "dt_event_count_20d": 1.0,
                    "turnover": ((i * 7) % 20) / 20,
                }
            )
    features = build_dragon_tiger_readonly_features(pd.DataFrame(rows))
    from factor_lab.dragon_tiger_source import DragonTigerConfig

    report = build_dragon_tiger_report(features, config=DragonTigerConfig(min_rows=100, min_dates=5, min_tickers=20))
    assert report["best_signal"]["name"] == "high_dt_net_amount_sum_20d"
    assert report["best_signal"]["spread_mean"] > report["benchmark"]["value_quality_no_distress_bucket_spread"]
    assert report["decision"]["decision"] == "proceed_controlled_dragon_tiger_probe_plan"
