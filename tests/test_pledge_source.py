from __future__ import annotations

import pandas as pd

from factor_lab.pledge_source import (
    PledgeSourceConfig,
    asof_merge_pledge_features,
    build_pledge_readonly_features,
    build_pledge_report,
    build_pledge_statement_features,
    pledge_preflight,
)


def test_pledge_preflight_requires_pit_date_and_signal_field() -> None:
    report = pledge_preflight({"pledge_stat": pd.DataFrame({"ts_code": ["000001.SZ"], "pledge_ratio": [12.0]})})
    endpoint = report["endpoint_reports"]["pledge_stat"]
    assert endpoint["required_fields_present"] is False
    assert "ann_date" in endpoint["missing_required_fields"]
    assert endpoint["signal_fields_present"] is True
    assert report["pit_safe_endpoints"] == 0


def test_build_pledge_statement_features_aggregates_ratio_and_changes() -> None:
    raw = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ", "000001.SZ"],
            "ann_date": ["20200131", "20200131", "20200430"],
            "end_date": ["20191231", "20191231", "20200331"],
            "pledge_ratio": [10.0, 20.0, 25.0],
            "pledge_amount": [100.0, 200.0, 300.0],
        }
    )
    out = build_pledge_statement_features({"pledge_stat": raw})
    assert len(out) == 2
    assert round(float(out.iloc[0]["pledge_ratio_mean"]), 4) == 15.0
    assert round(float(out.iloc[0]["pledge_amount_sum"]), 4) == 300.0
    assert round(float(out.iloc[1]["pledge_ratio_mean_change"]), 4) == 10.0


def test_asof_merge_pledge_features_uses_ann_date_not_future_end_date() -> None:
    cache = pd.DataFrame(
        {
            "date": ["20200115", "20200215", "20200515"],
            "ticker": ["000001.SZ"] * 3,
            "forward_return_5d": [0.01, 0.02, 0.03],
            "industry_relative_book_yield": [1.0, 1.0, 1.0],
            "roe": [0.1, 0.1, 0.1],
        }
    )
    statements = build_pledge_statement_features(
        {"pledge_stat": pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000001.SZ"],
                "ann_date": ["20200131", "20200430"],
                "end_date": ["20191231", "20200331"],
                "pledge_ratio": [8.0, 10.0],
            }
        )}
    )
    merged = asof_merge_pledge_features(cache, statements)
    assert pd.isna(merged.iloc[0]["pledge_ratio_mean"])
    assert float(merged.iloc[1]["pledge_ratio_mean"]) == 8.0
    assert float(merged.iloc[2]["pledge_ratio_mean"]) == 10.0


def test_build_pledge_report_can_pass_when_low_pledge_beats_benchmark() -> None:
    rows = []
    for date in ["20200131", "20200228", "20200331", "20200430", "20200529"]:
        for i in range(20):
            pledge_pressure = i / 19
            rows.append(
                {
                    "date": date,
                    "ticker": f"{i:06d}.SZ",
                    "forward_return_5d": (1 - pledge_pressure) * 0.05 - 0.01,
                    "industry_relative_book_yield": (i % 4) * 0.01,
                    "roe": (i % 5) * 0.01,
                    "pledge_ratio_mean": pledge_pressure,
                    "turnover": ((i * 7) % 20) / 20,
                }
            )
    features = build_pledge_readonly_features(pd.DataFrame(rows))
    report = build_pledge_report(features, config=PledgeSourceConfig(min_rows=100, min_dates=5, min_tickers=20))
    assert report["best_signal"]["name"] == "low_pledge_ratio_mean"
    assert report["best_signal"]["spread_mean"] > report["benchmark"]["value_quality_no_distress_bucket_spread"]
    assert report["decision"]["decision"] == "proceed_controlled_pledge_probe_plan"
