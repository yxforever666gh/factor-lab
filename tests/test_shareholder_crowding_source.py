from __future__ import annotations

import pandas as pd

from factor_lab.shareholder_crowding_source import (
    asof_merge_holdernumber_features,
    build_holdernumber_statement_features,
    build_shareholder_crowding_features,
    build_shareholder_crowding_report,
    holdernumber_preflight,
)


def test_holdernumber_preflight_requires_pit_fields() -> None:
    df = pd.DataFrame({"ts_code": ["000001.SZ"], "ann_date": ["20200131"], "end_date": ["20191231"], "holder_num": [1000]})
    report = holdernumber_preflight(df)
    assert report["required_fields_present"] is True
    assert report["tickers"] == 1
    assert report["holder_num_nonnull_rate"] == 1.0


def test_build_holdernumber_statement_features_adds_changes() -> None:
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"] * 5,
            "ann_date": ["20200131", "20200430", "20200731", "20201031", "20210131"],
            "end_date": ["20191231", "20200331", "20200630", "20200930", "20201231"],
            "holder_num": [1000, 900, 990, 800, 700],
        }
    )
    out = build_holdernumber_statement_features(df)
    assert "holder_num_change_qoq" in out.columns
    assert "holder_num_change_yoy" in out.columns
    assert round(float(out.iloc[1]["holder_num_change_qoq"]), 4) == -0.1
    assert round(float(out.iloc[4]["holder_num_change_yoy"]), 4) == -0.3


def test_asof_merge_holdernumber_features_uses_ann_date_not_end_date() -> None:
    cache = pd.DataFrame(
        {
            "date": ["20200115", "20200215", "20200515"],
            "ticker": ["000001.SZ"] * 3,
            "forward_return_5d": [0.01, 0.02, 0.03],
            "industry_relative_book_yield": [1.0, 1.0, 1.0],
            "roe": [0.1, 0.1, 0.1],
        }
    )
    holders = build_holdernumber_statement_features(
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000001.SZ"],
                "ann_date": ["20200131", "20200430"],
                "end_date": ["20191231", "20200331"],
                "holder_num": [1000, 900],
            }
        )
    )
    merged = asof_merge_holdernumber_features(cache, holders)
    assert pd.isna(merged.iloc[0]["holder_num"])
    assert float(merged.iloc[1]["holder_num"]) == 1000
    assert float(merged.iloc[2]["holder_num"]) == 900


def test_build_shareholder_crowding_report_can_pass_on_incremental_signal() -> None:
    rows = []
    for date_idx, date in enumerate(["20200131", "20200228", "20200331", "20200430", "20200529"]):
        for i in range(20):
            low_crowding = i / 19
            baseline = i / 19
            rows.append(
                {
                    "date": date,
                    "ticker": f"{i:06d}.SZ",
                    "forward_return_5d": low_crowding * 0.05 - 0.01,
                    "industry_relative_book_yield": baseline,
                    "roe": 0.1,
                    "low_shareholder_crowding_raw": low_crowding,
                    "turnover": 1 - low_crowding,
                }
            )
    features = build_shareholder_crowding_features(pd.DataFrame(rows))
    report = build_shareholder_crowding_report(features)
    assert report["coverage"]["rows"] == 100
    assert report["diagnostics"]["low_shareholder_crowding"]["spread_mean"] > 0
