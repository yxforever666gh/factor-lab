from __future__ import annotations

import pandas as pd

from factor_lab.ownership_concentration_source import (
    asof_merge_top10_features,
    build_ownership_concentration_features,
    build_ownership_concentration_report,
    build_top10_statement_features,
    top10_preflight,
)


def test_top10_preflight_requires_ann_date_for_pit() -> None:
    report = top10_preflight({"top10_holders": pd.DataFrame({"ts_code": ["000001.SZ"], "end_date": ["20231231"]})})
    endpoint = report["endpoint_reports"]["top10_holders"]
    assert endpoint["required_fields_present"] is False
    assert "ann_date" in endpoint["missing_required_fields"]
    assert report["pit_safe_endpoints"] == 0


def test_build_top10_statement_features_aggregates_float_holder_ratios_and_changes() -> None:
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"] * 4,
            "ann_date": ["20200131", "20200131", "20200430", "20200430"],
            "end_date": ["20191231", "20191231", "20200331", "20200331"],
            "holder_name": ["香港中央结算有限公司", "某证券投资基金", "香港中央结算有限公司", "某证券投资基金"],
            "holder_type": ["外资", "基金", "外资", "基金"],
            "hold_float_ratio": [5.0, 3.0, 6.0, 4.0],
            "hold_change": [1.0, 2.0, 3.0, 4.0],
        }
    )
    out = build_top10_statement_features({"top10_floatholders": df})
    assert "top10_float_ratio_sum" in out.columns
    assert "top10_float_fund_like_ratio_sum" in out.columns
    assert "top10_float_hkscc_ratio" in out.columns
    assert round(float(out.iloc[0]["top10_float_ratio_sum"]), 4) == 8.0
    assert round(float(out.iloc[1]["top10_float_ratio_sum_change"]), 4) == 2.0
    assert round(float(out.iloc[1]["top10_float_fund_like_ratio_sum_change"]), 4) == 1.0


def test_asof_merge_top10_features_uses_ann_date_not_end_date() -> None:
    cache = pd.DataFrame(
        {
            "date": ["20200115", "20200215", "20200515"],
            "ticker": ["000001.SZ"] * 3,
            "forward_return_5d": [0.01, 0.02, 0.03],
            "industry_relative_book_yield": [1.0, 1.0, 1.0],
            "roe": [0.1, 0.1, 0.1],
        }
    )
    statements = build_top10_statement_features(
        {"top10_floatholders": pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000001.SZ"],
                "ann_date": ["20200131", "20200430"],
                "end_date": ["20191231", "20200331"],
                "holder_name": ["a", "a"],
                "hold_float_ratio": [8.0, 10.0],
            }
        )}
    )
    merged = asof_merge_top10_features(cache, statements)
    assert pd.isna(merged.iloc[0]["top10_float_ratio_sum"])
    assert float(merged.iloc[1]["top10_float_ratio_sum"]) == 8.0
    assert float(merged.iloc[2]["top10_float_ratio_sum"]) == 10.0


def test_build_ownership_concentration_report_can_pass_when_best_signal_beats_benchmark() -> None:
    rows = []
    for date in ["20200131", "20200228", "20200331", "20200430", "20200529"]:
        for i in range(20):
            signal = i / 19
            rows.append(
                {
                    "date": date,
                    "ticker": f"{i:06d}.SZ",
                    "forward_return_5d": signal * 0.05 - 0.01,
                    "industry_relative_book_yield": 0.0,
                    "roe": 0.1,
                    "top10_float_ratio_sum": signal,
                    "turnover": 1 - signal,
                }
            )
    features = build_ownership_concentration_features(pd.DataFrame(rows))
    report = build_ownership_concentration_report(features)
    assert report["best_signal"]["name"] == "high_top10_float_ratio_sum"
    assert report["best_signal"]["spread_mean"] > report["benchmark"]["value_quality_no_distress_bucket_spread"]
    assert report["decision"]["decision"] != "stop_ownership_concentration_not_incremental"
