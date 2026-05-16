from __future__ import annotations

import pandas as pd

from factor_lab.earnings_event_source import (
    EarningsEventConfig,
    asof_merge_earnings_event_features,
    build_earnings_event_features,
    build_earnings_event_report,
    build_earnings_event_statement_features,
    event_preflight,
)


def test_event_preflight_requires_ann_date_for_pit() -> None:
    report = event_preflight({"forecast": pd.DataFrame({"ts_code": ["000001.SZ"], "end_date": ["20231231"]})})
    endpoint = report["endpoint_reports"]["forecast"]
    assert endpoint["required_fields_present"] is False
    assert "ann_date" in endpoint["missing_required_fields"]
    assert report["pit_safe_endpoints"] == 0


def test_build_forecast_statement_features_scores_warning_and_change() -> None:
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "ann_date": ["20200131", "20200430"],
            "end_date": ["20191231", "20200331"],
            "type": ["预减", "预增"],
            "p_change_min": [-30.0, 20.0],
            "p_change_max": [-10.0, 40.0],
        }
    )
    out = build_earnings_event_statement_features({"forecast": df})
    assert "forecast_type_score" in out.columns
    assert "forecast_p_change_mid" in out.columns
    assert float(out.iloc[0]["forecast_type_score"]) == -1.0
    assert float(out.iloc[1]["forecast_type_score_qoq"]) == 2.0
    assert float(out.iloc[1]["forecast_p_change_mid"]) == 30.0


def test_asof_merge_earnings_event_features_uses_ann_date_not_end_date() -> None:
    cache = pd.DataFrame(
        {
            "date": ["20200115", "20200215", "20200515"],
            "ticker": ["000001.SZ"] * 3,
            "forward_return_5d": [0.01, 0.02, 0.03],
            "industry_relative_book_yield": [1.0, 1.0, 1.0],
            "roe": [0.1, 0.1, 0.1],
        }
    )
    statements = build_earnings_event_statement_features(
        {"express": pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000001.SZ"],
                "ann_date": ["20200131", "20200430"],
                "end_date": ["20191231", "20200331"],
                "n_income_log_qoq": [0.1, 0.2],
            }
        )}
    )
    merged = asof_merge_earnings_event_features(cache, statements)
    assert pd.isna(merged.iloc[0]["express_n_income_log_qoq"])
    assert float(merged.iloc[1]["express_n_income_log_qoq"]) == 0.1
    assert float(merged.iloc[2]["express_n_income_log_qoq"]) == 0.2


def test_build_earnings_event_report_can_pass_when_best_signal_beats_benchmark() -> None:
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
                    "express_n_income_log_qoq": signal,
                    "turnover": 1 - signal,
                }
            )
    features = build_earnings_event_features(pd.DataFrame(rows))
    report = build_earnings_event_report(features, config=EarningsEventConfig(min_rows=50, min_dates=5, min_tickers=20))
    assert report["best_signal"]["name"] == "high_express_n_income_log_qoq"
    assert report["best_signal"]["spread_mean"] > report["benchmark"]["value_quality_no_distress_bucket_spread"]
    assert report["decision"]["decision"] == "proceed_earnings_event_controlled_probe_plan"
