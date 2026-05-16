import pandas as pd

from factor_lab.margin_source_mvp import (
    add_margin_features,
    classify_tickers,
    correlation_precheck,
    decide_margin_mvp,
    field_sanity,
    is_stock_like_ts_code,
    merge_margin_with_features,
)


def test_stock_like_classifier_separates_etf_prefixes():
    assert is_stock_like_ts_code("000001.SZ") is True
    assert is_stock_like_ts_code("600000.SH") is True
    assert is_stock_like_ts_code("510050.SH") is False
    assert is_stock_like_ts_code("159915.SZ") is False


def test_classify_tickers_counts_stock_like_ratio():
    df = pd.DataFrame({"ts_code": ["000001.SZ", "600000.SH", "510050.SH", "510050.SH"]})
    result = classify_tickers(df)
    assert result["unique_tickers"] == 3
    assert result["stock_like_tickers"] == 2
    assert result["stock_like_ratio"] == 0.6667


def test_field_sanity_reports_zero_and_negative_rates():
    df = pd.DataFrame({"rzye": [1.0, 0.0, -1.0, None]})
    result = field_sanity(df, ("rzye", "missing"))
    assert result["rzye"]["available"] is True
    assert result["rzye"]["negative_rate"] == 0.25
    assert result["missing"]["available"] is False


def test_merge_and_add_margin_features():
    margin = pd.DataFrame({"trade_date": ["20231229"], "ts_code": ["000001.SZ"], "rzye": [100.0], "rqye": [5.0], "rzmre": [10.0], "rzche": [2.0]})
    features = pd.DataFrame({"date": ["20231229"], "ts_code": ["000001.SZ"], "total_mv": [1.0], "turnover": [0.2]})
    merged = merge_margin_with_features(margin, features)
    out = add_margin_features(merged)
    assert len(out) == 1
    assert "margin_balance_to_mv" in out.columns
    assert out["margin_balance_to_mv"].iloc[0] == 0.01


def test_correlation_precheck_detects_redundancy_flag():
    df = pd.DataFrame({
        "rzye": list(range(20)),
        "rqye": [0] * 20,
        "rzmre": list(range(20)),
        "rzche": [0] * 20,
        "total_mv": [1] * 20,
        "turnover": list(range(20)),
        "volatility_20": list(range(20)),
    })
    result = correlation_precheck(df)
    assert result["available"] is True
    assert result["redundancy_flag"] == "high"
    assert result["primary_margin_balance_redundancy_flag"] == "high"


def test_decide_margin_mvp_allows_non_redundant_overlap():
    coverage = {"overall": {"stock_like_ratio": 0.9}, "feature_overlap_rows": 100}
    sanity = {"rzye": {"available": True, "nonnull_rate": 1.0}}
    corr = {"redundancy_flag": "low"}
    result = decide_margin_mvp(coverage, sanity, corr)
    assert result["decision"] == "proceed_margin_factor_probe_plan"
