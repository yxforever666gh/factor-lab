import pandas as pd

from factor_lab.margin_feature_builder import (
    MarginFeatureBuildConfig,
    assign_quantile_buckets,
    build_margin_low_crowding_features,
    build_readonly_margin_probe_report,
    bucket_spread,
    date_zscore,
)
from scripts.write_margin_feature_sample import derive_month_end_trade_dates


def test_date_zscore_is_date_scoped():
    s = pd.Series([1.0, 2.0, 10.0, 12.0])
    dates = pd.Series(["20200101", "20200101", "20200102", "20200102"])
    out = date_zscore(s, dates)
    assert round(float(out.iloc[0]), 6) == -1.0
    assert round(float(out.iloc[1]), 6) == 1.0
    assert round(float(out.iloc[2]), 6) == -1.0
    assert round(float(out.iloc[3]), 6) == 1.0


def test_build_margin_low_crowding_features_constructs_expected_columns():
    df = pd.DataFrame({
        "date": ["20200101"] * 6,
        "trade_date": ["20200101"] * 6,
        "ts_code": [f"00000{i}.SZ" for i in range(6)],
        "ticker": [f"00000{i}.SZ" for i in range(6)],
        "rzye": [1, 2, 3, 4, 5, 6],
        "total_mv": [1] * 6,
        "forward_return_5d": [0, 0.01, 0.02, 0.03, 0.04, 0.05],
        "industry_relative_book_yield": [0, 1, 2, 3, 4, 5],
        "roe": [0.1] * 6,
    })
    out = build_margin_low_crowding_features(df)
    assert "margin_balance_to_mv" in out.columns
    assert "low_margin_crowding" in out.columns
    assert "margin_low_crowding_confirmation" in out.columns
    assert len(out) == 6
    assert out.sort_values("rzye")["low_margin_crowding"].iloc[0] > out.sort_values("rzye")["low_margin_crowding"].iloc[-1]


def test_bucket_spread_uses_configured_bucket_pair():
    df = pd.DataFrame({
        "date": ["20200101"] * 10,
        "ticker": [str(i) for i in range(10)],
        "score": list(range(10)),
        "forward_return_5d": [0, 0, 0, 0, 0, 1, 1, 1, 1, 1],
    })
    result = bucket_spread(df, "score", quantiles=5, long_quantile=3, short_quantile=0)
    assert result["available"] is True
    assert result["observations"] == 1
    assert result["spread_mean"] == 1.0


def test_readonly_report_proceeds_when_signal_is_incremental_and_positive():
    rows = []
    for d in ["20200101", "20200102", "20200103"]:
        for i in range(20):
            low_margin = i / 10
            baseline = (i % 5) / 10
            rows.append({
                "date": d,
                "ticker": f"{i:06d}.SZ",
                "forward_return_5d": low_margin * 0.02,
                "value_quality_baseline": baseline,
                "margin_balance_to_mv": 1 - low_margin,
                "low_margin_crowding": low_margin,
                "margin_low_crowding_confirmation": low_margin + baseline,
                "turnover": baseline,
                "turnover_shock_5_20": baseline,
            })
    df = pd.DataFrame(rows)
    result = build_readonly_margin_probe_report(
        df,
        config=MarginFeatureBuildConfig(min_overlap_rows=30, min_overlap_dates=3, benchmark_spread=0.001),
    )
    assert result["decision"]["decision"] == "proceed_controlled_margin_low_crowding_probe"


def test_readonly_report_requires_feature_store_extension_for_tiny_overlap():
    df = pd.DataFrame({
        "date": ["20200101"] * 5,
        "ticker": [str(i) for i in range(5)],
        "forward_return_5d": [0, 1, 2, 3, 4],
        "value_quality_baseline": [0, 1, 2, 3, 4],
        "margin_balance_to_mv": [4, 3, 2, 1, 0],
        "low_margin_crowding": [0, 1, 2, 3, 4],
        "margin_low_crowding_confirmation": [0, 2, 4, 6, 8],
    })
    result = build_readonly_margin_probe_report(df, config=MarginFeatureBuildConfig(min_overlap_rows=100, min_overlap_dates=3))
    assert result["decision"]["decision"] == "need_margin_feature_store_extension"


def test_derive_month_end_trade_dates_uses_feature_cache_dates(tmp_path):
    cache = tmp_path / "tushare_2020-01-01_2020-03-31_3.csv"
    pd.DataFrame({
        "date": ["2020-01-02", "2020-01-31", "2020-02-03", "2020-02-28", "2020-03-15"],
        "ticker": ["000001.SZ"] * 5,
    }).to_csv(cache, index=False)

    dates = derive_month_end_trade_dates(cache, start="2020-01-01", end="2020-03-31", max_dates=10)

    assert dates == ("20200131", "20200228", "20200315")
