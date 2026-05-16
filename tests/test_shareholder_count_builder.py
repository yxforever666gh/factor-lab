import pandas as pd

from factor_lab.shareholder_count_builder import (
    ShareholderCountConfig,
    add_shareholder_change_features,
    build_daily_asof_shareholder_frame,
    build_shareholder_crowding_features,
    build_shareholder_probe_report,
    normalize_holdernumber_frame,
)


def _holder_rows():
    rows = []
    for i in range(6):
        code = f"00000{i}.SZ"
        end_dates = ["20200331", "20200630", "20200930", "20201231", "20210331"]
        for q, (ann, end_date) in enumerate(zip(["20200430", "20200831", "20201031", "20210430", "20210831"], end_dates), start=1):
            rows.append({"ts_code": code, "ann_date": ann, "end_date": end_date, "holder_num": 1000 + i * 50 - q * 20})
    return pd.DataFrame(rows)


def test_normalize_holdernumber_frame_requires_pit_fields():
    df = pd.DataFrame({"ts_code": ["000001.SZ"], "ann_date": [20200430], "end_date": [20200331], "holder_num": [1234]})
    out = normalize_holdernumber_frame(df)
    assert out["ann_date"].iloc[0] == "20200430"
    assert out["holder_num"].iloc[0] == 1234


def test_add_shareholder_change_features_builds_qoq_change():
    out = add_shareholder_change_features(_holder_rows())
    assert "holder_num_qoq_log_change" in out.columns
    assert out.groupby("ts_code")["holder_num_qoq_log_change"].apply(lambda s: s.notna().sum()).min() >= 4


def test_daily_asof_uses_announcement_date_not_end_date():
    base = pd.DataFrame({
        "date": ["2020-04-15", "2020-05-15"],
        "ticker": ["000001.SZ", "000001.SZ"],
        "forward_return_5d": [0.01, 0.02],
        "industry_relative_book_yield": [1.0, 1.0],
        "roe": [0.1, 0.1],
    })
    holder = pd.DataFrame({"ts_code": ["000001.SZ"], "ann_date": ["20200430"], "end_date": ["20200331"], "holder_num": [1000]})
    out = build_daily_asof_shareholder_frame(base, holder, start_date="2020-04-01", end_date="2020-05-31")
    assert len(out) == 2
    assert pd.isna(out.loc[out["date"] == "2020-04-15", "holder_num"].iloc[0])
    assert out.loc[out["date"] == "2020-05-15", "holder_num"].iloc[0] == 1000


def test_build_shareholder_crowding_features_constructs_signal():
    base = []
    for i in range(10):
        for date in ["2021-05-10", "2021-05-11"]:
            base.append({"date": date, "ticker": f"00000{i}.SZ", "ts_code": f"00000{i}.SZ", "forward_return_5d": i * 0.01, "industry_relative_book_yield": i, "roe": 0.1})
    holder = []
    for i in range(10):
        code = f"00000{i}.SZ"
        for q, ann in enumerate(["20200430", "20200831", "20201031", "20210430"], start=1):
            holder.append({"ts_code": code, "ann_date": ann, "end_date": str(20200000 + q * 100 + 30), "holder_num": 1000 + i * 100 - q * i * 10})
    asof = build_daily_asof_shareholder_frame(pd.DataFrame(base), pd.DataFrame(holder), start_date="2021-05-01", end_date="2021-05-31")
    features = build_shareholder_crowding_features(asof)
    assert "low_shareholder_crowding_qoq" in features.columns
    assert "shareholder_crowding_confirmation_qoq" in features.columns
    assert len(features) > 0


def test_probe_report_can_proceed_when_confirmation_beats_benchmark():
    rows = []
    for d in ["20200101", "20200102", "20200103", "20200104", "20200105"]:
        for i in range(20):
            qoq = i / 10
            baseline = (i % 5) / 100
            rows.append({
                "date": d,
                "ticker": f"{i:06d}.SZ",
                "ann_date": "20191231",
                "holder_num": 1000,
                "holder_num_qoq_log_change": -qoq,
                "holder_num_yoy_log_change": -qoq,
                "forward_return_5d": qoq * 0.02,
                "value_quality_baseline": baseline,
                "low_shareholder_crowding_qoq": qoq,
                "low_shareholder_crowding_yoy": qoq,
                "shareholder_crowding_confirmation_qoq": qoq + baseline,
                "shareholder_crowding_confirmation_yoy": qoq + baseline,
                "turnover": baseline,
            })
    result = build_shareholder_probe_report(pd.DataFrame(rows), config=ShareholderCountConfig(min_rows=50, min_dates=3, benchmark_spread=0.001))
    assert result["decision"]["decision"] == "proceed_shareholder_crowding_controlled_probe_plan"
