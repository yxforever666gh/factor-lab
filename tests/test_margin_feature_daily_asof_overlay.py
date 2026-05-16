import pandas as pd

from scripts.write_margin_feature_daily_asof_overlay import build_daily_asof_margin_frame, decision_from_report


def test_build_daily_asof_margin_frame_forward_fills_prior_margin_by_ticker():
    base = pd.DataFrame({
        "date": ["2020-06-01", "2020-06-02", "2020-06-03"],
        "ticker": ["000001.SZ"] * 3,
        "total_mv": [1, 1, 1],
        "forward_return_5d": [0.0, 0.1, 0.2],
        "industry_relative_book_yield": [0.1, 0.1, 0.1],
        "roe": [0.1, 0.1, 0.1],
    })
    raw = pd.DataFrame({"trade_date": ["20200601", "20200603"], "ts_code": ["000001.SZ", "000001.SZ"], "rzye": [10, 30]})
    out = build_daily_asof_margin_frame(base, raw)
    assert out["rzye"].tolist() == [10, 10, 30]


def test_decision_from_report_allows_strong_daily_asof():
    report = {
        "coverage": {"rows": 10000, "dates": 500},
        "diagnostics": {"low_margin_crowding": {"spread_mean": 0.01}, "baseline": {"spread_mean": 0.001}, "confirmation": {"spread_mean": 0.02}},
        "correlations": {"low_margin_vs_baseline": 0.1, "low_margin_vs_turnover": 0.2},
    }
    assert decision_from_report(report)["decision"] == "proceed_single_controlled_workflow_with_daily_asof_overlay"
