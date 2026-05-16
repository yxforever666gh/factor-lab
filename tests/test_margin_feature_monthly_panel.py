import pandas as pd

from scripts.write_margin_feature_monthly_panel import month_end_trade_dates, monthly_decision


def test_month_end_trade_dates_selects_last_available_per_month(tmp_path):
    p = tmp_path / "features.csv"
    pd.DataFrame({"date": ["2020-01-02", "2020-01-31", "2020-02-03", "2020-02-28"]}).to_csv(p, index=False)
    assert month_end_trade_dates(p, start="20200101", end="20200229") == ["20200131", "20200228"]


def test_monthly_decision_allows_good_panel():
    report = {
        "coverage": {"dates": 24, "rows": 500},
        "diagnostics": {
            "low_margin_crowding": {"spread_mean": 0.01},
            "baseline": {"spread_mean": 0.001},
            "confirmation": {"spread_mean": 0.02},
        },
        "correlations": {"low_margin_vs_baseline": 0.1, "low_margin_vs_turnover": 0.2},
    }
    assert monthly_decision(report)["decision"] == "proceed_margin_controlled_workflow_config"


def test_monthly_decision_requires_coverage_before_workflow():
    report = {
        "coverage": {"dates": 7, "rows": 157},
        "diagnostics": {
            "low_margin_crowding": {"spread_mean": 0.01},
            "baseline": {"spread_mean": 0.001},
            "confirmation": {"spread_mean": 0.02},
        },
        "correlations": {"low_margin_vs_baseline": 0.1, "low_margin_vs_turnover": 0.2},
    }
    assert monthly_decision(report)["decision"] == "need_more_margin_panel_coverage"
