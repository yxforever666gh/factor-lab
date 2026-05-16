import pandas as pd

from factor_lab.pit_financial_features import build_pit_financial_features, summarize_feature_coverage


def test_build_features_uses_only_announced_statements():
    trades = pd.DataFrame([
        {"ts_code": "000001.SZ", "date": "2024-03-01"},
        {"ts_code": "000001.SZ", "date": "2024-05-01"},
    ])
    statements = {
        "cashflow": pd.DataFrame([{"ts_code": "000001.SZ", "end_date": "20231231", "ann_date": "20240425", "n_cashflow_act": 200, "net_profit": 100}]),
        "balancesheet": pd.DataFrame([{"ts_code": "000001.SZ", "end_date": "20231231", "ann_date": "20240425", "total_liab": 40, "total_assets": 100}]),
        "fina_indicator": pd.DataFrame([{"ts_code": "000001.SZ", "end_date": "20231231", "ann_date": "20240425", "q_netprofit_yoy": 5, "q_sales_yoy": 3, "roe": 10}]),
    }
    features = build_pit_financial_features(statements, trades)
    assert pd.isna(features.loc[0, "operating_cashflow_to_profit"])
    assert features.loc[1, "operating_cashflow_to_profit"] == 2
    assert features.loc[1, "debt_to_assets"] == 0.4
    assert features.loc[1, "netprofit_yoy"] == 5
    assert features.loc[1, "tr_yoy"] == 3
    assert features.loc[1, "profit_growth_ok"] is True or features.loc[1, "profit_growth_ok"] == True
    assert features.loc[1, "pit_feature_validated"] is True or features.loc[1, "pit_feature_validated"] == True


def test_division_by_zero_blocks_ratio_with_reason():
    trades = pd.DataFrame([{"ts_code": "000001.SZ", "date": "2024-05-01"}])
    statements = {
        "cashflow": pd.DataFrame([{"ts_code": "000001.SZ", "end_date": "20231231", "ann_date": "20240425", "n_cashflow_act": 200, "net_profit": 0}]),
        "balancesheet": pd.DataFrame([{"ts_code": "000001.SZ", "end_date": "20231231", "ann_date": "20240425", "total_liab": 40, "total_assets": 0}]),
    }
    features = build_pit_financial_features(statements, trades)
    assert pd.isna(features.loc[0, "operating_cashflow_to_profit"])
    assert "operating_cashflow_to_profit:zero_denominator" in features.loc[0, "pit_feature_blocked_reason"]
    assert pd.isna(features.loc[0, "debt_to_assets"])


def test_missing_pit_metadata_blocks_feature_emission():
    trades = pd.DataFrame([{"ts_code": "000001.SZ", "date": "2024-05-01"}])
    statements = {
        "cashflow": pd.DataFrame([{"ts_code": "000001.SZ", "end_date": "20231231", "n_cashflow_act": 200, "net_profit": 100}]),
    }
    features = build_pit_financial_features(statements, trades)
    assert pd.isna(features.loc[0, "operating_cashflow_to_profit"])
    assert features.loc[0, "pit_feature_validated"] is False or features.loc[0, "pit_feature_validated"] == False
    assert "no_pit_statement_asof" in features.loc[0, "pit_feature_blocked_reason"]


def test_tushare_wins_and_diemeng_fills_missing_with_disagreement_recorded():
    trades = pd.DataFrame([
        {"ts_code": "000001.SZ", "date": "2024-05-01"},
        {"ts_code": "000002.SZ", "date": "2024-05-01"},
    ])
    statements = {
        "fina_indicator": pd.DataFrame([
            {"ts_code": "000001.SZ", "end_date": "20231231", "ann_date": "20240425", "roe": 10},
        ]),
        "diemeng.financial_indicator": pd.DataFrame([
            {"ts_code": "000001.SZ", "end_date": "20231231", "ann_date": "20240425", "roe": 12},
            {"ts_code": "000002.SZ", "end_date": "20231231", "ann_date": "20240425", "roe": 8},
        ]),
    }
    features = build_pit_financial_features(statements, trades)
    assert features.loc[0, "roe"] == 10
    assert features.loc[0, "roe_source"] == "tushare.fina_indicator"
    assert "roe:cross_source_disagreement" in features.loc[0, "pit_feature_warnings"]
    assert features.loc[1, "roe"] == 8
    assert features.loc[1, "roe_source"] == "diemeng.financial_indicator"


def test_summarize_feature_coverage():
    df = pd.DataFrame({"a": [1, None, 2], "b": [None, None, None]})
    summary = summarize_feature_coverage(df, ["a", "b", "c"])
    assert summary["features"]["a"]["coverage"] == 0.6667
    assert summary["features"]["b"]["available"] is True
    assert summary["features"]["c"]["available"] is False
