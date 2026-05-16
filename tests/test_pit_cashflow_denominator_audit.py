from __future__ import annotations

import pandas as pd

from factor_lab.pit_cashflow_denominator_audit import build_cashflow_denominator_audit
from factor_lab.pit_financial_features import build_pit_financial_features


def test_denominator_audit_recommends_income_fallback_when_it_lifts_coverage() -> None:
    df = pd.DataFrame(
        {
            "ticker": ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"],
            "date": pd.to_datetime(["2020-06-30"] * 4),
            "pit_cashflow_numerator_raw": [10.0, 20.0, 30.0, 40.0],
            "pit_cashflow_denominator_raw": [10.0, None, 0.0, None],
            "pit_cashflow_income_n_income_attr_p_raw": [10.0, 20.0, 30.0, 40.0],
            "pit_cashflow_income_total_profit_raw": [9.0, 19.0, 29.0, 39.0],
            "netprofit_yoy": [1.0, 2.0, 3.0, 4.0],
        }
    )

    audit = build_cashflow_denominator_audit(df, viability_threshold=0.60)

    assert audit["decision"] == "cashflow_denominator_fixed_candidate"
    by_name = {row["candidate"]: row for row in audit["candidates"]}
    assert by_name["cashflow.net_profit"]["nonzero_coverage"] == 0.25
    assert by_name["income.n_income_attr_p"]["nonzero_coverage"] == 1.0
    assert by_name["fina_indicator.netprofit_yoy"]["eligible_denominator"] is False
    assert audit["recommended_fallback_order"][:2] == ["cashflow.net_profit", "income.n_income_attr_p"]


def test_denominator_audit_splits_retained_fallback_source_coverage() -> None:
    df = pd.DataFrame(
        {
            "ticker": ["A", "B", "C", "D"],
            "date": pd.to_datetime(["2020-01-01"] * 4),
            "pit_cashflow_denominator_raw": [10.0, 20.0, 30.0, 40.0],
            "pit_cashflow_denominator_source": [
                "tushare.cashflow.net_profit",
                "tushare.income.n_income_attr_p",
                "tushare.income.n_income_attr_p",
                "tushare.income.n_income_attr_p",
            ],
        }
    )

    audit = build_cashflow_denominator_audit(df, viability_threshold=0.60)

    by_name = {row["candidate"]: row for row in audit["candidates"]}
    assert by_name["cashflow.net_profit"]["nonzero_coverage"] == 0.25
    assert by_name["income.n_income_attr_p"]["nonzero_coverage"] == 0.75
    assert audit["best_candidate"] == "income.n_income_attr_p"


def test_pit_financial_features_fall_back_to_income_profit_denominator_pit_safely() -> None:
    trade_dates = pd.DataFrame({"ts_code": ["000001.SZ", "000001.SZ"], "date": pd.to_datetime(["2020-04-20", "2020-05-10"])})
    statements = {
        "cashflow": pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "end_date": "20200331",
                    "ann_date": "20200430",
                    "f_ann_date": "20200430",
                    "n_cashflow_act": 200.0,
                    "net_profit": None,
                }
            ]
        ),
        "income": pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "end_date": "20200331",
                    "ann_date": "20200430",
                    "f_ann_date": "20200430",
                    "n_income_attr_p": 100.0,
                    "total_profit": 110.0,
                }
            ]
        ),
    }

    features = build_pit_financial_features(statements, trade_dates)

    assert pd.isna(features.loc[0, "operating_cashflow_to_profit"])
    assert features.loc[1, "operating_cashflow_to_profit"] == 2.0
    assert features.loc[1, "pit_cashflow_denominator_raw"] == 100.0
    assert features.loc[1, "pit_cashflow_denominator_source"] == "tushare.income.n_income_attr_p"
