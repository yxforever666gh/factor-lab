from __future__ import annotations

import pandas as pd

from factor_lab.pit_cashflow_coverage_diagnostics import build_cashflow_coverage_diagnostics


def test_cashflow_coverage_diagnostics_classifies_missing_numerator_gap() -> None:
    df = pd.DataFrame(
        {
            "date": ["2020-01-01", "2020-01-01", "2020-01-02"],
            "ticker": ["A", "B", "C"],
            "industry": ["I1", "I1", "I2"],
            "operating_cashflow_to_profit": [1.0, None, None],
            "netprofit_yoy": [2.0, 3.0, 4.0],
            "pit_feature_validated": [True, True, True],
            "pit_source_ann_date": [20200101, 20200101, 20200101],
            "pit_source_end_date": [20191231, 20191231, 20191231],
            "pit_feature_blocked_reason": [None, None, None],
        }
    )

    report = build_cashflow_coverage_diagnostics(df)

    assert report["coverage"] == 1 / 3
    assert report["non_null_rows"] == 1
    assert report["diagnosis"] == "coverage_ok"
    categories = {row["category"]: row["rows"] for row in report["category_breakdown"]}
    assert categories["available"] == 1
    assert categories["missing_cashflow_statement_or_numerator"] == 2


def test_cashflow_coverage_diagnostics_flags_pit_asof_gap_when_metadata_missing() -> None:
    df = pd.DataFrame(
        {
            "date": ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04"],
            "ticker": ["A", "B", "C", "D"],
            "industry": ["I1", "I1", "I2", "I2"],
            "operating_cashflow_to_profit": [None, None, None, 1.0],
            "pit_feature_validated": [False, False, False, True],
            "pit_source_ann_date": [None, None, None, 20200101],
            "pit_source_end_date": [None, None, None, 20191231],
            "pit_feature_blocked_reason": ["missing_ann_date", "missing_ann_date", "missing_ann_date", None],
        }
    )

    report = build_cashflow_coverage_diagnostics(df)

    assert report["hard_stop_triggered"] is True
    assert report["diagnosis"] == "pit_asof_or_announcement_gap"
