from __future__ import annotations

import pandas as pd

from factor_lab.pit_cashflow_source_audit import build_cashflow_source_audit, expected_pit_financial_cache_path


def test_cashflow_source_audit_detects_output_retention_gap_when_raw_columns_missing(tmp_path) -> None:
    df = pd.DataFrame(
        {
            "date": ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04"],
            "ticker": ["A", "B", "C", "D"],
            "operating_cashflow_to_profit": [1.0, None, None, None],
            "pit_feature_validated": [True, True, True, True],
            "pit_source_ann_date": [20200101, 20200101, 20200101, 20200101],
            "pit_source_end_date": [20191231, 20191231, 20191231, 20191231],
        }
    )
    cache_path = expected_pit_financial_cache_path(df, tmp_path)
    assert cache_path is not None
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df[["ticker", "date", "operating_cashflow_to_profit", "pit_feature_validated"]].to_csv(cache_path, index=False)

    report = build_cashflow_source_audit(df, cache_dir=tmp_path)

    assert report["final_diagnosis"] == "output_column_dropped_before_dataset"
    assert report["raw_source_proof_level"] == "blocked_by_missing_raw_retention"
    categories = {row["category"]: row["rows"] for row in report["missingness_split"]}
    assert categories["output_column_dropped_before_dataset"] == 3


def test_cashflow_source_audit_splits_denominator_missing_when_raw_inputs_visible(tmp_path) -> None:
    df = pd.DataFrame(
        {
            "date": ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04"],
            "ticker": ["A", "B", "C", "D"],
            "operating_cashflow_to_profit": [1.0, None, None, None],
            "pit_feature_validated": [True, True, True, True],
            "pit_source_ann_date": [20200101, 20200101, 20200101, 20200101],
            "pit_source_end_date": [20191231, 20191231, 20191231, 20191231],
            "pit_cashflow_numerator_raw": [10.0, 11.0, 12.0, None],
            "pit_cashflow_denominator_raw": [10.0, 0.0, None, 4.0],
        }
    )

    report = build_cashflow_source_audit(df, cache_dir=tmp_path)

    assert report["raw_source_proof_level"] == "direct"
    categories = {row["category"]: row["rows"] for row in report["missingness_split"]}
    assert categories["available"] == 1
    assert categories["denominator_missing_or_zero"] == 2
    assert categories["raw_cashflow_fetched_but_missing_field"] == 1
