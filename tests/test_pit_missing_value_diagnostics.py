from __future__ import annotations

import pandas as pd

from factor_lab.pit_missing_value_diagnostics import build_missing_value_diagnostics


def test_missing_value_diagnostics_compares_treatments() -> None:
    rows = []
    for date in ["2020-01-01", "2020-01-02", "2020-01-03"]:
        for i in range(6):
            rows.append({
                "date": date,
                "ticker": f"T{i}",
                "industry": "I1" if i < 3 else "I2",
                "operating_cashflow_to_profit": float(i) if i % 2 == 0 else None,
                "forward_return_5d": float(i),
            })
    df = pd.DataFrame(rows)

    report = build_missing_value_diagnostics(df, fields=["operating_cashflow_to_profit"])

    row = report["fields"][0]
    variants = {v["variant"]: v for v in row["variants"]}
    assert set(variants) == {
        "dropna",
        "date_industry_median_fill",
        "date_median_fill",
        "missing_penalty_flag",
        "high_coverage_universe_only",
    }
    assert variants["dropna"]["coverage"] == 0.5
    assert variants["date_median_fill"]["coverage"] == 1.0
    assert row["fragility"] in {"direction_changes_by_missing_treatment", "single_treatment_positive_only", "stable_or_consistently_weak"}


def test_missing_value_diagnostics_handles_missing_field() -> None:
    df = pd.DataFrame({"date": ["2020-01-01"], "forward_return_5d": [0.1]})
    report = build_missing_value_diagnostics(df, fields=["missing"])
    assert report["fields"][0]["fragility"] == "missing_field"
