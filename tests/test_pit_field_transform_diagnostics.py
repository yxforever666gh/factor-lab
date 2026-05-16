from __future__ import annotations

import pandas as pd

from factor_lab.pit_field_transform_diagnostics import build_field_transform_diagnostics


def test_field_transform_diagnostics_reports_reversed_variant_when_raw_is_negative() -> None:
    rows = []
    for date in ["2020-01-01", "2020-01-02", "2020-01-03"]:
        for i in range(6):
            rows.append({
                "date": date,
                "industry": "I1" if i < 3 else "I2",
                "operating_cashflow_to_profit": float(i),
                "forward_return_5d": float(6 - i),
            })
    df = pd.DataFrame(rows)

    report = build_field_transform_diagnostics(df, fields=["operating_cashflow_to_profit"])

    row = report["fields"][0]
    assert row["exists"] is True
    assert row["best_variant_by_ic"] in {"reversed", "reversed_winsorized_zscore"}
    variants = {v["variant"]: v for v in row["variants"]}
    assert variants["reversed"]["rank_ic_mean"] > 0
    assert variants["raw"]["rank_ic_mean"] < 0


def test_field_transform_diagnostics_handles_missing_field() -> None:
    df = pd.DataFrame({"date": ["2020-01-01"], "forward_return_5d": [0.1]})
    report = build_field_transform_diagnostics(df, fields=["missing"])
    assert report["fields"][0]["exists"] is False
