from __future__ import annotations

import pandas as pd

from factor_lab.pit_value_trap_attribution import (
    build_field_coverage_report,
    build_final_decision,
    build_incremental_contribution_report,
    build_missing_value_treatment_report,
    build_single_field_ic_report,
)


def _sample_frame() -> pd.DataFrame:
    rows = []
    for date in ["2020-01-01", "2020-01-02", "2020-01-03"]:
        for i in range(10):
            rows.append(
                {
                    "date": date,
                    "ticker": f"00000{i}.SZ",
                    "industry": "A" if i < 5 else "B",
                    "forward_return_5d": i / 100.0,
                    "industry_relative_book_yield": i,
                    "operating_cashflow_to_profit": i if i < 2 else None,
                    "debt_to_assets": 10 - i,
                    "netprofit_yoy": 10 - i,
                    "tr_yoy": i,
                }
            )
    return pd.DataFrame(rows)


def test_field_coverage_flags_low_cashflow_and_final_coverage() -> None:
    df = _sample_frame()
    report = build_field_coverage_report(df)
    fields = {row["field"]: row for row in report["fields"]}

    assert fields["operating_cashflow_to_profit"]["coverage"] == 0.2
    assert report["final_expression_coverage"] == 0.2
    assert fields["debt_to_assets"]["coverage"] == 1.0


def test_single_field_ic_detects_negative_direction() -> None:
    df = _sample_frame()
    report = build_single_field_ic_report(df)
    fields = {row["field"]: row for row in report["fields"]}

    assert fields["industry_relative_book_yield"]["rank_ic_mean"] > 0
    assert fields["debt_to_assets"]["rank_ic_mean"] < 0
    assert fields["debt_to_assets"]["direction_decision"] == "negative_as_written"


def test_final_decision_stops_on_low_coverage_and_negative_ic() -> None:
    df = _sample_frame()
    coverage = build_field_coverage_report(df)
    ic = build_single_field_ic_report(df)
    incremental = build_incremental_contribution_report(df)
    decision = build_final_decision(coverage=coverage, ic=ic, scaling={"fields": []}, incremental=incremental)

    assert decision["decision"] == "stop_value_trap_combo_line_pending_attribution_fix"
    assert any("operating_cashflow_to_profit: coverage_below_30pct" == item for item in decision["hard_stops"])
    assert any("debt_to_assets: negative_single_field_ic" == item for item in decision["hard_stops"])


def test_missing_value_treatment_report_includes_fill_and_flag_variants() -> None:
    df = _sample_frame()
    report = build_missing_value_treatment_report(df, fields=["operating_cashflow_to_profit"])
    row = report["fields"][0]
    variants = {variant["variant"] for variant in row["variants"]}

    assert row["field"] == "operating_cashflow_to_profit"
    assert {"drop_missing", "global_median_fill", "date_industry_median_fill", "missing_flag_only"} <= variants
