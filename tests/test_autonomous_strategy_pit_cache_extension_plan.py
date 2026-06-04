import pandas as pd

from factor_lab.autonomous_strategy_pit_cache_extension_plan import build_pit_cache_extension_plan


def frame(tickers, dates):
    rows = []
    for ticker in tickers:
        for date in dates:
            rows.append({"ticker": ticker, "date": pd.Timestamp(date)})
    return pd.DataFrame(rows)


def test_pit_cache_extension_plan_requires_human_when_overlay_low():
    base = frame(["A", "B", "C"], ["2020-01-01", "2020-01-02"])
    pit = frame(["A", "B"], ["2020-01-02"])
    overlay = {"overlay_coverage": {"profit_yoy": 0.4, "debt_to_asset": 0.4, "operating_cashflow_to_profit": 0.4}, "overlap_rows": 2}
    plan = build_pit_cache_extension_plan(run_id="r", overlay_diagnostic=overlay, base_frame=base, pit_frame=pit, base_path="base", pit_path="pit")
    assert plan["decision"] == "await_human_approval_for_pit_cache_extension"
    assert plan["recommended_next_step"] == "approve_or_decline_pit_cache_extension"
    assert plan["human_required"] is True
    assert plan["missing_tickers"] == ["C"]
    assert plan["missing_early_window"] == {"start_date": "2020-01-01", "end_date": "2020-01-01"}
    assert plan["controlled_execution_allowed"] is False
    assert plan["queue_write_allowed"] is False


def test_pit_cache_extension_plan_no_extension_when_coverage_enough():
    base = frame(["A"], ["2020-01-01"])
    pit = frame(["A"], ["2020-01-01"])
    overlay = {"overlay_coverage": {"profit_yoy": 0.7, "debt_to_asset": 0.8, "operating_cashflow_to_profit": 0.9}, "overlap_rows": 1}
    plan = build_pit_cache_extension_plan(run_id="r", overlay_diagnostic=overlay, base_frame=base, pit_frame=pit, base_path="base", pit_path="pit")
    assert plan["decision"] == "no_extension_needed"
    assert plan["recommended_next_step"] == "rerun_proxy_field_resolution_with_overlay"
    assert plan["human_required"] is False


def test_pit_cache_extension_plan_detects_late_window():
    base = frame(["A"], ["2020-01-01", "2020-01-03"])
    pit = frame(["A"], ["2020-01-01"])
    overlay = {"overlay_coverage": {"profit_yoy": 0.5, "debt_to_asset": 0.5, "operating_cashflow_to_profit": 0.5}, "overlap_rows": 1}
    plan = build_pit_cache_extension_plan(run_id="r", overlay_diagnostic=overlay, base_frame=base, pit_frame=pit, base_path="base", pit_path="pit")
    assert plan["missing_late_window"] == {"start_date": "2020-01-02", "end_date": "2020-01-03"}
