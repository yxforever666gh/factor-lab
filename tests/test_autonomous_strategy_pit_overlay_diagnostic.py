import pandas as pd

from factor_lab.autonomous_strategy_pit_overlay_diagnostic import build_pit_overlay_diagnostic


def base_frame():
    return pd.DataFrame(
        {
            "ticker": ["A", "A", "B", "B"],
            "date": pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-01", "2020-01-02"]),
            "profit_yoy": [pd.NA, pd.NA, pd.NA, pd.NA],
            "debt_to_asset": [pd.NA, pd.NA, pd.NA, pd.NA],
            "operating_cashflow_to_profit": [pd.NA, pd.NA, pd.NA, pd.NA],
        }
    )


def test_pit_overlay_diagnostic_extends_when_overlay_coverage_low():
    pit = pd.DataFrame(
        {
            "ticker": ["A"],
            "date": pd.to_datetime(["2020-01-01"]),
            "profit_yoy": [1.0],
            "debt_to_asset": [0.4],
            "operating_cashflow_to_profit": [2.0],
        }
    )
    report = build_pit_overlay_diagnostic(run_id="r", base_frame=base_frame(), pit_frame=pit, base_path="base", pit_path="pit")
    assert report["decision"] == "extend_pit_cache_coverage"
    assert report["overlay_coverage"]["profit_yoy"] == 0.25
    assert report["controlled_execution_allowed"] is False
    assert report["queue_write_allowed"] is False


def test_pit_overlay_diagnostic_prepares_pit_review_when_coverage_enough():
    pit = pd.DataFrame(
        {
            "ticker": ["A", "A", "B", "B"],
            "date": pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-01", "2020-01-02"]),
            "profit_yoy": [1.0, 1.1, 1.2, 1.3],
            "debt_to_asset": [0.4, 0.5, 0.6, 0.7],
            "operating_cashflow_to_profit": [2.0, 2.1, 2.2, 2.3],
        }
    )
    report = build_pit_overlay_diagnostic(run_id="r", base_frame=base_frame(), pit_frame=pit, base_path="base", pit_path="pit")
    assert report["decision"] == "prepare_proxy_pit_alignment_review"
    assert report["recommended_next_step"] == "prove_proxy_report_date_alignment"
    assert report["overlay_coverage"]["operating_cashflow_to_profit"] == 1.0


def test_pit_overlay_diagnostic_blocks_empty_pit_frame():
    report = build_pit_overlay_diagnostic(run_id="r", base_frame=base_frame(), pit_frame=pd.DataFrame(), base_path="base", pit_path="pit")
    assert report["decision"] == "blocked_empty_pit_frame"
    assert report["recommended_next_step"] == "inspect_base_or_pit_cache"
