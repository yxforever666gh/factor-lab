import pandas as pd

from factor_lab.autonomous_strategy_proxy_pit_alignment import build_proxy_pit_alignment_review


def good_frame():
    return pd.DataFrame(
        {
            "ticker": ["A", "A"],
            "date": pd.to_datetime(["2020-05-01", "2020-05-02"]),
            "profit_yoy": [1.0, 1.1],
            "debt_to_asset": [0.4, 0.5],
            "operating_cashflow_to_profit": [2.0, 2.1],
            "pit_source_ann_date": ["20200430", "20200430"],
            "pit_source_end_date": ["20191231", "20191231"],
            "pit_feature_validated": [True, True],
        }
    )


def test_proxy_pit_alignment_prepares_cheap_screen_when_pit_safe():
    report = build_proxy_pit_alignment_review(run_id="r", pit_frame=good_frame(), pit_path="pit.csv")
    assert report["decision"] == "prepare_proxy_cheap_screen_plan"
    assert report["recommended_next_step"] == "write_proxy_cheap_screen_plan"
    assert report["usable_coverage"] == 1.0
    assert report["controlled_execution_allowed"] is False
    assert report["queue_write_allowed"] is False


def test_proxy_pit_alignment_blocks_future_ann_date():
    frame = good_frame()
    frame.loc[0, "pit_source_ann_date"] = "20200503"
    frame.loc[1, "pit_source_ann_date"] = "20200504"
    report = build_proxy_pit_alignment_review(run_id="r", pit_frame=frame, pit_path="pit.csv")
    assert report["decision"] == "block_proxy_pit_alignment"
    assert report["usable_coverage"] == 0.0


def test_proxy_pit_alignment_blocks_unvalidated_rows():
    frame = good_frame()
    frame["pit_feature_validated"] = [False, False]
    report = build_proxy_pit_alignment_review(run_id="r", pit_frame=frame, pit_path="pit.csv")
    assert report["decision"] == "block_proxy_pit_alignment"
    assert report["pit_feature_validated_coverage"] == 0.0
