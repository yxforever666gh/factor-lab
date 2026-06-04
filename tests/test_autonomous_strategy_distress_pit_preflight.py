from __future__ import annotations

import pandas as pd

from factor_lab.autonomous_strategy_distress_pit_preflight import build_distress_pit_preflight


def field_resolution():
    return {"field_resolutions": [{"field": "interest_coverage", "resolution_status": "missing_external_or_derivation_required"}]}


def test_distress_pit_preflight_allows_proxy_screen_when_pit_safe_and_core_proxy_covered():
    frame = pd.DataFrame({
        "ticker": ["a", "b", "c"],
        "debt_to_asset": [0.1, 0.2, 0.3],
        "operating_cashflow_to_profit": [1.0, 0.5, 0.2],
        "profit_yoy": [0.1, 0.2, None],
        "netprofit_yoy": [0.1, None, 0.3],
        "pit_feature_validated": [True, True, True],
        "pit_source_ann_date": ["2020-04-01", "2020-04-02", "2020-04-03"],
        "pit_source_end_date": ["2019-12-31", "2019-12-31", "2019-12-31"],
    })
    report = build_distress_pit_preflight(run_id="x", field_resolution=field_resolution(), pit_frame=frame, min_ticker_coverage=0.5)
    assert report["ready_for_proxy_distress_screen"] is True
    assert report["decision"] == "use_proxy_distress_screen_without_interest_coverage"
    assert report["queue_write_allowed"] is False


def test_distress_pit_preflight_blocks_when_pit_alignment_missing():
    frame = pd.DataFrame({
        "ticker": ["a", "b"],
        "debt_to_asset": [0.1, 0.2],
        "operating_cashflow_to_profit": [1.0, 0.5],
        "pit_feature_validated": [False, False],
    })
    report = build_distress_pit_preflight(run_id="x", field_resolution=field_resolution(), pit_frame=frame)
    assert report["ready_for_proxy_distress_screen"] is False
    assert report["decision"] == "request_data_or_fix_pit_alignment"
