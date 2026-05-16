from __future__ import annotations

import pandas as pd

from factor_lab.institutional_holding_source_mvp import (
    build_institutional_holding_source_report,
    endpoint_schema_report,
)


def test_endpoint_schema_report_blocks_end_date_only() -> None:
    df = pd.DataFrame({"ts_code": ["000001.SZ"], "end_date": ["20231231"], "holder_name": ["x"]})
    report = endpoint_schema_report(df, endpoint="top10_holders")
    assert report["pit_control"] == "end_date_only_not_pit_safe"


def test_build_report_stops_when_only_end_date_rows() -> None:
    report = build_institutional_holding_source_report({
        "top10_holders": pd.DataFrame({"ts_code": ["000001.SZ"] * 120, "end_date": ["20231231"] * 120, "holder_name": ["x"] * 120})
    })
    assert report["decision"]["decision"] == "stop_institutional_holding_not_pit_safe"
    assert "not_pit_safe_end_date_only_or_missing_announcement_date" in report["decision"]["reasons"]


def test_build_report_can_proceed_with_ann_date_rows() -> None:
    rows = [{"ts_code": f"000{i:03d}.SZ", "ann_date": "20240131", "end_date": "20231231", "holder_name": "x"} for i in range(120)]
    report = build_institutional_holding_source_report({"top10_floatholders": pd.DataFrame(rows)})
    assert report["decision"]["decision"] == "proceed_institutional_holding_readonly_feature_plan"
