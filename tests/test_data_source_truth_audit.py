from pathlib import Path

import pandas as pd

from factor_lab.data_source_truth_audit import (
    build_data_source_truth_audit,
    classify_pit_status,
    compute_field_coverage_by_year,
    write_data_source_truth_audit,
)


def test_classifies_daily_price_fields_as_market_daily_observed():
    pit_status, decision, leakage, pit_required, _ = classify_pit_status(
        "close", present=True, source_kind="tushare_feature_cache"
    )
    assert pit_status == "market_daily_observed"
    assert decision == "usable"
    assert leakage == "low"
    assert pit_required is False


def test_classifies_pit_financial_fields_with_ann_date_as_strict_pit():
    pit_status, decision, leakage, pit_required, _ = classify_pit_status(
        "netprofit_yoy",
        present=True,
        source_kind="pit_financial_cache",
        has_pit_validated=True,
        has_ann_date=True,
    )
    assert pit_status == "strict_pit"
    assert decision == "usable"
    assert leakage == "low"
    assert pit_required is True


def test_classifies_legacy_roe_ambiguity():
    pit_status, decision, leakage, pit_required, _ = classify_pit_status(
        "roe", present=True, source_kind="tushare_feature_cache"
    )
    assert pit_status == "legacy_ambiguous"
    assert decision == "ambiguous_legacy"
    assert leakage == "medium"
    assert pit_required is True


def test_blocks_missing_fields():
    pit_status, decision, leakage, pit_required, _ = classify_pit_status(
        "shareholder_count", present=False, source_kind=None
    )
    assert pit_status == "blocked"
    assert decision == "blocked_missing_field"
    assert leakage == "high"
    assert pit_required is False


def test_computes_per_year_coverage_from_toy_frame():
    frame = pd.DataFrame(
        {
            "date": ["2020-01-01", "2020-01-02", "2021-01-01"],
            "ticker": ["A", "B", "A"],
            "close": [1.0, None, 3.0],
        }
    )
    coverage = compute_field_coverage_by_year(frame, ["close"])
    assert coverage["close"] == {"2020": 0.5, "2021": 1.0}


def test_writes_json_and_md_outputs(tmp_path: Path):
    cache = tmp_path / "artifacts" / "tushare_cache"
    cache.mkdir(parents=True)
    pd.DataFrame(
        {
            "date": ["2020-01-01", "2020-01-02"],
            "ticker": ["A", "B"],
            "close": [1.0, 2.0],
            "pb": [1.2, 1.3],
        }
    ).to_csv(cache / "tushare_2020-01-01_2020-01-02_2.csv", index=False)
    pd.DataFrame(
        {
            "date": ["2020-01-01", "2020-01-02"],
            "ticker": ["A", "B"],
            "netprofit_yoy": [10.0, 20.0],
            "pit_feature_validated": [True, True],
            "pit_source_ann_date": ["2019-10-31", "2019-10-31"],
        }
    ).to_csv(cache / "pit_financial_2020-01-01_2020-01-02_2_abcdef_v2.csv", index=False)

    report = write_data_source_truth_audit(tmp_path)
    assert Path(report["json_path"]).exists()
    assert Path(report["markdown_path"]).exists()
    assert Path(report["knowledge_path"]).exists()
    by_field = {r["field_name"]: r for r in report["fields"]}
    assert by_field["close"]["pit_status"] == "market_daily_observed"
    assert by_field["netprofit_yoy"]["pit_status"] == "strict_pit"
    assert by_field["roe"]["decision"] in {"blocked_missing_field", "ambiguous_legacy"}


def test_build_report_has_no_side_effect_markers(tmp_path: Path):
    (tmp_path / "artifacts" / "tushare_cache").mkdir(parents=True)
    report = build_data_source_truth_audit(tmp_path)
    assert report["no_network"] is True
    assert report["no_queue_write"] is True
    assert report["no_daemon_start"] is True
