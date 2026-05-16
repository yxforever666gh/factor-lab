from factor_lab.pit_data_preflight import decide_field_preflight, summarize_table_preflight, preflight_report_ready


def test_field_preflight_allows_high_coverage_ann_date():
    decision = decide_field_preflight("cashflow", available=True, coverage=0.9, ann_date_rate=1.0)
    assert decision.pit_safe is True


def test_field_preflight_blocks_low_coverage():
    decision = decide_field_preflight("cashflow", available=True, coverage=0.5, ann_date_rate=1.0)
    assert decision.pit_safe is False
    assert "coverage_below_threshold" in decision.reasons


def test_field_preflight_blocks_low_ann_date_rate():
    decision = decide_field_preflight("cashflow", available=True, coverage=0.9, ann_date_rate=0.9)
    assert decision.pit_safe is False
    assert "ann_date_rate_below_threshold" in decision.reasons


def test_summarize_table_preflight():
    result = summarize_table_preflight({
        "income": {"rows": 10, "ann_date_nonnull_rate": 1.0, "decision": {"coverage_vs_active_universe": 0.8}},
        "cashflow": {"rows": 0, "ann_date_nonnull_rate": None, "decision": {"coverage_vs_active_universe": 0.0}},
    })
    assert result["ready"] is False
    assert len(result["decisions"]) == 2


def test_preflight_report_ready_requires_sample_or_full_market_mode_and_no_side_effects():
    report = {
        "mode": "sample",
        "no_factor_run": True,
        "no_queue_write": True,
        "no_daemon_start": True,
        "tushare": {"summary": {"ready_for_p0_value_trap_experiment": True}},
        "diemeng": {"summary": {"ready_for_p0_value_trap_experiment": False}},
    }
    decision = preflight_report_ready(report)
    assert decision["ready"] is True
    assert decision["mode"] == "sample"
    assert decision["primary_source"] == "tushare"


def test_preflight_report_blocks_missing_mode_or_queue_write():
    report = {
        "no_factor_run": True,
        "no_queue_write": False,
        "no_daemon_start": True,
        "tushare": {"summary": {"ready_for_p0_value_trap_experiment": True}},
    }
    decision = preflight_report_ready(report)
    assert decision["ready"] is False
    assert "missing_or_invalid_mode" in decision["reasons"]
    assert "queue_write_not_allowed_in_preflight" in decision["reasons"]
