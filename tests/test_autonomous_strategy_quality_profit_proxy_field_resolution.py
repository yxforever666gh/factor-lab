from factor_lab.autonomous_strategy_quality_profit_proxy_field_resolution import build_quality_profit_proxy_field_resolution


def revision(fields):
    return {
        "mechanism_id": "quality_profit_proxy_value_repair_v1",
        "proxy_required_fields": fields,
        "proxy_caveats": ["proxy caveat"],
    }


def test_proxy_field_resolution_blocks_missing_fields():
    report = build_quality_profit_proxy_field_resolution(
        run_id="r",
        proxy_revision=revision(["date", "ticker", "roe"]),
        available_fields={"date", "ticker"},
        coverage_by_field={"date": 1.0, "ticker": 1.0},
    )
    assert report["decision"] == "request_data"
    assert report["missing_fields"] == ["roe"]
    assert report["controlled_execution_allowed"] is False
    assert report["queue_write_allowed"] is False


def test_proxy_field_resolution_blocks_low_coverage_before_pit():
    report = build_quality_profit_proxy_field_resolution(
        run_id="r",
        proxy_revision=revision(["date", "roe"]),
        available_fields={"date", "roe"},
        coverage_by_field={"date": 1.0, "roe": 0.2},
    )
    assert report["decision"] == "block_low_coverage"
    assert report["recommended_next_step"] == "extend_proxy_field_coverage"
    assert report["low_coverage_fields"] == ["roe"]


def test_proxy_field_resolution_blocks_until_pit_alignment_when_fields_exist():
    fields = ["date", "ticker", "industry", "pb", "pe_ttm", "forward_return_5d", "roe", "profit_yoy", "debt_to_asset", "operating_cashflow_to_profit"]
    report = build_quality_profit_proxy_field_resolution(
        run_id="r",
        proxy_revision=revision(fields),
        available_fields=set(fields),
        coverage_by_field={field: 0.95 for field in fields},
    )
    assert report["decision"] == "block_until_pit_alignment"
    assert report["recommended_next_step"] == "prove_proxy_report_date_alignment"
    assert report["ready_for_proxy_cheap_screen_plan"] is False
    assert set(report["pit_validation_fields"]) == {"roe", "profit_yoy", "debt_to_asset", "operating_cashflow_to_profit"}


def test_proxy_field_resolution_allows_plan_for_non_pit_fields_only():
    fields = ["date", "ticker", "industry", "pb", "pe_ttm", "forward_return_5d"]
    report = build_quality_profit_proxy_field_resolution(
        run_id="r",
        proxy_revision=revision(fields),
        available_fields=set(fields),
        coverage_by_field={field: 1.0 for field in fields},
    )
    assert report["decision"] == "prepare_proxy_cheap_screen_plan"
    assert report["ready_for_proxy_cheap_screen_plan"] is True
    assert report["next_allowed_actions"] == ["proxy_cheap_screen_plan"]
