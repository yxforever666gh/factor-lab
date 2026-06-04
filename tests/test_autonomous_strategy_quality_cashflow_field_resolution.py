from factor_lab.autonomous_strategy_quality_cashflow_field_resolution import build_quality_cashflow_field_resolution


def request(fields):
    return {"mechanism_id": "quality_cashflow_value_repair_v1", "required_fields": fields}


def test_quality_cashflow_field_resolution_requests_data_for_missing_and_proxy_fields():
    report = build_quality_cashflow_field_resolution(
        run_id="r",
        mechanism_request=request([
            "date",
            "ticker",
            "pb",
            "roe",
            "ocfps",
            "net_profit_yoy",
            "debt_to_assets",
            "current_ratio",
        ]),
        available_fields={"date", "ticker", "pb", "roe", "operating_cashflow_to_profit", "profit_yoy", "debt_to_asset"},
    )
    statuses = {row["field"]: row["resolution_status"] for row in report["field_resolutions"]}
    assert statuses["date"] == "available"
    assert statuses["roe"] == "available_requires_pit_validation"
    assert statuses["ocfps"] == "proxy_available_but_not_equivalent"
    assert statuses["net_profit_yoy"] == "available_alias_requires_pit_validation"
    assert statuses["debt_to_assets"] == "available_alias_requires_pit_validation"
    assert statuses["current_ratio"] == "missing_external_or_not_supported"
    assert report["decision"] == "request_data"
    assert report["ready_for_cheap_screen"] is False
    assert report["controlled_execution_allowed"] is False
    assert report["queue_write_allowed"] is False


def test_quality_cashflow_field_resolution_blocks_until_pit_when_all_fields_exist():
    fields = ["date", "ticker", "pb", "roe", "gross_margin"]
    report = build_quality_cashflow_field_resolution(
        run_id="r",
        mechanism_request=request(fields),
        available_fields=set(fields),
    )
    assert report["decision"] == "block_until_pit_alignment"
    assert report["recommended_next_step"] == "prove_report_date_alignment"
    assert report["ready_for_cheap_screen"] is False
    assert "roe" in report["pit_validation_fields"]


def test_quality_cashflow_field_resolution_allows_cheap_screen_for_non_pit_fields_only():
    fields = ["date", "ticker", "industry", "pb", "pe_ttm", "forward_return_5d"]
    report = build_quality_cashflow_field_resolution(
        run_id="r",
        mechanism_request=request(fields),
        available_fields=set(fields),
    )
    assert report["decision"] == "prepare_quality_cashflow_cheap_screen"
    assert report["ready_for_cheap_screen"] is True
    assert report["next_allowed_actions"] == ["cheap_screen_plan"]
    assert report["controlled_execution_allowed"] is False
