import pandas as pd

from factor_lab.pit_financial_schema import (
    CANONICAL_DATE_COLUMNS,
    CANONICAL_TABLE_NAMES,
    P0_FEATURE_NAMES,
    field_names,
    fields_by_group,
    resolve_source_field,
    source_alternatives_for_feature,
    validate_required_fields,
)


def test_schema_contains_all_plan_p0_value_trap_fields():
    names = field_names(p0_only=True)
    for expected in P0_FEATURE_NAMES:
        assert expected in names


def test_schema_groups_are_auditable_and_match_plan_names():
    grouped = fields_by_group()
    assert "cashflow_quality" in grouped
    assert "leverage_distress" in grouped
    assert "growth_repair" in grouped
    assert "profitability_quality" in grouped
    assert "valuation_base" in grouped


def test_every_p0_feature_has_source_and_pit_policy():
    grouped = fields_by_group()
    all_records = [record for records in grouped.values() for record in records]
    by_name = {record["name"]: record for record in all_records}
    for name in P0_FEATURE_NAMES:
        record = by_name[name]
        assert record["preferred_sources"]
        assert record["source_fields"]
        assert "requires_pit" in record
        if record["group"] != "valuation_base":
            assert record["requires_pit"] is True


def test_canonical_tables_and_date_columns_are_explicit():
    assert CANONICAL_TABLE_NAMES == ("income", "balancesheet", "cashflow", "financial_indicator")
    assert CANONICAL_DATE_COLUMNS == ("ann_date", "f_ann_date", "end_date")


def test_validate_required_fields_blocks_unknown():
    result = validate_required_fields(["operating_cashflow_to_profit", "fake_field"])
    assert result["known"] is False
    assert result["missing_fields"] == ["fake_field"]


def test_source_mapping_resolves_tushare_fina_indicator_alias():
    result = resolve_source_field("tushare", "fina_indicator", "netprofit_yoy")
    assert result["known"] is True
    assert result["canonical_table"] == "financial_indicator"
    assert result["canonical_field"] == "netprofit_yoy"


def test_unknown_source_field_returns_blocked_result_not_none():
    result = resolve_source_field("tushare", "cashflow", "not_a_real_field")
    assert result["known"] is False
    assert result["blocked_reason"] == "unknown_source_field"


def test_canonical_feature_lists_source_alternatives():
    alternatives = source_alternatives_for_feature("debt_to_assets")
    assert "tushare.balancesheet.total_liab" in alternatives
    assert "tushare.balancesheet.total_assets" in alternatives
    assert "diemeng.financial_indicator.debt_to_assets" in alternatives
