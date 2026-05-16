from factor_lab.feature_schema import TUSHARE_AVAILABLE_FEATURE_COLUMNS, TUSHARE_BLOCKED_FEATURE_COLUMNS, TUSHARE_FEATURE_COLUMNS


def test_feature_schema_includes_value_research_p0_fields():
    expected = {
        "ps_ttm",
        "ps_yield",
        "roe_yoy",
        "roe_delta",
        "revenue_yoy",
        "profit_yoy",
        "debt_to_asset",
        "operating_cashflow_to_profit",
        "dividend_yield",
        "volatility_20",
        "volatility_60",
        "industry_relative_pb",
        "industry_relative_pe",
        "industry_relative_book_yield",
        "industry_relative_earnings_yield",
    }

    assert expected.issubset(TUSHARE_FEATURE_COLUMNS)
    assert {"debt_to_asset", "operating_cashflow_to_profit"}.issubset(TUSHARE_BLOCKED_FEATURE_COLUMNS)
    assert not TUSHARE_BLOCKED_FEATURE_COLUMNS & TUSHARE_AVAILABLE_FEATURE_COLUMNS
