from __future__ import annotations

"""Feature schema shared across planners/generators.

The research loop needs a stable notion of which fields are available for factor expressions.
We keep this conservative to avoid hard-stopping the daemon on deterministic config errors.
"""

from typing import Final


# Columns produced by TushareDataProvider._build_feature_frame (excluding date/ticker).
TUSHARE_FEATURE_COLUMNS: Final[set[str]] = {
    "industry",
    "close",
    "return_1d",
    "forward_return_5d",
    "turnover",
    "momentum_20",
    "momentum_60",
    "momentum_120",
    "momentum_60_skip_5",
    "turnover_shock_5_20",
    "earnings_yield",
    "book_yield",
    "roe",
    "size_inv",
    "pe_ttm",
    "pb",
    "ps_ttm",
    "ps_yield",
    "revenue_yoy",
    "profit_yoy",
    "roe_yoy",
    "roe_delta",
    "debt_to_asset",
    "operating_cashflow_to_profit",
    "dividend_yield",
    "volatility_20",
    "volatility_60",
    "industry_relative_pb",
    "industry_relative_pe",
    "industry_relative_book_yield",
    "industry_relative_earnings_yield",
    "total_mv",
}

# Backward-compatible aliases for expression fields.
EXPRESSION_ALIASES: Final[dict[str, str]] = {}

# Fields that are part of the research schema but not yet populated by the live provider/cache.
# Governance/preflight treats these as blockers until provider support is implemented.
TUSHARE_BLOCKED_FEATURE_COLUMNS: Final[set[str]] = {
    "revenue_yoy",
    "profit_yoy",
    "debt_to_asset",
    "operating_cashflow_to_profit",
}

TUSHARE_AVAILABLE_FEATURE_COLUMNS: Final[set[str]] = TUSHARE_FEATURE_COLUMNS - TUSHARE_BLOCKED_FEATURE_COLUMNS
