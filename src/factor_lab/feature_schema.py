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
    "total_mv",
}

# Backward-compatible aliases for expression fields.
EXPRESSION_ALIASES: Final[dict[str, str]] = {}
