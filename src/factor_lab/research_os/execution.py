"""Compatibility exports for the canonical execution kernel.

New code should import :mod:`factor_lab.research_os.execution_kernel`.  This
module remains so existing callers keep a stable import path while all
calculations are performed by the single shared kernel.
"""

from factor_lab.research_os.execution_kernel import (
    AShareCostPolicy,
    TradeCostBreakdown,
    calculate_trade_costs,
    maximum_executable_notional,
    stamp_duty_rate,
)

__all__ = [
    "AShareCostPolicy",
    "TradeCostBreakdown",
    "calculate_trade_costs",
    "maximum_executable_notional",
    "stamp_duty_rate",
]
