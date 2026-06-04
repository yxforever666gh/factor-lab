from __future__ import annotations

from typing import Any

from factor_lab.harvest_self_correction_planner import DEFAULT_WINDOWS


def _actions(plan: dict[str, Any], action_type: str) -> list[dict[str, Any]]:
    return [a for a in plan.get("actions") or [] if a.get("type") == action_type]


def _last_action_value(plan: dict[str, Any], action_type: str, key: str, default: Any) -> Any:
    matches = _actions(plan, action_type)
    if matches and key in matches[-1]:
        return matches[-1][key]
    return default


def estimate_backtest_count(plan: dict[str, Any]) -> int:
    signals = _last_action_value(plan, "set_signal_columns", "signal_columns", ["industry_relative_book_yield"])
    costs = _last_action_value(plan, "restrict_costs", "cost_bps_values", [0, 30, 60])
    holdings = _last_action_value(plan, "set_holding_counts", "holding_counts", [50, 75, 100])
    windows = _last_action_value(plan, "set_windows", "year_windows", DEFAULT_WINDOWS)
    return max(0, len(signals or []) * len(costs or []) * len(holdings or []) * len(windows or []))


def budget_gate(*, estimated_backtests: int, used_backtests: int, max_backtests: int) -> dict[str, Any]:
    remaining = int(max_backtests) - int(used_backtests)
    if int(estimated_backtests) > remaining:
        return {"decision": "block", "reason": "backtest_budget_exceeded", "remaining": max(0, remaining)}
    return {"decision": "allow", "reason": "within_budget", "remaining": max(0, remaining - int(estimated_backtests))}
