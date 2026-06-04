from __future__ import annotations

from typing import Any


def budget_after_admission(*, max_cycle: int, max_daily: int, admitted: int, completed_today: int = 0) -> dict[str, int]:
    return {
        "remaining_cycle_experiments": max(0, max_cycle - admitted),
        "remaining_daily_experiments": max(0, max_daily - completed_today - admitted),
    }


def allocate_harvest_budget(
    policy: dict[str, Any], *, requested_experiments: int, completed_today: int = 0
) -> dict[str, Any]:
    max_cycle = min(2, int(policy.get("max_experiments_per_cycle", 2)))
    max_daily = max_cycle * int(policy.get("max_cycles_per_day", 4))
    daily_remaining = max(0, max_daily - int(completed_today))
    admitted = min(max_cycle, int(requested_experiments), daily_remaining)
    after = budget_after_admission(max_cycle=max_cycle, max_daily=max_daily, admitted=admitted, completed_today=completed_today)
    return {
        "requested_experiments": requested_experiments,
        "admitted_experiments": admitted,
        "max_cycle_experiments": max_cycle,
        "max_daily_experiments": max_daily,
        "budget_exhausted": admitted == 0 and requested_experiments > 0,
        **after,
    }
