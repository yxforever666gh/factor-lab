from __future__ import annotations

from typing import Any


def validate_real_backtest_result(result: dict[str, Any]) -> dict[str, Any]:
    if result.get("status") == "simulated_controlled_placeholder":
        return {"valid": False, "reason": "placeholder_status"}
    summary = result.get("summary") or {}
    execution = result.get("execution") or {}
    executed = execution.get("executed_count") or summary.get("executed_count") or result.get("executed_backtest_count")
    best = result.get("best_result") or {}
    if not executed or not best:
        return {"valid": False, "reason": "missing_metric_bearing_result"}
    return {"valid": True, "reason": "metric_bearing_result"}
