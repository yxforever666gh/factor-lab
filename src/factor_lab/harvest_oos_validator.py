from __future__ import annotations

from typing import Any


def _ok_rows(result_payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [row for row in result_payload.get("results") or [] if row.get("status") == "ok"]


def validate_oos_robustness(
    result_payload: dict[str, Any],
    *,
    sharpe_min: float = 0.7,
    max_drawdown_min: float = -0.35,
    min_ok_windows: int = 2,
    positive_cost_bps: float = 30.0,
) -> dict[str, Any]:
    rows = _ok_rows(result_payload)
    labels = {str(row.get("label")) for row in rows if row.get("label") is not None}
    positive_cost_rows = [
        row
        for row in rows
        if float(row.get("cost_bps") or 0.0) >= positive_cost_bps and float(row.get("total_return") or 0.0) > 0.0
    ]
    best_sharpe = max((float(row.get("sharpe") or 0.0) for row in rows), default=0.0)
    worst_drawdown = min((float(row.get("max_drawdown") or 0.0) for row in rows), default=0.0)
    best_total_return = max((float(row.get("total_return") or 0.0) for row in rows), default=0.0)
    cost_robust = len({str(row.get("label")) for row in positive_cost_rows}) >= min_ok_windows

    reasons: list[str] = []
    if not rows:
        reasons.append("no_ok_rows")
        oos_class = "insufficient_data"
    else:
        if len(labels) < min_ok_windows:
            reasons.append("too_few_ok_windows")
        if best_sharpe < sharpe_min:
            reasons.append("sharpe_below_threshold")
        if worst_drawdown < max_drawdown_min:
            reasons.append("drawdown_below_threshold")
        if not cost_robust:
            reasons.append("not_cost_robust")
        if not reasons:
            oos_class = "pass"
        elif best_total_return > 0 and len(labels) >= min_ok_windows and best_sharpe >= sharpe_min * 0.9 and worst_drawdown >= max_drawdown_min - 0.05:
            oos_class = "near_miss"
        else:
            oos_class = "fail"

    return {
        "schema_version": 1,
        "oos_class": oos_class,
        "ok_window_count": len(labels),
        "ok_row_count": len(rows),
        "cost_positive_window_count": len({str(row.get("label")) for row in positive_cost_rows}),
        "cost_robust": cost_robust,
        "best_sharpe": best_sharpe,
        "worst_drawdown": worst_drawdown,
        "best_total_return": best_total_return,
        "thresholds": {
            "sharpe_min": sharpe_min,
            "max_drawdown_min": max_drawdown_min,
            "min_ok_windows": min_ok_windows,
            "positive_cost_bps": positive_cost_bps,
        },
        "reasons": reasons,
        "promotion_manual_review_required": oos_class == "pass",
    }
