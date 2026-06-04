from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Any


def _rows(result_payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw = result_payload.get("results") or result_payload.get("backtest_results") or []
    return [r for r in raw if isinstance(r, dict) and str(r.get("status", "ok")).lower() in {"ok", "success"}]


def _num(row: dict[str, Any], *keys: str, default: float = 0.0) -> float:
    for key in keys:
        value = row.get(key)
        if value is not None:
            try:
                return float(value)
            except Exception:
                pass
    return default


def _key(row: dict[str, Any], *keys: str, default: str = "unknown") -> str:
    for key in keys:
        value = row.get(key)
        if value is not None and value != "":
            return str(value)
    return default


def _summarize_group(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    sharpes = [_num(r, "sharpe", "sharpe_net") for r in rows]
    drawdowns = [_num(r, "max_drawdown", "drawdown") for r in rows]
    returns = [_num(r, "total_return", "return") for r in rows]
    return {
        "key": key,
        "count": len(rows),
        "best_sharpe": max(sharpes) if sharpes else None,
        "worst_sharpe": min(sharpes) if sharpes else None,
        "mean_sharpe": mean(sharpes) if sharpes else None,
        "best_drawdown": max(drawdowns) if drawdowns else None,
        "worst_drawdown": min(drawdowns) if drawdowns else None,
        "mean_drawdown": mean(drawdowns) if drawdowns else None,
        "best_total_return": max(returns) if returns else None,
        "worst_total_return": min(returns) if returns else None,
        "mean_total_return": mean(returns) if returns else None,
        "profitable_count": sum(1 for v in returns if v > 0),
        "cost_positive_count": sum(1 for r in rows if _num(r, "cost_bps") > 0),
    }


def _group(rows: list[dict[str, Any]], picker) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        buckets[picker(row)].append(row)
    return [_summarize_group(v, k) for k, v in sorted(buckets.items())]


def attribute_harvest_failure(result_payload: dict[str, Any], oos_validation: dict[str, Any] | None = None) -> dict[str, Any]:
    ok_rows = _rows(result_payload)
    if not ok_rows:
        return {
            "schema_version": 1,
            "attribution_class": "insufficient_data",
            "ok_row_count": 0,
            "primary_blockers": ["no_ok_rows"],
            "groups": {},
        }

    thresholds = (oos_validation or {}).get("thresholds") or {}
    sharpe_min = float(thresholds.get("sharpe_min", 0.7))
    drawdown_min = float(thresholds.get("max_drawdown_min", -0.35))
    by_window = _group(ok_rows, lambda r: _key(r, "label", "window"))
    by_signal = _group(ok_rows, lambda r: _key(r, "signal_column", "signal"))
    by_cost = _group(ok_rows, lambda r: str(int(_num(r, "cost_bps"))))
    by_holding = _group(ok_rows, lambda r: str(int(_num(r, "holding_count", default=0))))
    blockers: list[str] = []

    worst_window = min(by_window, key=lambda g: g.get("worst_drawdown") if g.get("worst_drawdown") is not None else 0)
    worst_signal = min(by_signal, key=lambda g: g.get("worst_drawdown") if g.get("worst_drawdown") is not None else 0)
    if (worst_window.get("worst_drawdown") or 0) < drawdown_min:
        blockers.append("drawdown_concentrated_by_window")
    if (worst_signal.get("worst_drawdown") or 0) < drawdown_min:
        blockers.append("drawdown_concentrated_by_signal")

    best_row = max(ok_rows, key=lambda r: _num(r, "sharpe", "sharpe_net"))
    positive_cost_rows = [r for r in ok_rows if _num(r, "cost_bps") > 0]
    if _num(best_row, "cost_bps") == 0 and positive_cost_rows:
        if max(_num(r, "sharpe", "sharpe_net") for r in positive_cost_rows) < _num(best_row, "sharpe", "sharpe_net"):
            blockers.append("zero_cost_only_best")
            blockers.append("cost_sensitivity")

    if max(_num(r, "sharpe", "sharpe_net") for r in ok_rows) < sharpe_min:
        blockers.append("weak_all_windows")
    holding_means = [g.get("mean_sharpe") for g in by_holding if g.get("mean_sharpe") is not None]
    if holding_means and max(holding_means) - min(holding_means) > 0.5:
        blockers.append("holding_count_instability")
    if any(_num(r, "total_return") > 0 for r in ok_rows) and any(_num(r, "max_drawdown") < drawdown_min for r in ok_rows):
        blockers.append("possible_portfolio_construction_issue")

    return {
        "schema_version": 1,
        "attribution_class": "attributed" if blockers else "no_primary_blocker",
        "ok_row_count": len(ok_rows),
        "primary_blockers": list(dict.fromkeys(blockers)),
        "worst_window": worst_window,
        "worst_signal": worst_signal,
        "groups": {"window": by_window, "signal": by_signal, "cost_bps": by_cost, "holding_count": by_holding},
    }
