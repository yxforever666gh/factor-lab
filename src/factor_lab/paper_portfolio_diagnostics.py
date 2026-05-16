from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def load_portfolio(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _latest_previous_portfolio(history_path: str | Path) -> dict[str, Any] | None:
    p = Path(history_path)
    if not p.exists():
        return None
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, list) or not payload:
        return None
    for item in reversed(payload):
        if isinstance(item, dict):
            return item
    return None


def _position_map(portfolio: dict[str, Any] | None) -> dict[str, float]:
    if not portfolio:
        return {}
    positions = portfolio.get("positions") or []
    result: dict[str, float] = {}
    if not isinstance(positions, list):
        return result
    fallback_weight = 1.0 / len(positions) if positions else 0.0
    for row in positions:
        if not isinstance(row, dict) or not row.get("ticker"):
            continue
        ticker = str(row["ticker"])
        try:
            weight = float(row.get("weight", fallback_weight))
        except (TypeError, ValueError):
            weight = fallback_weight
        result[ticker] = weight
    return result


def build_turnover_diagnostics(current: dict[str, Any], previous: dict[str, Any] | None) -> dict[str, Any]:
    current_weights = _position_map(current)
    if not previous:
        return {
            "history_status": "insufficient_history",
            "current_count": len(current_weights),
            "previous_count": None,
            "added_count": None,
            "removed_count": None,
            "overlap_count": None,
            "added_tickers": [],
            "removed_tickers": [],
            "turnover_one_way_estimate": None,
        }

    previous_weights = _position_map(previous)
    current_tickers = set(current_weights)
    previous_tickers = set(previous_weights)
    added = sorted(current_tickers - previous_tickers)
    removed = sorted(previous_tickers - current_tickers)
    overlap = current_tickers & previous_tickers

    all_tickers = current_tickers | previous_tickers
    one_way = 0.5 * sum(abs(current_weights.get(ticker, 0.0) - previous_weights.get(ticker, 0.0)) for ticker in all_tickers)

    return {
        "history_status": "ok",
        "current_count": len(current_weights),
        "previous_count": len(previous_weights),
        "added_count": len(added),
        "removed_count": len(removed),
        "overlap_count": len(overlap),
        "added_tickers": added,
        "removed_tickers": removed,
        "turnover_one_way_estimate": round(float(one_way), 6),
    }


def build_cost_diagnostics(turnover: dict[str, Any], cost_bps: float) -> dict[str, Any]:
    bps = float(cost_bps)
    one_way_turnover = turnover.get("turnover_one_way_estimate")
    if one_way_turnover is None:
        estimated_one_way_cost = None
        estimated_round_trip_cost = None
    else:
        estimated_one_way_cost = round(float(one_way_turnover) * bps / 10000.0, 6)
        estimated_round_trip_cost = round(estimated_one_way_cost * 2.0, 6)
    return {
        "cost_bps": bps,
        "estimated_one_way_cost": estimated_one_way_cost,
        "estimated_round_trip_cost": estimated_round_trip_cost,
    }


def build_paper_portfolio_diagnostics(
    current_path: str | Path,
    history_path: str | Path,
    benchmark_id: str,
    benchmark_name: str,
    cost_bps: float,
) -> dict[str, Any]:
    current = load_portfolio(current_path)
    previous = _latest_previous_portfolio(history_path)
    turnover = build_turnover_diagnostics(current, previous)
    cost = build_cost_diagnostics(turnover, cost_bps)
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "strategy_name": current.get("strategy_name"),
        "as_of_date": current.get("as_of_date"),
        "position_count": current.get("position_count", len(current.get("positions") or [])),
        "benchmark": {
            "benchmark_id": benchmark_id,
            "benchmark_name": benchmark_name,
            "tracking_mode": "metadata_only",
        },
        "turnover": turnover,
        "cost": cost,
    }


def diagnostics_to_markdown(payload: dict[str, Any]) -> str:
    benchmark = payload.get("benchmark") or {}
    turnover = payload.get("turnover") or {}
    cost = payload.get("cost") or {}
    lines = [
        "# Paper Portfolio Diagnostics",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Strategy: {payload.get('strategy_name')}",
        f"As-of date: {payload.get('as_of_date')}",
        f"Position count: {payload.get('position_count')}",
        "",
        "## Benchmark",
        f"- benchmark_id: {benchmark.get('benchmark_id')}",
        f"- benchmark_name: {benchmark.get('benchmark_name')}",
        f"- tracking_mode: {benchmark.get('tracking_mode')}",
        "",
        "## Turnover",
        f"- history_status: {turnover.get('history_status')}",
        f"- added_count: {turnover.get('added_count')}",
        f"- removed_count: {turnover.get('removed_count')}",
        f"- overlap_count: {turnover.get('overlap_count')}",
        f"- turnover_one_way_estimate: {turnover.get('turnover_one_way_estimate')}",
        "",
        "## Cost",
        f"- cost_bps: {cost.get('cost_bps')}",
        f"- estimated_one_way_cost: {cost.get('estimated_one_way_cost')}",
        f"- estimated_round_trip_cost: {cost.get('estimated_round_trip_cost')}",
    ]
    return "\n".join(lines) + "\n"


def write_paper_portfolio_diagnostics(
    *,
    current_path: str | Path,
    history_path: str | Path,
    json_path: str | Path,
    markdown_path: str | Path,
    benchmark_id: str,
    benchmark_name: str,
    cost_bps: float,
) -> dict[str, Any]:
    payload = build_paper_portfolio_diagnostics(
        current_path=current_path,
        history_path=history_path,
        benchmark_id=benchmark_id,
        benchmark_name=benchmark_name,
        cost_bps=cost_bps,
    )
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(diagnostics_to_markdown(payload), encoding="utf-8")
    return payload
