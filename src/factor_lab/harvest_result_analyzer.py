from __future__ import annotations

import json
from pathlib import Path
from typing import Any

HARVEST_ROOT = Path("artifacts/harvest_agent")


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _ok_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [r for r in payload.get("results") or [] if r.get("status") == "ok"]


def analyze_result_payload(payload: dict[str, Any], *, cycle_id: str | None = None, drawdown_limit: float = -0.35, sharpe_min: float = 0.7) -> dict[str, Any]:
    rows = _ok_rows(payload)
    best = payload.get("best_result") or (max(rows, key=lambda r: (_num(r.get("total_return")), _num(r.get("sharpe"))), default={}))
    best_signal = best.get("signal_column")
    best_label = best.get("label")
    best_cost = _num(best.get("cost_bps"))
    best_total_return = _num(best.get("total_return"))
    best_sharpe = _num(best.get("sharpe"))
    best_drawdown = _num(best.get("max_drawdown"))

    same_signal_rows = [r for r in rows if r.get("signal_column") == best_signal]
    positive_cost_rows = [r for r in same_signal_rows if _num(r.get("cost_bps")) >= 30]
    positive_cost_profitable = [r for r in positive_cost_rows if _num(r.get("total_return")) > 0]
    labels = {str(r.get("label")) for r in rows if r.get("label")}
    positive_labels = {str(r.get("label")) for r in positive_cost_profitable if r.get("label")}

    cost_sensitive = best_cost <= 0 and bool(positive_cost_rows) and len(positive_cost_profitable) < max(1, len(positive_cost_rows) // 2)
    window_concentration = bool(best_label) and len(labels) > 1 and len(positive_labels) < 2
    drawdown_too_high = best_drawdown < drawdown_limit
    sharpe_too_low = best_sharpe < sharpe_min
    promotion_ready = (not drawdown_too_high) and (not sharpe_too_low) and (not cost_sensitive) and (not window_concentration) and best_total_return > 0

    return {
        "schema_version": 1,
        "cycle_id": cycle_id,
        "matrix_status": payload.get("matrix_status"),
        "ok_count": (payload.get("summary") or {}).get("ok_count", len(rows)),
        "result_count": (payload.get("summary") or {}).get("result_count", len(rows)),
        "best_result": best,
        "best_signal_column": best_signal,
        "best_label": best_label,
        "best_cost_bps": best_cost,
        "best_total_return": best_total_return,
        "best_sharpe": best_sharpe,
        "best_max_drawdown": best_drawdown,
        "cost_sensitive": cost_sensitive,
        "window_concentration_risk": window_concentration,
        "drawdown_too_high": drawdown_too_high,
        "sharpe_too_low": sharpe_too_low,
        "promotion_ready": promotion_ready,
        "positive_cost_profitable_count": len(positive_cost_profitable),
        "positive_cost_row_count": len(positive_cost_rows),
        "positive_cost_labels": sorted(positive_labels),
    }


def analyze_cycle_result(root: str | Path, cycle_id: str) -> dict[str, Any]:
    cycle_dir = Path(root) / HARVEST_ROOT / cycle_id
    matches = sorted(cycle_dir.glob("runs/*/result.json"))
    if not matches:
        return {"schema_version": 1, "cycle_id": cycle_id, "status": "missing_result", "source_result_path": None, "promotion_ready": False}
    payload = json.loads(matches[0].read_text(encoding="utf-8"))
    analysis = analyze_result_payload(payload, cycle_id=cycle_id)
    analysis["source_result_path"] = str(matches[0])
    return analysis
