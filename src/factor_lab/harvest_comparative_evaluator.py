from __future__ import annotations

from typing import Any


def _best(payload: dict[str, Any]) -> dict[str, Any]:
    return payload.get("best_result") or {}


def _num(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def compare_results(baseline: dict[str, Any] | None, candidate: dict[str, Any]) -> dict[str, Any]:
    base = _best(baseline or {})
    cand = _best(candidate)
    deltas = {
        "total_return_delta": round(_num(cand.get("total_return")) - _num(base.get("total_return")), 6),
        "sharpe_delta": round(_num(cand.get("sharpe")) - _num(base.get("sharpe")), 6),
        "max_drawdown_delta": round(_num(cand.get("max_drawdown")) - _num(base.get("max_drawdown")), 6),
    }
    improvements: list[str] = []
    regressions: list[str] = []
    if deltas["sharpe_delta"] > 0:
        improvements.append("sharpe_improved")
    elif deltas["sharpe_delta"] < 0:
        regressions.append("sharpe_regressed")
    if deltas["max_drawdown_delta"] > 0:
        improvements.append("drawdown_improved")
    elif deltas["max_drawdown_delta"] < 0:
        regressions.append("drawdown_regressed")
    if deltas["total_return_delta"] > 0:
        improvements.append("total_return_improved")
    elif deltas["total_return_delta"] < 0:
        regressions.append("total_return_regressed")

    if _num(cand.get("sharpe")) >= 0.7 and _num(cand.get("max_drawdown")) >= -0.35 and _num(cand.get("cost_bps")) >= 30:
        decision = "manual_review_for_promotion"
    elif improvements:
        decision = "continue_modified_route"
    elif regressions and not improvements:
        decision = "pivot_or_stop"
    else:
        decision = "continue_same_mainline"

    return {
        "schema_version": 1,
        "baseline_best_result": base,
        "candidate_best_result": cand,
        "deltas": deltas,
        "improvements": improvements,
        "regressions": regressions,
        "decision": decision,
    }
