from __future__ import annotations

from typing import Any


def diagnose_analysis(analysis: dict[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    hypotheses: list[str] = []
    priorities: list[str] = []

    if analysis.get("drawdown_too_high"):
        failures.append("drawdown_too_high")
        hypotheses.append("Signal may capture value rebounds but portfolio construction leaves unmanaged downside exposure.")
        priorities.append("reduce_drawdown")
    if analysis.get("sharpe_too_low"):
        failures.append("weak_risk_adjusted_return")
        hypotheses.append("Raw ranking does not produce enough risk-adjusted edge after volatility of returns.")
        priorities.append("improve_risk_adjusted_return")
    if analysis.get("cost_sensitive") or float(analysis.get("best_cost_bps") or 0) <= 0:
        failures.append("zero_cost_best_only")
        hypotheses.append("Best candidate may rely on zero-cost assumptions and needs cost-aware selection.")
        priorities.append("test_cost_robustness")
    if analysis.get("window_concentration_risk"):
        failures.append("window_concentration")
        hypotheses.append("Evidence may be regime-specific rather than stable across independent windows.")
        priorities.append("expand_window_stability")
    if not analysis.get("promotion_ready"):
        failures.append("not_promotion_ready")

    seen: set[str] = set()
    ordered_priorities = []
    for item in priorities:
        if item not in seen:
            ordered_priorities.append(item)
            seen.add(item)

    return {
        "schema_version": 1,
        "cycle_id": analysis.get("cycle_id"),
        "failure_classes": failures,
        "root_cause_hypotheses": hypotheses,
        "next_repair_priority": ordered_priorities,
        "promotion_ready": bool(analysis.get("promotion_ready")),
    }
