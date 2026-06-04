from __future__ import annotations

from typing import Any


def build_portfolio_branch_plan(
    *,
    decision: dict[str, Any],
    current_plan: dict[str, Any],
    failure_attribution: dict[str, Any],
    mechanism_route: dict[str, Any],
) -> dict[str, Any]:
    branch = str(decision.get("decision") or "repair_same_route")
    route_signals = list(mechanism_route.get("allowed_signals") or [])
    actions: list[dict[str, Any]] = []
    portfolio_construction: list[dict[str, Any]] = []
    if branch == "risk_reduction_branch":
        actions.extend([
            {"type": "add_filter", "field": "volatility_20", "operator": "<=", "quantile": 0.5},
            {"type": "add_filter", "field": "volatility_20", "operator": "<=", "quantile": 0.4},
            {"type": "set_holding_counts", "holding_counts": [75, 100]},
            {"type": "restrict_costs", "cost_bps_values": [30, 60]},
        ])
    elif branch == "portfolio_construction_branch":
        portfolio_construction.extend([
            {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 0},
            {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 1},
        ])
        actions.append({"type": "set_signal_columns", "signal_columns": route_signals})
    elif branch == "cost_robustness_branch":
        actions.extend([
            {"type": "restrict_costs", "cost_bps_values": [30, 60]},
            {"type": "add_filter", "field": "turnover", "operator": ">=", "quantile": 0.4},
            {"type": "prefer_cost_robust"},
        ])
    return {
        "schema_version": 1,
        "branch": branch,
        "mechanism_id": mechanism_route.get("mechanism_id"),
        "signal_columns": route_signals,
        "actions": actions,
        "portfolio_construction": portfolio_construction,
        "rationale": list(decision.get("rationale") or []),
    }
