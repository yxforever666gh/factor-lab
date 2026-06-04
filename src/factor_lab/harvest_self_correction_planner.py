from __future__ import annotations

from typing import Any

from factor_lab.harvest_mechanism_routes import select_mechanism_route

DEFAULT_WINDOWS = [
    {"label": "2020-2021", "start_date": "2020-01-01", "end_date": "2021-12-31"},
    {"label": "2021-2022", "start_date": "2021-01-01", "end_date": "2022-12-31"},
    {"label": "2022-2023", "start_date": "2022-01-01", "end_date": "2023-12-31"},
]


def _add(actions: list[dict[str, Any]], action: dict[str, Any]) -> None:
    if action not in actions:
        actions.append(action)


def build_correction_plan(
    analysis: dict[str, Any],
    diagnosis: dict[str, Any],
    *,
    next_cycle_id: str,
    attempt_index: int = 0,
    dataset_path: str | None = None,
    research_decision: dict[str, Any] | None = None,
    failure_attribution: dict[str, Any] | None = None,
    route_state: dict[str, Any] | None = None,
    portfolio_branch_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    failures = set(diagnosis.get("failure_classes") or [])
    decision = (research_decision or {}).get("decision")
    if decision in {"stop_route", "data_request"}:
        return {
            "schema_version": 2,
            "cycle_id": next_cycle_id,
            "based_on_cycle": analysis.get("cycle_id"),
            "plan_status": "stopped" if decision == "stop_route" else "blocked",
            "executable": False,
            "objective": "v3 non-executable research decision",
            "attempt_index": attempt_index,
            "dataset_path": dataset_path,
            "mechanism_route": select_mechanism_route(diagnosis, attempt_index=attempt_index),
            "actions": [],
            "research_decision": research_decision,
            "success_criteria": {},
        }
    route = select_mechanism_route(diagnosis, attempt_index=attempt_index)
    actions: list[dict[str, Any]] = []
    best_signal = analysis.get("best_signal_column") or "industry_relative_book_yield"

    if "drawdown_too_high" in failures:
        quantile = 0.6 if attempt_index % 2 == 0 else 0.5
        _add(actions, {"type": "add_filter", "field": "volatility_20", "operator": "<=", "quantile": quantile})
        _add(actions, {"type": "set_holding_counts", "holding_counts": [75, 100] if attempt_index % 2 == 0 else [50, 75]})
    if "zero_cost_best_only" in failures:
        _add(actions, {"type": "restrict_costs", "cost_bps_values": [30, 60] if attempt_index % 3 != 2 else [60]})
        _add(actions, {"type": "prefer_cost_robust"})
        _add(actions, {"type": "add_filter", "field": "turnover", "operator": ">=", "quantile": 0.3 if attempt_index % 2 == 0 else 0.4})
    if "weak_risk_adjusted_return" in failures:
        route_signals = list(route.get("allowed_signals") or [])
        signals = [best_signal] + route_signals + ["industry_relative_earnings_yield", "earnings_yield"]
        deduped = []
        for s in signals:
            if s and s not in deduped:
                deduped.append(s)
        if route_signals:
            allowed = set(route_signals)
            deduped = [s for s in deduped if s in allowed]
        if attempt_index % 3 == 1:
            deduped = list(reversed(deduped))
        _add(actions, {"type": "set_signal_columns", "signal_columns": deduped})
    if "window_concentration" in failures:
        _add(actions, {"type": "set_windows", "year_windows": DEFAULT_WINDOWS})

    if not actions:
        _add(actions, {"type": "set_signal_columns", "signal_columns": [best_signal]})
        _add(actions, {"type": "restrict_costs", "cost_bps_values": [30, 60]})

    for default_filter in route.get("default_filters") or []:
        _add(actions, {"type": "add_filter", **default_filter})
    if portfolio_branch_plan:
        for action in portfolio_branch_plan.get("actions") or []:
            _add(actions, dict(action))

    return {
        "schema_version": 2,
        "cycle_id": next_cycle_id,
        "based_on_cycle": analysis.get("cycle_id"),
        "plan_status": "planned",
        "objective": "repair route using prior-cycle diagnostics",
        "attempt_index": attempt_index,
        "dataset_path": dataset_path,
        "mechanism_route": route,
        "actions": actions,
        "research_decision": research_decision,
        "failure_attribution": failure_attribution,
        "route_state": route_state,
        "portfolio_branch_plan": portfolio_branch_plan,
        "executable": True,
        "success_criteria": {
            "max_drawdown_min": -0.35,
            "sharpe_min": 0.7,
            "positive_at_cost_bps": 30,
            "min_ok_windows": 2,
        },
    }
