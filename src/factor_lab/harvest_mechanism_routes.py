from __future__ import annotations

import json
from pathlib import Path
from typing import Any

DEFAULT_CONFIG_PATH = Path("configs/harvest_mechanism_routes.json")

_BUILTIN_ROUTES: dict[str, dict[str, Any]] = {
    "industry_relative_value": {
        "mechanism_id": "industry_relative_value",
        "allowed_signals": ["industry_relative_book_yield", "industry_relative_earnings_yield"],
        "required_fields": ["industry", "book_yield", "earnings_yield"],
        "default_filters": [],
        "rationale": "Test within-industry value ranks without adding unsupported data.",
    },
    "value_quality_no_distress": {
        "mechanism_id": "value_quality_no_distress",
        "allowed_signals": ["industry_relative_earnings_yield", "earnings_yield"],
        "required_fields": ["earnings_yield", "roe", "pb"],
        "default_filters": [
            {"field": "roe", "operator": ">=", "quantile": 0.4},
            {"field": "pb", "operator": "<=", "quantile": 0.8},
        ],
        "rationale": "Prefer cheap companies that are not obvious quality traps.",
    },
    "value_momentum_confirmation": {
        "mechanism_id": "value_momentum_confirmation",
        "allowed_signals": ["industry_relative_earnings_yield", "earnings_yield", "momentum_20"],
        "required_fields": ["earnings_yield", "momentum_20"],
        "default_filters": [{"field": "momentum_20", "operator": ">=", "quantile": 0.4}],
        "rationale": "Require market confirmation before trusting valuation repair.",
    },
    "cost_robust_value_quality": {
        "mechanism_id": "cost_robust_value_quality",
        "allowed_signals": ["industry_relative_earnings_yield", "earnings_yield"],
        "required_fields": ["earnings_yield", "roe", "turnover"],
        "default_filters": [{"field": "turnover", "operator": ">=", "quantile": 0.4}],
        "rationale": "Prioritize value-quality evidence that survives non-zero transaction costs.",
    },
    "low_volatility_value_quality": {
        "mechanism_id": "low_volatility_value_quality",
        "allowed_signals": ["industry_relative_earnings_yield", "earnings_yield", "industry_relative_book_yield"],
        "required_fields": ["earnings_yield", "roe", "volatility_20"],
        "default_filters": [{"field": "volatility_20", "operator": "<=", "quantile": 0.6}],
        "rationale": "Reduce drawdown by avoiding the highest short-term volatility names.",
    },
}


def load_mechanism_routes(config_path: str | Path | None = None) -> dict[str, dict[str, Any]]:
    path = Path(config_path) if config_path is not None else DEFAULT_CONFIG_PATH
    if not path.exists():
        return dict(_BUILTIN_ROUTES)
    payload = json.loads(path.read_text(encoding="utf-8"))
    routes = {}
    for route in payload.get("routes") or []:
        mechanism_id = route.get("mechanism_id")
        if mechanism_id:
            routes[str(mechanism_id)] = dict(route)
    return routes or dict(_BUILTIN_ROUTES)


def select_mechanism_route(diagnosis: dict[str, Any], *, config_path: str | Path | None = None, attempt_index: int = 0) -> dict[str, Any]:
    routes = load_mechanism_routes(config_path)
    failures = set(diagnosis.get("failure_classes") or [])
    if "zero_cost_best_only" in failures:
        key = "cost_robust_value_quality"
    elif "drawdown_too_high" in failures:
        key = "low_volatility_value_quality"
    elif "weak_risk_adjusted_return" in failures and attempt_index % 2 == 1:
        key = "value_momentum_confirmation"
    elif "weak_risk_adjusted_return" in failures:
        key = "value_quality_no_distress"
    else:
        key = "industry_relative_value"
    route = dict(routes.get(key) or routes["industry_relative_value"])
    route.setdefault("mechanism_id", key)
    route.setdefault("allowed_signals", [])
    route.setdefault("required_fields", [])
    route.setdefault("default_filters", [])
    route.setdefault("rationale", "bounded Harvest v2 mechanism route")
    return route
