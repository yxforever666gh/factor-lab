from __future__ import annotations

from typing import Any

from factor_lab.feature_schema import TUSHARE_AVAILABLE_FEATURE_COLUMNS

_RECOMMENDED: dict[str, list[str]] = {
    "value_quality_no_distress": ["debt_to_asset", "operating_cashflow_to_profit", "profit_yoy", "revenue_yoy", "dividend_yield"],
    "industry_relative_value": ["industry_relative_pb", "industry_relative_pe", "dividend_yield"],
    "value_momentum_confirmation": ["profit_yoy", "revenue_yoy"],
    "low_volatility_value_quality": ["debt_to_asset", "operating_cashflow_to_profit"],
    "cost_robust_value_quality": ["turnover", "operating_cashflow_to_profit"],
}


def build_harvest_data_request(mechanism_route: dict[str, Any], available_fields: set[str] | None = None) -> dict[str, Any]:
    available = set(available_fields) if available_fields is not None else set(TUSHARE_AVAILABLE_FEATURE_COLUMNS)
    required = list(mechanism_route.get("required_fields") or [])
    missing = [field for field in required if field not in available]
    mid = str(mechanism_route.get("mechanism_id") or "unknown")
    recommended = [field for field in _RECOMMENDED.get(mid, []) if field not in available]
    rationale: list[str] = []
    if missing:
        rationale.append("required fields missing; branch must be blocked before controlled execution")
    if recommended:
        rationale.append("additional fields would improve mechanism test quality")
    return {
        "schema_version": 1,
        "mechanism_id": mid,
        "blocked": bool(missing),
        "required_fields": required,
        "available_required_fields": [f for f in required if f in available],
        "missing_required_fields": missing,
        "recommended_data": recommended,
        "rationale": rationale,
    }
