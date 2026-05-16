from __future__ import annotations

from typing import Any, Iterable

from factor_lab.expression_validation import validate_expression
from factor_lab.feature_schema import TUSHARE_AVAILABLE_FEATURE_COLUMNS, TUSHARE_FEATURE_COLUMNS


def evaluate_factor_coverage(
    *,
    factor: dict[str, Any],
    available_fields: Iterable[str] | None = None,
    valid_ratio: float | None = None,
    min_full_run_coverage: float = 0.6,
    min_cheap_screen_coverage: float = 0.3,
) -> dict[str, Any]:
    available = set(available_fields or TUSHARE_AVAILABLE_FEATURE_COLUMNS)
    required = set(factor.get("required_data_fields") or [])
    expression = str(factor.get("expression") or "")
    validation = validate_expression(expression, available_fields=available) if expression else None
    expression_missing = set(validation.unknown_fields if validation else [])
    missing = sorted((required - available) | expression_missing)
    if missing:
        return {
            "factor_name": factor.get("name"),
            "coverage_status": "blocked_missing_fields",
            "missing_fields": missing,
            "valid_ratio": valid_ratio,
            "recommendation": "block",
            "reasons": ["missing required or expression fields"],
        }
    ratio = 1.0 if valid_ratio is None else float(valid_ratio)
    if ratio >= min_full_run_coverage:
        recommendation = "full_run"
        status = "ready"
    elif ratio >= min_cheap_screen_coverage:
        recommendation = "cheap_screen"
        status = "low_coverage"
    else:
        recommendation = "block"
        status = "coverage_too_low"
    return {
        "factor_name": factor.get("name"),
        "coverage_status": status,
        "missing_fields": [],
        "valid_ratio": ratio,
        "recommendation": recommendation,
        "reasons": [] if recommendation == "full_run" else [status],
    }
