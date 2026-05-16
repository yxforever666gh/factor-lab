from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from factor_lab.feature_schema import TUSHARE_AVAILABLE_FEATURE_COLUMNS, TUSHARE_FEATURE_COLUMNS

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_VALUE_ROUTE_PATH = ROOT / "configs" / "value_research_family.json"


def _load_payload(path: Path = DEFAULT_VALUE_ROUTE_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_value_research_routes(path: str | Path | None = None) -> list[dict[str, Any]]:
    payload = _load_payload(Path(path) if path else DEFAULT_VALUE_ROUTE_PATH)
    routes = payload.get("routes") or []
    normalized: list[dict[str, Any]] = []
    for route in routes:
        row = dict(route)
        row.setdefault("mechanism_id", row.get("route_id"))
        row.setdefault("target_family", "value")
        row.setdefault("budget_bucket", "mechanism_validation")
        row.setdefault("expected_horizons", ["60d", "120d"])
        row.setdefault("falsification_criteria", [])
        normalized.append(row)
    return normalized


def build_value_route_candidates(
    *,
    available_fields: Iterable[str] | None = None,
    routes: Iterable[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    available = set(available_fields or TUSHARE_AVAILABLE_FEATURE_COLUMNS)
    output: list[dict[str, Any]] = []
    for route in routes or load_value_research_routes():
        required = [str(field) for field in route.get("required_data_fields") or []]
        missing = sorted(set(required) - available)
        base = {
            "route_id": route.get("route_id"),
            "mechanism_id": route.get("mechanism_id") or route.get("route_id"),
            "target_family": route.get("target_family", "value"),
            "hypothesis": route.get("hypothesis", ""),
            "required_data_fields": required,
            "missing_fields": missing,
            "expected_horizons": route.get("expected_horizons") or ["60d", "120d"],
            "falsification_criteria": route.get("falsification_criteria") or [],
            "budget_bucket": route.get("budget_bucket") or "mechanism_validation",
        }
        if missing:
            output.append({**base, "status": "blocked_missing_fields", "candidate_expressions": []})
            continue
        for expression in route.get("candidate_expressions") or []:
            output.append({**base, "status": "ready", "name": f"{route.get('route_id')}::{expression}", "expression": expression})
    return output
