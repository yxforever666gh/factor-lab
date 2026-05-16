from __future__ import annotations

from typing import Any, Iterable

from factor_lab.feature_schema import TUSHARE_FEATURE_COLUMNS


def evaluate_bucket_aware_preflight(
    payload: dict[str, Any],
    *,
    available_fields: Iterable[str] | None = None,
    route_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    construction = payload.get("portfolio_construction") or {}
    if construction.get("mode") != "bucket_pair":
        return {"decision": "allow", "reasons": [], "missing_fields": []}
    fields = set(available_fields or TUSHARE_FEATURE_COLUMNS)
    required = [str(f) for f in (payload.get("required_data_fields") or [])]
    missing = sorted([f for f in required if f not in fields])
    if missing:
        return {"decision": "block", "reasons": ["bucket_aware_coverage_preflight_failed"], "missing_fields": missing}
    route = str(payload.get("route_id") or "")
    policy_row = ((route_policy or {}).get("routes") or {}).get(route, {})
    if isinstance(policy_row, dict) and policy_row.get("decision") == "hold":
        return {"decision": "hold", "reasons": [str(policy_row.get("reason") or "route_policy_hold")], "missing_fields": []}
    if isinstance(policy_row, dict) and policy_row.get("decision") == "demote":
        return {"decision": "block", "reasons": [str(policy_row.get("reason") or "route_policy_demoted")], "missing_fields": []}
    return {"decision": "allow", "reasons": [], "missing_fields": []}
