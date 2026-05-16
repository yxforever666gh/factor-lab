from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from factor_lab.bucket_aware_coverage_preflight import evaluate_bucket_aware_preflight
from factor_lab.feature_schema import TUSHARE_AVAILABLE_FEATURE_COLUMNS
from factor_lab.pit_cashflow_closure_policy import evaluate_cashflow_closure
from factor_lab.workflow_admission import evaluate_workflow_admission
from factor_lab.portfolio_construction_config import parse_portfolio_construction

PIT_FINANCIAL_ADMISSION_FIELDS = {
    "operating_cashflow_to_profit",
    "debt_to_asset",
    "debt_to_assets",
    "profit_yoy",
    "revenue_yoy",
    "netprofit_yoy",
    "tr_yoy",
    "profit_growth_ok",
    "revenue_growth_ok",
    "operating_cashflow_to_profit_zscore_by_date_industry",
    "reversed_operating_cashflow_to_profit_zscore_by_date_industry",
    "debt_to_assets_zscore_by_date_industry",
    "debt_to_asset_zscore_by_date_industry",
    "low_debt_to_assets_zscore_by_date_industry",
    "netprofit_yoy_zscore_by_date_industry",
    "reversed_netprofit_yoy_zscore_by_date_industry",
    "tr_yoy_zscore_by_date_industry",
    "reversed_tr_yoy_zscore_by_date_industry",
}


def _payload(task: Mapping[str, Any]) -> dict[str, Any]:
    payload = task.get("payload") or task.get("payload_json") or {}
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _load_config(config_path: str | None) -> tuple[dict[str, Any], str | None]:
    if not config_path:
        return {}, None
    try:
        return json.loads(Path(config_path).read_text(encoding="utf-8")), None
    except Exception as exc:
        return {}, str(exc)


def admission_input_from_task(task: Mapping[str, Any]) -> dict[str, Any]:
    enriched = dict(task)
    payload = _payload(task)
    config, error = _load_config(str(payload.get("config_path") or "") or None)
    if config:
        for key in (
            "route_id",
            "mechanism_id",
            "required_data_fields",
            "required_pit_features",
            "pit_requirements",
            "feature_overlay_columns",
            "feature_overlay_csv",
            "factors",
            "factor_definitions",
            "baseline_reason",
            "validation_protocol_name",
            "portfolio_construction",
            "governance",
        ):
            if key in config and key not in payload:
                payload[key] = config[key]
    if error:
        payload["_config_load_error"] = error
    enriched["payload"] = payload
    return enriched


def _pit_admission_fields(payload: Mapping[str, Any]) -> set[str] | None:
    overlay_fields = {str(field) for field in (payload.get("feature_overlay_columns") or []) if str(field)}
    if payload.get("pit_requirements") or payload.get("required_pit_features"):
        return set(TUSHARE_AVAILABLE_FEATURE_COLUMNS) | PIT_FINANCIAL_ADMISSION_FIELDS | overlay_fields
    if overlay_fields:
        return set(TUSHARE_AVAILABLE_FEATURE_COLUMNS) | overlay_fields
    return None


def enforce_workflow_admission(task: Mapping[str, Any], *, policy_path: str | None = None) -> dict[str, Any]:
    task_type = str(task.get("task_type") or "")
    if task_type != "workflow":
        return {"decision": "allow", "reasons": ["non_workflow_task"], "task": dict(task), "admission": None}

    enriched = admission_input_from_task(task)
    payload = _payload(enriched)
    if payload.get("_config_load_error"):
        return {
            "decision": "block",
            "reasons": ["config_load_failed"],
            "task": enriched,
            "admission": {"decision": "block", "reasons": ["config_load_failed"], "error": payload.get("_config_load_error")},
        }
    try:
        parse_portfolio_construction(payload)
    except ValueError as exc:
        return {
            "decision": "block",
            "reasons": ["invalid_portfolio_construction"],
            "task": enriched,
            "admission": {"decision": "block", "reasons": ["invalid_portfolio_construction"], "error": str(exc)},
        }
    closure = evaluate_cashflow_closure(payload)
    if not closure.allowed:
        return {
            "decision": "block",
            "reasons": list(closure.reasons),
            "task": enriched,
            "admission": {"decision": "block", **closure.to_dict()},
        }
    bucket_preflight = evaluate_bucket_aware_preflight(payload, available_fields=_pit_admission_fields(payload))
    if bucket_preflight.get("decision") in {"block", "hold"}:
        return {
            "decision": "block",
            "reasons": list(bucket_preflight.get("reasons") or []),
            "task": enriched,
            "admission": {"decision": "block", **bucket_preflight},
        }

    admission_fields = _pit_admission_fields(payload)
    admission = evaluate_workflow_admission(enriched, available_fields=admission_fields)
    return {
        "decision": admission.get("decision"),
        "reasons": list(admission.get("reasons") or []),
        "task": enriched,
        "admission": admission,
    }


def assert_workflow_admitted(task: Mapping[str, Any]) -> dict[str, Any]:
    result = enforce_workflow_admission(task)
    if result.get("decision") != "allow":
        raise RuntimeError("workflow_admission_blocked:" + ",".join(result.get("reasons") or []))
    return result
