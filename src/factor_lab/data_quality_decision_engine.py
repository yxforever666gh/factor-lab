from __future__ import annotations

from typing import Any


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def build_data_quality_response(context: dict[str, Any], source_label: str = "heuristic") -> dict[str, Any]:
    inputs = context.get("inputs") or {}
    latest_run = inputs.get("latest_run") or {}
    dataset_rows = _as_int(latest_run.get("dataset_rows") or inputs.get("dataset_rows") or 0)
    last_error = str(inputs.get("last_error") or latest_run.get("last_error") or "")
    token_missing = "TUSHARE_TOKEN" in last_error and ("Missing" in last_error or "missing" in last_error)
    coverage_status = "empty" if dataset_rows == 0 else "ok"
    severity = "critical" if token_missing or (dataset_rows == 0 and (latest_run.get("status") == "failed" or last_error)) else ("warning" if dataset_rows == 0 else "ok")
    if token_missing:
        scope = "provider"
        likely_cause = "missing_tushare_token"
        recommended_action = "configure TUSHARE_TOKEN and restart daemon"
        token_status = "missing"
    else:
        scope = "dataset"
        likely_cause = "empty_dataset" if dataset_rows == 0 else "no_obvious_data_issue"
        recommended_action = "inspect data range/cache" if dataset_rows == 0 else "continue"
        token_status = "unknown"
    finding = {
        "scope": scope,
        "severity": severity,
        "symptom": last_error[:240] if last_error else f"dataset_rows={dataset_rows}",
        "likely_cause": likely_cause,
        "recommended_action": recommended_action,
    }
    return {
        "schema_version": "factor_lab.data_quality_agent_response.v1",
        "agent_name": "data-quality-local",
        "decision_source": source_label,
        "decision_context_id": context.get("context_id"),
        "data_quality_findings": [finding],
        "dataset_health": {
            "dataset_rows": dataset_rows,
            "coverage_status": coverage_status,
            "token_status": token_status,
        },
        "should_pause_research": severity == "critical",
        "summary_markdown": f"- data quality severity: {severity}",
    }
