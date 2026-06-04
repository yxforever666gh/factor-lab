from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

_HISTORY_DERIVATION_RE = re.compile(r"rolling_history_window:(?P<days>\d+)d")


def _candidate_route(route_registry: dict[str, Any], route_id: str) -> dict[str, Any]:
    for route in route_registry.get("routes") or []:
        if route.get("route_id") == route_id:
            return route
    raise ValueError(f"route_id not found in route registry: {route_id}")


def _history_specs_for_route(*, route_id: str, derivation_specs: dict[str, Any]) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    for spec in derivation_specs.get("derived_fields") or []:
        if route_id not in (spec.get("routes") or []):
            continue
        derivation = str(spec.get("derivation") or "")
        match = _HISTORY_DERIVATION_RE.fullmatch(derivation)
        if not match:
            continue
        item = dict(spec)
        item["window_days"] = int(match.group("days"))
        specs.append(item)
    return specs


def _coverage_for_source(
    *,
    frame: pd.DataFrame,
    derived_field: str,
    source_field: str,
    min_observations: int,
    min_eligible_ticker_ratio: float,
) -> dict[str, Any]:
    ticker_count = int(frame["ticker"].nunique()) if "ticker" in frame.columns else 0
    base = {
        "derived_field": derived_field,
        "source_field": source_field,
        "min_observations": int(min_observations),
        "min_eligible_ticker_ratio": float(min_eligible_ticker_ratio),
        "source_field_exists": source_field in frame.columns,
        "ticker_count": ticker_count,
        "non_null_rows": 0,
        "total_rows": int(len(frame)),
        "non_null_row_ratio": 0.0,
        "eligible_ticker_count": 0,
        "eligible_ticker_ratio": 0.0,
        "status": "missing_source_field",
    }
    if source_field not in frame.columns:
        return base
    if "ticker" not in frame.columns:
        base["status"] = "missing_ticker_field"
        return base

    usable = frame[["ticker", source_field]].copy()
    usable = usable[usable[source_field].notna()]
    non_null_rows = int(len(usable))
    eligible = usable.groupby("ticker")[source_field].size()
    eligible_ticker_count = int((eligible >= min_observations).sum())
    eligible_ticker_ratio = eligible_ticker_count / ticker_count if ticker_count else 0.0
    non_null_row_ratio = non_null_rows / len(frame) if len(frame) else 0.0
    status = "pass" if eligible_ticker_ratio >= min_eligible_ticker_ratio else "insufficient_history"
    base.update(
        {
            "non_null_rows": non_null_rows,
            "non_null_row_ratio": round(non_null_row_ratio, 6),
            "eligible_ticker_count": eligible_ticker_count,
            "eligible_ticker_ratio": round(eligible_ticker_ratio, 6),
            "status": status,
        }
    )
    return base


def build_historical_valuation_coverage_preflight(
    *,
    run_id: str,
    route_registry: dict[str, Any],
    derivation_specs: dict[str, Any],
    frame: pd.DataFrame,
    source_path: str,
    route_id: str = "historical_relative_valuation_repair",
    min_observations: int | None = None,
    min_eligible_ticker_ratio: float = 0.60,
) -> dict[str, Any]:
    route = _candidate_route(route_registry, route_id)
    specs = _history_specs_for_route(route_id=route_id, derivation_specs=derivation_specs)
    field_coverage = []
    for spec in specs:
        required_observations = int(min_observations or spec.get("window_days") or 756)
        field_coverage.append(
            _coverage_for_source(
                frame=frame,
                derived_field=str(spec.get("field")),
                source_field=str(spec.get("source_field")),
                min_observations=required_observations,
                min_eligible_ticker_ratio=min_eligible_ticker_ratio,
            )
        )
    overall_status = "pass" if field_coverage and all(item["status"] == "pass" for item in field_coverage) else "blocked"
    next_allowed_actions = [
        "manual_review_coverage_report",
        "prepare_information_screen_preview",
    ] if overall_status == "pass" else [
        "manual_review_coverage_report",
        "request_data_or_extend_cache",
    ]

    date_min = str(pd.to_datetime(frame["date"]).min().date()) if "date" in frame.columns and len(frame) else None
    date_max = str(pd.to_datetime(frame["date"]).max().date()) if "date" in frame.columns and len(frame) else None
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "preflight_only",
        "route_id": route_id,
        "route_status": route.get("route_status"),
        "source_path": source_path,
        "date_min": date_min,
        "date_max": date_max,
        "row_count": int(len(frame)),
        "ticker_count": int(frame["ticker"].nunique()) if "ticker" in frame.columns else 0,
        "min_eligible_ticker_ratio": float(min_eligible_ticker_ratio),
        "overall_status": overall_status,
        "field_coverage": field_coverage,
        "required_manual_review": True,
        "queue_write_allowed": False,
        "controlled_execution_allowed": False,
        "automation_allowed": False,
        "next_allowed_actions": next_allowed_actions,
        "blocked_actions": [
            "full_backtest",
            "queue_write",
            "timer_enable",
            "broad_daemon_restore",
            "auto_promotion",
        ],
    }


def coverage_preflight_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Autonomous Strategy Coverage Preflight",
        "",
        f"run_id: {report.get('run_id')}",
        f"route_id: {report.get('route_id')}",
        f"mode: {report.get('mode')}",
        f"overall_status: {report.get('overall_status')}",
        f"source_path: {report.get('source_path')}",
        f"date_range: {report.get('date_min')} → {report.get('date_max')}",
        f"row_count: {report.get('row_count')}",
        f"ticker_count: {report.get('ticker_count')}",
        f"controlled_execution_allowed: {report.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "## Field coverage",
    ]
    for item in report.get("field_coverage") or []:
        lines.extend(
            [
                f"### {item.get('derived_field')}",
                f"- source_field: {item.get('source_field')}",
                f"- status: {item.get('status')}",
                f"- source_field_exists: {item.get('source_field_exists')}",
                f"- eligible_ticker_count: {item.get('eligible_ticker_count')} / {item.get('ticker_count')}",
                f"- eligible_ticker_ratio: {item.get('eligible_ticker_ratio')}",
                f"- min_observations: {item.get('min_observations')}",
                "",
            ]
        )
    if not report.get("field_coverage"):
        lines.append("- none")
    lines.extend(["## Next allowed actions"])
    lines.extend(f"- {action}" for action in report.get("next_allowed_actions") or [])
    lines.append("")
    lines.extend(["## Blocked actions"])
    lines.extend(f"- {action}" for action in report.get("blocked_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_coverage_preflight(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "historical_valuation_coverage_preflight.json"
    md_path = out / "historical_valuation_coverage_preflight.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(coverage_preflight_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
