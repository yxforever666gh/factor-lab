from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.market_phenomena_schema import SAFETY_FLAGS

FUTURE_FIELD_RE = re.compile(r"(^|_)future_|forward_", re.IGNORECASE)
FINANCIAL_FIELD_HINTS = {
    "profit_yoy",
    "roe",
    "debt_to_asset",
    "debt_to_asset_delta",
    "operating_cashflow_to_profit",
}


def default_data_catalog() -> dict[str, Any]:
    """Small v1 catalog for feasibility checks against currently known Factor Lab fields.

    This intentionally does not fetch new data. It records whether an idea is
    observable enough for minimal verification planning.
    """

    return {
        "available_fields": [
            "date",
            "ticker",
            "industry",
            "profit_yoy",
            "roe",
            "debt_to_asset",
            "debt_to_asset_delta",
            "operating_cashflow_to_profit",
            "pb",
            "industry_return_60d",
            "industry_relative_pb",
            "industry_relative_earnings_yield",
            "volume",
            "turnover_rate",
            "amount",
            "market_cap",
            "future_20d_return",
            "future_60d_return",
            "future_120d_return",
            "forward_return_60d",
        ],
        "pit_fields": ["profit_yoy", "roe", "debt_to_asset", "debt_to_asset_delta", "operating_cashflow_to_profit"],
        "coverage_by_field": {
            "profit_yoy": 0.9769,
            "roe": 1.0,
            "debt_to_asset": 0.9769,
            "debt_to_asset_delta": 0.90,
            "operating_cashflow_to_profit": 0.9769,
            "pb": 0.98,
            "industry": 0.98,
            "industry_return_60d": 0.95,
            "industry_relative_pb": 0.90,
            "industry_relative_earnings_yield": 0.90,
            "volume": 0.98,
            "turnover_rate": 0.96,
            "amount": 0.96,
            "market_cap": 0.95,
            "future_20d_return": 0.96,
            "future_60d_return": 0.94,
            "future_120d_return": 0.90,
            "forward_return_60d": 0.94,
        },
        "row_count": 120866,
        "ticker_count": 93,
        "target_horizons": ["5d", "20d", "60d", "120d"],
    }


def _extract_horizons(text: str | None) -> list[str]:
    if not text:
        return []
    return sorted(set(re.findall(r"\d+d", text)))


def _is_future_like(field: str) -> bool:
    return bool(FUTURE_FIELD_RE.search(field))


def review_candidate_data_feasibility(
    candidate: dict[str, Any],
    data_catalog: dict[str, Any],
    *,
    min_coverage: float = 0.60,
    min_rows: int = 250,
    min_tickers: int = 50,
) -> dict[str, Any]:
    fields = list(candidate.get("observable_variables") or [])
    available = set(data_catalog.get("available_fields") or [])
    pit_fields = set(data_catalog.get("pit_fields") or [])
    coverage = data_catalog.get("coverage_by_field") or {}

    missing_fields = [field for field in fields if field not in available]
    leakage_risk_fields = [field for field in fields if _is_future_like(field)]
    low_coverage_fields = [field for field in fields if field in available and float(coverage.get(field, 0.0)) < min_coverage]
    pit_required_fields = [field for field in fields if field in FINANCIAL_FIELD_HINTS]
    pit_unaligned_fields = [field for field in pit_required_fields if field not in pit_fields]

    target_horizons = set(data_catalog.get("target_horizons") or [])
    requested_horizons = _extract_horizons(candidate.get("prediction_target")) or _extract_horizons(candidate.get("expected_horizon"))
    missing_target_horizons = [h for h in requested_horizons if h not in target_horizons]

    sample_blockers = []
    if int(data_catalog.get("row_count") or 0) < min_rows:
        sample_blockers.append("row_count_below_minimum")
    if int(data_catalog.get("ticker_count") or 0) < min_tickers:
        sample_blockers.append("ticker_count_below_minimum")

    if missing_fields:
        decision = "blocked_missing_data"
    elif leakage_risk_fields:
        decision = "blocked_leakage_risk"
    elif low_coverage_fields:
        decision = "blocked_low_coverage"
    elif pit_unaligned_fields:
        decision = "blocked_pit_alignment_required"
    elif missing_target_horizons:
        decision = "blocked_missing_target_horizon"
    elif sample_blockers:
        decision = "blocked_insufficient_sample"
    else:
        decision = "ready_for_minimal_verification"

    return {
        "phenomenon_id": candidate.get("phenomenon_id"),
        "title": candidate.get("title"),
        "decision": decision,
        "observable_variables": fields,
        "missing_fields": missing_fields,
        "low_coverage_fields": low_coverage_fields,
        "leakage_risk_fields": leakage_risk_fields,
        "pit_required_fields": pit_required_fields,
        "pit_unaligned_fields": pit_unaligned_fields,
        "requested_target_horizons": requested_horizons,
        "missing_target_horizons": missing_target_horizons,
        "sample_blockers": sample_blockers,
        "field_coverage": {field: coverage.get(field) for field in fields if field in coverage},
        "row_count": data_catalog.get("row_count"),
        "ticker_count": data_catalog.get("ticker_count"),
    }


def _kept_ids(novelty_review: dict[str, Any]) -> set[str]:
    return {item.get("phenomenon_id") for item in novelty_review.get("reviewed_phenomena") or [] if item.get("decision") == "keep"}


def build_data_feasibility_review(
    *,
    run_id: str,
    candidates_report: dict[str, Any],
    novelty_review: dict[str, Any],
    data_catalog: dict[str, Any] | None = None,
) -> dict[str, Any]:
    catalog = data_catalog or default_data_catalog()
    keep_ids = _kept_ids(novelty_review)
    candidates = [item for item in candidates_report.get("phenomena") or [] if item.get("phenomenon_id") in keep_ids]
    reviewed = [review_candidate_data_feasibility(item, catalog) for item in candidates]
    summary = Counter(item["decision"] for item in reviewed)
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "data_feasibility_artifact_only",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_candidates_run_id": candidates_report.get("run_id"),
        "source_novelty_run_id": novelty_review.get("run_id"),
        "data_catalog_summary": {
            "available_field_count": len(catalog.get("available_fields") or []),
            "row_count": catalog.get("row_count"),
            "ticker_count": catalog.get("ticker_count"),
            "target_horizons": catalog.get("target_horizons") or [],
        },
        "reviewed_phenomena": reviewed,
        "summary": dict(sorted(summary.items())),
        **SAFETY_FLAGS,
    }


def update_data_requests(existing: dict[str, Any], review: dict[str, Any]) -> dict[str, Any]:
    requests = list(existing.get("requests") or [])
    by_id = {item.get("phenomenon_id"): dict(item) for item in requests}
    for item in review.get("reviewed_phenomena") or []:
        missing = item.get("missing_fields") or []
        low = item.get("low_coverage_fields") or []
        pit = item.get("pit_unaligned_fields") or []
        if not (missing or low or pit):
            continue
        by_id[item.get("phenomenon_id")] = {
            "phenomenon_id": item.get("phenomenon_id"),
            "title": item.get("title"),
            "decision": item.get("decision"),
            "missing_fields": missing,
            "low_coverage_fields": low,
            "pit_unaligned_fields": pit,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
    return {"schema_version": 1, "updated_at_utc": datetime.now(timezone.utc).isoformat(), "requests": list(by_id.values())}


def data_feasibility_to_markdown(review: dict[str, Any]) -> str:
    lines = [
        "# Market Phenomenon Data Feasibility",
        "",
        f"run_id: {review.get('run_id')}",
        f"mode: {review.get('mode')}",
        f"strategy_generation_allowed: {review.get('strategy_generation_allowed')}",
        f"backtest_allowed: {review.get('backtest_allowed')}",
        f"queue_write_allowed: {review.get('queue_write_allowed')}",
        "",
        "## Summary",
    ]
    for decision, count in (review.get("summary") or {}).items():
        lines.append(f"- {decision}: {count}")
    lines.append("")
    lines.append("## Reviewed phenomena")
    for item in review.get("reviewed_phenomena") or []:
        lines.extend([
            "",
            f"### {item.get('phenomenon_id')}: {item.get('title')}",
            f"- decision: {item.get('decision')}",
            f"- missing_fields: {', '.join(item.get('missing_fields') or []) or 'none'}",
            f"- low_coverage_fields: {', '.join(item.get('low_coverage_fields') or []) or 'none'}",
            f"- leakage_risk_fields: {', '.join(item.get('leakage_risk_fields') or []) or 'none'}",
            f"- missing_target_horizons: {', '.join(item.get('missing_target_horizons') or []) or 'none'}",
        ])
    return "\n".join(lines).rstrip() + "\n"


def write_data_feasibility_review(review: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "phenomenon_data_feasibility.json"
    markdown_path = out / "phenomenon_data_feasibility.md"
    json_path.write_text(json.dumps(review, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(data_feasibility_to_markdown(review), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
