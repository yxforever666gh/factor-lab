from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.market_phenomena_schema import SAFETY_FLAGS, validate_phenomenon


def review_phenomenon_quality(phenomenon: dict[str, Any]) -> dict[str, Any]:
    validation = validate_phenomenon(phenomenon)
    reason_codes = validation["reason_codes"]
    if not reason_codes:
        decision = "keep"
    elif "forbidden_indicator_core_logic" in reason_codes:
        decision = "reject_indicator_disguised_as_mechanism"
    elif "direct_strategy_rule_detected" in reason_codes:
        decision = "reject_strategy_disguised_as_phenomenon"
    elif any(code.startswith("missing_") for code in reason_codes):
        decision = "reject_missing_required_mechanism"
    else:
        decision = "manual_review"
    return {
        "phenomenon_id": phenomenon.get("phenomenon_id"),
        "title": phenomenon.get("title"),
        "decision": decision,
        "reason_codes": reason_codes,
        "missing_fields": validation.get("missing_fields") or [],
        "forbidden_core_logic_terms": validation.get("forbidden_core_logic_terms") or [],
        "direct_strategy_terms": validation.get("direct_strategy_terms") or [],
        "total_score": (phenomenon.get("scores") or {}).get("total_score"),
    }


def build_quality_review(*, run_id: str, candidates_report: dict[str, Any]) -> dict[str, Any]:
    reviewed = [review_phenomenon_quality(item) for item in candidates_report.get("phenomena") or []]
    summary = Counter(item["decision"] for item in reviewed)
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "quality_review_artifact_only",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_run_id": candidates_report.get("run_id"),
        "reviewed_phenomena": reviewed,
        "summary": dict(sorted(summary.items())),
        **SAFETY_FLAGS,
    }


def quality_review_to_markdown(review: dict[str, Any]) -> str:
    lines = [
        "# Market Phenomenon Quality Review",
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
        lines.extend(
            [
                "",
                f"### {item.get('phenomenon_id')}: {item.get('title')}",
                f"- decision: {item.get('decision')}",
                f"- reason_codes: {', '.join(item.get('reason_codes') or []) if item.get('reason_codes') else 'none'}",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def write_quality_review(review: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "phenomenon_quality_review.json"
    markdown_path = out / "phenomenon_quality_review.md"
    json_path.write_text(json.dumps(review, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(quality_review_to_markdown(review), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
