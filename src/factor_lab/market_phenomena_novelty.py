from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.market_phenomena_schema import SAFETY_FLAGS


def _tokens(values: Any) -> set[str]:
    if not values:
        return set()
    if isinstance(values, str):
        raw = [values]
    else:
        raw = list(values)
    tokens: set[str] = set()
    for value in raw:
        for token in str(value).replace("/", " ").replace(",", " ").split():
            token = token.strip().lower()
            if token:
                tokens.add(token)
    return tokens


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 0.0
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)


def compute_similarity(candidate: dict[str, Any], memory_item: dict[str, Any]) -> float:
    mechanism = 1.0 if candidate.get("mechanism_source") == memory_item.get("mechanism_source") else 0.0
    participants = _jaccard(_tokens(candidate.get("participants")), _tokens(memory_item.get("participants")))
    variables = _jaccard(_tokens(candidate.get("observable_variables")), _tokens(memory_item.get("observable_variables")))
    target = 1.0 if candidate.get("prediction_target") == memory_item.get("prediction_target") else 0.0
    return round(0.35 * mechanism + 0.25 * participants + 0.30 * variables + 0.10 * target, 6)


def review_candidate_novelty(candidate: dict[str, Any], memory: dict[str, Any]) -> dict[str, Any]:
    best_score = 0.0
    best_item: dict[str, Any] | None = None
    for item in memory.get("phenomena") or []:
        score = compute_similarity(candidate, item)
        if score > best_score:
            best_score = score
            best_item = item
    if best_score >= 0.70:
        decision = "reject_duplicate"
    elif best_score >= 0.55:
        decision = "revise_too_similar"
    else:
        decision = "keep"
    return {
        "phenomenon_id": candidate.get("phenomenon_id"),
        "title": candidate.get("title"),
        "decision": decision,
        "mechanism_similarity_max": best_score,
        "most_similar_phenomenon_id": best_item.get("phenomenon_id") if best_item else None,
        "most_similar_latest_verdict": best_item.get("latest_verdict") if best_item else None,
        "novelty_score": round(1.0 - best_score, 6),
    }


def _kept_ids(quality_review: dict[str, Any]) -> set[str]:
    return {
        item.get("phenomenon_id")
        for item in quality_review.get("reviewed_phenomena") or []
        if item.get("decision") == "keep"
    }


def build_novelty_review(*, run_id: str, quality_review: dict[str, Any], candidates_report: dict[str, Any], memory: dict[str, Any]) -> dict[str, Any]:
    keep_ids = _kept_ids(quality_review)
    candidates = [item for item in candidates_report.get("phenomena") or [] if item.get("phenomenon_id") in keep_ids]
    reviewed = [review_candidate_novelty(item, memory) for item in candidates]
    summary = Counter(item["decision"] for item in reviewed)
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "novelty_review_artifact_only",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_candidates_run_id": candidates_report.get("run_id"),
        "source_quality_run_id": quality_review.get("run_id"),
        "reviewed_phenomena": reviewed,
        "summary": dict(sorted(summary.items())),
        **SAFETY_FLAGS,
    }


def novelty_review_to_markdown(review: dict[str, Any]) -> str:
    lines = [
        "# Market Phenomenon Novelty Review",
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
            f"- mechanism_similarity_max: {item.get('mechanism_similarity_max')}",
            f"- novelty_score: {item.get('novelty_score')}",
            f"- most_similar_phenomenon_id: {item.get('most_similar_phenomenon_id')}",
        ])
    return "\n".join(lines).rstrip() + "\n"


def write_novelty_review(review: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "phenomenon_novelty_review.json"
    markdown_path = out / "phenomenon_novelty_review.md"
    json_path.write_text(json.dumps(review, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(novelty_review_to_markdown(review), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
