from __future__ import annotations

from typing import Any


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def build_reviewer_response(context: dict[str, Any], source_label: str = "heuristic") -> dict[str, Any]:
    inputs = context.get("inputs") or {}
    rows = ((inputs.get("promotion_scorecard") or {}).get("rows") or [])[:30]
    reviews = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = row.get("factor_name") or row.get("candidate_name") or "unknown"
        duplicate_count = int(row.get("duplicate_peer_count") or 0)
        classification = str(row.get("quality_classification") or row.get("quality_promotion_decision") or "")
        scores = row.get("quality_scores") or {}
        incremental = _as_float(scores.get("incremental_value") or row.get("incremental_value") or 0)
        concerns: list[str] = []
        if row.get("split_fail_count"):
            concerns.append("split_failures")
        if row.get("high_corr_peer_count"):
            concerns.append("high_correlation_peers")
        if duplicate_count > 0 or "duplicate" in classification:
            verdict = "suppress"
            assessment = "duplicate_like"
        elif incremental >= 15:
            verdict = "promote"
            assessment = "strong"
        elif incremental >= 8:
            verdict = "keep_validating"
            assessment = "medium"
        elif incremental > 0:
            verdict = "deprioritize"
            assessment = "weak"
        else:
            verdict = "diagnose"
            assessment = "unknown"
        reviews.append({
            "candidate_name": name,
            "quality_verdict": verdict,
            "incremental_value_assessment": assessment,
            "robustness_concerns": concerns,
            "evidence": [f"quality_classification={classification}", f"incremental_value={incremental}"],
            "recommended_action": verdict,
            "confidence_score": 0.55,
        })
    return {
        "schema_version": "factor_lab.reviewer_agent_response.v1",
        "agent_name": "reviewer-local",
        "decision_source": source_label,
        "decision_context_id": context.get("context_id"),
        "candidate_reviews": reviews,
        "portfolio_level_notes": [],
        "summary_markdown": f"- reviewed {len(reviews)} candidates",
    }
