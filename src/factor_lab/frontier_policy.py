from __future__ import annotations

from collections import defaultdict
from typing import Any


PREFERRED_DECISIONS = {"core_candidate", "validate_now", "dedupe_first"}
SUPPRESSED_DECISIONS = {"drop_from_frontier"}
SECONDARY_DECISIONS = {"regime_sensitive", "watchlist"}


def build_frontier_focus(scorecard_payload: dict[str, Any] | None) -> dict[str, Any]:
    scorecard_payload = scorecard_payload or {}
    rows = list(scorecard_payload.get("rows") or [])

    preferred_rows = [row for row in rows if row.get("decision_key") in PREFERRED_DECISIONS]
    secondary_rows = [row for row in rows if row.get("decision_key") in SECONDARY_DECISIONS]
    suppressed_rows = [row for row in rows if row.get("decision_key") in SUPPRESSED_DECISIONS]

    preferred_rows.sort(key=lambda row: (-float(row.get("promotion_score") or 0.0), row.get("factor_name") or ""))
    secondary_rows.sort(key=lambda row: (-float(row.get("promotion_score") or 0.0), row.get("factor_name") or ""))
    suppressed_rows.sort(key=lambda row: (-float(row.get("promotion_score") or 0.0), row.get("factor_name") or ""))

    duplicate_suppressed: set[str] = set()
    preferred_candidates: list[str] = []
    dedupe_candidates: list[str] = []
    for row in preferred_rows:
        factor_name = row.get("factor_name")
        if not factor_name or factor_name in duplicate_suppressed:
            continue
        preferred_candidates.append(factor_name)
        if row.get("decision_key") == "dedupe_first":
            dedupe_candidates.append(factor_name)
        for peer_name in row.get("duplicate_peers") or []:
            if peer_name and peer_name != factor_name:
                duplicate_suppressed.add(peer_name)

    overflow_preferred = preferred_candidates[3:]
    preferred_candidates = preferred_candidates[:3]
    secondary_candidates = [row.get("factor_name") for row in secondary_rows if row.get("factor_name") and row.get("factor_name") not in duplicate_suppressed]
    for name in overflow_preferred:
        if name and name not in secondary_candidates:
            secondary_candidates.append(name)
    suppressed_candidates = [row.get("factor_name") for row in suppressed_rows if row.get("factor_name")]
    for peer_name in sorted(duplicate_suppressed):
        if peer_name not in suppressed_candidates:
            suppressed_candidates.append(peer_name)
    regime_sensitive_candidates = [row.get("factor_name") for row in secondary_rows if row.get("decision_key") == "regime_sensitive" and row.get("factor_name") and row.get("factor_name") not in duplicate_suppressed]

    family_scores: dict[str, list[float]] = defaultdict(list)
    family_candidates: dict[str, list[str]] = defaultdict(list)
    for row in preferred_rows + secondary_rows:
        family = row.get("family") or "other"
        family_scores[family].append(float(row.get("promotion_score") or 0.0))
        if row.get("factor_name"):
            family_candidates[family].append(row["factor_name"])

    preferred_families = sorted(
        family_scores,
        key=lambda family: (
            -(max(family_scores.get(family) or [0.0])),
            -(sum(family_scores.get(family) or [0.0]) / max(len(family_scores.get(family) or []), 1)),
            family,
        ),
    )

    family_summary = [
        {
            "family": family,
            "top_score": round(max(scores), 6),
            "avg_score": round(sum(scores) / max(len(scores), 1), 6),
            "candidate_count": len(family_candidates.get(family) or []),
            "candidates": family_candidates.get(family) or [],
        }
        for family, scores in family_scores.items()
    ]
    family_summary.sort(key=lambda row: (-float(row.get("top_score") or 0.0), row.get("family") or ""))

    return {
        "preferred_candidates": preferred_candidates,
        "secondary_candidates": secondary_candidates,
        "suppressed_candidates": suppressed_candidates,
        "dedupe_candidates": dedupe_candidates,
        "regime_sensitive_candidates": regime_sensitive_candidates,
        "preferred_families": preferred_families,
        "family_summary": family_summary,
        "priority_rows": (scorecard_payload.get("summary") or {}).get("priority_rows") or [],
    }
