from __future__ import annotations

from collections import defaultdict
from typing import Any


PREFERRED_DECISIONS = {"core_candidate", "validate_now", "dedupe_first"}
SUPPRESSED_DECISIONS = {"drop_from_frontier"}
SECONDARY_DECISIONS = {"regime_sensitive", "watchlist"}


def _recent_score(row: dict[str, Any]) -> float:
    return float(row.get("latest_recent_final_score") or row.get("latest_final_score") or 0.0)



def _promotion_score(row: dict[str, Any]) -> float:
    return float(row.get("promotion_score") or 0.0)



def _dedupe_ranked_rows(rows: list[dict[str, Any]], limit: int | None = None) -> tuple[list[dict[str, Any]], set[str]]:
    duplicate_suppressed: set[str] = set()
    selected: list[dict[str, Any]] = []
    for row in rows:
        factor_name = row.get("factor_name")
        if not factor_name or factor_name in duplicate_suppressed:
            continue
        selected.append(row)
        for peer_name in row.get("duplicate_peers") or []:
            if peer_name and peer_name != factor_name:
                duplicate_suppressed.add(peer_name)
        if limit is not None and len(selected) >= limit:
            break
    return selected, duplicate_suppressed



def _is_robust_frontier_candidate(row: dict[str, Any]) -> bool:
    return (
        int(row.get("window_count") or 0) >= 4
        and float(row.get("pass_rate") or 0.0) >= 0.2
        and float(row.get("avg_final_score") or 0.0) >= 0.75
        and float(row.get("robustness_score") or 0.0) >= 0.72
        and float(row.get("risk_score") or 100.0) < 80.0
        and int(row.get("split_fail_count") or 0) <= 1
    )



def _is_soft_robust_frontier_candidate(row: dict[str, Any]) -> bool:
    return (
        int(row.get("window_count") or 0) >= 2
        and float(row.get("pass_rate") or 0.0) >= 0.2
        and float(row.get("avg_final_score") or 0.0) >= 1.0
        and float(row.get("robustness_score") or 0.0) >= 0.75
        and float(row.get("risk_score") or 100.0) < 90.0
        and int(row.get("split_fail_count") or 0) <= 1
    )



def _family_summary(rows: list[dict[str, Any]], sort_key: str) -> tuple[list[str], list[dict[str, Any]]]:
    family_scores: dict[str, list[float]] = defaultdict(list)
    family_candidates: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        family = row.get("family") or "other"
        family_scores[family].append(float(row.get(sort_key) or 0.0))
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
    return preferred_families, family_summary



def build_frontier_focus(scorecard_payload: dict[str, Any] | None) -> dict[str, Any]:
    scorecard_payload = scorecard_payload or {}
    rows = list(scorecard_payload.get("rows") or [])

    preferred_rows = [row for row in rows if row.get("decision_key") in PREFERRED_DECISIONS]
    secondary_rows = [row for row in rows if row.get("decision_key") in SECONDARY_DECISIONS]
    suppressed_rows = [row for row in rows if row.get("decision_key") in SUPPRESSED_DECISIONS]

    preferred_rows.sort(key=lambda row: (-_recent_score(row), -_promotion_score(row), row.get("factor_name") or ""))
    secondary_rows.sort(key=lambda row: (-_recent_score(row), -_promotion_score(row), row.get("factor_name") or ""))
    suppressed_rows.sort(key=lambda row: (-_promotion_score(row), row.get("factor_name") or ""))

    short_window_rows, short_duplicate_suppressed = _dedupe_ranked_rows(preferred_rows, limit=3)
    short_window_candidates = [row.get("factor_name") for row in short_window_rows if row.get("factor_name")]
    dedupe_candidates = [row.get("factor_name") for row in short_window_rows if row.get("decision_key") == "dedupe_first" and row.get("factor_name")]

    robust_input_rows = sorted(
        [row for row in preferred_rows + secondary_rows if _is_robust_frontier_candidate(row)],
        key=lambda row: (
            -float(row.get("avg_final_score") or 0.0),
            -float(row.get("pass_rate") or 0.0),
            -float(row.get("robustness_score") or 0.0),
            -_recent_score(row),
            row.get("factor_name") or "",
        ),
    )
    robust_rows, robust_duplicate_suppressed = _dedupe_ranked_rows(robust_input_rows, limit=3)
    robust_candidates = [row.get("factor_name") for row in robust_rows if row.get("factor_name")]

    soft_robust_input_rows = sorted(
        [
            row for row in preferred_rows + secondary_rows
            if _is_soft_robust_frontier_candidate(row)
            and row.get("factor_name") not in set(robust_candidates)
        ],
        key=lambda row: (
            -float(row.get("robustness_score") or 0.0),
            -float(row.get("avg_final_score") or 0.0),
            -float(row.get("pass_rate") or 0.0),
            -_recent_score(row),
            row.get("factor_name") or "",
        ),
    )
    soft_robust_rows, soft_robust_duplicate_suppressed = _dedupe_ranked_rows(soft_robust_input_rows, limit=3)
    soft_robust_candidates = [row.get("factor_name") for row in soft_robust_rows if row.get("factor_name")]

    secondary_candidates = [
        row.get("factor_name")
        for row in secondary_rows
        if row.get("factor_name")
        and row.get("factor_name") not in short_duplicate_suppressed
    ]
    suppressed_candidates = [row.get("factor_name") for row in suppressed_rows if row.get("factor_name")]
    for peer_name in sorted(short_duplicate_suppressed | robust_duplicate_suppressed | soft_robust_duplicate_suppressed):
        if peer_name not in suppressed_candidates:
            suppressed_candidates.append(peer_name)

    regime_sensitive_candidates = [
        row.get("factor_name")
        for row in secondary_rows
        if row.get("decision_key") == "regime_sensitive"
        and row.get("factor_name")
        and row.get("factor_name") not in short_duplicate_suppressed
    ]

    preferred_families, family_summary = _family_summary(preferred_rows + secondary_rows, "promotion_score")
    robust_families, robust_family_summary = _family_summary(robust_rows, "avg_final_score")
    soft_robust_families, soft_robust_family_summary = _family_summary(soft_robust_rows, "avg_final_score")

    priority_rows = [
        {
            "factor_name": row.get("factor_name"),
            "decision_label": row.get("decision_label"),
            "decision_summary": row.get("decision_summary"),
        }
        for row in short_window_rows
    ]
    robust_priority_rows = [
        {
            "factor_name": row.get("factor_name"),
            "decision_label": row.get("decision_label"),
            "decision_summary": row.get("decision_summary"),
        }
        for row in robust_rows
    ]
    soft_robust_priority_rows = [
        {
            "factor_name": row.get("factor_name"),
            "decision_label": row.get("decision_label"),
            "decision_summary": row.get("decision_summary"),
        }
        for row in soft_robust_rows
    ]

    return {
        "preferred_candidates": short_window_candidates,
        "short_window_candidates": short_window_candidates,
        "robust_candidates": robust_candidates,
        "soft_robust_candidates": soft_robust_candidates,
        "secondary_candidates": secondary_candidates,
        "suppressed_candidates": suppressed_candidates,
        "dedupe_candidates": dedupe_candidates,
        "regime_sensitive_candidates": regime_sensitive_candidates,
        "preferred_families": preferred_families,
        "short_window_families": preferred_families,
        "robust_families": robust_families,
        "soft_robust_families": soft_robust_families,
        "family_summary": family_summary,
        "robust_family_summary": robust_family_summary,
        "soft_robust_family_summary": soft_robust_family_summary,
        "priority_rows": priority_rows,
        "robust_priority_rows": robust_priority_rows,
        "soft_robust_priority_rows": soft_robust_priority_rows,
        "summary": (scorecard_payload.get("summary") or {}),
    }
