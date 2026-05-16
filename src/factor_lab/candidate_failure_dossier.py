from __future__ import annotations

import re
from collections import defaultdict
from typing import Any


_WINDOW_DAYS_RE = re.compile(r"(\d+)d")
_PARENT_RELATIONSHIP_TYPES = {"refinement_of", "duplicate_of", "hybrid_of"}


def _window_days(window_label: str | None) -> int | None:
    if not window_label:
        return None
    text = str(window_label).lower()
    match = _WINDOW_DAYS_RE.search(text)
    if match:
        try:
            return int(match.group(1))
        except Exception:
            return None
    if "expanding" in text:
        return 999
    return None


def _window_bucket(days: int | None) -> str:
    if days is None:
        return "unknown"
    if days <= 45:
        return "short"
    if days <= 90:
        return "medium"
    return "long"


def _retention(row: dict[str, Any]) -> float | None:
    raw_ic = row.get("raw_rank_ic_mean")
    neutral_ic = row.get("neutralized_rank_ic_mean")
    if raw_ic in {None, 0}:
        return None
    try:
        return float(neutral_ic or 0.0) / float(raw_ic)
    except Exception:
        return None


def _avg(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _relationship_map(relationships: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in relationships:
        left_name = row.get("left_name")
        right_name = row.get("right_name")
        if left_name:
            by_name[left_name].append(row)
        if right_name and right_name != left_name:
            by_name[right_name].append(row)
    return by_name


def _parent_names(candidate_name: str, rows: list[dict[str, Any]]) -> list[str]:
    parent_names: list[str] = []
    for row in rows:
        rel_type = row.get("relationship_type")
        if rel_type not in _PARENT_RELATIONSHIP_TYPES:
            continue
        details = row.get("details") or {}
        for key in ("parent_candidate", "left_candidate", "right_candidate"):
            value = details.get(key)
            if value and value != candidate_name and value not in parent_names:
                parent_names.append(value)
        for value in (row.get("left_name"), row.get("right_name")):
            if value and value != candidate_name and value not in parent_names:
                parent_names.append(value)
    return parent_names[:4]


def _window_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(
        rows,
        key=lambda row: (
            _window_days(row.get("window_label")) or 0,
            row.get("created_at_utc") or "",
        ),
    )
    summary: list[dict[str, Any]] = []
    for row in ordered:
        days = _window_days(row.get("window_label"))
        summary.append(
            {
                "window_label": row.get("window_label"),
                "days": days,
                "bucket": _window_bucket(days),
                "final_score": row.get("final_score"),
                "status": row.get("status"),
                "raw_rank_ic_mean": row.get("raw_rank_ic_mean"),
                "neutralized_rank_ic_mean": row.get("neutralized_rank_ic_mean"),
                "retention": _retention(row),
                "split_fail_count": row.get("split_fail_count"),
                "created_at_utc": row.get("created_at_utc"),
            }
        )
    return summary


def build_candidate_failure_dossier(
    candidate: dict[str, Any],
    evaluations: list[dict[str, Any]],
    relationships: list[dict[str, Any]],
    candidate_by_name: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    candidate_name = candidate.get("name") or candidate.get("factor_name") or ""
    candidate_rows = [row for row in evaluations if row.get("candidate_id") == candidate.get("id")]
    candidate_rows.sort(key=lambda row: row.get("created_at_utc") or "", reverse=True)
    windows = _window_summary(candidate_rows[:24])

    best_by_days: dict[int, dict[str, Any]] = {}
    for row in windows:
        days = row.get("days")
        if days is None:
            continue
        current = best_by_days.get(days)
        if current is None or float(row.get("final_score") or -999.0) > float(current.get("final_score") or -999.0):
            best_by_days[days] = row

    short_row = best_by_days.get(45) or best_by_days.get(30)
    medium_row = best_by_days.get(60) or best_by_days.get(90)
    long_row = best_by_days.get(120) or best_by_days.get(999)

    failure_modes: list[str] = []
    evidence: list[str] = []

    short_score = float((short_row or {}).get("final_score") or 0.0)
    medium_score = float((medium_row or {}).get("final_score") or 0.0)
    long_score = float((long_row or {}).get("final_score") or 0.0)
    if short_row and medium_row and short_score >= 1.0 and medium_score <= 0.25 * short_score:
        failure_modes.append("short_to_medium_decay")
        evidence.append(f"45d/30d -> 60d/90d score decays from {short_score:.3f} to {medium_score:.3f}")
    if medium_row and long_row and medium_score >= 0.75 and long_score <= 0.35 * medium_score:
        failure_modes.append("medium_to_long_decay")
        evidence.append(f"60d/90d -> 120d/expanding score decays from {medium_score:.3f} to {long_score:.3f}")

    retentions = [row.get("retention") for row in windows if row.get("retention") is not None]
    retention_avg = _avg([float(value) for value in retentions if value is not None])
    neutralized_break = False
    if retentions:
        low_retention_count = len([value for value in retentions if value is not None and float(value) < 0.25])
        if low_retention_count >= max(1, len(retentions) // 2):
            neutralized_break = True
    negative_neutralized = len([row for row in windows if float(row.get("neutralized_rank_ic_mean") or 0.0) < 0.0])
    if neutralized_break or (windows and negative_neutralized >= max(1, len(windows) // 2)):
        failure_modes.append("neutralized_break")
        evidence.append(
            f"neutralized retention weak (avg={retention_avg if retention_avg is not None else '-'}, negative_windows={negative_neutralized})"
        )

    related_rows = [row for row in relationships if candidate_name in {row.get("left_name"), row.get("right_name")}]
    parent_names = _parent_names(candidate_name, related_rows)
    parent_scores = []
    for parent_name in parent_names:
        parent = candidate_by_name.get(parent_name) or {}
        parent_scores.append(
            {
                "candidate_name": parent_name,
                "avg_final_score": parent.get("avg_final_score"),
                "latest_recent_final_score": parent.get("latest_recent_final_score"),
            }
        )
    parent_delta_status = "unknown"
    if parent_scores:
        candidate_avg = float(candidate.get("avg_final_score") or 0.0)
        strongest_parent_avg = max(float(row.get("avg_final_score") or 0.0) for row in parent_scores)
        if candidate_avg <= strongest_parent_avg + 0.05:
            parent_delta_status = "non_incremental"
            failure_modes.append("non_incremental_vs_parent")
            evidence.append(f"candidate avg_final_score={candidate_avg:.3f} vs parent best={strongest_parent_avg:.3f}")
        else:
            parent_delta_status = "incremental"

    duplicate_count = len([row for row in related_rows if row.get("relationship_type") in {"duplicate_of", "high_corr"}])
    if duplicate_count >= 2:
        failure_modes.append("crowded_neighbor_cluster")
        evidence.append(f"duplicate/high-corr relationships={duplicate_count}")

    regime_dependency = "unclear"
    if "short_to_medium_decay" in failure_modes or "medium_to_long_decay" in failure_modes:
        regime_dependency = "short_window_only"
    elif "neutralized_break" in failure_modes:
        regime_dependency = "exposure_dependent"
    elif short_row and medium_row and long_row and long_score > 0.0:
        regime_dependency = "cross_window_supported"

    if "non_incremental_vs_parent" in failure_modes and "crowded_neighbor_cluster" in failure_modes:
        recommended_action = "suppress"
    elif any(mode in failure_modes for mode in {"short_to_medium_decay", "medium_to_long_decay", "neutralized_break"}):
        recommended_action = "diagnose"
    elif regime_dependency == "cross_window_supported":
        recommended_action = "promote_validation"
    else:
        recommended_action = "keep_validating"

    return {
        "candidate_name": candidate_name,
        "candidate_id": candidate.get("id"),
        "failure_modes": failure_modes,
        "recommended_action": recommended_action,
        "regime_dependency": regime_dependency,
        "retention_avg": retention_avg,
        "parent_candidates": parent_names,
        "parent_delta_status": parent_delta_status,
        "window_evidence": windows,
        "evidence": evidence,
    }


def build_candidate_failure_dossiers(
    candidates: list[dict[str, Any]],
    evaluations: list[dict[str, Any]],
    relationships: list[dict[str, Any]],
    *,
    focus_names: list[str] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    candidate_by_name = {row.get("name") or row.get("factor_name"): row for row in candidates if row.get("name") or row.get("factor_name")}
    focus_set = set(focus_names or [])
    selected = []
    for candidate in candidates:
        name = candidate.get("name") or candidate.get("factor_name")
        if focus_set and name not in focus_set:
            continue
        selected.append(build_candidate_failure_dossier(candidate, evaluations, relationships, candidate_by_name))
    selected.sort(
        key=lambda row: (
            {"suppress": 0, "diagnose": 1, "keep_validating": 2, "promote_validation": 3}.get(row.get("recommended_action") or "keep_validating", 9),
            row.get("candidate_name") or "",
        )
    )
    if limit is not None:
        return selected[:limit]
    return selected
