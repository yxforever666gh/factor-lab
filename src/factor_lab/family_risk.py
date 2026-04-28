from __future__ import annotations

from typing import Any

from factor_lab.research_trials import family_trial_recommended_action


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def build_family_risk_profile(family_row: dict[str, Any], trial_summary: dict[str, Any] | None = None) -> dict[str, Any]:
    trial_summary = trial_summary or {}
    family_score = float(family_row.get("family_score") or 0.0)
    duplicate_pressure = float(family_row.get("duplicate_pressure") or 0.0)
    cluster_pressure = float(family_row.get("cluster_pressure") or 0.0)
    trial_pressure = float(trial_summary.get("trial_pressure") or 0.0)
    false_positive_pressure = float(trial_summary.get("false_positive_pressure") or 0.0)
    risk_score = _clip((100.0 - min(family_score, 100.0)) * 0.45 + duplicate_pressure * 8.0 + cluster_pressure * 8.0 + trial_pressure * 0.22 + false_positive_pressure * 0.25, 0.0, 100.0)
    recommended_action = family_row.get("recommended_action") or family_trial_recommended_action(trial_summary)
    if int(trial_summary.get("trial_count") or 0) > 0:
        recommended_action = family_trial_recommended_action(trial_summary)
    if risk_score < 35 and family_score >= 70 and recommended_action != 'pause':
        recommended_action = 'continue'
    elif risk_score >= 60:
        recommended_action = 'validate_risk'
    elif risk_score >= 45 and recommended_action == 'continue':
        recommended_action = 'refine'
    return {
        **family_row,
        "family_risk_profile": {
            "trial_count": int(trial_summary.get("trial_count") or 0),
            "informative_trial_count": int(trial_summary.get("informative_trial_count") or 0),
            "no_gain_trial_count": int(trial_summary.get("no_gain_trial_count") or 0),
            "failed_trial_count": int(trial_summary.get("failed_trial_count") or 0),
            "knowledge_gain_per_trial": trial_summary.get("knowledge_gain_per_trial"),
            "trial_pressure": round(trial_pressure, 6),
            "false_positive_pressure": round(false_positive_pressure, 6),
            "risk_score": round(risk_score, 6),
            "recommended_action": recommended_action,
        },
        "trial_count": int(trial_summary.get("trial_count") or 0),
        "informative_trial_count": int(trial_summary.get("informative_trial_count") or 0),
        "no_gain_trial_count": int(trial_summary.get("no_gain_trial_count") or 0),
        "failed_trial_count": int(trial_summary.get("failed_trial_count") or 0),
        "knowledge_gain_per_trial": trial_summary.get("knowledge_gain_per_trial"),
        "trial_pressure": round(trial_pressure, 6),
        "false_positive_pressure": round(false_positive_pressure, 6),
        "family_risk_score": round(risk_score, 6),
        "recommended_action": recommended_action,
    }


def build_family_risk_profiles(families: list[dict[str, Any]], trial_summary_by_family: dict[str, dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    trial_summary_by_family = trial_summary_by_family or {}
    rows = [build_family_risk_profile(row, trial_summary_by_family.get(row.get("family") or "other")) for row in families]
    rows.sort(key=lambda row: (-float(row.get("family_risk_score") or 0.0), -(float(row.get("family_score") or 0.0)), row.get("family") or ""))
    return rows
