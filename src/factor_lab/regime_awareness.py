from __future__ import annotations

from typing import Any


QUESTION_TYPES = ("confirm", "diagnose", "expand", "recombine", "probe")


_DEFAULT_WEIGHTS = {
    "confirm": 1.0,
    "diagnose": 1.0,
    "expand": 1.0,
    "recombine": 1.0,
    "probe": 1.0,
}


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def build_regime_context(snapshot: dict[str, Any]) -> dict[str, Any]:
    flow_state = snapshot.get("research_flow_state") or {}
    frontier_focus = snapshot.get("frontier_focus") or {}
    summary = frontier_focus.get("summary") or {}
    queue_budget = snapshot.get("queue_budget") or {}
    exploration_state = snapshot.get("exploration_state") or {}
    relationship_summary = snapshot.get("relationship_summary") or {}
    knowledge_gain_counter = snapshot.get("knowledge_gain_counter") or {}

    robust_count = len(frontier_focus.get("robust_candidates") or [])
    soft_robust_count = len(frontier_focus.get("soft_robust_candidates") or [])
    preferred_count = len(frontier_focus.get("preferred_candidates") or [])
    regime_sensitive_count = len(frontier_focus.get("regime_sensitive_candidates") or []) or int(summary.get("regime_sensitive_count") or 0) or int(summary.get("quality_regime_sensitive_count") or 0)
    watchlist_count = int(summary.get("watchlist_count") or 0)
    dedupe_count = len(frontier_focus.get("dedupe_candidates") or []) or int(summary.get("dedupe_first_count") or 0)
    duplicate_suppress_count = int(summary.get("duplicate_suppress_count") or 0)
    drop_count = int(summary.get("drop_count") or 0) or int(summary.get("quality_drop_count") or 0)
    duplicate_pressure = int(relationship_summary.get("duplicate_of") or 0) + duplicate_suppress_count
    no_gain_count = int(knowledge_gain_counter.get("no_significant_information_gain") or 0)

    weights = dict(_DEFAULT_WEIGHTS)
    reasons: list[str] = []
    regime = "neutral"

    flow_name = str(flow_state.get("state") or "")
    if flow_name == "recovering":
        regime = "validation_hardening"
        weights.update({"confirm": 1.35, "diagnose": 1.3, "expand": 0.72, "recombine": 0.84, "probe": 0.78})
        reasons.append("flow_state=recovering")
    elif flow_name == "recovered":
        regime = "expansion_ready"
        weights.update({"confirm": 0.95, "diagnose": 1.0, "expand": 1.22, "recombine": 1.18, "probe": 1.08})
        reasons.append("flow_state=recovered")

    if preferred_count > 0 and robust_count == 0 and soft_robust_count == 0:
        regime = "fragile_frontier"
        weights["confirm"] *= 1.18
        weights["diagnose"] *= 1.2
        weights["expand"] *= 0.8
        reasons.append("preferred_without_robust_support")

    if regime_sensitive_count >= max(2, preferred_count):
        regime = "regime_sensitive_frontier"
        weights["probe"] *= 1.28
        weights["diagnose"] *= 1.16
        weights["confirm"] *= 0.9
        weights["expand"] *= 0.82
        reasons.append("regime_sensitive_pressure")

    if dedupe_count >= 3 or duplicate_pressure >= 12:
        regime = "crowded_frontier"
        weights["diagnose"] *= 1.18
        weights["recombine"] *= 1.12
        weights["expand"] *= 0.88
        reasons.append("duplicate_pressure")

    if bool(exploration_state.get("should_throttle")) or int(queue_budget.get("exploration") or 0) <= 0:
        weights["probe"] *= 0.82
        weights["recombine"] *= 0.92
        reasons.append("exploration_throttled")

    if robust_count >= 2 and drop_count <= max(2, robust_count) and no_gain_count <= 1:
        regime = "expansion_ready"
        weights["expand"] *= 1.1
        weights["recombine"] *= 1.08
        weights["probe"] *= 1.05
        reasons.append("robust_frontier_present")

    confidence = 0.25
    confidence += min(0.2, robust_count * 0.08)
    confidence += min(0.15, soft_robust_count * 0.05)
    confidence += min(0.12, regime_sensitive_count * 0.04)
    confidence += 0.08 if flow_name in {"recovering", "recovered"} else 0.0
    confidence += 0.06 if dedupe_count >= 2 or duplicate_pressure >= 8 else 0.0
    if not reasons:
        reasons.append("no_strong_regime_signal")

    return {
        "regime": regime,
        "confidence": round(_clamp(confidence, 0.05, 0.95), 3),
        "weights": {key: round(max(0.5, min(1.6, value)), 3) for key, value in weights.items()},
        "signals": {
            "robust_count": robust_count,
            "soft_robust_count": soft_robust_count,
            "preferred_count": preferred_count,
            "regime_sensitive_count": regime_sensitive_count,
            "watchlist_count": watchlist_count,
            "dedupe_count": dedupe_count,
            "duplicate_pressure": duplicate_pressure,
            "drop_count": drop_count,
            "no_gain_count": no_gain_count,
        },
        "reasons": reasons,
    }
