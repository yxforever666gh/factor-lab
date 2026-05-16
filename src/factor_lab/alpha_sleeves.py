from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable

SLEEVE_BY_FAMILY = {
    "value": "value_sleeve",
    "quality": "quality_sleeve",
    "momentum": "momentum_sleeve",
    "reversal": "reversal_sleeve",
    "liquidity": "liquidity_sleeve",
    "risk": "risk_control_sleeve",
}


def assign_sleeve(candidate: dict[str, Any]) -> str:
    text = " ".join(
        str(candidate.get(key, ""))
        for key in ("mechanism_id", "family", "name", "expression", "target_family")
    ).lower()
    if "value" in text or "valuation" in text or "book_yield" in text or "earnings_yield" in text:
        return "value_sleeve"
    if "quality" in text or "roe" in text:
        return "quality_sleeve"
    if "momentum" in text or "mom" in text:
        return "momentum_sleeve"
    if "reversal" in text:
        return "reversal_sleeve"
    if "liquidity" in text or "turnover" in text:
        return "liquidity_sleeve"
    if "risk" in text or "volatility" in text or "drawdown" in text:
        return "risk_control_sleeve"
    family = str(candidate.get("family") or "").lower()
    return SLEEVE_BY_FAMILY.get(family, "exploration_sleeve")


def build_sleeve_summary(candidates: Iterable[dict[str, Any]]) -> dict[str, Any]:
    sleeves: dict[str, dict[str, Any]] = defaultdict(lambda: {"candidate_count": 0, "total_weight": 0.0, "candidates": [], "duplicate_cluster_count": 0, "recommended_action": "ok"})
    clusters_by_sleeve: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for candidate in candidates:
        sleeve = assign_sleeve(candidate)
        row = sleeves[sleeve]
        row["candidate_count"] += 1
        row["total_weight"] += float(candidate.get("allocated_weight") or candidate.get("weight") or 0.0)
        row["candidates"].append(candidate.get("name") or candidate.get("factor_name") or candidate.get("mechanism_id"))
        cluster = candidate.get("correlation_cluster")
        if cluster:
            clusters_by_sleeve[sleeve][str(cluster)] += 1
    for sleeve, clusters in clusters_by_sleeve.items():
        duplicate_clusters = sum(1 for count in clusters.values() if count > 1)
        sleeves[sleeve]["duplicate_cluster_count"] = duplicate_clusters
        if duplicate_clusters:
            sleeves[sleeve]["recommended_action"] = "cap_duplicate_cluster_weight"
    return {"schema_version": 1, "sleeves": dict(sleeves)}
