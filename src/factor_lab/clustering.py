from __future__ import annotations

from typing import Dict, List
import math

import pandas as pd


def greedy_correlation_clusters(correlation: pd.DataFrame, threshold: float = 0.8) -> List[Dict]:
    remaining = set(correlation.columns)
    clusters: List[Dict] = []

    while remaining:
        seed = sorted(remaining)[0]
        members = []
        for col in sorted(list(remaining)):
            value = correlation.loc[seed, col]
            if pd.notna(value) and abs(float(value)) >= threshold:
                members.append(col)
        for member in members:
            remaining.discard(member)
        clusters.append({"seed": seed, "members": members})

    return clusters


def _cluster_rep_limit(cluster_size: int) -> int:
    if cluster_size <= 2:
        return 1
    return min(4, max(1, math.ceil(cluster_size / 3)))


def pick_cluster_representatives(clusters: List[Dict], scores: List[Dict]) -> List[Dict]:
    score_map = {row["factor_name"]: row for row in scores}
    representatives: List[Dict] = []
    for cluster in clusters:
        ranked = sorted(
            [score_map[name] for name in cluster["members"] if name in score_map],
            key=lambda item: item["score"],
            reverse=True,
        )
        if not ranked:
            continue
        rep_limit = _cluster_rep_limit(len(cluster["members"]))
        score_span = max(float(ranked[0].get("score") or 0.0) - float(ranked[-1].get("score") or 0.0), 0.0)
        keep_threshold = max(0.12, round(score_span * 0.35, 6))
        kept = []
        anchor = float(ranked[0].get("score") or 0.0)
        for item in ranked:
            if len(kept) >= rep_limit:
                break
            score = float(item.get("score") or 0.0)
            if not kept or (anchor - score) <= keep_threshold:
                kept.append(item)
        if not kept:
            kept = ranked[:1]
        for rep_rank, item in enumerate(kept, start=1):
            row = dict(item)
            row["cluster_members"] = cluster["members"]
            row["cluster_rep_rank"] = rep_rank
            row["cluster_rep_count"] = len(kept)
            row["is_primary_representative"] = rep_rank == 1
            representatives.append(row)
    return representatives
