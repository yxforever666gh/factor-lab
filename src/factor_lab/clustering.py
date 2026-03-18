from __future__ import annotations

from typing import Dict, List

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
        top = dict(ranked[0])
        top["cluster_members"] = cluster["members"]
        representatives.append(top)
    return representatives
