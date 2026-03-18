from __future__ import annotations

from typing import Dict, List


def score_factors(
    raw_results: List[Dict],
    neutralized_results: List[Dict],
    split_results: List[Dict],
    correlation_lookup: Dict[str, List[str]],
) -> List[Dict]:
    neutral_map = {row["factor_name"]: row for row in neutralized_results}
    split_map: Dict[str, List[Dict]] = {}
    for row in split_results:
        split_map.setdefault(row["factor_name"], []).append(row)

    scored = []
    for row in raw_results:
        name = row["factor_name"]
        neutral = neutral_map.get(name, {})
        splits = split_map.get(name, [])
        split_penalty = sum(1 for item in splits if not item["pass_gate"]) * 0.2
        corr_penalty = max(0, len(correlation_lookup.get(name, [])) - 0) * 0.1
        raw_ic = float(row.get("rank_ic_mean", 0.0))
        raw_ir = float(row.get("rank_ic_ir", 0.0))
        neutral_ic = float(neutral.get("rank_ic_mean", 0.0) or 0.0)
        neutral_bonus = 0.3 if neutral.get("pass_gate") else -0.2
        score = (raw_ic * 2.5) + (raw_ir * 0.15) + (neutral_ic * 3.0) + neutral_bonus - split_penalty - corr_penalty
        scored.append(
            {
                "factor_name": name,
                "expression": row["expression"],
                "score": round(score, 6),
                "raw_rank_ic_mean": row["rank_ic_mean"],
                "raw_rank_ic_ir": row["rank_ic_ir"],
                "neutralized_rank_ic_mean": neutral.get("rank_ic_mean"),
                "neutralized_pass": neutral.get("pass_gate"),
                "split_fail_count": sum(1 for item in splits if not item["pass_gate"]),
                "high_corr_peers": correlation_lookup.get(name, []),
            }
        )
    return sorted(scored, key=lambda item: item["score"], reverse=True)
