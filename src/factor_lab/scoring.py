from __future__ import annotations

from typing import Dict, List

from factor_lab.analytics import summarize_rolling_windows


def score_factors(
    raw_results: List[Dict],
    neutralized_results: List[Dict],
    split_results: List[Dict],
    rolling_results: List[Dict],
    correlation_lookup: Dict[str, List[str]],
    metadata_lookup: Dict[str, Dict] | None = None,
) -> List[Dict]:
    metadata_lookup = metadata_lookup or {}
    neutral_map = {row["factor_name"]: row for row in neutralized_results}
    split_map: Dict[str, List[Dict]] = {}
    for row in split_results:
        split_map.setdefault(row["factor_name"], []).append(row)
    rolling_map: Dict[str, List[Dict]] = {}
    for row in rolling_results:
        rolling_map.setdefault(row["factor_name"], []).append(row)

    scored = []
    for row in raw_results:
        name = row["factor_name"]
        neutral = neutral_map.get(name, {})
        splits = split_map.get(name, [])
        rolling = rolling_map.get(name, [])
        rolling_summary = summarize_rolling_windows(rolling)
        metadata = metadata_lookup.get(name) or {}
        role = metadata.get("role") or "alpha_seed"

        raw_ic = float(row.get("rank_ic_mean", 0.0) or 0.0)
        raw_ir = float(row.get("rank_ic_ir", 0.0) or 0.0)
        neutral_ic = float(neutral.get("rank_ic_mean", 0.0) or 0.0)
        split_fail_count = sum(1 for item in splits if not item["pass_gate"])
        corr_peer_count = len(correlation_lookup.get(name, []))
        rolling_stability = float(rolling_summary.get("stability_score") or 0.0)

        raw_score = (raw_ic * 8.0) + (raw_ir * 0.8) + (0.35 if row.get("pass_gate") else -0.35)
        neutral_score = (neutral_ic * 8.5) + (0.35 if neutral.get("pass_gate") else -0.25 if neutral else 0.0)
        rolling_score = (float(rolling_summary.get("avg_rank_ic_mean") or 0.0) * 7.5) + (rolling_stability * 1.8) + (0.25 if rolling_summary.get("pass_gate") else -0.2 if rolling else 0.0)

        turnover_penalty = split_fail_count * 0.12
        correlation_penalty = corr_peer_count * 0.1
        style_exposure_penalty = 0.35 if role == "exposure_probe" else 0.1 if role == "family_probe" else 0.0
        sign_flip_penalty = (rolling_summary.get("sign_flip_count") or 0) * 0.05

        composite_score = (
            raw_score
            + neutral_score
            + rolling_score
            - turnover_penalty
            - correlation_penalty
            - style_exposure_penalty
            - sign_flip_penalty
        )

        scored.append(
            {
                "factor_name": name,
                "expression": row["expression"],
                "factor_role": role,
                "score": round(composite_score, 6),
                "composite_score": round(composite_score, 6),
                "raw_score": round(raw_score, 6),
                "neutral_score": round(neutral_score, 6),
                "rolling_score": round(rolling_score, 6),
                "turnover_penalty": round(turnover_penalty, 6),
                "correlation_penalty": round(correlation_penalty, 6),
                "style_exposure_penalty": round(style_exposure_penalty, 6),
                "rolling_sign_flip_penalty": round(sign_flip_penalty, 6),
                "raw_rank_ic_mean": row["rank_ic_mean"],
                "raw_rank_ic_ir": row["rank_ic_ir"],
                "neutralized_rank_ic_mean": neutral.get("rank_ic_mean"),
                "neutralized_pass": neutral.get("pass_gate"),
                "split_fail_count": split_fail_count,
                "rolling_window_count": rolling_summary.get("window_count", 0),
                "rolling_pass_count": rolling_summary.get("pass_count", 0),
                "rolling_fail_count": rolling_summary.get("fail_count", 0),
                "rolling_pass_rate": rolling_summary.get("pass_rate"),
                "rolling_pass": rolling_summary.get("pass_gate"),
                "rolling_sign_flip_count": rolling_summary.get("sign_flip_count", 0),
                "rolling_avg_rank_ic_mean": rolling_summary.get("avg_rank_ic_mean"),
                "rolling_stability_score": rolling_summary.get("stability_score"),
                "high_corr_peers": correlation_lookup.get(name, []),
            }
        )
    return sorted(scored, key=lambda item: item["score"], reverse=True)
