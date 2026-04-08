from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from factor_lab.analytics import summarize_rolling_windows


class FactorRegistry:
    def __init__(self, output_dir: str | Path) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def build_candidate_and_graveyard(
        self,
        raw_results: List[Dict],
        neutralized_results: List[Dict],
        split_results: List[Dict],
        rolling_results: List[Dict],
        correlation_lookup: Dict[str, List[str]],
        metadata_lookup: Dict[str, Dict] | None = None,
        score_lookup: Dict[str, Dict] | None = None,
    ) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
        metadata_lookup = metadata_lookup or {}
        score_lookup = score_lookup or {}
        neutral_map = {row["factor_name"]: row for row in neutralized_results}
        split_map: Dict[str, List[Dict]] = {}
        for row in split_results:
            split_map.setdefault(row["factor_name"], []).append(row)
        rolling_map: Dict[str, List[Dict]] = {}
        for row in rolling_results:
            rolling_map.setdefault(row["factor_name"], []).append(row)

        explore = []
        watchlist = []
        candidates = []
        graveyard = []

        for row in raw_results:
            name = row["factor_name"]
            neutral = neutral_map.get(name)
            splits = split_map.get(name, [])
            rolling_summary = summarize_rolling_windows(rolling_map.get(name, []))
            split_fail_count = sum(1 for item in splits if not item["pass_gate"])
            high_corr = correlation_lookup.get(name, [])
            metadata = metadata_lookup.get(name) or {}
            role = metadata.get("role") or "alpha_seed"
            score_row = score_lookup.get(name) or {}

            raw_pass = bool(row["pass_gate"])
            neutral_pass = bool(neutral["pass_gate"]) if neutral is not None else True
            rolling_pass = bool(rolling_summary.get("pass_gate")) if rolling_summary.get("window_count") else False
            raw_score = float(score_row.get("raw_score")) if "raw_score" in score_row else (1.0 if raw_pass else -1.0)
            neutral_score = float(score_row.get("neutral_score")) if "neutral_score" in score_row else (1.0 if neutral_pass else -1.0)
            rolling_score = float(score_row.get("rolling_score")) if "rolling_score" in score_row else (1.0 if rolling_pass else -1.0)
            turnover_penalty = float(score_row.get("turnover_penalty") or 0.0)
            correlation_penalty = float(score_row.get("correlation_penalty") or 0.0)
            style_exposure_penalty = float(score_row.get("style_exposure_penalty") or 0.0)

            watchlist_ready = raw_score >= 0 and (neutral_score >= 0 or rolling_score >= 0)
            candidate_ready = (
                raw_score >= 0
                and neutral_score >= 0
                and rolling_score >= 0
                and split_fail_count == 0
                and len(high_corr) <= 1
                and role != "exposure_probe"
            )

            blocking_reasons = []
            if not raw_pass:
                blocking_reasons.append(f"raw_fail:{row['fail_reason']}")
            if neutral is not None and not neutral["pass_gate"]:
                blocking_reasons.append(f"neutral_fail:{neutral['fail_reason']}")
            if split_fail_count:
                blocking_reasons.append(f"split_fail_count:{split_fail_count}")
            if rolling_summary.get("window_count") and not rolling_pass:
                blocking_reasons.append(
                    "rolling_fail:"
                    f"pass_rate={rolling_summary.get('pass_rate')},sign_flips={rolling_summary.get('sign_flip_count')}"
                )
            if high_corr:
                blocking_reasons.append(f"high_corr:{','.join(high_corr)}")
            if role == "exposure_probe":
                blocking_reasons.append("role:exposure_probe")

            if candidate_ready:
                research_stage = "candidate"
                promotion_reason = "raw, neutralized, and rolling checks all passed with low redundancy"
            elif watchlist_ready:
                research_stage = "watchlist"
                if neutral_pass and not rolling_pass:
                    promotion_reason = "raw and neutralized signals hold; rolling stability still needs work"
                elif rolling_pass and not neutral_pass:
                    promotion_reason = "raw and rolling hold; neutralized variant still needs work"
                else:
                    promotion_reason = "raw passed and at least one secondary validation held up"
            elif raw_pass:
                research_stage = "explore"
                promotion_reason = "raw score cleared the minimum bar, but neutral/rolling evidence is still weak"
            else:
                research_stage = "graveyard"
                promotion_reason = "raw signal did not clear the baseline gate"

            candidate_payload = {
                "factor_name": name,
                "expression": row["expression"],
                "factor_role": role,
                "raw_pass": raw_pass,
                "raw_rank_ic_mean": row["rank_ic_mean"],
                "raw_rank_ic_ir": row["rank_ic_ir"],
                "neutralized_pass": neutral_pass if neutral is not None else None,
                "neutralized_rank_ic_mean": neutral["rank_ic_mean"] if neutral else None,
                "split_fail_count": split_fail_count,
                "rolling_window_count": rolling_summary.get("window_count", 0),
                "rolling_pass_count": rolling_summary.get("pass_count", 0),
                "rolling_fail_count": rolling_summary.get("fail_count", 0),
                "rolling_pass_rate": rolling_summary.get("pass_rate"),
                "rolling_pass": rolling_pass,
                "rolling_sign_flip_count": rolling_summary.get("sign_flip_count", 0),
                "rolling_avg_rank_ic_mean": rolling_summary.get("avg_rank_ic_mean"),
                "rolling_rank_ic_std": rolling_summary.get("rank_ic_std"),
                "rolling_spread_std": rolling_summary.get("spread_std"),
                "rolling_stability_score": rolling_summary.get("stability_score"),
                "raw_score": raw_score,
                "neutral_score": neutral_score,
                "rolling_score": rolling_score,
                "turnover_penalty": turnover_penalty,
                "correlation_penalty": correlation_penalty,
                "style_exposure_penalty": style_exposure_penalty,
                "high_corr_peers": high_corr,
                "research_stage": research_stage,
                "promotion_reason": promotion_reason,
                "blocking_reasons": blocking_reasons,
            }

            if research_stage == "candidate":
                candidates.append(candidate_payload)
            elif research_stage == "watchlist":
                watchlist.append(candidate_payload)
            elif research_stage == "explore":
                explore.append(candidate_payload)
            else:
                graveyard.append({**candidate_payload, "graveyard_reason": "; ".join(blocking_reasons)})

        return explore, watchlist, candidates, graveyard

    def build_candidate_status_snapshot(
        self,
        explore: List[Dict],
        watchlist: List[Dict],
        candidates: List[Dict],
        graveyard: List[Dict],
    ) -> list[dict]:
        snapshot: list[dict] = []
        for row in [*explore, *watchlist, *candidates, *graveyard]:
            snapshot.append(
                {
                    "factor_name": row["factor_name"],
                    "factor_role": row.get("factor_role"),
                    "research_stage": row.get("research_stage"),
                    "raw_pass": row.get("raw_pass"),
                    "neutralized_pass": row.get("neutralized_pass"),
                    "rolling_pass": row.get("rolling_pass"),
                    "split_fail_count": row.get("split_fail_count", 0),
                    "rolling_pass_rate": row.get("rolling_pass_rate"),
                    "rolling_sign_flip_count": row.get("rolling_sign_flip_count", 0),
                    "high_corr_peers": row.get("high_corr_peers", []),
                    "promotion_reason": row.get("promotion_reason"),
                    "blocking_reasons": row.get("blocking_reasons", []),
                }
            )
        return sorted(snapshot, key=lambda item: (item.get("research_stage") or "", item["factor_name"]))

    def write_registry(
        self,
        explore: List[Dict],
        watchlist: List[Dict],
        candidates: List[Dict],
        graveyard: List[Dict],
        scored_factors: List[Dict] | None = None,
        cluster_representatives: List[Dict] | None = None,
    ) -> None:
        (self.output_dir / "explore_pool.json").write_text(
            json.dumps(explore, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (self.output_dir / "watchlist_pool.json").write_text(
            json.dumps(watchlist, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (self.output_dir / "candidate_pool.json").write_text(
            json.dumps(candidates, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (self.output_dir / "factor_graveyard.json").write_text(
            json.dumps(graveyard, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (self.output_dir / "candidate_status_snapshot.json").write_text(
            json.dumps(self.build_candidate_status_snapshot(explore, watchlist, candidates, graveyard), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        if scored_factors is not None:
            (self.output_dir / "factor_scores.json").write_text(
                json.dumps(scored_factors, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        if cluster_representatives is not None:
            (self.output_dir / "cluster_representatives.json").write_text(
                json.dumps(cluster_representatives, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
