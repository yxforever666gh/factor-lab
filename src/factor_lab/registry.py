from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List


class FactorRegistry:
    def __init__(self, output_dir: str | Path) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def build_candidate_and_graveyard(
        self,
        raw_results: List[Dict],
        neutralized_results: List[Dict],
        split_results: List[Dict],
        correlation_lookup: Dict[str, List[str]],
    ) -> tuple[list[dict], list[dict]]:
        neutral_map = {row["factor_name"]: row for row in neutralized_results}
        split_map: Dict[str, List[Dict]] = {}
        for row in split_results:
            split_map.setdefault(row["factor_name"], []).append(row)

        candidates = []
        graveyard = []

        for row in raw_results:
            name = row["factor_name"]
            neutral = neutral_map.get(name)
            splits = split_map.get(name, [])
            split_fail_count = sum(1 for item in splits if not item["pass_gate"])
            high_corr = correlation_lookup.get(name, [])

            candidate_payload = {
                "factor_name": name,
                "expression": row["expression"],
                "raw_pass": row["pass_gate"],
                "raw_rank_ic_mean": row["rank_ic_mean"],
                "raw_rank_ic_ir": row["rank_ic_ir"],
                "neutralized_pass": neutral["pass_gate"] if neutral else None,
                "neutralized_rank_ic_mean": neutral["rank_ic_mean"] if neutral else None,
                "split_fail_count": split_fail_count,
                "high_corr_peers": high_corr,
            }

            if (
                row["pass_gate"]
                and (neutral is None or neutral["pass_gate"])
                and split_fail_count == 0
            ):
                candidates.append(candidate_payload)
            else:
                reasons = []
                if not row["pass_gate"]:
                    reasons.append(f"raw_fail:{row['fail_reason']}")
                if neutral is not None and not neutral["pass_gate"]:
                    reasons.append(f"neutral_fail:{neutral['fail_reason']}")
                if split_fail_count:
                    reasons.append(f"split_fail_count:{split_fail_count}")
                graveyard.append({**candidate_payload, "graveyard_reason": "; ".join(reasons)})

        return candidates, graveyard

    def write_registry(self, candidates: List[Dict], graveyard: List[Dict]) -> None:
        (self.output_dir / "candidate_pool.json").write_text(
            json.dumps(candidates, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (self.output_dir / "factor_graveyard.json").write_text(
            json.dumps(graveyard, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
