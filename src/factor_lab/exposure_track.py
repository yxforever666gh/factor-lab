from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from factor_lab.storage import ExperimentStore
from factor_lab.exposure_scorecard import ExposurePolicy, build_exposure_scorecard


@dataclass
class ExposureConfig:
    # Strength-first thresholds (tune later; intentionally permissive)
    min_strength_score: float = 0.5
    min_raw_ic: float = 0.03
    min_observations: int = 0  # not available in factor_results today

    # Risk controls (used for recommended_max_weight)
    base_max_weight: float = 0.25
    crowding_penalty_per_peer: float = 0.02
    split_fail_penalty: float = 0.08
    min_max_weight: float = 0.05


# classify_exposure_type moved to exposure_scorecard (keep single source of truth)


def build_exposure_rows(store: ExperimentStore, run_id: str, cfg: ExposureConfig | None = None) -> list[dict[str, Any]]:
    cfg = cfg or ExposureConfig()
    now = datetime.now(timezone.utc).isoformat()

    # Pull raw-like metrics from candidate/graveyard rows (they carry raw IC/IR + score).
    rows = [dict(r) for r in store.conn.execute(
        """
        SELECT factor_name, expression, rank_ic_mean, rank_ic_ir, score, split_fail_count, high_corr_peers_json
        FROM factor_results
        WHERE run_id = ? AND variant IN ('candidate', 'graveyard')
        """,
        (run_id,),
    ).fetchall()]

    neutral_map: dict[str, dict[str, Any]] = {
        r["factor_name"]: dict(r)
        for r in store.conn.execute(
            """
            SELECT factor_name, rank_ic_mean, pass_gate
            FROM factor_results
            WHERE run_id = ? AND variant = 'neutralized'
            """,
            (run_id,),
        ).fetchall()
    }

    out: list[dict[str, Any]] = []
    for r in rows:
        factor_name = r["factor_name"]
        expression = r.get("expression")
        strength_score = r.get("score")
        raw_ic = r.get("rank_ic_mean")
        raw_ir = r.get("rank_ic_ir")
        split_fail = int(r.get("split_fail_count") or 0)
        peers = []
        try:
            peers = json.loads(r.get("high_corr_peers_json") or "[]")
        except Exception:
            peers = []
        crowding = len(peers)

        neutral = neutral_map.get(factor_name) or {}
        neutral_ic = neutral.get("rank_ic_mean")
        neutral_pass = neutral.get("pass_gate")

        scorecard = build_exposure_scorecard(
            factor_name=factor_name,
            expression=expression,
            strength_score=strength_score,
            raw_rank_ic_mean=raw_ic,
            raw_rank_ic_ir=raw_ir,
            neutralized_rank_ic_mean=neutral_ic,
            neutralized_pass_gate=neutral_pass,
            split_fail_count=split_fail,
            crowding_peers=crowding,
            policy=ExposurePolicy(),
        )

        notes = {
            "high_corr_peers": peers[:12],
            "heuristics": {
                "strong": bool((strength_score is not None and strength_score >= cfg.min_strength_score) or (raw_ic is not None and raw_ic >= cfg.min_raw_ic)),
                "split_fail_count": split_fail,
                "crowding_peers": crowding,
            },
            "interpretation": {
                "exposure_track": True,
                "neutralized_is_label_only": True,
                "all_a_daily_policy": True,
            },
            "scorecard": scorecard,
        }

        out.append(
            {
                "run_id": run_id,
                "factor_name": factor_name,
                "exposure_type": scorecard["exposure_type"],
                "exposure_label": scorecard["exposure_label"],
                "bucket_key": scorecard["bucket_key"],
                "bucket_label": scorecard["bucket_label"],
                "effective_bucket_key": scorecard["effective_bucket_key"],
                "effective_bucket_label": scorecard["effective_bucket_label"],
                "strength_score": strength_score,
                "raw_rank_ic_mean": raw_ic,
                "raw_rank_ic_ir": raw_ir,
                "neutralized_rank_ic_mean": neutral_ic,
                "neutralized_pass_gate": neutral_pass,
                "retention_industry": scorecard["retention_industry"],
                "retention_industry_size": scorecard["retention_industry_size"],
                "retention_full": scorecard["retention_full"],
                "industry_top1_weight": scorecard["industry_top1_weight"],
                "industry_hhi": scorecard["industry_hhi"],
                "turnover_daily": scorecard["turnover_daily"],
                "net_metric": scorecard["net_metric"],
                "liquidity_bottom20_retention": scorecard["liquidity_bottom20_retention"],
                "strength_subscore": scorecard["strength_subscore"],
                "robustness_subscore": scorecard["robustness_subscore"],
                "controllability_subscore": scorecard["controllability_subscore"],
                "implementability_subscore": scorecard["implementability_subscore"],
                "novelty_subscore": scorecard["novelty_subscore"],
                "total_score": scorecard["total_score"],
                "split_fail_count": split_fail,
                "crowding_peers": crowding,
                "recommended_max_weight": scorecard["recommended_max_weight"],
                "status": scorecard["status"],
                "hard_flags": scorecard["hard_flags"],
                "notes": notes,
                "created_at_utc": now,
                "updated_at_utc": now,
            }
        )

    return out


def refresh_exposure_track(store: ExperimentStore, run_id: str, cfg: ExposureConfig | None = None) -> dict[str, Any]:
    rows = build_exposure_rows(store, run_id, cfg=cfg)
    store.upsert_exposure_rows(rows)
    return {
        "run_id": run_id,
        "exposure_factor_count": len(rows),
        "usable": len([r for r in rows if r.get("status") == "usable"]),
        "usable_limited": len([r for r in rows if r.get("status") == "usable_limited"]),
    }
