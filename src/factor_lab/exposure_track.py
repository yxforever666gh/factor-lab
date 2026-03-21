from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from factor_lab.storage import ExperimentStore


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


def classify_exposure_type(factor_name: str) -> tuple[str, str]:
    name = (factor_name or "").lower()
    if "mom" in name or "momentum" in name:
        return "momentum", "Momentum"
    if "value" in name or name.endswith("_ep") or name.endswith("_bp") or "earnings" in name or "book" in name:
        return "value", "Value"
    if "size" in name:
        return "size", "Size"
    if "liq" in name or "turnover" in name:
        return "liquidity", "Liquidity/Turnover"
    if "quality" in name or "roe" in name:
        return "quality", "Quality"
    return "other", "Other"


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
        exposure_type, exposure_label = classify_exposure_type(factor_name)
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

        # Strength-first status
        strong = False
        if strength_score is not None and strength_score >= cfg.min_strength_score:
            strong = True
        if raw_ic is not None and raw_ic >= cfg.min_raw_ic:
            strong = True

        # Risk-based sizing suggestion
        max_w = cfg.base_max_weight
        max_w -= cfg.crowding_penalty_per_peer * crowding
        max_w -= cfg.split_fail_penalty * split_fail
        max_w = max(cfg.min_max_weight, round(max_w, 4))

        status = "watch"
        if strong:
            status = "usable" if split_fail == 0 else "usable_limited"

        notes = {
            "high_corr_peers": peers[:12],
            "heuristics": {
                "strong": strong,
                "split_fail_count": split_fail,
                "crowding_peers": crowding,
            },
            "interpretation": {
                "exposure_track": True,
                "neutralized_is_label_only": True,
            },
        }

        out.append(
            {
                "run_id": run_id,
                "factor_name": factor_name,
                "exposure_type": exposure_type,
                "exposure_label": exposure_label,
                "strength_score": strength_score,
                "raw_rank_ic_mean": raw_ic,
                "raw_rank_ic_ir": raw_ir,
                "neutralized_rank_ic_mean": neutral_ic,
                "neutralized_pass_gate": neutral_pass,
                "split_fail_count": split_fail,
                "crowding_peers": crowding,
                "recommended_max_weight": max_w,
                "status": status,
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
