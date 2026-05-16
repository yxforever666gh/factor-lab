from __future__ import annotations

from typing import Any


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def evaluate_bucket_aware_gate(*, factor_result: dict | None, bucket_result: dict | None, thresholds: dict | None = None) -> dict:
    thresholds = thresholds or {}
    if not bucket_result:
        return {"decision": "fail", "reasons": ["missing_bucket_aware_result"], "rank_ic_mean": _f((factor_result or {}).get("rank_ic_mean")), "bucket_spread_mean": None}
    rank_ic = _f((factor_result or {}).get("rank_ic_mean"))
    bucket_spread = _f(bucket_result.get("spread_mean"))
    min_rank_ic = _f(thresholds.get("min_rank_ic"), 0.0)
    min_bucket_spread = _f(thresholds.get("min_bucket_spread", thresholds.get("min_top_bottom_spread")), 0.0)
    reasons: list[str] = []
    if rank_ic < min_rank_ic:
        reasons.append(f"rank_ic_mean<{min_rank_ic}")
    if bucket_spread < min_bucket_spread:
        reasons.append(f"bucket_spread<{min_bucket_spread}")
    return {"decision": "fail" if reasons else "pass", "reasons": reasons, "rank_ic_mean": rank_ic, "bucket_spread_mean": bucket_spread}
