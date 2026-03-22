from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ExposurePolicy:
    bucket_budget: dict[str, float] | None = None
    b2_industry_top1_hard_limit: float = 0.30
    b2_retention_industry_hard_floor: float = 0.25
    implementability_cost_hard_floor: float = 0.0
    liquidity_bottom20_retention_floor: float = 0.60
    default_liquidity_bottom20_retention: float = 0.75

    def __post_init__(self) -> None:
        if self.bucket_budget is None:
            object.__setattr__(self, "bucket_budget", {
                "raw_exposure": 0.25,
                "controlled_composite": 0.60,
                "residual_like": 0.15,
            })


def clamp(value: float | None, lo: float = 0.0, hi: float = 100.0) -> float:
    if value is None or math.isnan(value):
        return 0.0
    return max(lo, min(hi, float(value)))


def classify_bucket(factor_name: str, expression: str | None = None) -> tuple[str, str]:
    haystack = f"{factor_name or ''} {expression or ''}".lower()
    if any(token in haystack for token in ["resid", "residual", "neutral", "orth", "idiosyncratic"]):
        return "residual_like", "Residual-like"
    if any(token in haystack for token in ["combo", "hybrid", "blend", "+", "/", "spread", "mix"]):
        return "controlled_composite", "Controlled Composite"
    return "raw_exposure", "Raw Exposure"


def classify_exposure_type(factor_name: str, expression: str | None = None) -> tuple[str, str]:
    name = f"{factor_name or ''} {expression or ''}".lower()
    if "mom" in name or "momentum" in name:
        return "momentum", "Momentum"
    if "value" in name or name.endswith("_ep") or name.endswith("_bp") or "earnings" in name or "book" in name:
        return "value", "Value"
    if "size" in name:
        return "size", "Size"
    if "liq" in name or "turnover" in name or "volume" in name:
        return "liquidity", "Liquidity/Turnover"
    if "quality" in name or "roe" in name or "profit" in name or "margin" in name:
        return "quality", "Quality"
    if "vol" in name or "variance" in name or "atr" in name:
        return "volatility", "Volatility"
    return "other", "Other"


def _industry_top1_weight(factor_name: str, exposure_type: str, split_fail_count: int, crowding_peers: int) -> float:
    base = {
        "momentum": 0.18,
        "value": 0.20,
        "size": 0.22,
        "liquidity": 0.17,
        "quality": 0.15,
        "volatility": 0.19,
        "other": 0.16,
    }.get(exposure_type, 0.16)
    return round(min(0.55, base + split_fail_count * 0.035 + crowding_peers * 0.012), 6)


def build_exposure_scorecard(
    *,
    factor_name: str,
    expression: str | None,
    strength_score: float | None,
    raw_rank_ic_mean: float | None,
    raw_rank_ic_ir: float | None,
    neutralized_rank_ic_mean: float | None,
    neutralized_pass_gate: bool | None,
    split_fail_count: int,
    crowding_peers: int,
    policy: ExposurePolicy | None = None,
) -> dict[str, Any]:
    policy = policy or ExposurePolicy()
    bucket_key, bucket_label = classify_bucket(factor_name, expression)
    exposure_type, exposure_label = classify_exposure_type(factor_name, expression)

    raw_ic = float(raw_rank_ic_mean or 0.0)
    raw_ir = float(raw_rank_ic_ir or 0.0)
    neutral_ic = float(neutralized_rank_ic_mean or 0.0)
    strength = float(strength_score or 0.0)
    split_fail_count = int(split_fail_count or 0)
    crowding_peers = int(crowding_peers or 0)

    industry_top1_weight = _industry_top1_weight(factor_name, exposure_type, split_fail_count, crowding_peers)
    industry_hhi = round(min(0.35, industry_top1_weight * 0.65 + 0.04 + crowding_peers * 0.006), 6)
    retention_industry = round(neutral_ic / raw_ic, 6) if abs(raw_ic) > 1e-9 else 0.0
    retention_industry_size = round(retention_industry * 0.9, 6)
    retention_full = round(retention_industry * 0.8, 6)
    turnover_daily = round(min(1.20, 0.10 + crowding_peers * 0.03 + split_fail_count * 0.06 + max(0.0, raw_ir) * 0.05), 6)
    cost_penalty = 0.18 * turnover_daily + 0.08 * crowding_peers
    net_metric = round((raw_ic * 6.0) + (raw_ir * 0.75) - cost_penalty, 6)
    liquidity_bottom20_retention = round(
        max(0.1, min(1.0, retention_industry * 0.55 + (1.0 - turnover_daily * 0.35) - crowding_peers * 0.03)),
        6,
    ) if raw_ic > 0 else round(policy.default_liquidity_bottom20_retention, 6)

    strength_subscore = clamp((max(raw_ic, 0.0) / 0.06) * 55 + (max(raw_ir, 0.0) / 1.0) * 25 + strength * 20)
    robustness_subscore = clamp(68 - split_fail_count * 18 + max(raw_ir, 0.0) * 10 + (12 if neutralized_pass_gate else -8))
    controllability_subscore = clamp(
        72
        + retention_industry * 32
        - max(0.0, industry_top1_weight - 0.18) * 160
        - crowding_peers * 4
    )
    implementability_subscore = clamp(
        78
        + min(net_metric, 0.5) * 45
        - max(0.0, turnover_daily - 0.35) * 90
        - max(0.0, policy.liquidity_bottom20_retention_floor - liquidity_bottom20_retention) * 120
    )
    novelty_subscore = clamp(66 - crowding_peers * 9 - max(0, split_fail_count - 1) * 6)

    weight_map = {
        "raw_exposure": {
            "strength": 45,
            "robustness": 20,
            "controllability": 20,
            "implementability": 10,
            "novelty": 5,
        },
        "controlled_composite": {
            "strength": 30,
            "robustness": 25,
            "controllability": 25,
            "implementability": 15,
            "novelty": 5,
        },
        "residual_like": {
            "strength": 20,
            "robustness": 25,
            "controllability": 20,
            "implementability": 15,
            "novelty": 20,
        },
    }
    weights = weight_map[bucket_key]
    total_score = round(
        strength_subscore * weights["strength"] / 100.0
        + robustness_subscore * weights["robustness"] / 100.0
        + controllability_subscore * weights["controllability"] / 100.0
        + implementability_subscore * weights["implementability"] / 100.0
        + novelty_subscore * weights["novelty"] / 100.0,
        6,
    )

    hard_flags: list[str] = []
    downgrade_bucket_to = None
    if bucket_key == "controlled_composite" and industry_top1_weight > policy.b2_industry_top1_hard_limit:
        hard_flags.append("b2_industry_concentration_exceeded")
        downgrade_bucket_to = "raw_exposure"
    if bucket_key == "controlled_composite" and retention_industry < policy.b2_retention_industry_hard_floor and raw_ic > 0.03:
        hard_flags.append("b2_retention_industry_too_low")
        downgrade_bucket_to = "raw_exposure"
    if bucket_key in {"controlled_composite", "residual_like"} and net_metric <= policy.implementability_cost_hard_floor:
        hard_flags.append("net_metric_non_positive")
    if bucket_key in {"controlled_composite", "residual_like"} and liquidity_bottom20_retention < policy.liquidity_bottom20_retention_floor:
        hard_flags.append("liquidity_bottom20_retention_too_low")

    effective_bucket_key = downgrade_bucket_to or bucket_key
    effective_bucket_label = {
        "raw_exposure": "Raw Exposure",
        "controlled_composite": "Controlled Composite",
        "residual_like": "Residual-like",
    }[effective_bucket_key]

    if hard_flags:
        status = "rejected" if any(flag in {"net_metric_non_positive", "liquidity_bottom20_retention_too_low"} for flag in hard_flags) else "downgraded"
    else:
        if total_score >= 70:
            status = "promoted"
        elif total_score >= 55:
            status = "qualified"
        else:
            status = "watch"

    recommended_max_weight = round(max(0.03, min(0.30, 0.24 - split_fail_count * 0.03 - crowding_peers * 0.015)), 4)

    return {
        "bucket_key": bucket_key,
        "bucket_label": bucket_label,
        "effective_bucket_key": effective_bucket_key,
        "effective_bucket_label": effective_bucket_label,
        "bucket_budget_weight": policy.bucket_budget.get(effective_bucket_key, 0.0),
        "exposure_type": exposure_type,
        "exposure_label": exposure_label,
        "industry_top1_weight": industry_top1_weight,
        "industry_hhi": industry_hhi,
        "retention_industry": retention_industry,
        "retention_industry_size": retention_industry_size,
        "retention_full": retention_full,
        "turnover_daily": turnover_daily,
        "net_metric": net_metric,
        "liquidity_bottom20_retention": liquidity_bottom20_retention,
        "strength_subscore": round(strength_subscore, 6),
        "robustness_subscore": round(robustness_subscore, 6),
        "controllability_subscore": round(controllability_subscore, 6),
        "implementability_subscore": round(implementability_subscore, 6),
        "novelty_subscore": round(novelty_subscore, 6),
        "total_score": total_score,
        "status": status,
        "recommended_max_weight": recommended_max_weight,
        "hard_flags": hard_flags,
        "policy_snapshot": {
            "bucket_budget": policy.bucket_budget,
            "b2_industry_top1_hard_limit": policy.b2_industry_top1_hard_limit,
            "b2_retention_industry_hard_floor": policy.b2_retention_industry_hard_floor,
            "implementability_cost_hard_floor": policy.implementability_cost_hard_floor,
            "liquidity_bottom20_retention_floor": policy.liquidity_bottom20_retention_floor,
        },
    }
