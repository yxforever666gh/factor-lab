from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import List

import pandas as pd


@dataclass
class FactorEvaluation:
    factor_name: str
    expression: str
    observations: int
    rank_ic_mean: float
    rank_ic_std: float
    rank_ic_ir: float
    top_bottom_spread_mean: float
    pass_gate: bool
    fail_reason: str

    def to_dict(self):
        return asdict(self)


def _rank_ic_by_date(frame: pd.DataFrame) -> pd.Series:
    pairs: List[tuple] = []
    for date, group in frame.groupby("date", sort=True):
        ranked_factor = group["factor_value"].rank()
        ranked_return = group["forward_return_5d"].rank()
        if ranked_factor.nunique() <= 1 or ranked_return.nunique() <= 1:
            continue
        pairs.append((date, float(ranked_factor.corr(ranked_return))))
    return pd.Series(dict(pairs), name="rank_ic")


def _quintile_spread_by_date(frame: pd.DataFrame) -> pd.Series:
    spreads: List[tuple] = []
    for date, group in frame.groupby("date", sort=True):
        if group["factor_value"].nunique() < 5:
            continue
        ranked = group.assign(bucket=pd.qcut(group["factor_value"].rank(method="first"), 5, labels=False))
        top = ranked.loc[ranked["bucket"] == 4, "forward_return_5d"].mean()
        bottom = ranked.loc[ranked["bucket"] == 0, "forward_return_5d"].mean()
        spreads.append((date, float(top - bottom)))
    return pd.Series(dict(spreads), name="spread")


def evaluate_factor(frame: pd.DataFrame, factor_name: str, expression: str, thresholds: dict) -> FactorEvaluation:
    if frame.empty:
        ic_mean = 0.0
        ic_std = 0.0
        ic_ir = 0.0
        spread_mean = 0.0
    else:
        work = frame[["date", "factor_value", "forward_return_5d"]].copy()
        work = work.dropna(subset=["date", "factor_value", "forward_return_5d"])

        if work.empty:
            ic_mean = 0.0
            ic_std = 0.0
            ic_ir = 0.0
            spread_mean = 0.0
        else:
            by_date = work.groupby("date", sort=True)
            work["rank_factor"] = by_date["factor_value"].rank()
            work["rank_return"] = by_date["forward_return_5d"].rank()

            rank_stats = work.groupby("date", sort=True).agg(
                rf_mean=("rank_factor", "mean"),
                rr_mean=("rank_return", "mean"),
                rf_std=("rank_factor", lambda s: float(s.std(ddof=0))),
                rr_std=("rank_return", lambda s: float(s.std(ddof=0))),
                rf_nunique=("rank_factor", "nunique"),
                rr_nunique=("rank_return", "nunique"),
            )
            work = work.join(rank_stats[["rf_mean", "rr_mean"]], on="date")
            work["cov_term"] = (work["rank_factor"] - work["rf_mean"]) * (work["rank_return"] - work["rr_mean"])
            cov_by_date = work.groupby("date", sort=True)["cov_term"].mean()
            corr_by_date = cov_by_date / (rank_stats["rf_std"] * rank_stats["rr_std"])
            valid_corr = corr_by_date[(rank_stats["rf_nunique"] > 1) & (rank_stats["rr_nunique"] > 1)].dropna()

            if valid_corr.empty:
                ic_mean = 0.0
                ic_std = 0.0
                ic_ir = 0.0
            else:
                ic_mean = float(valid_corr.mean())
                ic_std = float(valid_corr.std(ddof=0)) if len(valid_corr) > 1 else 0.0
                ic_ir = ic_mean / ic_std if ic_std > 0 else 0.0

            work["bucket"] = (by_date["factor_value"].rank(method="first", pct=True) * 5).clip(upper=5).apply(lambda x: int(x) - 1)
            bucket_means = work.groupby(["date", "bucket"], sort=True)["forward_return_5d"].mean().unstack()
            if 0 in bucket_means.columns and 4 in bucket_means.columns:
                spreads = (bucket_means[4] - bucket_means[0]).dropna()
                spread_mean = float(spreads.mean()) if not spreads.empty else 0.0
            else:
                spread_mean = 0.0

    fail_reasons = []
    min_rank_ic = thresholds.get("min_rank_ic", 0.03)
    min_spread = thresholds.get("min_top_bottom_spread", 0.0)
    if ic_mean < min_rank_ic:
        fail_reasons.append(f"rank_ic_mean<{min_rank_ic}")
    if spread_mean < min_spread:
        fail_reasons.append(f"top_bottom_spread<{min_spread}")

    return FactorEvaluation(
        factor_name=factor_name,
        expression=expression,
        observations=int(len(frame)),
        rank_ic_mean=round(ic_mean, 6),
        rank_ic_std=round(ic_std, 6),
        rank_ic_ir=round(ic_ir, 6),
        top_bottom_spread_mean=round(spread_mean, 6),
        pass_gate=not fail_reasons,
        fail_reason="; ".join(fail_reasons) if fail_reasons else "",
    )
