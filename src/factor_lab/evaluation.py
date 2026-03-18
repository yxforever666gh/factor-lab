from __future__ import annotations

from dataclasses import asdict, dataclass
import math
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
    def corr_fn(group: pd.DataFrame) -> float:
        ranked_factor = group["factor_value"].rank()
        ranked_return = group["forward_return_5d"].rank()
        if ranked_factor.nunique() <= 1 or ranked_return.nunique() <= 1:
            return float("nan")
        return ranked_factor.corr(ranked_return)

    return frame.groupby("date", sort=True).apply(corr_fn).dropna()


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
    rank_ic = _rank_ic_by_date(frame)
    spreads = _quintile_spread_by_date(frame)

    ic_mean = float(rank_ic.mean()) if not rank_ic.empty else 0.0
    ic_std = float(rank_ic.std(ddof=0)) if len(rank_ic) > 1 else 0.0
    ic_ir = ic_mean / ic_std if ic_std > 0 else 0.0
    spread_mean = float(spreads.mean()) if not spreads.empty else 0.0

    fail_reasons = []
    if ic_mean < thresholds.get("min_rank_ic", 0.03):
        fail_reasons.append(f"rank_ic_mean<{thresholds['min_rank_ic']}")
    if spread_mean < thresholds.get("min_top_bottom_spread", 0.0):
        fail_reasons.append(f"top_bottom_spread<{thresholds['min_top_bottom_spread']}")

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
