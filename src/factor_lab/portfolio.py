from __future__ import annotations

from dataclasses import asdict, dataclass
from math import ceil
from typing import Iterable, List, Mapping

import pandas as pd

from factor_lab.factors import FactorDefinition, apply_factor
from factor_lab.neutralization import neutralize_by_date


@dataclass
class PortfolioEvaluation:
    strategy_name: str
    annual_return: float
    annual_volatility: float
    sharpe: float
    max_drawdown: float
    avg_turnover: float
    turnover_cost_estimate: float
    cost_adjusted_annual_return: float
    observations: int

    def to_dict(self):
        return asdict(self)


def build_composite_factor(
    frame: pd.DataFrame,
    definitions: Iterable[FactorDefinition],
    neutralize: bool = False,
    *,
    factor_value_cache: Mapping[str, pd.Series] | None = None,
    factor_weights: Mapping[str, float] | None = None,
) -> pd.Series:
    signals: List[pd.Series] = []
    weights: List[float] = []
    for definition in definitions:
        if factor_value_cache is not None and definition.name in factor_value_cache:
            values = factor_value_cache[definition.name]
        else:
            values = apply_factor(frame, definition)
        if neutralize and {"industry", "total_mv"}.issubset(frame.columns):
            tmp = frame[["date", "ticker", "industry", "total_mv"]].copy()
            tmp["raw_factor"] = values
            values = neutralize_by_date(tmp, factor_col="raw_factor")
        zscored = values.groupby(frame["date"]).transform(
            lambda s: (s - s.mean()) / s.std(ddof=0) if s.std(ddof=0) not in (0, 0.0) else 0.0
        )
        signals.append(zscored.fillna(0.0))
        weight = float((factor_weights or {}).get(definition.name, 1.0) or 0.0)
        weights.append(weight if weight > 0 else 0.0)
    if not signals:
        raise ValueError("build_composite_factor requires at least one factor definition")
    total_weight = sum(weights)
    if total_weight <= 0:
        total_weight = float(len(signals))
        weights = [1.0 for _ in signals]
    combined = None
    for signal, weight in zip(signals, weights):
        term = signal * (weight / total_weight)
        combined = term if combined is None else combined + term
    return combined


def evaluate_long_short_portfolio(
    frame: pd.DataFrame,
    composite_signal: pd.Series,
    top_q: float = 0.2,
    bottom_q: float = 0.2,
    cost_bps_per_turnover: float = 10.0,
) -> PortfolioEvaluation:
    work = frame[["date", "ticker", "forward_return_5d"]].copy()
    work["signal"] = composite_signal.values
    work = work.dropna(subset=["signal", "forward_return_5d"]).copy()
    if work.empty:
        return PortfolioEvaluation("long_short", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0)

    group_sizes = work.groupby("date")["signal"].transform("size")
    work = work[group_sizes >= 10].copy()
    if work.empty:
        return PortfolioEvaluation("long_short", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0)

    group_sizes = work.groupby("date")["signal"].transform("size")
    long_n = group_sizes.apply(lambda n: max(1, int(ceil(n * top_q))))
    short_n = group_sizes.apply(lambda n: max(1, int(ceil(n * bottom_q))))
    long_rank = work.groupby("date")["signal"].rank(method="first", ascending=False)
    short_rank = work.groupby("date")["signal"].rank(method="first", ascending=True)

    long_mask = long_rank <= long_n
    short_mask = short_rank <= short_n
    if not long_mask.any() or not short_mask.any():
        return PortfolioEvaluation("long_short", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0)

    long_counts = long_mask.groupby(work["date"]).transform("sum")
    short_counts = short_mask.groupby(work["date"]).transform("sum")
    work["weight"] = 0.0
    work.loc[long_mask, "weight"] = 1.0 / long_counts[long_mask]
    work.loc[short_mask, "weight"] = -1.0 / short_counts[short_mask]

    series = (work["weight"] * work["forward_return_5d"]).groupby(work["date"]).sum().sort_index()
    if series.empty:
        return PortfolioEvaluation("long_short", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0)

    turnovers = []
    prev_weights = None
    for date, group in work.loc[work["weight"] != 0.0].groupby("date", sort=True):
        weights = group.set_index("ticker")["weight"]
        if prev_weights is not None:
            all_idx = weights.index.union(prev_weights.index)
            turnover = (weights.reindex(all_idx, fill_value=0.0) - prev_weights.reindex(all_idx, fill_value=0.0)).abs().sum() / 2.0
            turnovers.append(float(turnover))
        prev_weights = weights

    nav = (1.0 + series).cumprod()
    peak = nav.cummax()
    drawdown = nav / peak - 1.0
    annual_return = float(series.mean() * 48)
    annual_vol = float(series.std(ddof=0) * (48 ** 0.5)) if len(series) > 1 else 0.0
    sharpe = annual_return / annual_vol if annual_vol > 0 else 0.0
    max_dd = float(drawdown.min())
    avg_turnover = float(pd.Series(turnovers).mean()) if turnovers else 0.0
    turnover_cost_estimate = avg_turnover * (cost_bps_per_turnover / 10000.0) * 48
    cost_adjusted_annual_return = annual_return - turnover_cost_estimate

    return PortfolioEvaluation(
        strategy_name="long_short_top_bottom",
        annual_return=round(annual_return, 6),
        annual_volatility=round(annual_vol, 6),
        sharpe=round(sharpe, 6),
        max_drawdown=round(max_dd, 6),
        avg_turnover=round(avg_turnover, 6),
        turnover_cost_estimate=round(turnover_cost_estimate, 6),
        cost_adjusted_annual_return=round(cost_adjusted_annual_return, 6),
        observations=int(len(series)),
    )
