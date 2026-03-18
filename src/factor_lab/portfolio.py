from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable, List

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
    observations: int

    def to_dict(self):
        return asdict(self)


def build_composite_factor(
    frame: pd.DataFrame,
    definitions: Iterable[FactorDefinition],
    neutralize: bool = False,
) -> pd.Series:
    signals: List[pd.Series] = []
    for definition in definitions:
        values = apply_factor(frame, definition)
        if neutralize and {"industry", "total_mv"}.issubset(frame.columns):
            tmp = frame.copy()
            tmp["raw_factor"] = values
            values = neutralize_by_date(tmp, factor_col="raw_factor")
        zscored = values.groupby(frame["date"]).transform(
            lambda s: (s - s.mean()) / s.std(ddof=0) if s.std(ddof=0) not in (0, 0.0) else 0.0
        )
        signals.append(zscored.fillna(0.0))
    return sum(signals) / len(signals)


def evaluate_long_short_portfolio(
    frame: pd.DataFrame,
    composite_signal: pd.Series,
    top_q: float = 0.2,
    bottom_q: float = 0.2,
) -> PortfolioEvaluation:
    work = frame[["date", "ticker", "forward_return_5d"]].copy()
    work["signal"] = composite_signal.values

    daily_rets = []
    prev_weights = None
    turnovers = []

    for date, group in work.groupby("date", sort=True):
        group = group.dropna(subset=["signal", "forward_return_5d"]).copy()
        if len(group) < 10:
            continue
        long_cut = group["signal"].quantile(1 - top_q)
        short_cut = group["signal"].quantile(bottom_q)

        group["weight"] = 0.0
        long_mask = group["signal"] >= long_cut
        short_mask = group["signal"] <= short_cut
        if long_mask.sum() == 0 or short_mask.sum() == 0:
            continue
        group.loc[long_mask, "weight"] = 1.0 / long_mask.sum()
        group.loc[short_mask, "weight"] = -1.0 / short_mask.sum()

        ret = float((group["weight"] * group["forward_return_5d"]).sum())
        daily_rets.append((date, ret))

        weights = group.set_index("ticker")["weight"]
        if prev_weights is not None:
            all_idx = weights.index.union(prev_weights.index)
            turnover = (weights.reindex(all_idx, fill_value=0.0) - prev_weights.reindex(all_idx, fill_value=0.0)).abs().sum() / 2.0
            turnovers.append(float(turnover))
        prev_weights = weights

    series = pd.Series(dict(daily_rets)).sort_index()
    if series.empty:
        return PortfolioEvaluation("long_short", 0.0, 0.0, 0.0, 0.0, 0.0, 0)

    nav = (1.0 + series).cumprod()
    peak = nav.cummax()
    drawdown = nav / peak - 1.0
    annual_return = float(series.mean() * 48)
    annual_vol = float(series.std(ddof=0) * (48 ** 0.5)) if len(series) > 1 else 0.0
    sharpe = annual_return / annual_vol if annual_vol > 0 else 0.0
    max_dd = float(drawdown.min())
    avg_turnover = float(pd.Series(turnovers).mean()) if turnovers else 0.0

    return PortfolioEvaluation(
        strategy_name="long_short_top_bottom",
        annual_return=round(annual_return, 6),
        annual_volatility=round(annual_vol, 6),
        sharpe=round(sharpe, 6),
        max_drawdown=round(max_dd, 6),
        avg_turnover=round(avg_turnover, 6),
        observations=int(len(series)),
    )
