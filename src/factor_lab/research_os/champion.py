"""Champion/challenger comparison using stitched outer-OOS and new shadow data."""

from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ChampionChallengePolicy:
    periods_per_year: float = 252.0 / 5.0
    min_active_information_ratio: float = 0.10
    max_drawdown_deterioration: float = 0.02
    min_positive_outer_years: int = 3
    min_shadow_sessions: int = 60


@dataclass(frozen=True)
class ChampionChallengeDecision:
    decision: str
    checks: dict[str, bool]
    metrics: dict[str, float | int]
    fallback: str

    def to_dict(self) -> dict:
        return asdict(self)


def _max_drawdown(returns: pd.Series) -> float:
    wealth = (1.0 + returns.fillna(0.0).astype(float)).cumprod()
    if wealth.empty:
        return 0.0
    return float((wealth / wealth.cummax() - 1.0).min())


def _information_ratio(active_returns: pd.Series, periods_per_year: float) -> float:
    clean = active_returns.dropna().astype(float)
    if len(clean) < 2:
        return 0.0
    volatility = float(clean.std(ddof=1))
    return float(clean.mean() / volatility * np.sqrt(periods_per_year)) if volatility > 0 else 0.0


def evaluate_challenger(
    challenger_returns: pd.Series,
    champion_returns: pd.Series,
    *,
    shadow_challenger_returns: pd.Series | None = None,
    shadow_champion_returns: pd.Series | None = None,
    policy: ChampionChallengePolicy | None = None,
) -> ChampionChallengeDecision:
    cfg = policy or ChampionChallengePolicy()
    aligned = pd.concat(
        [challenger_returns.rename("challenger"), champion_returns.rename("champion")],
        axis=1,
    ).dropna()
    if aligned.empty:
        return ChampionChallengeDecision(
            "retain_champion",
            {"historical_data": False},
            {"observations": 0},
            "static_champion",
        )
    active = aligned["challenger"] - aligned["champion"]
    active_ir = _information_ratio(active, cfg.periods_per_year)
    challenger_dd = _max_drawdown(aligned["challenger"])
    champion_dd = _max_drawdown(aligned["champion"])
    if isinstance(aligned.index, pd.DatetimeIndex):
        yearly = active.groupby(aligned.index.year).apply(lambda values: float((1.0 + values).prod() - 1.0))
    else:
        year_bucket = np.arange(len(active)) // max(int(round(cfg.periods_per_year)), 1)
        yearly = active.groupby(year_bucket).apply(lambda values: float((1.0 + values).prod() - 1.0))
    positive_years = int((yearly > 0).sum())

    shadow_sessions = 0
    shadow_excess = 0.0
    if shadow_challenger_returns is not None and shadow_champion_returns is not None:
        shadow = pd.concat(
            [shadow_challenger_returns.rename("challenger"), shadow_champion_returns.rename("champion")],
            axis=1,
        ).dropna()
        shadow_sessions = len(shadow)
        if shadow_sessions:
            shadow_excess = float(
                (1.0 + shadow["challenger"]).prod() - (1.0 + shadow["champion"]).prod()
            )
    checks = {
        "historical_data": len(aligned) >= max(20, int(cfg.periods_per_year)),
        "active_information_ratio": active_ir >= cfg.min_active_information_ratio,
        "drawdown_not_materially_worse": challenger_dd >= champion_dd - cfg.max_drawdown_deterioration,
        "positive_outer_years": positive_years >= cfg.min_positive_outer_years,
        "shadow_observation": shadow_sessions >= cfg.min_shadow_sessions,
        "shadow_excess_positive": shadow_excess > 0,
    }
    passed = all(checks.values())
    return ChampionChallengeDecision(
        "challenger_research_recovered" if passed else "retain_champion",
        checks,
        {
            "observations": len(aligned),
            "active_information_ratio": active_ir,
            "challenger_max_drawdown": challenger_dd,
            "champion_max_drawdown": champion_dd,
            "positive_outer_years": positive_years,
            "shadow_sessions": shadow_sessions,
            "shadow_excess": shadow_excess,
        },
        "challenger" if passed else "static_champion",
    )
