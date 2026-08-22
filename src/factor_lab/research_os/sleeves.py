"""Sleeve aggregation, state-conditioned overlays and defensive allocation."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from factor_lab.research_os.lifecycle import SleeveState


@dataclass(frozen=True)
class SleeveDescriptor:
    sleeve_id: str
    cluster_id: str
    eligible: bool = True


@dataclass(frozen=True)
class PortfolioAllocation:
    sleeve_weights: dict[str, float]
    benchmark_weight: float = 0.0
    cash_weight: float = 0.0
    reason: str = ""

    @property
    def total_weight(self) -> float:
        return sum(self.sleeve_weights.values()) + self.benchmark_weight + self.cash_weight

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["total_weight"] = round(self.total_weight, 12)
        return payload


def _bounded_weights(raw: Mapping[str, float], cap: float) -> tuple[dict[str, float], float]:
    if not 0 < cap <= 1:
        raise ValueError("cap must be in (0, 1]")
    positive = {str(key): max(float(value), 0.0) for key, value in raw.items() if float(value) > 0}
    total = sum(positive.values())
    if total <= 0:
        return {}, 1.0
    weights = {key: value / total for key, value in positive.items()}
    fixed: dict[str, float] = {}
    remaining = set(weights)
    residual = 1.0
    while remaining:
        remaining_raw = sum(weights[key] for key in remaining)
        if remaining_raw <= 0:
            break
        proposed = {key: residual * weights[key] / remaining_raw for key in remaining}
        breached = {key for key, value in proposed.items() if value > cap + 1e-12}
        if not breached:
            fixed.update(proposed)
            residual = 0.0
            break
        for key in breached:
            fixed[key] = cap
            residual -= cap
            remaining.remove(key)
        if residual <= 1e-12:
            residual = 0.0
            break
    fixed = {key: max(min(value, cap), 0.0) for key, value in fixed.items()}
    return fixed, max(0.0, 1.0 - sum(fixed.values()))


def build_cluster_balanced_champion(
    sleeves: Iterable[SleeveDescriptor | Mapping[str, Any]],
    *,
    sleeve_cap: float = 0.35,
) -> PortfolioAllocation:
    descriptors: list[SleeveDescriptor] = []
    for row in sleeves:
        if isinstance(row, SleeveDescriptor):
            item = row
        else:
            item = SleeveDescriptor(
                sleeve_id=str(row["sleeve_id"]),
                cluster_id=str(row.get("cluster_id") or row["sleeve_id"]),
                eligible=bool(row.get("eligible", True)),
            )
        if item.eligible:
            descriptors.append(item)
    if not descriptors:
        return PortfolioAllocation({}, benchmark_weight=0.5, cash_weight=0.5, reason="no_eligible_sleeves")

    by_cluster: dict[str, list[SleeveDescriptor]] = {}
    for item in descriptors:
        by_cluster.setdefault(item.cluster_id, []).append(item)
    cluster_weight = 1.0 / len(by_cluster)
    raw: dict[str, float] = {}
    for rows in by_cluster.values():
        member_weight = cluster_weight / len(rows)
        for item in rows:
            raw[item.sleeve_id] = member_weight
    bounded, residual = _bounded_weights(raw, sleeve_cap)
    return PortfolioAllocation(
        bounded,
        benchmark_weight=residual,
        reason="cluster_balanced_static_champion",
    )


def blend_adaptive_challenger(
    champion_weights: Mapping[str, float],
    adaptive_scores: Mapping[str, float],
    *,
    previous_weights: Mapping[str, float] | None = None,
    adaptive_fraction: float = 0.25,
    sleeve_cap: float = 0.35,
    max_monthly_change: float = 0.05,
) -> PortfolioAllocation:
    if not 0 <= adaptive_fraction <= 0.25:
        raise ValueError("adaptive_fraction must be in [0, 0.25]")
    champion_total = sum(max(float(v), 0.0) for v in champion_weights.values())
    champion = {
        str(k): max(float(v), 0.0) / champion_total
        for k, v in champion_weights.items()
        if champion_total > 0 and float(v) > 0
    }
    positive_scores = {str(k): max(float(v), 0.0) for k, v in adaptive_scores.items() if float(v) > 0}
    score_total = sum(positive_scores.values())
    adaptive = {key: value / score_total for key, value in positive_scores.items()} if score_total else {}
    if not champion:
        return PortfolioAllocation({}, benchmark_weight=0.5, cash_weight=0.5, reason="missing_static_champion")

    all_ids = set(champion) | set(adaptive)
    target = {
        sleeve_id: (1.0 - adaptive_fraction) * champion.get(sleeve_id, 0.0)
        + adaptive_fraction * adaptive.get(sleeve_id, 0.0)
        for sleeve_id in all_ids
    }
    if previous_weights is not None:
        clamped: dict[str, float] = {}
        for sleeve_id in set(target) | set(previous_weights):
            previous = max(float(previous_weights.get(sleeve_id, 0.0)), 0.0)
            desired = max(float(target.get(sleeve_id, 0.0)), 0.0)
            clamped[sleeve_id] = min(max(desired, previous - max_monthly_change), previous + max_monthly_change)
        target = clamped
    bounded, residual = _bounded_weights(target, sleeve_cap)
    return PortfolioAllocation(
        bounded,
        benchmark_weight=residual,
        reason="75pct_static_25pct_state_conditioned" if adaptive else "static_champion_no_positive_overlay",
    )


def apply_health_fallback(
    target_weights: Mapping[str, float],
    states: Mapping[str, SleeveState | str],
    *,
    data_quality_ok: bool = True,
) -> PortfolioAllocation:
    if not data_quality_ok:
        return PortfolioAllocation({}, cash_weight=1.0, reason="data_integrity_failure")

    retained: dict[str, float] = {}
    for sleeve_id, target in target_weights.items():
        state = SleeveState(states.get(sleeve_id, SleeveState.DORMANT))
        target = max(float(target), 0.0)
        if state == SleeveState.ACTIVE:
            retained[sleeve_id] = target
        elif state == SleeveState.PROBATION:
            retained[sleeve_id] = min(target, 0.05)
        elif state == SleeveState.REDUCED:
            retained[sleeve_id] = target * 0.5

    retained_total = sum(retained.values())
    if retained_total <= 1e-12:
        return PortfolioAllocation({}, benchmark_weight=0.5, cash_weight=0.5, reason="no_healthy_sleeve")
    residual = max(0.0, 1.0 - retained_total)
    return PortfolioAllocation(retained, benchmark_weight=residual, reason="degraded_weight_to_benchmark")


def build_market_state_snapshot(
    frame: pd.DataFrame,
    *,
    as_of: str | pd.Timestamp | None = None,
    date_column: str = "date",
    ticker_column: str = "ticker",
    close_column: str = "close_adj",
    amount_column: str = "amount",
) -> dict[str, Any]:
    """Build an interpretable state vector using observations available at ``as_of``."""

    required = {date_column, ticker_column, close_column}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"missing market-state columns: {sorted(missing)}")
    work = frame.copy()
    work[date_column] = pd.to_datetime(work[date_column])
    cutoff = pd.Timestamp(as_of) if as_of is not None else work[date_column].max()
    work = work[work[date_column] <= cutoff].sort_values([ticker_column, date_column])
    if work.empty:
        raise ValueError("no observations at or before as_of")
    work[close_column] = pd.to_numeric(work[close_column], errors="coerce")
    work["_return"] = work.groupby(ticker_column)[close_column].pct_change()
    for window in (20, 60):
        work[f"_ma_{window}"] = work.groupby(ticker_column)[close_column].transform(
            lambda series: series.rolling(window, min_periods=max(5, window // 2)).mean()
        )

    daily_return = work.groupby(date_column)["_return"].mean().sort_index().dropna()
    latest_date = work[date_column].max()
    latest = work[work[date_column] == latest_date]

    def compound(window: int) -> float:
        values = daily_return.tail(window)
        return float((1.0 + values).prod() - 1.0) if len(values) else 0.0

    payload: dict[str, Any] = {
        "as_of_date": str(latest_date.date()),
        "trend_20d": compound(20),
        "trend_60d": compound(60),
        "trend_120d": compound(120),
        "realized_volatility_20d": float(daily_return.tail(20).std(ddof=1) * np.sqrt(252)) if len(daily_return.tail(20)) > 1 else 0.0,
        "realized_volatility_60d": float(daily_return.tail(60).std(ddof=1) * np.sqrt(252)) if len(daily_return.tail(60)) > 1 else 0.0,
        "breadth_above_ma20": float((latest[close_column] > latest["_ma_20"]).mean()),
        "breadth_above_ma60": float((latest[close_column] > latest["_ma_60"]).mean()),
        "cross_sectional_dispersion": float(latest["_return"].std(ddof=1)) if latest["_return"].notna().sum() > 1 else 0.0,
    }
    if amount_column in work.columns:
        daily_liquidity = work.groupby(date_column)[amount_column].median().sort_index()
        trailing = float(daily_liquidity.tail(60).median()) if not daily_liquidity.empty else 0.0
        latest_liquidity = float(daily_liquidity.iloc[-1]) if not daily_liquidity.empty else 0.0
        payload["liquidity_ratio_60d"] = latest_liquidity / trailing - 1.0 if trailing > 0 else 0.0
    else:
        payload["liquidity_ratio_60d"] = 0.0
    for name in ("size_active_return", "value_active_return", "momentum_active_return", "sleeve_correlation", "crowding_score"):
        if name in work.columns:
            series = work.groupby(date_column)[name].mean().sort_index().tail(20)
            payload[f"{name}_20d"] = float(series.mean()) if not series.empty else 0.0
    return payload


def fit_state_conditioned_overlay(
    state_history: pd.DataFrame,
    sleeve_returns: pd.DataFrame,
    *,
    ridge_alpha: float = 100.0,
    min_observations: int = 60,
) -> dict[str, Any]:
    """Predict next-period sleeve returns with a strongly shrunk linear model.

    The latest state is used only for prediction.  Training pairs state at
    ``t`` with the return realised at ``t+1``.
    """

    if state_history.empty or sleeve_returns.empty:
        return {"status": "insufficient_data", "weights": {}, "predictions": {}}
    x = state_history.select_dtypes(include=[np.number]).sort_index()
    r = sleeve_returns.select_dtypes(include=[np.number]).sort_index()
    common = x.index.intersection(r.index)
    x = x.loc[common]
    r = r.loc[common]
    if len(common) < min_observations + 1:
        return {"status": "insufficient_data", "weights": {}, "predictions": {}, "observations": len(common)}

    latest_x = x.iloc[[-1]]
    train_x = x.iloc[:-1]
    train_y = r.shift(-1).iloc[:-1]
    valid = train_x.notna().all(axis=1) & train_y.notna().all(axis=1)
    train_x = train_x.loc[valid]
    train_y = train_y.loc[valid]
    if len(train_x) < min_observations:
        return {"status": "insufficient_data", "weights": {}, "predictions": {}, "observations": len(train_x)}
    means = train_x.mean()
    scales = train_x.std(ddof=0).replace(0.0, 1.0)
    design = ((train_x - means) / scales).to_numpy(dtype=float)
    latest_design = ((latest_x - means) / scales).fillna(0.0).to_numpy(dtype=float)
    outcomes = train_y.to_numpy(dtype=float)
    # Closed-form ridge keeps the core usable without an sklearn runtime.
    augmented = np.column_stack([np.ones(len(design)), design])
    latest_augmented = np.column_stack([np.ones(len(latest_design)), latest_design])
    penalty = np.eye(augmented.shape[1]) * float(ridge_alpha)
    penalty[0, 0] = 0.0
    coefficients = np.linalg.pinv(augmented.T @ augmented + penalty) @ augmented.T @ outcomes
    predictions_array = (latest_augmented @ coefficients).ravel()
    predictions = {name: float(value) for name, value in zip(train_y.columns, predictions_array)}
    positive = {name: max(value, 0.0) for name, value in predictions.items() if value > 0}
    total = sum(positive.values())
    weights = {name: value / total for name, value in positive.items()} if total else {}
    return {
        "status": "ok",
        "weights": weights,
        "predictions": predictions,
        "observations": len(train_x),
        "ridge_alpha": float(ridge_alpha),
        "train_end": str(train_x.index.max()),
        "prediction_as_of": str(latest_x.index.max()),
    }
