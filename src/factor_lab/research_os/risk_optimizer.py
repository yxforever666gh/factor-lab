"""Cost-aware, long-only stock optimizer with explicit PIT exposure gates."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

import numpy as np
import pandas as pd
from scipy.optimize import minimize


@dataclass(frozen=True)
class StockOptimizationPolicy:
    min_positions: int = 50
    max_positions: int = 100
    max_position_weight: float = 0.02
    industry_deviation: float = 0.05
    size_deviation: float = 0.05
    beta_min: float = 0.9
    beta_max: float = 1.1
    max_adv_participation: float = 0.05
    capital: float = 50_000_000.0
    risk_aversion: float = 5.0
    turnover_penalty: float = 0.01
    minimum_return_observations: int = 60


@dataclass(frozen=True)
class OptimizedStockPortfolio:
    status: str
    weights: dict[str, float]
    cash_weight: float
    promotion_eligible: bool
    audit: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _shrunk_covariance(returns: pd.DataFrame) -> tuple[np.ndarray, str]:
    clean = returns.fillna(0.0).to_numpy(dtype=float)
    if clean.shape[0] < 2:
        return np.eye(clean.shape[1]) * 1e-4, "diagonal_minimum_history"
    try:
        from sklearn.covariance import LedoitWolf

        return LedoitWolf().fit(clean).covariance_, "ledoit_wolf"
    except Exception:
        sample = np.cov(clean, rowvar=False, ddof=1)
        sample = np.atleast_2d(sample)
        diagonal = np.diag(np.diag(sample))
        return 0.5 * sample + 0.5 * diagonal, "diagonal_shrinkage_fallback"


def optimize_stock_weights(
    scores: pd.Series,
    returns_history: pd.DataFrame,
    metadata: pd.DataFrame,
    benchmark_weights: pd.Series,
    *,
    previous_weights: pd.Series | None = None,
    policy: StockOptimizationPolicy | None = None,
) -> OptimizedStockPortfolio:
    cfg = policy or StockOptimizationPolicy()
    if not 1 <= cfg.min_positions <= cfg.max_positions:
        raise ValueError("min_positions must be positive and no greater than max_positions")
    if not 0 < cfg.max_position_weight <= 1:
        raise ValueError("max_position_weight must be in (0, 1]")
    if not 0 < cfg.max_adv_participation <= 1 or cfg.capital <= 0:
        raise ValueError("capacity participation and capital must be positive")
    if cfg.minimum_return_observations < 2:
        raise ValueError("minimum_return_observations must be at least 2")
    required = {"industry", "size_bucket", "beta", "adv_20", "industry_is_pit"}
    missing = required - set(metadata.columns)
    if missing:
        return OptimizedStockPortfolio(
            "blocked_missing_pit_exposure_data",
            {},
            1.0,
            False,
            {"missing_columns": sorted(missing)},
        )
    pit_flags = metadata["industry_is_pit"].map(
        lambda value: (
            bool(value)
            if isinstance(value, (bool, np.bool_))
            else (
                float(value) == 1.0
                if isinstance(value, (int, float, np.number)) and pd.notna(value)
                else str(value).strip().lower() in {"true", "yes", "y", "1"}
            )
        )
    )
    if not pit_flags.all():
        return OptimizedStockPortfolio(
            "blocked_non_pit_industry",
            {},
            1.0,
            False,
            {"industry_constraint_status": "missing_pit_data"},
        )

    invalid_exposure = {
        "industry_null_count": int(metadata["industry"].isna().sum()),
        "size_bucket_null_count": int(metadata["size_bucket"].isna().sum()),
        "beta_invalid_count": int(
            pd.to_numeric(metadata["beta"], errors="coerce").isna().sum()
        ),
        "adv_invalid_count": int(
            (
                pd.to_numeric(metadata["adv_20"], errors="coerce").isna()
                | pd.to_numeric(metadata["adv_20"], errors="coerce").le(0)
            ).sum()
        ),
    }
    if any(invalid_exposure.values()):
        return OptimizedStockPortfolio(
            "blocked_invalid_exposure_data",
            {},
            1.0,
            False,
            invalid_exposure,
        )

    common = scores.dropna().index.intersection(metadata.index).intersection(returns_history.columns)
    if len(common) < cfg.min_positions:
        return OptimizedStockPortfolio(
            "insufficient_universe",
            {},
            1.0,
            False,
            {"available_positions": len(common), "required_positions": cfg.min_positions},
        )
    selected = scores.loc[common].sort_values(ascending=False).head(cfg.max_positions).index
    meta = metadata.loc[selected].copy()
    benchmark = benchmark_weights.reindex(metadata.index, fill_value=0.0).clip(lower=0.0)
    if benchmark.sum() > 0:
        benchmark /= benchmark.sum()
    selected_scores = scores.loc[selected].astype(float)
    score_std = float(selected_scores.std(ddof=0))
    alpha = ((selected_scores - selected_scores.mean()) / (score_std if score_std > 0 else 1.0)).to_numpy()
    history = returns_history.loc[:, selected].apply(pd.to_numeric, errors="coerce")
    history = history.dropna(how="all")
    if len(history) < cfg.minimum_return_observations:
        return OptimizedStockPortfolio(
            "blocked_insufficient_return_history",
            {},
            1.0,
            False,
            {
                "return_observations": len(history),
                "minimum_return_observations": cfg.minimum_return_observations,
            },
        )
    covariance, covariance_method = _shrunk_covariance(history)
    previous = (
        previous_weights.reindex(selected, fill_value=0.0).clip(lower=0.0).to_numpy(dtype=float)
        if previous_weights is not None
        else np.zeros(len(selected), dtype=float)
    )
    capacity_caps = (
        pd.to_numeric(meta["adv_20"], errors="coerce").fillna(0.0)
        * cfg.max_adv_participation
        / cfg.capital
    ).clip(lower=0.0, upper=cfg.max_position_weight)
    bounds = [(0.0, float(capacity_caps.loc[ticker])) for ticker in selected]
    max_investable = sum(bound[1] for bound in bounds)
    if max_investable < 1.0 - 1e-9:
        return OptimizedStockPortfolio(
            "blocked_capacity",
            {},
            1.0,
            False,
            {"max_investable_weight": max_investable},
        )

    def objective(weights: np.ndarray) -> float:
        risk = float(weights @ covariance @ weights)
        turnover = float(np.sqrt((weights - previous) ** 2 + 1e-10).sum())
        return -float(alpha @ weights) + cfg.risk_aversion * risk + cfg.turnover_penalty * turnover

    constraints: list[dict[str, Any]] = [{"type": "eq", "fun": lambda weights: float(weights.sum() - 1.0)}]
    all_industries = sorted(metadata["industry"].astype(str).unique())
    for industry in all_industries:
        mask = (meta["industry"].astype(str) == industry).to_numpy(dtype=float)
        benchmark_weight = float(benchmark.loc[metadata["industry"].astype(str) == industry].sum())
        lower = max(0.0, benchmark_weight - cfg.industry_deviation)
        upper = min(1.0, benchmark_weight + cfg.industry_deviation)
        constraints.extend(
            [
                {"type": "ineq", "fun": lambda weights, m=mask, lo=lower: float(weights @ m - lo)},
                {"type": "ineq", "fun": lambda weights, m=mask, hi=upper: float(hi - weights @ m)},
            ]
        )
    all_size_buckets = sorted(metadata["size_bucket"].astype(str).unique())
    for bucket in all_size_buckets:
        mask = (meta["size_bucket"].astype(str) == bucket).to_numpy(dtype=float)
        benchmark_weight = float(benchmark.loc[metadata["size_bucket"].astype(str) == bucket].sum())
        lower = max(0.0, benchmark_weight - cfg.size_deviation)
        upper = min(1.0, benchmark_weight + cfg.size_deviation)
        constraints.extend(
            [
                {"type": "ineq", "fun": lambda weights, m=mask, lo=lower: float(weights @ m - lo)},
                {"type": "ineq", "fun": lambda weights, m=mask, hi=upper: float(hi - weights @ m)},
            ]
        )
    beta = pd.to_numeric(meta["beta"], errors="coerce").fillna(1.0).to_numpy(dtype=float)
    constraints.extend(
        [
            {"type": "ineq", "fun": lambda weights: float(weights @ beta - cfg.beta_min)},
            {"type": "ineq", "fun": lambda weights: float(cfg.beta_max - weights @ beta)},
        ]
    )

    # Capacity-proportional initialization is guaranteed to respect every
    # individual cap whenever the aggregate capacity can fund the portfolio.
    initial = capacity_caps.to_numpy(dtype=float, copy=True)
    initial /= initial.sum()
    result = minimize(
        objective,
        initial,
        method="SLSQP",
        bounds=bounds,
        constraints=constraints,
        options={"maxiter": 1000, "ftol": 1e-10},
    )
    weights = pd.Series(result.x if result.success else initial, index=selected).clip(lower=0.0)
    weights = weights[weights > 1e-8]
    total = float(weights.sum())
    if total > 1.0 + 1e-8:
        weights /= total
    industry_actual = weights.groupby(meta.loc[weights.index, "industry"].astype(str)).sum()
    size_actual = weights.groupby(meta.loc[weights.index, "size_bucket"].astype(str)).sum()
    max_industry_deviation = max(
        [
            abs(
                float(industry_actual.get(label, 0.0))
                - float(benchmark.loc[metadata["industry"].astype(str) == label].sum())
            )
            for label in all_industries
        ]
        or [0.0]
    )
    max_size_deviation = max(
        [
            abs(
                float(size_actual.get(label, 0.0))
                - float(benchmark.loc[metadata["size_bucket"].astype(str) == label].sum())
            )
            for label in all_size_buckets
        ]
        or [0.0]
    )
    portfolio_beta = float(weights.reindex(selected, fill_value=0.0).to_numpy() @ beta)
    audit = {
        "solver_success": bool(result.success),
        "solver_message": str(result.message),
        "position_count": len(weights),
        "max_position_weight": float(weights.max()) if len(weights) else 0.0,
        "max_industry_deviation": max_industry_deviation,
        "max_size_deviation": max_size_deviation,
        "portfolio_beta": portfolio_beta,
        "capacity_violation_count": sum(
            float(weights.get(ticker, 0.0))
            > float(capacity_caps.loc[ticker]) + 1e-8
            for ticker in selected
        ),
        "covariance_method": covariance_method,
        "return_observations": len(history),
        "constraints": {
            "industry_deviation": cfg.industry_deviation,
            "size_deviation": cfg.size_deviation,
            "beta_min": cfg.beta_min,
            "beta_max": cfg.beta_max,
            "max_position_weight": cfg.max_position_weight,
            "max_adv_participation": cfg.max_adv_participation,
            "capital": cfg.capital,
            "minimum_return_observations": cfg.minimum_return_observations,
        },
    }
    eligible = (
        bool(result.success)
        and cfg.min_positions <= len(weights) <= cfg.max_positions
        and audit["max_position_weight"] <= cfg.max_position_weight + 1e-8
        and max_industry_deviation <= cfg.industry_deviation + 1e-7
        and max_size_deviation <= cfg.size_deviation + 1e-7
        and cfg.beta_min - 1e-7 <= portfolio_beta <= cfg.beta_max + 1e-7
        and audit["capacity_violation_count"] == 0
        and covariance_method == "ledoit_wolf"
    )
    return OptimizedStockPortfolio(
        "ok" if eligible else "constraint_failure",
        {str(key): float(value) for key, value in weights.items()},
        max(0.0, 1.0 - float(weights.sum())),
        eligible,
        audit,
    )
