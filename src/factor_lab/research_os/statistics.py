"""Selection-aware statistics for the continuous research loop."""

from __future__ import annotations

from dataclasses import dataclass
from math import e, sqrt
from statistics import NormalDist
from typing import Callable, Iterable, Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class BlockBootstrapResult:
    observed: float
    bootstrap_mean: float
    confidence_interval: tuple[float, float]
    probability_positive: float
    one_sided_p_value: float
    block_size: int
    resamples: int


@dataclass(frozen=True)
class DeflatedSharpeResult:
    observed_sharpe: float
    expected_maximum_sharpe: float
    deflated_sharpe_probability: float
    selection_bias: float
    number_of_trials: int
    observations: int
    skewness: float
    kurtosis: float


@dataclass(frozen=True)
class OnlineAlphaDecision:
    trial_index: int
    p_value: float
    alpha: float
    rejected: bool
    cumulative_alpha_spent: float
    remaining_alpha_budget: float


def _clean_values(values: Iterable[float]) -> np.ndarray:
    array = np.asarray(list(values), dtype=float)
    return array[np.isfinite(array)]


def annualized_sharpe(values: Iterable[float], *, periods_per_year: float = 252.0 / 5.0) -> float:
    array = _clean_values(values)
    if len(array) < 2:
        return 0.0
    standard_deviation = float(np.std(array, ddof=1))
    if standard_deviation <= 0:
        return 0.0
    return float(np.mean(array) / standard_deviation * sqrt(periods_per_year))


def circular_block_bootstrap_samples(
    values: Iterable[float],
    *,
    block_size: int,
    resamples: int,
    seed: int = 0,
) -> np.ndarray:
    """Return circular moving-block samples while preserving local dependence."""

    array = _clean_values(values)
    if len(array) < 2:
        raise ValueError("at least two finite observations are required")
    if not 1 <= block_size <= len(array):
        raise ValueError("block_size must be between 1 and the sample length")
    if resamples <= 0:
        raise ValueError("resamples must be positive")
    rng = np.random.default_rng(seed)
    blocks_per_sample = int(np.ceil(len(array) / block_size))
    starts = rng.integers(0, len(array), size=(resamples, blocks_per_sample))
    offsets = np.arange(block_size, dtype=int)
    indices = (starts[:, :, None] + offsets[None, None, :]) % len(array)
    indices = indices.reshape(resamples, -1)[:, : len(array)]
    return array[indices]


def block_bootstrap(
    values: Iterable[float],
    *,
    statistic: str | Callable[[np.ndarray], float] = "annualized_mean",
    periods_per_year: float = 252.0 / 5.0,
    block_size: int | None = None,
    resamples: int = 2_000,
    confidence: float = 0.95,
    seed: int = 0,
) -> BlockBootstrapResult:
    """Estimate a time-block confidence interval and one-sided zero test."""

    array = _clean_values(values)
    if len(array) < 8:
        raise ValueError("block bootstrap requires at least eight finite observations")
    if not 0 < confidence < 1:
        raise ValueError("confidence must be in (0, 1)")
    size = block_size or max(2, int(round(len(array) ** (1.0 / 3.0))))
    samples = circular_block_bootstrap_samples(array, block_size=size, resamples=resamples, seed=seed)
    if statistic == "annualized_mean":
        observed = float(np.mean(array) * periods_per_year)
        estimates = np.mean(samples, axis=1) * periods_per_year
    elif statistic == "annualized_sharpe":
        observed = annualized_sharpe(array, periods_per_year=periods_per_year)
        means = np.mean(samples, axis=1)
        standard_deviations = np.std(samples, axis=1, ddof=1)
        estimates = np.divide(
            means * sqrt(periods_per_year),
            standard_deviations,
            out=np.zeros_like(means),
            where=standard_deviations > 0,
        )
    elif callable(statistic):
        observed = float(statistic(array))
        estimates = np.asarray([float(statistic(row)) for row in samples], dtype=float)
    else:
        raise ValueError(f"unsupported bootstrap statistic: {statistic!r}")
    tail = (1.0 - confidence) / 2.0
    lower, upper = np.quantile(estimates, [tail, 1.0 - tail])
    non_positive = int(np.count_nonzero(estimates <= 0))
    probability_positive = float(np.mean(estimates > 0))
    return BlockBootstrapResult(
        observed=observed,
        bootstrap_mean=float(np.mean(estimates)),
        confidence_interval=(float(lower), float(upper)),
        probability_positive=probability_positive,
        one_sided_p_value=float((non_positive + 1) / (resamples + 1)),
        block_size=size,
        resamples=resamples,
    )


def deflated_sharpe_ratio(
    returns: Iterable[float],
    *,
    number_of_trials: int | None = None,
    trial_sharpes: Sequence[float] | None = None,
    periods_per_year: float = 252.0 / 5.0,
) -> DeflatedSharpeResult:
    """Calculate the Bailey/Lopez de Prado selection-adjusted Sharpe probability.

    ``trial_sharpes`` must contain annualized Sharpes from the complete trial
    family, including failures.  When they are unavailable, the sampling
    standard error is used as a conservative proxy for cross-trial dispersion.
    """

    values = _clean_values(returns)
    if len(values) < 3:
        raise ValueError("deflated Sharpe requires at least three finite returns")
    if periods_per_year <= 0:
        raise ValueError("periods_per_year must be positive")
    observed_annual = annualized_sharpe(values, periods_per_year=periods_per_year)
    observed_periodic = observed_annual / sqrt(periods_per_year)
    centered = values - float(np.mean(values))
    population_std = float(np.std(values, ddof=0))
    if population_std <= 0:
        skewness, kurtosis = 0.0, 3.0
    else:
        standardized = centered / population_std
        skewness = float(np.mean(standardized**3))
        kurtosis = float(np.mean(standardized**4))
    variance_term = max(
        1.0 - skewness * observed_periodic + ((kurtosis - 1.0) / 4.0) * observed_periodic**2,
        1e-12,
    )
    sampling_standard_error_annual = sqrt(variance_term / max(len(values) - 1, 1)) * sqrt(periods_per_year)

    provided_sharpes = _clean_values(() if trial_sharpes is None else trial_sharpes)
    trials = int(number_of_trials if number_of_trials is not None else max(len(provided_sharpes), 1))
    if trials < 1:
        raise ValueError("number_of_trials must be positive")
    if len(provided_sharpes) >= 2:
        trial_dispersion = float(np.std(provided_sharpes, ddof=1))
    else:
        trial_dispersion = sampling_standard_error_annual
    if trials == 1:
        expected_maximum = 0.0
    else:
        normal = NormalDist()
        euler_mascheroni = 0.5772156649015329
        first_probability = min(max(1.0 - 1.0 / trials, 1e-12), 1.0 - 1e-12)
        second_probability = min(max(1.0 - 1.0 / (trials * e), 1e-12), 1.0 - 1e-12)
        expected_maximum = trial_dispersion * (
            (1.0 - euler_mascheroni) * normal.inv_cdf(first_probability)
            + euler_mascheroni * normal.inv_cdf(second_probability)
        )
    expected_periodic = expected_maximum / sqrt(periods_per_year)
    z_score = (observed_periodic - expected_periodic) * sqrt(len(values) - 1) / sqrt(variance_term)
    probability = float(NormalDist().cdf(z_score))
    return DeflatedSharpeResult(
        observed_sharpe=observed_annual,
        expected_maximum_sharpe=float(expected_maximum),
        deflated_sharpe_probability=probability,
        selection_bias=float(expected_maximum),
        number_of_trials=trials,
        observations=len(values),
        skewness=skewness,
        kurtosis=kurtosis,
    )


def holm_adjust(p_values: Sequence[float]) -> tuple[float, ...]:
    """Return Holm step-down family-wise adjusted p-values."""

    values = np.asarray(p_values, dtype=float)
    if np.any(~np.isfinite(values)) or np.any((values < 0) | (values > 1)):
        raise ValueError("p-values must be finite and in [0, 1]")
    count = len(values)
    if not count:
        return ()
    order = np.argsort(values, kind="stable")
    ordered = values[order]
    adjusted_ordered = np.maximum.accumulate((count - np.arange(count)) * ordered)
    adjusted_ordered = np.clip(adjusted_ordered, 0.0, 1.0)
    adjusted = np.empty(count, dtype=float)
    adjusted[order] = adjusted_ordered
    return tuple(float(item) for item in adjusted)


def holm_rejections(p_values: Sequence[float], *, alpha: float = 0.05) -> tuple[bool, ...]:
    if not 0 < alpha < 1:
        raise ValueError("alpha must be in (0, 1)")
    adjusted = holm_adjust(p_values)
    return tuple(item <= alpha for item in adjusted)


def dependence_adjusted_online_alpha(
    p_values: Sequence[float],
    *,
    family_alpha_budget: float = 0.10,
    maximum_family_trials: int = 120,
) -> tuple[OnlineAlphaDecision, ...]:
    """Conservative online alpha spending valid under arbitrary dependence.

    A pre-registered ``1 / (i(i+1))`` spending sequence is divided by the
    Benjamini-Yekutieli harmonic factor.  This controls family-wise error and
    therefore FDR even when sequential family tests are dependent.  Rejections
    do not replenish the budget, preventing feedback-driven search inflation.
    """

    if not 0 < family_alpha_budget < 1:
        raise ValueError("family_alpha_budget must be in (0, 1)")
    if maximum_family_trials <= 0:
        raise ValueError("maximum_family_trials must be positive")
    if len(p_values) > maximum_family_trials:
        raise ValueError("p-values exceed the pre-registered family trial limit")
    values = np.asarray(p_values, dtype=float)
    if np.any(~np.isfinite(values)) or np.any((values < 0) | (values > 1)):
        raise ValueError("p-values must be finite and in [0, 1]")
    harmonic = sum(1.0 / item for item in range(1, maximum_family_trials + 1))
    gamma_normalizer = sum(1.0 / (item * (item + 1.0)) for item in range(1, maximum_family_trials + 1))
    spent = 0.0
    decisions: list[OnlineAlphaDecision] = []
    for index, p_value in enumerate(values, start=1):
        gamma = (1.0 / (index * (index + 1.0))) / gamma_normalizer
        allocation = family_alpha_budget * gamma / harmonic
        spent += allocation
        decisions.append(
            OnlineAlphaDecision(
                trial_index=index,
                p_value=float(p_value),
                alpha=float(allocation),
                rejected=bool(p_value <= allocation),
                cumulative_alpha_spent=float(spent),
                remaining_alpha_budget=float(max(family_alpha_budget - spent, 0.0)),
            )
        )
    return tuple(decisions)
