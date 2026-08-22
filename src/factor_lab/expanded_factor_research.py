from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

import numpy as np
import pandas as pd


MAX_PREREGISTERED_VARIANTS = 6
DEFAULT_Q_THRESHOLD = 0.10
Direction = Literal["train_selected", "higher_is_better", "lower_is_better", "unavailable"]


@dataclass(frozen=True)
class FactorDefinition:
    """A preregistered long-only signal whose sign is selected on train only."""

    name: str
    family: str
    direction: Direction
    allow_in_long_only: bool
    expression: str
    required_fields: tuple[str, ...]
    calculator: Callable[[pd.DataFrame], pd.Series]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "family": self.family,
            "direction": self.direction,
            "allow_in_long_only": self.allow_in_long_only,
            "expression": self.expression,
            "required_fields": list(self.required_fields),
        }


@dataclass(frozen=True)
class FactorComputationResult:
    series_by_name: dict[str, pd.Series]
    unavailable: dict[str, tuple[str, ...]]


@dataclass(frozen=True)
class FrozenDirection:
    name: str
    family: str
    direction: Direction
    multiplier: int
    train_rank_ic: float | None
    train_observation_count: int
    train_ic_date_count: int
    non_overlapping_step: int
    selected_on: str = "train"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "family": self.family,
            "direction": self.direction,
            "multiplier": self.multiplier,
            "train_rank_ic": self.train_rank_ic,
            "train_observation_count": self.train_observation_count,
            "train_ic_date_count": self.train_ic_date_count,
            "non_overlapping_step": self.non_overlapping_step,
            "selected_on": self.selected_on,
        }


@dataclass(frozen=True)
class ExpandedFactorResearchResult:
    definitions: tuple[FactorDefinition, ...]
    computation: FactorComputationResult
    frozen_directions: dict[str, FrozenDirection]
    window_metrics: list[dict[str, Any]]
    family_trial_ledger: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "definitions": [row.to_dict() for row in self.definitions],
            "unavailable": {key: list(value) for key, value in self.computation.unavailable.items()},
            "frozen_directions": {key: value.to_dict() for key, value in self.frozen_directions.items()},
            "window_metrics": self.window_metrics,
            "family_trial_ledger": self.family_trial_ledger,
        }


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce").reset_index(drop=True)


def _dates(frame: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(frame["date"], errors="coerce").reset_index(drop=True)


def _date_zscore(values: pd.Series, dates: pd.Series) -> pd.Series:
    work = pd.DataFrame({"value": values, "date": dates})
    means = work.groupby("date", dropna=False)["value"].transform("mean")
    stds = work.groupby("date", dropna=False)["value"].transform("std").replace(0.0, np.nan)
    return (work["value"] - means) / stds


def _industry_size_adjusted_value(frame: pd.DataFrame) -> pd.Series:
    dates = _dates(frame)
    industries = frame["industry"].reset_index(drop=True)
    book = _date_zscore(_numeric(frame, "book_yield"), dates)
    earnings = _date_zscore(_numeric(frame, "earnings_yield"), dates)
    raw_value = pd.concat([book, earnings], axis=1).mean(axis=1, skipna=False)

    group = pd.DataFrame({"date": dates, "industry": industries, "value": raw_value})
    industry_adjusted = group["value"] - group.groupby(["date", "industry"], dropna=False)["value"].transform("mean")

    market_value = _numeric(frame, "total_mv").where(lambda value: value > 0.0)
    size = _date_zscore(np.log(market_value), dates)
    regression = pd.DataFrame({"date": dates, "value": industry_adjusted, "size": size})
    valid_pair = regression["value"].notna() & regression["size"].notna()
    cross_product = (regression["value"] * regression["size"]).where(valid_pair)
    size_square = regression["size"].pow(2).where(valid_pair)
    numerator = cross_product.groupby(regression["date"], dropna=False).transform("sum")
    denominator = size_square.groupby(regression["date"], dropna=False).transform("sum").replace(0.0, np.nan)
    adjusted = regression["value"] - (numerator / denominator) * regression["size"]
    return _date_zscore(adjusted, dates)


def _quality_value_composite(frame: pd.DataFrame) -> pd.Series:
    dates = _dates(frame)
    book = _date_zscore(_numeric(frame, "book_yield"), dates)
    earnings = _date_zscore(_numeric(frame, "earnings_yield"), dates)
    value = pd.concat([book, earnings], axis=1).mean(axis=1, skipna=False)
    quality = _date_zscore(_numeric(frame, "roe"), dates)
    return pd.concat([value, quality], axis=1).mean(axis=1, skipna=False)


def _sorted_market_frame(frame: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "row": np.arange(len(frame)),
            "date": _dates(frame),
            "ticker": frame["ticker"].astype(str).reset_index(drop=True),
            "close": _numeric(frame, "close"),
        }
    ).sort_values(["ticker", "date", "row"], kind="stable")


def _price_return_between_lags(frame: pd.DataFrame, recent_lag: int, old_lag: int) -> pd.Series:
    work = _sorted_market_frame(frame)
    grouped = work.groupby("ticker", sort=False)["close"]
    signal = grouped.shift(recent_lag) / grouped.shift(old_lag) - 1.0
    result = pd.Series(np.nan, index=np.arange(len(frame)), dtype=float)
    result.iloc[work["row"].to_numpy()] = signal.to_numpy()
    return result


def _momentum_12_1(frame: pd.DataFrame) -> pd.Series:
    return _price_return_between_lags(frame, recent_lag=21, old_lag=252)


def _momentum_6_1(frame: pd.DataFrame) -> pd.Series:
    return _price_return_between_lags(frame, recent_lag=21, old_lag=126)


def _short_term_reversal(frame: pd.DataFrame) -> pd.Series:
    return -_price_return_between_lags(frame, recent_lag=0, old_lag=21)


def _low_volatility_defensive(frame: pd.DataFrame) -> pd.Series:
    work = pd.DataFrame(
        {
            "row": np.arange(len(frame)),
            "date": _dates(frame),
            "ticker": frame["ticker"].astype(str).reset_index(drop=True),
            "return_1d": _numeric(frame, "return_1d"),
        }
    ).sort_values(["ticker", "date", "row"], kind="stable")
    volatility = (
        work.groupby("ticker", sort=False)["return_1d"]
        .rolling(window=60, min_periods=40)
        .std()
        .reset_index(level=0, drop=True)
    )
    result = pd.Series(np.nan, index=np.arange(len(frame)), dtype=float)
    result.iloc[work["row"].to_numpy()] = (-volatility).to_numpy()
    return result


PREREGISTERED_LONG_ONLY_FACTORS: tuple[FactorDefinition, ...] = (
    FactorDefinition(
        name="industry_size_adjusted_value",
        family="value",
        direction="train_selected",
        allow_in_long_only=True,
        expression="neutralize_by_date_size(industry_center(z(book_yield) + z(earnings_yield)))",
        required_fields=("date", "industry", "book_yield", "earnings_yield", "total_mv"),
        calculator=_industry_size_adjusted_value,
    ),
    FactorDefinition(
        name="quality_value_composite",
        family="value",
        direction="train_selected",
        allow_in_long_only=True,
        expression="0.5 * mean(z(book_yield), z(earnings_yield)) + 0.5 * z(roe)",
        required_fields=("date", "book_yield", "earnings_yield", "roe"),
        calculator=_quality_value_composite,
    ),
    FactorDefinition(
        name="momentum_12_1",
        family="momentum",
        direction="train_selected",
        allow_in_long_only=True,
        expression="close.shift(21) / close.shift(252) - 1",
        required_fields=("date", "ticker", "close"),
        calculator=_momentum_12_1,
    ),
    FactorDefinition(
        name="momentum_6_1",
        family="momentum",
        direction="train_selected",
        allow_in_long_only=True,
        expression="close.shift(21) / close.shift(126) - 1",
        required_fields=("date", "ticker", "close"),
        calculator=_momentum_6_1,
    ),
    FactorDefinition(
        name="short_term_reversal",
        family="reversal",
        direction="train_selected",
        allow_in_long_only=True,
        expression="-(close / close.shift(21) - 1)",
        required_fields=("date", "ticker", "close"),
        calculator=_short_term_reversal,
    ),
    FactorDefinition(
        name="low_volatility_defensive",
        family="defensive",
        direction="train_selected",
        allow_in_long_only=True,
        expression="-rolling_std_60(return_1d)",
        required_fields=("date", "ticker", "return_1d"),
        calculator=_low_volatility_defensive,
    ),
)


def preregistered_long_only_factors() -> tuple[FactorDefinition, ...]:
    """Return the immutable six-factor research menu."""

    return PREREGISTERED_LONG_ONLY_FACTORS


def _validate_definitions(definitions: Sequence[FactorDefinition]) -> tuple[FactorDefinition, ...]:
    selected = tuple(definitions)
    if len(selected) > MAX_PREREGISTERED_VARIANTS:
        raise ValueError(f"at most {MAX_PREREGISTERED_VARIANTS} factor variants are allowed")
    registered = {row.name: row for row in PREREGISTERED_LONG_ONLY_FACTORS}
    if len({row.name for row in selected}) != len(selected):
        raise ValueError("factor variant names must be unique")
    if any(row.name not in registered or row != registered[row.name] for row in selected):
        raise ValueError("only preregistered long-only factor variants are allowed")
    return selected


def compute_preregistered_factors(
    frame: pd.DataFrame,
    definitions: Sequence[FactorDefinition] = PREREGISTERED_LONG_ONLY_FACTORS,
) -> FactorComputationResult:
    """Compute each available factor independently so one missing field cannot fan out."""

    selected = _validate_definitions(definitions)
    available = set(frame.columns)
    series_by_name: dict[str, pd.Series] = {}
    unavailable: dict[str, tuple[str, ...]] = {}
    for definition in selected:
        missing = tuple(field for field in definition.required_fields if field not in available)
        if missing:
            unavailable[definition.name] = missing
            continue
        calculated = pd.to_numeric(definition.calculator(frame), errors="coerce")
        if len(calculated) != len(frame):
            raise ValueError(f"factor {definition.name} returned {len(calculated)} rows for a {len(frame)} row frame")
        series_by_name[definition.name] = pd.Series(calculated.to_numpy(), index=frame.index, name=definition.name)
    return FactorComputationResult(series_by_name=series_by_name, unavailable=unavailable)


def fixed_sample_labels(dates: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(dates, errors="coerce")
    labels = pd.Series("out_of_scope", index=dates.index, dtype="object")
    labels.loc[(parsed >= "2017-01-01") & (parsed <= "2022-12-31")] = "train"
    labels.loc[(parsed >= "2023-01-01") & (parsed <= "2024-12-31")] = "validation"
    labels.loc[parsed >= "2025-01-01"] = "observed_audit"
    return labels


def split_fixed_samples(frame: pd.DataFrame, *, date_column: str = "date") -> dict[str, pd.DataFrame]:
    if date_column not in frame.columns:
        raise KeyError(date_column)
    labels = fixed_sample_labels(frame[date_column])
    return {
        name: frame.loc[labels == name].copy()
        for name in ("train", "validation", "observed_audit")
    }


def _daily_rank_ics(dates: pd.Series, signal: pd.Series, forward_returns: pd.Series) -> pd.Series:
    work = pd.DataFrame(
        {
            "date": pd.to_datetime(dates, errors="coerce").to_numpy(),
            "signal": pd.to_numeric(signal, errors="coerce").to_numpy(),
            "forward_return": pd.to_numeric(forward_returns, errors="coerce").to_numpy(),
        }
    ).dropna()
    values: list[float] = []
    for _, part in work.groupby("date", sort=True):
        if len(part) < 3 or part["signal"].nunique() < 2 or part["forward_return"].nunique() < 2:
            continue
        value = part["signal"].corr(part["forward_return"], method="spearman")
        if value is not None and math.isfinite(float(value)):
            values.append(float(value))
    return pd.Series(values, dtype=float)


def _validate_non_overlapping_step(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("non_overlapping_step must be a positive integer")
    return value


def _window_metric(
    frame: pd.DataFrame,
    signal: pd.Series,
    mask: pd.Series,
    *,
    multiplier: int,
    forward_return_column: str,
    non_overlapping_step: int = 5,
) -> dict[str, Any]:
    step = _validate_non_overlapping_step(non_overlapping_step)
    parsed_dates = pd.to_datetime(frame["date"], errors="coerce")
    ordered_window_dates = (
        pd.Series(parsed_dates.loc[mask].dropna().unique(), dtype="datetime64[ns]")
        .sort_values(kind="stable")
        .reset_index(drop=True)
    )
    sampled_dates = set(ordered_window_dates.iloc[::step].tolist())
    sampled_mask = mask & parsed_dates.isin(sampled_dates)
    positions = sampled_mask.to_numpy(dtype=bool)
    raw_ics = _daily_rank_ics(
        frame.loc[sampled_mask, "date"],
        pd.Series(signal.to_numpy()[positions]),
        frame.loc[sampled_mask, forward_return_column],
    )
    raw_mean = float(raw_ics.mean()) if not raw_ics.empty else None
    directional = raw_ics * multiplier
    directional_mean = float(directional.mean()) if not directional.empty else None
    p_value: float | None = None
    if len(directional) >= 2:
        standard_error = float(directional.std(ddof=1)) / math.sqrt(len(directional))
        if standard_error > 0.0 and math.isfinite(standard_error):
            z_score = float(directional.mean()) / standard_error
            p_value = 0.5 * math.erfc(z_score / math.sqrt(2.0))
        elif directional_mean is not None:
            p_value = 0.0 if directional_mean > 0.0 else 1.0
    return {
        "raw_rank_ic": raw_mean,
        "directional_rank_ic": directional_mean,
        "p_value": p_value,
        "date_count": int(len(directional)),
        "non_overlapping_step": step,
        "observation_count": int(
            pd.DataFrame(
                {
                    "signal": signal.to_numpy()[positions],
                    "forward_return": pd.to_numeric(frame.loc[sampled_mask, forward_return_column], errors="coerce").to_numpy(),
                }
            ).dropna().shape[0]
        ),
    }


def freeze_directions_from_train(
    frame: pd.DataFrame,
    computation: FactorComputationResult,
    definitions: Sequence[FactorDefinition] = PREREGISTERED_LONG_ONLY_FACTORS,
    *,
    forward_return_column: str = "forward_return_5d",
    non_overlapping_step: int = 5,
) -> dict[str, FrozenDirection]:
    """Choose each sign exclusively from 2017-2022 observations and freeze it."""

    selected = _validate_definitions(definitions)
    if "date" not in frame.columns:
        raise KeyError("date")
    if forward_return_column not in frame.columns:
        raise KeyError(forward_return_column)
    step = _validate_non_overlapping_step(non_overlapping_step)
    train_mask = fixed_sample_labels(frame["date"]) == "train"
    frozen: dict[str, FrozenDirection] = {}
    for definition in selected:
        signal = computation.series_by_name.get(definition.name)
        if signal is None:
            continue
        metric = _window_metric(
            frame,
            signal,
            train_mask,
            multiplier=1,
            forward_return_column=forward_return_column,
            non_overlapping_step=step,
        )
        train_ic = metric["raw_rank_ic"]
        if train_ic is None:
            direction: Direction = "unavailable"
            multiplier = 0
        elif train_ic < 0.0:
            direction = "lower_is_better"
            multiplier = -1
        else:
            direction = "higher_is_better"
            multiplier = 1
        frozen[definition.name] = FrozenDirection(
            name=definition.name,
            family=definition.family,
            direction=direction,
            multiplier=multiplier,
            train_rank_ic=train_ic,
            train_observation_count=metric["observation_count"],
            train_ic_date_count=metric["date_count"],
            non_overlapping_step=step,
        )
    return frozen


def benjamini_hochberg(p_values: Sequence[float | None]) -> list[float | None]:
    """Return monotone BH-adjusted q-values in the original order."""

    total_hypotheses = len(p_values)
    if total_hypotheses == 0:
        return []
    valid: list[tuple[int, float]] = []
    for index, raw in enumerate(p_values):
        if raw is None or not math.isfinite(float(raw)):
            continue
        value = float(raw)
        if not 0.0 <= value <= 1.0:
            raise ValueError("p-values must be between 0 and 1")
        valid.append((index, value))
    ordered = sorted(valid, key=lambda row: row[1])
    adjusted: dict[int, float] = {}
    running = 1.0
    for rank_index in range(len(ordered) - 1, -1, -1):
        original_index, value = ordered[rank_index]
        rank = rank_index + 1
        running = min(running, value * total_hypotheses / rank)
        adjusted[original_index] = min(1.0, running)
    return [adjusted.get(index) for index in range(total_hypotheses)]


def build_family_trial_ledger(
    trials: Sequence[Mapping[str, Any]],
    *,
    q_threshold: float = DEFAULT_Q_THRESHOLD,
) -> list[dict[str, Any]]:
    """Apply BH within each preregistered family and retain every attempted trial."""

    if not 0.0 <= q_threshold <= 1.0:
        raise ValueError("q_threshold must be between 0 and 1")
    ledger = [dict(row) for row in trials]
    by_family: dict[str, list[int]] = {}
    for index, row in enumerate(ledger):
        family = str(row.get("family") or "unknown")
        by_family.setdefault(family, []).append(index)
    for family, indices in by_family.items():
        q_values = benjamini_hochberg([ledger[index].get("p_value") for index in indices])
        for family_index, (index, q_value) in enumerate(zip(indices, q_values), start=1):
            ledger[index].update(
                {
                    "family": family,
                    "family_trial_index": family_index,
                    "family_trial_count": len(indices),
                    "q_value": q_value,
                    "q_threshold": q_threshold,
                    "passes_fdr": q_value is not None and q_value <= q_threshold,
                }
            )
    return ledger


def run_expanded_factor_research(
    frame: pd.DataFrame,
    *,
    forward_return_column: str = "forward_return_5d",
    q_threshold: float = DEFAULT_Q_THRESHOLD,
    non_overlapping_step: int = 5,
) -> ExpandedFactorResearchResult:
    """Run the fixed six-factor, fixed-window, train-direction research protocol."""

    step = _validate_non_overlapping_step(non_overlapping_step)
    definitions = preregistered_long_only_factors()
    computation = compute_preregistered_factors(frame, definitions)
    frozen = freeze_directions_from_train(
        frame,
        computation,
        definitions,
        forward_return_column=forward_return_column,
        non_overlapping_step=step,
    )
    labels = fixed_sample_labels(frame["date"])
    window_metrics: list[dict[str, Any]] = []
    validation_trials: list[dict[str, Any]] = []
    for definition in definitions:
        signal = computation.series_by_name.get(definition.name)
        direction = frozen.get(definition.name)
        if signal is None or direction is None:
            continue
        for window in ("train", "validation", "observed_audit"):
            metric = _window_metric(
                frame,
                signal,
                labels == window,
                multiplier=direction.multiplier,
                forward_return_column=forward_return_column,
                non_overlapping_step=step,
            )
            row = {
                "name": definition.name,
                "family": definition.family,
                "window": window,
                "direction": direction.direction,
                **metric,
            }
            window_metrics.append(row)
            if window == "validation":
                validation_trials.append(row)
    ledger = build_family_trial_ledger(validation_trials, q_threshold=q_threshold)
    return ExpandedFactorResearchResult(
        definitions=definitions,
        computation=computation,
        frozen_directions=frozen,
        window_metrics=window_metrics,
        family_trial_ledger=ledger,
    )
