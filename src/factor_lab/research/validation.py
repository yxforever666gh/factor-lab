"""Train-only factor screening and holdout diagnostics.

Stage-A admission is intentionally a function of the 2017--2022 training
segment only. Validation is consumed by the portfolio gate, while the audit
segment may only falsify an otherwise successful result.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from .contracts import FactorSpec, ValidationSpec
from .signals import BuiltinSignal, evaluate_factor_signal


@dataclass(frozen=True, slots=True)
class BootstrapInterval:
    estimate: float | None
    lower: float | None
    upper: float | None
    confidence: float
    samples: int
    block_size: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "estimate": self.estimate,
            "lower": self.lower,
            "upper": self.upper,
            "confidence": self.confidence,
            "samples": self.samples,
            "block_size": self.block_size,
        }


@dataclass(frozen=True, slots=True)
class WindowDiagnostics:
    split: str
    start: str
    end: str | None
    expected_date_count: int
    evaluable_date_count: int
    evaluable_date_ratio: float
    median_cross_section_coverage: float
    rank_ic_mean: float | None
    rank_ic_std: float | None
    signed_rank_ic_mean: float | None = None
    top_tail_excess_mean: float | None = None
    top_bottom_spread_mean: float | None = None
    decile_monotonicity_mean: float | None = None
    positive_ic_year_ratio: float = 0.0
    positive_tail_year_ratio: float = 0.0
    positive_year_ratio: float = 0.0
    year_count: int = 0
    yearly: tuple[Mapping[str, Any], ...] = ()
    bootstrap: Mapping[str, BootstrapInterval] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "split": self.split,
            "start": self.start,
            "end": self.end,
            "expected_date_count": self.expected_date_count,
            "evaluable_date_count": self.evaluable_date_count,
            "evaluable_date_ratio": self.evaluable_date_ratio,
            "median_cross_section_coverage": self.median_cross_section_coverage,
            "rank_ic_mean": self.rank_ic_mean,
            "rank_ic_std": self.rank_ic_std,
            "signed_rank_ic_mean": self.signed_rank_ic_mean,
            "top_tail_excess_mean": self.top_tail_excess_mean,
            "top_bottom_spread_mean": self.top_bottom_spread_mean,
            "decile_monotonicity_mean": self.decile_monotonicity_mean,
            "positive_ic_year_ratio": self.positive_ic_year_ratio,
            "positive_tail_year_ratio": self.positive_tail_year_ratio,
            "positive_year_ratio": self.positive_year_ratio,
            "year_count": self.year_count,
            "yearly": [dict(row) for row in self.yearly],
            "bootstrap": {
                name: interval.to_dict()
                for name, interval in (self.bootstrap or {}).items()
            },
        }


@dataclass(frozen=True, slots=True)
class FactorValidation:
    factor_name: str
    family: str
    frozen_direction: int
    label_column: str
    train: WindowDiagnostics
    validation: WindowDiagnostics
    audit: WindowDiagnostics
    direction_consistent: bool
    stage_b_eligible: bool
    blockers: tuple[str, ...]
    selection_basis: str = "train_only"
    audit_signal_failures: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "factor_name": self.factor_name,
            "family": self.family,
            "frozen_direction": self.frozen_direction,
            "label_column": self.label_column,
            "train": self.train.to_dict(),
            "validation": self.validation.to_dict(),
            "audit": self.audit.to_dict(),
            "direction_consistent": self.direction_consistent,
            "stage_b_eligible": self.stage_b_eligible,
            "blockers": list(self.blockers),
            "selection_basis": self.selection_basis,
            "audit_signal_failures": list(self.audit_signal_failures),
        }


@dataclass(frozen=True, slots=True)
class SignalSimilarity:
    left: str
    right: str
    date_count: int
    mean_rank_correlation: float | None
    median_rank_correlation: float | None
    homogeneous: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "left": self.left,
            "right": self.right,
            "date_count": self.date_count,
            "mean_rank_correlation": self.mean_rank_correlation,
            "median_rank_correlation": self.median_rank_correlation,
            "homogeneous": self.homogeneous,
        }


@dataclass(frozen=True, slots=True)
class SelectionDecision:
    factor_name: str
    selected: bool
    reason: str
    correlated_with: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "factor_name": self.factor_name,
            "selected": self.selected,
            "reason": self.reason,
            "correlated_with": self.correlated_with,
        }


@dataclass(frozen=True, slots=True)
class StageASelection:
    selected: tuple[FactorValidation, ...]
    decisions: tuple[SelectionDecision, ...]
    similarities: tuple[SignalSimilarity, ...]
    basis: str = "train_only"

    def to_dict(self) -> dict[str, Any]:
        return {
            "basis": self.basis,
            "selected": [row.factor_name for row in self.selected],
            "decisions": [row.to_dict() for row in self.decisions],
            "similarity_threshold_applied": True,
            "similarities": [row.to_dict() for row in self.similarities],
        }


def _round(value: float | None) -> float | None:
    if value is None or not np.isfinite(value):
        return None
    return round(float(value), 8)


def _stable_seed(base_seed: int, key: str) -> int:
    digest = hashlib.sha256(f"{base_seed}:{key}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False)


def deterministic_block_bootstrap_mean(
    values: Sequence[float],
    *,
    samples: int,
    block_size: int,
    confidence: float,
    seed: int,
    key: str,
) -> BootstrapInterval:
    """Circular moving-block bootstrap for a time-ordered mean."""

    array = np.asarray(values, dtype=float)
    array = array[np.isfinite(array)]
    estimate = _round(float(np.mean(array))) if len(array) else None
    effective_block = min(max(1, int(block_size)), max(1, len(array)))
    if len(array) <= 1:
        return BootstrapInterval(
            estimate=estimate,
            lower=estimate,
            upper=estimate,
            confidence=float(confidence),
            samples=int(samples),
            block_size=effective_block,
        )

    generator = np.random.Generator(np.random.PCG64(_stable_seed(seed, key)))
    blocks_needed = math.ceil(len(array) / effective_block)
    offsets = np.arange(effective_block, dtype=int)
    bootstrapped = np.empty(int(samples), dtype=float)
    for sample_index in range(int(samples)):
        starts = generator.integers(0, len(array), size=blocks_needed)
        indices = ((starts[:, None] + offsets[None, :]) % len(array)).ravel()[: len(array)]
        bootstrapped[sample_index] = float(np.mean(array[indices]))
    alpha = (1.0 - float(confidence)) / 2.0
    return BootstrapInterval(
        estimate=estimate,
        lower=_round(float(np.quantile(bootstrapped, alpha, method="linear"))),
        upper=_round(float(np.quantile(bootstrapped, 1.0 - alpha, method="linear"))),
        confidence=float(confidence),
        samples=int(samples),
        block_size=effective_block,
    )


def _exit_dates(
    frame: pd.DataFrame, validation: ValidationSpec
) -> tuple[pd.Series, pd.DatetimeIndex, dict[pd.Timestamp, pd.Timestamp]]:
    dates = pd.to_datetime(frame[validation.date_column], errors="coerce").dt.normalize()
    all_dates = pd.DatetimeIndex(sorted(dates.dropna().unique()))
    fallback = {
        signal_date: all_dates[index + validation.holding_days + 1]
        if index + validation.holding_days + 1 < len(all_dates)
        else pd.NaT
        for index, signal_date in enumerate(all_dates)
    }
    if "label_exit_date" in frame.columns:
        observed_rows = pd.DataFrame(
            {
                "date": dates,
                "exit": pd.to_datetime(
                    frame["label_exit_date"], errors="coerce"
                ).dt.normalize(),
            }
        ).dropna(subset=["date"])
        observed = observed_rows.groupby("date", sort=False)["exit"].max().to_dict()
        fallback.update({key: value for key, value in observed.items() if pd.notna(value)})
    return dates, all_dates, fallback


def _sampled_dates(
    frame: pd.DataFrame,
    validation: ValidationSpec,
    *,
    start: str,
    end: str | None,
) -> tuple[pd.Series, pd.DatetimeIndex]:
    dates, all_dates, exit_by_date = _exit_dates(frame, validation)
    lower = pd.Timestamp(start)
    upper = pd.Timestamp(end) if end is not None else None
    sampled = all_dates[:: validation.holding_days]
    sampled = sampled[sampled >= lower]
    if upper is not None:
        sampled = sampled[sampled <= upper]
        sampled = pd.DatetimeIndex(
            [
                signal_date
                for signal_date in sampled
                if pd.notna(exit_by_date.get(signal_date, pd.NaT))
                and exit_by_date[signal_date] <= upper
            ]
        )
    return dates, sampled


def _tail_count(size: int, validation: ValidationSpec) -> int:
    target = max(
        validation.top_tail_min_count,
        int(math.ceil(size * validation.top_tail_fraction)),
    )
    return min(validation.top_tail_max_count, target, max(1, size // 2))


def _research_visibility_mask(frame: pd.DataFrame) -> pd.Series:
    mask = pd.Series(True, index=frame.index)
    for column in ("eligible", "universe_member"):
        if column not in frame.columns:
            continue
        values = frame[column]
        if values.dtype == bool:
            accepted = values.fillna(False)
        else:
            accepted = values.astype(str).str.strip().str.casefold().isin(
                {"1", "true", "yes", "y"}
            )
        mask &= accepted
    return mask


def _cross_section_metrics(
    group: pd.DataFrame,
    *,
    direction: int,
    validation: ValidationSpec,
) -> dict[str, float] | None:
    finite = group[["ticker", "signal", "label"]].replace(
        [np.inf, -np.inf], np.nan
    ).dropna()
    if (
        len(finite) < validation.min_cross_section
        or finite["signal"].nunique() < 2
        or finite["label"].nunique() < 2
    ):
        return None
    raw_ic = finite["signal"].rank(method="average").corr(
        finite["label"].rank(method="average")
    )
    if pd.isna(raw_ic):
        return None
    finite = finite.assign(directed=finite["signal"] * direction)
    ordered = finite.sort_values(["directed", "ticker"], ascending=[False, True])
    tail = _tail_count(len(ordered), validation)
    top = float(ordered.head(tail)["label"].mean())
    bottom = float(ordered.tail(tail)["label"].mean())
    universe = float(ordered["label"].mean())

    ascending = finite.sort_values(["directed", "ticker"], ascending=[True, True]).copy()
    bucket_count = min(validation.decile_count, len(ascending))
    ascending["bucket"] = np.floor(
        np.arange(len(ascending), dtype=float) * bucket_count / len(ascending)
    ).astype(int)
    bucket_returns = ascending.groupby("bucket", sort=True)["label"].mean()
    monotonicity = pd.Series(bucket_returns.index, dtype=float).corr(
        bucket_returns.reset_index(drop=True), method="spearman"
    )
    return {
        "rank_ic": float(raw_ic),
        "signed_rank_ic": float(raw_ic) * direction,
        "top_tail_excess": top - universe,
        "top_bottom_spread": top - bottom,
        "decile_monotonicity": float(monotonicity) if pd.notna(monotonicity) else np.nan,
    }


def _raw_window_ic(
    frame: pd.DataFrame,
    signal: pd.Series,
    *,
    label_column: str,
    validation: ValidationSpec,
    start: str,
    end: str | None,
) -> float | None:
    dates, sampled = _sampled_dates(
        frame,
        validation,
        start=start,
        end=end,
    )
    rows = pd.DataFrame(
        {
            "date": dates,
            "signal": pd.to_numeric(signal, errors="coerce"),
            "label": pd.to_numeric(frame[label_column], errors="coerce"),
        },
        index=frame.index,
    )
    values: list[float] = []
    visible = _research_visibility_mask(frame)
    for _, group in rows.loc[dates.isin(sampled) & visible].groupby("date", sort=True):
        finite = group[["signal", "label"]].replace(
            [np.inf, -np.inf], np.nan
        ).dropna()
        if (
            len(finite) < validation.min_cross_section
            or finite["signal"].nunique() < 2
            or finite["label"].nunique() < 2
        ):
            continue
        correlation = finite["signal"].rank(method="average").corr(
            finite["label"].rank(method="average")
        )
        if pd.notna(correlation):
            values.append(float(correlation))
    return float(np.mean(values)) if values else None


def _diagnose_window(
    frame: pd.DataFrame,
    signal: pd.Series,
    *,
    factor_name: str,
    split: str,
    start: str,
    end: str | None,
    label_column: str,
    validation: ValidationSpec,
    direction: int,
) -> WindowDiagnostics:
    dates, sampled_dates = _sampled_dates(frame, validation, start=start, end=end)
    rows = pd.DataFrame(
        {
            "date": dates,
            "ticker": frame[validation.ticker_column].astype(str),
            "signal": pd.to_numeric(signal, errors="coerce"),
            "label": pd.to_numeric(frame[label_column], errors="coerce"),
        },
        index=frame.index,
    ).loc[dates.isin(sampled_dates) & _research_visibility_mask(frame)]

    coverages: list[float] = []
    daily: list[dict[str, Any]] = []
    for date, group in rows.groupby("date", sort=True):
        finite_count = len(
            group[["signal", "label"]].replace([np.inf, -np.inf], np.nan).dropna()
        )
        coverages.append(float(finite_count / len(group)) if len(group) else 0.0)
        metrics = _cross_section_metrics(group, direction=direction, validation=validation)
        if metrics is not None:
            daily.append({"date": pd.Timestamp(date), **metrics})

    daily_frame = pd.DataFrame(daily)
    yearly: list[Mapping[str, Any]] = []
    if not daily_frame.empty:
        for year, group in daily_frame.groupby(daily_frame["date"].dt.year, sort=True):
            yearly.append(
                {
                    "year": int(year),
                    "date_count": int(len(group)),
                    "signed_rank_ic_mean": _round(float(group["signed_rank_ic"].mean())),
                    "top_tail_excess_mean": _round(float(group["top_tail_excess"].mean())),
                    "decile_monotonicity_mean": _round(
                        float(group["decile_monotonicity"].mean())
                    ),
                }
            )
    positive_ic_year_ratio = (
        float(np.mean([float(row["signed_rank_ic_mean"] or 0.0) > 0.0 for row in yearly]))
        if yearly
        else 0.0
    )
    positive_tail_year_ratio = (
        float(np.mean([float(row["top_tail_excess_mean"] or 0.0) > 0.0 for row in yearly]))
        if yearly
        else 0.0
    )
    positive_year_ratio = (
        float(
            np.mean(
                [
                    float(row["signed_rank_ic_mean"] or 0.0) > 0.0
                    and float(row["top_tail_excess_mean"] or 0.0) > 0.0
                    for row in yearly
                ]
            )
        )
        if yearly
        else 0.0
    )

    metric_names = (
        "signed_rank_ic",
        "top_tail_excess",
        "top_bottom_spread",
        "decile_monotonicity",
    )
    intervals = {
        metric: deterministic_block_bootstrap_mean(
            daily_frame[metric].tolist() if metric in daily_frame else [],
            samples=validation.bootstrap_samples,
            block_size=validation.bootstrap_block_size,
            confidence=validation.bootstrap_confidence,
            seed=validation.bootstrap_seed,
            # Common random blocks make semantically identical signals
            # invariant to display-name changes and improve paired comparison.
            key=f"factor_diagnostic:{split}:{metric}",
        )
        for metric in metric_names
    }
    return WindowDiagnostics(
        split=split,
        start=start,
        end=end,
        expected_date_count=len(sampled_dates),
        evaluable_date_count=len(daily_frame),
        evaluable_date_ratio=round(len(daily_frame) / len(sampled_dates), 8)
        if len(sampled_dates)
        else 0.0,
        median_cross_section_coverage=round(float(np.median(coverages)), 8)
        if coverages
        else 0.0,
        rank_ic_mean=_round(float(daily_frame["rank_ic"].mean()))
        if not daily_frame.empty
        else None,
        rank_ic_std=_round(float(daily_frame["rank_ic"].std(ddof=0)))
        if not daily_frame.empty
        else None,
        signed_rank_ic_mean=_round(float(daily_frame["signed_rank_ic"].mean()))
        if not daily_frame.empty
        else None,
        top_tail_excess_mean=_round(float(daily_frame["top_tail_excess"].mean()))
        if not daily_frame.empty
        else None,
        top_bottom_spread_mean=_round(float(daily_frame["top_bottom_spread"].mean()))
        if not daily_frame.empty
        else None,
        decile_monotonicity_mean=_round(float(daily_frame["decile_monotonicity"].mean()))
        if not daily_frame.empty
        else None,
        positive_ic_year_ratio=round(positive_ic_year_ratio, 8),
        positive_tail_year_ratio=round(positive_tail_year_ratio, 8),
        positive_year_ratio=round(positive_year_ratio, 8),
        year_count=len(yearly),
        yearly=tuple(yearly),
        bootstrap=intervals,
    )


def _choose_label(frame: pd.DataFrame, validation: ValidationSpec) -> str:
    label = next((name for name in validation.label_columns if name in frame.columns), None)
    if label is None:
        raise ValueError(
            "missing diagnostic label; expected one of " + ", ".join(validation.label_columns)
        )
    return label


def evaluate_stage_a(
    frame: pd.DataFrame,
    factor: FactorSpec,
    validation: ValidationSpec | None = None,
    *,
    signal: pd.Series | None = None,
    aliases: Mapping[str, pd.Series] | None = None,
    builtins: Mapping[str, BuiltinSignal] | None = None,
) -> FactorValidation:
    """Evaluate diagnostics while making shortlist eligibility train-only."""

    policy = validation or ValidationSpec()
    for column in (policy.date_column, policy.ticker_column):
        if column not in frame.columns:
            raise ValueError(f"missing research column: {column}")
    label_column = _choose_label(frame, policy)
    if signal is None:
        signal = evaluate_factor_signal(
            frame,
            factor,
            date_column=policy.date_column,
            aliases=aliases,
            builtins=builtins,
        )
    elif len(signal) != len(frame):
        raise ValueError("signal length does not match research frame")
    else:
        signal = pd.Series(np.asarray(signal), index=frame.index, name=factor.name)

    direction_start = (
        pd.to_datetime(frame[policy.date_column], errors="coerce").min().date().isoformat()
        if factor.direction_policy == "all_history_ic"
        else policy.train_start
    )
    direction_end = None if factor.direction_policy == "all_history_ic" else policy.train_end
    raw_direction_ic = _raw_window_ic(
        frame,
        signal,
        label_column=label_column,
        validation=policy,
        start=direction_start,
        end=direction_end,
    )
    frozen_direction = (
        1
        if factor.direction_policy == "pre_directed"
        else 1
        if (raw_direction_ic or 0.0) >= 0.0
        else -1
    )
    train = _diagnose_window(
        frame,
        signal,
        factor_name=factor.name,
        split="train",
        start=policy.train_start,
        end=policy.train_end,
        label_column=label_column,
        validation=policy,
        direction=frozen_direction,
    )
    validation_window = _diagnose_window(
        frame,
        signal,
        factor_name=factor.name,
        split="validation",
        start=policy.validation_start,
        end=policy.validation_end,
        label_column=label_column,
        validation=policy,
        direction=frozen_direction,
    )
    audit = _diagnose_window(
        frame,
        signal,
        factor_name=factor.name,
        split="audit",
        start=policy.audit_start,
        end=None,
        label_column=label_column,
        validation=policy,
        direction=frozen_direction,
    )

    blockers: list[str] = []
    if train.rank_ic_mean is None:
        blockers.append("missing_train_rank_ic")
    if train.evaluable_date_ratio < policy.min_evaluable_ratio:
        blockers.append("train_evaluable_date_ratio_below_threshold")
    if train.median_cross_section_coverage < policy.min_median_coverage:
        blockers.append("train_cross_section_coverage_below_threshold")
    if (train.top_tail_excess_mean or 0.0) <= policy.min_train_top_tail_excess:
        blockers.append("train_top_tail_excess_not_positive")
    if (train.decile_monotonicity_mean or 0.0) <= policy.min_train_decile_monotonicity:
        blockers.append("train_decile_monotonicity_not_positive")
    if train.positive_year_ratio < policy.min_train_positive_year_ratio:
        blockers.append("train_positive_year_ratio_below_threshold")

    direction_consistent = bool(
        validation_window.signed_rank_ic_mean is not None
        and validation_window.signed_rank_ic_mean > 0.0
    )
    audit_failures: list[str] = []
    if audit.evaluable_date_count >= policy.audit_min_observations:
        audit_intervals = audit.bootstrap or {}
        rank_ic_interval = audit_intervals.get("signed_rank_ic")
        tail_interval = audit_intervals.get("top_tail_excess")
        if (
            rank_ic_interval is not None
            and rank_ic_interval.upper is not None
            and rank_ic_interval.upper < 0.0
        ):
            audit_failures.append("audit_signed_rank_ic_bootstrap_upper_negative")
        if (
            tail_interval is not None
            and tail_interval.upper is not None
            and tail_interval.upper < 0.0
        ):
            audit_failures.append("audit_top_tail_bootstrap_upper_negative")

    return FactorValidation(
        factor_name=factor.name,
        family=factor.family,
        frozen_direction=frozen_direction,
        label_column=label_column,
        train=train,
        validation=validation_window,
        audit=audit,
        direction_consistent=direction_consistent,
        stage_b_eligible=not blockers,
        blockers=tuple(blockers),
        selection_basis=(
            "pre_directed_components"
            if factor.direction_policy == "pre_directed"
            else "all_observed_history_direction"
            if factor.direction_policy == "all_history_ic"
            else "train_only"
        ),
        audit_signal_failures=tuple(audit_failures),
    )


def diagnose_train_similarity(
    frame: pd.DataFrame,
    signals: Mapping[str, pd.Series],
    results: Iterable[FactorValidation],
    validation: ValidationSpec | None = None,
) -> list[SignalSimilarity]:
    """Measure train-only daily cross-sectional signal rank correlation."""

    policy = validation or ValidationSpec()
    result_map = {row.factor_name: row for row in results}
    names = sorted(set(signals) & set(result_map))
    dates, sampled = _sampled_dates(
        frame,
        policy,
        start=policy.train_start,
        end=policy.train_end,
    )
    rows = pd.DataFrame(
        {
            "date": dates,
            **{
                name: pd.to_numeric(signals[name], errors="coerce")
                * result_map[name].frozen_direction
                for name in names
            },
        },
        index=frame.index,
    ).loc[dates.isin(sampled) & _research_visibility_mask(frame)]
    pairs = [
        (left, right)
        for left_index, left in enumerate(names)
        for right in names[left_index + 1 :]
    ]
    values: dict[tuple[str, str], list[float]] = {pair: [] for pair in pairs}
    for _, group in rows.groupby("date", sort=True):
        correlations = group[names].replace([np.inf, -np.inf], np.nan).corr(
            method="spearman", min_periods=policy.min_cross_section
        )
        for left, right in pairs:
            correlation = correlations.loc[left, right]
            if pd.notna(correlation):
                values[(left, right)].append(float(correlation))

    output: list[SignalSimilarity] = []
    for left, right in pairs:
        correlations = values[(left, right)]
        mean = float(np.mean(correlations)) if correlations else None
        output.append(
            SignalSimilarity(
                left=left,
                right=right,
                date_count=len(correlations),
                mean_rank_correlation=_round(mean),
                median_rank_correlation=_round(float(np.median(correlations)))
                if correlations
                else None,
                homogeneous=bool(
                    mean is not None and abs(mean) >= policy.similarity_threshold
                ),
            )
        )
    return output


def _selection_key(row: FactorValidation) -> tuple[Any, ...]:
    tail_interval = (row.train.bootstrap or {}).get("top_tail_excess")
    return (
        -(tail_interval.lower if tail_interval and tail_interval.lower is not None else -np.inf),
        -(row.train.top_tail_excess_mean or -np.inf),
        -(row.train.decile_monotonicity_mean or -np.inf),
        -(row.train.signed_rank_ic_mean or -np.inf),
        -row.train.positive_year_ratio,
        -row.train.median_cross_section_coverage,
        row.factor_name,
    )


def build_stage_a_selection(
    results: Iterable[FactorValidation],
    similarities: Iterable[SignalSimilarity],
    validation: ValidationSpec | None = None,
    *,
    excluded_names: Iterable[str] = (),
) -> StageASelection:
    """Greedily keep train-ranked, non-homogeneous challengers."""

    policy = validation or ValidationSpec()
    excluded = set(excluded_names)
    pairs = tuple(similarities)
    homogeneous = {
        frozenset((row.left, row.right)): row.homogeneous for row in pairs
    }
    ranked = sorted(
        (row for row in results if row.factor_name not in excluded),
        key=_selection_key,
    )
    selected: list[FactorValidation] = []
    decisions: list[SelectionDecision] = []
    references = sorted(excluded)
    for row in ranked:
        if not row.stage_b_eligible:
            decisions.append(SelectionDecision(row.factor_name, False, "train_gate_blocked"))
            continue
        correlated = next(
            (
                name
                for name in [*references, *(item.factor_name for item in selected)]
                if homogeneous.get(frozenset((row.factor_name, name)), False)
            ),
            None,
        )
        if correlated is not None:
            decisions.append(
                SelectionDecision(
                    row.factor_name,
                    False,
                    "train_signal_homogeneous",
                    correlated,
                )
            )
            continue
        if len(selected) >= policy.max_challengers:
            decisions.append(SelectionDecision(row.factor_name, False, "stage_b_capacity"))
            continue
        selected.append(row)
        decisions.append(SelectionDecision(row.factor_name, True, "selected"))
    return StageASelection(tuple(selected), tuple(decisions), pairs)


def select_stage_b(
    results: Iterable[FactorValidation],
    validation: ValidationSpec | None = None,
    *,
    excluded_names: Iterable[str] = (),
    similarities: Iterable[SignalSimilarity] = (),
) -> list[FactorValidation]:
    """Compatibility wrapper returning only selected training results."""

    return list(
        build_stage_a_selection(
            results,
            similarities,
            validation,
            excluded_names=excluded_names,
        ).selected
    )


__all__ = [
    "BootstrapInterval",
    "FactorValidation",
    "SelectionDecision",
    "SignalSimilarity",
    "StageASelection",
    "WindowDiagnostics",
    "build_stage_a_selection",
    "deterministic_block_bootstrap_mean",
    "diagnose_train_similarity",
    "evaluate_stage_a",
    "select_stage_b",
]
