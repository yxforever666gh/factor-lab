"""Non-overlapping Rank-IC diagnostics and deterministic Stage-B selection."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from .contracts import FactorSpec, ValidationSpec
from .signals import BuiltinSignal, evaluate_factor_signal


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
        }


def _round(value: float | None) -> float | None:
    if value is None or not np.isfinite(value):
        return None
    return round(float(value), 8)


def _diagnose_window(
    frame: pd.DataFrame,
    signal: pd.Series,
    *,
    split: str,
    start: str,
    end: str | None,
    label_column: str,
    validation: ValidationSpec,
    direction: int | None = None,
) -> WindowDiagnostics:
    dates = pd.to_datetime(frame[validation.date_column], errors="coerce").dt.normalize()
    lower = pd.Timestamp(start)
    upper = pd.Timestamp(end) if end is not None else None
    all_dates = pd.DatetimeIndex(sorted(dates.dropna().unique()))
    # The sampling anchor is global and therefore identical for train,
    # validation, audit, and the portfolio engine.  Restarting [::5] inside
    # each split silently evaluates different weekdays in Stage A and B.
    sampled_dates = all_dates[:: validation.holding_days]
    sampled_dates = sampled_dates[sampled_dates >= lower]
    if upper is not None:
        sampled_dates = sampled_dates[sampled_dates <= upper]

    fallback_exit_by_date = {
        signal_date: all_dates[index + validation.holding_days + 1]
        if index + validation.holding_days + 1 < len(all_dates)
        else pd.NaT
        for index, signal_date in enumerate(all_dates)
    }
    if "label_exit_date" in frame.columns:
        exit_rows = pd.DataFrame(
            {
                "date": dates,
                "exit": pd.to_datetime(frame["label_exit_date"], errors="coerce").dt.normalize(),
            }
        ).dropna(subset=["date"])
        observed_exit_by_date = exit_rows.groupby("date", sort=False)["exit"].max().to_dict()
    else:
        observed_exit_by_date = {}

    def exit_date(signal_date: pd.Timestamp) -> pd.Timestamp | pd.NaT:
        observed = observed_exit_by_date.get(signal_date)
        return observed if pd.notna(observed) else fallback_exit_by_date.get(signal_date, pd.NaT)

    # A finite split may only use labels whose exit is also inside it.  This
    # keeps late-December validation returns from leaking January audit data.
    if upper is not None:
        sampled_dates = pd.DatetimeIndex(
            [
                signal_date
                for signal_date in sampled_dates
                if pd.notna(exit_date(signal_date)) and exit_date(signal_date) <= upper
            ]
        )
    mask = dates.isin(sampled_dates)

    rows = pd.DataFrame(
        {
            "date": dates,
            "signal": pd.to_numeric(signal, errors="coerce"),
            "label": pd.to_numeric(frame[label_column], errors="coerce"),
        },
        index=frame.index,
    )
    rows = rows.loc[mask]
    correlations: list[float] = []
    coverages: list[float] = []
    for _, group in rows.groupby("date", sort=True):
        finite = group[["signal", "label"]].replace([np.inf, -np.inf], np.nan).dropna()
        coverages.append(float(len(finite) / len(group)) if len(group) else 0.0)
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
            correlations.append(float(correlation))

    expected = len(sampled_dates)
    mean = float(np.mean(correlations)) if correlations else None
    return WindowDiagnostics(
        split=split,
        start=start,
        end=end,
        expected_date_count=expected,
        evaluable_date_count=len(correlations),
        evaluable_date_ratio=round(len(correlations) / expected, 8) if expected else 0.0,
        median_cross_section_coverage=round(float(np.median(coverages)), 8)
        if coverages
        else 0.0,
        rank_ic_mean=_round(mean),
        rank_ic_std=_round(float(np.std(correlations))) if correlations else None,
        signed_rank_ic_mean=_round(mean * direction)
        if mean is not None and direction is not None
        else None,
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
    """Evaluate train/validation/audit IC without overlapping 5-day labels."""

    policy = validation or ValidationSpec()
    if policy.date_column not in frame.columns:
        raise ValueError(f"missing date column: {policy.date_column}")
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

    train = _diagnose_window(
        frame,
        signal,
        split="train",
        start=policy.train_start,
        end=policy.train_end,
        label_column=label_column,
        validation=policy,
    )
    frozen_direction = 1 if (train.rank_ic_mean or 0.0) >= 0.0 else -1
    train = replace(
        train,
        signed_rank_ic_mean=_round(train.rank_ic_mean * frozen_direction)
        if train.rank_ic_mean is not None
        else None,
    )
    validation_window = _diagnose_window(
        frame,
        signal,
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
    direction_consistent = bool(
        validation_window.signed_rank_ic_mean is not None
        and validation_window.signed_rank_ic_mean > 0.0
    )
    if not direction_consistent:
        blockers.append("validation_direction_inconsistent")
    if validation_window.evaluable_date_ratio < policy.min_evaluable_ratio:
        blockers.append("validation_evaluable_date_ratio_below_threshold")
    if validation_window.median_cross_section_coverage < policy.min_median_coverage:
        blockers.append("validation_cross_section_coverage_below_threshold")

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
    )


def select_stage_b(
    results: Iterable[FactorValidation],
    validation: ValidationSpec | None = None,
    *,
    excluded_names: Iterable[str] = (),
) -> list[FactorValidation]:
    """Select at most three eligible challengers with deterministic ties."""

    policy = validation or ValidationSpec()
    excluded = set(excluded_names)
    eligible = [
        row
        for row in results
        if row.stage_b_eligible and row.factor_name not in excluded
    ]
    eligible.sort(
        key=lambda row: (
            -(row.validation.signed_rank_ic_mean or float("-inf")),
            -row.validation.median_cross_section_coverage,
            row.factor_name,
        )
    )
    return eligible[: policy.max_challengers]
