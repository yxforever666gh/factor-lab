"""Purged, embargoed expanding nested walk-forward validation splits."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class WalkForwardFold:
    fold_id: str
    train_positions: tuple[int, ...]
    test_positions: tuple[int, ...]
    purged_positions: tuple[int, ...]
    embargo_positions: tuple[int, ...]
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp
    promotion_eligible: bool = True

    def assert_no_label_overlap(self, *, label_horizon_sessions: int) -> None:
        if not self.train_positions or not self.test_positions:
            raise ValueError(f"fold {self.fold_id!r} has an empty train or test window")
        if set(self.train_positions).intersection(self.test_positions):
            raise ValueError(f"fold {self.fold_id!r} has overlapping train and test positions")
        separation = min(self.test_positions) - max(self.train_positions) - 1
        if separation < label_horizon_sessions:
            raise ValueError(
                f"fold {self.fold_id!r} has only {separation} purged sessions; "
                f"label horizon requires {label_horizon_sessions}"
            )
        if set(self.embargo_positions).intersection(self.train_positions) or set(self.embargo_positions).intersection(self.test_positions):
            raise ValueError(f"fold {self.fold_id!r} embargo overlaps train/test")


@dataclass(frozen=True)
class NestedOuterFold:
    outer: WalkForwardFold
    inner: tuple[WalkForwardFold, ...]


@dataclass(frozen=True)
class NestedWalkForwardPlan:
    sessions: tuple[pd.Timestamp, ...]
    outer_folds: tuple[NestedOuterFold, ...]
    diagnostic_folds: tuple[WalkForwardFold, ...]
    purge_sessions: int
    embargo_sessions: int


@dataclass(frozen=True)
class StitchedOuterOOS:
    returns: pd.Series
    fold_ids: tuple[str, ...]
    observation_counts: Mapping[str, int]


def _value(source: Mapping[str, Any] | Any, name: str, default: Any) -> Any:
    if isinstance(source, Mapping):
        return source.get(name, default)
    return getattr(source, name, default)


def _normalise_sessions(sessions: Iterable[Any]) -> pd.DatetimeIndex:
    values = pd.DatetimeIndex(pd.to_datetime(list(sessions), errors="raise")).tz_localize(None).normalize()
    if values.empty:
        raise ValueError("sessions must not be empty")
    if values.has_duplicates:
        raise ValueError("sessions must be unique")
    if not values.is_monotonic_increasing:
        raise ValueError("sessions must be monotonically increasing")
    return values


def _year_positions(sessions: pd.DatetimeIndex, year: int) -> np.ndarray:
    return np.flatnonzero(sessions.year == int(year))


def _make_expanding_fold(
    sessions: pd.DatetimeIndex,
    *,
    fold_id: str,
    train_start: pd.Timestamp,
    test_positions: Sequence[int],
    purge_sessions: int,
    embargo_sessions: int,
    promotion_eligible: bool,
    excluded_positions: Iterable[int] = (),
) -> WalkForwardFold:
    test = np.asarray(test_positions, dtype=int)
    if not len(test):
        raise ValueError(f"fold {fold_id!r} has no test sessions")
    first_test, last_test = int(test.min()), int(test.max())
    excluded = set(int(item) for item in excluded_positions)
    candidate_train = np.asarray(
        [
            int(item)
            for item in np.flatnonzero((sessions >= train_start) & (np.arange(len(sessions)) < first_test))
            if int(item) not in excluded
        ],
        dtype=int,
    )
    if len(candidate_train) <= purge_sessions:
        raise ValueError(f"fold {fold_id!r} has insufficient training sessions after purge")
    if purge_sessions:
        purged = candidate_train[-purge_sessions:]
        train = candidate_train[:-purge_sessions]
    else:
        purged = np.asarray([], dtype=int)
        train = candidate_train
    embargo_end = min(len(sessions), last_test + 1 + embargo_sessions)
    embargo = np.arange(last_test + 1, embargo_end, dtype=int)
    return WalkForwardFold(
        fold_id=fold_id,
        train_positions=tuple(int(item) for item in train),
        test_positions=tuple(int(item) for item in test),
        purged_positions=tuple(int(item) for item in purged),
        embargo_positions=tuple(int(item) for item in embargo),
        train_start=sessions[int(train.min())],
        train_end=sessions[int(train.max())],
        test_start=sessions[first_test],
        test_end=sessions[last_test],
        promotion_eligible=promotion_eligible,
    )


def build_nested_walk_forward_plan(
    sessions: Iterable[Any],
    protocol: Mapping[str, Any] | Any,
    *,
    minimum_inner_train_years: int = 2,
    require_all_outer_years: bool = True,
) -> NestedWalkForwardPlan:
    """Build expanding outer tests with expanding inner model-selection folds.

    The final ``purge_sessions`` observations before every test are excluded
    from training so forward labels cannot cross the boundary.  The first
    ``embargo_sessions`` after a test are explicitly reserved and exposed to
    callers; they must not be used as training observations for that fold.
    """

    index = _normalise_sessions(sessions)
    train_start = pd.Timestamp(_value(protocol, "initial_train_start", "2017-01-01")).normalize()
    initial_train_end = pd.Timestamp(_value(protocol, "initial_train_end", "2020-12-31")).normalize()
    outer_years = tuple(int(item) for item in _value(protocol, "outer_test_years", (2021, 2022, 2023, 2024, 2025)))
    diagnostic_years = tuple(int(item) for item in _value(protocol, "diagnostic_years", (2026,)))
    purge_sessions = int(_value(protocol, "purge_sessions", 6))
    embargo_sessions = int(_value(protocol, "embargo_sessions", 5))
    if purge_sessions < 0 or embargo_sessions < 0:
        raise ValueError("purge_sessions and embargo_sessions must be non-negative")
    if minimum_inner_train_years < 1:
        raise ValueError("minimum_inner_train_years must be positive")
    if train_start > initial_train_end:
        raise ValueError("initial_train_start must not exceed initial_train_end")
    if not ((index >= train_start) & (index <= initial_train_end)).any():
        raise ValueError("initial training window has no sessions")

    outer: list[NestedOuterFold] = []
    prior_outer_embargo: set[int] = set()
    for year in outer_years:
        test_positions = np.asarray(
            [int(item) for item in _year_positions(index, year) if int(item) not in prior_outer_embargo],
            dtype=int,
        )
        if not len(test_positions):
            if require_all_outer_years:
                raise ValueError(f"required outer test year {year} has no sessions")
            continue
        if index[int(test_positions.min())] <= initial_train_end:
            raise ValueError(f"outer test year {year} overlaps initial training window")
        outer_fold = _make_expanding_fold(
            index,
            fold_id=f"outer-{year}",
            train_start=train_start,
            test_positions=test_positions,
            purge_sessions=purge_sessions,
            embargo_sessions=embargo_sessions,
            promotion_eligible=True,
            excluded_positions=prior_outer_embargo,
        )
        outer_fold.assert_no_label_overlap(label_horizon_sessions=purge_sessions)

        available_inner_years = sorted(
            year_value
            for year_value in set(index.year)
            if year_value < year and year_value >= train_start.year + minimum_inner_train_years
        )
        inner_folds: list[WalkForwardFold] = []
        outer_train_set = set(outer_fold.train_positions)
        prior_inner_embargo: set[int] = set()
        for inner_year in available_inner_years:
            inner_test = [
                int(item)
                for item in _year_positions(index, inner_year)
                if int(item) in outer_train_set and int(item) not in prior_inner_embargo
            ]
            if not inner_test:
                continue
            fold = _make_expanding_fold(
                index,
                fold_id=f"outer-{year}/inner-{inner_year}",
                train_start=train_start,
                test_positions=inner_test,
                purge_sessions=purge_sessions,
                embargo_sessions=embargo_sessions,
                promotion_eligible=False,
                excluded_positions=prior_inner_embargo,
            )
            fold.assert_no_label_overlap(label_horizon_sessions=purge_sessions)
            inner_folds.append(fold)
            prior_inner_embargo = set(fold.embargo_positions)
        if not inner_folds:
            raise ValueError(f"outer fold {year} has no valid inner folds")
        outer.append(NestedOuterFold(outer=outer_fold, inner=tuple(inner_folds)))
        prior_outer_embargo = set(outer_fold.embargo_positions)

    diagnostics: list[WalkForwardFold] = []
    for year in diagnostic_years:
        positions = np.asarray(
            [int(item) for item in _year_positions(index, year) if int(item) not in prior_outer_embargo],
            dtype=int,
        )
        if not len(positions):
            continue
        fold = _make_expanding_fold(
            index,
            fold_id=f"diagnostic-{year}",
            train_start=train_start,
            test_positions=positions,
            purge_sessions=purge_sessions,
            embargo_sessions=embargo_sessions,
            promotion_eligible=False,
            excluded_positions=prior_outer_embargo,
        )
        fold.assert_no_label_overlap(label_horizon_sessions=purge_sessions)
        diagnostics.append(fold)
        prior_outer_embargo = set(fold.embargo_positions)

    return NestedWalkForwardPlan(
        sessions=tuple(pd.Timestamp(item) for item in index),
        outer_folds=tuple(outer),
        diagnostic_folds=tuple(diagnostics),
        purge_sessions=purge_sessions,
        embargo_sessions=embargo_sessions,
    )


def select_fold_rows(frame: pd.DataFrame, fold: WalkForwardFold, *, time_column: str = "date") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return train/test rows for a fold, including every security per session."""

    if time_column not in frame.columns:
        raise KeyError(time_column)
    row_sessions = pd.to_datetime(frame[time_column], errors="raise").dt.tz_localize(None).dt.normalize()
    train = frame.loc[row_sessions.between(fold.train_start, fold.train_end, inclusive="both")].copy()
    test = frame.loc[row_sessions.between(fold.test_start, fold.test_end, inclusive="both")].copy()
    return train, test


def stitch_outer_oos_returns(
    plan: NestedWalkForwardPlan,
    fold_returns: Mapping[str, pd.Series],
) -> StitchedOuterOOS:
    """Validate and concatenate each promotion-eligible outer return stream."""

    expected_folds = tuple(item.outer.fold_id for item in plan.outer_folds)
    if set(fold_returns) != set(expected_folds):
        missing = sorted(set(expected_folds).difference(fold_returns))
        extra = sorted(set(fold_returns).difference(expected_folds))
        raise ValueError(f"outer OOS fold mismatch; missing={missing}, extra={extra}")
    pieces: list[pd.Series] = []
    counts: dict[str, int] = {}
    seen_timestamps: set[pd.Timestamp] = set()
    for nested in plan.outer_folds:
        fold = nested.outer
        values = fold_returns[fold.fold_id]
        if not isinstance(values, pd.Series):
            raise TypeError(f"{fold.fold_id} returns must be a pandas Series")
        if values.empty or values.isna().any():
            raise ValueError(f"{fold.fold_id} returns must be non-empty and finite")
        numeric = pd.to_numeric(values, errors="coerce")
        if numeric.isna().any() or not np.isfinite(numeric.to_numpy(dtype=float)).all():
            raise ValueError(f"{fold.fold_id} returns must be numeric and finite")
        timestamps = pd.DatetimeIndex(pd.to_datetime(values.index, errors="raise")).tz_localize(None)
        if timestamps.has_duplicates or not timestamps.is_monotonic_increasing:
            raise ValueError(f"{fold.fold_id} return timestamps must be unique and increasing")
        normalized = timestamps.normalize()
        if (normalized < fold.test_start).any() or (normalized > fold.test_end).any():
            raise ValueError(f"{fold.fold_id} contains returns outside its outer test window")
        overlap = seen_timestamps.intersection(pd.Timestamp(item) for item in timestamps)
        if overlap:
            raise ValueError("outer OOS return timestamps overlap across folds")
        seen_timestamps.update(pd.Timestamp(item) for item in timestamps)
        piece = pd.Series(numeric.to_numpy(dtype=float), index=timestamps, name="net_excess_return")
        pieces.append(piece)
        counts[fold.fold_id] = len(piece)
    stitched = pd.concat(pieces)
    if not stitched.index.is_monotonic_increasing:
        raise ValueError("stitched outer OOS return stream is not chronological")
    return StitchedOuterOOS(returns=stitched, fold_ids=expected_folds, observation_counts=counts)
