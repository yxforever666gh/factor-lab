"""Deterministic fixed-core and low-churn target construction.

The module contains no execution, accounting, I/O, or mutable global state.
It turns a canonical point-in-time cross-section into the fixed-core score and
then applies one independent retention state per absolute calendar sleeve.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


REQUIRED_SIGNAL_COLUMNS = (
    "date",
    "ticker",
    "eligible",
    "universe_member",
    "earnings_yield",
    "pb",
    "book_yield",
    "volatility_20",
)


@dataclass(frozen=True)
class LowChurnStrategyConfig:
    """Small immutable configuration for the 6.0 low-churn route."""

    defensive_weight: float = 0.70
    position_count: int = 10
    retention_exit_rank: int = 25
    sleeve_count: int = 10
    position_weight: float = 0.10
    anchor_date: str = "2017-01-03"

    def __post_init__(self) -> None:
        if not 0.0 <= float(self.defensive_weight) <= 1.0:
            raise ValueError("defensive_weight must be between zero and one")
        if int(self.position_count) != self.position_count or self.position_count <= 0:
            raise ValueError("position_count must be a positive integer")
        if (
            int(self.retention_exit_rank) != self.retention_exit_rank
            or self.retention_exit_rank < self.position_count
        ):
            raise ValueError(
                "retention_exit_rank must be an integer at least position_count"
            )
        if int(self.sleeve_count) != self.sleeve_count or self.sleeve_count <= 0:
            raise ValueError("sleeve_count must be a positive integer")
        if not 0.0 < float(self.position_weight) <= 1.0 / self.position_count:
            raise ValueError(
                "position_weight must be positive and no larger than 1 / position_count"
            )
        pd.Timestamp(self.anchor_date)

    @property
    def retention_buffer(self) -> int:
        return int(self.retention_exit_rank - self.position_count)

    def with_retention_exit_rank(self, value: int) -> "LowChurnStrategyConfig":
        """Return a copy for an otherwise identical retention comparator."""

        return replace(self, retention_exit_rank=value)


def _bool_series(values: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(values.dtype):
        return values.fillna(False).astype(bool)
    if pd.api.types.is_numeric_dtype(values.dtype):
        return pd.to_numeric(values, errors="coerce").fillna(0).ne(0)
    return (
        values.astype("string")
        .str.strip()
        .str.casefold()
        .isin({"1", "true", "yes", "y", "on"})
    )


def _normalized_timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(None)
    return timestamp.normalize()


def fixed_core_score(
    frame: pd.DataFrame,
    config: LowChurnStrategyConfig | None = None,
) -> pd.Series:
    """Compute the causal fixed-core score over each investable cross-section.

    Ranks use pandas' average percentile rank.  Only rows with both canonical
    eligibility flags participate.  Rows outside that common investable set,
    or rows without a finite control score, receive ``NaN``.  A missing
    defensive component falls back to the control rank for that row.
    """

    cfg = config or LowChurnStrategyConfig()
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("frame must be a pandas DataFrame")
    missing = sorted(set(REQUIRED_SIGNAL_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"signal frame missing required columns: {missing}")
    if frame.empty:
        return pd.Series(index=frame.index, dtype=float, name="fixed_core_score")

    work = frame.loc[:, REQUIRED_SIGNAL_COLUMNS].reset_index(drop=True).copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
    if work["date"].isna().any():
        raise ValueError("signal frame contains invalid dates")
    work["ticker"] = work["ticker"].astype(str)
    if work.duplicated(["date", "ticker"]).any():
        raise ValueError("signal frame contains duplicate date/ticker rows")

    investable = _bool_series(work["eligible"]) & _bool_series(
        work["universe_member"]
    )
    active = work.loc[investable].copy()
    output = np.full(len(work), np.nan, dtype=float)
    if active.empty:
        return pd.Series(output, index=frame.index, name="fixed_core_score")

    numeric = active[
        ["earnings_yield", "pb", "book_yield", "volatility_20"]
    ].apply(pd.to_numeric, errors="coerce")
    dates = active["date"]
    control_raw = (numeric["earnings_yield"] / numeric["pb"]).replace(
        [np.inf, -np.inf], np.nan
    )
    control_rank = control_raw.groupby(dates, sort=False).rank(
        method="average", pct=True
    )
    defensive_raw = pd.DataFrame(
        {
            "book": numeric["book_yield"]
            .groupby(dates, sort=False)
            .rank(method="average", pct=True),
            "earnings": numeric["earnings_yield"]
            .groupby(dates, sort=False)
            .rank(method="average", pct=True),
            "low_volatility": (-numeric["volatility_20"])
            .groupby(dates, sort=False)
            .rank(method="average", pct=True),
        }
    ).sum(axis=1, min_count=3)
    defensive_rank = defensive_raw.groupby(dates, sort=False).rank(
        method="average", pct=True
    )
    effective_defensive = defensive_rank.where(
        defensive_rank.notna(), control_rank
    )
    score = (
        (1.0 - float(cfg.defensive_weight)) * control_rank
        + float(cfg.defensive_weight) * effective_defensive
    )
    score = pd.to_numeric(score, errors="coerce").where(np.isfinite(score))
    output[active.index.to_numpy(dtype=int)] = score.to_numpy(dtype=float)
    return pd.Series(output, index=frame.index, name="fixed_core_score")


def ranked_tickers_for_date(
    frame: pd.DataFrame,
    scores: pd.Series | Sequence[float],
    signal_date: Any,
) -> tuple[str, ...]:
    """Return the finite-score rank order with the frozen ticker tie-break."""

    if len(scores) != len(frame):
        raise ValueError("scores length must equal frame length")
    if "date" not in frame or "ticker" not in frame:
        raise ValueError("frame must contain date and ticker columns")
    dates = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    if dates.isna().any():
        raise ValueError("signal frame contains invalid dates")
    requested = _normalized_timestamp(signal_date)
    day = pd.DataFrame(
        {
            "ticker": frame["ticker"].astype(str).to_numpy(),
            "score": pd.to_numeric(pd.Series(scores), errors="coerce").to_numpy(),
            "date": dates.to_numpy(),
        }
    )
    day = day.loc[day["date"].eq(requested) & np.isfinite(day["score"])].copy()
    if day["ticker"].duplicated().any():
        raise ValueError(f"duplicate ticker at signal date {requested.date()}")
    day = day.sort_values(
        ["score", "ticker"], ascending=[False, True], kind="mergesort"
    )
    return tuple(day["ticker"].tolist())


def select_low_churn_targets(
    ranked_tickers: Sequence[str],
    previous_targets: Mapping[str, float] | Iterable[str] = (),
    config: LowChurnStrategyConfig | None = None,
) -> dict[str, float]:
    """Apply Top-N entry and exit-rank retention to one sleeve's prior state."""

    cfg = config or LowChurnStrategyConfig()
    ranking = tuple(map(str, ranked_tickers))
    if len(set(ranking)) != len(ranking):
        raise ValueError("ranked_tickers must be unique")
    previous = (
        {str(ticker) for ticker, weight in previous_targets.items() if float(weight) > 0.0}
        if isinstance(previous_targets, Mapping)
        else set(map(str, previous_targets))
    )
    eligible_for_retention = set(ranking[: int(cfg.retention_exit_rank)])
    retained = previous & eligible_for_retention
    selected = [ticker for ticker in ranking if ticker in retained]
    for ticker in ranking:
        if len(selected) >= int(cfg.position_count):
            break
        if ticker not in retained:
            selected.append(ticker)
    selected = selected[: int(cfg.position_count)]
    rank_order = {ticker: rank for rank, ticker in enumerate(ranking)}
    selected.sort(key=rank_order.__getitem__)
    if not selected:
        return {}
    weight = min(float(cfg.position_weight), 1.0 / len(selected))
    return {ticker: weight for ticker in selected}


def generate_sleeve_target_schedule(
    frame: pd.DataFrame,
    calendar: Sequence[Any],
    config: LowChurnStrategyConfig | None = None,
    *,
    initial_targets_by_sleeve: Mapping[
        int, Mapping[str, float] | Iterable[str]
    ]
    | None = None,
) -> list[dict[str, Any]]:
    """Generate targets with ten independent absolute-calendar sleeve states.

    The anchor session is calendar index zero.  An empty signal cross-section
    is emitted as an explicit skipped decision and does not reindex the later
    sessions or mutate that sleeve's state.
    """

    cfg = config or LowChurnStrategyConfig()
    sessions = [_normalized_timestamp(value) for value in calendar]
    if not sessions:
        raise ValueError("calendar must not be empty")
    if len(set(sessions)) != len(sessions) or sessions != sorted(sessions):
        raise ValueError("calendar must be unique and strictly increasing")
    anchor = _normalized_timestamp(cfg.anchor_date)
    try:
        anchor_index = sessions.index(anchor)
    except ValueError as exc:
        raise ValueError("calendar does not contain anchor_date") from exc

    scores = fixed_core_score(frame, cfg)
    score_dates = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    ranked_frame = pd.DataFrame(
        {
            "date": score_dates.to_numpy(),
            "ticker": frame["ticker"].astype(str).to_numpy(),
            "score": pd.to_numeric(pd.Series(scores), errors="coerce").to_numpy(),
        }
    )
    ranked_frame = ranked_frame.loc[np.isfinite(ranked_frame["score"])].sort_values(
        ["date", "score", "ticker"],
        ascending=[True, False, True],
        kind="mergesort",
    )
    rankings = {
        pd.Timestamp(date).normalize(): tuple(group["ticker"].tolist())
        for date, group in ranked_frame.groupby("date", sort=False)
    }
    states: dict[int, dict[str, float]] = {
        sleeve: {} for sleeve in range(int(cfg.sleeve_count))
    }
    for raw_sleeve, raw_targets in (initial_targets_by_sleeve or {}).items():
        sleeve = int(raw_sleeve)
        if sleeve not in states:
            raise ValueError(f"initial target sleeve out of range: {raw_sleeve}")
        if isinstance(raw_targets, Mapping):
            states[sleeve] = {
                str(ticker): float(weight)
                for ticker, weight in raw_targets.items()
                if float(weight) > 0.0
            }
        else:
            tickers = tuple(dict.fromkeys(map(str, raw_targets)))
            weight = min(float(cfg.position_weight), 1.0 / len(tickers)) if tickers else 0.0
            states[sleeve] = {ticker: weight for ticker in tickers}

    output: list[dict[str, Any]] = []
    for session_index in range(anchor_index, len(sessions)):
        signal_date = sessions[session_index]
        absolute_index = session_index - anchor_index
        sleeve = absolute_index % int(cfg.sleeve_count)
        ranking = rankings.get(signal_date, ())
        previous = dict(states[sleeve])
        if ranking:
            targets = select_low_churn_targets(ranking, previous, cfg)
            states[sleeve] = dict(targets)
            status = "ok"
        else:
            targets = previous
            status = "skipped_empty_signal"
        output.append(
            {
                "signal_date": signal_date.date().isoformat(),
                "calendar_index": absolute_index,
                "sleeve": sleeve,
                "status": status,
                "ranked_count": len(ranking),
                "previous_tickers": list(previous),
                "selected_tickers": list(targets),
                "target_weights": dict(targets),
            }
        )
    return output


__all__ = [
    "LowChurnStrategyConfig",
    "REQUIRED_SIGNAL_COLUMNS",
    "fixed_core_score",
    "generate_sleeve_target_schedule",
    "ranked_tickers_for_date",
    "select_low_churn_targets",
]
