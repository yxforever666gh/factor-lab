"""Pure point-in-time rules for the 12.0 quarterly stock route.

The module deliberately knows nothing about raw files or future outcomes.  It
accepts one already-built quarter-end snapshot, applies the frozen market gate
and stock ranking, and returns deterministic targets plus an auditable decision
record.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from math import isfinite, sqrt
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


STRATEGY_ID = "quarterly_pit_dual_market_gate_trend_lowvol_top80"
FORBIDDEN_INPUT_FRAGMENTS = ("forward", "future", "label", "outcome")
REQUIRED_SNAPSHOT_COLUMNS = {
    "ticker",
    "signal_date",
    "universe_member",
    "mom12",
    "mom6",
    "vol63",
    "adv20",
    "industry",
    "size_bucket",
}


class PITStockContractError(ValueError):
    """Raised when a snapshot cannot prove the frozen PIT contract."""


@dataclass(frozen=True)
class PITStockStrategyConfig:
    universe_size: int = 1000
    position_count: int = 80
    long_start_lag: int = 252
    short_start_lag: int = 126
    end_lag: int = 21
    volatility_sessions: int = 63
    minimum_listing_sessions: int = 253
    market_long_median_min: float = 0.0
    market_short_median_min: float = 0.0

    def __post_init__(self) -> None:
        integers = {
            "universe_size": self.universe_size,
            "position_count": self.position_count,
            "long_start_lag": self.long_start_lag,
            "short_start_lag": self.short_start_lag,
            "end_lag": self.end_lag,
            "volatility_sessions": self.volatility_sessions,
            "minimum_listing_sessions": self.minimum_listing_sessions,
        }
        if any(isinstance(value, bool) or int(value) != value or value <= 0 for value in integers.values()):
            raise ValueError("strategy integer settings must be positive integers")
        if self.position_count > self.universe_size:
            raise ValueError("position_count cannot exceed universe_size")
        if not self.long_start_lag > self.short_start_lag > self.end_lag:
            raise ValueError("momentum lags must satisfy long > short > end")
        if not all(
            isfinite(float(value))
            for value in (self.market_long_median_min, self.market_short_median_min)
        ):
            raise ValueError("market gate thresholds must be finite")


@dataclass(frozen=True)
class PITStockDecision:
    strategy_id: str
    signal_date: str
    market_long_median: float
    market_short_median: float
    market_gate_open: bool
    universe_count: int
    positive_long_count: int
    selected_count: int
    target_weight_sum: float
    cash_weight: float
    target_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_sha256(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _date(value: Any, *, field: str) -> pd.Timestamp:
    if value is None or value is pd.NaT or pd.isna(value):
        raise PITStockContractError(f"{field} must be known")
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    else:
        parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        raise PITStockContractError(f"invalid {field}: {value!r}")
    result = pd.Timestamp(parsed)
    if result.tzinfo is not None:
        result = result.tz_convert(None)
    return result.normalize()


def official_quarter_end_sessions(
    values: Sequence[Any], *, calendar_through_date: Any | None = None
) -> tuple[pd.Timestamp, ...]:
    """Return official last-open sessions for fully covered calendar quarters.

    A historical boundary is proven by the next open session.  The final
    boundary can also be proven without next-quarter market data when the
    official calendar itself is captured through that calendar quarter end.
    """

    sessions = tuple(_date(value, field="official session") for value in values)
    if not sessions or list(sessions) != sorted(sessions) or len(set(sessions)) != len(sessions):
        raise PITStockContractError("official sessions must be non-empty, unique, and increasing")
    result = [
        current
        for current, following in zip(sessions, sessions[1:])
        if current.to_period("Q") != following.to_period("Q")
    ]
    if calendar_through_date is not None:
        through = _date(calendar_through_date, field="calendar_through_date")
        final = sessions[-1]
        quarter_end = final.to_period("Q").end_time.normalize()
        if through >= quarter_end and (not result or result[-1] != final):
            result.append(final)
    return tuple(result)


def _strict_bool_series(values: pd.Series, *, field: str) -> pd.Series:
    if values.isna().any():
        raise PITStockContractError(f"{field} contains unknown values")
    if pd.api.types.is_bool_dtype(values.dtype):
        return values.astype(bool)
    text = values.astype("string").str.strip().str.casefold()
    if not text.isin(["true", "false", "1", "0"]).all():
        raise PITStockContractError(f"{field} must be strict booleans")
    return text.isin(["true", "1"])


def _normalized_snapshot(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.Timestamp]:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("snapshot must be a pandas DataFrame")
    forbidden = sorted(
        str(column)
        for column in frame.columns
        if any(fragment in str(column).strip().casefold() for fragment in FORBIDDEN_INPUT_FRAGMENTS)
    )
    if forbidden:
        raise PITStockContractError(f"snapshot contains forbidden future columns: {forbidden}")
    missing = sorted(REQUIRED_SNAPSHOT_COLUMNS - set(frame.columns))
    if missing:
        raise PITStockContractError(f"snapshot missing columns: {missing}")
    work = frame.copy()
    work["ticker"] = work["ticker"].astype("string").str.strip()
    if work["ticker"].isna().any() or work["ticker"].eq("").any():
        raise PITStockContractError("snapshot contains an unknown ticker")
    dates = work["signal_date"].map(lambda value: _date(value, field="signal_date"))
    if dates.nunique() != 1:
        raise PITStockContractError("snapshot must contain exactly one signal date")
    signal_date = pd.Timestamp(dates.iloc[0])
    work["signal_date"] = signal_date
    if work.duplicated("ticker").any():
        raise PITStockContractError("snapshot contains duplicate tickers")
    work["universe_member"] = _strict_bool_series(
        work["universe_member"], field="universe_member"
    )
    for column in ("mom12", "mom6", "vol63", "adv20"):
        work[column] = pd.to_numeric(work[column], errors="coerce")
    work["industry"] = work["industry"].astype("string").fillna("UNKNOWN")
    work["size_bucket"] = work["size_bucket"].astype("string").fillna("UNKNOWN_SIZE")
    return work.sort_values("ticker", kind="mergesort").reset_index(drop=True), signal_date


def select_quarterly_targets(
    snapshot: pd.DataFrame,
    config: PITStockStrategyConfig = PITStockStrategyConfig(),
) -> tuple[pd.DataFrame, PITStockDecision]:
    """Apply the frozen dual-market gate and trend/low-vol Top-80 rule."""

    work, signal_date = _normalized_snapshot(snapshot)
    universe = work.loc[work["universe_member"]].copy()
    if len(universe) != config.universe_size:
        raise PITStockContractError(
            f"universe count must be exactly {config.universe_size}, got {len(universe)}"
        )
    numeric = universe[["mom12", "mom6", "vol63", "adv20"]].to_numpy(dtype=float)
    if not np.isfinite(numeric).all() or universe["adv20"].le(0).any() or universe["vol63"].lt(0).any():
        raise PITStockContractError("universe factors must be finite with positive ADV and non-negative vol")
    market_long = float(universe["mom12"].median())
    market_short = float(universe["mom6"].median())
    gate_open = bool(
        market_long > config.market_long_median_min
        and market_short > config.market_short_median_min
    )
    eligible = universe.loc[universe["mom12"].gt(0)].copy()
    eligible["rank_mom12"] = eligible["mom12"].rank(
        pct=True, method="average", ascending=True
    )
    eligible["rank_lowvol"] = (-eligible["vol63"]).rank(
        pct=True, method="average", ascending=True
    )
    eligible["score"] = 0.5 * eligible["rank_mom12"] + 0.5 * eligible["rank_lowvol"]
    if gate_open:
        selected = eligible.sort_values(
            ["score", "ticker"], ascending=[False, True], kind="mergesort"
        ).head(config.position_count)
    else:
        selected = eligible.iloc[0:0].copy()
    targets = selected[
        [
            "ticker",
            "signal_date",
            "mom12",
            "mom6",
            "vol63",
            "adv20",
            "industry",
            "size_bucket",
            "rank_mom12",
            "rank_lowvol",
            "score",
        ]
    ].copy()
    targets["selection_rank"] = np.arange(1, len(targets) + 1, dtype=int)
    targets["target_weight"] = 1.0 / config.position_count
    targets.insert(0, "strategy_id", STRATEGY_ID)
    targets = targets.sort_values("ticker", kind="mergesort").reset_index(drop=True)
    target_payload = [
        {"ticker": str(row.ticker), "target_weight": float(row.target_weight)}
        for row in targets[["ticker", "target_weight"]].itertuples(index=False)
    ]
    target_sum = float(targets["target_weight"].sum()) if not targets.empty else 0.0
    decision = PITStockDecision(
        strategy_id=STRATEGY_ID,
        signal_date=str(signal_date.date()),
        market_long_median=market_long,
        market_short_median=market_short,
        market_gate_open=gate_open,
        universe_count=len(universe),
        positive_long_count=len(eligible),
        selected_count=len(targets),
        target_weight_sum=target_sum,
        cash_weight=max(1.0 - target_sum, 0.0),
        target_sha256=canonical_sha256(target_payload),
    )
    return targets, decision


def annualized_metrics(
    period_returns: Iterable[float], *, periods_per_year: float = 4.0
) -> dict[str, float | int]:
    values = np.asarray(list(period_returns), dtype=float)
    if values.size == 0 or not np.isfinite(values).all() or np.any(values <= -1.0):
        raise PITStockContractError("period returns must be finite, non-empty, and greater than -100%")
    if not isfinite(float(periods_per_year)) or periods_per_year <= 0:
        raise ValueError("periods_per_year must be positive")
    growth = float(np.prod(1.0 + values))
    years = values.size / float(periods_per_year)
    cagr = growth ** (1.0 / years) - 1.0
    volatility = float(values.std(ddof=1) * sqrt(periods_per_year)) if values.size > 1 else 0.0
    sharpe = float(values.mean() * periods_per_year / volatility) if volatility > 0 else 0.0
    nav = np.concatenate(([1.0], np.cumprod(1.0 + values)))
    drawdown = nav / np.maximum.accumulate(nav) - 1.0
    return {
        "observations": int(values.size),
        "compound_return": growth - 1.0,
        "cagr": float(cagr),
        "annual_volatility": volatility,
        "sharpe": sharpe,
        "max_drawdown": float(drawdown.min()),
        "positive_period_fraction": float(np.mean(values > 0)),
    }


def group_contributions(
    realized: pd.DataFrame,
    *,
    group_columns: Sequence[str] = ("industry", "size_bucket", "market_state"),
) -> pd.DataFrame:
    """Aggregate exhaustive target-weight return contributions by PIT groups."""

    required = {"signal_date", "ticker", "target_weight", "net_stock_return", *group_columns}
    missing = sorted(required - set(realized.columns))
    if missing:
        raise PITStockContractError(f"realized rows missing columns: {missing}")
    work = realized.copy()
    work["target_weight"] = pd.to_numeric(work["target_weight"], errors="coerce")
    work["net_stock_return"] = pd.to_numeric(work["net_stock_return"], errors="coerce")
    if not np.isfinite(work[["target_weight", "net_stock_return"]].to_numpy(dtype=float)).all():
        raise PITStockContractError("realized weights and returns must be finite")
    work["contribution"] = work["target_weight"] * work["net_stock_return"]
    rows: list[pd.DataFrame] = []
    for column in group_columns:
        values = work[column].astype("string").fillna(f"UNKNOWN_{column.upper()}")
        grouped = (
            work.assign(_group=values)
            .groupby(["signal_date", "_group"], sort=True, dropna=False)
            .agg(
                stock_count=("ticker", "size"),
                target_weight=("target_weight", "sum"),
                contribution=("contribution", "sum"),
            )
            .reset_index()
            .rename(columns={"_group": "group_value"})
        )
        grouped.insert(1, "group_type", column)
        rows.append(grouped)
    return pd.concat(rows, ignore_index=True).sort_values(
        ["signal_date", "group_type", "group_value"], kind="mergesort"
    ).reset_index(drop=True)


__all__ = [
    "PITStockContractError",
    "PITStockDecision",
    "PITStockStrategyConfig",
    "STRATEGY_ID",
    "annualized_metrics",
    "canonical_sha256",
    "group_contributions",
    "official_quarter_end_sessions",
    "select_quarterly_targets",
]
