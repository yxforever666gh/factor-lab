"""Pure causal kernel for the fixed 7.0 multi-asset ETF route.

No function performs file or network I/O.  Month ends come from an explicit
official SSE calendar.  Target weights are formed at the signal close, order
shares are sealed at that close, and the next open supplies only the execution
price.  Cash distributions accrue as receivables on ex-date and become
spendable only on their payment date.
"""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
import math
from operator import ge, le
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd


CANDIDATE_ID = "causal_multi_horizon_trend_budget"
CONTROL_ID = "static_risk_budget"
CASH_ONLY_ID = "cash_only_511880"
VOLATILITY_BALANCED_ID = "causal_monthly_volatility_balanced_budget"
CASH_CODE = "511880.SH"
RISK_BUDGETS: dict[str, float] = {
    "510300.SH": 0.30,
    "159920.SZ": 0.10,
    "513100.SH": 0.10,
    "518880.SH": 0.20,
    "511010.SH": 0.30,
}
RISK_CODES = tuple(RISK_BUDGETS)
ALL_CODES = (*RISK_CODES, CASH_CODE)
TREND_HORIZONS = (63, 126, 252)
VOLATILITY_LEVEL_LOOKBACK = 127
VOLATILITY_RETURN_COUNT = VOLATILITY_LEVEL_LOOKBACK - 1
VOLATILITY_FLOOR = 1e-12
FIRST_SIGNAL_DATE = pd.Timestamp("2015-02-27")
INITIAL_CAPITAL_RMB = 1_000_000.0
LOT_SIZE = 100
MAX_SIGNAL_ADV_PARTICIPATION = 0.10
BASE_COST_BPS_PER_SIDE = 8.0
STRESS_COST_BPS_PER_SIDE = 16.0
TRADING_DAYS_PER_YEAR = 252.0
ACCOUNTING_ABS_TOL_RMB = 1e-8
REQUIRED_COLUMNS = {
    "trade_date",
    "open",
    "close",
    "dividend_cash",
    "dividend_pay_date",
    "total_return_index",
    "adv20_rmb",
}
UNIT_MULTIPLIER_COLUMN = "unit_multiplier"
REFERENCE_RESET_COLUMN = "reference_price_reset"


@dataclass(frozen=True)
class SimulationConfig:
    """Frozen execution settings; only per-side cost may be stressed."""

    cost_bps_per_side: float = BASE_COST_BPS_PER_SIDE

    def __post_init__(self) -> None:
        value = float(self.cost_bps_per_side)
        if not math.isfinite(value) or value < 0.0:
            raise ValueError("cost_bps_per_side must be finite and non-negative")

    @property
    def cost_rate(self) -> float:
        return float(self.cost_bps_per_side) / 10_000.0


def _floor_lot(shares: float) -> int:
    if not math.isfinite(float(shares)) or float(shares) <= 0.0:
        return 0
    return int(math.floor(float(shares) / LOT_SIZE + 1e-12)) * LOT_SIZE


def _scale_lot_exact(shares: int, multiplier: float, *, role: str) -> int:
    scaled = int(shares) * float(multiplier)
    rounded = int(round(scaled))
    if (
        not math.isfinite(scaled)
        or not math.isclose(scaled, rounded, rel_tol=0.0, abs_tol=1e-9)
        or rounded % LOT_SIZE != 0
    ):
        raise RuntimeError(
            f"unit multiplier makes {role} non-integer or non-lot-aligned"
        )
    return rounded


def _official_sessions(values: Sequence[Any]) -> tuple[pd.Timestamp, ...]:
    sessions = tuple(pd.Timestamp(value).normalize() for value in values)
    if len(sessions) < 2:
        raise ValueError("official_sessions must contain at least two sessions")
    if list(sessions) != sorted(sessions) or len(set(sessions)) != len(sessions):
        raise ValueError("official_sessions must be unique and strictly increasing")
    return sessions


def _normalize_market_data(
    market_data: Mapping[str, pd.DataFrame],
    official_sessions: Sequence[Any],
) -> tuple[dict[str, pd.DataFrame], tuple[pd.Timestamp, ...]]:
    official = _official_sessions(official_sessions)
    official_set = set(official)
    if not isinstance(market_data, Mapping):
        raise TypeError("market_data must be a mapping of code to DataFrame")
    actual_codes = set(map(str, market_data))
    if actual_codes != set(ALL_CODES):
        raise ValueError(
            "fixed ETF universe mismatch; "
            f"missing={sorted(set(ALL_CODES) - actual_codes)}, "
            f"extra={sorted(actual_codes - set(ALL_CODES))}"
        )

    observed_frames: dict[str, pd.DataFrame] = {}
    first_dates: list[pd.Timestamp] = []
    last_dates: list[pd.Timestamp] = []
    for code in ALL_CODES:
        raw = market_data[code]
        if not isinstance(raw, pd.DataFrame) or raw.empty:
            raise ValueError(f"{code} market frame must be a non-empty DataFrame")
        missing = sorted(REQUIRED_COLUMNS - set(raw.columns))
        if missing:
            raise ValueError(f"{code} market frame missing columns: {missing}")
        selected = sorted(
            REQUIRED_COLUMNS
            | {
                column
                for column in (UNIT_MULTIPLIER_COLUMN, REFERENCE_RESET_COLUMN)
                if column in raw.columns
            }
        )
        frame = raw.loc[:, selected].copy()
        if UNIT_MULTIPLIER_COLUMN not in frame:
            frame[UNIT_MULTIPLIER_COLUMN] = 1.0
        if REFERENCE_RESET_COLUMN not in frame:
            frame[REFERENCE_RESET_COLUMN] = False
        dates = pd.to_datetime(frame["trade_date"], errors="coerce")
        if dates.isna().any():
            raise ValueError(f"{code} contains an invalid trade_date")
        if getattr(dates.dt, "tz", None) is not None:
            dates = dates.dt.tz_localize(None)
        frame["trade_date"] = dates.dt.normalize()
        if frame["trade_date"].duplicated().any():
            raise ValueError(f"{code} contains duplicate trade_date rows")
        frame = frame.sort_values("trade_date", kind="mergesort")
        if not frame["trade_date"].isin(official_set).all():
            raise ValueError(f"{code} contains a non-official session")

        for column in ("open", "close", "dividend_cash", "total_return_index", "adv20_rmb"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        present_open = frame["open"].notna()
        if bool(
            (
                (~np.isfinite(frame.loc[present_open, "open"]))
                | frame.loc[present_open, "open"].le(0.0)
            ).any()
        ):
            raise ValueError(f"{code} contains an invalid open")
        for column in ("close", "total_return_index"):
            values = frame[column]
            if values.isna().any() or not np.isfinite(values).all() or bool(values.le(0.0).any()):
                raise ValueError(f"{code} contains invalid {column}")
        dividends = frame["dividend_cash"]
        if dividends.isna().any() or not np.isfinite(dividends).all() or bool(dividends.lt(0.0).any()):
            raise ValueError(f"{code} contains invalid dividend_cash")
        observed_adv = frame["adv20_rmb"].notna()
        if bool(
            (
                (~np.isfinite(frame.loc[observed_adv, "adv20_rmb"]))
                | frame.loc[observed_adv, "adv20_rmb"].lt(0.0)
            ).any()
        ):
            raise ValueError(f"{code} contains invalid adv20_rmb")

        frame[UNIT_MULTIPLIER_COLUMN] = pd.to_numeric(
            frame[UNIT_MULTIPLIER_COLUMN], errors="coerce"
        )
        multipliers = frame[UNIT_MULTIPLIER_COLUMN]
        if (
            multipliers.isna().any()
            or not np.isfinite(multipliers).all()
            or bool(multipliers.le(0.0).any())
        ):
            raise ValueError(f"{code} contains invalid unit_multiplier")
        reference_values = frame[REFERENCE_RESET_COLUMN]
        if reference_values.isna().any() or not reference_values.map(
            lambda value: isinstance(value, (bool, np.bool_))
        ).all():
            raise ValueError(f"{code} contains invalid reference_price_reset")
        frame[REFERENCE_RESET_COLUMN] = reference_values.astype(bool)
        if (
            not math.isclose(
                float(frame[UNIT_MULTIPLIER_COLUMN].iloc[0]),
                1.0,
                rel_tol=0.0,
                abs_tol=0.0,
            )
            or bool(frame[REFERENCE_RESET_COLUMN].iloc[0])
        ):
            raise ValueError(f"{code} starts with an unanchored unit/reference event")

        pay_raw = frame["dividend_pay_date"]
        parsed_pay = pd.to_datetime(pay_raw, errors="coerce").dt.normalize()
        invalid_pay = pay_raw.notna() & parsed_pay.isna()
        if invalid_pay.any():
            raise ValueError(f"{code} contains an invalid dividend_pay_date")
        has_dividend = dividends.gt(0.0)
        if bool((has_dividend & parsed_pay.isna()).any()):
            raise ValueError(f"{code} dividend requires a payment date")
        if bool(
            (
                has_dividend
                & parsed_pay.lt(frame["trade_date"])
            ).any()
        ):
            raise ValueError(f"{code} dividend payment precedes ex-date")
        frame["dividend_pay_date"] = parsed_pay
        frame = frame.set_index("trade_date", drop=False)
        observed_frames[code] = frame
        first_dates.append(pd.Timestamp(frame.index[0]))
        last_dates.append(pd.Timestamp(frame.index[-1]))

    active_start = min(first_dates)
    active_end = max(last_dates)
    active_sessions = tuple(
        session for session in official if active_start <= session <= active_end
    )
    if len(active_sessions) < 2:
        raise ValueError("market frames do not cover two official sessions")
    active_index = pd.DatetimeIndex(active_sessions, name="trade_date")

    normalized: dict[str, pd.DataFrame] = {}
    for code in ALL_CODES:
        source = observed_frames[code]
        observed = pd.Series(active_index.isin(source.index), index=active_index)
        gaps = np.flatnonzero(~observed.to_numpy(dtype=bool))
        if len(gaps) and (
            gaps[0] == 0
            or any(right == left + 1 for left, right in zip(gaps[:-1], gaps[1:]))
        ):
            raise ValueError(f"{code} has an uncarryable or consecutive official-session gap")
        frame = source.reindex(active_index).copy()
        frame["trade_date"] = active_index
        frame["_observed"] = observed.to_numpy(dtype=bool)
        frame["close"] = frame["close"].ffill(limit=1)
        frame["total_return_index"] = frame["total_return_index"].ffill(limit=1)
        frame["dividend_cash"] = frame["dividend_cash"].fillna(0.0)
        frame[UNIT_MULTIPLIER_COLUMN] = frame[UNIT_MULTIPLIER_COLUMN].fillna(1.0)
        frame[REFERENCE_RESET_COLUMN] = frame[REFERENCE_RESET_COLUMN].eq(True)
        frame["_tradable"] = frame["_observed"] & frame["open"].notna()
        if frame[["close", "total_return_index"]].isna().any().any():
            raise ValueError(f"{code} has a valuation gap that cannot be carried")
        normalized[code] = frame
    return normalized, official


def _volatility_balanced_snapshot(
    frames: Mapping[str, pd.DataFrame], signal_date: pd.Timestamp
) -> tuple[dict[str, float], dict[str, float], bool]:
    """Return one all-or-static causal inverse-volatility snapshot."""

    volatilities: dict[str, float] = {}
    for code in RISK_CODES:
        frame = frames[code]
        observed = frame.loc[
            frame.index.to_series().le(signal_date) & frame["_observed"],
            "total_return_index",
        ].tail(VOLATILITY_LEVEL_LOOKBACK)
        if len(observed) != VOLATILITY_LEVEL_LOOKBACK:
            return ({**RISK_BUDGETS, CASH_CODE: 0.0}, {}, True)
        levels = observed.to_numpy(dtype=float)
        returns = levels[1:] / levels[:-1] - 1.0
        if len(returns) != VOLATILITY_RETURN_COUNT or not np.isfinite(returns).all():
            return ({**RISK_BUDGETS, CASH_CODE: 0.0}, {}, True)
        volatility = float(
            np.std(returns, ddof=1) * math.sqrt(TRADING_DAYS_PER_YEAR)
        )
        if not math.isfinite(volatility) or volatility <= VOLATILITY_FLOOR:
            return ({**RISK_BUDGETS, CASH_CODE: 0.0}, {}, True)
        volatilities[code] = volatility

    raw = {
        code: float(RISK_BUDGETS[code]) / volatilities[code]
        for code in RISK_CODES
    }
    denominator = math.fsum(raw.values())
    if not math.isfinite(denominator) or denominator <= 0.0:
        return ({**RISK_BUDGETS, CASH_CODE: 0.0}, {}, True)
    weights = {code: raw[code] / denominator for code in RISK_CODES}
    last_code = RISK_CODES[-1]
    weights[last_code] = 1.0 - math.fsum(
        weights[code] for code in RISK_CODES[:-1]
    )
    if (
        any(not math.isfinite(value) or value < 0.0 for value in weights.values())
        or not math.isclose(
            math.fsum(weights.values()), 1.0, rel_tol=0.0, abs_tol=1e-12
        )
    ):
        return ({**RISK_BUDGETS, CASH_CODE: 0.0}, {}, True)
    weights[CASH_CODE] = 0.0
    return weights, volatilities, False


def build_monthly_targets(
    market_data: Mapping[str, pd.DataFrame],
    official_sessions: Sequence[Any],
    strategy_id: str = CANDIDATE_ID,
) -> pd.DataFrame:
    """Build targets from official month ends without reading next-session prices."""

    if strategy_id not in {
        CANDIDATE_ID,
        CONTROL_ID,
        CASH_ONLY_ID,
        VOLATILITY_BALANCED_ID,
    }:
        raise ValueError(f"unknown multi-asset strategy_id: {strategy_id}")
    frames, official = _normalize_market_data(market_data, official_sessions)
    available = tuple(frames[CASH_CODE].index)
    available_position = {date: index for index, date in enumerate(available)}
    cash_tri = frames[CASH_CODE]["total_return_index"].to_numpy(dtype=float)
    rows: list[dict[str, Any]] = []

    for official_index in range(len(official) - 1):
        signal_date = official[official_index]
        execution_date = official[official_index + 1]
        if (
            signal_date < FIRST_SIGNAL_DATE
            or signal_date not in available_position
            or (signal_date.year, signal_date.month)
            == (execution_date.year, execution_date.month)
        ):
            continue
        if not all(bool(frames[code].at[signal_date, "_observed"]) for code in ALL_CODES):
            continue
        signal_index = available_position[signal_date]
        weights: dict[str, float] = {}
        fractions: dict[str, float] = {}
        volatilities: dict[str, float] = {}
        volatility_fallback_static = False
        if strategy_id == VOLATILITY_BALANCED_ID:
            (
                weights,
                volatilities,
                volatility_fallback_static,
            ) = _volatility_balanced_snapshot(frames, signal_date)
        else:
            for code, budget in RISK_BUDGETS.items():
                if strategy_id == CASH_ONLY_ID:
                    fraction = 0.0
                elif strategy_id == CONTROL_ID:
                    fraction = 1.0
                elif signal_index < max(TREND_HORIZONS):
                    fraction = 0.0
                else:
                    tri = frames[code]["total_return_index"].to_numpy(dtype=float)
                    positives = sum(
                        (
                            (tri[signal_index] / tri[signal_index - horizon])
                            / (cash_tri[signal_index] / cash_tri[signal_index - horizon])
                        )
                        > 1.0
                        for horizon in TREND_HORIZONS
                    )
                    fraction = float(positives) / len(TREND_HORIZONS)
                fractions[code] = fraction
                weights[code] = float(budget) * fraction
            weights[CASH_CODE] = 1.0 - math.fsum(weights.values())

        for code in ALL_CODES:
            adv = frames[code].at[signal_date, "adv20_rmb"]
            if pd.isna(adv) or not math.isfinite(float(adv)):
                raise ValueError(f"{code} lacks finite ADV20 on {signal_date.date()}")
            rows.append(
                {
                    "strategy_id": strategy_id,
                    "signal_date": signal_date,
                    "execution_date": execution_date,
                    "code": code,
                    "target_weight": float(weights[code]),
                    "base_budget": float(RISK_BUDGETS.get(code, 0.0)),
                    "trend_positive_fraction": (
                        float(fractions[code]) if code in fractions else np.nan
                    ),
                    "signal_adv20_rmb": float(adv),
                    "realized_volatility": (
                        float(volatilities[code])
                        if strategy_id == VOLATILITY_BALANCED_ID
                        and code in volatilities
                        else np.nan
                    ),
                    "volatility_fallback_static": (
                        bool(volatility_fallback_static)
                        if strategy_id == VOLATILITY_BALANCED_ID
                        else np.nan
                    ),
                }
            )
    columns = [
        "strategy_id",
        "signal_date",
        "execution_date",
        "code",
        "target_weight",
        "base_budget",
        "trend_positive_fraction",
        "signal_adv20_rmb",
    ]
    if strategy_id == VOLATILITY_BALANCED_ID:
        columns.extend(["realized_volatility", "volatility_fallback_static"])
    return pd.DataFrame(rows, columns=columns)


def _validate_targets(
    targets: pd.DataFrame,
    frames: Mapping[str, pd.DataFrame],
    official: Sequence[pd.Timestamp],
) -> pd.DataFrame:
    required = {
        "strategy_id",
        "signal_date",
        "execution_date",
        "code",
        "target_weight",
        "signal_adv20_rmb",
    }
    if not isinstance(targets, pd.DataFrame) or targets.empty:
        raise ValueError("targets must be a non-empty DataFrame")
    missing = sorted(required - set(targets.columns))
    if missing:
        raise ValueError(f"targets missing columns: {missing}")
    work = targets.copy()
    work["strategy_id"] = work["strategy_id"].astype(str)
    if len(set(work["strategy_id"])) != 1 or work["strategy_id"].iloc[0] not in {
        CANDIDATE_ID,
        CONTROL_ID,
        CASH_ONLY_ID,
        VOLATILITY_BALANCED_ID,
    }:
        raise ValueError("targets must contain exactly one registered strategy")
    strategy_id = str(work["strategy_id"].iloc[0])
    volatility_columns = {"realized_volatility", "volatility_fallback_static"}
    if strategy_id == VOLATILITY_BALANCED_ID:
        volatility_required = {
            "base_budget",
            "trend_positive_fraction",
            *volatility_columns,
        }
        volatility_missing = sorted(volatility_required - set(work.columns))
        if volatility_missing:
            raise ValueError(
                "volatility-balanced targets lack frozen diagnostics: "
                f"{volatility_missing}"
            )
        work["realized_volatility"] = pd.to_numeric(
            work["realized_volatility"], errors="coerce"
        )
    elif volatility_columns & set(work.columns):
        raise ValueError("volatility diagnostics require the exact volatility strategy_id")
    for column in ("signal_date", "execution_date"):
        work[column] = pd.to_datetime(work[column], errors="coerce").dt.normalize()
    work["code"] = work["code"].astype(str)
    for column in ("target_weight", "signal_adv20_rmb"):
        work[column] = pd.to_numeric(work[column], errors="coerce")
    if work[list(required - {"strategy_id", "code"})].isna().any().any():
        raise ValueError("targets contain invalid dates, weights, or signal ADV")
    if not np.isfinite(work["target_weight"]).all() or bool(work["target_weight"].lt(0.0).any()):
        raise ValueError("target weights must be finite and non-negative")
    if work.duplicated(["signal_date", "code"]).any():
        raise ValueError("targets contain duplicate signal-date/code rows")

    official_position = {date: index for index, date in enumerate(official)}
    available = set(frames[CASH_CODE].index)
    for signal_date, group in work.groupby("signal_date", sort=True):
        executions = set(group["execution_date"])
        if len(executions) != 1 or set(group["code"]) != set(ALL_CODES):
            raise ValueError("each signal must contain the exact fixed ETF universe")
        execution_date = next(iter(executions))
        if signal_date not in available or signal_date not in official_position:
            raise ValueError("target signal is outside available official history")
        if (
            execution_date not in official_position
            or official_position[execution_date] != official_position[signal_date] + 1
        ):
            raise ValueError("execution must be the immediate next official session")
        if (signal_date.year, signal_date.month) == (
            execution_date.year,
            execution_date.month,
        ):
            raise ValueError("signal must be an official calendar month end")
        if not math.isclose(
            math.fsum(float(value) for value in group["target_weight"]),
            1.0,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise ValueError("target weights must sum to one")
        if strategy_id == CASH_ONLY_ID:
            cash_only_weights = group.set_index("code")["target_weight"]
            if (
                any(float(cash_only_weights.at[code]) != 0.0 for code in RISK_CODES)
                or float(cash_only_weights.at[CASH_CODE]) != 1.0
            ):
                raise ValueError(
                    "cash-only targets must allocate exactly zero to every risk ETF "
                    f"and one to {CASH_CODE}"
                )
        if strategy_id == VOLATILITY_BALANCED_ID:
            indexed = group.set_index("code")
            expected_weights, expected_volatilities, expected_fallback = (
                _volatility_balanced_snapshot(frames, signal_date)
            )
            if any(
                float(indexed.at[code, "target_weight"])
                != float(expected_weights[code])
                for code in ALL_CODES
            ):
                raise ValueError(
                    "volatility-balanced target weights differ from causal history"
                )
            if any(
                float(indexed.at[code, "base_budget"])
                != float(RISK_BUDGETS.get(code, 0.0))
                for code in ALL_CODES
            ) or not indexed["trend_positive_fraction"].isna().all():
                raise ValueError("volatility-balanced frozen diagnostics differ")
            fallback_values = indexed["volatility_fallback_static"]
            if not fallback_values.map(
                lambda value: isinstance(value, (bool, np.bool_))
            ).all() or not fallback_values.map(bool).eq(expected_fallback).all():
                raise ValueError("volatility-balanced fallback identity differs")
            if expected_fallback:
                if not indexed["realized_volatility"].isna().all():
                    raise ValueError("static fallback must not disclose partial volatility")
            else:
                if not pd.isna(indexed.at[CASH_CODE, "realized_volatility"]) or any(
                    float(indexed.at[code, "realized_volatility"])
                    != float(expected_volatilities[code])
                    for code in RISK_CODES
                ):
                    raise ValueError(
                        "volatility-balanced realized volatility differs from causal history"
                    )
        for row in group.itertuples(index=False):
            if not bool(frames[row.code].at[signal_date, "_observed"]):
                raise ValueError("target uses an unobserved signal row")
            source_adv = frames[row.code].at[signal_date, "adv20_rmb"]
            if pd.isna(source_adv) or not math.isclose(
                float(row.signal_adv20_rmb),
                float(source_adv),
                rel_tol=0.0,
                abs_tol=1e-6,
            ):
                raise ValueError("target signal ADV does not match its source")
    return work.sort_values(["signal_date", "code"], kind="mergesort").reset_index(drop=True)


def _effective_payment_session(
    pay_date: pd.Timestamp,
    official: Sequence[pd.Timestamp],
) -> pd.Timestamp:
    index = bisect_left(official, pd.Timestamp(pay_date).normalize())
    if index >= len(official):
        return pd.Timestamp(pay_date).normalize()
    return official[index]


def simulate_targets(
    market_data: Mapping[str, pd.DataFrame],
    targets: pd.DataFrame,
    official_sessions: Sequence[Any],
    config: SimulationConfig | None = None,
) -> dict[str, Any]:
    """Seal close-sized orders and execute their fixed shares at next open."""

    cfg = config or SimulationConfig()
    frames, official = _normalize_market_data(market_data, official_sessions)
    target_frame = _validate_targets(targets, frames, official)
    targets_by_signal = {
        pd.Timestamp(date): group.set_index("code")
        for date, group in target_frame.groupby("signal_date", sort=True)
    }
    dates = tuple(frames[CASH_CODE].index)
    cash = INITIAL_CAPITAL_RMB
    shares = {code: 0 for code in ALL_CODES}
    receivables: dict[pd.Timestamp, float] = {}
    previous_closes: dict[str, float] = {}
    previous_nav = INITIAL_CAPITAL_RMB
    orders_by_execution: dict[pd.Timestamp, list[dict[str, Any]]] = {}
    order_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    holding_rows: list[dict[str, Any]] = []

    def executed_record(
        order: dict[str, Any],
        executed: int,
        open_price: float | None,
        status: str,
    ) -> dict[str, Any]:
        actual_notional = executed * open_price if open_price is not None else 0.0
        valuation_price = (
            float(open_price)
            if open_price is not None
            else float(order["signal_price"])
        )
        cost = actual_notional * cfg.cost_rate
        order["status"] = status
        order["executed_shares"] = int(executed)
        order["execution_price"] = open_price if open_price is not None else np.nan
        order["actual_executed_notional"] = actual_notional
        order["requested_execution_notional"] = (
            int(order["requested_shares"]) * valuation_price
        )
        order["capacity_limited_execution_notional"] = (
            int(order["capacity_limited_shares"]) * valuation_price
        )
        order["executed_signal_notional"] = (
            int(executed) * float(order["signal_price"])
        )
        order["cost"] = cost
        return dict(order)

    for date in dates:
        old_shares = dict(shares)
        planned = orders_by_execution.get(date, [])
        dividend_accrual = 0.0
        for code in ALL_CODES:
            dividend = float(frames[code].at[date, "dividend_cash"])
            if dividend <= 0.0 or old_shares[code] == 0:
                continue
            amount = old_shares[code] * dividend
            pay_date = pd.Timestamp(frames[code].at[date, "dividend_pay_date"])
            payment_session = _effective_payment_session(pay_date, official)
            receivables[payment_session] = receivables.get(payment_session, 0.0) + amount
            dividend_accrual += amount

        # Entitlement belongs to the prior-close units.  Only after that
        # entitlement is fixed do unit events scale holdings and the sealed
        # overnight share instructions.  Their frozen RMB notionals and ADV
        # capacity remain the signal-close evidence and are never rewritten.
        unit_event_count = 0
        for code in ALL_CODES:
            multiplier = float(frames[code].at[date, UNIT_MULTIPLIER_COLUMN])
            if math.isclose(multiplier, 1.0, rel_tol=0.0, abs_tol=0.0):
                continue
            unit_event_count += 1
            shares[code] = _scale_lot_exact(
                old_shares[code], multiplier, role=f"{code} holding"
            )
            for order in (row for row in planned if row["code"] == code):
                for field in (
                    "pre_signal_shares",
                    "desired_shares",
                    "requested_shares",
                    "capacity_capped_shares",
                    "planned_shares",
                    "capacity_limited_shares",
                ):
                    order[field] = _scale_lot_exact(
                        int(order[field]), multiplier, role=f"{code} order {field}"
                    )
                for field in ("requested_delta_shares", "planned_delta_shares"):
                    sign = -1 if int(order[field]) < 0 else 1
                    order[field] = sign * _scale_lot_exact(
                        abs(int(order[field])),
                        multiplier,
                        role=f"{code} order {field}",
                    )
                order["execution_unit_multiplier"] = (
                    float(order.get("execution_unit_multiplier", 1.0)) * multiplier
                )
        opening_shares = dict(shares)
        dividend_cash_paid = receivables.pop(date, 0.0)
        cash += dividend_cash_paid
        pretrade_nav = cash + math.fsum(receivables.values()) + math.fsum(
            opening_shares[code]
            * (
                float(frames[code].at[date, "open"])
                if bool(frames[code].at[date, "_observed"])
                and pd.notna(frames[code].at[date, "open"])
                else previous_closes.get(
                    code, float(frames[code].at[date, "close"])
                )
            )
            for code in ALL_CODES
        )

        trades_today: list[dict[str, Any]] = []
        for order in sorted(
            (row for row in planned if row["side"] == "sell"),
            key=lambda row: row["code"],
        ):
            code = str(order["code"])
            sealed_shares = int(order["planned_shares"])
            raw_open = frames[code].at[date, "open"]
            observed = bool(frames[code].at[date, "_observed"])
            open_price = float(raw_open) if observed and pd.notna(raw_open) else None
            if sealed_shares == 0:
                executed, status = 0, "blocked_capacity"
            elif open_price is None:
                executed, status = 0, "blocked_missing_open"
            else:
                if shares[code] < sealed_shares:
                    raise RuntimeError("sealed sell order exceeds current shares")
                executed, status = sealed_shares, "executed"
                notional = executed * open_price
                shares[code] -= executed
                cash += notional * (1.0 - cfg.cost_rate)
            trade = executed_record(order, executed, open_price, status)
            trade_rows.append(trade)
            trades_today.append(trade)

        # All buy orders share one fixed cash scaling.  A blocked sell changes
        # available cash, but never causes target or sealed-order recomputation.
        valid_buys: dict[str, tuple[dict[str, Any], float]] = {}
        for order in sorted(
            (row for row in planned if row["side"] == "buy"),
            key=lambda row: row["code"],
        ):
            code = str(order["code"])
            sealed_shares = int(order["planned_shares"])
            raw_open = frames[code].at[date, "open"]
            observed = bool(frames[code].at[date, "_observed"])
            open_price = float(raw_open) if observed and pd.notna(raw_open) else None
            if sealed_shares == 0:
                trade = executed_record(order, 0, open_price, "blocked_capacity")
                trade_rows.append(trade)
                trades_today.append(trade)
            elif open_price is None:
                trade = executed_record(order, 0, None, "blocked_missing_open")
                trade_rows.append(trade)
                trades_today.append(trade)
            else:
                valid_buys[code] = (order, open_price)

        required_cash = math.fsum(
            int(order["planned_shares"]) * price * (1.0 + cfg.cost_rate)
            for order, price in valid_buys.values()
        )
        if required_cash <= cash + ACCOUNTING_ABS_TOL_RMB:
            allocations = {
                code: int(order["planned_shares"])
                for code, (order, _price) in valid_buys.items()
            }
        else:
            scale = max(cash, 0.0) / required_cash if required_cash > 0.0 else 0.0
            allocations = {
                code: _floor_lot(int(order["planned_shares"]) * scale)
                for code, (order, _price) in valid_buys.items()
            }

        for code in sorted(valid_buys):
            order, open_price = valid_buys[code]
            executed = allocations[code]
            sealed_shares = int(order["planned_shares"])
            status = (
                "executed"
                if executed == sealed_shares
                else "partial_cash"
                if executed > 0
                else "blocked_cash"
            )
            notional = executed * open_price
            cost = notional * cfg.cost_rate
            shares[code] += executed
            cash -= notional + cost
            if cash < -ACCOUNTING_ABS_TOL_RMB:
                raise RuntimeError("pro-rata buy allocation overdraws cash")
            cash = max(cash, 0.0)
            trade = executed_record(order, executed, open_price, status)
            trade_rows.append(trade)
            trades_today.append(trade)

        closes = {code: float(frames[code].at[date, "close"]) for code in ALL_CODES}
        receivable_balance = math.fsum(receivables.values())
        market_value = math.fsum(shares[code] * closes[code] for code in ALL_CODES)
        nav = cash + receivable_balance + market_value
        total_cost = math.fsum(float(row["cost"]) for row in trades_today)
        pnl_parts: list[float] = [
            opening_shares[code] * closes[code]
            - old_shares[code] * previous_closes[code]
            for code in ALL_CODES
            if code in previous_closes
        ]
        for trade in trades_today:
            quantity = int(trade["executed_shares"])
            if quantity == 0:
                continue
            open_price = float(trade["execution_price"])
            pnl_parts.append(
                quantity * (closes[trade["code"]] - open_price)
                if trade["side"] == "buy"
                else quantity * (open_price - closes[trade["code"]])
            )
        price_pnl = math.fsum(pnl_parts)
        accounting_error = (nav - previous_nav) - (
            price_pnl + dividend_accrual - total_cost
        )
        if not math.isclose(
            accounting_error,
            0.0,
            rel_tol=0.0,
            abs_tol=ACCOUNTING_ABS_TOL_RMB,
        ):
            raise RuntimeError(f"daily account does not reconcile on {date.date()}")

        requested_notional = math.fsum(
            float(row["requested_execution_notional"]) for row in trades_today
        )
        executed_notional = math.fsum(
            float(row["actual_executed_notional"]) for row in trades_today
        )
        capacity_limited_requested = math.fsum(
            float(row["capacity_limited_execution_notional"])
            for row in trades_today
        )
        actual_executed_notional = math.fsum(
            float(row["actual_executed_notional"]) for row in trades_today
        )
        daily_rows.append(
            {
                "trade_date": date,
                "nav": nav,
                "cash": cash,
                "dividend_receivable": receivable_balance,
                "market_value": market_value,
                "net_return": nav / previous_nav - 1.0,
                "dividend_accrual": dividend_accrual,
                "dividend_cash_paid": dividend_cash_paid,
                "unit_event_count": unit_event_count,
                "reference_price_reset_count": sum(
                    bool(frames[code].at[date, REFERENCE_RESET_COLUMN])
                    for code in ALL_CODES
                ),
                "price_pnl": price_pnl,
                "trade_cost": total_cost,
                "accounting_error": accounting_error,
                "pretrade_nav": pretrade_nav,
                "turnover": (
                    actual_executed_notional / pretrade_nav
                    if pretrade_nav > 0.0
                    else 0.0
                ),
                "requested_notional": requested_notional,
                "executed_notional": executed_notional,
                "capacity_limited_requested_notional": capacity_limited_requested,
                "trade_count": sum(int(row["executed_shares"] > 0) for row in trades_today),
                "blocked_order_count": sum(int(row["executed_shares"] == 0) for row in trades_today),
            }
        )
        for code in ALL_CODES:
            value = shares[code] * closes[code]
            holding_rows.append(
                {
                    "trade_date": date,
                    "code": code,
                    "shares": int(shares[code]),
                    "close": closes[code],
                    "market_value": value,
                    "weight": value / nav if nav > 0.0 else 0.0,
                    "observed": bool(frames[code].at[date, "_observed"]),
                    "tradable": bool(frames[code].at[date, "_tradable"]),
                    "unit_multiplier": float(
                        frames[code].at[date, UNIT_MULTIPLIER_COLUMN]
                    ),
                    "reference_price_reset": bool(
                        frames[code].at[date, REFERENCE_RESET_COLUMN]
                    ),
                }
            )

        # Seal orders only after the signal close NAV and holdings are known.
        target_group = targets_by_signal.get(date)
        if target_group is not None:
            execution_date = pd.Timestamp(target_group["execution_date"].iloc[0])
            sealed: list[dict[str, Any]] = []
            for code in sorted(ALL_CODES):
                signal_price = closes[code]
                target_weight = float(target_group.at[code, "target_weight"])
                desired = _floor_lot(target_weight * nav / signal_price)
                delta = desired - shares[code]
                if delta == 0:
                    continue
                side = "buy" if delta > 0 else "sell"
                requested = abs(delta)
                signal_adv = float(target_group.at[code, "signal_adv20_rmb"])
                capacity_shares = _floor_lot(
                    signal_adv * MAX_SIGNAL_ADV_PARTICIPATION / signal_price
                )
                planned_shares = min(requested, capacity_shares)
                order = {
                    "strategy_id": str(target_group["strategy_id"].iloc[0]),
                    "signal_date": date,
                    "execution_date": execution_date,
                    "code": code,
                    "side": side,
                    "target_weight": target_weight,
                    "signal_price": signal_price,
                    "signal_adv20_rmb": signal_adv,
                    "capacity_rmb": signal_adv * MAX_SIGNAL_ADV_PARTICIPATION,
                    "pre_signal_shares": int(shares[code]),
                    "desired_shares": desired,
                    "requested_delta_shares": int(delta),
                    "requested_shares": requested,
                    "capacity_capped_shares": planned_shares,
                    "planned_delta_shares": (
                        planned_shares if side == "buy" else -planned_shares
                    ),
                    "planned_shares": planned_shares,
                    "requested_signal_notional": requested * signal_price,
                    "planned_signal_notional": planned_shares * signal_price,
                    "capacity_limited_shares": requested - planned_shares,
                    "capacity_limited_signal_notional": (
                        requested - planned_shares
                    )
                    * signal_price,
                    "capacity_limited": planned_shares < requested,
                    "execution_unit_multiplier": 1.0,
                    "status": "pending",
                    "executed_shares": 0,
                    "execution_price": np.nan,
                    "actual_executed_notional": 0.0,
                    "cost": 0.0,
                }
                order_rows.append(order)
                sealed.append(order)
            orders_by_execution.setdefault(execution_date, []).extend(sealed)

        previous_closes = closes
        previous_nav = nav

    daily_nav = pd.DataFrame(daily_rows)
    holdings = pd.DataFrame(holding_rows)
    orders = pd.DataFrame(order_rows)
    trades = pd.DataFrame(trade_rows)
    requested_total = math.fsum(
        float(value) for value in trades.get("requested_execution_notional", [])
    )
    executed_total = math.fsum(
        float(value) for value in trades.get("actual_executed_notional", [])
    )
    capacity_limited_total = math.fsum(
        float(row.capacity_limited_execution_notional)
        for row in trades.itertuples(index=False)
    )
    capacity = {
        "order_count": int(len(trades)),
        "requested_notional_total": requested_total,
        "executed_notional_total": executed_total,
        "capacity_limited_requested_notional": capacity_limited_total,
        "capacity_limited_requested_notional_ratio": (
            capacity_limited_total / requested_total if requested_total > 0.0 else 0.0
        ),
        "requested_notional_fill_ratio": (
            executed_total / requested_total if requested_total > 0.0 else 1.0
        ),
        "capacity_violation_count": int(
            sum(
                float(row.planned_signal_notional)
                > float(row.capacity_rmb) + ACCOUNTING_ABS_TOL_RMB
                for row in trades.itertuples(index=False)
            )
        ),
        "capacity_source": "signal_close_adv20_rmb_and_signal_close_price",
    }
    cumulative_error = (
        float(daily_nav["nav"].iloc[-1])
        - INITIAL_CAPITAL_RMB
        - math.fsum(
            float(row.price_pnl + row.dividend_accrual - row.trade_cost)
            for row in daily_nav.itertuples(index=False)
        )
    )
    max_error = float(daily_nav["accounting_error"].abs().max())
    reconciliation = {
        "initial_capital_rmb": INITIAL_CAPITAL_RMB,
        "final_nav_rmb": float(daily_nav["nav"].iloc[-1]),
        "max_abs_daily_accounting_error_rmb": max_error,
        "cumulative_identity_error_rmb": cumulative_error,
        "passed": bool(
            max_error <= ACCOUNTING_ABS_TOL_RMB
            and abs(cumulative_error) <= ACCOUNTING_ABS_TOL_RMB
        ),
    }
    if not reconciliation["passed"]:
        raise RuntimeError("full account reconciliation failed")
    return {
        "strategy_id": str(target_frame["strategy_id"].iloc[0]),
        "targets": target_frame,
        "orders": orders,
        "daily_nav": daily_nav,
        "holdings": holdings,
        "trades": trades,
        "capacity": capacity,
        "reconciliation": reconciliation,
        "execution_contract": {
            "initial_capital_rmb": INITIAL_CAPITAL_RMB,
            "lot_size": LOT_SIZE,
            "cost_bps_per_side": float(cfg.cost_bps_per_side),
            "max_signal_adv_participation": MAX_SIGNAL_ADV_PARTICIPATION,
            "order_sizing": "signal_close_nav_price_adv_sealed_shares",
            "dividend_timing": "ex_date_receivable_pay_date_spendable_cash",
            "trade_order": "sell_then_buy_without_order_recalculation",
        },
    }


def run_strategy(
    market_data: Mapping[str, pd.DataFrame],
    official_sessions: Sequence[Any],
    strategy_id: str = CANDIDATE_ID,
    config: SimulationConfig | None = None,
) -> dict[str, Any]:
    return simulate_targets(
        market_data,
        build_monthly_targets(market_data, official_sessions, strategy_id),
        official_sessions,
        config,
    )


def phase_metrics(
    daily_nav_or_result: pd.DataFrame | Mapping[str, Any],
    *,
    start: Any | None = None,
    end: Any | None = None,
) -> dict[str, Any]:
    """Compute one inclusive phase, retaining first-day execution return."""

    daily_nav = (
        daily_nav_or_result.get("daily_nav")
        if isinstance(daily_nav_or_result, Mapping)
        else daily_nav_or_result
    )
    if not isinstance(daily_nav, pd.DataFrame) or daily_nav.empty:
        raise ValueError("phase_metrics requires a non-empty daily_nav DataFrame")
    if not {"trade_date", "nav"}.issubset(daily_nav.columns):
        raise ValueError("daily_nav must contain trade_date and nav")
    full = daily_nav.copy()
    full["trade_date"] = pd.to_datetime(full["trade_date"], errors="coerce").dt.normalize()
    full["nav"] = pd.to_numeric(full["nav"], errors="coerce")
    if full[["trade_date", "nav"]].isna().any().any() or not np.isfinite(full["nav"]).all() or bool(full["nav"].le(0.0).any()):
        raise ValueError("daily_nav contains invalid dates or NAV")
    full = full.sort_values("trade_date", kind="mergesort").reset_index(drop=True)
    if full["trade_date"].duplicated().any():
        raise ValueError("daily_nav contains duplicate dates")
    full["derived_return"] = full["nav"].pct_change()
    phase = full
    if start is not None:
        phase = phase.loc[phase["trade_date"].ge(pd.Timestamp(start).normalize())]
    if end is not None:
        phase = phase.loc[phase["trade_date"].le(pd.Timestamp(end).normalize())]
    if len(phase) < 2:
        raise ValueError("phase requires at least two daily observations")
    first_full_index = int(phase.index[0])
    has_seed = bool(start is not None and first_full_index > 0)
    if start is not None and not has_seed:
        raise ValueError("phase start requires an explicit preceding seed NAV")
    baseline_nav = (
        float(full.loc[first_full_index - 1, "nav"])
        if has_seed
        else float(phase["nav"].iloc[0])
    )
    baseline_date = (
        pd.Timestamp(full.loc[first_full_index - 1, "trade_date"])
        if has_seed
        else pd.Timestamp(phase["trade_date"].iloc[0])
    )
    phase = phase.reset_index(drop=True)
    intervals = len(phase) if has_seed else len(phase) - 1
    end_nav = float(phase["nav"].iloc[-1])
    returns = phase["derived_return"].dropna().to_numpy(dtype=float)
    std = float(np.std(returns, ddof=1)) if len(returns) > 1 else 0.0
    sharpe = (
        float(np.mean(returns)) / std * math.sqrt(TRADING_DAYS_PER_YEAR)
        if std > 1e-15
        else 0.0
    )
    drawdown_source = pd.concat(
        [pd.Series([baseline_nav]), phase["nav"]], ignore_index=True
    ) if has_seed else phase["nav"].reset_index(drop=True)
    drawdown = drawdown_source / drawdown_source.cummax() - 1.0
    phase_drawdown = drawdown.iloc[1:] if has_seed else drawdown

    complete_year_returns: dict[str, float] = {}
    for year, group in phase.groupby(phase["trade_date"].dt.year, sort=True):
        if (
            int(group["trade_date"].dt.month.iloc[0]) == 1
            and int(group["trade_date"].dt.day.iloc[0]) <= 10
            and int(group["trade_date"].dt.month.iloc[-1]) == 12
            and int(group["trade_date"].dt.day.iloc[-1]) >= 20
            and len(group) >= 230
        ):
            first_index = int(group.index[0])
            denominator = (
                float(phase.loc[first_index - 1, "nav"])
                if first_index > 0
                else baseline_nav
            )
            complete_year_returns[str(int(year))] = (
                float(group["nav"].iloc[-1]) / denominator - 1.0
            )
    positive_years = sum(value > 0.0 for value in complete_year_returns.values())

    def total(column: str) -> float:
        if column not in phase:
            return 0.0
        values = pd.to_numeric(phase[column], errors="coerce")
        if values.isna().any() or not np.isfinite(values).all():
            raise ValueError(f"daily_nav contains invalid {column}")
        return math.fsum(float(value) for value in values)

    requested = total("requested_notional")
    executed = total("executed_notional")
    capacity_limited = total("capacity_limited_requested_notional")
    turnover = total("turnover")
    if "accounting_error" in phase:
        accounting_values = pd.to_numeric(phase["accounting_error"], errors="coerce")
        if accounting_values.isna().any() or not np.isfinite(accounting_values).all():
            raise ValueError("daily_nav contains invalid accounting_error")
        max_error = float(accounting_values.abs().max())
    else:
        max_error = 0.0
    total_return = end_nav / baseline_nav - 1.0
    return {
        "observations": int(len(phase)),
        "start_date": phase["trade_date"].iloc[0].date().isoformat(),
        "performance_start": phase["trade_date"].iloc[0].date().isoformat(),
        "baseline_date": baseline_date.date().isoformat(),
        "end_date": phase["trade_date"].iloc[-1].date().isoformat(),
        "start_nav": baseline_nav,
        "end_nav": end_nav,
        "total_return": total_return,
        "cagr": (1.0 + total_return) ** (TRADING_DAYS_PER_YEAR / intervals) - 1.0,
        "sharpe": sharpe,
        "max_drawdown": float(phase_drawdown.min()),
        "turnover": turnover,
        "annualized_turnover": turnover * TRADING_DAYS_PER_YEAR / intervals,
        "complete_year_returns": complete_year_returns,
        "complete_year_count": len(complete_year_returns),
        "positive_complete_year_count": positive_years,
        "positive_complete_year_ratio": (
            positive_years / len(complete_year_returns)
            if complete_year_returns
            else 0.0
        ),
        "requested_notional_fill_ratio": executed / requested if requested else 1.0,
        "capacity_limited_requested_notional_ratio": (
            capacity_limited / requested if requested else 0.0
        ),
        "max_abs_accounting_error": max_error,
        "nav_reconciliation_error": max_error,
    }


def combine_phase_metrics(
    base_candidate: Mapping[str, Any],
    stress_candidate: Mapping[str, Any],
    control: Mapping[str, Any],
) -> dict[str, Any]:
    """Attach exact stress and candidate-minus-control metrics."""

    required = {"observations", "start_date", "end_date", "cagr", "sharpe", "max_drawdown"}
    for role, values in (
        ("base_candidate", base_candidate),
        ("stress_candidate", stress_candidate),
        ("control", control),
    ):
        missing = sorted(required - set(values))
        if missing:
            raise ValueError(f"{role} phase metrics missing: {missing}")
    identity = tuple(base_candidate[key] for key in ("observations", "start_date", "end_date"))
    for role, values in (("stress_candidate", stress_candidate), ("control", control)):
        if tuple(values[key] for key in ("observations", "start_date", "end_date")) != identity:
            raise ValueError(f"{role} must use exact candidate phase dates")
    output = dict(base_candidate)
    output.update(
        {
            "stress_cagr": float(stress_candidate["cagr"]),
            "stress_cost_cagr": float(stress_candidate["cagr"]),
            "relative_cagr": float(base_candidate["cagr"]) - float(control["cagr"]),
            "relative_sharpe": float(base_candidate["sharpe"]) - float(control["sharpe"]),
            "relative_max_drawdown": float(base_candidate["max_drawdown"])
            - float(control["max_drawdown"]),
            "stress": dict(stress_candidate),
            "control": dict(control),
        }
    )
    return output


_GateSpec = tuple[str, Callable[[float, float], bool]]
_GATE_SPECS: dict[str, _GateSpec] = {
    "observations_min": ("observations", ge),
    "cagr_min": ("cagr", ge),
    "sharpe_min": ("sharpe", ge),
    "max_drawdown_min": ("max_drawdown", ge),
    "positive_complete_years_min": ("positive_complete_year_count", ge),
    "positive_complete_year_ratio_min": ("positive_complete_year_ratio", ge),
    "requested_notional_fill_ratio_min": ("requested_notional_fill_ratio", ge),
    "capacity_limited_requested_notional_ratio_max": (
        "capacity_limited_requested_notional_ratio",
        le,
    ),
    "max_abs_accounting_error_max": ("max_abs_accounting_error", le),
    "turnover_max": ("turnover", le),
    "annualized_turnover_max": ("annualized_turnover", le),
    "stress_cagr_min": ("stress_cagr", ge),
    "relative_cagr_min": ("relative_cagr", ge),
    "relative_sharpe_min": ("relative_sharpe", ge),
    "relative_max_drawdown_min": ("relative_max_drawdown", ge),
}
_PROTOCOL_GATE_SPECS: dict[str, _GateSpec] = {
    "net_sharpe_at_least": ("sharpe", ge),
    "daily_max_drawdown_at_least": ("max_drawdown", ge),
    "positive_complete_year_count_at_least": ("positive_complete_year_count", ge),
    "relative_cagr_at_least": ("relative_cagr", ge),
    "relative_sharpe_at_least": ("relative_sharpe", ge),
    "relative_max_drawdown_at_least": ("relative_max_drawdown", ge),
    "requested_notional_fill_ratio_at_least": ("requested_notional_fill_ratio", ge),
    "capacity_limited_requested_notional_ratio_at_most": (
        "capacity_limited_requested_notional_ratio",
        le,
    ),
    "nav_reconciliation_error_at_most": ("nav_reconciliation_error", le),
}


def evaluate_phase_gate(
    metrics: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    special = {
        "net_cagr_strictly_positive",
        "stress_cost_cagr_strictly_positive",
        "future_input_violation_count",
    }
    allowed = set(_GATE_SPECS) | set(_PROTOCOL_GATE_SPECS) | special
    unknown = sorted(set(config) - allowed)
    if unknown:
        raise ValueError(f"unknown phase-gate thresholds: {unknown}")
    checks: dict[str, bool] = {}
    values: dict[str, dict[str, float]] = {}
    for name, raw_threshold in config.items():
        if name in {"net_cagr_strictly_positive", "stress_cost_cagr_strictly_positive"}:
            if raw_threshold is not True:
                raise ValueError(f"{name} must be true")
            metric_name = "cagr" if name.startswith("net_") else "stress_cost_cagr"
            threshold = 0.0
            comparison: Callable[[float, float], bool] = lambda value, _: value > 0.0
        elif name == "future_input_violation_count":
            metric_name = name
            threshold = float(raw_threshold)
            comparison = lambda value, bound: value == bound
        else:
            metric_name, comparison = _GATE_SPECS.get(name) or _PROTOCOL_GATE_SPECS[name]
            threshold = float(raw_threshold)
        if metric_name not in metrics:
            raise ValueError(f"phase metrics missing required value: {metric_name}")
        metric_value = float(metrics[metric_name])
        passed = bool(
            math.isfinite(metric_value)
            and math.isfinite(threshold)
            and comparison(metric_value, threshold)
        )
        checks[name] = passed
        values[name] = {"metric": metric_value, "threshold": threshold}
    return {"passed": bool(all(checks.values())), "checks": checks, "values": values}


def selection_gate(
    train_metrics: Mapping[str, Any],
    validation_metrics: Mapping[str, Any] | None,
    *,
    train_config: Mapping[str, Any],
    validation_config: Mapping[str, Any],
) -> dict[str, Any]:
    train_gate = evaluate_phase_gate(train_metrics, train_config)
    if not train_gate["passed"]:
        return {
            "status": "train_failed",
            "candidate_id": CANDIDATE_ID,
            "selected_candidate_id": None,
            "train_gate": train_gate,
            "validation_evaluated": False,
            "validation_gate": None,
        }
    if validation_metrics is None:
        raise ValueError("validation metrics are required after train passes")
    validation_gate = evaluate_phase_gate(validation_metrics, validation_config)
    return {
        "status": "selected" if validation_gate["passed"] else "validation_failed",
        "candidate_id": CANDIDATE_ID,
        "selected_candidate_id": CANDIDATE_ID if validation_gate["passed"] else None,
        "train_gate": train_gate,
        "validation_evaluated": True,
        "validation_gate": validation_gate,
    }
