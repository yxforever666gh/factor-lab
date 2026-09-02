"""Strict staged A/B/C minute execution for the 13.0 real-share account.

The public API deliberately accepts one completed execution window at a time.
Window A cannot receive B/C columns, window B cannot receive C columns, and a
state transition succeeds only once and in protocol order.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass, replace
from math import floor, fsum, isfinite
from typing import Any, Mapping

import pandas as pd

from factor_lab.portfolio.execution import (
    calculate_trade_costs,
    maximum_executable_notional,
    validate_long_only_targets,
)
from factor_lab.research.pit_stock import PITStockContractError, canonical_sha256
from factor_lab.research.pit_stock_real_account import (
    PnlPosting,
    RawMark,
    RealShareAccount,
    RealSharePolicy,
)


MINUTE_EXECUTION_CONTEXT_COLUMNS = (
    "ticker",
    "signal_adv20",
    "signal_vol_daily",
    "up_limit",
    "down_limit",
)
MINUTE_EXECUTION_BAR_COLUMNS = (
    "ticker",
    "trade_time",
    "observable_at",
    "open",
    "high",
    "low",
    "close",
    "volume_shares",
    "amount_rmb",
)
WINDOW_SPECS = {
    "A": ("09:35:00", "09:36:00"),
    "B": ("09:41:00", "09:42:00"),
    "C": ("09:47:00", "09:48:00"),
}
DECISION_TIMES = {
    "A": "09:30_after_open_events",
    "B": "09:36:00",
    "C": "09:42:00",
}
PHASE_A = "awaiting_window_A"
PHASE_B = "awaiting_window_B"
PHASE_C = "awaiting_window_C"
PHASE_COMPLETE = "complete"
_TOLERANCE = 1e-8


@dataclass(frozen=True)
class SequentialMinutePolicy:
    real_share: RealSharePolicy
    max_window_amount_participation: float = 0.05
    buy_limit_premium: float = 0.01

    def __post_init__(self) -> None:
        if not 0.0 < float(self.max_window_amount_participation) <= 1.0:
            raise ValueError("window participation must be in (0,1]")
        if not 0.0 <= float(self.buy_limit_premium) <= 0.10:
            raise ValueError("buy limit premium must be in [0,0.10]")


@dataclass(frozen=True)
class MinuteExecutionContext:
    ticker: str
    signal_adv20: float | None
    signal_vol_daily: float | None
    up_limit: float | None
    down_limit: float | None


@dataclass(frozen=True)
class MinuteWindowBar:
    ticker: str
    trade_time: str
    observable_at: str
    open: float
    high: float
    low: float
    close: float
    volume_shares: float
    amount_rmb: float
    vwap: float | None


@dataclass(frozen=True)
class MinuteOrderPlan:
    order_id: str
    window: str
    decision_time: str
    ticker: str
    side: str
    target_weight: float
    decision_price: float
    target_gap_notional: float
    requested_shares: float
    planned_shares: int
    available_tradable_shares: int
    full_exit: bool
    limit_price: float | None = None
    reserved_cash: float = 0.0
    preblock_reason: str | None = None
    lot_limited: bool = False
    cash_scaled: bool = False
    pending_limited: bool = False


@dataclass(frozen=True)
class MinuteRealShareOrder:
    order_id: str
    date: str
    decision_time: str
    window: str
    trade_time: str
    observable_at: str
    ticker: str
    side: str
    target_weight: float
    decision_price: float
    target_gap_notional: float
    requested_shares: float
    requested_notional: float
    planned_shares: int
    reserved_cash: float
    filled_decision_notional: float
    available_tradable_shares: int
    executed_shares: int
    executed_notional: float
    execution_vwap: float | None
    limit_price: float | None
    status: str
    block_reason: str | None
    signal_adv_limited: bool
    window_capacity_limited: bool
    lot_limited: bool
    cash_scaled: bool
    pending_limited: bool
    costs: dict[str, float]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SequentialMinuteExecutionResult:
    pretrade_nav: float
    after_window_a_nav: float
    after_window_b_nav: float
    posttrade_nav: float
    target_weights: dict[str, float]
    orders: tuple[MinuteRealShareOrder, ...]
    total_cost: float
    reconciliation_error: float


@dataclass
class SequentialMinuteState:
    phase: str
    trade_date: str
    period_signal: str
    policy: SequentialMinutePolicy
    targets: tuple[tuple[str, float], ...]
    contexts: tuple[MinuteExecutionContext, ...]
    pretrade_nav: float
    after_window_a_nav: float | None
    after_window_b_nav: float | None
    cash_snapshot_c: float | None
    planned_orders: tuple[MinuteOrderPlan, ...]
    required_bar_tickers: tuple[str, ...]
    used_signal_notional: tuple[tuple[str, float], ...]
    orders: tuple[MinuteRealShareOrder, ...]
    posting_start: int
    working_account: RealShareAccount
    staged_postings: tuple[PnlPosting, ...]
    account_fingerprint: str
    postings_fingerprint: str
    working_fingerprint: str
    staged_postings_fingerprint: str
    result: SequentialMinuteExecutionResult | None = None

    @property
    def target_map(self) -> dict[str, float]:
        return dict(self.targets)

    @property
    def context_map(self) -> dict[str, MinuteExecutionContext]:
        return {value.ticker: value for value in self.contexts}


@dataclass(frozen=True)
class _Capacity:
    executable_shares: int
    signal_executable_shares: int
    window_executable_shares: int


@dataclass(frozen=True)
class _PreparedBuy:
    plan: MinuteOrderPlan
    bar: MinuteWindowBar
    shares: int
    notional: float
    costs: dict[str, float]
    order: MinuteRealShareOrder


def _date(value: Any) -> pd.Timestamp:
    result = pd.Timestamp(value)
    if pd.isna(result):
        raise PITStockContractError("minute execution date is unknown")
    if result.tzinfo is not None:
        result = result.tz_convert("Asia/Shanghai").tz_localize(None)
    return result.normalize()


def _timestamp(value: Any, *, field: str) -> pd.Timestamp:
    result = pd.Timestamp(value)
    if pd.isna(result):
        raise PITStockContractError(f"minute {field} is unknown")
    if result.tzinfo is not None:
        result = result.tz_convert("Asia/Shanghai").tz_localize(None)
    return result


def _account_payload(account: RealShareAccount) -> dict[str, Any]:
    return {
        "cash": float(account.cash),
        "positions": sorted((str(k), int(v)) for k, v in account.positions.items()),
        "pending_shares": [
            asdict(account.pending_shares[key])
            for key in sorted(account.pending_shares)
        ],
        "receivables": [
            asdict(account.receivables[key]) for key in sorted(account.receivables)
        ],
        "marks": [
            {"ticker": key, **asdict(account.marks[key])}
            for key in sorted(account.marks)
        ],
        "record_entitlements": sorted(
            (str(k), int(v)) for k, v in account.record_entitlements.items()
        ),
        "applied_stages": sorted(
            tuple(map(str, value)) for value in account.applied_stages
        ),
        "extinguished_pending": sorted(map(str, account.extinguished_pending)),
    }


def _account_fingerprint(account: RealShareAccount) -> str:
    return canonical_sha256(_account_payload(account))


def _postings_fingerprint(postings: list[PnlPosting]) -> str:
    return canonical_sha256([value.to_dict() for value in postings])


def _copy_account(destination: RealShareAccount, source: RealShareAccount) -> None:
    destination.cash = source.cash
    destination.positions = source.positions
    destination.pending_shares = source.pending_shares
    destination.receivables = source.receivables
    destination.marks = source.marks
    destination.record_entitlements = source.record_entitlements
    destination.applied_stages = source.applied_stages
    destination.extinguished_pending = source.extinguished_pending


def _post(
    postings: list[PnlPosting],
    *,
    date: pd.Timestamp,
    phase: str,
    period_signal: str,
    ticker: str,
    kind: str,
    amount: float,
    order_id: str | None = None,
) -> None:
    value = float(amount)
    if not isfinite(value):
        raise PITStockContractError("minute execution posting is invalid")
    if abs(value) > 1e-12:
        postings.append(
            PnlPosting(
                date=date.date().isoformat(),
                phase=phase,
                period_signal=period_signal,
                ticker=ticker,
                kind=kind,
                amount=value,
                order_id=order_id,
            )
        )


def _contexts(
    frame: pd.DataFrame, *, expected_tickers: set[str]
) -> tuple[MinuteExecutionContext, ...]:
    if tuple(map(str, frame.columns)) != MINUTE_EXECUTION_CONTEXT_COLUMNS:
        raise PITStockContractError("minute execution context columns differ")
    tickers = frame["ticker"].astype(str)
    if tickers.duplicated().any() or set(tickers) != expected_tickers:
        raise PITStockContractError("minute execution context scope differs")
    result: list[MinuteExecutionContext] = []
    for row in frame.to_dict("records"):
        ticker = str(row["ticker"])
        values: dict[str, float | None] = {}
        for field in ("signal_adv20", "signal_vol_daily"):
            raw = row[field]
            if pd.isna(raw):
                values[field] = None
                continue
            value = float(raw)
            if not isfinite(value):
                raise PITStockContractError("minute signal input is infinite")
            if field == "signal_adv20" and value <= 0.0:
                raise PITStockContractError("minute signal ADV is not positive")
            if field == "signal_vol_daily" and value < 0.0:
                raise PITStockContractError("minute signal volatility is negative")
            values[field] = value
        raw_up, raw_down = row["up_limit"], row["down_limit"]
        if pd.isna(raw_up) or pd.isna(raw_down):
            if not (pd.isna(raw_up) and pd.isna(raw_down)):
                raise PITStockContractError("minute daily price limits are incomplete")
            up = down = None
        else:
            up = float(raw_up)
            down = float(raw_down)
            if (
                not all(isfinite(value) and value > 0.0 for value in (up, down))
                or down > up
            ):
                raise PITStockContractError("minute daily price limits are invalid")
        result.append(
            MinuteExecutionContext(
                ticker=ticker,
                signal_adv20=values["signal_adv20"],
                signal_vol_daily=values["signal_vol_daily"],
                up_limit=up,
                down_limit=down,
            )
        )
    return tuple(sorted(result, key=lambda value: value.ticker))


def _bars(
    frame: pd.DataFrame,
    *,
    window: str,
    date: pd.Timestamp,
    expected_tickers: set[str],
    complete_no_bar_tickers: set[str] | None,
) -> dict[str, MinuteWindowBar]:
    if tuple(map(str, frame.columns)) != MINUTE_EXECUTION_BAR_COLUMNS:
        raise PITStockContractError(
            "minute window columns differ or contain future data"
        )
    unavailable = {str(value) for value in (complete_no_bar_tickers or set())}
    if not unavailable.issubset(expected_tickers):
        raise PITStockContractError("minute complete-no-bar scope differs")
    tickers = frame["ticker"].astype(str)
    if tickers.duplicated().any():
        raise PITStockContractError("minute window tickers are duplicate")
    observed = set(tickers)
    if observed & unavailable or observed | unavailable != expected_tickers:
        raise PITStockContractError("minute partition is missing or unverified")
    trade_clock, observable_clock = WINDOW_SPECS[window]
    expected_trade = pd.Timestamp(f"{date.date()} {trade_clock}")
    expected_observable = pd.Timestamp(f"{date.date()} {observable_clock}")
    result: dict[str, MinuteWindowBar] = {}
    for row in frame.to_dict("records"):
        ticker = str(row["ticker"])
        trade_time = _timestamp(row["trade_time"], field="trade_time")
        observable_at = _timestamp(row["observable_at"], field="observable_at")
        if trade_time != expected_trade or observable_at != expected_observable:
            raise PITStockContractError("minute window time/observability differs")
        numeric: dict[str, float] = {}
        for field in ("open", "high", "low", "close", "volume_shares", "amount_rmb"):
            value = float(row[field])
            if not isfinite(value):
                raise PITStockContractError(f"minute window {field} is invalid")
            numeric[field] = value
        if min(numeric[field] for field in ("open", "high", "low", "close")) <= 0.0:
            raise PITStockContractError("minute window price is not positive")
        if (
            numeric["high"] < max(numeric["open"], numeric["close"], numeric["low"])
            or numeric["low"] > min(numeric["open"], numeric["close"], numeric["high"])
            or numeric["volume_shares"] < 0.0
            or numeric["amount_rmb"] < 0.0
            or (numeric["volume_shares"] == 0.0) != (numeric["amount_rmb"] == 0.0)
        ):
            raise PITStockContractError("minute window geometry/liquidity differs")
        vwap = None
        if numeric["volume_shares"] > 0.0:
            vwap = numeric["amount_rmb"] / numeric["volume_shares"]
            if not (
                numeric["low"] - 0.005 - 1e-12
                <= vwap
                <= numeric["high"] + 0.005 + 1e-12
            ):
                raise PITStockContractError("minute window VWAP escapes OHLC")
        result[ticker] = MinuteWindowBar(
            ticker=ticker,
            trade_time=trade_time.strftime("%Y-%m-%d %H:%M:%S"),
            observable_at=observable_at.strftime("%Y-%m-%d %H:%M:%S"),
            vwap=vwap,
            **numeric,
        )
    return result


def _assert_transition(
    state: SequentialMinuteState,
    *,
    phase: str,
    account: RealShareAccount,
    postings: list[PnlPosting],
) -> None:
    if state.phase != phase:
        raise PITStockContractError(
            f"minute state transition requires {phase}, found {state.phase}"
        )
    if _account_fingerprint(account) != state.account_fingerprint:
        raise PITStockContractError("minute account changed outside its state")
    if _postings_fingerprint(postings) != state.postings_fingerprint:
        raise PITStockContractError("minute postings changed outside their state")
    if _account_fingerprint(state.working_account) != state.working_fingerprint:
        raise PITStockContractError("minute staged account changed outside its state")
    if (
        _postings_fingerprint(list(state.staged_postings))
        != state.staged_postings_fingerprint
    ):
        raise PITStockContractError(
            "minute staged postings changed outside their state"
        )


def _minimum_costs(
    *,
    notional: float,
    side: str,
    window_amount: float,
    volatility: float,
    date: pd.Timestamp,
    policy: SequentialMinutePolicy,
) -> dict[str, float]:
    result = calculate_trade_costs(
        notional=notional,
        side=side,
        adv=window_amount,
        volatility=volatility,
        trade_date=date,
        policy=policy.real_share.costs,
    )
    commission = max(
        float(result["commission"]),
        float(policy.real_share.minimum_commission_rmb),
    )
    result["total"] += commission - float(result["commission"])
    result["commission"] = commission
    return result


def _worst_buy_reserve(
    *,
    shares: int,
    limit_price: float,
    volatility: float,
    date: pd.Timestamp,
    policy: SequentialMinutePolicy,
) -> float:
    notional = int(shares) * float(limit_price)
    if notional <= 0.0:
        return 0.0
    costs = _minimum_costs(
        notional=notional,
        side="buy",
        window_amount=notional,
        volatility=volatility,
        date=date,
        policy=policy,
    )
    return notional + float(costs["total"])


def _signal(context: MinuteExecutionContext) -> tuple[float, float] | None:
    if context.signal_adv20 is None or context.signal_vol_daily is None:
        return None
    return float(context.signal_adv20), float(context.signal_vol_daily)


def _one_price_limit(
    context: MinuteExecutionContext,
    bar: MinuteWindowBar,
    *,
    side: str,
) -> bool:
    if bar.vwap is None:
        return False
    limit = context.up_limit if side == "buy" else context.down_limit
    if limit is None:
        raise PITStockContractError(
            "tradable minute order lacks exact daily price limits"
        )
    return (
        abs(bar.high - bar.low) <= 1e-12
        and abs(bar.vwap - limit) <= 0.005 + 1e-12
    )


def _capacity(
    *,
    price: float,
    signal_adv: float,
    used_signal_notional: float,
    window_amount: float,
    policy: SequentialMinutePolicy,
) -> _Capacity:
    signal_total = maximum_executable_notional(
        signal_adv, policy.real_share.max_adv_participation
    )
    signal_remaining = max(signal_total - float(used_signal_notional), 0.0)
    window_cap = maximum_executable_notional(
        window_amount, policy.max_window_amount_participation
    )
    lot = policy.real_share.lot_size
    signal_shares = floor(signal_remaining / price / lot) * lot
    window_shares = floor(window_cap / price / lot) * lot
    return _Capacity(
        executable_shares=int(max(min(signal_shares, window_shares), 0)),
        signal_executable_shares=int(max(signal_shares, 0)),
        window_executable_shares=int(max(window_shares, 0)),
    )


def _mark_to(
    account: RealShareAccount,
    *,
    ticker: str,
    price: float,
    date: pd.Timestamp,
    mark_kind: str,
    postings: list[PnlPosting],
    period_signal: str,
) -> None:
    owned = account.owned_shares(ticker)
    if owned <= 0:
        return
    old = account.marks.get(ticker)
    if old is None:
        raise PITStockContractError("minute holding lacks a carrying mark")
    value = float(price)
    if not isfinite(value) or value <= 0.0:
        raise PITStockContractError("minute mark price is invalid")
    account.marks[ticker] = RawMark(value, date.date().isoformat(), mark_kind)
    _post(
        postings,
        date=date,
        phase=f"new_period_{mark_kind}",
        period_signal=period_signal,
        ticker=ticker,
        kind="minute_mark",
        amount=owned * (value - old.price),
    )


def _mark_window_close(
    account: RealShareAccount,
    *,
    bars: Mapping[str, MinuteWindowBar],
    window: str,
    date: pd.Timestamp,
    postings: list[PnlPosting],
    period_signal: str,
) -> None:
    for ticker in sorted(account.required_tickers()):
        bar = bars.get(ticker)
        if bar is not None:
            _mark_to(
                account,
                ticker=ticker,
                price=bar.close,
                date=date,
                mark_kind=f"window_{window}_close",
                postings=postings,
                period_signal=period_signal,
            )


def _requested_notional(plan: MinuteOrderPlan) -> float:
    if plan.decision_price <= 0.0:
        return 0.0
    return float(plan.requested_shares) * float(plan.decision_price)


def _order(
    *,
    date: pd.Timestamp,
    plan: MinuteOrderPlan,
    status: str,
    block_reason: str | None,
    executed_shares: int = 0,
    executed_notional: float = 0.0,
    execution_vwap: float | None = None,
    signal_adv_limited: bool = False,
    window_capacity_limited: bool = False,
    costs: dict[str, float] | None = None,
) -> MinuteRealShareOrder:
    trade_clock, observable_clock = WINDOW_SPECS[plan.window]
    return MinuteRealShareOrder(
        order_id=plan.order_id,
        date=date.date().isoformat(),
        decision_time=plan.decision_time,
        window=plan.window,
        trade_time=f"{date.date()} {trade_clock}",
        observable_at=f"{date.date()} {observable_clock}",
        ticker=plan.ticker,
        side=plan.side,
        target_weight=plan.target_weight,
        decision_price=plan.decision_price,
        target_gap_notional=plan.target_gap_notional,
        requested_shares=plan.requested_shares,
        requested_notional=_requested_notional(plan),
        planned_shares=plan.planned_shares,
        reserved_cash=plan.reserved_cash,
        filled_decision_notional=min(
            float(executed_shares), float(plan.requested_shares)
        )
        * max(plan.decision_price, 0.0),
        available_tradable_shares=plan.available_tradable_shares,
        executed_shares=int(executed_shares),
        executed_notional=float(executed_notional),
        execution_vwap=execution_vwap,
        limit_price=plan.limit_price,
        status=status,
        block_reason=block_reason,
        signal_adv_limited=bool(signal_adv_limited),
        window_capacity_limited=bool(window_capacity_limited),
        lot_limited=plan.lot_limited,
        cash_scaled=plan.cash_scaled,
        pending_limited=plan.pending_limited,
        costs=dict(costs or {}),
    )


def _execute_sell(
    account: RealShareAccount,
    *,
    plan: MinuteOrderPlan,
    bar: MinuteWindowBar | None,
    context: MinuteExecutionContext,
    date: pd.Timestamp,
    policy: SequentialMinutePolicy,
    postings: list[PnlPosting],
    period_signal: str,
    used_signal_notional: dict[str, float],
) -> MinuteRealShareOrder:
    available = int(account.positions.get(plan.ticker, 0))
    if available != plan.available_tradable_shares:
        raise PITStockContractError("minute sell plan no longer matches settled shares")
    if bar is None:
        return _order(
            date=date,
            plan=plan,
            status="blocked",
            block_reason=f"missing_window_{plan.window}",
        )
    if bar.vwap is None:
        return _order(
            date=date,
            plan=plan,
            status="blocked",
            block_reason="zero_window_liquidity",
        )
    if _one_price_limit(context, bar, side="sell"):
        return _order(
            date=date,
            plan=plan,
            status="blocked",
            block_reason="one_price_limit_down",
            execution_vwap=bar.vwap,
        )
    signal = _signal(context)
    if signal is None:
        return _order(
            date=date,
            plan=plan,
            status="blocked",
            block_reason="missing_signal_adv_or_volatility",
            execution_vwap=bar.vwap,
            signal_adv_limited=True,
        )
    adv, volatility = signal
    used = used_signal_notional.get(plan.ticker, 0.0)
    cap = _capacity(
        price=bar.vwap,
        signal_adv=adv,
        used_signal_notional=used,
        window_amount=bar.amount_rmb,
        policy=policy,
    )
    signal_remaining = max(
        maximum_executable_notional(adv, policy.real_share.max_adv_participation)
        - used,
        0.0,
    )
    window_cap = maximum_executable_notional(
        bar.amount_rmb, policy.max_window_amount_participation
    )
    if (
        plan.full_exit
        and available * bar.vwap
        <= min(signal_remaining, window_cap) + 1e-9
    ):
        executed = available
        signal_possible = available
        window_possible = available
    else:
        executed = min(plan.planned_shares, available, cap.executable_shares)
        signal_possible = cap.signal_executable_shares
        window_possible = cap.window_executable_shares
    signal_limited = signal_possible < plan.planned_shares
    window_limited = window_possible < plan.planned_shares
    if executed <= 0:
        return _order(
            date=date,
            plan=plan,
            status="unfilled",
            block_reason=None,
            execution_vwap=bar.vwap,
            signal_adv_limited=signal_limited,
            window_capacity_limited=window_limited,
        )
    _mark_to(
        account,
        ticker=plan.ticker,
        price=bar.vwap,
        date=date,
        mark_kind=f"window_{plan.window}_vwap",
        postings=postings,
        period_signal=period_signal,
    )
    notional = int(executed) * bar.vwap
    costs = _minimum_costs(
        notional=notional,
        side="sell",
        window_amount=bar.amount_rmb,
        volatility=volatility,
        date=date,
        policy=policy,
    )
    account.positions[plan.ticker] -= int(executed)
    if account.positions[plan.ticker] == 0:
        account.positions.pop(plan.ticker)
    account.cash += notional - float(costs["total"])
    if account.cash < -_TOLERANCE:
        raise PITStockContractError("minute sell fees would borrow cash")
    used_signal_notional[plan.ticker] = used + notional
    _post(
        postings,
        date=date,
        phase=f"new_period_window_{plan.window}",
        period_signal=period_signal,
        ticker=plan.ticker,
        kind="trade_cost",
        amount=-float(costs["total"]),
        order_id=plan.order_id,
    )
    return _order(
        date=date,
        plan=plan,
        status="executed",
        block_reason=None,
        executed_shares=int(executed),
        executed_notional=notional,
        execution_vwap=bar.vwap,
        signal_adv_limited=signal_limited,
        window_capacity_limited=window_limited,
        costs=costs,
    )


def _assert_reconciliation(
    *, start_nav: float, end_nav: float, postings: list[PnlPosting]
) -> None:
    error = float(end_nav) - float(start_nav) - fsum(value.amount for value in postings)
    if not isfinite(error) or abs(error) > _TOLERANCE:
        raise PITStockContractError("minute window P&L does not reconcile")


def _commit(
    state: SequentialMinuteState,
    *,
    account: RealShareAccount,
    working: RealShareAccount,
    postings: list[PnlPosting],
    new_postings: list[PnlPosting],
    phase: str,
    planned_orders: tuple[MinuteOrderPlan, ...],
    required_bar_tickers: set[str],
    used_signal_notional: dict[str, float],
    orders: list[MinuteRealShareOrder],
    after_window_a_nav: float | None,
    after_window_b_nav: float | None,
    cash_snapshot_c: float | None,
    result: SequentialMinuteExecutionResult | None = None,
) -> None:
    staged = tuple((*state.staged_postings, *new_postings))
    publish = phase == PHASE_COMPLETE
    if publish:
        _copy_account(account, working)
        postings.extend(staged)
    state.phase = phase
    state.planned_orders = planned_orders
    state.required_bar_tickers = tuple(sorted(required_bar_tickers))
    state.used_signal_notional = tuple(sorted(used_signal_notional.items()))
    state.orders = tuple(orders)
    state.after_window_a_nav = after_window_a_nav
    state.after_window_b_nav = after_window_b_nav
    state.cash_snapshot_c = cash_snapshot_c
    state.result = result
    state.working_account = working
    state.staged_postings = staged
    state.working_fingerprint = _account_fingerprint(working)
    state.staged_postings_fingerprint = _postings_fingerprint(list(staged))
    if publish:
        state.account_fingerprint = _account_fingerprint(account)
        state.postings_fingerprint = _postings_fingerprint(postings)


def begin_sequential_minute_rebalance(
    account: RealShareAccount,
    targets: Mapping[str, float],
    context: pd.DataFrame,
    *,
    trade_date: Any,
    policy: SequentialMinutePolicy,
    postings: list[PnlPosting],
    period_signal: str,
    raw_open_carried_tickers: set[str] | None = None,
) -> SequentialMinuteState:
    """Freeze target-zero A orders without accepting any minute bars."""

    date = _date(trade_date)
    normalized = validate_long_only_targets(
        targets, max_position_weight=policy.real_share.max_position_weight
    )
    expected = account.required_tickers() | set(normalized)
    contexts = _contexts(context, expected_tickers=expected)
    carried = {str(value) for value in (raw_open_carried_tickers or set())}
    if not carried.issubset(account.required_tickers()):
        raise PITStockContractError("raw-open carried ticker scope differs")
    for ticker in sorted(account.required_tickers()):
        mark = account.marks.get(ticker)
        if mark is None:
            raise PITStockContractError("minute holding lacks raw-open/carrying mark")
        if ticker not in carried and (
            mark.session != date.date().isoformat() or mark.kind != "open"
        ):
            raise PITStockContractError(
                "minute holding lacks current-session raw-open mark"
            )
    pretrade_nav = account.nav()
    if not isfinite(pretrade_nav) or pretrade_nav <= 0.0:
        raise PITStockContractError("minute pretrade NAV is not positive")
    plans: list[MinuteOrderPlan] = []
    for ticker in sorted(account.required_tickers()):
        if normalized.get(ticker, 0.0) != 0.0:
            continue
        available = int(account.positions.get(ticker, 0))
        reference = float(account.marks[ticker].price)
        plans.append(
            MinuteOrderPlan(
                order_id=f"{date.date()}:A:{ticker}:sell",
                window="A",
                decision_time=DECISION_TIMES["A"],
                ticker=ticker,
                side="sell",
                target_weight=0.0,
                decision_price=reference,
                target_gap_notional=available * reference,
                requested_shares=float(available),
                planned_shares=available,
                available_tradable_shares=available,
                full_exit=True,
                pending_limited=account.pending_by_ticker(ticker) > 0,
            )
        )
    return SequentialMinuteState(
        phase=PHASE_A,
        trade_date=date.date().isoformat(),
        period_signal=str(period_signal),
        policy=policy,
        targets=tuple(sorted(normalized.items())),
        contexts=contexts,
        pretrade_nav=pretrade_nav,
        after_window_a_nav=None,
        after_window_b_nav=None,
        cash_snapshot_c=None,
        planned_orders=tuple(plans),
        required_bar_tickers=tuple(sorted(account.required_tickers())),
        used_signal_notional=(),
        orders=(),
        posting_start=len(postings),
        working_account=deepcopy(account),
        staged_postings=(),
        account_fingerprint=_account_fingerprint(account),
        postings_fingerprint=_postings_fingerprint(postings),
        working_fingerprint=_account_fingerprint(account),
        staged_postings_fingerprint=_postings_fingerprint([]),
    )


def observe_window_a(
    state: SequentialMinuteState,
    account: RealShareAccount,
    bars: pd.DataFrame,
    *,
    postings: list[PnlPosting],
    complete_no_bar_tickers: set[str] | None = None,
) -> SequentialMinuteState:
    """Settle A, mark its close, then freeze B reduction quantities."""

    _assert_transition(state, phase=PHASE_A, account=account, postings=postings)
    date = _date(state.trade_date)
    observed = _bars(
        bars,
        window="A",
        date=date,
        expected_tickers=set(state.required_bar_tickers),
        complete_no_bar_tickers=complete_no_bar_tickers,
    )
    working = deepcopy(state.working_account)
    new_postings: list[PnlPosting] = []
    used = dict(state.used_signal_notional)
    orders = list(state.orders)
    contexts = state.context_map
    start_nav = working.nav()
    for plan in state.planned_orders:
        orders.append(
            _execute_sell(
                working,
                plan=plan,
                bar=observed.get(plan.ticker),
                context=contexts[plan.ticker],
                date=date,
                policy=state.policy,
                postings=new_postings,
                period_signal=state.period_signal,
                used_signal_notional=used,
            )
        )
    _mark_window_close(
        working,
        bars=observed,
        window="A",
        date=date,
        postings=new_postings,
        period_signal=state.period_signal,
    )
    after_a = working.nav()
    _assert_reconciliation(start_nav=start_nav, end_nav=after_a, postings=new_postings)
    targets = state.target_map
    plans: list[MinuteOrderPlan] = []
    lot = state.policy.real_share.lot_size
    for ticker in sorted(working.positions):
        weight = targets.get(ticker, 0.0)
        bar = observed.get(ticker)
        if weight <= 0.0 or bar is None:
            continue
        reference = bar.close
        economic = working.owned_shares(ticker) * reference
        excess = max(economic - after_a * weight, 0.0)
        if excess <= 1e-9:
            continue
        requested = min(excess / reference, working.positions[ticker])
        planned = floor(requested / lot) * lot
        plans.append(
            MinuteOrderPlan(
                order_id=f"{date.date()}:B:{ticker}:sell",
                window="B",
                decision_time=DECISION_TIMES["B"],
                ticker=ticker,
                side="sell",
                target_weight=weight,
                decision_price=reference,
                target_gap_notional=excess,
                requested_shares=float(requested),
                planned_shares=int(planned),
                available_tradable_shares=int(working.positions[ticker]),
                full_exit=False,
                lot_limited=planned + 1e-12 < requested,
                pending_limited=working.pending_by_ticker(ticker) > 0,
            )
        )
    next_scope = working.required_tickers() | set(targets)
    _commit(
        state,
        account=account,
        working=working,
        postings=postings,
        new_postings=new_postings,
        phase=PHASE_B,
        planned_orders=tuple(plans),
        required_bar_tickers=next_scope,
        used_signal_notional=used,
        orders=orders,
        after_window_a_nav=after_a,
        after_window_b_nav=None,
        cash_snapshot_c=None,
    )
    return state


def _plan_window_c(
    state: SequentialMinuteState,
    account: RealShareAccount,
    *,
    bars: Mapping[str, MinuteWindowBar],
    after_b_nav: float,
    date: pd.Timestamp,
) -> tuple[MinuteOrderPlan, ...]:
    targets = state.target_map
    contexts = state.context_map
    lot = state.policy.real_share.lot_size
    initial: list[MinuteOrderPlan] = []
    valid_indices: list[int] = []
    for ticker, weight in sorted(targets.items()):
        bar = bars.get(ticker)
        available = int(account.positions.get(ticker, 0))
        pending = account.pending_by_ticker(ticker) > 0
        if bar is None:
            gap = max(after_b_nav * weight, 0.0)
            if gap > 1e-9:
                initial.append(
                    MinuteOrderPlan(
                        order_id=f"{date.date()}:C:{ticker}:buy",
                        window="C",
                        decision_time=DECISION_TIMES["C"],
                        ticker=ticker,
                        side="buy",
                        target_weight=weight,
                        decision_price=0.0,
                        target_gap_notional=gap,
                        requested_shares=0.0,
                        planned_shares=0,
                        available_tradable_shares=available,
                        full_exit=False,
                        preblock_reason="missing_window_B",
                        pending_limited=pending,
                    )
                )
            continue
        reference = bar.close
        current = account.owned_shares(ticker) * reference
        gap = max(after_b_nav * weight - current, 0.0)
        if gap <= 1e-9:
            continue
        requested = floor(gap / reference / lot) * lot
        up_limit = contexts[ticker].up_limit
        if up_limit is None:
            raise PITStockContractError(
                "planned minute buy lacks exact daily price limits"
            )
        limit_price = min(
            up_limit,
            floor(reference * (1.0 + state.policy.buy_limit_premium) * 100.0 + 1e-12)
            / 100.0,
        )
        signal = _signal(contexts[ticker])
        plan = MinuteOrderPlan(
            order_id=f"{date.date()}:C:{ticker}:buy",
            window="C",
            decision_time=DECISION_TIMES["C"],
            ticker=ticker,
            side="buy",
            target_weight=weight,
            decision_price=reference,
            target_gap_notional=gap,
            requested_shares=float(requested),
            planned_shares=int(requested),
            available_tradable_shares=available,
            full_exit=False,
            limit_price=limit_price,
            preblock_reason=(
                None if signal is not None else "missing_signal_adv_or_volatility"
            ),
            lot_limited=requested * reference < gap - 1e-9,
            pending_limited=pending,
        )
        initial.append(plan)
        if signal is not None and requested > 0:
            valid_indices.append(len(initial) - 1)

    def reserved(scale: float) -> float:
        total = 0.0
        for index in valid_indices:
            plan = initial[index]
            context = contexts[plan.ticker]
            assert plan.limit_price is not None and context.signal_vol_daily is not None
            shares = floor(plan.requested_shares * scale / lot) * lot
            total += _worst_buy_reserve(
                shares=int(shares),
                limit_price=plan.limit_price,
                volatility=context.signal_vol_daily,
                date=date,
                policy=state.policy,
            )
        return total

    scale = 1.0
    if reserved(scale) > account.cash + 1e-9:
        low, high = 0.0, 1.0
        for _ in range(48):
            middle = (low + high) / 2.0
            if reserved(middle) <= account.cash + 1e-9:
                low = middle
            else:
                high = middle
        scale = low
    for index in valid_indices:
        plan = initial[index]
        context = contexts[plan.ticker]
        assert plan.limit_price is not None and context.signal_vol_daily is not None
        shares = floor(plan.requested_shares * scale / lot) * lot
        reserve = _worst_buy_reserve(
            shares=int(shares),
            limit_price=plan.limit_price,
            volatility=context.signal_vol_daily,
            date=date,
            policy=state.policy,
        )
        initial[index] = replace(
            plan,
            planned_shares=int(shares),
            reserved_cash=reserve,
            cash_scaled=scale < 1.0,
        )
    if fsum(value.reserved_cash for value in initial) > account.cash + 1e-8:
        raise PITStockContractError("minute C reservations exceed frozen cash")
    return tuple(initial)


def observe_window_b(
    state: SequentialMinuteState,
    account: RealShareAccount,
    bars: pd.DataFrame,
    *,
    postings: list[PnlPosting],
    complete_no_bar_tickers: set[str] | None = None,
) -> SequentialMinuteState:
    """Settle B, mark its close, then freeze cash-reserved C quantities."""

    _assert_transition(state, phase=PHASE_B, account=account, postings=postings)
    date = _date(state.trade_date)
    observed = _bars(
        bars,
        window="B",
        date=date,
        expected_tickers=set(state.required_bar_tickers),
        complete_no_bar_tickers=complete_no_bar_tickers,
    )
    working = deepcopy(state.working_account)
    new_postings: list[PnlPosting] = []
    used = dict(state.used_signal_notional)
    orders = list(state.orders)
    contexts = state.context_map
    start_nav = working.nav()
    for plan in state.planned_orders:
        orders.append(
            _execute_sell(
                working,
                plan=plan,
                bar=observed.get(plan.ticker),
                context=contexts[plan.ticker],
                date=date,
                policy=state.policy,
                postings=new_postings,
                period_signal=state.period_signal,
                used_signal_notional=used,
            )
        )
    _mark_window_close(
        working,
        bars=observed,
        window="B",
        date=date,
        postings=new_postings,
        period_signal=state.period_signal,
    )
    after_b = working.nav()
    _assert_reconciliation(start_nav=start_nav, end_nav=after_b, postings=new_postings)
    c_plans = _plan_window_c(
        state,
        working,
        bars=observed,
        after_b_nav=after_b,
        date=date,
    )
    c_scope = working.required_tickers() | {
        plan.ticker
        for plan in c_plans
        if plan.preblock_reason is None and plan.planned_shares > 0
    }
    _commit(
        state,
        account=account,
        working=working,
        postings=postings,
        new_postings=new_postings,
        phase=PHASE_C,
        planned_orders=c_plans,
        required_bar_tickers=c_scope,
        used_signal_notional=used,
        orders=orders,
        after_window_a_nav=state.after_window_a_nav,
        after_window_b_nav=after_b,
        cash_snapshot_c=working.cash,
    )
    return state


def _prepare_buy(
    *,
    state: SequentialMinuteState,
    plan: MinuteOrderPlan,
    bar: MinuteWindowBar | None,
    used_signal_notional: Mapping[str, float],
    date: pd.Timestamp,
) -> tuple[MinuteRealShareOrder, _PreparedBuy | None]:
    if plan.preblock_reason is not None:
        return (
            _order(
                date=date,
                plan=plan,
                status="blocked",
                block_reason=plan.preblock_reason,
            ),
            None,
        )
    if plan.planned_shares <= 0:
        return (
            _order(
                date=date,
                plan=plan,
                status="unfilled",
                block_reason=None,
                execution_vwap=None if bar is None else bar.vwap,
            ),
            None,
        )
    if bar is None:
        return (
            _order(
                date=date,
                plan=plan,
                status="blocked",
                block_reason="missing_window_C",
            ),
            None,
        )
    if bar.vwap is None:
        return (
            _order(
                date=date,
                plan=plan,
                status="blocked",
                block_reason="zero_window_liquidity",
            ),
            None,
        )
    context = state.context_map[plan.ticker]
    if _one_price_limit(context, bar, side="buy"):
        return (
            _order(
                date=date,
                plan=plan,
                status="blocked",
                block_reason="one_price_limit_up",
                execution_vwap=bar.vwap,
            ),
            None,
        )
    assert plan.limit_price is not None
    if bar.high > plan.limit_price + 1e-12:
        return (
            _order(
                date=date,
                plan=plan,
                status="blocked",
                block_reason="buy_limit_not_reached",
                execution_vwap=bar.vwap,
            ),
            None,
        )
    signal = _signal(context)
    if signal is None:
        raise PITStockContractError("unblocked C plan lost its signal inputs")
    adv, volatility = signal
    used = float(used_signal_notional.get(plan.ticker, 0.0))
    cap = _capacity(
        price=bar.vwap,
        signal_adv=adv,
        used_signal_notional=used,
        window_amount=bar.amount_rmb,
        policy=state.policy,
    )
    shares = min(plan.planned_shares, cap.executable_shares)
    signal_limited = cap.signal_executable_shares < plan.planned_shares
    window_limited = cap.window_executable_shares < plan.planned_shares
    if shares <= 0:
        return (
            _order(
                date=date,
                plan=plan,
                status="unfilled",
                block_reason=None,
                execution_vwap=bar.vwap,
                signal_adv_limited=signal_limited,
                window_capacity_limited=window_limited,
            ),
            None,
        )
    notional = shares * bar.vwap
    costs = _minimum_costs(
        notional=notional,
        side="buy",
        window_amount=bar.amount_rmb,
        volatility=volatility,
        date=date,
        policy=state.policy,
    )
    if notional + float(costs["total"]) > plan.reserved_cash + 1e-8:
        raise PITStockContractError("actual C outlay exceeds its causal reservation")
    order = _order(
        date=date,
        plan=plan,
        status="executed",
        block_reason=None,
        executed_shares=shares,
        executed_notional=notional,
        execution_vwap=bar.vwap,
        signal_adv_limited=signal_limited,
        window_capacity_limited=window_limited,
        costs=costs,
    )
    return order, _PreparedBuy(plan, bar, shares, notional, costs, order)


def observe_window_c(
    state: SequentialMinuteState,
    account: RealShareAccount,
    bars: pd.DataFrame,
    *,
    postings: list[PnlPosting],
    complete_no_bar_tickers: set[str] | None = None,
) -> SequentialMinuteExecutionResult:
    """Settle frozen C plans, mark C close, and permanently complete state."""

    _assert_transition(state, phase=PHASE_C, account=account, postings=postings)
    date = _date(state.trade_date)
    observed = _bars(
        bars,
        window="C",
        date=date,
        expected_tickers=set(state.required_bar_tickers),
        complete_no_bar_tickers=complete_no_bar_tickers,
    )
    working = deepcopy(state.working_account)
    new_postings: list[PnlPosting] = []
    used = dict(state.used_signal_notional)
    orders = list(state.orders)
    prepared: list[_PreparedBuy] = []
    for plan in state.planned_orders:
        order, fill = _prepare_buy(
            state=state,
            plan=plan,
            bar=observed.get(plan.ticker),
            used_signal_notional=used,
            date=date,
        )
        orders.append(order)
        if fill is not None:
            prepared.append(fill)
    frozen_cash = float(state.cash_snapshot_c or 0.0)
    total_outlay = fsum(
        fill.notional + float(fill.costs["total"]) for fill in prepared
    )
    if (
        abs(working.cash - frozen_cash) > _TOLERANCE
        or total_outlay > frozen_cash + _TOLERANCE
    ):
        raise PITStockContractError("C execution escapes its frozen cash snapshot")
    start_nav = working.nav()
    for fill in prepared:
        ticker = fill.plan.ticker
        if working.owned_shares(ticker) > 0:
            _mark_to(
                working,
                ticker=ticker,
                price=fill.bar.vwap,
                date=date,
                mark_kind="window_C_vwap",
                postings=new_postings,
                period_signal=state.period_signal,
            )
        working.cash -= fill.notional + float(fill.costs["total"])
        working.positions[ticker] = working.positions.get(ticker, 0) + fill.shares
        used[ticker] = used.get(ticker, 0.0) + fill.notional
        working.marks[ticker] = RawMark(
            fill.bar.vwap,
            date.date().isoformat(),
            "window_C_vwap",
        )
        _post(
            new_postings,
            date=date,
            phase="new_period_window_C",
            period_signal=state.period_signal,
            ticker=ticker,
            kind="trade_cost",
            amount=-float(fill.costs["total"]),
            order_id=fill.plan.order_id,
        )
    if working.cash < -_TOLERANCE:
        raise PITStockContractError("minute C buys attempted to borrow cash")
    _mark_window_close(
        working,
        bars=observed,
        window="C",
        date=date,
        postings=new_postings,
        period_signal=state.period_signal,
    )
    posttrade_nav = working.nav()
    _assert_reconciliation(
        start_nav=start_nav, end_nav=posttrade_nav, postings=new_postings
    )
    all_postings = [*state.staged_postings, *new_postings]
    reconciliation = (
        posttrade_nav
        - state.pretrade_nav
        - fsum(value.amount for value in all_postings)
    )
    if not isfinite(reconciliation) or abs(reconciliation) > _TOLERANCE:
        raise PITStockContractError("minute execution account does not reconcile")
    if state.after_window_a_nav is None or state.after_window_b_nav is None:
        raise PITStockContractError("minute execution state lacks prior window NAV")
    result = SequentialMinuteExecutionResult(
        pretrade_nav=state.pretrade_nav,
        after_window_a_nav=state.after_window_a_nav,
        after_window_b_nav=state.after_window_b_nav,
        posttrade_nav=posttrade_nav,
        target_weights=state.target_map,
        orders=tuple(orders),
        total_cost=fsum(float(order.costs.get("total", 0.0)) for order in orders),
        reconciliation_error=reconciliation,
    )
    _commit(
        state,
        account=account,
        working=working,
        postings=postings,
        new_postings=new_postings,
        phase=PHASE_COMPLETE,
        planned_orders=(),
        required_bar_tickers=set(),
        used_signal_notional=used,
        orders=orders,
        after_window_a_nav=state.after_window_a_nav,
        after_window_b_nav=state.after_window_b_nav,
        cash_snapshot_c=state.cash_snapshot_c,
        result=result,
    )
    return result


__all__ = [
    "DECISION_TIMES",
    "MINUTE_EXECUTION_BAR_COLUMNS",
    "MINUTE_EXECUTION_CONTEXT_COLUMNS",
    "MinuteExecutionContext",
    "MinuteOrderPlan",
    "MinuteRealShareOrder",
    "MinuteWindowBar",
    "PHASE_A",
    "PHASE_B",
    "PHASE_C",
    "PHASE_COMPLETE",
    "SequentialMinuteExecutionResult",
    "SequentialMinutePolicy",
    "SequentialMinuteState",
    "WINDOW_SPECS",
    "begin_sequential_minute_rebalance",
    "observe_window_a",
    "observe_window_b",
    "observe_window_c",
]
