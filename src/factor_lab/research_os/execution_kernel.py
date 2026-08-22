"""Deterministic long-only execution and accounting kernel.

Both historical research and the event-sourced shadow portfolio call this
module for target validation, order construction, trade blocking, capacity,
costs, fills and cash/position accounting.  The kernel has no broker or
network integration and deliberately accepts only already-authorized market
observations.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from math import floor, isfinite, sqrt
from typing import Any, Mapping

import pandas as pd


_EPSILON = 1e-8


@dataclass(frozen=True)
class AShareCostPolicy:
    commission_rate: float = 0.0003
    slippage_bps_per_side: float = 5.0
    stamp_duty_before_2023_08_28: float = 0.001
    stamp_duty_from_2023_08_28: float = 0.0005
    exchange_handling_rate: float = 0.0000341
    transfer_fee_rate: float = 0.00001
    impact_coefficient: float = 0.5


@dataclass(frozen=True)
class TradeCostBreakdown:
    commission: float = 0.0
    slippage: float = 0.0
    stamp_duty: float = 0.0
    exchange_handling: float = 0.0
    transfer_fee: float = 0.0
    impact: float = 0.0
    total: float = 0.0

    def to_dict(self) -> dict[str, float]:
        return asdict(self)


def _policy_value(policy: Any, name: str, default: float) -> float:
    if isinstance(policy, Mapping):
        raw = policy.get(name, default)
    else:
        raw = getattr(policy, name, default)
    return float(raw)


def stamp_duty_rate(
    trade_date: str | date | datetime | pd.Timestamp,
    policy: AShareCostPolicy | Mapping[str, Any] | Any = AShareCostPolicy(),
) -> float:
    timestamp = pd.Timestamp(trade_date).normalize()
    if timestamp < pd.Timestamp("2023-08-28"):
        return _policy_value(policy, "stamp_duty_before_2023_08_28", 0.001)
    return _policy_value(policy, "stamp_duty_from_2023_08_28", 0.0005)


def calculate_trade_costs(
    *,
    notional: float,
    side: str,
    adv: float,
    volatility: float,
    trade_date: str | date | datetime | pd.Timestamp,
    policy: AShareCostPolicy | Mapping[str, Any] | Any = AShareCostPolicy(),
) -> dict[str, float]:
    """Calculate all costs on the actually executed notional."""

    normalized_side = str(side).strip().lower()
    if normalized_side not in {"buy", "sell"}:
        raise ValueError("side must be 'buy' or 'sell'")
    executed = max(float(notional), 0.0)
    if executed == 0.0:
        return TradeCostBreakdown().to_dict()
    daily_value = float(adv)
    participation = (
        min(max(executed / daily_value, 0.0), 1.0) if daily_value > 0 else 0.0
    )
    impact_rate = (
        _policy_value(policy, "impact_coefficient", 0.5)
        * max(float(volatility), 0.0)
        * sqrt(participation)
    )
    commission = executed * _policy_value(policy, "commission_rate", 0.0003)
    slippage = (
        executed * _policy_value(policy, "slippage_bps_per_side", 5.0) / 10_000.0
    )
    stamp = (
        executed * stamp_duty_rate(trade_date, policy)
        if normalized_side == "sell"
        else 0.0
    )
    handling = executed * _policy_value(
        policy, "exchange_handling_rate", 0.0000341
    )
    transfer = executed * _policy_value(policy, "transfer_fee_rate", 0.00001)
    impact = executed * impact_rate
    return TradeCostBreakdown(
        commission=commission,
        slippage=slippage,
        stamp_duty=stamp,
        exchange_handling=handling,
        transfer_fee=transfer,
        impact=impact,
        total=commission + slippage + stamp + handling + transfer + impact,
    ).to_dict()


def maximum_executable_notional(
    adv: float, max_adv_participation: float = 0.05
) -> float:
    if not 0 < float(max_adv_participation) <= 1:
        raise ValueError("max_adv_participation must be in (0, 1]")
    return max(float(adv), 0.0) * float(max_adv_participation)


@dataclass
class ExecutionPosition:
    ticker: str
    quantity: float
    last_price: float
    average_cost: float = 0.0

    def __post_init__(self) -> None:
        self.ticker = str(self.ticker)
        self.quantity = float(self.quantity)
        self.last_price = float(self.last_price)
        self.average_cost = float(self.average_cost or self.last_price)
        if not all(isfinite(value) for value in (self.quantity, self.last_price, self.average_cost)):
            raise ValueError("position accounting values must be finite")
        if self.quantity < -_EPSILON:
            raise ValueError("long-only position quantity cannot be negative")

    @property
    def market_value(self) -> float:
        return max(self.quantity, 0.0) * max(self.last_price, 0.0)


@dataclass
class ExecutionAccount:
    cash: float
    positions: dict[str, ExecutionPosition] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.cash = float(self.cash)
        if not isfinite(self.cash):
            raise ValueError("account cash must be finite")
        if self.cash < -_EPSILON:
            raise ValueError("long-only account cannot start with borrowed cash")
        normalized: dict[str, ExecutionPosition] = {}
        for ticker, position in self.positions.items():
            key = str(ticker)
            if position.quantity > _EPSILON:
                normalized[key] = position
        self.positions = normalized

    def nav(self) -> float:
        return self.cash + sum(position.market_value for position in self.positions.values())


@dataclass(frozen=True)
class ExecutionColumns:
    open: str = "open_adj"
    mark: str = "close_adj"
    adv: str = "adv_20"
    volatility: str = "volatility_20"
    limit_up: str | None = "is_one_price_limit_up"
    limit_down: str | None = "is_one_price_limit_down"
    suspended: str | None = "is_suspended"
    delisted: str | None = "is_delisted"
    split_ratio: str | None = "split_ratio"
    cash_dividend: str | None = "cash_dividend"


@dataclass(frozen=True)
class ExecutionPolicy:
    max_adv_participation: float = 0.05
    max_position_weight: float = 0.02
    lot_size: int = 0
    costs: AShareCostPolicy | Mapping[str, Any] | Any = field(
        default_factory=AShareCostPolicy
    )

    def __post_init__(self) -> None:
        participation = float(self.max_adv_participation)
        position_weight = float(self.max_position_weight)
        if not isfinite(participation) or not 0 < participation <= 1:
            raise ValueError("max_adv_participation must be in (0, 1]")
        if not isfinite(position_weight) or not 0 < position_weight <= 1:
            raise ValueError("max_position_weight must be in (0, 1]")
        if int(self.lot_size) < 0:
            raise ValueError("lot_size cannot be negative")


@dataclass(frozen=True)
class CorporateAction:
    action_type: str
    ticker: str
    payload: dict[str, float]


@dataclass(frozen=True)
class ExecutionOrder:
    date: str
    ticker: str
    side: str
    requested_notional: float
    executed_notional: float
    price: float | None
    quantity: float
    adv: float | None
    participation: float
    status: str
    reason: str | None
    costs: dict[str, float]
    capacity_limited: bool = False

    def to_trade_dict(self, *, rounded: bool = True) -> dict[str, Any]:
        def number(value: float, digits: int) -> float:
            return round(float(value), digits) if rounded else float(value)

        payload: dict[str, Any] = {
            "date": self.date,
            "ticker": self.ticker,
            "side": self.side,
            "requested_notional": number(self.requested_notional, 4),
            "executed_notional": number(self.executed_notional, 4),
            "status": self.status,
            "reason": self.reason,
        }
        if self.status == "executed":
            payload.update(
                {
                    "price": None if self.price is None else number(self.price, 10),
                    "quantity": number(self.quantity, 10),
                    "adv": None if self.adv is None else number(self.adv, 4),
                    "participation": number(self.participation, 8),
                    "costs": {
                        key: number(value, 6) for key, value in self.costs.items()
                    },
                }
            )
        return payload


@dataclass(frozen=True)
class ExecutionResult:
    pretrade_nav: float
    posttrade_nav: float
    target_weights: dict[str, float]
    orders: tuple[ExecutionOrder, ...]
    corporate_actions: tuple[CorporateAction, ...]
    costs: dict[str, float]
    weights: dict[str, float]
    cash_weight: float
    capacity_violation_count: int
    capacity_usage: float

    @property
    def blocked_trade_count(self) -> int:
        return sum(order.status == "blocked" for order in self.orders)

    @property
    def traded_notional(self) -> float:
        return sum(
            order.executed_notional
            for order in self.orders
            if order.status == "executed"
        )


def _truthy(value: Any) -> bool:
    if value is None:
        return False
    try:
        if bool(pd.isna(value)):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _finite_positive(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if isfinite(number) and number > 0 else None


def _finite_nonnegative(value: Any, *, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float(default)
    return number if isfinite(number) and number >= 0 else float(default)


def _row_value(row: Mapping[str, Any], column: str | None, default: Any = None) -> Any:
    if not column:
        return default
    return row.get(column, default)


def validate_long_only_targets(
    target_weights: Mapping[str, float],
    *,
    max_position_weight: float,
) -> dict[str, float]:
    """Validate and canonically order a fully-funded-or-cash long-only target."""

    if not isinstance(target_weights, Mapping):
        raise TypeError("target_weights must be a mapping")
    normalized: dict[str, float] = {}
    for raw_ticker, raw_weight in target_weights.items():
        ticker = str(raw_ticker).strip()
        if not ticker:
            raise ValueError("target ticker cannot be empty")
        weight = float(raw_weight)
        if not isfinite(weight):
            raise ValueError(f"target weight for {ticker} must be finite")
        if weight < -1e-12:
            raise ValueError("target weights must be long-only")
        if weight > float(max_position_weight) + 1e-12:
            raise ValueError("target exceeds max_position_weight")
        if weight > _EPSILON:
            normalized[ticker] = max(weight, 0.0)
    if sum(normalized.values()) > 1.0 + 1e-9:
        raise ValueError("target weights cannot exceed 100%")
    return dict(sorted(normalized.items()))


def _market_map(
    market: Mapping[str, Mapping[str, Any]] | pd.DataFrame,
    *,
    ticker_column: str,
) -> dict[str, Mapping[str, Any]]:
    if isinstance(market, pd.DataFrame):
        if ticker_column not in market.columns:
            raise ValueError(f"market observations require {ticker_column}")
        return {
            str(row[ticker_column]): row
            for _, row in market.drop_duplicates(ticker_column, keep="last").iterrows()
        }
    return {str(ticker): row for ticker, row in market.items()}


def apply_corporate_actions(
    account: ExecutionAccount,
    market: Mapping[str, Mapping[str, Any]],
    *,
    columns: ExecutionColumns,
) -> tuple[CorporateAction, ...]:
    actions: list[CorporateAction] = []
    for ticker, position in list(account.positions.items()):
        row = market.get(ticker)
        if row is None:
            continue
        raw_split_ratio = _row_value(row, columns.split_ratio, 1.0)
        split_ratio = (
            1.0
            if raw_split_ratio is None or pd.isna(raw_split_ratio)
            else float(raw_split_ratio)
        )
        raw_cash_dividend = _row_value(row, columns.cash_dividend, 0.0)
        cash_dividend = (
            0.0
            if raw_cash_dividend is None or pd.isna(raw_cash_dividend)
            else float(raw_cash_dividend)
        )
        if split_ratio <= 0 or not isfinite(split_ratio):
            raise ValueError(f"invalid split ratio for {ticker}")
        if abs(split_ratio - 1.0) > 1e-12:
            old_quantity = position.quantity
            position.quantity *= split_ratio
            if position.last_price > 0:
                position.last_price /= split_ratio
            if position.average_cost > 0:
                position.average_cost /= split_ratio
            actions.append(
                CorporateAction(
                    "split",
                    ticker,
                    {
                        "ratio": split_ratio,
                        "old_quantity": old_quantity,
                        "new_quantity": position.quantity,
                    },
                )
            )
        if cash_dividend:
            if not isfinite(cash_dividend) or cash_dividend < 0:
                raise ValueError(f"invalid cash dividend for {ticker}")
            cash_delta = position.quantity * cash_dividend
            account.cash += cash_delta
            actions.append(
                CorporateAction(
                    "dividend",
                    ticker,
                    {"cash_per_share": cash_dividend, "cash_delta": cash_delta},
                )
            )
    return tuple(actions)


def _block_reason(
    row: Mapping[str, Any] | None,
    *,
    side: str,
    columns: ExecutionColumns,
) -> str | None:
    if row is None:
        return "missing_market_bar"
    if _finite_positive(_row_value(row, columns.open)) is None:
        return "missing_open"
    if _truthy(_row_value(row, columns.suspended)):
        return "suspended"
    if side == "buy" and _truthy(_row_value(row, columns.delisted)):
        return "delisted"
    if side == "buy" and _truthy(_row_value(row, columns.limit_up)):
        return "one_price_limit_up"
    if side == "sell" and _truthy(_row_value(row, columns.limit_down)):
        return "one_price_limit_down"
    if _finite_positive(_row_value(row, columns.adv)) is None:
        return "missing_adv"
    return None


def _empty_costs() -> dict[str, float]:
    return TradeCostBreakdown().to_dict()


def _blocked_order(
    *,
    trade_date: pd.Timestamp,
    ticker: str,
    side: str,
    requested: float,
    reason: str,
) -> ExecutionOrder:
    return ExecutionOrder(
        date=str(trade_date.date()),
        ticker=ticker,
        side=side,
        requested_notional=requested,
        executed_notional=0.0,
        price=None,
        quantity=0.0,
        adv=None,
        participation=0.0,
        status="blocked",
        reason=reason,
        costs=_empty_costs(),
    )


def _lot_notional(notional: float, price: float, lot_size: int) -> float:
    if lot_size <= 0:
        return max(float(notional), 0.0)
    quantity = floor(max(float(notional), 0.0) / price / lot_size) * lot_size
    return quantity * price


def execute_rebalance(
    account: ExecutionAccount,
    target_weights: Mapping[str, float],
    market: Mapping[str, Mapping[str, Any]] | pd.DataFrame,
    *,
    trade_date: str | date | pd.Timestamp,
    policy: ExecutionPolicy,
    columns: ExecutionColumns = ExecutionColumns(),
    ticker_column: str = "ticker",
    process_corporate_actions: bool = True,
) -> ExecutionResult:
    """Execute one target at the supplied opening observations.

    Sells always precede buys.  Buys are scaled pro rata when transaction
    costs would otherwise borrow cash, avoiding ticker-order fill bias.
    """

    normalized_targets = validate_long_only_targets(
        target_weights, max_position_weight=policy.max_position_weight
    )
    timestamp = pd.Timestamp(trade_date).normalize()
    rows = _market_map(market, ticker_column=ticker_column)
    actions = (
        apply_corporate_actions(account, rows, columns=columns)
        if process_corporate_actions
        else ()
    )

    for ticker, position in account.positions.items():
        row = rows.get(ticker)
        price = (
            _finite_positive(_row_value(row, columns.open)) if row is not None else None
        )
        if price is not None:
            position.last_price = price
    pretrade_nav = account.nav()
    if pretrade_nav <= 0:
        raise ValueError("pretrade NAV must be positive")

    orders: list[ExecutionOrder] = []
    capacity_violations = 0

    # Build requested sells from marked values, then apply common blockers,
    # capacity and costs through the same path used by the shadow ledger.
    for ticker in sorted(list(account.positions)):
        position = account.positions.get(ticker)
        if position is None:
            continue
        current_value = position.market_value
        requested = max(
            current_value - pretrade_nav * normalized_targets.get(ticker, 0.0),
            0.0,
        )
        if requested <= _EPSILON:
            continue
        row = rows.get(ticker)
        reason = _block_reason(row, side="sell", columns=columns)
        if reason:
            orders.append(
                _blocked_order(
                    trade_date=timestamp,
                    ticker=ticker,
                    side="sell",
                    requested=requested,
                    reason=reason,
                )
            )
            continue
        assert row is not None
        price = float(_row_value(row, columns.open))
        adv = float(_row_value(row, columns.adv))
        capacity = maximum_executable_notional(
            adv, policy.max_adv_participation
        )
        capacity_limited = requested > capacity + _EPSILON
        capacity_violations += int(capacity_limited)
        executed = min(requested, capacity, current_value)
        if executed <= _EPSILON:
            orders.append(
                _blocked_order(
                    trade_date=timestamp,
                    ticker=ticker,
                    side="sell",
                    requested=requested,
                    reason="adv_capacity_or_zero_quantity",
                )
            )
            continue
        volatility = _finite_nonnegative(
            _row_value(row, columns.volatility, 0.0)
        )
        fees = calculate_trade_costs(
            notional=executed,
            side="sell",
            adv=adv,
            volatility=volatility,
            trade_date=timestamp,
            policy=policy.costs,
        )
        quantity = min(executed / price, position.quantity)
        executed = quantity * price
        position.quantity = max(position.quantity - quantity, 0.0)
        position.last_price = price
        account.cash += executed - fees["total"]
        if position.quantity <= 1e-12:
            account.positions.pop(ticker, None)
        orders.append(
            ExecutionOrder(
                date=str(timestamp.date()),
                ticker=ticker,
                side="sell",
                requested_notional=requested,
                executed_notional=executed,
                price=price,
                quantity=quantity,
                adv=adv,
                participation=executed / adv,
                status="executed",
                reason=None,
                costs=fees,
                capacity_limited=capacity_limited,
            )
        )

    candidates: list[dict[str, Any]] = []
    for ticker, weight in normalized_targets.items():
        row = rows.get(ticker)
        current_value = account.positions.get(
            ticker, ExecutionPosition(ticker, 0.0, 0.0)
        ).market_value
        requested = max(pretrade_nav * weight - current_value, 0.0)
        if requested <= _EPSILON:
            continue
        reason = _block_reason(row, side="buy", columns=columns)
        if reason:
            orders.append(
                _blocked_order(
                    trade_date=timestamp,
                    ticker=ticker,
                    side="buy",
                    requested=requested,
                    reason=reason,
                )
            )
            continue
        assert row is not None
        price = float(_row_value(row, columns.open))
        adv = float(_row_value(row, columns.adv))
        capacity = maximum_executable_notional(
            adv, policy.max_adv_participation
        )
        capacity_limited = requested > capacity + _EPSILON
        capacity_violations += int(capacity_limited)
        executable = min(requested, capacity)
        candidates.append(
            {
                "ticker": ticker,
                "requested": requested,
                "executable": executable,
                "price": price,
                "adv": adv,
                "volatility": _finite_nonnegative(
                    _row_value(row, columns.volatility, 0.0)
                ),
                "capacity_limited": capacity_limited,
            }
        )

    def buy_outlay(scale: float) -> float:
        total = 0.0
        for candidate in candidates:
            notional = _lot_notional(
                candidate["executable"] * scale,
                candidate["price"],
                policy.lot_size,
            )
            fees = calculate_trade_costs(
                notional=notional,
                side="buy",
                adv=candidate["adv"],
                volatility=candidate["volatility"],
                trade_date=timestamp,
                policy=policy.costs,
            )
            total += notional + fees["total"]
        return total

    buy_scale = 1.0
    if candidates and buy_outlay(1.0) > account.cash + 1e-9:
        low, high = 0.0, 1.0
        for _ in range(48):
            middle = (low + high) / 2.0
            if buy_outlay(middle) <= account.cash + 1e-9:
                low = middle
            else:
                high = middle
        buy_scale = low

    for candidate in candidates:
        ticker = str(candidate["ticker"])
        requested = float(candidate["requested"])
        price = float(candidate["price"])
        adv = float(candidate["adv"])
        executed = _lot_notional(
            float(candidate["executable"]) * buy_scale,
            price,
            policy.lot_size,
        )
        if executed <= _EPSILON:
            orders.append(
                _blocked_order(
                    trade_date=timestamp,
                    ticker=ticker,
                    side="buy",
                    requested=requested,
                    reason="cash_or_adv_capacity",
                )
            )
            continue
        fees = calculate_trade_costs(
            notional=executed,
            side="buy",
            adv=adv,
            volatility=float(candidate["volatility"]),
            trade_date=timestamp,
            policy=policy.costs,
        )
        outlay = executed + fees["total"]
        if outlay > account.cash + 1e-6:
            raise RuntimeError("execution kernel attempted to borrow cash")
        account.cash -= outlay
        if -1e-6 < account.cash < 0:
            account.cash = 0.0
        quantity = executed / price
        position = account.positions.get(ticker)
        if position is None:
            account.positions[ticker] = ExecutionPosition(
                ticker=ticker,
                quantity=quantity,
                last_price=price,
                average_cost=outlay / quantity,
            )
        else:
            old_quantity = position.quantity
            old_cost = old_quantity * position.average_cost
            position.quantity += quantity
            position.last_price = price
            position.average_cost = (old_cost + outlay) / position.quantity
        orders.append(
            ExecutionOrder(
                date=str(timestamp.date()),
                ticker=ticker,
                side="buy",
                requested_notional=requested,
                executed_notional=executed,
                price=price,
                quantity=quantity,
                adv=adv,
                participation=executed / adv,
                status="executed",
                reason=None,
                costs=fees,
                capacity_limited=bool(candidate["capacity_limited"]),
            )
        )

    if account.cash < -1e-6:
        raise RuntimeError("long-only execution produced borrowed cash")
    posttrade_nav = account.nav()
    weights = {
        ticker: position.market_value / posttrade_nav
        for ticker, position in sorted(account.positions.items())
        if posttrade_nav > 0 and position.market_value > _EPSILON
    }
    if any(weight < -1e-12 for weight in weights.values()):
        raise RuntimeError("long-only execution produced a negative weight")
    costs = _empty_costs()
    for order in orders:
        if order.status == "executed":
            for name, value in order.costs.items():
                costs[name] += float(value)
    return ExecutionResult(
        pretrade_nav=pretrade_nav,
        posttrade_nav=posttrade_nav,
        target_weights=normalized_targets,
        orders=tuple(orders),
        corporate_actions=tuple(actions),
        costs=costs,
        weights=weights,
        cash_weight=max(account.cash / posttrade_nav, 0.0) if posttrade_nav > 0 else 0.0,
        capacity_violation_count=capacity_violations,
        capacity_usage=max(
            (order.participation for order in orders if order.status == "executed"),
            default=0.0,
        ),
    )


def mark_to_market(
    account: ExecutionAccount,
    market: Mapping[str, Mapping[str, Any]] | pd.DataFrame,
    *,
    columns: ExecutionColumns = ExecutionColumns(),
    ticker_column: str = "ticker",
) -> float:
    """Update known marks without creating fills and return account NAV."""

    rows = _market_map(market, ticker_column=ticker_column)
    for ticker, position in account.positions.items():
        row = rows.get(ticker)
        if row is None:
            continue
        price = _finite_positive(_row_value(row, columns.mark))
        if price is None:
            price = _finite_positive(_row_value(row, columns.open))
        if price is not None:
            position.last_price = price
    return account.nav()


__all__ = [
    "AShareCostPolicy",
    "CorporateAction",
    "ExecutionAccount",
    "ExecutionColumns",
    "ExecutionOrder",
    "ExecutionPolicy",
    "ExecutionPosition",
    "ExecutionResult",
    "TradeCostBreakdown",
    "apply_corporate_actions",
    "calculate_trade_costs",
    "execute_rebalance",
    "mark_to_market",
    "maximum_executable_notional",
    "stamp_duty_rate",
    "validate_long_only_targets",
]
