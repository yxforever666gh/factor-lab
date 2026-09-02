"""Raw-price, integer-share account primitives for the 13.0 closure."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from math import floor, fsum, isfinite
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from factor_lab.portfolio.execution import (
    AShareCostPolicy,
    calculate_trade_costs,
    maximum_executable_notional,
    validate_long_only_targets,
)
from factor_lab.research.pit_stock import PITStockContractError


@dataclass(frozen=True)
class PendingShareLot:
    action_id: str
    ticker: str
    shares: int
    ex_date: str
    list_session: str


@dataclass(frozen=True)
class CashReceivable:
    action_id: str
    ticker: str
    amount: float
    ex_date: str
    pay_session: str


@dataclass(frozen=True)
class RawMark:
    price: float
    session: str
    kind: str


@dataclass(frozen=True)
class RealShareAction:
    action_id: str
    ticker: str
    available_date: str
    record_date: str
    ex_date: str
    pay_session: str | None
    list_session: str | None
    stock_dividend_per_share: float
    cash_dividend_before_tax_per_share: float


@dataclass(frozen=True)
class PnlPosting:
    date: str
    phase: str
    period_signal: str | None
    ticker: str
    kind: str
    amount: float
    action_id: str | None = None
    order_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RealShareAccount:
    cash: float
    positions: dict[str, int] = field(default_factory=dict)
    pending_shares: dict[str, PendingShareLot] = field(default_factory=dict)
    receivables: dict[str, CashReceivable] = field(default_factory=dict)
    marks: dict[str, RawMark] = field(default_factory=dict)
    record_entitlements: dict[str, int] = field(default_factory=dict)
    applied_stages: set[tuple[str, str]] = field(default_factory=set)
    extinguished_pending: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        self.cash = float(self.cash)
        if not isfinite(self.cash) or self.cash < -1e-9:
            raise ValueError("real-share account cash must be finite/non-negative")
        normalized = {}
        for ticker, shares in self.positions.items():
            if isinstance(shares, bool) or int(shares) != shares or shares < 0:
                raise ValueError("settled positions must be non-negative integers")
            if shares:
                normalized[str(ticker)] = int(shares)
        self.positions = normalized

    def pending_by_ticker(self, ticker: str) -> int:
        return sum(
            lot.shares
            for lot in self.pending_shares.values()
            if lot.ticker == ticker
        )

    def owned_shares(self, ticker: str) -> int:
        return self.positions.get(ticker, 0) + self.pending_by_ticker(ticker)

    def required_tickers(self) -> set[str]:
        return set(self.positions) | {
            lot.ticker for lot in self.pending_shares.values()
        }

    def ticker_value(self, ticker: str) -> float:
        owned = self.owned_shares(ticker)
        mark = self.marks.get(ticker)
        market = owned * mark.price if owned and mark is not None else 0.0
        receivable = fsum(
            item.amount
            for item in self.receivables.values()
            if item.ticker == ticker
        )
        return market + receivable

    def nav(self) -> float:
        tickers = self.required_tickers() | {
            item.ticker for item in self.receivables.values()
        }
        return self.cash + fsum(self.ticker_value(ticker) for ticker in tickers)


@dataclass(frozen=True)
class RealSharePolicy:
    max_adv_participation: float
    max_position_weight: float
    lot_size: int = 100
    minimum_commission_rmb: float = 5.0
    costs: AShareCostPolicy = AShareCostPolicy()

    def __post_init__(self) -> None:
        if not 0 < self.max_adv_participation <= 1:
            raise ValueError("max_adv_participation must be in (0,1]")
        if not 0 < self.max_position_weight <= 1:
            raise ValueError("max_position_weight must be in (0,1]")
        if self.lot_size != 100:
            raise ValueError("13.0 real-share lot_size is frozen at 100")
        if self.minimum_commission_rmb < 0:
            raise ValueError("minimum commission cannot be negative")


@dataclass(frozen=True)
class RealShareOrder:
    order_id: str
    date: str
    ticker: str
    side: str
    requested_shares: float
    requested_notional: float
    available_tradable_shares: int
    executed_shares: int
    executed_notional: float
    price: float | None
    status: str
    market_block_reason: str | None
    capacity_limited: bool
    lot_limited: bool
    cash_limited: bool
    pending_limited: bool
    costs: dict[str, float]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RealShareExecutionResult:
    pretrade_nav: float
    posttrade_nav: float
    target_weights: dict[str, float]
    orders: tuple[RealShareOrder, ...]
    traded_notional: float
    total_cost: float


def _date(value: Any) -> pd.Timestamp:
    result = pd.Timestamp(value)
    if pd.isna(result):
        raise PITStockContractError("unknown real-share event date")
    if result.tzinfo is not None:
        result = result.tz_convert(None)
    return result.normalize()


def _first_session_on_or_after(
    value: Any, sessions: Sequence[pd.Timestamp], *, field_name: str
) -> str:
    date = _date(value)
    array = np.asarray(sessions, dtype="datetime64[ns]")
    index = int(np.searchsorted(array, np.datetime64(date), side="left"))
    if index >= len(sessions):
        raise PITStockContractError(
            f"{field_name} lies after official calendar coverage"
        )
    return pd.Timestamp(sessions[index]).date().isoformat()


def prepare_real_share_actions(
    frame: pd.DataFrame,
    official_sessions: Sequence[Any],
) -> tuple[RealShareAction, ...]:
    required = {
        "action_id",
        "ticker",
        "available_date",
        "record_date",
        "ex_date",
        "pay_date",
        "share_arrival_date",
        "stock_dividend_per_share",
        "cash_dividend_before_tax_per_share",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise PITStockContractError(f"resolved actions missing columns: {missing}")
    sessions = tuple(_date(value) for value in official_sessions)
    if not sessions or list(sessions) != sorted(sessions) or len(set(sessions)) != len(sessions):
        raise PITStockContractError("official sessions must be unique/increasing")
    session_set = set(sessions)
    rows = []
    for row in frame.sort_values(
        ["ex_date", "ticker", "action_id"], kind="mergesort"
    ).itertuples(index=False):
        stock = float(row.stock_dividend_per_share)
        cash = float(row.cash_dividend_before_tax_per_share)
        if not all(isfinite(value) and value >= 0 for value in (stock, cash)):
            raise PITStockContractError("corporate-action economics are invalid")
        available = _date(row.available_date)
        record = _date(row.record_date)
        ex = _date(row.ex_date)
        if not available <= ex or not record < ex:
            raise PITStockContractError("corporate-action availability/order differs")
        if record not in session_set or ex not in session_set:
            raise PITStockContractError(
                "record/ex date is absent from official sessions"
            )
        pay = (
            _first_session_on_or_after(
                row.pay_date, sessions, field_name="pay_date"
            )
            if cash > 0
            else None
        )
        listed = (
            _first_session_on_or_after(
                row.share_arrival_date,
                sessions,
                field_name="share_arrival_date",
            )
            if stock > 0
            else None
        )
        if pay is not None and _date(pay) < ex:
            raise PITStockContractError("pay session precedes ex session")
        if listed is not None and _date(listed) < ex:
            raise PITStockContractError("share-list session precedes ex session")
        rows.append(
            RealShareAction(
                action_id=str(row.action_id),
                ticker=str(row.ticker),
                available_date=available.date().isoformat(),
                record_date=record.date().isoformat(),
                ex_date=ex.date().isoformat(),
                pay_session=pay,
                list_session=listed,
                stock_dividend_per_share=stock,
                cash_dividend_before_tax_per_share=cash,
            )
        )
    if len({row.action_id for row in rows}) != len(rows):
        raise PITStockContractError("real-share action IDs are duplicate")
    return tuple(rows)


def _post(
    postings: list[PnlPosting],
    *,
    date: Any,
    phase: str,
    period_signal: str | None,
    ticker: str,
    kind: str,
    amount: float,
    action_id: str | None = None,
    order_id: str | None = None,
) -> None:
    value = float(amount)
    if not isfinite(value):
        raise PITStockContractError("P&L posting amount is not finite")
    if abs(value) > 1e-12:
        postings.append(
            PnlPosting(
                date=_date(date).date().isoformat(),
                phase=phase,
                period_signal=period_signal,
                ticker=str(ticker),
                kind=kind,
                amount=value,
                action_id=action_id,
                order_id=order_id,
            )
        )


def mark_owned_shares(
    account: RealShareAccount,
    market: pd.DataFrame,
    *,
    session: Any,
    price_column: str,
    suspended_tickers: set[str],
    delisted_tickers: set[str],
    postings: list[PnlPosting],
    period_signal: str | None,
    phase: str,
    additional_tickers: set[str] | None = None,
) -> None:
    if "ticker" not in market or price_column not in market:
        raise PITStockContractError("raw mark market columns differ")
    rows = market.drop_duplicates("ticker", keep="last").set_index("ticker")
    required = account.required_tickers() | set(additional_tickers or ())
    for ticker in sorted(required):
        if ticker in delisted_tickers:
            continue
        owned = account.owned_shares(ticker)
        if ticker in rows.index:
            if ticker in suspended_tickers:
                continue
            price = float(rows.at[ticker, price_column])
            if not isfinite(price) or price <= 0:
                raise PITStockContractError(f"invalid raw mark for {ticker}")
            old = account.marks.get(ticker)
            old_value = owned * old.price if old is not None else owned * price
            new_value = owned * price
            account.marks[ticker] = RawMark(
                price=price,
                session=_date(session).date().isoformat(),
                kind=price_column,
            )
            _post(
                postings,
                date=session,
                phase=phase,
                period_signal=period_signal,
                ticker=ticker,
                kind="raw_mark",
                amount=new_value - old_value,
            )
        elif ticker not in suspended_tickers:
            raise PITStockContractError(
                f"owned ticker lacks raw bar or suspension proof: {ticker}"
            )


def capture_record_close(
    account: RealShareAccount,
    actions: Sequence[RealShareAction],
    *,
    session: Any,
) -> None:
    date = _date(session).date().isoformat()
    for action in actions:
        if action.record_date != date:
            continue
        key = (action.action_id, "record")
        if key in account.applied_stages:
            continue
        account.record_entitlements[action.action_id] = account.positions.get(
            action.ticker, 0
        )
        account.applied_stages.add(key)


def accrue_ex_open(
    account: RealShareAccount,
    actions: Sequence[RealShareAction],
    *,
    session: Any,
    cash_withholding_rate: float,
    postings: list[PnlPosting],
    period_signal: str | None,
) -> None:
    date = _date(session).date().isoformat()
    if not 0 <= cash_withholding_rate <= 1:
        raise ValueError("cash_withholding_rate must be in [0,1]")
    for action in actions:
        if action.ex_date != date:
            continue
        key = (action.action_id, "ex")
        if key in account.applied_stages:
            continue
        if action.action_id not in account.record_entitlements:
            raise PITStockContractError(
                f"action lacks frozen record entitlement: {action.action_id}"
            )
        entitled = account.record_entitlements[action.action_id]
        mark = account.marks.get(action.ticker)
        if (
            entitled
            and action.stock_dividend_per_share > 0.0
            and (
                mark is None
                or mark.session != date
                or mark.kind != "open"
            )
        ):
            raise PITStockContractError(
                "stock ex action lacks current-session raw open mark"
            )
        cash = (
            entitled
            * action.cash_dividend_before_tax_per_share
            * (1.0 - cash_withholding_rate)
        )
        shares = floor(entitled * action.stock_dividend_per_share + 1e-12)
        if cash > 0:
            assert action.pay_session is not None
            account.receivables[action.action_id] = CashReceivable(
                action_id=action.action_id,
                ticker=action.ticker,
                amount=float(cash),
                ex_date=date,
                pay_session=action.pay_session,
            )
            _post(
                postings,
                date=session,
                phase="old_period_open",
                period_signal=period_signal,
                ticker=action.ticker,
                kind="cash_receivable_accrual",
                amount=cash,
                action_id=action.action_id,
            )
        if shares > 0:
            assert action.list_session is not None and mark is not None
            account.pending_shares[action.action_id] = PendingShareLot(
                action_id=action.action_id,
                ticker=action.ticker,
                shares=int(shares),
                ex_date=date,
                list_session=action.list_session,
            )
            _post(
                postings,
                date=session,
                phase="old_period_open",
                period_signal=period_signal,
                ticker=action.ticker,
                kind="pending_share_accrual",
                amount=shares * mark.price,
                action_id=action.action_id,
            )
        account.applied_stages.add(key)


def settle_pay_and_list_open(
    account: RealShareAccount,
    actions: Sequence[RealShareAction],
    *,
    session: Any,
) -> None:
    date = _date(session).date().isoformat()
    for action in actions:
        pay_key = (action.action_id, "pay")
        if action.pay_session == date and pay_key not in account.applied_stages:
            receivable = account.receivables.pop(action.action_id, None)
            if receivable is not None:
                account.cash += receivable.amount
            account.applied_stages.add(pay_key)
        list_key = (action.action_id, "list")
        if action.list_session == date and list_key not in account.applied_stages:
            lot = account.pending_shares.pop(action.action_id, None)
            if lot is not None and action.action_id not in account.extinguished_pending:
                account.positions[action.ticker] = (
                    account.positions.get(action.ticker, 0) + lot.shares
                )
            account.applied_stages.add(list_key)


def write_down_delists(
    account: RealShareAccount,
    tickers: set[str],
    *,
    session: Any,
    postings: list[PnlPosting],
    period_signal: str | None,
) -> None:
    for ticker in sorted(tickers & account.required_tickers()):
        mark = account.marks.get(ticker)
        if account.owned_shares(ticker) and mark is None:
            raise PITStockContractError("delisted holding lacks a carrying mark")
        carrying = account.owned_shares(ticker) * (
            mark.price if mark is not None else 0.0
        )
        account.positions.pop(ticker, None)
        for action_id, lot in list(account.pending_shares.items()):
            if lot.ticker == ticker:
                account.pending_shares.pop(action_id)
                account.extinguished_pending.add(action_id)
        _post(
            postings,
            date=session,
            phase="old_period_open",
            period_signal=period_signal,
            ticker=ticker,
            kind="delist_write_down",
            amount=-carrying,
        )


def _costs_with_minimum(
    *,
    notional: float,
    side: str,
    adv: float,
    volatility: float,
    trade_date: Any,
    policy: RealSharePolicy,
) -> dict[str, float]:
    costs = calculate_trade_costs(
        notional=notional,
        side=side,
        adv=adv,
        volatility=volatility,
        trade_date=trade_date,
        policy=policy.costs,
    )
    commission = max(costs["commission"], policy.minimum_commission_rmb)
    costs["total"] += commission - costs["commission"]
    costs["commission"] = commission
    return costs


def _block_reason(row: pd.Series | None, *, side: str) -> str | None:
    if row is None:
        return "missing_open"
    if bool(row.get("is_suspended", False)):
        return "suspended"
    price = row.get("open")
    if price is None or not isfinite(float(price)) or float(price) <= 0:
        return "missing_open"
    if side == "buy" and bool(row.get("is_one_price_limit_up", False)):
        return "one_price_limit_up"
    if side == "sell" and bool(row.get("is_one_price_limit_down", False)):
        return "one_price_limit_down"
    return None


def execute_real_share_rebalance(
    account: RealShareAccount,
    targets: Mapping[str, float],
    market: pd.DataFrame,
    *,
    trade_date: Any,
    policy: RealSharePolicy,
    postings: list[PnlPosting],
    period_signal: str,
) -> RealShareExecutionResult:
    normalized = validate_long_only_targets(
        targets, max_position_weight=policy.max_position_weight
    )
    rows = {
        str(row.ticker): row
        for row in market.drop_duplicates("ticker", keep="last").itertuples(
            index=False
        )
    }
    pretrade_nav = account.nav()
    if pretrade_nav <= 0:
        raise PITStockContractError("real-share pretrade NAV is not positive")
    orders: list[RealShareOrder] = []

    def market_values(ticker: str, price: float) -> tuple[float, float]:
        settled = account.positions.get(ticker, 0) * price
        pending = account.pending_by_ticker(ticker) * price
        return settled, pending

    for ticker in sorted(account.required_tickers()):
        row = rows.get(ticker)
        reason = _block_reason(
            pd.Series(row._asdict()) if row is not None else None, side="sell"
        )
        price = float(row.open) if row is not None and reason is None else 0.0
        settled_value, pending_value = market_values(ticker, price or account.marks[ticker].price)
        desired = pretrade_nav * normalized.get(ticker, 0.0)
        requested_notional = max(settled_value + pending_value - desired, 0.0)
        if requested_notional <= 1e-9:
            continue
        available = account.positions.get(ticker, 0)
        requested_shares = min(requested_notional / price, available) if price else float(available)
        order_id = f"{_date(trade_date).date()}:{ticker}:sell"
        if reason:
            orders.append(
                RealShareOrder(
                    order_id,
                    _date(trade_date).date().isoformat(),
                    ticker,
                    "sell",
                    requested_shares,
                    requested_notional,
                    available,
                    0,
                    0.0,
                    None,
                    "blocked",
                    reason,
                    False,
                    False,
                    False,
                    account.pending_by_ticker(ticker) > 0,
                    {},
                )
            )
            continue
        adv = float(row.signal_adv20)
        volatility = float(row.signal_vol_daily)
        if not isfinite(adv) or adv <= 0 or not isfinite(volatility) or volatility < 0:
            raise PITStockContractError("sell signal ADV/volatility is invalid")
        capacity_notional = maximum_executable_notional(
            adv, policy.max_adv_participation
        )
        capacity_shares = floor(
            capacity_notional
            / price
            / policy.lot_size
        ) * policy.lot_size
        full_target_zero = normalized.get(ticker, 0.0) == 0.0
        if full_target_zero and available * price <= capacity_notional + 1e-9:
            executed_shares = available
        else:
            executed_shares = min(
                available,
                floor(requested_shares / policy.lot_size) * policy.lot_size,
                capacity_shares,
            )
        executed_shares = int(max(executed_shares, 0))
        if not executed_shares:
            orders.append(
                RealShareOrder(
                    order_id,
                    _date(trade_date).date().isoformat(),
                    ticker,
                    "sell",
                    requested_shares,
                    requested_notional,
                    available,
                    0,
                    0.0,
                    price,
                    "unfilled",
                    None,
                    requested_notional > capacity_notional + 1e-9,
                    floor(requested_shares / policy.lot_size)
                    * policy.lot_size
                    + 1e-12
                    < requested_shares,
                    False,
                    account.pending_by_ticker(ticker) > 0,
                    {},
                )
            )
            continue
        notional = executed_shares * price
        costs = _costs_with_minimum(
            notional=notional,
            side="sell",
            adv=adv,
            volatility=volatility,
            trade_date=trade_date,
            policy=policy,
        )
        account.positions[ticker] -= executed_shares
        if not account.positions[ticker]:
            account.positions.pop(ticker)
        account.cash += notional - costs["total"]
        _post(
            postings,
            date=trade_date,
            phase="new_period_trade",
            period_signal=period_signal,
            ticker=ticker,
            kind="trade_cost",
            amount=-costs["total"],
            order_id=order_id,
        )
        orders.append(
            RealShareOrder(
                order_id,
                _date(trade_date).date().isoformat(),
                ticker,
                "sell",
                requested_shares,
                requested_notional,
                available,
                executed_shares,
                notional,
                price,
                "executed",
                None,
                requested_notional > capacity_notional + 1e-9,
                executed_shares + 1e-12 < requested_shares,
                False,
                account.pending_by_ticker(ticker) > 0,
                costs,
            )
        )

    candidates = []
    for ticker, weight in sorted(normalized.items()):
        row = rows.get(ticker)
        reason = _block_reason(
            pd.Series(row._asdict()) if row is not None else None, side="buy"
        )
        open_value = float(row.open) if row is not None else float("nan")
        valuation_price = (
            open_value
            if reason is None and isfinite(open_value) and open_value > 0.0
            else (
                account.marks[ticker].price
                if ticker in account.marks
                else (
                    open_value
                    if isfinite(open_value) and open_value > 0.0
                    else 0.0
                )
            )
        )
        price = open_value if reason is None else 0.0
        settled_value, pending_value = market_values(ticker, valuation_price)
        economic = settled_value + pending_value
        requested_notional = max(pretrade_nav * weight - economic, 0.0)
        if requested_notional <= 1e-9:
            continue
        order_id = f"{_date(trade_date).date()}:{ticker}:buy"
        if reason:
            orders.append(
                RealShareOrder(
                    order_id,
                    _date(trade_date).date().isoformat(),
                    ticker,
                    "buy",
                    requested_notional / price if price else 0.0,
                    requested_notional,
                    account.positions.get(ticker, 0),
                    0,
                    0.0,
                    None,
                    "blocked",
                    reason,
                    False,
                    False,
                    False,
                    False,
                    {},
                )
            )
            continue
        adv = float(row.signal_adv20)
        volatility = float(row.signal_vol_daily)
        if not isfinite(adv) or adv <= 0 or not isfinite(volatility) or volatility < 0:
            raise PITStockContractError("buy signal ADV/volatility is invalid")
        cap = maximum_executable_notional(adv, policy.max_adv_participation)
        shares = floor(
            min(requested_notional, cap) / price / policy.lot_size
        ) * policy.lot_size
        candidates.append(
            {
                "ticker": ticker,
                "row": row,
                "order_id": order_id,
                "price": price,
                "adv": adv,
                "volatility": volatility,
                "requested_notional": requested_notional,
                "requested_shares": requested_notional / price,
                "capacity_notional": cap,
                "shares": int(max(shares, 0)),
            }
        )

    def outlay(scale: float) -> float:
        total = 0.0
        for item in candidates:
            shares = floor(
                item["shares"] * scale / policy.lot_size
            ) * policy.lot_size
            if not shares:
                continue
            notional = shares * item["price"]
            costs = _costs_with_minimum(
                notional=notional,
                side="buy",
                adv=item["adv"],
                volatility=item["volatility"],
                trade_date=trade_date,
                policy=policy,
            )
            total += notional + costs["total"]
        return total

    scale = 1.0
    if outlay(scale) > account.cash + 1e-9:
        low, high = 0.0, 1.0
        for _ in range(48):
            middle = (low + high) / 2.0
            if outlay(middle) <= account.cash + 1e-9:
                low = middle
            else:
                high = middle
        scale = low
    for item in candidates:
        shares = int(
            floor(item["shares"] * scale / policy.lot_size)
            * policy.lot_size
        )
        if not shares:
            orders.append(
                RealShareOrder(
                    item["order_id"],
                    _date(trade_date).date().isoformat(),
                    item["ticker"],
                    "buy",
                    item["requested_shares"],
                    item["requested_notional"],
                    account.positions.get(item["ticker"], 0),
                    0,
                    0.0,
                    item["price"],
                    "unfilled",
                    None,
                    item["requested_notional"]
                    > item["capacity_notional"] + 1e-9,
                    item["shares"] == 0,
                    scale < 1.0,
                    False,
                    {},
                )
            )
            continue
        notional = shares * item["price"]
        costs = _costs_with_minimum(
            notional=notional,
            side="buy",
            adv=item["adv"],
            volatility=item["volatility"],
            trade_date=trade_date,
            policy=policy,
        )
        if notional + costs["total"] > account.cash + 1e-6:
            raise PITStockContractError("real-share buy attempted to borrow cash")
        account.cash -= notional + costs["total"]
        account.positions[item["ticker"]] = (
            account.positions.get(item["ticker"], 0) + shares
        )
        account.marks[item["ticker"]] = RawMark(
            price=item["price"],
            session=_date(trade_date).date().isoformat(),
            kind="open",
        )
        _post(
            postings,
            date=trade_date,
            phase="new_period_trade",
            period_signal=period_signal,
            ticker=item["ticker"],
            kind="trade_cost",
            amount=-costs["total"],
            order_id=item["order_id"],
        )
        orders.append(
            RealShareOrder(
                item["order_id"],
                _date(trade_date).date().isoformat(),
                item["ticker"],
                "buy",
                item["requested_shares"],
                item["requested_notional"],
                account.positions.get(item["ticker"], 0) - shares,
                shares,
                notional,
                item["price"],
                "executed",
                None,
                item["requested_notional"]
                > item["capacity_notional"] + 1e-9,
                shares + 1e-12 < item["requested_shares"],
                scale < 1.0,
                False,
                costs,
            )
        )
    if account.cash < -1e-6:
        raise PITStockContractError("real-share account borrowed cash")
    posttrade_nav = account.nav()
    return RealShareExecutionResult(
        pretrade_nav=pretrade_nav,
        posttrade_nav=posttrade_nav,
        target_weights=normalized,
        orders=tuple(orders),
        traded_notional=fsum(order.executed_notional for order in orders),
        total_cost=fsum(
            float(order.costs.get("total", 0.0)) for order in orders
        ),
    )


__all__ = [
    "CashReceivable",
    "PendingShareLot",
    "PnlPosting",
    "RawMark",
    "RealShareAccount",
    "RealShareAction",
    "RealShareExecutionResult",
    "RealShareOrder",
    "RealSharePolicy",
    "accrue_ex_open",
    "capture_record_close",
    "execute_real_share_rebalance",
    "mark_owned_shares",
    "prepare_real_share_actions",
    "settle_pay_and_list_open",
    "write_down_delists",
]
