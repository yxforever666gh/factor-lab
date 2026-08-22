"""Point-in-time, event-sourced shadow portfolio accounting.

This engine consumes targets made after a close and market observations from a
strictly later session.  It deliberately rejects forward-return/label columns.
It is research-only and exposes no broker integration.
"""

from __future__ import annotations

import ast
import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timezone
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from factor_lab.research_os.execution_kernel import (
    AShareCostPolicy,
    CorporateAction,
    ExecutionAccount,
    ExecutionColumns,
    ExecutionPolicy,
    ExecutionPosition,
    apply_corporate_actions,
    execute_rebalance,
    mark_to_market as kernel_mark_to_market,
    validate_long_only_targets,
)
from factor_lab.research_os.field_safety import is_forward_derived_field


# Kept as a compatibility export for older callers.  Enforcement uses the
# shared classifier below, which also catches exact/aliased names such as
# ``label``, ``target``, ``return_5d`` and ``y``.
FORBIDDEN_FORWARD_TOKENS = (
    "forward_",
    "future_",
    "target_return",
    "return_label",
    "label_",
    "next_",
    "fwd_",
    "lead_",
)
_SHANGHAI = ZoneInfo("Asia/Shanghai")


def assert_point_in_time_columns(columns: list[str] | pd.Index) -> None:
    forbidden = [
        str(column)
        for column in columns
        if is_forward_derived_field(column)
    ]
    if forbidden:
        raise ValueError(f"shadow engine forbids forward/label columns: {sorted(forbidden)}")


def assert_no_forward_label_access(source: str, *, source_name: str = "<source>") -> None:
    """Statically reject direct Python access to a forward/label field.

    Runtime DataFrame validation remains mandatory.  This guard complements it
    for CI by finding literal ``frame[\"forward_*\"]``, ``row.get(\"next_*\")``
    and attribute-style accesses before the shadow module can be deployed.
    It intentionally inspects field access nodes rather than arbitrary string
    literals, so documentation and explicit deny-list tests remain legal.
    """

    try:
        tree = ast.parse(source, filename=source_name)
    except SyntaxError as exc:
        raise ValueError(f"cannot statically inspect invalid Python source {source_name}") from exc
    accessed: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Subscript):
            value = node.slice
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                accessed.add(value.value)
        elif isinstance(node, ast.Attribute):
            accessed.add(node.attr)
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"get", "pop", "setdefault"}
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            accessed.add(node.args[0].value)
    try:
        assert_point_in_time_columns(sorted(accessed))
    except ValueError as exc:
        raise ValueError(f"{source_name}: {exc}") from exc


@dataclass
class ShadowPosition:
    ticker: str
    quantity: float
    last_price: float
    average_cost: float = 0.0

    def __post_init__(self) -> None:
        if self.average_cost <= 0 and self.last_price > 0:
            self.average_cost = float(self.last_price)

    @property
    def market_value(self) -> float:
        return max(float(self.quantity), 0.0) * max(float(self.last_price), 0.0)


@dataclass(frozen=True)
class LedgerEvent:
    sequence: int
    event_type: str
    event_date: str
    recorded_at_utc: str
    payload: dict[str, Any]
    previous_hash: str
    event_hash: str


@dataclass
class ShadowAccount:
    account_id: str
    initial_capital: float = 50_000_000.0
    cash: float | None = None
    positions: dict[str, ShadowPosition] = field(default_factory=dict)
    events: list[LedgerEvent] = field(default_factory=list)
    nav_history: list[dict[str, Any]] = field(default_factory=list)
    benchmark_nav: float | None = None
    record_open_event: bool = field(default=True, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        if self.cash is None:
            self.cash = float(self.initial_capital)
        if self.benchmark_nav is None:
            self.benchmark_nav = float(self.initial_capital)
        if not self.events and self.record_open_event:
            self.append_event("account_opened", date.today(), {"initial_capital": self.initial_capital})

    def append_event(self, event_type: str, event_date: str | date | pd.Timestamp, payload: Mapping[str, Any]) -> LedgerEvent:
        previous_hash = self.events[-1].event_hash if self.events else "0" * 64
        core = {
            "sequence": len(self.events) + 1,
            "event_type": str(event_type),
            "event_date": str(pd.Timestamp(event_date).date()),
            "payload": dict(payload),
            "previous_hash": previous_hash,
        }
        canonical = json.dumps(core, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        event_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        event = LedgerEvent(
            **core,
            recorded_at_utc=datetime.now(timezone.utc).isoformat(),
            event_hash=event_hash,
        )
        self.events.append(event)
        return event

    def validate_hash_chain(self) -> bool:
        previous = "0" * 64
        for expected_sequence, event in enumerate(self.events, start=1):
            core = {
                "sequence": event.sequence,
                "event_type": event.event_type,
                "event_date": event.event_date,
                "payload": event.payload,
                "previous_hash": event.previous_hash,
            }
            canonical = json.dumps(core, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
            expected_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            if event.sequence != expected_sequence or event.previous_hash != previous or event.event_hash != expected_hash:
                return False
            previous = event.event_hash
        return True

    def nav(self) -> float:
        return float(self.cash or 0.0) + sum(position.market_value for position in self.positions.values())

    def state_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "initial_capital": self.initial_capital,
            "cash": self.cash,
            "benchmark_nav": self.benchmark_nav,
            "positions": {key: asdict(value) for key, value in self.positions.items()},
            "events": [asdict(value) for value in self.events],
            "nav_history": list(self.nav_history),
        }


@dataclass(frozen=True)
class ShadowExecutionConfig:
    max_adv_participation: float = 0.05
    max_position_weight: float = 0.02
    lot_size: int = 100
    open_column: str = "open_adj"
    close_column: str = "close_adj"
    adv_column: str = "adv_20"
    volatility_column: str = "volatility_20"
    limit_up_column: str = "is_one_price_limit_up"
    limit_down_column: str = "is_one_price_limit_down"
    suspended_column: str = "is_suspended"
    delisted_column: str = "is_delisted"
    session_column: str = "trade_date"
    execution_event_time_column: str = "execution_event_time"
    execution_available_at_column: str = "execution_available_at"
    execution_observation_deadline_minutes: int = 5
    mark_event_time_column: str = "mark_event_time"
    mark_available_at_column: str = "mark_available_at"
    # Explicit role bindings are required by the daily API.  The legacy field
    # remains only for the backwards-compatible ``execute_target`` wrapper.
    decision_snapshot_id_column: str = "decision_snapshot_id"
    execution_snapshot_id_column: str = "execution_snapshot_id"
    mark_snapshot_id_column: str = "mark_snapshot_id"
    snapshot_id_column: str = "gold_snapshot_id"
    costs: AShareCostPolicy = field(default_factory=AShareCostPolicy)

    def __post_init__(self) -> None:
        if not 1 <= int(self.execution_observation_deadline_minutes) <= 5:
            raise ValueError(
                "execution observation deadline must be between one and five minutes"
            )


@dataclass(frozen=True)
class ShadowSnapshotBindings:
    """Immutable lineage roles for one projected trading session."""

    decision_snapshot_id: str | None
    execution_snapshot_id: str
    mark_snapshot_id: str

    def __post_init__(self) -> None:
        values = {
            "execution_snapshot_id": self.execution_snapshot_id,
            "mark_snapshot_id": self.mark_snapshot_id,
        }
        if self.decision_snapshot_id is not None:
            values["decision_snapshot_id"] = self.decision_snapshot_id
        missing = sorted(name for name, value in values.items() if not str(value).strip())
        if missing:
            raise ValueError("snapshot bindings require non-empty " + ", ".join(missing))
        normalized = [str(value).strip() for value in values.values()]
        if len(set(normalized)) != len(normalized):
            raise ValueError(
                "decision, execution and mark snapshot roles must use distinct snapshots"
            )

    def to_dict(self) -> dict[str, str | None]:
        return asdict(self)


class ShadowPortfolioEngine:
    def __init__(self, account: ShadowAccount, config: ShadowExecutionConfig | None = None) -> None:
        self.account = account
        self.config = config or ShadowExecutionConfig()

    def _validate_snapshot_binding(self, bars: pd.DataFrame, snapshot_id: str) -> None:
        column = self.config.snapshot_id_column
        if not snapshot_id or column not in bars.columns:
            raise ValueError(
                f"market bars require {column} bound to the authoritative Gold snapshot"
            )
        observed = {
            str(value).strip()
            for value in bars[column]
            if value is not None and not pd.isna(value) and str(value).strip()
        }
        if observed != {str(snapshot_id)}:
            raise ValueError(
                "all market bars must be bound to the same authoritative Gold snapshot"
            )

    @staticmethod
    def _observed_binding(bars: pd.DataFrame, column: str) -> set[str]:
        if column not in bars.columns:
            raise ValueError(f"market bars require explicit {column}")
        return {
            str(value).strip()
            for value in bars[column]
            if value is not None and not pd.isna(value) and str(value).strip()
        }

    def _validate_role_bindings(
        self,
        bars: pd.DataFrame,
        bindings: ShadowSnapshotBindings,
    ) -> None:
        expected = (
            (
                self.config.execution_snapshot_id_column,
                bindings.execution_snapshot_id,
                "execution",
            ),
            (self.config.mark_snapshot_id_column, bindings.mark_snapshot_id, "mark"),
        )
        for column, snapshot_id, role in expected:
            observed = self._observed_binding(bars, column)
            if observed != {snapshot_id}:
                raise ValueError(
                    f"all {role} observations must be bound to {role}_snapshot_id"
                )

    @staticmethod
    def _trusted_next_session(
        decision_date: date,
        sessions: Sequence[str | date | pd.Timestamp],
    ) -> date:
        if not sessions:
            raise ValueError("trusted trading calendar sessions are mandatory")
        normalized = tuple(pd.Timestamp(value).date() for value in sessions)
        if normalized != tuple(sorted(set(normalized))):
            raise ValueError("trusted trading calendar sessions must be unique and ordered")
        try:
            index = normalized.index(decision_date)
        except ValueError as exc:
            raise ValueError("decision_date is absent from the trusted trading calendar") from exc
        if index + 1 >= len(normalized):
            raise ValueError("trusted trading calendar has no session after decision_date")
        return normalized[index + 1]

    @staticmethod
    def _aware_timestamp(value: Any, *, field_name: str) -> pd.Timestamp:
        if value is None or pd.isna(value):
            raise ValueError(f"market bars contain missing {field_name}")
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            raise ValueError(f"market bars require timezone-aware {field_name}")
        return timestamp.tz_convert(timezone.utc)

    def _validate_temporal_contract(
        self,
        bars: pd.DataFrame,
        *,
        decision_date: date,
        trade_date: date,
        expected_next_session: date,
        decision_cutoff: datetime | pd.Timestamp | None = None,
    ) -> dict[str, str]:
        """Validate field-level bitemporal cutoffs for a close-to-open step.

        ``execution_available_at`` is the maximum availability timestamp of
        every input used to create an opening fill (open price, trade status,
        ADV, volatility and corporate actions).  ``mark_available_at`` covers
        the close price and benchmark observation used only for end-of-session
        valuation.  A row-level close timestamp can therefore never authorize
        an opening fill retroactively.
        """

        if expected_next_session != trade_date:
            raise ValueError("trade_date is not the exchange calendar's next session")
        if trade_date <= decision_date:
            raise ValueError("shadow execution must occur after the signal decision close")

        required = {
            self.config.session_column,
            self.config.execution_event_time_column,
            self.config.execution_available_at_column,
            self.config.mark_event_time_column,
            self.config.mark_available_at_column,
        }
        missing = sorted(required - set(bars.columns))
        if missing:
            raise ValueError(
                "market bars are missing bitemporal execution fields: "
                + ", ".join(missing)
            )
        if bars.empty:
            raise ValueError("empty open-session market bars are forbidden")

        sessions = {pd.Timestamp(value).date() for value in bars[self.config.session_column]}
        if sessions != {trade_date}:
            raise ValueError(
                f"market bars must contain only expected next session {trade_date.isoformat()}"
            )

        minimum_decision_cutoff = pd.Timestamp(
            datetime.combine(decision_date, time(15, 0), tzinfo=_SHANGHAI)
        ).tz_convert(timezone.utc)
        open_cutoff = pd.Timestamp(
            datetime.combine(trade_date, time(9, 30), tzinfo=_SHANGHAI)
        ).tz_convert(timezone.utc)
        close_cutoff = pd.Timestamp(
            datetime.combine(trade_date, time(15, 0), tzinfo=_SHANGHAI)
        ).tz_convert(timezone.utc)
        observation_deadline = open_cutoff + pd.Timedelta(
            minutes=self.config.execution_observation_deadline_minutes
        )
        if decision_cutoff is None:
            effective_decision_cutoff = minimum_decision_cutoff
        else:
            effective_decision_cutoff = pd.Timestamp(decision_cutoff)
            if effective_decision_cutoff.tzinfo is None:
                raise ValueError("decision_cutoff must include a timezone")
            effective_decision_cutoff = effective_decision_cutoff.tz_convert(
                timezone.utc
            )
            if effective_decision_cutoff < minimum_decision_cutoff:
                raise ValueError(
                    "decision_cutoff cannot precede the decision session close"
                )
            if effective_decision_cutoff >= open_cutoff:
                raise ValueError(
                    "decision_cutoff must precede the next-session execution open"
                )

        latest_execution_available = effective_decision_cutoff
        latest_mark_available = close_cutoff
        for _, row in bars.iterrows():
            execution_event = self._aware_timestamp(
                row[self.config.execution_event_time_column],
                field_name=self.config.execution_event_time_column,
            )
            execution_available = self._aware_timestamp(
                row[self.config.execution_available_at_column],
                field_name=self.config.execution_available_at_column,
            )
            mark_event = self._aware_timestamp(
                row[self.config.mark_event_time_column],
                field_name=self.config.mark_event_time_column,
            )
            mark_available = self._aware_timestamp(
                row[self.config.mark_available_at_column],
                field_name=self.config.mark_available_at_column,
            )
            if execution_event != open_cutoff:
                raise ValueError("execution_event_time must be the expected session open")
            if (
                execution_available < execution_event
                or execution_available > observation_deadline
            ):
                raise ValueError(
                    "opening fill evidence exceeded the live observation deadline"
                )
            if execution_available <= effective_decision_cutoff:
                raise ValueError(
                    "opening execution observation must follow the prior decision close"
                )
            if mark_event != close_cutoff:
                raise ValueError("mark_event_time must be the expected session close")
            if mark_available < mark_event:
                raise ValueError("closing mark cannot be available before its event time")
            latest_execution_available = max(latest_execution_available, execution_available)
            latest_mark_available = max(latest_mark_available, mark_available)

        return {
            "decision_cutoff": effective_decision_cutoff.isoformat(),
            "execution_event_time": open_cutoff.isoformat(),
            "execution_available_at": latest_execution_available.isoformat(),
            "mark_event_time": close_cutoff.isoformat(),
            "mark_available_at": latest_mark_available.isoformat(),
        }

    def _kernel_account(self) -> ExecutionAccount:
        return ExecutionAccount(
            cash=float(self.account.cash or 0.0),
            positions={
                ticker: ExecutionPosition(
                    ticker=ticker,
                    quantity=position.quantity,
                    last_price=position.last_price,
                    average_cost=position.average_cost,
                )
                for ticker, position in self.account.positions.items()
            },
        )

    def _sync_kernel_account(self, account: ExecutionAccount) -> None:
        if account.cash < -1e-6:
            raise RuntimeError("shadow account cannot borrow cash")
        self.account.cash = max(float(account.cash), 0.0)
        self.account.positions = {
            ticker: ShadowPosition(
                ticker=ticker,
                quantity=position.quantity,
                last_price=position.last_price,
                average_cost=position.average_cost,
            )
            for ticker, position in account.positions.items()
        }

    def _kernel_columns(self) -> ExecutionColumns:
        return ExecutionColumns(
            open=self.config.open_column,
            mark=self.config.close_column,
            adv=self.config.adv_column,
            volatility=self.config.volatility_column,
            limit_up=self.config.limit_up_column,
            limit_down=self.config.limit_down_column,
            suspended=self.config.suspended_column,
            delisted=self.config.delisted_column,
            split_ratio="split_ratio",
            cash_dividend="cash_dividend",
        )

    def _kernel_policy(self) -> ExecutionPolicy:
        return ExecutionPolicy(
            max_adv_participation=self.config.max_adv_participation,
            max_position_weight=self.config.max_position_weight,
            lot_size=self.config.lot_size,
            costs=self.config.costs,
        )

    def _apply_session_corporate_actions(
        self,
        market_bars: pd.DataFrame,
        session_date: pd.Timestamp,
    ) -> tuple[CorporateAction, ...]:
        rows = {
            str(row["ticker"]): row
            for _, row in market_bars.drop_duplicates("ticker", keep="last").iterrows()
        }
        kernel_account = self._kernel_account()
        actions = apply_corporate_actions(
            kernel_account,
            rows,
            columns=self._kernel_columns(),
        )
        self._sync_kernel_account(kernel_account)
        for action in actions:
            self.account.append_event(
                f"corporate_action_{action.action_type}",
                session_date,
                {"ticker": action.ticker, **action.payload},
            )
        return actions

    def project_session(
        self,
        *,
        session_date: str | date | pd.Timestamp,
        market_bars: pd.DataFrame,
        snapshot_bindings: ShadowSnapshotBindings,
        benchmark_return: float,
        trusted_calendar_sessions: Sequence[str | date | pd.Timestamp],
        target_weights: Mapping[str, float] | None = None,
        decision_date: str | date | pd.Timestamp | None = None,
        model_version: str | None = None,
        decision_cutoff: datetime | pd.Timestamp | None = None,
        session_metrics: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Project one authoritative trading session, with or without orders.

        Corporate actions, cash, positions, fees, marks and benchmark NAV are
        advanced on every call.  ``target_weights=None`` is a mark-only day;
        an empty mapping is an explicit 100% cash target and therefore still
        requires trusted execution observations.
        """

        assert_point_in_time_columns(market_bars.columns)
        trade_ts = pd.Timestamp(session_date).normalize()
        sessions = tuple(pd.Timestamp(item).date() for item in trusted_calendar_sessions)
        if sessions != tuple(sorted(set(sessions))) or trade_ts.date() not in sessions:
            raise ValueError("session_date requires a unique ordered trusted trading calendar")
        index = sessions.index(trade_ts.date())
        if index == 0:
            raise ValueError("trusted trading calendar requires a prior decision session")
        prior_session = sessions[index - 1]
        effective_decision = (
            pd.Timestamp(decision_date).date() if decision_date is not None else prior_session
        )
        if effective_decision != prior_session:
            raise ValueError("decision_date must be the immediately prior trusted session")
        if target_weights is not None:
            if snapshot_bindings.decision_snapshot_id is None:
                raise ValueError("a rebalance requires decision_snapshot_id")
            if not str(model_version or "").strip():
                raise ValueError("a rebalance requires model_version")

        self._validate_role_bindings(market_bars, snapshot_bindings)
        timing = self._validate_temporal_contract(
            market_bars,
            decision_date=effective_decision,
            trade_date=trade_ts.date(),
            expected_next_session=trade_ts.date(),
            decision_cutoff=decision_cutoff,
        )
        normalized_targets: dict[str, float] | None = None
        if target_weights is not None:
            normalized_targets = validate_long_only_targets(
                target_weights,
                max_position_weight=self.config.max_position_weight,
            )
            self.account.append_event(
                "target_received",
                effective_decision,
                {
                    "trade_date": str(trade_ts.date()),
                    "decision_snapshot_id": snapshot_bindings.decision_snapshot_id,
                    "execution_snapshot_id": snapshot_bindings.execution_snapshot_id,
                    "mark_snapshot_id": snapshot_bindings.mark_snapshot_id,
                    "model_version": str(model_version),
                    "weights": normalized_targets,
                    "timing": timing,
                },
            )
        self._apply_session_corporate_actions(market_bars, trade_ts)

        session_costs = 0.0
        if normalized_targets is not None:
            kernel_account = self._kernel_account()
            execution = execute_rebalance(
                kernel_account,
                normalized_targets,
                market_bars,
                trade_date=trade_ts,
                policy=self._kernel_policy(),
                columns=self._kernel_columns(),
                ticker_column="ticker",
                process_corporate_actions=False,
            )
            self._sync_kernel_account(kernel_account)
            for order in execution.orders:
                payload = order.to_trade_dict(rounded=False)
                if order.status == "blocked":
                    self.account.append_event("order_blocked", trade_ts, payload)
                    continue
                session_costs += float(order.costs.get("total", 0.0))
                self.account.append_event(
                    "fill",
                    trade_ts,
                    {
                        **payload,
                        "notional": order.executed_notional,
                        "fees": dict(order.costs),
                        "execution_snapshot_id": snapshot_bindings.execution_snapshot_id,
                    },
                )

        metric_payload = dict(session_metrics or {})
        assert_point_in_time_columns(list(metric_payload))
        self.account.append_event(
            "session_evidence",
            trade_ts,
            {
                "snapshot_bindings": snapshot_bindings.to_dict(),
                "timing": timing,
                "rebalanced": target_weights is not None,
                "fees": session_costs,
                "metrics": metric_payload,
            },
        )
        mark = self.mark_to_market(
            trade_ts,
            market_bars,
            snapshot_id=snapshot_bindings.mark_snapshot_id,
            benchmark_return=benchmark_return,
            snapshot_column=self.config.mark_snapshot_id_column,
        )
        mark["snapshot_bindings"] = snapshot_bindings.to_dict()
        mark["session_fees"] = session_costs
        # Replace the just-appended mark payload with explicit lineage while
        # preserving the event hash contract for subsequent persistence.
        mark_event = self.account.events[-1]
        self.account.events.pop()
        self.account.append_event(
            mark_event.event_type,
            mark_event.event_date,
            mark,
        )
        return mark

    def execute_target(
        self,
        *,
        decision_date: str | date | pd.Timestamp,
        trade_date: str | date | pd.Timestamp,
        expected_next_session: str | date | pd.Timestamp,
        target_weights: Mapping[str, float],
        market_bars: pd.DataFrame,
        snapshot_id: str,
        model_version: str,
        benchmark_return: float | None = None,
        trusted_calendar_sessions: Sequence[str | date | pd.Timestamp] | None = None,
        decision_cutoff: datetime | pd.Timestamp | None = None,
    ) -> dict[str, Any]:
        if expected_next_session is None:
            raise ValueError("expected_next_session is mandatory exchange-calendar evidence")
        decision_ts = pd.Timestamp(decision_date).normalize()
        trade_ts = pd.Timestamp(trade_date).normalize()
        expected_ts = pd.Timestamp(expected_next_session).normalize()
        if trade_ts.date() <= decision_ts.date():
            raise ValueError("shadow execution must occur after the signal decision close")
        trusted_next = self._trusted_next_session(
            decision_ts.date(), trusted_calendar_sessions or ()
        )
        if expected_ts.date() != trusted_next or trade_ts.date() != trusted_next:
            raise ValueError(
                "trade_date is not the exchange calendar's next session "
                "(persisted trusted calendar)"
            )
        self._validate_snapshot_binding(market_bars, snapshot_id)
        timing = self._validate_temporal_contract(
            market_bars,
            decision_date=decision_ts.date(),
            trade_date=trade_ts.date(),
            expected_next_session=expected_ts.date(),
            decision_cutoff=decision_cutoff,
        )
        normalized_targets = validate_long_only_targets(
            target_weights,
            max_position_weight=self.config.max_position_weight,
        )
        self.account.append_event(
            "target_received",
            decision_ts,
            {
                "trade_date": str(trade_ts.date()),
                "snapshot_id": snapshot_id,
                "model_version": model_version,
                "weights": normalized_targets,
                "timing": timing,
            },
        )
        kernel_account = self._kernel_account()
        execution = execute_rebalance(
            kernel_account,
            normalized_targets,
            market_bars,
            trade_date=trade_ts,
            policy=self._kernel_policy(),
            columns=self._kernel_columns(),
            ticker_column="ticker",
            process_corporate_actions=True,
        )
        self._sync_kernel_account(kernel_account)
        for action in execution.corporate_actions:
            self.account.append_event(
                f"corporate_action_{action.action_type}",
                trade_ts,
                {"ticker": action.ticker, **action.payload},
            )
        for order in execution.orders:
            payload = order.to_trade_dict(rounded=False)
            if order.status == "blocked":
                self.account.append_event("order_blocked", trade_ts, payload)
                continue
            self.account.append_event(
                "fill",
                trade_ts,
                {
                    **payload,
                    # Preserve the legacy event field while exposing the
                    # canonical order/fill amounts for parity auditing.
                    "notional": order.executed_notional,
                    "fees": dict(order.costs),
                },
            )

        return self.mark_to_market(
            trade_ts,
            market_bars,
            snapshot_id=snapshot_id,
            benchmark_return=benchmark_return,
        )

    def mark_to_market(
        self,
        as_of_date: str | date | pd.Timestamp,
        market_bars: pd.DataFrame,
        *,
        snapshot_id: str,
        benchmark_return: float | None = None,
        snapshot_column: str | None = None,
    ) -> dict[str, Any]:
        assert_point_in_time_columns(market_bars.columns)
        if snapshot_column is None:
            self._validate_snapshot_binding(market_bars, snapshot_id)
        else:
            observed = self._observed_binding(market_bars, snapshot_column)
            if observed != {snapshot_id}:
                raise ValueError(
                    f"all closing marks must be bound to {snapshot_column}"
                )
        kernel_account = self._kernel_account()
        kernel_mark_to_market(
            kernel_account,
            market_bars,
            columns=self._kernel_columns(),
            ticker_column="ticker",
        )
        self._sync_kernel_account(kernel_account)
        if benchmark_return is not None:
            self.account.benchmark_nav = float(self.account.benchmark_nav or self.account.initial_capital) * (1.0 + float(benchmark_return))
        nav = self.account.nav()
        row = {
            "date": str(pd.Timestamp(as_of_date).date()),
            "cash": float(self.account.cash or 0.0),
            "positions_value": sum(position.market_value for position in self.account.positions.values()),
            "nav": nav,
            "benchmark_nav": float(self.account.benchmark_nav or 0.0),
            "position_count": len(self.account.positions),
        }
        self.account.nav_history.append(row)
        self.account.append_event("mark_to_market", as_of_date, row)
        return row


__all__ = [
    "FORBIDDEN_FORWARD_TOKENS",
    "LedgerEvent",
    "ShadowAccount",
    "ShadowExecutionConfig",
    "ShadowPortfolioEngine",
    "ShadowPosition",
    "ShadowSnapshotBindings",
    "assert_no_forward_label_access",
    "assert_point_in_time_columns",
]
