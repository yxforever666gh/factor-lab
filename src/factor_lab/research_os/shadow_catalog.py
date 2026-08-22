"""Transactional bridge between the shadow engine and the Research OS catalog.

The portfolio engine deliberately has no persistence concerns.  This module
rehydrates its account from the authoritative catalog, executes one supplied
next-session opening auction, and commits the resulting domain events plus
account/position projections as one optimistic-locked transaction.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
import hashlib
from math import isclose, isfinite
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from .catalog import (
    CatalogNotFound,
    ResearchCatalog,
    ShadowAccountRecord,
    ShadowEvent,
    ShadowEventInput,
)
from .contracts import DataQualityStatus, SnapshotTier
from .fingerprint import content_fingerprint
from .shadow import (
    LedgerEvent,
    ShadowAccount,
    ShadowExecutionConfig,
    ShadowPortfolioEngine,
    ShadowPosition,
    ShadowSnapshotBindings,
    assert_point_in_time_columns,
)


_SHANGHAI = ZoneInfo("Asia/Shanghai")
_STEP_METADATA_KEY = "research_os_shadow_step"
_BLOCKING_TRUST_LABELS = frozenset(
    {
        "st_history_unverified",
        "historical_st_empty",
        "hash_mismatch",
        "source_disputed",
        "data_quarantined",
    }
)


class ShadowCatalogBridgeError(RuntimeError):
    """Base error for a shadow step that was not safely committed."""


class ShadowCatalogIntegrityError(ShadowCatalogBridgeError):
    """Raised when the persisted event chain or projections are inconsistent."""


class ShadowDataBlocked(ShadowCatalogBridgeError):
    """Raised when an untrusted snapshot or non-PIT execution input is supplied."""


class ShadowStepAlreadyApplied(ShadowCatalogBridgeError):
    """Raised on an idempotent retry of an already authoritative shadow step."""

    def __init__(self, message: str, *, step_id: str | None = None) -> None:
        super().__init__(message)
        self.step_id = step_id


@dataclass(frozen=True)
class ShadowStepResult:
    step_id: str
    account_id: str
    decision_date: str
    trade_date: str
    snapshot_id: str
    model_version: str
    cash: float
    nav: float
    benchmark_nav: float
    position_count: int
    domain_event_count: int
    persisted_event_count: int
    first_event_sequence: int
    last_event_sequence: int
    last_event_hash: str
    chain_verified: bool
    domain_event_types: tuple[str, ...]
    decision_snapshot_id: str | None = None
    execution_snapshot_id: str | None = None
    mark_snapshot_id: str | None = None
    rebalanced: bool = True

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class _LoadedAccount:
    record: ShadowAccountRecord
    account: ShadowAccount
    position_tickers: frozenset[str]


def _session_date(value: str | date | datetime | pd.Timestamp) -> date:
    return pd.Timestamp(value).date()


def _logical_event_time(event: LedgerEvent) -> datetime:
    """Map the engine's session date to monotonic A-share business time."""

    event_date = _session_date(event.event_date)
    if event.event_type == "target_received":
        timing = event.payload.get("timing")
        if isinstance(timing, Mapping) and timing.get("decision_cutoff"):
            cutoff = pd.Timestamp(timing["decision_cutoff"])
            if cutoff.tzinfo is None:
                raise ShadowCatalogIntegrityError(
                    "target decision_cutoff must include a timezone"
                )
            return cutoff.tz_convert(timezone.utc).to_pydatetime()
        event_time = time(15, 0)
    elif event.event_type.startswith("corporate_action_"):
        event_time = time(9, 25)
    elif event.event_type in {"fill", "order_blocked"}:
        event_time = time(9, 30)
    else:
        event_time = time(15, 0)
    return datetime.combine(event_date, event_time, tzinfo=_SHANGHAI).astimezone(
        timezone.utc
    )


def _projection_time(trade_date: date) -> datetime:
    return datetime.combine(trade_date, time(15, 0), tzinfo=_SHANGHAI).astimezone(
        timezone.utc
    )


def _approximately_equal(left: float, right: float) -> bool:
    return isclose(float(left), float(right), rel_tol=1e-9, abs_tol=0.01)


class ShadowStepService:
    """Run exactly one research-only shadow portfolio step.

    ``expected_next_session`` is mandatory exchange-calendar evidence, not an
    optional hint.  Opening execution inputs and closing valuation inputs carry
    separate event/availability timestamps; the bridge never lets a close-time
    row retroactively authorize an opening fill or reads a forward label.
    """

    def __init__(
        self,
        catalog: ResearchCatalog,
        execution_config: ShadowExecutionConfig | None = None,
    ) -> None:
        self.catalog = catalog
        self.execution_config = execution_config or ShadowExecutionConfig()

    def load_account(self, account_id: str) -> ShadowAccount:
        """Rehydrate a long-only account after validating its authoritative chain."""

        return self._load_account(account_id).account

    def _load_account(self, account_id: str) -> _LoadedAccount:
        record = self.catalog.get_shadow_account(account_id)
        if record is None:
            raise CatalogNotFound(f"shadow account {account_id!r} was not found")
        if not self.catalog.verify_shadow_chain(account_id):
            raise ShadowCatalogIntegrityError(
                f"shadow account {account_id!r} has an invalid authoritative hash chain"
            )
        if record.initial_capital <= 0:
            raise ShadowCatalogIntegrityError("shadow account initial capital must be positive")

        records = self.catalog.list_shadow_positions(account_id)
        positions: dict[str, ShadowPosition] = {}
        projected_market_value = 0.0
        for item in records:
            values = (
                item.quantity,
                item.average_cost,
                item.market_price,
                item.market_value,
            )
            if any(not isfinite(float(value)) or float(value) < 0 for value in values):
                raise ShadowCatalogIntegrityError(
                    f"shadow position {item.ticker!r} violates long-only projection invariants"
                )
            if item.quantity > 1e-12 and item.average_cost <= 0:
                raise ShadowCatalogIntegrityError(
                    f"shadow position {item.ticker!r} has no valid average cost"
                )
            calculated_value = float(item.quantity) * float(item.market_price)
            if not _approximately_equal(calculated_value, item.market_value):
                raise ShadowCatalogIntegrityError(
                    f"shadow position {item.ticker!r} market value projection is inconsistent"
                )
            positions[item.ticker] = ShadowPosition(
                ticker=item.ticker,
                quantity=float(item.quantity),
                last_price=float(item.market_price),
                average_cost=float(item.average_cost),
            )
            projected_market_value += float(item.market_value)

        calculated_nav = float(record.cash) + projected_market_value
        if not _approximately_equal(calculated_nav, record.nav):
            raise ShadowCatalogIntegrityError(
                "shadow account projection violates cash + positions = NAV"
            )
        account = ShadowAccount(
            account_id=record.account_id,
            initial_capital=float(record.initial_capital),
            cash=float(record.cash),
            positions=positions,
            benchmark_nav=float(record.benchmark_nav),
            record_open_event=False,
        )
        return _LoadedAccount(
            record=record,
            account=account,
            position_tickers=frozenset(positions),
        )

    def _validate_snapshot(
        self,
        *,
        snapshot_id: str,
        decision_date: date,
        decision_cutoff: datetime,
    ):
        snapshot = self.catalog.get_snapshot(snapshot_id)
        if snapshot is None:
            raise ShadowDataBlocked(
                f"shadow step requires registered immutable snapshot {snapshot_id!r}"
            )
        reference = snapshot.reference
        if reference.tier is not SnapshotTier.GOLD:
            raise ShadowDataBlocked("shadow step requires a published Gold snapshot")
        if reference.quality_status is not DataQualityStatus.ACCEPTED:
            raise ShadowDataBlocked(
                f"snapshot data quality is {reference.quality_status.value}; shadow step is fail-closed"
            )
        trust_labels = {str(label).strip().lower() for label in reference.trust_labels}
        blocking = sorted(trust_labels & _BLOCKING_TRUST_LABELS)
        if blocking:
            raise ShadowDataBlocked(
                "snapshot contains blocking trust labels: " + ", ".join(blocking)
            )
        minimum_cutoff = datetime.combine(
            decision_date, time(15, 0), tzinfo=_SHANGHAI
        ).astimezone(timezone.utc)
        if decision_cutoff.tzinfo is None or decision_cutoff.utcoffset() is None:
            raise ShadowDataBlocked("decision_cutoff must include a timezone")
        decision_cutoff = decision_cutoff.astimezone(timezone.utc)
        if decision_cutoff < minimum_cutoff:
            raise ShadowDataBlocked(
                "decision_cutoff cannot precede the signal decision close"
            )
        if reference.as_of.astimezone(timezone.utc) > decision_cutoff:
            raise ShadowDataBlocked(
                "snapshot became available after the signal decision close/cutoff"
            )
        return snapshot

    def _validate_role_snapshot(
        self,
        *,
        snapshot_id: str,
        role: str,
        available_by: datetime,
    ):
        """Validate one immutable data role without borrowing another cutoff."""

        if available_by.tzinfo is None or available_by.utcoffset() is None:
            raise ShadowDataBlocked(f"{role} cutoff must include a timezone")
        snapshot = self.catalog.get_snapshot(snapshot_id)
        if snapshot is None:
            raise ShadowDataBlocked(
                f"{role} requires registered immutable snapshot {snapshot_id!r}"
            )
        reference = snapshot.reference
        if reference.tier is not SnapshotTier.GOLD:
            raise ShadowDataBlocked(f"{role} requires a published Gold snapshot")
        if reference.quality_status is not DataQualityStatus.ACCEPTED:
            raise ShadowDataBlocked(
                f"{role} snapshot quality is {reference.quality_status.value}"
            )
        trust_labels = {str(label).strip().lower() for label in reference.trust_labels}
        blocking = sorted(trust_labels & _BLOCKING_TRUST_LABELS)
        if blocking:
            raise ShadowDataBlocked(
                f"{role} snapshot contains blocking trust labels: " + ", ".join(blocking)
            )
        if reference.as_of.astimezone(timezone.utc) > available_by.astimezone(timezone.utc):
            raise ShadowDataBlocked(f"{role} snapshot became available after its cutoff")
        return snapshot

    @staticmethod
    def _trusted_calendar_sessions(snapshot) -> tuple[date, ...]:
        """Read immutable, catalog-persisted exchange sessions from Gold metadata."""

        manifest = snapshot.reference.manifest
        calendar = manifest.get("trading_calendar") if isinstance(manifest, Mapping) else None
        if not isinstance(calendar, Mapping):
            raise ShadowDataBlocked(
                "Gold snapshot lacks a persisted trusted trading calendar"
            )
        if str(calendar.get("quality_status") or "").strip().lower() != "accepted":
            raise ShadowDataBlocked("trading calendar quality is not accepted")
        if not str(calendar.get("source") or "").strip():
            raise ShadowDataBlocked("trading calendar source is not recorded")
        raw_sessions = calendar.get("sessions")
        if not isinstance(raw_sessions, (list, tuple)) or not raw_sessions:
            raise ShadowDataBlocked("trusted trading calendar contains no sessions")
        try:
            sessions = tuple(_session_date(value) for value in raw_sessions)
        except Exception as exc:
            raise ShadowDataBlocked("trusted trading calendar contains an invalid date") from exc
        if sessions != tuple(sorted(set(sessions))):
            raise ShadowDataBlocked(
                "trusted trading calendar sessions must be unique and ordered"
            )
        encoded = "\n".join(item.isoformat() for item in sessions).encode("ascii")
        expected_hash = hashlib.sha256(encoded).hexdigest()
        if str(calendar.get("content_hash") or "") != expected_hash:
            raise ShadowDataBlocked("trusted trading calendar content hash mismatch")
        return sessions

    @staticmethod
    def _next_calendar_session(decision_date: date, sessions: Sequence[date]) -> date:
        try:
            index = sessions.index(decision_date)
        except ValueError as exc:
            raise ShadowDataBlocked(
                "decision_date is absent from the persisted trusted trading calendar"
            ) from exc
        if index + 1 >= len(sessions):
            raise ShadowDataBlocked(
                "persisted trusted trading calendar has no session after decision_date"
            )
        return sessions[index + 1]

    @staticmethod
    def _market_timestamp(value: Any, *, column: str) -> datetime:
        if value is None or pd.isna(value):
            raise ShadowDataBlocked(f"market bars contain missing {column}")
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            raise ShadowDataBlocked(f"market bars require timezone-aware {column}")
        return timestamp.tz_convert(timezone.utc).to_pydatetime()

    def _validate_market_bars(
        self,
        bars: pd.DataFrame,
        *,
        decision_date: date,
        trade_date: date,
        expected_next_session: date,
        snapshot_id: str,
        decision_cutoff: datetime,
    ) -> None:
        assert_point_in_time_columns(bars.columns)
        if bars.empty:
            raise ShadowDataBlocked("empty open-session market bars are forbidden")
        required = {
            "ticker",
            self.execution_config.open_column,
            self.execution_config.close_column,
            self.execution_config.adv_column,
            self.execution_config.volatility_column,
            self.execution_config.session_column,
            self.execution_config.execution_event_time_column,
            self.execution_config.execution_available_at_column,
            self.execution_config.mark_event_time_column,
            self.execution_config.mark_available_at_column,
            self.execution_config.snapshot_id_column,
        }
        missing = sorted(required - set(bars.columns))
        if missing:
            raise ShadowDataBlocked(
                "market bars are missing required execution fields: " + ", ".join(missing)
            )
        tickers = bars["ticker"].astype(str)
        if tickers.eq("").any() or tickers.duplicated().any():
            raise ShadowDataBlocked("market bars require unique non-empty tickers")

        bound_snapshots = {
            str(value).strip()
            for value in bars[self.execution_config.snapshot_id_column]
            if value is not None and not pd.isna(value) and str(value).strip()
        }
        if bound_snapshots != {snapshot_id}:
            raise ShadowDataBlocked(
                "all market bars must be bound to the same accepted Gold snapshot"
            )

        if expected_next_session != trade_date:
            raise ShadowDataBlocked("trade_date is not the exchange calendar's next session")
        observed = {
            _session_date(value)
            for value in bars[self.execution_config.session_column]
        }
        if observed != {trade_date}:
            raise ShadowDataBlocked(
                f"market bars must contain only expected next session {trade_date.isoformat()}"
            )

        open_cutoff = datetime.combine(
            trade_date, time(9, 30), tzinfo=_SHANGHAI
        ).astimezone(timezone.utc)
        close_cutoff = datetime.combine(
            trade_date, time(15, 0), tzinfo=_SHANGHAI
        ).astimezone(timezone.utc)
        observation_deadline = open_cutoff + pd.Timedelta(
            minutes=self.execution_config.execution_observation_deadline_minutes
        ).to_pytimedelta()
        for _, row in bars.iterrows():
            execution_event = self._market_timestamp(
                row[self.execution_config.execution_event_time_column],
                column=self.execution_config.execution_event_time_column,
            )
            execution_available = self._market_timestamp(
                row[self.execution_config.execution_available_at_column],
                column=self.execution_config.execution_available_at_column,
            )
            mark_event = self._market_timestamp(
                row[self.execution_config.mark_event_time_column],
                column=self.execution_config.mark_event_time_column,
            )
            mark_available = self._market_timestamp(
                row[self.execution_config.mark_available_at_column],
                column=self.execution_config.mark_available_at_column,
            )
            if execution_event != open_cutoff:
                raise ShadowDataBlocked(
                    "execution_event_time must be the expected session open"
                )
            if (
                execution_available < execution_event
                or execution_available > observation_deadline
            ):
                raise ShadowDataBlocked(
                    "opening fill evidence exceeded the live observation deadline"
                )
            if execution_available <= decision_cutoff:
                raise ShadowDataBlocked(
                    "opening execution observation must follow the prior decision close"
                )
            if mark_event != close_cutoff:
                raise ShadowDataBlocked("mark_event_time must be the expected session close")
            if mark_available < mark_event:
                raise ShadowDataBlocked(
                    "closing mark cannot be available before its event time"
                )

    def _validate_daily_bindings(
        self,
        bars: pd.DataFrame,
        *,
        bindings: ShadowSnapshotBindings,
        has_target: bool,
        decision_date: date,
        trade_date: date,
        decision_cutoff: datetime,
    ):
        assert_point_in_time_columns(bars.columns)
        required = {
            "ticker",
            self.execution_config.session_column,
            self.execution_config.execution_event_time_column,
            self.execution_config.execution_available_at_column,
            self.execution_config.mark_event_time_column,
            self.execution_config.mark_available_at_column,
            self.execution_config.execution_snapshot_id_column,
            self.execution_config.mark_snapshot_id_column,
            self.execution_config.open_column,
            self.execution_config.close_column,
        }
        missing = sorted(required - set(bars.columns))
        if missing:
            raise ShadowDataBlocked(
                "daily market bars are missing explicit lineage fields: "
                + ", ".join(missing)
            )
        if bars.empty:
            raise ShadowDataBlocked("empty daily market bars are forbidden")
        observed_dates = {_session_date(value) for value in bars[self.execution_config.session_column]}
        if observed_dates != {trade_date}:
            raise ShadowDataBlocked("daily market bars must contain only the projected session")
        for column, expected, role in (
            (
                self.execution_config.execution_snapshot_id_column,
                bindings.execution_snapshot_id,
                "execution",
            ),
            (
                self.execution_config.mark_snapshot_id_column,
                bindings.mark_snapshot_id,
                "mark",
            ),
        ):
            observed = {
                str(value).strip()
                for value in bars[column]
                if value is not None and not pd.isna(value) and str(value).strip()
            }
            if observed != {expected}:
                raise ShadowDataBlocked(f"{role} observations are not bound to {role}_snapshot_id")

        execution_cutoff = max(
            self._market_timestamp(value, column=self.execution_config.execution_available_at_column)
            for value in bars[self.execution_config.execution_available_at_column]
        )
        mark_cutoff = max(
            self._market_timestamp(value, column=self.execution_config.mark_available_at_column)
            for value in bars[self.execution_config.mark_available_at_column]
        )
        if has_target:
            if bindings.decision_snapshot_id is None:
                raise ShadowDataBlocked("a rebalance requires decision_snapshot_id")
            decision_snapshot = self._validate_snapshot(
                snapshot_id=bindings.decision_snapshot_id,
                decision_date=decision_date,
                decision_cutoff=decision_cutoff,
            )
        else:
            decision_snapshot = None
        execution_snapshot = self._validate_role_snapshot(
            snapshot_id=bindings.execution_snapshot_id,
            role="execution",
            available_by=execution_cutoff,
        )
        mark_snapshot = self._validate_role_snapshot(
            snapshot_id=bindings.mark_snapshot_id,
            role="mark",
            available_by=mark_cutoff,
        )
        return decision_snapshot, execution_snapshot, mark_snapshot

    def _step_id(
        self,
        *,
        account_id: str,
        decision_date: date,
        trade_date: date,
        expected_next_session: date,
        target_weights: Mapping[str, float],
        snapshot_id: str,
        model_version: str,
        benchmark_return: float | None,
        decision_cutoff: datetime,
    ) -> str:
        digest = content_fingerprint(
            {
                "account_id": account_id,
                "decision_date": decision_date,
                "decision_cutoff": decision_cutoff,
                "trade_date": trade_date,
                "expected_next_session": expected_next_session,
                "target_weights": {
                    str(ticker): float(weight)
                    for ticker, weight in sorted(target_weights.items())
                },
                "snapshot_id": snapshot_id,
                "model_version": model_version,
                "benchmark_return": benchmark_return,
                "execution_config": self.execution_config,
            },
            domain="factor-lab/research-os/v1/shadow-step",
        )
        return f"sst_{digest[:32]}"

    def _daily_step_id(
        self,
        *,
        account_id: str,
        decision_date: date,
        trade_date: date,
        target_weights: Mapping[str, float] | None,
        bindings: ShadowSnapshotBindings,
        model_version: str | None,
        benchmark_return: float,
        decision_cutoff: datetime,
        session_metrics: Mapping[str, Any],
    ) -> str:
        digest = content_fingerprint(
            {
                "account_id": account_id,
                "decision_date": decision_date,
                "decision_cutoff": decision_cutoff,
                "trade_date": trade_date,
                "target_weights": (
                    None
                    if target_weights is None
                    else {
                        str(ticker): float(weight)
                        for ticker, weight in sorted(target_weights.items())
                    }
                ),
                "snapshot_bindings": bindings,
                "model_version": model_version,
                "benchmark_return": benchmark_return,
                "session_metrics": dict(session_metrics),
                "execution_config": self.execution_config,
            },
            domain="factor-lab/research-os/v1/shadow-daily-projection",
        )
        return f"sdp_{digest[:32]}"

    def _reject_duplicate(self, account_id: str, step_id: str) -> None:
        for event in self.catalog.list_shadow_events(account_id=account_id, limit=1000):
            metadata = event.payload.get(_STEP_METADATA_KEY)
            if isinstance(metadata, Mapping) and metadata.get("step_id") == step_id:
                raise ShadowStepAlreadyApplied(
                    f"shadow step {step_id} is already authoritative for {account_id!r}",
                    step_id=step_id,
                )

    def _require_contiguous_daily_projection(
        self,
        *,
        account_id: str,
        trade_date: date,
        trusted_sessions: Sequence[date],
    ) -> None:
        """Reject a skipped account day before the engine can mutate positions.

        Data incidents explain why a projection is absent; they do not apply
        dividends, splits, marks, or fees.  Therefore the bridge never jumps
        from the latest persisted daily projection to a later trading session.
        Recovery must replay the missing trusted session in order, or bind a
        separately initialized evidence segment while the old account remains
        frozen.
        """

        projections = self.catalog.list_shadow_events_by_type(
            account_id=account_id,
            event_type="account_projected",
            since=None,
            through=None,
            limit=1_000,
        )
        daily = [
            event
            for event in projections
            if isinstance(event.payload.get(_STEP_METADATA_KEY), Mapping)
            and event.payload[_STEP_METADATA_KEY].get("kind")
            == "account_projection"
        ]
        if not daily:
            return
        latest = max(daily, key=lambda event: event.sequence_number)
        latest_date = latest.occurred_at.astimezone(_SHANGHAI).date()
        if latest_date == trade_date:
            # Let the normal idempotency/conflict path distinguish an exact
            # retry from changed inputs for the already-projected day.
            return
        sessions = tuple(trusted_sessions)
        try:
            latest_index = sessions.index(latest_date)
        except ValueError as exc:
            raise ShadowCatalogIntegrityError(
                "latest daily projection is outside the trusted trading calendar"
            ) from exc
        expected = (
            sessions[latest_index + 1]
            if latest_index + 1 < len(sessions)
            else None
        )
        if expected != trade_date:
            raise ShadowDataBlocked(
                "shadow account cannot skip a trusted trading session; replay the "
                "missing session/company actions or start a new evidence segment"
            )

    @staticmethod
    def _domain_event_input(event: LedgerEvent, *, step_id: str) -> ShadowEventInput:
        payload = dict(event.payload)
        payload[_STEP_METADATA_KEY] = {
            "step_id": step_id,
            "kind": "domain_event",
            "engine_sequence": event.sequence,
            "engine_previous_hash": event.previous_hash,
            "engine_event_hash": event.event_hash,
            "engine_recorded_at_utc": event.recorded_at_utc,
        }
        return ShadowEventInput(
            event_type=event.event_type,
            occurred_at=_logical_event_time(event),
            payload=payload,
        )

    @staticmethod
    def _position_projection_input(
        *,
        step_id: str,
        trade_date: date,
        ticker: str,
        position: ShadowPosition | None,
    ) -> ShadowEventInput:
        if position is None:
            state = {
                "ticker": ticker,
                "quantity": 0.0,
                "average_cost": 0.0,
                "market_price": 0.0,
                "market_value": 0.0,
            }
        else:
            state = {
                "ticker": position.ticker,
                "quantity": float(position.quantity),
                "average_cost": float(position.average_cost),
                "market_price": float(position.last_price),
                "market_value": float(position.market_value),
            }
        return ShadowEventInput(
            event_type="position_projected",
            occurred_at=_projection_time(trade_date),
            payload={
                _STEP_METADATA_KEY: {"step_id": step_id, "kind": "position_projection"},
                "position_state": state,
            },
        )

    @staticmethod
    def _account_projection_input(
        *,
        step_id: str,
        trade_date: date,
        account: ShadowAccount,
        account_status: str,
    ) -> ShadowEventInput:
        return ShadowEventInput(
            event_type="account_projected",
            occurred_at=_projection_time(trade_date),
            payload={
                _STEP_METADATA_KEY: {"step_id": step_id, "kind": "account_projection"},
                "account_status": account_status,
                "account_state": {
                    "cash": float(account.cash or 0.0),
                    "nav": float(account.nav()),
                    "benchmark_nav": float(account.benchmark_nav or 0.0),
                },
            },
        )

    @staticmethod
    def _validate_committed_segment(
        events: Sequence[ShadowEvent],
        *,
        expected_previous_hash: str,
        expected_first_sequence: int,
    ) -> None:
        previous_hash = expected_previous_hash
        expected_sequence = expected_first_sequence
        for event in events:
            if (
                event.sequence_number != expected_sequence
                or event.previous_event_hash != previous_hash
            ):
                raise ShadowCatalogIntegrityError(
                    "catalog returned a discontinuous authoritative shadow event segment"
                )
            previous_hash = event.event_hash
            expected_sequence += 1

    def _validate_final_projection(
        self,
        *,
        account: ShadowAccount,
        record_status: str,
    ) -> ShadowAccountRecord:
        projected = self.catalog.get_shadow_account(account.account_id)
        if projected is None:
            raise ShadowCatalogIntegrityError("shadow account disappeared after commit")
        if projected.status != record_status:
            raise ShadowCatalogIntegrityError("shadow account status projection changed unexpectedly")
        for name, expected, actual in (
            ("cash", float(account.cash or 0.0), projected.cash),
            ("nav", account.nav(), projected.nav),
            ("benchmark_nav", float(account.benchmark_nav or 0.0), projected.benchmark_nav),
        ):
            if not _approximately_equal(expected, actual):
                raise ShadowCatalogIntegrityError(
                    f"shadow account {name} projection does not match the engine"
                )

        positions = {item.ticker: item for item in self.catalog.list_shadow_positions(account.account_id)}
        if set(positions) != set(account.positions):
            raise ShadowCatalogIntegrityError("shadow position projection set does not match the engine")
        for ticker, expected in account.positions.items():
            actual = positions[ticker]
            for name, expected_value, actual_value in (
                ("quantity", expected.quantity, actual.quantity),
                ("average_cost", expected.average_cost, actual.average_cost),
                ("market_price", expected.last_price, actual.market_price),
                ("market_value", expected.market_value, actual.market_value),
            ):
                if not _approximately_equal(expected_value, actual_value):
                    raise ShadowCatalogIntegrityError(
                        f"shadow position {ticker!r} {name} projection does not match the engine"
                    )
        return projected

    def project_session(
        self,
        *,
        account_id: str,
        trade_date: str | date | datetime | pd.Timestamp,
        market_bars: pd.DataFrame,
        snapshot_bindings: ShadowSnapshotBindings,
        benchmark_return: float,
        target_weights: Mapping[str, float] | None = None,
        decision_date: str | date | datetime | pd.Timestamp | None = None,
        model_version: str | None = None,
        decision_cutoff: str | datetime | pd.Timestamp | None = None,
        session_metrics: Mapping[str, Any] | None = None,
    ) -> ShadowStepResult:
        """Atomically advance one account on every trusted trading session.

        A missing target means hold/mark, not skip.  An empty target means an
        explicit cash intent and can only become fills when trusted execution
        observations are present.
        """

        trade = _session_date(trade_date)
        metric_values = dict(session_metrics or {})
        assert_point_in_time_columns(list(metric_values))
        if not isfinite(float(benchmark_return)):
            raise ValueError("benchmark_return must be finite on every session")
        if target_weights is not None:
            weights = {str(ticker): float(weight) for ticker, weight in target_weights.items()}
            if any(not isfinite(weight) for weight in weights.values()):
                raise ValueError("target weights must be finite")
            if not str(model_version or "").strip():
                raise ValueError("model_version is required for a rebalance")
        else:
            weights = None

        if market_bars.empty:
            raise ShadowDataBlocked("empty daily market bars are forbidden")
        execution_available_column = self.execution_config.execution_available_at_column
        if execution_available_column not in market_bars.columns:
            raise ShadowDataBlocked(
                f"daily market bars are missing {execution_available_column}"
            )
        # Calendar evidence is read from execution data because it must exist
        # on both rebalance and hold-only sessions.
        execution_snapshot = self._validate_role_snapshot(
            snapshot_id=snapshot_bindings.execution_snapshot_id,
            role="execution",
            available_by=max(
                self._market_timestamp(
                    value,
                    column=execution_available_column,
                )
                for value in market_bars[execution_available_column]
            )
        )
        trusted_sessions = self._trusted_calendar_sessions(execution_snapshot)
        try:
            trade_index = trusted_sessions.index(trade)
        except ValueError as exc:
            raise ShadowDataBlocked("trade_date is absent from the trusted trading calendar") from exc
        if trade_index == 0:
            raise ShadowDataBlocked("trusted trading calendar has no prior decision session")
        prior_session = trusted_sessions[trade_index - 1]
        decision = _session_date(decision_date) if decision_date is not None else prior_session
        if decision != prior_session:
            raise ShadowDataBlocked("decision_date must be the immediately prior trusted session")
        if decision_cutoff is None:
            cutoff = datetime.combine(decision, time(15, 0), tzinfo=_SHANGHAI).astimezone(
                timezone.utc
            )
        else:
            parsed_cutoff = pd.Timestamp(decision_cutoff)
            if parsed_cutoff.tzinfo is None:
                raise ShadowDataBlocked("decision_cutoff must include a timezone")
            cutoff = parsed_cutoff.tz_convert(timezone.utc).to_pydatetime()

        self._validate_daily_bindings(
            market_bars,
            bindings=snapshot_bindings,
            has_target=weights is not None,
            decision_date=decision,
            trade_date=trade,
            decision_cutoff=cutoff,
        )
        loaded = self._load_account(account_id)
        step_id = self._daily_step_id(
            account_id=account_id,
            decision_date=decision,
            trade_date=trade,
            target_weights=weights,
            bindings=snapshot_bindings,
            model_version=model_version,
            benchmark_return=float(benchmark_return),
            decision_cutoff=cutoff,
            session_metrics=metric_values,
        )
        self._reject_duplicate(account_id, step_id)
        self._require_contiguous_daily_projection(
            account_id=account_id,
            trade_date=trade,
            trusted_sessions=trusted_sessions,
        )
        authoritative_session = loaded.record.as_of.astimezone(_SHANGHAI).date()
        if trade <= authoritative_session:
            raise ShadowStepAlreadyApplied(
                "shadow account is already projected at or beyond the supplied trade session"
            )

        engine = ShadowPortfolioEngine(loaded.account, self.execution_config)
        engine.project_session(
            session_date=trade,
            market_bars=market_bars.copy(deep=True),
            snapshot_bindings=snapshot_bindings,
            benchmark_return=float(benchmark_return),
            trusted_calendar_sessions=trusted_sessions,
            target_weights=weights,
            decision_date=decision,
            model_version=model_version,
            decision_cutoff=cutoff,
            session_metrics=metric_values,
        )
        if not loaded.account.events or not loaded.account.validate_hash_chain():
            raise ShadowCatalogIntegrityError("transient daily shadow event chain is invalid")

        pending: list[ShadowEventInput] = [
            self._domain_event_input(event, step_id=step_id)
            for event in loaded.account.events
        ]
        all_tickers = sorted(loaded.position_tickers | set(loaded.account.positions))
        pending.extend(
            self._position_projection_input(
                step_id=step_id,
                trade_date=trade,
                ticker=ticker,
                position=loaded.account.positions.get(ticker),
            )
            for ticker in all_tickers
        )
        pending.append(
            self._account_projection_input(
                step_id=step_id,
                trade_date=trade,
                account=loaded.account,
                account_status=loaded.record.status,
            )
        )
        committed = self.catalog.append_shadow_events_atomic(
            account_id=account_id,
            events=pending,
            expected_previous_hash=loaded.record.last_event_hash,
        )
        self._validate_committed_segment(
            committed,
            expected_previous_hash=loaded.record.last_event_hash,
            expected_first_sequence=loaded.record.last_event_sequence + 1,
        )
        projected = self._validate_final_projection(
            account=loaded.account,
            record_status=loaded.record.status,
        )
        chain_verified = self.catalog.verify_shadow_chain(account_id)
        if not chain_verified:
            raise ShadowCatalogIntegrityError("daily shadow hash chain failed verification")
        return ShadowStepResult(
            step_id=step_id,
            account_id=account_id,
            decision_date=decision.isoformat(),
            trade_date=trade.isoformat(),
            snapshot_id=snapshot_bindings.mark_snapshot_id,
            model_version=str(model_version or "hold"),
            cash=projected.cash,
            nav=projected.nav,
            benchmark_nav=projected.benchmark_nav,
            position_count=len(loaded.account.positions),
            domain_event_count=len(loaded.account.events),
            persisted_event_count=len(committed),
            first_event_sequence=committed[0].sequence_number,
            last_event_sequence=committed[-1].sequence_number,
            last_event_hash=committed[-1].event_hash,
            chain_verified=chain_verified,
            domain_event_types=tuple(event.event_type for event in loaded.account.events),
            decision_snapshot_id=snapshot_bindings.decision_snapshot_id,
            execution_snapshot_id=snapshot_bindings.execution_snapshot_id,
            mark_snapshot_id=snapshot_bindings.mark_snapshot_id,
            rebalanced=weights is not None,
        )

    def recover_projected_session(
        self,
        *,
        account_id: str,
        step_id: str,
    ) -> ShadowStepResult:
        """Rehydrate the exact latest daily result after a coordinator crash.

        This method accepts only the deterministic step identity produced by
        :meth:`project_session`.  The idempotent wrapper calls it only after a
        full retry input validation found that exact step in the event ledger;
        a changed target, snapshot, metric or benchmark therefore cannot reuse
        an earlier projection.
        """

        normalized_step = str(step_id or "").strip()
        if not normalized_step.startswith("sdp_"):
            raise ShadowCatalogIntegrityError(
                "daily recovery requires a deterministic projection step_id"
            )
        self._load_account(account_id)
        recent = sorted(
            self.catalog.list_shadow_events(account_id=account_id, limit=1_000),
            key=lambda event: event.sequence_number,
        )
        linked = [
            event
            for event in recent
            if isinstance(event.payload.get(_STEP_METADATA_KEY), Mapping)
            and event.payload[_STEP_METADATA_KEY].get("step_id")
            == normalized_step
        ]
        if not linked:
            raise ShadowCatalogIntegrityError(
                "authoritative daily step is absent from the account event chain"
            )
        if any(
            current.sequence_number != previous.sequence_number + 1
            or current.previous_event_hash != previous.event_hash
            for previous, current in zip(linked, linked[1:])
        ):
            raise ShadowCatalogIntegrityError(
                "recovered daily step is not one contiguous event segment"
            )
        projections = [
            event
            for event in linked
            if event.event_type == "account_projected"
            and event.payload[_STEP_METADATA_KEY].get("kind")
            == "account_projection"
        ]
        evidence_events = [
            event for event in linked if event.event_type == "session_evidence"
        ]
        mark_events = [
            event for event in linked if event.event_type == "mark_to_market"
        ]
        target_events = [
            event for event in linked if event.event_type == "target_received"
        ]
        if (
            len(projections) != 1
            or len(evidence_events) != 1
            or len(mark_events) != 1
            or projections[0] is not linked[-1]
        ):
            raise ShadowCatalogIntegrityError(
                "recovered daily step lacks its unique evidence/mark/projection"
            )
        projection = projections[0]
        evidence = dict(evidence_events[0].payload)
        mark = dict(mark_events[0].payload)
        bindings = evidence.get("snapshot_bindings")
        mark_bindings = mark.get("snapshot_bindings")
        if (
            not isinstance(bindings, Mapping)
            or not isinstance(mark_bindings, Mapping)
            or dict(bindings) != dict(mark_bindings)
        ):
            raise ShadowCatalogIntegrityError(
                "recovered daily step snapshot lineage is inconsistent"
            )
        decision_snapshot_id = bindings.get("decision_snapshot_id")
        execution_snapshot_id = str(
            bindings.get("execution_snapshot_id") or ""
        ).strip()
        mark_snapshot_id = str(bindings.get("mark_snapshot_id") or "").strip()
        if not execution_snapshot_id or not mark_snapshot_id:
            raise ShadowCatalogIntegrityError(
                "recovered daily step snapshot lineage is incomplete"
            )
        trade = projection.occurred_at.astimezone(_SHANGHAI).date()
        rebalanced = bool(evidence.get("rebalanced"))
        if rebalanced != (decision_snapshot_id is not None):
            raise ShadowCatalogIntegrityError(
                "recovered daily step rebalance lineage is inconsistent"
            )
        if rebalanced:
            if len(target_events) != 1:
                raise ShadowCatalogIntegrityError(
                    "recovered rebalance lacks its unique target event"
                )
            target = dict(target_events[0].payload)
            decision = target_events[0].occurred_at.astimezone(_SHANGHAI).date()
            model_version = str(target.get("model_version") or "").strip()
            if (
                str(target.get("decision_snapshot_id") or "")
                != str(decision_snapshot_id)
                or str(target.get("execution_snapshot_id") or "")
                != execution_snapshot_id
                or str(target.get("mark_snapshot_id") or "")
                != mark_snapshot_id
                or not model_version
            ):
                raise ShadowCatalogIntegrityError(
                    "recovered target differs from daily snapshot lineage"
                )
        else:
            if target_events:
                raise ShadowCatalogIntegrityError(
                    "recovered hold-only step unexpectedly contains a target"
                )
            execution_snapshot = self.catalog.get_snapshot(execution_snapshot_id)
            if execution_snapshot is None:
                raise ShadowCatalogIntegrityError(
                    "recovered daily step execution snapshot disappeared"
                )
            sessions = self._trusted_calendar_sessions(execution_snapshot)
            try:
                index = sessions.index(trade)
            except ValueError as exc:
                raise ShadowCatalogIntegrityError(
                    "recovered daily step is outside its trusted calendar"
                ) from exc
            if index == 0:
                raise ShadowCatalogIntegrityError(
                    "recovered daily step has no prior decision session"
                )
            decision = sessions[index - 1]
            model_version = "hold"
        account_state = projection.payload.get("account_state")
        if not isinstance(account_state, Mapping):
            raise ShadowCatalogIntegrityError(
                "recovered account projection state is malformed"
            )
        try:
            cash = float(mark["cash"])
            nav = float(mark["nav"])
            benchmark_nav = float(mark["benchmark_nav"])
            position_count = int(mark["position_count"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ShadowCatalogIntegrityError(
                "recovered daily mark state is malformed"
            ) from exc
        for name, expected in (
            ("cash", cash),
            ("nav", nav),
            ("benchmark_nav", benchmark_nav),
        ):
            if not _approximately_equal(
                float(account_state.get(name, float("nan"))), expected
            ):
                raise ShadowCatalogIntegrityError(
                    f"recovered account projection {name} differs from its mark"
                )
        if position_count < 0:
            raise ShadowCatalogIntegrityError(
                "recovered daily position count is invalid"
            )
        chain_verified = self.catalog.verify_shadow_chain(account_id)
        if not chain_verified:
            raise ShadowCatalogIntegrityError(
                "recovered daily shadow hash chain failed verification"
            )
        domain_events = [
            event
            for event in linked
            if event.payload[_STEP_METADATA_KEY].get("kind") == "domain_event"
        ]
        return ShadowStepResult(
            step_id=normalized_step,
            account_id=account_id,
            decision_date=decision.isoformat(),
            trade_date=trade.isoformat(),
            snapshot_id=mark_snapshot_id,
            model_version=model_version,
            cash=cash,
            nav=nav,
            benchmark_nav=benchmark_nav,
            position_count=position_count,
            domain_event_count=len(domain_events),
            persisted_event_count=len(linked),
            first_event_sequence=linked[0].sequence_number,
            last_event_sequence=projection.sequence_number,
            last_event_hash=projection.event_hash,
            chain_verified=chain_verified,
            domain_event_types=tuple(event.event_type for event in domain_events),
            decision_snapshot_id=(
                None
                if decision_snapshot_id is None
                else str(decision_snapshot_id)
            ),
            execution_snapshot_id=execution_snapshot_id,
            mark_snapshot_id=mark_snapshot_id,
            rebalanced=rebalanced,
        )

    def project_or_recover_session(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> ShadowStepResult:
        """Project once, or verify/reuse the exact committed daily step."""

        try:
            return self.project_session(*args, **kwargs)
        except ShadowStepAlreadyApplied as exc:
            if exc.step_id is None:
                raise
            return self.recover_projected_session(
                account_id=str(kwargs.get("account_id") or ""),
                step_id=exc.step_id,
            )

    def step(
        self,
        *,
        account_id: str,
        decision_date: str | date | datetime | pd.Timestamp,
        trade_date: str | date | datetime | pd.Timestamp,
        target_weights: Mapping[str, float],
        market_bars: pd.DataFrame,
        snapshot_id: str,
        model_version: str,
        expected_next_session: str | date | datetime | pd.Timestamp,
        benchmark_return: float | None = None,
        decision_cutoff: str | datetime | pd.Timestamp | None = None,
    ) -> ShadowStepResult:
        if expected_next_session is None:
            raise ShadowDataBlocked(
                "expected_next_session is mandatory exchange-calendar evidence"
            )
        decision = _session_date(decision_date)
        trade = _session_date(trade_date)
        expected = _session_date(expected_next_session)
        if decision_cutoff is None:
            cutoff = datetime.combine(
                decision, time(15, 0), tzinfo=_SHANGHAI
            ).astimezone(timezone.utc)
        else:
            parsed_cutoff = pd.Timestamp(decision_cutoff)
            if parsed_cutoff.tzinfo is None:
                raise ShadowDataBlocked("decision_cutoff must include a timezone")
            cutoff = parsed_cutoff.tz_convert(timezone.utc).to_pydatetime()
        open_cutoff = datetime.combine(
            trade, time(9, 30), tzinfo=_SHANGHAI
        ).astimezone(timezone.utc)
        if cutoff >= open_cutoff:
            raise ShadowDataBlocked(
                "decision_cutoff must precede the next-session execution open"
            )
        if trade <= decision:
            raise ShadowDataBlocked("shadow execution must use a session after the decision close")
        if not snapshot_id or not model_version:
            raise ValueError("snapshot_id and model_version are required")
        weights = {str(ticker): float(weight) for ticker, weight in target_weights.items()}
        if any(not isfinite(weight) for weight in weights.values()):
            raise ValueError("target weights must be finite")
        if benchmark_return is not None and not isfinite(float(benchmark_return)):
            raise ValueError("benchmark_return must be finite")

        snapshot = self._validate_snapshot(
            snapshot_id=snapshot_id,
            decision_date=decision,
            decision_cutoff=cutoff,
        )
        trusted_sessions = self._trusted_calendar_sessions(snapshot)
        authoritative_next = self._next_calendar_session(decision, trusted_sessions)
        if expected != authoritative_next or trade != authoritative_next:
            raise ShadowDataBlocked(
                "trade_date is not the exchange calendar's next session "
                "(persisted trusted calendar)"
            )
        self._validate_market_bars(
            market_bars,
            decision_date=decision,
            trade_date=trade,
            expected_next_session=expected,
            snapshot_id=snapshot_id,
            decision_cutoff=cutoff,
        )
        loaded = self._load_account(account_id)
        step_id = self._step_id(
            account_id=account_id,
            decision_date=decision,
            trade_date=trade,
            expected_next_session=expected,
            target_weights=weights,
            snapshot_id=snapshot_id,
            model_version=model_version,
            benchmark_return=benchmark_return,
            decision_cutoff=cutoff,
        )
        self._reject_duplicate(account_id, step_id)
        authoritative_session = loaded.record.as_of.astimezone(_SHANGHAI).date()
        if trade <= authoritative_session:
            raise ShadowStepAlreadyApplied(
                "shadow account is already projected at or beyond the supplied trade session"
            )

        engine = ShadowPortfolioEngine(loaded.account, self.execution_config)
        engine.execute_target(
            decision_date=decision,
            trade_date=trade,
            expected_next_session=expected,
            target_weights=weights,
            market_bars=market_bars.copy(deep=True),
            snapshot_id=snapshot_id,
            model_version=model_version,
            benchmark_return=benchmark_return,
            trusted_calendar_sessions=trusted_sessions,
            decision_cutoff=cutoff,
        )
        if not loaded.account.events or not loaded.account.validate_hash_chain():
            raise ShadowCatalogIntegrityError("transient shadow engine event chain is invalid")

        pending: list[ShadowEventInput] = [
            self._domain_event_input(event, step_id=step_id)
            for event in loaded.account.events
        ]
        all_tickers = sorted(loaded.position_tickers | set(loaded.account.positions))
        pending.extend(
            self._position_projection_input(
                step_id=step_id,
                trade_date=trade,
                ticker=ticker,
                position=loaded.account.positions.get(ticker),
            )
            for ticker in all_tickers
        )
        pending.append(
            self._account_projection_input(
                step_id=step_id,
                trade_date=trade,
                account=loaded.account,
                account_status=loaded.record.status,
            )
        )

        committed = self.catalog.append_shadow_events_atomic(
            account_id=account_id,
            events=pending,
            expected_previous_hash=loaded.record.last_event_hash,
        )
        self._validate_committed_segment(
            committed,
            expected_previous_hash=loaded.record.last_event_hash,
            expected_first_sequence=loaded.record.last_event_sequence + 1,
        )
        projected = self._validate_final_projection(
            account=loaded.account,
            record_status=loaded.record.status,
        )
        chain_verified = self.catalog.verify_shadow_chain(account_id)
        if not chain_verified:
            raise ShadowCatalogIntegrityError(
                "authoritative shadow hash chain failed verification after commit"
            )
        return ShadowStepResult(
            step_id=step_id,
            account_id=account_id,
            decision_date=decision.isoformat(),
            trade_date=trade.isoformat(),
            snapshot_id=snapshot_id,
            model_version=model_version,
            cash=projected.cash,
            nav=projected.nav,
            benchmark_nav=projected.benchmark_nav,
            position_count=len(loaded.account.positions),
            domain_event_count=len(loaded.account.events),
            persisted_event_count=len(committed),
            first_event_sequence=committed[0].sequence_number,
            last_event_sequence=committed[-1].sequence_number,
            last_event_hash=committed[-1].event_hash,
            chain_verified=chain_verified,
            domain_event_types=tuple(event.event_type for event in loaded.account.events),
        )


__all__ = [
    "ShadowCatalogBridgeError",
    "ShadowCatalogIntegrityError",
    "ShadowDataBlocked",
    "ShadowStepAlreadyApplied",
    "ShadowStepResult",
    "ShadowStepService",
]
