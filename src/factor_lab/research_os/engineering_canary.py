"""Bounded, non-forward engineering canary for the Research OS data plane.

The canary is deliberately not a research experiment.  It accepts only an
in-memory, typed canonical bundle, publishes immutable Bronze/Silver/Gold
lineage markers, and advances one research-only shadow account for exactly 20
sessions.  Every artifact is marked ``engineering_canary`` / ``non_forward``
and can never be reclassified as formal forward evidence.

No filesystem path, broker, network client, factor label, or caller-supplied
performance statistic is part of this contract.  The existing catalog and
shadow engine remain the authorities for snapshots, lifecycle events, account
events, projections, costs, and hash-chain verification.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from math import isfinite
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from .catalog import CatalogConflict, LifecycleEvent, ResearchCatalog, RunRecord
from .contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    LifecycleState,
    SnapshotTier,
)
from .fingerprint import content_fingerprint
from .shadow import ShadowSnapshotBindings, assert_point_in_time_columns
from .shadow_catalog import ShadowStepService


EVIDENCE_CLASS = "engineering_canary"
EVIDENCE_SCOPE = "non_forward"
INPUT_CLASS = "accepted_canonical"
SECURITY_COUNT = 50
PROJECTION_SESSION_COUNT = 20
CALENDAR_SESSION_COUNT = PROJECTION_SESSION_COUNT + 1
INITIAL_CAPITAL = 50_000_000.0

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CANONICAL_BAR_FIELDS = frozenset(
    {
        "ticker",
        "trade_date",
        "open_adj",
        "close_adj",
        "adv_20",
        "volatility_20",
        "execution_event_time",
        "execution_available_at",
        "mark_event_time",
        "mark_available_at",
        "is_suspended",
        "is_one_price_limit_up",
        "is_one_price_limit_down",
        "is_delisted",
    }
)
_FORMAL_OPENING_FIELDS = frozenset(
    {
        "ticker",
        "open_adj",
        "execution_event_time",
        "execution_available_at",
        "is_suspended",
        "is_one_price_limit_up",
        "is_one_price_limit_down",
    }
)


class EngineeringCanaryError(RuntimeError):
    """Base error for a rejected or incomplete engineering canary."""


class CanaryInputRejected(EngineeringCanaryError):
    """Raised when the typed accepted/canonical contract is not satisfied."""


class FormalEpochAdmissionDenied(EngineeringCanaryError):
    """Raised whenever canary output is offered to the formal evidence epoch."""


def _aware(value: datetime, *, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise CanaryInputRejected(f"{name} must include a timezone")
    return value.astimezone(timezone.utc)


def _session_time(session: date, value: time) -> datetime:
    return datetime.combine(session, value, tzinfo=_SHANGHAI).astimezone(timezone.utc)


def _labels() -> dict[str, Any]:
    return {
        "evidence_class": EVIDENCE_CLASS,
        "evidence_scope": EVIDENCE_SCOPE,
        "formal_epoch_eligible": False,
    }


@dataclass(frozen=True)
class OpeningExecutionCapability:
    """Observed source capability, separate from the market rows themselves.

    A deterministic fixture can exercise the engineering pipeline while still
    being correctly rejected for a formal epoch.  Formal readiness additionally
    requires a real bounded probe, immutable probe hash, accepted status, PIT
    timestamps, official open-auction semantics, and the complete trade-status
    field set.
    """

    source_id: str
    status: str
    observed_fields: tuple[str, ...]
    event_semantics: str
    point_in_time: bool
    real_source_probe: bool
    probe_hash: str | None = None

    def __post_init__(self) -> None:
        source = str(self.source_id).strip()
        if not source:
            raise CanaryInputRejected("opening capability requires source_id")
        object.__setattr__(self, "source_id", source)
        status = str(self.status).strip().lower()
        if status not in {"accepted", "degraded", "unavailable", "disputed"}:
            raise CanaryInputRejected("opening capability has an unsupported status")
        object.__setattr__(self, "status", status)
        fields = tuple(sorted({str(item).strip() for item in self.observed_fields if str(item).strip()}))
        assert_point_in_time_columns(list(fields))
        object.__setattr__(self, "observed_fields", fields)
        semantics = str(self.event_semantics).strip().lower()
        if not semantics:
            raise CanaryInputRejected("opening capability requires event_semantics")
        object.__setattr__(self, "event_semantics", semantics)
        if self.probe_hash is not None:
            digest = str(self.probe_hash).strip().lower()
            if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
                raise CanaryInputRejected("opening capability probe_hash must be SHA-256")
            object.__setattr__(self, "probe_hash", digest)

    @property
    def missing_formal_fields(self) -> tuple[str, ...]:
        return tuple(sorted(_FORMAL_OPENING_FIELDS - set(self.observed_fields)))

    @property
    def canary_ready(self) -> bool:
        return (
            self.status in {"accepted", "degraded"}
            and self.point_in_time
            and not self.missing_formal_fields
            and self.event_semantics in {"official_open_auction_09_30", "canonical_open_09_30"}
        )

    @property
    def formal_ready(self) -> bool:
        return (
            self.canary_ready
            and self.status == "accepted"
            and self.real_source_probe
            and self.probe_hash is not None
            and self.event_semantics == "official_open_auction_09_30"
        )

    def fingerprint_payload(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "status": self.status,
            "observed_fields": list(self.observed_fields),
            "event_semantics": self.event_semantics,
            "point_in_time": self.point_in_time,
            "real_source_probe": self.real_source_probe,
            "probe_hash": self.probe_hash,
        }


@dataclass(frozen=True)
class CapabilityAssessment:
    evidence_class: str
    evidence_scope: str
    formal_epoch_eligible: bool
    source_id: str
    canary_ready: bool
    formal_opening_ready: bool
    missing_fields: tuple[str, ...]
    reason: str


def assess_opening_execution_capability(
    capability: OpeningExecutionCapability,
) -> CapabilityAssessment:
    if capability.missing_formal_fields:
        reason = "opening-execution capability is missing required fields"
    elif capability.status != "accepted":
        reason = f"opening-execution capability status is {capability.status}"
    elif not capability.point_in_time:
        reason = "opening-execution capability is not point-in-time"
    elif capability.event_semantics != "official_open_auction_09_30":
        reason = "opening-execution capability lacks official 09:30 auction semantics"
    elif not capability.real_source_probe or capability.probe_hash is None:
        reason = "opening-execution capability lacks a verified real-source probe"
    else:
        reason = "opening-execution capability is technically ready; canary evidence remains non-forward"
    return CapabilityAssessment(
        **_labels(),
        source_id=capability.source_id,
        canary_ready=capability.canary_ready,
        formal_opening_ready=capability.formal_ready,
        missing_fields=capability.missing_formal_fields,
        reason=reason,
    )


@dataclass(frozen=True)
class CanonicalMarketBar:
    """One accepted point-in-time market observation; no extension bag exists."""

    ticker: str
    trade_date: date
    open_adj: float
    close_adj: float
    adv_20: float
    volatility_20: float
    execution_event_time: datetime
    execution_available_at: datetime
    mark_event_time: datetime
    mark_available_at: datetime
    is_suspended: bool = False
    is_one_price_limit_up: bool = False
    is_one_price_limit_down: bool = False
    is_delisted: bool = False

    def __post_init__(self) -> None:
        ticker = str(self.ticker).strip()
        if not ticker:
            raise CanaryInputRejected("canonical market bar requires a ticker")
        object.__setattr__(self, "ticker", ticker)
        for name in ("open_adj", "close_adj", "adv_20", "volatility_20"):
            value = float(getattr(self, name))
            if not isfinite(value) or value <= 0:
                raise CanaryInputRejected(f"canonical market bar {name} must be finite and positive")
            object.__setattr__(self, name, value)
        execution_event = _aware(self.execution_event_time, name="execution_event_time")
        execution_available = _aware(
            self.execution_available_at, name="execution_available_at"
        )
        mark_event = _aware(self.mark_event_time, name="mark_event_time")
        mark_available = _aware(self.mark_available_at, name="mark_available_at")
        expected_open = _session_time(self.trade_date, time(9, 30))
        expected_close = _session_time(self.trade_date, time(15, 0))
        if execution_event != expected_open:
            raise CanaryInputRejected("execution_event_time must be the session 09:30 open")
        if execution_available != expected_open:
            raise CanaryInputRejected("opening observation must be available exactly at the 09:30 canary cutoff")
        if mark_event != expected_close or mark_available < expected_close:
            raise CanaryInputRejected("closing observation must be available no earlier than the 15:00 mark")
        object.__setattr__(self, "execution_event_time", execution_event)
        object.__setattr__(self, "execution_available_at", execution_available)
        object.__setattr__(self, "mark_event_time", mark_event)
        object.__setattr__(self, "mark_available_at", mark_available)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CanonicalMarketBar":
        """Build from a closed schema; unknown/forward-only fields fail closed."""

        assert_point_in_time_columns(list(map(str, value.keys())))
        unknown = set(value) - _CANONICAL_BAR_FIELDS
        missing = _CANONICAL_BAR_FIELDS - set(value)
        if unknown:
            raise CanaryInputRejected(
                "canonical market bar has unsupported fields: " + ", ".join(sorted(map(str, unknown)))
            )
        if missing:
            raise CanaryInputRejected(
                "canonical market bar is missing fields: " + ", ".join(sorted(missing))
            )
        return cls(**dict(value))

    def fingerprint_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CanonicalCanarySession:
    trade_date: date
    bars: tuple[CanonicalMarketBar, ...]

    def __post_init__(self) -> None:
        rows = tuple(self.bars)
        object.__setattr__(self, "bars", rows)
        if len(rows) != SECURITY_COUNT:
            raise CanaryInputRejected(
                f"each canary session requires exactly {SECURITY_COUNT} securities"
            )
        tickers = tuple(row.ticker for row in rows)
        if len(set(tickers)) != SECURITY_COUNT:
            raise CanaryInputRejected("each canary session requires unique tickers")
        if any(row.trade_date != self.trade_date for row in rows):
            raise CanaryInputRejected("market bar trade_date differs from its canary session")
        if tickers != tuple(sorted(tickers)):
            raise CanaryInputRejected("canonical market bars must be sorted by ticker")

    def fingerprint_payload(self) -> dict[str, Any]:
        return {
            "trade_date": self.trade_date,
            "bars": [row.fingerprint_payload() for row in self.bars],
        }


@dataclass(frozen=True)
class AcceptedCanonicalCanaryInput:
    """Closed, in-memory input contract for the 50 x 20 engineering canary."""

    calendar_sessions: tuple[date, ...]
    sessions: tuple[CanonicalCanarySession, ...]
    opening_execution_capability: OpeningExecutionCapability
    input_class: str = INPUT_CLASS
    reconciliation_status: str = "accepted"

    def __post_init__(self) -> None:
        if self.input_class != INPUT_CLASS:
            raise CanaryInputRejected(f"canary requires input_class={INPUT_CLASS!r}")
        if str(self.reconciliation_status).strip().lower() != "accepted":
            raise CanaryInputRejected("canary requires accepted canonical reconciliation")
        calendar = tuple(self.calendar_sessions)
        sessions = tuple(self.sessions)
        object.__setattr__(self, "calendar_sessions", calendar)
        object.__setattr__(self, "sessions", sessions)
        if len(calendar) != CALENDAR_SESSION_COUNT:
            raise CanaryInputRejected(
                f"canary requires exactly {CALENDAR_SESSION_COUNT} trusted calendar sessions"
            )
        if calendar != tuple(sorted(set(calendar))):
            raise CanaryInputRejected("trusted calendar sessions must be unique and ordered")
        if len(sessions) != PROJECTION_SESSION_COUNT:
            raise CanaryInputRejected(
                f"canary requires exactly {PROJECTION_SESSION_COUNT} projected sessions"
            )
        if tuple(item.trade_date for item in sessions) != calendar[1:]:
            raise CanaryInputRejected("projected sessions must exactly match the calendar after its seed session")
        expected_tickers = tuple(row.ticker for row in sessions[0].bars)
        if any(tuple(row.ticker for row in item.bars) != expected_tickers for item in sessions):
            raise CanaryInputRejected("all canary sessions must contain the same canonical 50-security universe")
        if not self.opening_execution_capability.canary_ready:
            assessment = assess_opening_execution_capability(
                self.opening_execution_capability
            )
            raise CanaryInputRejected(
                "opening-execution capability cannot run the engineering canary: "
                + assessment.reason
            )

    @property
    def tickers(self) -> tuple[str, ...]:
        return tuple(row.ticker for row in self.sessions[0].bars)

    @property
    def canonical_hash(self) -> str:
        payload = {
            "input_class": self.input_class,
            "reconciliation_status": self.reconciliation_status,
            "calendar_sessions": self.calendar_sessions,
            "sessions": [item.fingerprint_payload() for item in self.sessions],
            "opening_execution_capability": self.opening_execution_capability.fingerprint_payload(),
        }
        return content_fingerprint(
            payload, domain="factor-lab/research-os/v1/engineering-canary-input"
        )


@dataclass(frozen=True)
class CanaryEvidenceMarker:
    evidence_class: str
    evidence_scope: str
    formal_epoch_eligible: bool
    snapshot_id: str
    tier: str
    role: str
    trade_date: str
    content_hash: str
    parent_snapshot_ids: tuple[str, ...]


@dataclass(frozen=True)
class CanaryDailyProjection:
    evidence_class: str
    evidence_scope: str
    formal_epoch_eligible: bool
    trade_date: str
    decision_snapshot_id: str | None
    execution_snapshot_id: str
    mark_snapshot_id: str
    rebalanced: bool
    cash: float
    nav: float
    benchmark_nav: float
    position_count: int
    account_event_hash: str
    chain_verified: bool


@dataclass(frozen=True)
class EngineeringCanaryResult:
    evidence_class: str
    evidence_scope: str
    formal_epoch_eligible: bool
    run_id: str
    input_hash: str
    sleeve_id: str
    sleeve_state: str
    account_id: str
    security_count: int
    projected_session_count: int
    account_projection_count: int
    chain_verified: bool
    capability: CapabilityAssessment
    evidence_markers: tuple[CanaryEvidenceMarker, ...]
    projections: tuple[CanaryDailyProjection, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def require_formal_epoch_admission(result: EngineeringCanaryResult) -> None:
    """Fail closed: an engineering canary can never become epoch evidence."""

    if not result.capability.formal_opening_ready:
        raise FormalEpochAdmissionDenied(
            "opening-execution capability is insufficient for a formal epoch: "
            + result.capability.reason
        )
    raise FormalEpochAdmissionDenied(
        "engineering_canary/non_forward output cannot be admitted to the formal epoch"
    )


class EngineeringCanaryService:
    """Publish lineage and project one deterministic 50 x 20 shadow canary."""

    def __init__(self, catalog: ResearchCatalog) -> None:
        self.catalog = catalog
        self.shadow = ShadowStepService(catalog)

    @staticmethod
    def _calendar_manifest(bundle: AcceptedCanonicalCanaryInput) -> dict[str, Any]:
        encoded = "\n".join(item.isoformat() for item in bundle.calendar_sessions).encode("ascii")
        import hashlib

        return {
            "quality_status": "accepted",
            "source": "research_os_engineering_canary_canonical",
            "sessions": [item.isoformat() for item in bundle.calendar_sessions],
            "content_hash": hashlib.sha256(encoded).hexdigest(),
        }

    def _publish_snapshot(
        self,
        *,
        run_id: str,
        bundle: AcceptedCanonicalCanaryInput,
        trade_date: date,
        role: str,
        tier: SnapshotTier,
        available_at: datetime,
        source_payload_hash: str,
        parent_snapshot_ids: tuple[str, ...] = (),
        persist: bool = True,
    ) -> tuple[DataSnapshotRef, CanaryEvidenceMarker]:
        manifest = {
            **_labels(),
            "input_class": INPUT_CLASS,
            "run_id": run_id,
            "role": role,
            "tier": tier.value,
            "trade_date": trade_date.isoformat(),
            "source_payload_hash": source_payload_hash,
            "security_count": SECURITY_COUNT,
            "trading_calendar": self._calendar_manifest(bundle),
        }
        digest = content_fingerprint(
            {
                "manifest": manifest,
                "parent_snapshot_ids": parent_snapshot_ids,
                "available_at": available_at,
            },
            domain="factor-lab/research-os/v1/engineering-canary-snapshot",
        )
        snapshot_id = f"engcan_{tier.value}_{digest[:40]}"
        reference = DataSnapshotRef(
            snapshot_id=snapshot_id,
            tier=tier,
            # This is an immutable catalog marker, not a claim that a physical
            # object was uploaded.  The production data plane owns object I/O.
            uri=(
                f"urn:factor-lab:engineering-canary:{run_id}:"
                f"{trade_date.isoformat()}:{role}:{tier.value}:{digest}"
            ),
            content_hash=digest,
            parent_snapshot_ids=parent_snapshot_ids,
            as_of=available_at,
            quality_status=DataQualityStatus.ACCEPTED,
            trust_labels=(EVIDENCE_CLASS, EVIDENCE_SCOPE),
            manifest=manifest,
        )
        if persist:
            self.catalog.register_snapshot(reference)
        marker = CanaryEvidenceMarker(
            **_labels(),
            snapshot_id=reference.snapshot_id,
            tier=tier.value,
            role=role,
            trade_date=trade_date.isoformat(),
            content_hash=reference.content_hash,
            parent_snapshot_ids=reference.parent_snapshot_ids,
        )
        return reference, marker

    def _publish_chain(
        self,
        *,
        run_id: str,
        bundle: AcceptedCanonicalCanaryInput,
        trade_date: date,
        role: str,
        available_at: datetime,
        source_payload: Any,
        persist: bool = True,
    ) -> tuple[DataSnapshotRef, tuple[CanaryEvidenceMarker, ...]]:
        source_hash = content_fingerprint(
            source_payload,
            domain="factor-lab/research-os/v1/engineering-canary-source-payload",
        )
        bronze, bronze_marker = self._publish_snapshot(
            run_id=run_id,
            bundle=bundle,
            trade_date=trade_date,
            role=role,
            tier=SnapshotTier.BRONZE,
            available_at=available_at,
            source_payload_hash=source_hash,
            persist=persist,
        )
        silver, silver_marker = self._publish_snapshot(
            run_id=run_id,
            bundle=bundle,
            trade_date=trade_date,
            role=role,
            tier=SnapshotTier.SILVER,
            available_at=available_at,
            source_payload_hash=source_hash,
            parent_snapshot_ids=(bronze.snapshot_id,),
            persist=persist,
        )
        gold, gold_marker = self._publish_snapshot(
            run_id=run_id,
            bundle=bundle,
            trade_date=trade_date,
            role=role,
            tier=SnapshotTier.GOLD,
            available_at=available_at,
            source_payload_hash=source_hash,
            parent_snapshot_ids=(silver.snapshot_id,),
            persist=persist,
        )
        return gold, (bronze_marker, silver_marker, gold_marker)

    @staticmethod
    def _bars_frame(
        session: CanonicalCanarySession,
        *,
        decision_snapshot_id: str | None,
        execution_snapshot_id: str,
        mark_snapshot_id: str,
    ) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for item in session.bars:
            rows.append(
                {
                    **item.fingerprint_payload(),
                    "decision_snapshot_id": decision_snapshot_id,
                    "execution_snapshot_id": execution_snapshot_id,
                    "mark_snapshot_id": mark_snapshot_id,
                }
            )
        frame = pd.DataFrame(rows)
        assert_point_in_time_columns(frame.columns)
        return frame

    @staticmethod
    def _benchmark_return(session: CanonicalCanarySession) -> float:
        # Same-session open-to-close observations are available at the mark and
        # are never used to create the opening fill.
        return float(
            sum(item.close_adj / item.open_adj - 1.0 for item in session.bars)
            / len(session.bars)
        )

    def _existing_projection(
        self,
        *,
        account_id: str,
        trade_date: date,
        decision_snapshot_id: str | None,
        execution_snapshot_id: str,
        mark_snapshot_id: str,
        rebalanced: bool,
    ) -> CanaryDailyProjection:
        start = datetime.combine(trade_date, time.min, tzinfo=timezone.utc)
        end = datetime.combine(trade_date, time.max, tzinfo=timezone.utc)
        marks = self.catalog.list_shadow_events_by_type(
            account_id=account_id,
            event_type="mark_to_market",
            since=start,
            through=end,
            limit=10,
        )
        projections = self.catalog.list_shadow_events_by_type(
            account_id=account_id,
            event_type="account_projected",
            since=start,
            through=end,
            limit=10,
        )
        if len(marks) != 1 or len(projections) != 1:
            raise EngineeringCanaryError(
                f"existing canary session {trade_date.isoformat()} is incomplete or ambiguous"
            )
        mark = marks[0].payload
        state = projections[0].payload.get("account_state")
        if not isinstance(state, Mapping):
            raise EngineeringCanaryError("existing canary account projection is corrupt")
        return CanaryDailyProjection(
            **_labels(),
            trade_date=trade_date.isoformat(),
            decision_snapshot_id=decision_snapshot_id,
            execution_snapshot_id=execution_snapshot_id,
            mark_snapshot_id=mark_snapshot_id,
            rebalanced=rebalanced,
            cash=float(state["cash"]),
            nav=float(state["nav"]),
            benchmark_nav=float(state["benchmark_nav"]),
            position_count=int(mark["position_count"]),
            account_event_hash=projections[0].event_hash,
            chain_verified=self.catalog.verify_shadow_chain(account_id),
        )

    def _append_lifecycle_path(
        self,
        *,
        sleeve_id: str,
        run_id: str,
        bundle: AcceptedCanonicalCanaryInput,
        completed: bool,
    ) -> None:
        states: Sequence[tuple[LifecycleState | None, LifecycleState, str]] = (
            (None, LifecycleState.PROPOSED, "bounded engineering canary registered"),
            (
                LifecycleState.PROPOSED,
                LifecycleState.PREREGISTERED,
                "fixed 50-security 20-session contract",
            ),
            (
                LifecycleState.PREREGISTERED,
                LifecycleState.CANARY,
                "accepted canonical input admitted to engineering canary only",
            ),
            (
                LifecycleState.CANARY,
                LifecycleState.WALK_FORWARD,
                "deterministic plumbing exercise; no investment inference",
            ),
        )
        base = _session_time(bundle.calendar_sessions[0], time(10, 0))
        for index, (from_state, to_state, cause) in enumerate(states):
            self.catalog.append_lifecycle_event(
                LifecycleEvent(
                    idempotency_key=f"{run_id}:lifecycle:{to_state.value}",
                    sleeve_id=sleeve_id,
                    from_state=from_state,
                    to_state=to_state,
                    cause=cause,
                    occurred_at=base + timedelta(minutes=index),
                    evidence={
                        **_labels(),
                        "run_id": run_id,
                        "input_hash": bundle.canonical_hash,
                    },
                )
            )
        if completed:
            self.catalog.append_lifecycle_event(
                LifecycleEvent(
                    idempotency_key=f"{run_id}:lifecycle:shadow",
                    sleeve_id=sleeve_id,
                    from_state=LifecycleState.WALK_FORWARD,
                    to_state=LifecycleState.SHADOW,
                    cause="20 daily engineering projections completed with a valid event chain",
                    occurred_at=_session_time(bundle.calendar_sessions[-1], time(15, 1)),
                    evidence={
                        **_labels(),
                        "run_id": run_id,
                        "input_hash": bundle.canonical_hash,
                        "account_projection_count": PROJECTION_SESSION_COUNT,
                    },
                )
            )

    @staticmethod
    def _parent_metadata(
        *,
        input_hash: str,
        sleeve_id: str,
        account_id: str,
        capability: CapabilityAssessment,
        evidence_run_id: str,
        attempt_number: int,
    ) -> dict[str, Any]:
        return {
            **_labels(),
            "input_class": INPUT_CLASS,
            "input_hash": input_hash,
            "security_count": SECURITY_COUNT,
            "projected_session_count": PROJECTION_SESSION_COUNT,
            "sleeve_id": sleeve_id,
            "account_id": account_id,
            "opening_execution_formal_ready": capability.formal_opening_ready,
            "evidence_run_id": evidence_run_id,
            "attempt_number": attempt_number,
        }

    @staticmethod
    def _attempt_run_id(evidence_run_id: str, attempt_number: int) -> str:
        if attempt_number < 1:
            raise ValueError("canary attempt_number must be positive")
        return (
            evidence_run_id
            if attempt_number == 1
            else f"{evidence_run_id}_attempt_{attempt_number}"
        )

    @classmethod
    def _attempt_number(cls, run: RunRecord, *, evidence_run_id: str) -> int | None:
        if run.run_id == evidence_run_id:
            return 1
        prefix = f"{evidence_run_id}_attempt_"
        if not run.run_id.startswith(prefix):
            return None
        raw = run.run_id[len(prefix) :]
        if not raw.isdigit() or int(raw) < 2:
            return None
        return int(raw)

    @staticmethod
    def _validate_parent_identity(
        parent: RunRecord,
        *,
        input_hash: str,
        expected_metadata: Mapping[str, Any],
    ) -> None:
        if parent.run_type != EVIDENCE_CLASS or parent.input_fingerprint != input_hash:
            raise EngineeringCanaryError("engineering canary parent identity is corrupt")
        required = {
            key: value
            for key, value in expected_metadata.items()
            if key not in {"attempt_number", "evidence_run_id"}
        }
        if any(parent.metadata.get(key) != value for key, value in required.items()):
            raise EngineeringCanaryError("engineering canary parent metadata is corrupt")

    def _claim_parent_run(
        self,
        *,
        bundle: AcceptedCanonicalCanaryInput,
        input_hash: str,
        evidence_run_id: str,
        sleeve_id: str,
        account_id: str,
        capability: CapabilityAssessment,
    ) -> tuple[RunRecord, bool]:
        """Create the authoritative parent before any child evidence is written.

        A running parent resumes its idempotent children. A succeeded parent is
        read-only and is verified without touching child records. A failed
        parent remains immutable; recovery creates a new deterministic attempt
        id while reusing the stable evidence/account identities.
        """

        for _ in range(100):
            attempts: list[tuple[int, RunRecord]] = []
            for candidate in self.catalog.list_runs(limit=1_000, run_type=EVIDENCE_CLASS):
                attempt_number = self._attempt_number(
                    candidate, evidence_run_id=evidence_run_id
                )
                if attempt_number is not None:
                    attempts.append((attempt_number, candidate))
            attempts.sort(key=lambda item: item[0])
            if attempts:
                attempt_number, latest = attempts[-1]
                expected = self._parent_metadata(
                    input_hash=input_hash,
                    sleeve_id=sleeve_id,
                    account_id=account_id,
                    capability=capability,
                    evidence_run_id=evidence_run_id,
                    attempt_number=attempt_number,
                )
                self._validate_parent_identity(
                    latest,
                    input_hash=input_hash,
                    expected_metadata=expected,
                )
                if latest.status == "succeeded":
                    return latest, True
                if latest.status == "running":
                    return latest, False
                if latest.status != "failed":
                    raise EngineeringCanaryError(
                        f"unsupported engineering canary parent status {latest.status!r}"
                    )
                attempt_number += 1
            else:
                attempt_number = 1

            proposed = RunRecord(
                run_id=self._attempt_run_id(evidence_run_id, attempt_number),
                run_type=EVIDENCE_CLASS,
                status="running",
                input_fingerprint=input_hash,
                started_at=(
                    _session_time(bundle.calendar_sessions[0], time(9, 0))
                    + timedelta(microseconds=attempt_number - 1)
                ),
                metadata=self._parent_metadata(
                    input_hash=input_hash,
                    sleeve_id=sleeve_id,
                    account_id=account_id,
                    capability=capability,
                    evidence_run_id=evidence_run_id,
                    attempt_number=attempt_number,
                ),
            )
            try:
                stored, won = self.catalog.claim_run(proposed)
            except CatalogConflict:
                # Another process may have terminalized the same newly chosen
                # attempt between discovery and claim. Re-read and select the
                # next immutable attempt rather than overwriting it.
                continue
            if won or stored.status == "running":
                return stored, False
            if stored.status == "succeeded":
                return stored, True
        raise EngineeringCanaryError("engineering canary attempt allocation did not converge")

    def _terminalize_parent(
        self,
        *,
        parent: RunRecord,
        status: str,
        metadata: Mapping[str, Any],
        completed_at: datetime,
        error: str | None = None,
    ) -> RunRecord:
        latest = self.catalog.get_run(parent.run_id)
        if latest is None:
            raise EngineeringCanaryError("engineering canary parent run disappeared")
        if latest.status == "succeeded":
            return latest
        return self.catalog.save_run(
            RunRecord(
                run_id=latest.run_id,
                run_type=latest.run_type,
                status=status,
                input_fingerprint=latest.input_fingerprint,
                started_at=latest.started_at,
                completed_at=max(completed_at, latest.started_at),
                metadata=dict(metadata),
                error=error,
            )
        )

    def _read_succeeded(
        self,
        bundle: AcceptedCanonicalCanaryInput,
        *,
        parent: RunRecord,
        evidence_run_id: str,
    ) -> EngineeringCanaryResult:
        """Reconstruct and verify a succeeded canary without any writes."""

        input_hash = bundle.canonical_hash
        sleeve_id = f"engineering_canary_sleeve_{input_hash[:20]}"
        account_id = f"engineering_canary_account_{input_hash[:20]}"
        capability = assess_opening_execution_capability(
            bundle.opening_execution_capability
        )
        markers: list[CanaryEvidenceMarker] = []
        projections: list[CanaryDailyProjection] = []

        def expected_chain(
            *,
            trade_date: date,
            role: str,
            available_at: datetime,
            source_payload: Any,
        ) -> tuple[DataSnapshotRef, tuple[CanaryEvidenceMarker, ...]]:
            gold, expected_markers = self._publish_chain(
                run_id=evidence_run_id,
                bundle=bundle,
                trade_date=trade_date,
                role=role,
                available_at=available_at,
                source_payload=source_payload,
                persist=False,
            )
            for marker in expected_markers:
                stored = self.catalog.get_snapshot(marker.snapshot_id)
                if stored is None:
                    raise EngineeringCanaryError(
                        "succeeded canary is missing immutable snapshot evidence"
                    )
                reference = stored.reference
                if (
                    reference.tier.value != marker.tier
                    or reference.content_hash != marker.content_hash
                    or reference.parent_snapshot_ids != marker.parent_snapshot_ids
                    or reference.trust_labels != (EVIDENCE_CLASS, EVIDENCE_SCOPE)
                    or reference.manifest.get("run_id") != evidence_run_id
                    or reference.manifest.get("input_class") != INPUT_CLASS
                ):
                    raise EngineeringCanaryError(
                        "succeeded canary immutable snapshot evidence differs"
                    )
            return gold, expected_markers

        seed_date = bundle.calendar_sessions[0]
        seed_gold, seed_markers = expected_chain(
            trade_date=seed_date,
            role="decision_seed",
            available_at=_session_time(seed_date, time(15, 0)),
            source_payload={
                "calendar_seed": seed_date,
                "tickers": bundle.tickers,
                **_labels(),
            },
        )
        markers.extend(seed_markers)
        previous_mark = seed_gold
        for index, session in enumerate(bundle.sessions):
            trade_date = session.trade_date
            execution_gold, execution_markers = expected_chain(
                trade_date=trade_date,
                role="execution_open",
                available_at=_session_time(trade_date, time(9, 30)),
                source_payload=[
                    {
                        "ticker": item.ticker,
                        "trade_date": item.trade_date,
                        "open_adj": item.open_adj,
                        "adv_20": item.adv_20,
                        "volatility_20": item.volatility_20,
                        "execution_event_time": item.execution_event_time,
                        "execution_available_at": item.execution_available_at,
                        "is_suspended": item.is_suspended,
                        "is_one_price_limit_up": item.is_one_price_limit_up,
                        "is_one_price_limit_down": item.is_one_price_limit_down,
                        "is_delisted": item.is_delisted,
                    }
                    for item in session.bars
                ],
            )
            mark_gold, mark_markers = expected_chain(
                trade_date=trade_date,
                role="closing_mark",
                available_at=_session_time(trade_date, time(15, 0)),
                source_payload=[item.fingerprint_payload() for item in session.bars],
            )
            markers.extend(execution_markers)
            markers.extend(mark_markers)
            rebalanced = index % 5 == 0
            decision_snapshot_id = previous_mark.snapshot_id if rebalanced else None
            projections.append(
                self._existing_projection(
                    account_id=account_id,
                    trade_date=trade_date,
                    decision_snapshot_id=decision_snapshot_id,
                    execution_snapshot_id=execution_gold.snapshot_id,
                    mark_snapshot_id=mark_gold.snapshot_id,
                    rebalanced=rebalanced,
                )
            )
            previous_mark = mark_gold

        account = self.catalog.get_shadow_account(account_id)
        account_projection_count = self.catalog.count_shadow_sessions(
            account_id=account_id,
            since=seed_date,
            through=bundle.calendar_sessions[-1],
        )
        lifecycle = self.catalog.list_lifecycle_events(
            sleeve_id=sleeve_id, limit=100
        )
        if (
            account is None
            or abs(account.initial_capital - INITIAL_CAPITAL) > 0.01
            or account_projection_count != PROJECTION_SESSION_COUNT
            or len(lifecycle) != 5
            or self.catalog.latest_lifecycle_state(sleeve_id) is not LifecycleState.SHADOW
            or not self.catalog.verify_shadow_chain(account_id)
        ):
            raise EngineeringCanaryError(
                "succeeded canary child evidence is incomplete or corrupt"
            )
        return EngineeringCanaryResult(
            **_labels(),
            run_id=parent.run_id,
            input_hash=input_hash,
            sleeve_id=sleeve_id,
            sleeve_state=LifecycleState.SHADOW.value,
            account_id=account_id,
            security_count=SECURITY_COUNT,
            projected_session_count=len(projections),
            account_projection_count=account_projection_count,
            chain_verified=True,
            capability=capability,
            evidence_markers=tuple(markers),
            projections=tuple(projections),
        )

    def run(self, bundle: AcceptedCanonicalCanaryInput) -> EngineeringCanaryResult:
        if not isinstance(bundle, AcceptedCanonicalCanaryInput):
            raise TypeError("engineering canary accepts only AcceptedCanonicalCanaryInput")
        input_hash = bundle.canonical_hash
        evidence_run_id = f"engcan_{input_hash[:32]}"
        sleeve_id = f"engineering_canary_sleeve_{input_hash[:20]}"
        account_id = f"engineering_canary_account_{input_hash[:20]}"
        capability = assess_opening_execution_capability(
            bundle.opening_execution_capability
        )
        parent, read_only = self._claim_parent_run(
            bundle=bundle,
            input_hash=input_hash,
            evidence_run_id=evidence_run_id,
            sleeve_id=sleeve_id,
            account_id=account_id,
            capability=capability,
        )
        if read_only:
            return self._read_succeeded(
                bundle,
                parent=parent,
                evidence_run_id=evidence_run_id,
            )
        metadata = dict(parent.metadata)
        try:
            result = self._run_claimed(
                bundle,
                parent_run_id=parent.run_id,
                evidence_run_id=evidence_run_id,
            )
        except Exception as exc:
            self._terminalize_parent(
                parent=parent,
                status="failed",
                metadata={
                    **metadata,
                    "failure_type": type(exc).__name__,
                },
                completed_at=datetime.now(timezone.utc),
                error=f"{type(exc).__name__}: {exc}",
            )
            raise

        self._terminalize_parent(
            parent=parent,
            status="succeeded",
            metadata=metadata,
            completed_at=_session_time(bundle.calendar_sessions[-1], time(15, 2)),
        )
        return result

    def _run_claimed(
        self,
        bundle: AcceptedCanonicalCanaryInput,
        *,
        parent_run_id: str,
        evidence_run_id: str,
    ) -> EngineeringCanaryResult:
        if not isinstance(bundle, AcceptedCanonicalCanaryInput):
            raise TypeError("engineering canary accepts only AcceptedCanonicalCanaryInput")
        input_hash = bundle.canonical_hash
        sleeve_id = f"engineering_canary_sleeve_{input_hash[:20]}"
        account_id = f"engineering_canary_account_{input_hash[:20]}"
        capability = assess_opening_execution_capability(
            bundle.opening_execution_capability
        )
        markers: list[CanaryEvidenceMarker] = []
        projections: list[CanaryDailyProjection] = []

        seed_date = bundle.calendar_sessions[0]
        seed_gold, seed_markers = self._publish_chain(
            run_id=evidence_run_id,
            bundle=bundle,
            trade_date=seed_date,
            role="decision_seed",
            available_at=_session_time(seed_date, time(15, 0)),
            source_payload={
                "calendar_seed": seed_date,
                "tickers": bundle.tickers,
                **_labels(),
            },
        )
        markers.extend(seed_markers)

        account = self.catalog.get_shadow_account(account_id)
        if account is None:
            self.catalog.create_shadow_account(
                account_id=account_id,
                name="Engineering canary / non-forward shadow account",
                initial_capital=INITIAL_CAPITAL,
                opened_at=_session_time(seed_date, time(9, 0)),
            )
        elif abs(account.initial_capital - INITIAL_CAPITAL) > 0.01:
            raise EngineeringCanaryError("existing deterministic canary account identity is corrupt")
        self._append_lifecycle_path(
            sleeve_id=sleeve_id,
            run_id=evidence_run_id,
            bundle=bundle,
            completed=False,
        )

        previous_mark = seed_gold
        for index, session in enumerate(bundle.sessions):
            trade_date = session.trade_date
            open_payload = [
                {
                    "ticker": item.ticker,
                    "trade_date": item.trade_date,
                    "open_adj": item.open_adj,
                    "adv_20": item.adv_20,
                    "volatility_20": item.volatility_20,
                    "execution_event_time": item.execution_event_time,
                    "execution_available_at": item.execution_available_at,
                    "is_suspended": item.is_suspended,
                    "is_one_price_limit_up": item.is_one_price_limit_up,
                    "is_one_price_limit_down": item.is_one_price_limit_down,
                    "is_delisted": item.is_delisted,
                }
                for item in session.bars
            ]
            execution_gold, execution_markers = self._publish_chain(
                run_id=evidence_run_id,
                bundle=bundle,
                trade_date=trade_date,
                role="execution_open",
                available_at=_session_time(trade_date, time(9, 30)),
                source_payload=open_payload,
            )
            mark_gold, mark_markers = self._publish_chain(
                run_id=evidence_run_id,
                bundle=bundle,
                trade_date=trade_date,
                role="closing_mark",
                available_at=_session_time(trade_date, time(15, 0)),
                source_payload=[item.fingerprint_payload() for item in session.bars],
            )
            markers.extend(execution_markers)
            markers.extend(mark_markers)

            rebalanced = index % 5 == 0
            decision_snapshot_id = previous_mark.snapshot_id if rebalanced else None
            bindings = ShadowSnapshotBindings(
                decision_snapshot_id=decision_snapshot_id,
                execution_snapshot_id=execution_gold.snapshot_id,
                mark_snapshot_id=mark_gold.snapshot_id,
            )
            existing = self.catalog.get_shadow_account(account_id)
            if existing is None:
                raise EngineeringCanaryError("canary shadow account disappeared")
            existing_session = existing.as_of.astimezone(_SHANGHAI).date()
            if existing_session >= trade_date:
                projection = self._existing_projection(
                    account_id=account_id,
                    trade_date=trade_date,
                    decision_snapshot_id=decision_snapshot_id,
                    execution_snapshot_id=execution_gold.snapshot_id,
                    mark_snapshot_id=mark_gold.snapshot_id,
                    rebalanced=rebalanced,
                )
            else:
                weights = (
                    {ticker: 1.0 / SECURITY_COUNT for ticker in bundle.tickers}
                    if rebalanced
                    else None
                )
                result = self.shadow.project_session(
                    account_id=account_id,
                    trade_date=trade_date,
                    market_bars=self._bars_frame(
                        session,
                        decision_snapshot_id=decision_snapshot_id,
                        execution_snapshot_id=execution_gold.snapshot_id,
                        mark_snapshot_id=mark_gold.snapshot_id,
                    ),
                    snapshot_bindings=bindings,
                    benchmark_return=self._benchmark_return(session),
                    target_weights=weights,
                    decision_date=bundle.calendar_sessions[index],
                    model_version=("engineering-canary-equal-weight-v1" if rebalanced else None),
                    decision_cutoff=_session_time(
                        bundle.calendar_sessions[index], time(15, 0)
                    ),
                    session_metrics={
                        **_labels(),
                        "input_class": INPUT_CLASS,
                        "security_count": SECURITY_COUNT,
                    },
                )
                projection = CanaryDailyProjection(
                    **_labels(),
                    trade_date=result.trade_date,
                    decision_snapshot_id=result.decision_snapshot_id,
                    execution_snapshot_id=result.execution_snapshot_id or "",
                    mark_snapshot_id=result.mark_snapshot_id or "",
                    rebalanced=result.rebalanced,
                    cash=result.cash,
                    nav=result.nav,
                    benchmark_nav=result.benchmark_nav,
                    position_count=result.position_count,
                    account_event_hash=result.last_event_hash,
                    chain_verified=result.chain_verified,
                )
            projections.append(projection)
            previous_mark = mark_gold

        account_projection_count = self.catalog.count_shadow_sessions(
            account_id=account_id,
            since=seed_date,
            through=bundle.calendar_sessions[-1],
        )
        chain_verified = self.catalog.verify_shadow_chain(account_id)
        if account_projection_count != PROJECTION_SESSION_COUNT or not chain_verified:
            raise EngineeringCanaryError(
                "canary requires exactly 20 authoritative daily projections and a valid event chain"
            )
        if any(not item.chain_verified for item in projections):
            raise EngineeringCanaryError("a canary daily projection did not verify its event chain")

        self._append_lifecycle_path(
            sleeve_id=sleeve_id,
            run_id=evidence_run_id,
            bundle=bundle,
            completed=True,
        )
        if self.catalog.latest_lifecycle_state(sleeve_id) is not LifecycleState.SHADOW:
            raise EngineeringCanaryError("engineering Sleeve did not reach SHADOW")

        return EngineeringCanaryResult(
            **_labels(),
            run_id=parent_run_id,
            input_hash=input_hash,
            sleeve_id=sleeve_id,
            sleeve_state=LifecycleState.SHADOW.value,
            account_id=account_id,
            security_count=SECURITY_COUNT,
            projected_session_count=len(projections),
            account_projection_count=account_projection_count,
            chain_verified=chain_verified,
            capability=capability,
            evidence_markers=tuple(markers),
            projections=tuple(projections),
        )


__all__ = [
    "AcceptedCanonicalCanaryInput",
    "CALENDAR_SESSION_COUNT",
    "CanaryDailyProjection",
    "CanaryEvidenceMarker",
    "CanaryInputRejected",
    "CanonicalCanarySession",
    "CanonicalMarketBar",
    "CapabilityAssessment",
    "EVIDENCE_CLASS",
    "EVIDENCE_SCOPE",
    "EngineeringCanaryError",
    "EngineeringCanaryResult",
    "EngineeringCanaryService",
    "FormalEpochAdmissionDenied",
    "INPUT_CLASS",
    "OpeningExecutionCapability",
    "PROJECTION_SESSION_COUNT",
    "SECURITY_COUNT",
    "assess_opening_execution_capability",
    "require_formal_epoch_admission",
]
