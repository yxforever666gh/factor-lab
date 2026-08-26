"""Deterministic recovery-case workflow backed only by catalog evidence.

The lifecycle monitor opens a recovery case.  This module closes the missing
middle of the workflow: persist a bounded diagnosis, associate at most three
registered Challenger experiments, bind their shadow accounts, and evaluate
the 60-new-session observation gate from the event ledger.  Callers cannot
claim recovery with an in-memory counter or an arbitrary report file.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta
from math import isfinite
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from .catalog import CatalogConflict, LifecycleEvent, ResearchCatalog
from .contracts import (
    DataQualityStatus,
    LifecycleState,
    RecoveryCase,
    RecoveryCaseStatus,
    SnapshotTier,
)
from .fingerprint import content_fingerprint


_ACTIVE_CASE_STATUSES = {
    RecoveryCaseStatus.OPEN,
    RecoveryCaseStatus.DIAGNOSING,
    RecoveryCaseStatus.OBSERVING,
}
_FORBIDDEN_TRUST_MARKERS = ("unverified", "disputed", "quarantined", "frozen")
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class RecoveryWorkflowError(RuntimeError):
    """Raised when persisted evidence cannot authorize a recovery action."""


@dataclass(frozen=True)
class ChallengerObservation:
    challenger_id: str
    account_id: str
    observation_started_at: datetime | None
    baseline_at: datetime | None
    baseline_event_hash: str
    last_session_at: datetime | None
    last_session_event_hash: str
    chain_verified: bool
    session_count: int
    active_return_20: float | None
    active_return_60: float | None
    health_observed_at: datetime | None
    ic_direction_restored: bool
    risk_alerts: tuple[str, ...]
    eligible_for_recovery: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RecoveryObservationResult:
    recovery_case_id: str
    as_of: datetime
    observations: tuple[ChallengerObservation, ...]
    eligible_challenger_ids: tuple[str, ...]
    observation_complete: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")


def _event_state(case: RecoveryCase) -> LifecycleState:
    return LifecycleState(case.lifecycle_state.value)


def _parse_datetime(value: Any, *, name: str) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value)
        parsed = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    _require_aware(parsed, name)
    return parsed


def _required_datetime(value: Any, *, name: str) -> datetime:
    parsed = _parse_datetime(value, name=name)
    if parsed is None:
        raise ValueError(f"{name} is required")
    return parsed


def _normalized_strings(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for item in values if (value := str(item).strip())))


def _idempotency_key(kind: str, payload: Mapping[str, Any]) -> str:
    digest = content_fingerprint(
        payload, domain=f"factor-lab/research-os/v1/recovery-idempotency/{kind}"
    )
    return f"recovery:{kind}:{digest[:40]}"


@dataclass(frozen=True)
class _ProjectionPoint:
    occurred_at: datetime
    nav: float
    benchmark_nav: float
    event_hash: str
    sequence_number: int


def _projection_from_event(event: Any) -> _ProjectionPoint | None:
    if event.event_type != "account_projected":
        return None
    state = event.payload.get("account_state")
    if not isinstance(state, Mapping):
        return None
    try:
        nav = float(state["nav"])
        benchmark_nav = float(state["benchmark_nav"])
    except (KeyError, TypeError, ValueError):
        return None
    if not isfinite(nav) or not isfinite(benchmark_nav) or min(nav, benchmark_nav) <= 0:
        return None
    return _ProjectionPoint(
        occurred_at=event.occurred_at,
        nav=nav,
        benchmark_nav=benchmark_nav,
        event_hash=str(event.event_hash),
        sequence_number=int(event.sequence_number),
    )


class RecoveryCoordinator:
    """Persist and evaluate the recovery SLA without trusting caller counters."""

    def __init__(self, catalog: ResearchCatalog) -> None:
        self.catalog = catalog

    def _case(self, recovery_case_id: str) -> RecoveryCase:
        case = self.catalog.get_recovery_case(str(recovery_case_id))
        if case is None:
            raise RecoveryWorkflowError(f"recovery case {recovery_case_id!r} was not found")
        if case.status not in _ACTIVE_CASE_STATUSES:
            raise RecoveryWorkflowError(
                f"recovery case {recovery_case_id!r} is already {case.status.value}"
            )
        return case

    def _case_any_status(self, recovery_case_id: str) -> RecoveryCase:
        case = self.catalog.get_recovery_case(str(recovery_case_id))
        if case is None:
            raise RecoveryWorkflowError(f"recovery case {recovery_case_id!r} was not found")
        return case

    def _events(self, case: RecoveryCase) -> list[LifecycleEvent]:
        return self.catalog.list_lifecycle_events(sleeve_id=case.sleeve_id, limit=1_000)

    def _causal_event_time(
        self, sleeve_id: str, requested_at: datetime
    ) -> datetime:
        latest = self.catalog.list_lifecycle_events(
            sleeve_id=sleeve_id, limit=1
        )
        if latest and requested_at <= latest[0].occurred_at:
            return latest[0].occurred_at + timedelta(microseconds=1)
        return requested_at

    def _append_case_event(
        self, case: RecoveryCase, event: LifecycleEvent
    ) -> LifecycleEvent:
        """Append recovery evidence, bootstrapping legacy empty streams atomically."""

        history = self.catalog.list_lifecycle_events(
            sleeve_id=case.sleeve_id, limit=1_000
        )
        if history:
            return self.catalog.append_lifecycle_event(event)
        root_time = case.triggered_at
        event_time = max(event.occurred_at, root_time + timedelta(microseconds=1))
        bootstrap = LifecycleEvent(
            idempotency_key=(
                f"recovery:bootstrap:{case.recovery_case_id}:"
                f"{case.lifecycle_state.value}"
            ),
            sleeve_id=case.sleeve_id,
            from_state=None,
            to_state=case.lifecycle_state,
            cause="recovery_case_state_bootstrapped",
            occurred_at=root_time,
            evidence={"recovery_case_id": case.recovery_case_id},
        )
        requested = replace(
            event,
            from_state=case.lifecycle_state,
            to_state=(
                case.lifecycle_state
                if event.from_state == event.to_state
                else event.to_state
            ),
            occurred_at=event_time,
        )
        return self.catalog.append_lifecycle_path((bootstrap, requested))[-1]

    def _advance_case_status(
        self,
        recovery_case_id: str,
        target: RecoveryCaseStatus,
        *,
        lifecycle_state: LifecycleState | None = None,
    ) -> RecoveryCase:
        order = {
            RecoveryCaseStatus.OPEN: 0,
            RecoveryCaseStatus.DIAGNOSING: 1,
            RecoveryCaseStatus.OBSERVING: 2,
            RecoveryCaseStatus.RECOVERED: 3,
            RecoveryCaseStatus.CLOSED: 4,
        }
        for _ in range(8):
            current = self._case_any_status(recovery_case_id)
            updates: dict[str, Any] = {}
            if order[current.status] < order[target]:
                updates["status"] = target
            if lifecycle_state is not None and current.lifecycle_state != lifecycle_state:
                updates["lifecycle_state"] = lifecycle_state
            if not updates:
                return current
            try:
                return self.catalog.save_recovery_case(
                    current.model_copy(update=updates)
                )
            except CatalogConflict:
                continue
        raise RecoveryWorkflowError(
            "recovery case projection remained contended after durable evidence was written"
        )

    @staticmethod
    def _case_event(
        events: Sequence[LifecycleEvent],
        case: RecoveryCase,
        cause: str,
    ) -> LifecycleEvent | None:
        return next(
            (
                event
                for event in events
                if event.cause == cause
                and event.evidence.get("recovery_case_id") == case.recovery_case_id
            ),
            None,
        )

    @staticmethod
    def _registered_challengers(
        case: RecoveryCase,
        events: Sequence[LifecycleEvent],
    ) -> tuple[str, ...]:
        """Rebuild the monotonic Challenger set after a partial write/retry."""

        registered = list(case.challenger_ids)
        registration_events = sorted(
            (
                event
                for event in events
                if event.cause == "recovery_challenger_registered"
                and event.evidence.get("recovery_case_id") == case.recovery_case_id
            ),
            key=lambda event: (event.occurred_at, event.idempotency_key),
        )
        for event in registration_events:
            challenger_id = str(event.evidence.get("challenger_id") or "").strip()
            if challenger_id and challenger_id not in registered:
                registered.append(challenger_id)

        # Compatibility with the first implementation, which wrote the whole
        # tuple in a single event.  It remains append-only evidence.
        legacy_events = sorted(
            (
                event
                for event in events
                if event.cause == "recovery_challengers_registered"
                and event.evidence.get("recovery_case_id") == case.recovery_case_id
            ),
            key=lambda event: (event.occurred_at, event.idempotency_key),
        )
        for event in legacy_events:
            for challenger_id in event.evidence.get("challenger_ids", ()):
                normalized = str(challenger_id).strip()
                if normalized and normalized not in registered:
                    registered.append(normalized)
        return tuple(registered)

    def _accepted_gold(self, snapshot_id: str) -> None:
        snapshot = self.catalog.get_snapshot(snapshot_id)
        if snapshot is None:
            raise RecoveryWorkflowError("recovery evidence snapshot is not registered")
        reference = snapshot.reference
        if (
            reference.tier is not SnapshotTier.GOLD
            or reference.quality_status is not DataQualityStatus.ACCEPTED
        ):
            raise RecoveryWorkflowError("recovery evidence requires an accepted Gold snapshot")
        if any(
            marker in str(label).lower()
            for label in reference.trust_labels
            for marker in _FORBIDDEN_TRUST_MARKERS
        ):
            raise RecoveryWorkflowError("unverified Gold evidence cannot advance recovery")

    def complete_diagnosis(
        self,
        recovery_case_id: str,
        *,
        diagnosed_at: datetime,
        snapshot_id: str,
        findings: Mapping[str, Any],
        diagnostic_branches: Sequence[str] = (),
    ) -> RecoveryCase:
        _require_aware(diagnosed_at, "diagnosed_at")
        case = self._case(recovery_case_id)
        if diagnosed_at < case.triggered_at:
            raise RecoveryWorkflowError("diagnosis predates the recovery trigger")
        if not findings:
            raise RecoveryWorkflowError("a recovery diagnosis cannot be empty")
        branches = _normalized_strings(diagnostic_branches)
        if len(branches) > 2:
            raise RecoveryWorkflowError("at most two preregistered diagnostic branches are allowed")
        self._accepted_gold(snapshot_id)
        evidence = {
            "recovery_case_id": case.recovery_case_id,
            "snapshot_id": snapshot_id,
            "findings": dict(findings),
            "diagnostic_branches": list(branches),
        }
        evidence["diagnosis_hash"] = content_fingerprint(
            evidence, domain="factor-lab/research-os/v1/recovery-diagnosis"
        )
        existing = self._case_event(
            self._events(case), case, "recovery_diagnosis_completed"
        )
        if existing is not None:
            if existing.evidence.get("diagnosis_hash") != evidence["diagnosis_hash"]:
                raise RecoveryWorkflowError("the persisted recovery diagnosis is immutable")
            if case.status is RecoveryCaseStatus.OPEN:
                case = self.catalog.save_recovery_case(
                    case.model_copy(update={"status": RecoveryCaseStatus.DIAGNOSING})
                )
            return case
        state = _event_state(case)
        self._append_case_event(
            case,
            LifecycleEvent(
                idempotency_key=_idempotency_key(
                    "diagnosis", {"recovery_case_id": case.recovery_case_id}
                ),
                sleeve_id=case.sleeve_id,
                from_state=state,
                to_state=state,
                cause="recovery_diagnosis_completed",
                evidence=evidence,
                occurred_at=self._causal_event_time(case.sleeve_id, diagnosed_at),
            )
        )
        return self._advance_case_status(
            case.recovery_case_id, RecoveryCaseStatus.DIAGNOSING
        )

    def register_challengers(
        self,
        recovery_case_id: str,
        challenger_ids: Sequence[str],
        *,
        registered_at: datetime,
    ) -> RecoveryCase:
        _require_aware(registered_at, "registered_at")
        case = self._case(recovery_case_id)
        additions = _normalized_strings(challenger_ids)
        if not 1 <= len(additions) <= 3:
            raise RecoveryWorkflowError("a registration call requires one to three Challengers")
        lifecycle_events = self._events(case)
        diagnosis = self._case_event(
            lifecycle_events, case, "recovery_diagnosis_completed"
        )
        if diagnosis is None:
            raise RecoveryWorkflowError("Challengers cannot be registered before diagnosis")
        if registered_at < diagnosis.occurred_at:
            raise RecoveryWorkflowError("Challenger registration predates the diagnosis")
        current_ids = self._registered_challengers(case, lifecycle_events)
        merged_ids = current_ids + tuple(item for item in additions if item not in current_ids)
        if len(merged_ids) > 3:
            raise RecoveryWorkflowError("a recovery case cannot register more than three Challengers")
        missing = [item for item in additions if self.catalog.get_experiment(item) is None]
        if missing:
            raise RecoveryWorkflowError(f"Challenger experiments are not registered: {missing}")
        state = _event_state(case)
        for challenger_id in additions:
            if challenger_id in current_ids:
                continue
            registration_hash = content_fingerprint(
                {
                    "recovery_case_id": case.recovery_case_id,
                    "challenger_id": challenger_id,
                },
                domain="factor-lab/research-os/v1/recovery-challenger-registration",
            )
            self._append_case_event(
                case,
                LifecycleEvent(
                    idempotency_key=(
                        _idempotency_key(
                            "challenger",
                            {
                                "recovery_case_id": case.recovery_case_id,
                                "challenger_id": challenger_id,
                            },
                        )
                    ),
                    sleeve_id=case.sleeve_id,
                    from_state=state,
                    to_state=state,
                    cause="recovery_challenger_registered",
                    evidence={
                        "recovery_case_id": case.recovery_case_id,
                        "challenger_id": challenger_id,
                        "registration_hash": registration_hash,
                    },
                    occurred_at=self._causal_event_time(
                        case.sleeve_id, registered_at
                    ),
                )
            )

        # Rebuild from events so a retry also repairs a case projection after a
        # crash between append_lifecycle_event and save_recovery_case.
        persisted_ids = self._registered_challengers(case, self._events(case))
        if len(persisted_ids) > 3:
            raise RecoveryWorkflowError("persisted recovery evidence exceeds three Challengers")
        for _ in range(8):
            current = self._case_any_status(case.recovery_case_id)
            persisted_ids = self._registered_challengers(
                current, self._events(current)
            )
            if len(persisted_ids) > 3:
                raise RecoveryWorkflowError(
                    "persisted recovery evidence exceeds three Challengers"
                )
            desired_status = (
                current.status
                if current.status
                in {RecoveryCaseStatus.RECOVERED, RecoveryCaseStatus.CLOSED}
                else RecoveryCaseStatus.OBSERVING
            )
            desired = current.model_copy(
                update={
                    "status": desired_status,
                    "challenger_ids": persisted_ids,
                }
            )
            try:
                return self.catalog.save_recovery_case(desired)
            except CatalogConflict:
                continue
        raise RecoveryWorkflowError(
            "recovery Challenger projection remained contended"
        )

    def _shadow_projections(
        self,
        account_id: str,
        *,
        since: datetime | None = None,
        through: datetime | None = None,
        limit: int = 1_000,
    ) -> list[_ProjectionPoint]:
        points = [
            point
            for event in self.catalog.list_shadow_events_by_type(
                account_id=account_id,
                event_type="account_projected",
                since=since,
                through=through,
                limit=limit,
            )
            if (point := _projection_from_event(event)) is not None
        ]
        return sorted(points, key=lambda point: point.sequence_number)

    @staticmethod
    def _dormancy_started_at(
        case: RecoveryCase,
        lifecycle_events: Sequence[LifecycleEvent],
    ) -> datetime:
        transitions = [
            event
            for event in lifecycle_events
            if event.occurred_at >= case.triggered_at
            and event.to_state is LifecycleState.DORMANT
            and event.from_state is not LifecycleState.DORMANT
        ]
        if transitions:
            return max(event.occurred_at for event in transitions)
        if case.lifecycle_state is LifecycleState.DORMANT:
            # A case may be opened directly in dormant state.  Its trigger is
            # then the only persisted start of dormancy.
            return case.triggered_at
        raise RecoveryWorkflowError("dormancy start is absent from lifecycle evidence")

    def bind_shadow_account(
        self,
        recovery_case_id: str,
        challenger_id: str,
        account_id: str,
        *,
        bound_at: datetime,
    ) -> None:
        _require_aware(bound_at, "bound_at")
        case = self._case(recovery_case_id)
        if challenger_id not in case.challenger_ids:
            raise RecoveryWorkflowError("shadow account does not belong to a registered Challenger")
        if bound_at < case.triggered_at:
            raise RecoveryWorkflowError("shadow account binding predates the recovery trigger")
        lifecycle_events = self._events(case)
        bindings = [
            event
            for event in lifecycle_events
            if event.cause == "recovery_challenger_shadow_bound"
            and event.evidence.get("recovery_case_id") == case.recovery_case_id
            and event.evidence.get("challenger_id") == challenger_id
        ]
        if bindings:
            bound_accounts = {str(event.evidence.get("account_id") or "") for event in bindings}
            if bound_accounts == {str(account_id)}:
                if not self.catalog.verify_shadow_chain(account_id):
                    raise RecoveryWorkflowError("Challenger shadow account hash chain is invalid")
                return
            raise RecoveryWorkflowError("a Challenger shadow account binding is immutable")
        account = self.catalog.get_shadow_account(account_id)
        if account is None or not self.catalog.verify_shadow_chain(account_id):
            raise RecoveryWorkflowError("Challenger shadow account is missing or has an invalid chain")
        baselines = [
            point
            for point in self._shadow_projections(
                account_id, through=bound_at, limit=1
            )
            if point.occurred_at <= bound_at
        ]
        if not baselines:
            raise RecoveryWorkflowError(
                "Challenger shadow account requires an account_projected baseline at binding"
            )
        baseline = baselines[-1]
        state = _event_state(case)
        self._append_case_event(
            case,
            LifecycleEvent(
                idempotency_key=_idempotency_key(
                    "shadow",
                    {
                        "recovery_case_id": case.recovery_case_id,
                        "challenger_id": challenger_id,
                        "account_id": account_id,
                    },
                ),
                sleeve_id=case.sleeve_id,
                from_state=state,
                to_state=state,
                cause="recovery_challenger_shadow_bound",
                evidence={
                    "recovery_case_id": case.recovery_case_id,
                    "challenger_id": challenger_id,
                    "account_id": account_id,
                    "baseline_at": baseline.occurred_at,
                    "baseline_sequence": baseline.sequence_number,
                    "baseline_event_hash": baseline.event_hash,
                    "baseline_nav": baseline.nav,
                    "baseline_benchmark_nav": baseline.benchmark_nav,
                },
                occurred_at=self._causal_event_time(case.sleeve_id, bound_at),
            )
        )

    def record_challenger_health(
        self,
        recovery_case_id: str,
        challenger_id: str,
        *,
        observed_at: datetime,
        snapshot_id: str,
        ic_direction_restored: bool,
        risk_alerts: Sequence[str] = (),
    ) -> None:
        _require_aware(observed_at, "observed_at")
        case = self._case(recovery_case_id)
        if challenger_id not in case.challenger_ids:
            raise RecoveryWorkflowError("health evidence is for an unregistered Challenger")
        self._accepted_gold(snapshot_id)
        binding = next(
            (
                event
                for event in self._events(case)
                if event.cause == "recovery_challenger_shadow_bound"
                and event.evidence.get("recovery_case_id") == case.recovery_case_id
                and event.evidence.get("challenger_id") == challenger_id
            ),
            None,
        )
        if binding is None:
            raise RecoveryWorkflowError("health cannot be recorded before shadow binding")
        if observed_at < binding.occurred_at:
            raise RecoveryWorkflowError("health observation predates shadow binding")
        alerts = _normalized_strings(risk_alerts)
        idempotency_key = _idempotency_key(
            "health",
            {
                "recovery_case_id": case.recovery_case_id,
                "challenger_id": challenger_id,
                "session_date": observed_at.astimezone(_SHANGHAI).date(),
            },
        )
        evidence = {
            "recovery_case_id": case.recovery_case_id,
            "challenger_id": challenger_id,
            "snapshot_id": snapshot_id,
            "ic_direction_restored": bool(ic_direction_restored),
            "risk_alerts": list(alerts),
        }
        existing = next(
            (event for event in self._events(case) if event.idempotency_key == idempotency_key),
            None,
        )
        if existing is not None:
            if dict(existing.evidence) != evidence:
                raise RecoveryWorkflowError(
                    "daily Challenger health evidence is immutable once recorded"
                )
            return
        state = _event_state(case)
        self._append_case_event(
            case,
            LifecycleEvent(
                idempotency_key=idempotency_key,
                sleeve_id=case.sleeve_id,
                from_state=state,
                to_state=state,
                cause="recovery_challenger_health_observed",
                evidence={
                    "recovery_case_id": case.recovery_case_id,
                    "challenger_id": challenger_id,
                    "snapshot_id": snapshot_id,
                    "ic_direction_restored": bool(ic_direction_restored),
                    "risk_alerts": list(alerts),
                },
                occurred_at=self._causal_event_time(case.sleeve_id, observed_at),
            )
        )

    def record_challenger_health_from_event_chain(
        self,
        recovery_case_id: str,
        challenger_id: str,
        *,
        observed_at: datetime,
        lifecycle_record: Any,
        monitor_policy: Any = None,
    ):
        """Derive and persist Challenger health without a monitor-input file."""

        case = self._case(recovery_case_id)
        binding = next(
            (
                event
                for event in self._events(case)
                if event.cause == "recovery_challenger_shadow_bound"
                and event.evidence.get("recovery_case_id") == case.recovery_case_id
                and event.evidence.get("challenger_id") == challenger_id
            ),
            None,
        )
        if binding is None:
            raise RecoveryWorkflowError("event-chain health requires a bound shadow account")
        account_id = str(binding.evidence.get("account_id") or "")
        from .monitor import EventChainHealthBuilder, EventChainMonitorPolicy

        policy = monitor_policy or EventChainMonitorPolicy()
        evidence = EventChainHealthBuilder(self.catalog).derive(
            account_id=account_id,
            record=lifecycle_record,
            policy=policy,
            through=observed_at.astimezone(_SHANGHAI).date(),
        )
        self.record_challenger_health(
            recovery_case_id,
            challenger_id,
            observed_at=observed_at,
            snapshot_id=evidence.snapshot_id,
            ic_direction_restored=evidence.observation.ic_direction_ok,
            risk_alerts=evidence.observation.alarm_reasons(),
        )
        state = _event_state(case)
        event_chain_idempotency_key = _idempotency_key(
            "event_chain_health",
            {
                "recovery_case_id": recovery_case_id,
                "challenger_id": challenger_id,
                "evidence_hash": evidence.evidence_hash,
            },
        )
        event_chain_payload = {
            "recovery_case_id": recovery_case_id,
            "challenger_id": challenger_id,
            "account_id": account_id,
            "event_chain_evidence_hash": evidence.evidence_hash,
            "last_event_hash": evidence.last_event_hash,
            "snapshot_id": evidence.snapshot_id,
        }
        existing_chain_event = next(
            (
                event
                for event in self._events(case)
                if event.idempotency_key == event_chain_idempotency_key
            ),
            None,
        )
        if existing_chain_event is not None:
            if dict(existing_chain_event.evidence) != event_chain_payload:
                raise RecoveryWorkflowError(
                    "event-chain health evidence is immutable once recorded"
                )
            return evidence
        self._append_case_event(
            case,
            LifecycleEvent(
                idempotency_key=event_chain_idempotency_key,
                sleeve_id=case.sleeve_id,
                from_state=state,
                to_state=state,
                cause="recovery_challenger_event_chain_health_bound",
                occurred_at=self._causal_event_time(
                    case.sleeve_id, observed_at
                ),
                evidence=event_chain_payload,
            )
        )
        return evidence

    @staticmethod
    def _active_return(
        baseline: _ProjectionPoint,
        points: Sequence[_ProjectionPoint],
        window: int,
        *,
        complete_from_binding: bool,
    ) -> float | None:
        if len(points) < window:
            return None
        endpoint = points[-1]
        if len(points) == window:
            if not complete_from_binding:
                return None
            anchor = baseline
        else:
            # N session returns require N+1 levels.  The predecessor of the
            # first selected session is therefore points[-window-1], not the
            # first point inside the window (which would silently compute 59
            # returns for a requested 60-session window).
            anchor = points[-window - 1]
        return float(
            endpoint.nav / anchor.nav
            - endpoint.benchmark_nav / anchor.benchmark_nav
        )

    @staticmethod
    def _result_from_evidence(evidence: Mapping[str, Any]) -> RecoveryObservationResult:
        observations = []
        for raw in evidence.get("observations", ()):
            observations.append(
                ChallengerObservation(
                    challenger_id=str(raw["challenger_id"]),
                    account_id=str(raw.get("account_id") or ""),
                    observation_started_at=_parse_datetime(
                        raw.get("observation_started_at"), name="observation_started_at"
                    ),
                    baseline_at=_parse_datetime(raw.get("baseline_at"), name="baseline_at"),
                    baseline_event_hash=str(raw.get("baseline_event_hash") or ""),
                    last_session_at=_parse_datetime(
                        raw.get("last_session_at"), name="last_session_at"
                    ),
                    last_session_event_hash=str(raw.get("last_session_event_hash") or ""),
                    chain_verified=bool(raw.get("chain_verified")),
                    session_count=int(raw.get("session_count") or 0),
                    active_return_20=(
                        None
                        if raw.get("active_return_20") is None
                        else float(raw["active_return_20"])
                    ),
                    active_return_60=(
                        None
                        if raw.get("active_return_60") is None
                        else float(raw["active_return_60"])
                    ),
                    health_observed_at=_parse_datetime(
                        raw.get("health_observed_at"), name="health_observed_at"
                    ),
                    ic_direction_restored=bool(raw.get("ic_direction_restored")),
                    risk_alerts=tuple(map(str, raw.get("risk_alerts", ()))),
                    eligible_for_recovery=bool(raw.get("eligible_for_recovery")),
                )
            )
        return RecoveryObservationResult(
            recovery_case_id=str(evidence["recovery_case_id"]),
            as_of=_required_datetime(evidence["as_of"], name="as_of"),
            observations=tuple(observations),
            eligible_challenger_ids=tuple(
                map(str, evidence.get("eligible_challenger_ids", ()))
            ),
            observation_complete=bool(evidence.get("observation_complete")),
        )

    def evaluate_observation(
        self,
        recovery_case_id: str,
        *,
        as_of: datetime,
        minimum_new_sessions: int = 60,
    ) -> RecoveryObservationResult:
        _require_aware(as_of, "as_of")
        if minimum_new_sessions < 60:
            raise ValueError("recovery observation cannot be shorter than 60 sessions")
        case = self._case_any_status(recovery_case_id)
        lifecycle_events = self._events(case)
        completed = self._case_event(
            lifecycle_events, case, "recovery_shadow_observation_completed"
        )
        if completed is not None:
            if (
                completed.from_state is not LifecycleState.DORMANT
                or completed.to_state is not LifecycleState.PROBATION
            ):
                raise RecoveryWorkflowError("persisted recovery transition is invalid")
            result = self._result_from_evidence(completed.evidence)
            if not result.observation_complete:
                raise RecoveryWorkflowError("persisted recovery evidence is incomplete")
            if (
                case.status is not RecoveryCaseStatus.RECOVERED
                or case.lifecycle_state is not LifecycleState.PROBATION
            ):
                self.catalog.save_recovery_case(
                    case.model_copy(
                        update={
                            "status": RecoveryCaseStatus.RECOVERED,
                            "lifecycle_state": LifecycleState.PROBATION,
                        }
                    )
                )
            return result
        if case.status not in _ACTIVE_CASE_STATUSES:
            raise RecoveryWorkflowError(
                f"recovery case {recovery_case_id!r} is already {case.status.value}"
            )
        if case.lifecycle_state is not LifecycleState.DORMANT:
            raise RecoveryWorkflowError("only a dormant Sleeve can recover into probation")
        dormancy_started_at = self._dormancy_started_at(case, lifecycle_events)
        observations: list[ChallengerObservation] = []
        for challenger_id in case.challenger_ids:
            binding = next(
                (
                    event
                    for event in lifecycle_events
                    if event.cause == "recovery_challenger_shadow_bound"
                    and event.evidence.get("recovery_case_id") == case.recovery_case_id
                    and event.evidence.get("challenger_id") == challenger_id
                ),
                None,
            )
            account_id = str(binding.evidence.get("account_id") or "") if binding else ""
            observation_started_at = (
                max(binding.occurred_at, dormancy_started_at) if binding else None
            )
            chain_verified = bool(
                account_id and self.catalog.verify_shadow_chain(account_id)
            )
            baseline: _ProjectionPoint | None = None
            points: list[_ProjectionPoint] = []
            authoritative_session_count = 0
            if account_id and binding and chain_verified:
                try:
                    bound_baseline = _ProjectionPoint(
                        occurred_at=_required_datetime(
                            binding.evidence.get("baseline_at"), name="baseline_at"
                        ),
                        nav=float(binding.evidence["baseline_nav"]),
                        benchmark_nav=float(binding.evidence["baseline_benchmark_nav"]),
                        event_hash=str(binding.evidence["baseline_event_hash"]),
                        sequence_number=int(binding.evidence["baseline_sequence"]),
                    )
                except (KeyError, TypeError, ValueError):
                    bound_baseline = None
                prior = self._shadow_projections(
                    account_id,
                    through=observation_started_at,
                    limit=1,
                )
                baseline = prior[-1] if prior else None
                if (
                    baseline is None
                    and bound_baseline is not None
                    and bound_baseline.occurred_at <= observation_started_at
                ):
                    baseline = bound_baseline
                all_points = self._shadow_projections(
                    account_id=account_id,
                    since=observation_started_at,
                    through=as_of,
                )
                by_session: dict[Any, _ProjectionPoint] = {}
                for point in all_points:
                    if not observation_started_at < point.occurred_at <= as_of:
                        continue
                    session = point.occurred_at.astimezone(_SHANGHAI).date()
                    previous = by_session.get(session)
                    if previous is None or point.sequence_number > previous.sequence_number:
                        by_session[session] = point
                points = sorted(by_session.values(), key=lambda point: point.occurred_at)
                authoritative_session_count = self.catalog.count_shadow_sessions(
                    account_id=account_id,
                    since=observation_started_at.astimezone(_SHANGHAI).date(),
                    through=as_of.astimezone(_SHANGHAI).date(),
                )
                authoritative_session_count = max(
                    authoritative_session_count, len(points)
                )
            health = next(
                (
                    event
                    for event in lifecycle_events
                    if event.cause == "recovery_challenger_health_observed"
                    and event.evidence.get("recovery_case_id") == case.recovery_case_id
                    and event.evidence.get("challenger_id") == challenger_id
                    and binding is not None
                    and observation_started_at is not None
                    and event.occurred_at >= observation_started_at
                    and event.occurred_at <= as_of
                ),
                None,
            )
            ic_restored = bool(health and health.evidence.get("ic_direction_restored"))
            alerts = (
                _normalized_strings(health.evidence.get("risk_alerts", ())) if health else ()
            )
            health_observed_at = health.occurred_at if health else None
            complete_from_binding = bool(
                baseline is not None and authoritative_session_count == len(points)
            )
            active_20 = (
                self._active_return(
                    baseline, points, 20, complete_from_binding=complete_from_binding
                )
                if baseline is not None
                else None
            )
            active_60 = (
                self._active_return(
                    baseline, points, 60, complete_from_binding=complete_from_binding
                )
                if baseline is not None
                else None
            )
            health_is_current = bool(
                health_observed_at
                and points
                and health_observed_at >= points[-1].occurred_at
            )
            eligible = bool(
                as_of >= case.earliest_recovery_review_at
                and chain_verified
                and baseline is not None
                and authoritative_session_count >= minimum_new_sessions
                and active_20 is not None
                and active_60 is not None
                and active_20 > 0
                and active_60 > 0
                and health_is_current
                and ic_restored
                and not alerts
            )
            observations.append(
                ChallengerObservation(
                    challenger_id=challenger_id,
                    account_id=account_id,
                    observation_started_at=observation_started_at,
                    baseline_at=baseline.occurred_at if baseline else None,
                    baseline_event_hash=baseline.event_hash if baseline else "",
                    last_session_at=points[-1].occurred_at if points else None,
                    last_session_event_hash=points[-1].event_hash if points else "",
                    chain_verified=chain_verified,
                    session_count=authoritative_session_count,
                    active_return_20=active_20,
                    active_return_60=active_60,
                    health_observed_at=health_observed_at,
                    ic_direction_restored=ic_restored,
                    risk_alerts=alerts,
                    eligible_for_recovery=eligible,
                )
            )
        eligible_ids = tuple(
            row.challenger_id for row in observations if row.eligible_for_recovery
        )
        result = RecoveryObservationResult(
            recovery_case_id=case.recovery_case_id,
            as_of=as_of,
            observations=tuple(observations),
            eligible_challenger_ids=eligible_ids,
            observation_complete=bool(eligible_ids),
        )
        if eligible_ids:
            self._append_case_event(
                case,
                LifecycleEvent(
                    idempotency_key=_idempotency_key(
                        "observation", {"recovery_case_id": case.recovery_case_id}
                    ),
                    sleeve_id=case.sleeve_id,
                    from_state=LifecycleState.DORMANT,
                    to_state=LifecycleState.PROBATION,
                    cause="recovery_shadow_observation_completed",
                    evidence=result.to_dict(),
                    occurred_at=self._causal_event_time(case.sleeve_id, as_of),
                )
            )
            self._advance_case_status(
                case.recovery_case_id,
                RecoveryCaseStatus.RECOVERED,
                lifecycle_state=LifecycleState.PROBATION,
            )
        return result


__all__ = [
    "ChallengerObservation",
    "RecoveryCoordinator",
    "RecoveryObservationResult",
    "RecoveryWorkflowError",
]
