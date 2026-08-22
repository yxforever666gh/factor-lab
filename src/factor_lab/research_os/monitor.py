"""Persisted weekly sleeve-health tick and recovery SLA management."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from math import isfinite, sqrt
from statistics import fmean, pstdev
from typing import Any, Iterable, Mapping, Sequence

from .catalog import LifecycleEvent, ResearchCatalog
from .contracts import (
    LifecycleState,
    RecoveryCase,
    RecoveryCaseStatus,
)
from .lifecycle import (
    LifecycleDecision,
    SleeveHealthObservation,
    SleeveLifecycleRecord,
    SleeveState,
    advance_lifecycle,
)
from .fingerprint import content_fingerprint


@dataclass(frozen=True)
class MonitorTickResult:
    sleeve_id: str
    as_of_date: date
    state: str
    recommended_action: str
    alarm_reasons: tuple[str, ...]
    transition: dict[str, Any] | None
    recovery_case_id: str | None
    record: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class MonitorEvidenceError(RuntimeError):
    """Raised when the event ledger cannot deterministically support health."""


@dataclass(frozen=True)
class EventChainMonitorPolicy:
    expected_ic_direction: int = 1
    baseline_cost_rate: float = 0.0005
    drawdown_95_limit: float = 0.25
    minimum_sessions: int = 60

    def __post_init__(self) -> None:
        if self.expected_ic_direction not in {-1, 1}:
            raise ValueError("expected_ic_direction must be -1 or 1")
        if not isfinite(self.baseline_cost_rate) or self.baseline_cost_rate <= 0:
            raise ValueError("baseline_cost_rate must be positive")
        if not isfinite(self.drawdown_95_limit) or self.drawdown_95_limit <= 0:
            raise ValueError("drawdown_95_limit must be positive")
        if self.minimum_sessions < 60:
            raise ValueError("event-chain monitoring requires at least 60 sessions")


@dataclass(frozen=True)
class DerivedHealthEvidence:
    account_id: str
    observation: SleeveHealthObservation
    snapshot_id: str
    projection_event_hashes: tuple[str, ...]
    snapshot_content_hashes: tuple[str, ...]
    last_event_hash: str
    evidence_hash: str


@dataclass(frozen=True)
class _DailyLevel:
    session: date
    nav: float
    benchmark_nav: float
    fees: float
    rank_ic: float | None
    risk_alerts: tuple[str, ...]
    projection_event_hash: str
    mark_snapshot_id: str


def _information_ratio(returns: Sequence[float]) -> float:
    if not returns:
        return 0.0
    deviation = pstdev(returns)
    if deviation <= 1e-15:
        return 0.0
    return float(fmean(returns) / deviation * sqrt(252.0))


def _active_return(
    levels: Sequence[_DailyLevel],
    window: int,
    *,
    initial_nav: float,
    initial_benchmark_nav: float,
) -> float:
    if len(levels) < window:
        return 0.0
    end = levels[-1]
    if len(levels) == window:
        return float(
            end.nav / initial_nav - end.benchmark_nav / initial_benchmark_nav
        )
    start = levels[-window - 1]
    return float(
        end.nav / start.nav - end.benchmark_nav / start.benchmark_nav
    )


class EventChainHealthBuilder:
    """Recompute health only from the verified immutable shadow event chain."""

    def __init__(self, catalog: ResearchCatalog) -> None:
        self.catalog = catalog

    @staticmethod
    def _metric_value(raw: Any) -> float | None:
        if isinstance(raw, Mapping):
            raw = raw.get("value")
        if raw is None:
            return None
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return None
        return value if isfinite(value) else None

    def derive(
        self,
        *,
        account_id: str,
        record: SleeveLifecycleRecord,
        policy: EventChainMonitorPolicy = EventChainMonitorPolicy(),
        through: date | None = None,
    ) -> DerivedHealthEvidence:
        account = self.catalog.get_shadow_account(account_id)
        if account is None or not self.catalog.verify_shadow_chain(account_id):
            raise MonitorEvidenceError("shadow account is missing or its hash chain is invalid")
        through_time = (
            None
            if through is None
            else datetime.combine(through, time.max, tzinfo=timezone.utc)
        )
        projections = self.catalog.list_shadow_events_by_type(
            account_id=account_id,
            event_type="account_projected",
            since=None,
            through=through_time,
            limit=1_000,
        )
        session_events = self.catalog.list_shadow_events_by_type(
            account_id=account_id,
            event_type="session_evidence",
            since=None,
            through=through_time,
            limit=1_000,
        )
        incidents = self.catalog.list_shadow_events_by_type(
            account_id=account_id,
            event_type="data_incident",
            since=None,
            through=through_time,
            limit=1_000,
        )
        revalidations = self.catalog.list_shadow_events_by_type(
            account_id=account_id,
            event_type="data_revalidated",
            since=None,
            through=through_time,
            limit=1_000,
        )
        events = sorted(
            (*projections, *session_events, *incidents, *revalidations),
            key=lambda event: event.sequence_number,
        )
        step_evidence: dict[str, Any] = {}
        for event in events:
            metadata = event.payload.get("research_os_shadow_step")
            step_id = str(metadata.get("step_id") or "") if isinstance(metadata, Mapping) else ""
            if event.event_type == "session_evidence" and step_id:
                step_evidence[step_id] = event

        levels: list[_DailyLevel] = []
        snapshot_hashes: set[str] = set()
        for event in events:
            if event.event_type != "account_projected":
                continue
            session = event.occurred_at.date()
            if through is not None and session > through:
                continue
            metadata = event.payload.get("research_os_shadow_step")
            step_id = str(metadata.get("step_id") or "") if isinstance(metadata, Mapping) else ""
            evidence_event = step_evidence.get(step_id)
            if evidence_event is None:
                raise MonitorEvidenceError(
                    "account projection lacks same-step session_evidence lineage"
                )
            bindings = evidence_event.payload.get("snapshot_bindings")
            if not isinstance(bindings, Mapping) or not str(bindings.get("mark_snapshot_id") or ""):
                raise MonitorEvidenceError("session evidence lacks mark_snapshot_id")
            mark_snapshot_id = str(bindings["mark_snapshot_id"])
            snapshot = self.catalog.get_snapshot(mark_snapshot_id)
            if snapshot is None:
                raise MonitorEvidenceError("session mark snapshot is not registered")
            timing = evidence_event.payload.get("timing")
            if not isinstance(timing, Mapping) or not timing.get("mark_available_at"):
                raise MonitorEvidenceError("session evidence lacks mark availability cutoff")
            mark_available = datetime.fromisoformat(
                str(timing["mark_available_at"]).replace("Z", "+00:00")
            )
            if mark_available.tzinfo is None or mark_available.utcoffset() is None:
                raise MonitorEvidenceError("mark availability cutoff must include a timezone")
            if snapshot.reference.as_of > mark_available:
                raise MonitorEvidenceError("mark snapshot was unavailable at the event cutoff")
            snapshot_hashes.add(snapshot.reference.content_hash)
            state = event.payload.get("account_state")
            if not isinstance(state, Mapping):
                raise MonitorEvidenceError("account projection lacks account_state")
            try:
                nav = float(state["nav"])
                benchmark_nav = float(state["benchmark_nav"])
            except (KeyError, TypeError, ValueError) as exc:
                raise MonitorEvidenceError("account projection has invalid NAV") from exc
            if not all(isfinite(value) and value > 0 for value in (nav, benchmark_nav)):
                raise MonitorEvidenceError("account projection NAV must be finite and positive")
            metrics = evidence_event.payload.get("metrics")
            metrics = metrics if isinstance(metrics, Mapping) else {}
            rank_ic = self._metric_value(metrics.get("rank_ic", metrics.get("ic")))
            alerts = tuple(
                sorted(
                    {
                        str(value).strip()
                        for value in metrics.get("risk_alerts", ())
                        if str(value).strip()
                    }
                )
            )
            fees = self._metric_value(evidence_event.payload.get("fees")) or 0.0
            levels.append(
                _DailyLevel(
                    session=session,
                    nav=nav,
                    benchmark_nav=benchmark_nav,
                    fees=fees,
                    rank_ic=rank_ic,
                    risk_alerts=alerts,
                    projection_event_hash=event.event_hash,
                    mark_snapshot_id=mark_snapshot_id,
                )
            )
        by_session: dict[date, _DailyLevel] = {level.session: level for level in levels}
        levels = [by_session[key] for key in sorted(by_session)]
        if len(levels) < policy.minimum_sessions:
            raise MonitorEvidenceError(
                f"event chain has {len(levels)} sessions; {policy.minimum_sessions} required"
            )

        daily_active = [
            current.nav / prior.nav - current.benchmark_nav / prior.benchmark_nav
            for prior, current in zip(levels, levels[1:])
        ]
        ic_values = [value for level in levels[-130:] if (value := level.rank_ic) is not None]
        if not ic_values:
            raise MonitorEvidenceError("event chain has no snapshot-bound IC observations")
        active_wealth = [
            level.nav / levels[0].nav / (level.benchmark_nav / levels[0].benchmark_nav)
            for level in levels
        ]
        peak = active_wealth[0]
        max_drawdown = 0.0
        for value in active_wealth:
            peak = max(peak, value)
            max_drawdown = min(max_drawdown, value / peak - 1.0)
        cost_denominator = sum(level.nav for level in levels[-130:])
        observed_cost_rate = (
            sum(level.fees for level in levels[-130:]) / cost_denominator
            if cost_denominator > 0
            else 0.0
        )
        latest_incident = max((event.occurred_at for event in incidents), default=None)
        latest_revalidation = max(
            (event.occurred_at for event in revalidations), default=None
        )
        data_ok = latest_incident is None or (
            latest_revalidation is not None and latest_revalidation > latest_incident
        )
        new_sessions = (
            sum(level.session > record.dormant_since for level in levels)
            if record.dormant_since is not None
            else 0
        )
        observation = SleeveHealthObservation(
            as_of_date=levels[-1].session,
            active_ir_13w=_information_ratio(daily_active[-65:]),
            active_ir_26w=_information_ratio(daily_active[-130:]),
            ic_26w=float(fmean(ic_values)),
            expected_ic_direction=policy.expected_ic_direction,
            cost_ratio_to_baseline=observed_cost_rate / policy.baseline_cost_rate,
            active_drawdown=max_drawdown,
            drawdown_95_limit=policy.drawdown_95_limit,
            active_return_20d=_active_return(
                levels,
                20,
                initial_nav=account.initial_capital,
                initial_benchmark_nav=account.initial_capital,
            ),
            active_return_60d=_active_return(
                levels,
                60,
                initial_nav=account.initial_capital,
                initial_benchmark_nav=account.initial_capital,
            ),
            new_sessions_since_dormant=new_sessions,
            data_quality_ok=data_ok,
            data_revalidation_passed=bool(
                latest_incident is not None
                and latest_revalidation is not None
                and latest_revalidation > latest_incident
            ),
            critical_risk_alert=bool(levels[-1].risk_alerts),
        )
        evidence_last_hash = max(events, key=lambda event: event.sequence_number).event_hash
        payload = {
            "account_id": account_id,
            "observation": asdict(observation),
            "projection_event_hashes": tuple(level.projection_event_hash for level in levels),
            "snapshot_content_hashes": tuple(sorted(snapshot_hashes)),
            "last_event_hash": evidence_last_hash,
        }
        evidence_hash = content_fingerprint(
            payload,
            domain="factor-lab/research-os/v1/event-chain-health",
        )
        return DerivedHealthEvidence(
            account_id=account_id,
            observation=observation,
            snapshot_id=levels[-1].mark_snapshot_id,
            projection_event_hashes=tuple(level.projection_event_hash for level in levels),
            snapshot_content_hashes=tuple(sorted(snapshot_hashes)),
            last_event_hash=evidence_last_hash,
            evidence_hash=evidence_hash,
        )


def _session_due(
    as_of: date, sessions: Iterable[date], offset: int
) -> datetime:
    future = sorted({item for item in sessions if item > as_of})
    if len(future) < offset:
        raise ValueError(
            f"trading calendar needs at least {offset} sessions after {as_of}"
        )
    # The exact exchange close timestamp is not needed for the SLA ordering;
    # UTC-aware dates keep persistence portable across Windows/Linux.
    return datetime.combine(future[offset - 1], time(15, 0), tzinfo=timezone.utc)


def _project_weekday_due(as_of: date, offset: int) -> datetime:
    """Fail-closed calendar fallback used only for projected deadline labels.

    Actual SLA completion/overdue decisions are made from persisted shadow
    sessions by the application service.  This projection never grants a gate.
    """

    current = as_of
    remaining = offset
    while remaining:
        current += timedelta(days=1)
        if current.weekday() < 5:
            remaining -= 1
    return datetime.combine(current, time(15, 0), tzinfo=timezone.utc)


def _at_close(value: date) -> datetime:
    return datetime.combine(value, time(15, 0), tzinfo=timezone.utc)


class LifecycleMonitor:
    def __init__(self, catalog: ResearchCatalog) -> None:
        self.catalog = catalog

    def tick(
        self,
        record: SleeveLifecycleRecord,
        observation: SleeveHealthObservation,
        *,
        snapshot_id: str,
        trading_sessions: Iterable[date] = (),
        active_recovery_case: RecoveryCase | None = None,
        shadow_account_id: str | None = None,
        allow_projected_deadlines: bool = False,
        derived_evidence: DerivedHealthEvidence | None = None,
    ) -> MonitorTickResult:
        if not snapshot_id.strip():
            raise ValueError("monitor tick requires the snapshot used for every metric")
        decision = advance_lifecycle(record, observation)
        recovery = active_recovery_case
        opens_recovery = (
            decision.transition is not None
            and decision.record.state in {SleeveState.REDUCED, SleeveState.FROZEN_DATA}
            and recovery is None
        )
        if opens_recovery:
            sessions = tuple(trading_sessions)
            if sessions:
                earliest_review = _session_due(observation.as_of_date, sessions, 60)
                diagnosis_due = _session_due(observation.as_of_date, sessions, 20)
                drift_due = _session_due(observation.as_of_date, sessions, 5)
            elif allow_projected_deadlines:
                earliest_review = _project_weekday_due(observation.as_of_date, 60)
                diagnosis_due = _project_weekday_due(observation.as_of_date, 20)
                drift_due = _project_weekday_due(observation.as_of_date, 5)
            else:
                # Preserve the strict domain API: callers must either provide a
                # trusted calendar or explicitly opt into label-only projections.
                earliest_review = _session_due(observation.as_of_date, sessions, 60)
                diagnosis_due = _session_due(observation.as_of_date, sessions, 20)
                drift_due = _session_due(observation.as_of_date, sessions, 5)
            recovery = RecoveryCase(
                recovery_case_id=(
                    f"recovery_{record.sleeve_id}_{observation.as_of_date.isoformat()}"
                ),
                sleeve_id=record.sleeve_id,
                status=RecoveryCaseStatus.OPEN,
                lifecycle_state=LifecycleState(decision.record.state.value),
                triggered_at=_at_close(observation.as_of_date),
                drift_event_due_at=drift_due,
                diagnosis_due_at=diagnosis_due,
                earliest_recovery_review_at=earliest_review,
                trigger_evidence={
                    "snapshot_id": snapshot_id,
                    "alarms": list(decision.alarm_reasons),
                    "observation": asdict(observation),
                    "shadow_account_id": shadow_account_id,
                    "deadline_projection": "weekday_only_non_authoritative",
                },
                data_integrity_failure=(
                    decision.record.state is SleeveState.FROZEN_DATA
                ),
            )
            self.catalog.save_recovery_case(recovery)
        elif recovery is not None and decision.transition is not None:
            if decision.record.state is SleeveState.PROBATION:
                recovery = recovery.model_copy(
                    update={
                        "status": RecoveryCaseStatus.OBSERVING,
                        "lifecycle_state": LifecycleState.PROBATION,
                    }
                )
            elif decision.record.state is SleeveState.ACTIVE:
                recovery = recovery.model_copy(
                    update={
                        "status": RecoveryCaseStatus.RECOVERED,
                        "lifecycle_state": LifecycleState.ACTIVE,
                    }
                )
            else:
                recovery = recovery.model_copy(
                    update={
                        "lifecycle_state": LifecycleState(decision.record.state.value)
                    }
                )
            self.catalog.save_recovery_case(recovery)

        event = LifecycleEvent(
            idempotency_key=(
                f"monitor:{record.sleeve_id}:{observation.as_of_date.isoformat()}"
            ),
            sleeve_id=record.sleeve_id,
            from_state=LifecycleState(record.state.value),
            to_state=LifecycleState(decision.record.state.value),
            cause="weekly_health_tick",
            occurred_at=_at_close(observation.as_of_date),
            evidence={
                "snapshot_id": snapshot_id,
                "record": asdict(decision.record),
                "observation": asdict(observation),
                "alarms": list(decision.alarm_reasons),
                "recommended_action": decision.recommended_action,
                "event_chain_evidence": (
                    None
                    if derived_evidence is None
                    else {
                        "account_id": derived_evidence.account_id,
                        "evidence_hash": derived_evidence.evidence_hash,
                        "last_event_hash": derived_evidence.last_event_hash,
                        "projection_event_hashes": list(
                            derived_evidence.projection_event_hashes
                        ),
                        "snapshot_content_hashes": list(
                            derived_evidence.snapshot_content_hashes
                        ),
                    }
                ),
                "recovery_case_id": (
                    recovery.recovery_case_id if recovery is not None else None
                ),
            },
        )
        self.catalog.append_lifecycle_event(event)
        return MonitorTickResult(
            sleeve_id=record.sleeve_id,
            as_of_date=observation.as_of_date,
            state=decision.record.state.value,
            recommended_action=decision.recommended_action,
            alarm_reasons=decision.alarm_reasons,
            transition=(asdict(decision.transition) if decision.transition else None),
            recovery_case_id=(
                recovery.recovery_case_id if recovery is not None else None
            ),
            record=asdict(decision.record),
        )

    def tick_from_event_chain(
        self,
        record: SleeveLifecycleRecord,
        *,
        shadow_account_id: str,
        policy: EventChainMonitorPolicy = EventChainMonitorPolicy(),
        trading_sessions: Iterable[date] = (),
        active_recovery_case: RecoveryCase | None = None,
    ) -> MonitorTickResult:
        evidence = EventChainHealthBuilder(self.catalog).derive(
            account_id=shadow_account_id,
            record=record,
            policy=policy,
        )
        return self.tick(
            record,
            evidence.observation,
            snapshot_id=evidence.snapshot_id,
            trading_sessions=trading_sessions,
            active_recovery_case=active_recovery_case,
            shadow_account_id=shadow_account_id,
            derived_evidence=evidence,
        )


__all__ = [
    "DerivedHealthEvidence",
    "EventChainHealthBuilder",
    "EventChainMonitorPolicy",
    "LifecycleMonitor",
    "MonitorEvidenceError",
    "MonitorTickResult",
]
