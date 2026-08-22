"""Formal PostgreSQL authority for forward shadow evidence.

The event ledger remains the accounting source of truth.  This module turns a
verified daily projection into the immutable, queryable authority rows added
by migration ``0007_production_ledger``.  A row is never accepted merely
because a caller reports a NAV: the service replays the account hash chain,
locates the matching ``session_evidence``/``mark_to_market`` events, verifies
the three snapshot roles, and derives all account values from those events.

Formal recovery comparisons use role bindings and an activated evidence epoch.
Champion and Challenger must have exactly aligned session dates in the same
epoch/window.  This deliberately excludes legacy JSON and the older event-count
shortcut from production evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
import hashlib
from math import isclose, isfinite
import re
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from .fingerprint import canonical_json, content_fingerprint

try:  # Keep lightweight research workers importable without infra extras.
    from sqlalchemy import create_engine, func, select, text
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.orm import Session, sessionmaker

    from . import orm
except ImportError:  # pragma: no cover - minimal workers do not persist evidence.
    Engine = Any  # type: ignore[misc,assignment]
    Session = Any  # type: ignore[misc,assignment]
    _SQLALCHEMY_AVAILABLE = False
else:
    _SQLALCHEMY_AVAILABLE = True


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}$")
_ZERO_EVENT_HASH = "0" * 64
_EVENT_HASH_DOMAIN = "factor-lab/research-os/v1/shadow-event"
_BINDING_HASH_DOMAIN = "factor-lab/research-os/v1/shadow-role-binding"
_SESSION_HASH_DOMAIN = "factor-lab/research-os/v1/shadow-session"
_WINDOW_HASH_DOMAIN = "factor-lab/research-os/v1/aligned-forward-shadow-window"
_FLEET_CLOSURE_HASH_DOMAIN = "factor-lab/research-os/v1/shadow-fleet-closure"
_FLEET_CLOSURE_SCHEMA = "research-os/shadow-fleet-closure/v1"
_STEP_KEY = "research_os_shadow_step"
_ACCOUNT_TOLERANCE = 0.01
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_OPEN_TIME = time(9, 30)
_CLOSE_TIME = time(15, 0)
_EXECUTION_SNAPSHOT_DOMAIN = (
    "factor-lab/research-os/v1/execution-role-snapshot"
)
_BLOCKING_TRUST_LABELS = {
    "st_history_unverified",
    "legacy_untrusted_data",
    "legacy_execution_regression_only",
    "disputed",
    "quarantined",
    "data_quarantined",
    "non_forward",
    "synthetic_or_test_source",
}
_BLOCKING_INCIDENT_STAGES = frozenset(
    {"source", "silver", "data_quality", "gold"}
)


class ShadowAuthorityError(RuntimeError):
    """Base class for fail-closed formal shadow evidence errors."""


class ShadowRoleConflict(ShadowAuthorityError):
    """A role binding is missing, ambiguous, or conflicts with durable state."""


class ShadowSessionRejected(ShadowAuthorityError):
    """A daily projection cannot be admitted as formal shadow evidence."""


class InsufficientForwardEvidence(ShadowAuthorityError):
    """Fewer than the required common forward sessions are available."""


class MisalignedForwardEvidence(ShadowAuthorityError):
    """Champion and Challenger forward dates or epoch bindings differ."""


class IncompleteFleetEvidence(MisalignedForwardEvidence):
    """A forward session is not part of an immutable closed daily fleet."""


class ShadowRole(str, Enum):
    CHAMPION = "champion"
    CHALLENGER = "challenger"
    SLEEVE = "sleeve"


class ShadowEvidenceClass(str, Enum):
    ENGINEERING = "engineering"
    FORWARD = "forward"


def _aware(value: datetime, *, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    return value.astimezone(timezone.utc)


def _db_aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _identifier(value: str, *, name: str) -> str:
    normalized = str(value or "").strip()
    if not _IDENTIFIER.fullmatch(normalized):
        raise ValueError(f"{name} contains unsupported characters or length")
    return normalized


def _hash(value: str, *, name: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _SHA256.fullmatch(normalized):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return normalized


def _finite_nonnegative(value: Any, *, name: str) -> float:
    number = float(value)
    if not isfinite(number) or number < 0:
        raise ShadowSessionRejected(f"{name} must be finite and non-negative")
    return number


def _event_hash(model: Any) -> str:
    return content_fingerprint(
        {
            "account_id": model.account_id,
            "sequence_number": int(model.sequence_number),
            "event_type": model.event_type,
            "occurred_at": _db_aware(model.occurred_at),
            "payload": dict(model.payload_json),
            "previous_event_hash": model.previous_event_hash,
        },
        domain=_EVENT_HASH_DOMAIN,
    )


@dataclass(frozen=True)
class ShadowRoleBinding:
    binding_id: str
    binding_hash: str
    role: ShadowRole
    role_key: str
    account_id: str
    sleeve_id: str | None
    experiment_id: str | None
    epoch_id: str | None
    active: bool
    bound_at: datetime
    unbound_at: datetime | None
    metadata: Mapping[str, Any]


@dataclass(frozen=True)
class ShadowSessionProjection:
    account_id: str
    trade_date: date
    role_binding_id: str
    epoch_id: str | None
    evidence_window_hash: str | None
    evidence_class: ShadowEvidenceClass
    decision_snapshot_id: str | None
    execution_snapshot_id: str
    mark_snapshot_id: str
    rebalanced: bool
    cash: float
    positions_value: float
    nav: float
    benchmark_nav: float
    position_count: int
    account_event_hash: str
    account_event_sequence: int
    session_hash: str
    created_at: datetime


@dataclass(frozen=True)
class ShadowFleetClosure:
    closure_id: str
    trade_date: date
    evidence_class: ShadowEvidenceClass
    epoch_id: str | None
    evidence_window_hash: str | None
    members: tuple[Mapping[str, Any], ...]
    closure_hash: str
    closed_at: datetime

    @property
    def member_count(self) -> int:
        return len(self.members)


@dataclass(frozen=True)
class AlignedForwardEvidence:
    epoch_id: str
    epoch_hash: str
    evidence_window_hash: str
    first_forward_session: date
    champion_binding: ShadowRoleBinding
    challenger_binding: ShadowRoleBinding
    sessions: tuple[date, ...]
    champion_returns: tuple[float, ...]
    challenger_returns: tuple[float, ...]
    champion_session_hashes: tuple[str, ...]
    challenger_session_hashes: tuple[str, ...]
    evidence_hash: str
    data_quality_ok: bool

    @property
    def observed_sessions(self) -> int:
        return len(self.sessions)

    @property
    def shadow_excess(self) -> float:
        challenger = 1.0
        champion = 1.0
        for value in self.challenger_returns:
            challenger *= 1.0 + float(value)
        for value in self.champion_returns:
            champion *= 1.0 + float(value)
        return float(challenger - champion)

    @property
    def challenger_outperformed_static(self) -> bool:
        return self.shadow_excess > 0.0

    @property
    def fallback(self) -> str:
        return "challenger" if self.challenger_outperformed_static else "static_champion"

    def champion_series(self) -> pd.Series:
        return pd.Series(
            self.champion_returns,
            index=pd.DatetimeIndex(self.sessions),
            dtype=float,
            name="champion",
        )

    def challenger_series(self) -> pd.Series:
        return pd.Series(
            self.challenger_returns,
            index=pd.DatetimeIndex(self.sessions),
            dtype=float,
            name="challenger",
        )

    def authority_metadata(self) -> dict[str, Any]:
        return {
            "epoch_id": self.epoch_id,
            "epoch_hash": self.epoch_hash,
            "evidence_window_hash": self.evidence_window_hash,
            "first_forward_session": self.first_forward_session.isoformat(),
            "first_session": self.sessions[0].isoformat(),
            "last_session": self.sessions[-1].isoformat(),
            "projection_count": self.observed_sessions,
            "evidence_hash": self.evidence_hash,
            "shadow_excess": self.shadow_excess,
            "fallback": self.fallback,
            "data_quality_ok": self.data_quality_ok,
        }


class ShadowEvidenceAuthority:
    """SQLAlchemy service for role bindings and immutable forward projections."""

    def __init__(
        self,
        database: str | Engine,
        *,
        enforce_realtime: bool | None = None,
        require_fleet_closure: bool | None = None,
        database_now_for_test: Callable[[], datetime] | None = None,
    ) -> None:
        if not _SQLALCHEMY_AVAILABLE:
            raise RuntimeError("SQLAlchemy is required for formal shadow evidence")
        self._owns_engine = isinstance(database, str)
        self.engine = (
            create_engine(database, pool_pre_ping=True)
            if self._owns_engine
            else database
        )
        self._sessions = sessionmaker(
            self.engine, expire_on_commit=False, autoflush=False
        )
        is_postgresql = self.engine.dialect.name == "postgresql"
        self._enforce_realtime = (
            is_postgresql if enforce_realtime is None else bool(enforce_realtime)
        )
        if is_postgresql and require_fleet_closure is False:
            raise ValueError(
                "production shadow authority cannot disable daily fleet closure"
            )
        self._require_fleet_closure = (
            is_postgresql
            if require_fleet_closure is None
            else bool(require_fleet_closure)
        )
        if is_postgresql and database_now_for_test is not None:
            raise ValueError(
                "production shadow authority cannot replace the PostgreSQL clock"
            )
        self._database_now_for_test = database_now_for_test

    def close(self) -> None:
        if self._owns_engine:
            self.engine.dispose()

    def __enter__(self) -> "ShadowEvidenceAuthority":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @staticmethod
    def _binding(model: Any) -> ShadowRoleBinding:
        payload = {
            "role": str(model.role),
            "role_key": str(model.role_key),
            "account_id": str(model.account_id),
            "sleeve_id": model.sleeve_id,
            "experiment_id": model.experiment_id,
            "epoch_id": model.epoch_id,
            "metadata": dict(model.metadata_json or {}),
            "bound_at": _db_aware(model.bound_at),
        }
        expected_hash = content_fingerprint(payload, domain=_BINDING_HASH_DOMAIN)
        if (
            str(model.binding_hash) != expected_hash
            or str(model.binding_id) != f"shadow_binding_{expected_hash[:64]}"
            or (bool(model.active) and model.unbound_at is not None)
            or (not bool(model.active) and model.unbound_at is None)
        ):
            raise ShadowRoleConflict("shadow role binding fingerprint is corrupt")
        return ShadowRoleBinding(
            binding_id=model.binding_id,
            binding_hash=model.binding_hash,
            role=ShadowRole(model.role),
            role_key=model.role_key,
            account_id=model.account_id,
            sleeve_id=model.sleeve_id,
            experiment_id=model.experiment_id,
            epoch_id=model.epoch_id,
            active=bool(model.active),
            bound_at=_db_aware(model.bound_at),
            unbound_at=(
                None if model.unbound_at is None else _db_aware(model.unbound_at)
            ),
            metadata=dict(model.metadata_json),
        )

    @staticmethod
    def _projection(model: Any) -> ShadowSessionProjection:
        return ShadowSessionProjection(
            account_id=model.account_id,
            trade_date=date.fromisoformat(model.trade_date),
            role_binding_id=str(model.role_binding_id),
            epoch_id=model.epoch_id,
            evidence_window_hash=model.evidence_window_hash,
            evidence_class=ShadowEvidenceClass(model.evidence_class),
            decision_snapshot_id=model.decision_snapshot_id,
            execution_snapshot_id=model.execution_snapshot_id,
            mark_snapshot_id=model.mark_snapshot_id,
            rebalanced=bool(model.rebalanced),
            cash=float(model.cash),
            positions_value=float(model.positions_value),
            nav=float(model.nav),
            benchmark_nav=float(model.benchmark_nav),
            position_count=int(model.position_count),
            account_event_hash=model.account_event_hash,
            account_event_sequence=int(model.account_event_sequence),
            session_hash=model.session_hash,
            created_at=_db_aware(model.created_at),
        )

    @staticmethod
    def _activated_epoch(session: Session) -> Any | None:
        model = session.scalar(
            select(orm.EvidenceEpochModel)
            .join(
                orm.EvidenceEpochPointerModel,
                orm.EvidenceEpochPointerModel.epoch_id
                == orm.EvidenceEpochModel.epoch_id,
            )
            .where(
                orm.EvidenceEpochPointerModel.pointer_key == "research_os",
                orm.EvidenceEpochModel.closed_at.is_(None),
            )
        )
        if model is None:
            return None
        activation = (
            model.first_forward_session,
            model.evidence_window_hash,
            model.activated_at,
        )
        if all(value is None for value in activation):
            return None
        if any(value is None for value in activation):
            raise ShadowAuthorityError("evidence epoch activation is incomplete")
        _hash(model.epoch_hash, name="epoch_hash")
        _hash(model.evidence_window_hash, name="evidence_window_hash")
        return model

    def _database_now(self, session: Session) -> datetime:
        if self._database_now_for_test is not None:
            return _aware(self._database_now_for_test(), name="database_now_for_test")
        return _db_aware(session.execute(select(func.now())).scalar_one())

    @staticmethod
    def _gap_incident_dispositions(
        session: Session, missing: Sequence[date]
    ) -> Mapping[date, str]:
        """Classify exact-date data blockers without authorizing a skipped ledger day.

        An arbitrary incident elsewhere in the pipeline must never make an
        account gap look continuous.  A still-open blocker keeps the account
        frozen.  A resolved blocker proves only why the gap occurred; the
        account must still replay the missing session (including corporate
        actions) or start a new evidence segment.
        """

        if not missing:
            return {}
        requested = tuple(value.isoformat() for value in missing)
        rows = list(
            session.scalars(
                select(orm.DataIncidentModel).where(
                    orm.DataIncidentModel.partition_key.in_(requested),
                    orm.DataIncidentModel.stage.in_(
                        tuple(sorted(_BLOCKING_INCIDENT_STAGES))
                    ),
                )
            )
        )
        by_date: dict[str, list[Any]] = {value: [] for value in requested}
        for row in rows:
            by_date.setdefault(str(row.partition_key), []).append(row)
        dispositions: dict[date, str] = {}
        for missing_date in missing:
            candidates = by_date.get(missing_date.isoformat(), [])
            if not candidates:
                dispositions[missing_date] = "unexplained"
                continue
            if any(str(row.status) == "open" for row in candidates):
                dispositions[missing_date] = "open_blocker"
                continue
            resolved_valid = False
            invalid = False
            for row in candidates:
                if (
                    str(row.status) != "resolved"
                    or row.resolved_at is None
                    or not _SHA256.fullmatch(str(row.resolution_hash or ""))
                ):
                    invalid = True
                    continue
                payload = dict(row.payload_json or {})
                resolution = payload.get("resolution")
                if not isinstance(resolution, Mapping):
                    invalid = True
                    continue
                snapshot_id = str(resolution.get("snapshot_id") or "")
                snapshot_hash = str(
                    resolution.get("snapshot_content_hash") or ""
                )
                snapshot = (
                    None
                    if not snapshot_id
                    else session.get(orm.DataSnapshotModel, snapshot_id)
                )
                if not (
                    snapshot is not None
                    and snapshot.tier == "gold"
                    and snapshot.quality_status == "accepted"
                    and snapshot.content_hash == snapshot_hash
                    and _SHA256.fullmatch(snapshot_hash)
                ):
                    invalid = True
                    continue
                resolved_valid = True
            dispositions[missing_date] = (
                "resolved_requires_replay"
                if resolved_valid and not invalid
                else "invalid_blocker"
            )
        return dispositions

    @staticmethod
    def _calendar_sessions(session: Session, epoch: Any) -> tuple[date, ...]:
        calendar_id = str(epoch.calendar_snapshot_id or "")
        calendar_snapshot = session.get(orm.DataSnapshotModel, calendar_id)
        if calendar_snapshot is None:
            raise ShadowSessionRejected("active epoch calendar snapshot is absent")
        reference = dict(calendar_snapshot.ref_json or {})
        manifest = reference.get("manifest")
        calendar = manifest.get("trading_calendar") if isinstance(manifest, Mapping) else None
        if not isinstance(calendar, Mapping):
            raise ShadowSessionRejected("active epoch calendar manifest is invalid")
        try:
            sessions = tuple(
                date.fromisoformat(str(value)) for value in calendar["sessions"]
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ShadowSessionRejected("active epoch calendar sessions are invalid") from exc
        calculated = hashlib.sha256(
            "\n".join(value.isoformat() for value in sessions).encode("ascii")
        ).hexdigest()
        if not (
            calendar_snapshot.tier == "gold"
            and calendar_snapshot.quality_status == "accepted"
            and calendar_snapshot.content_hash == epoch.calendar_snapshot_hash
            and str(reference.get("snapshot_id") or "") == calendar_id
            and str(reference.get("content_hash") or "")
            == str(epoch.calendar_snapshot_hash)
            and str(calendar.get("quality_status") or "").lower() == "accepted"
            and calculated == str(calendar.get("content_hash") or "")
            == str(epoch.calendar_content_hash)
            and sessions
            and sessions == tuple(sorted(set(sessions)))
        ):
            raise ShadowSessionRejected("active epoch calendar authority is corrupt")
        return sessions

    @staticmethod
    def _session_open(value: date) -> datetime:
        return datetime.combine(value, _OPEN_TIME, tzinfo=_SHANGHAI).astimezone(
            timezone.utc
        )

    @staticmethod
    def _session_close(value: date) -> datetime:
        return datetime.combine(value, _CLOSE_TIME, tzinfo=_SHANGHAI).astimezone(
            timezone.utc
        )

    @staticmethod
    def _reference_parts(model: Any) -> tuple[Mapping[str, Any], Mapping[str, Any], set[str]]:
        reference = model.ref_json
        if not isinstance(reference, Mapping):
            raise ShadowSessionRejected("snapshot reference is malformed")
        manifest = reference.get("manifest")
        if not isinstance(manifest, Mapping):
            raise ShadowSessionRejected("snapshot manifest is absent")
        labels = {str(value) for value in reference.get("trust_labels") or ()}
        if not (
            str(reference.get("snapshot_id") or "") == str(model.snapshot_id)
            and str(reference.get("content_hash") or "") == str(model.content_hash)
            and str(reference.get("uri") or "") == str(model.uri)
            and str(reference.get("tier") or "") == str(model.tier)
            and str(reference.get("quality_status") or "")
            == str(model.quality_status)
        ):
            raise ShadowSessionRejected("snapshot reference differs from catalog columns")
        return reference, manifest, labels

    def _snapshot(
        self,
        session: Session,
        snapshot_id: str,
        *,
        role: str,
        observed_date: date | None = None,
        epoch: Any | None = None,
        database_now: datetime | None = None,
    ) -> Any:
        model = session.get(orm.DataSnapshotModel, snapshot_id)
        if model is None:
            raise ShadowSessionRejected(f"{role} snapshot is not registered")
        if model.tier != "gold" or model.quality_status != "accepted":
            raise ShadowSessionRejected(f"{role} snapshot is not accepted Gold")
        if not self._enforce_realtime or observed_date is None or epoch is None:
            return model

        reference, manifest, labels = self._reference_parts(model)
        if _BLOCKING_TRUST_LABELS & labels:
            raise ShadowSessionRejected(f"{role} snapshot has blocking trust labels")
        as_of = _db_aware(model.as_of)
        opening = self._session_open(observed_date)
        close = self._session_close(observed_date)
        now = database_now or self._database_now(session)

        if role == "decision":
            manifest_role = str(manifest.get("role") or "")
            if manifest_role in {"execution", "mark", "shadow_typed_bars"}:
                raise ShadowSessionRejected("decision snapshot reuses a market-data role")
            if as_of >= opening:
                raise ShadowSessionRejected(
                    "decision snapshot was not available before execution open"
                )
            return model

        expected_prefix = f"{role}_"
        expected_hash = content_fingerprint(
            dict(manifest), domain=_EXECUTION_SNAPSHOT_DOMAIN
        )
        physical = manifest.get("physical_object")
        calendar = manifest.get("trading_calendar")
        parent_ids = tuple(map(str, manifest.get("parent_snapshot_ids") or ()))
        ref_parent_ids = tuple(map(str, reference.get("parent_snapshot_ids") or ()))
        required_labels = {
            "point_in_time",
            "quality_accepted",
            "physical_object_verified",
            f"shadow_{role}_role",
        }
        if not (
            str(manifest.get("role") or "") == role
            and str(manifest.get("trade_date") or "") == observed_date.isoformat()
            and str(manifest.get("tier") or "") == "gold"
            and str(manifest.get("quality_status") or "") == "pass"
            and str(manifest.get("capability") or "") == "accepted"
            and str(model.snapshot_id) == f"{expected_prefix}{expected_hash}"
            and str(model.content_hash) == expected_hash
            and required_labels.issubset(labels)
            and isinstance(physical, Mapping)
            and str(physical.get("uri") or "") == str(model.uri)
            and _SHA256.fullmatch(str(physical.get("sha256") or ""))
            and int(physical.get("size_bytes") or 0) > 0
            and parent_ids
            and parent_ids == ref_parent_ids
            and all(session.get(orm.DataSnapshotModel, value) is not None for value in parent_ids)
            and isinstance(calendar, Mapping)
            and observed_date.isoformat()
            in {str(value) for value in calendar.get("sessions") or ()}
            and str(calendar.get("content_hash") or "")
            == str(epoch.calendar_content_hash)
        ):
            raise ShadowSessionRejected(f"{role} typed snapshot authority is invalid")
        evidence = manifest.get("evidence")
        if not isinstance(evidence, Mapping) or not _SHA256.fullmatch(
            str(evidence.get("capability_evidence_hash") or "")
        ):
            raise ShadowSessionRejected(f"{role} capability evidence is absent")
        if role == "execution" and not (
            opening <= as_of <= opening + timedelta(minutes=5) and as_of <= now
        ):
            raise ShadowSessionRejected(
                "execution snapshot is outside the live 09:30 observation window"
            )
        if role == "mark" and (as_of < close or as_of > now):
            raise ShadowSessionRejected("mark snapshot is outside the completed session")
        return model

    @staticmethod
    def _typed_partition(
        session: Session,
        *,
        observed_date: date,
        execution_snapshot_id: str,
        mark_snapshot_id: str,
    ) -> Any:
        partition = session.scalar(
            select(orm.PartitionRunModel).where(
                orm.PartitionRunModel.source_id == "research_os",
                orm.PartitionRunModel.dataset == "typed_execution_snapshot",
                orm.PartitionRunModel.partition_key == observed_date.isoformat(),
            )
        )
        if partition is None or partition.status != "succeeded":
            raise ShadowSessionRejected("typed execution partition is not succeeded")
        details = dict(partition.details_json or {})
        expected_output = content_fingerprint(
            details,
            domain="factor-lab/research-os/v1/typed-execution-partition-result",
        )
        capability = details.get("capability")
        if not (
            str(details.get("execution_snapshot_id") or "")
            == execution_snapshot_id
            and str(details.get("mark_snapshot_id") or "") == mark_snapshot_id
            and str(details.get("trade_date") or "") == observed_date.isoformat()
            and isinstance(capability, Mapping)
            and str(capability.get("decision") or "") == "accepted"
            and _SHA256.fullmatch(str(capability.get("evidence_hash") or ""))
            and str(partition.output_hash or "") == expected_output
        ):
            raise ShadowSessionRejected("typed execution partition binding is invalid")
        return partition

    def bind_role(
        self,
        *,
        role: ShadowRole | str,
        role_key: str,
        account_id: str,
        bound_at: datetime,
        sleeve_id: str | None = None,
        experiment_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> ShadowRoleBinding:
        """Bind a production role to an account and the currently active epoch.

        Epoch identity is deliberately derived from PostgreSQL.  The caller
        cannot supply a self-asserted forward window.
        """

        normalized_role = role if isinstance(role, ShadowRole) else ShadowRole(role)
        normalized_key = _identifier(role_key, name="role_key")
        normalized_account = _identifier(account_id, name="account_id")
        timestamp = _aware(bound_at, name="bound_at")
        normalized_sleeve = (
            None if sleeve_id is None else _identifier(sleeve_id, name="sleeve_id")
        )
        normalized_experiment = (
            None
            if experiment_id is None
            else _identifier(experiment_id, name="experiment_id")
        )
        if normalized_role is ShadowRole.CHALLENGER and (
            normalized_sleeve is None or normalized_experiment is None
        ):
            raise ShadowRoleConflict(
                "a Challenger binding requires both sleeve_id and experiment_id"
            )
        meta = dict(metadata or {})
        canonical_json(meta)

        with self._sessions.begin() as session:
            account = session.get(orm.ShadowAccountModel, normalized_account)
            if account is None:
                raise ShadowRoleConflict("shadow role account is not registered")
            if normalized_experiment is not None:
                experiment = session.get(orm.ExperimentModel, normalized_experiment)
                if experiment is None:
                    raise ShadowRoleConflict("shadow role experiment is not registered")
                if normalized_role is ShadowRole.CHALLENGER:
                    result = session.scalar(
                        select(orm.ExperimentResultModel).where(
                            orm.ExperimentResultModel.experiment_id
                            == normalized_experiment,
                            orm.ExperimentResultModel.authoritative.is_(True),
                        )
                    )
                    if (
                        experiment.status != "completed"
                        or result is None
                        or result.outcome != "promoted_to_shadow"
                    ):
                        raise ShadowRoleConflict(
                            "Challenger role requires a completed promoted authoritative result"
                        )
            epoch = self._activated_epoch(session)
            epoch_id = None if epoch is None else epoch.epoch_id
            statement = select(orm.ShadowRoleBindingModel).where(
                orm.ShadowRoleBindingModel.role == normalized_role.value,
                orm.ShadowRoleBindingModel.role_key == normalized_key,
                orm.ShadowRoleBindingModel.active.is_(True),
            )
            if self.engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            existing = session.scalar(statement)
            identity = {
                "role": normalized_role.value,
                "role_key": normalized_key,
                "account_id": normalized_account,
                "sleeve_id": normalized_sleeve,
                "experiment_id": normalized_experiment,
                "epoch_id": epoch_id,
                "metadata": meta,
            }
            if existing is not None:
                current = {
                    "role": existing.role,
                    "role_key": existing.role_key,
                    "account_id": existing.account_id,
                    "sleeve_id": existing.sleeve_id,
                    "experiment_id": existing.experiment_id,
                    "epoch_id": existing.epoch_id,
                    "metadata": dict(existing.metadata_json),
                }
                if canonical_json(current) == canonical_json(identity):
                    return self._binding(existing)
                existing.active = False
                existing.unbound_at = timestamp

            payload = {**identity, "bound_at": timestamp}
            binding_hash = content_fingerprint(payload, domain=_BINDING_HASH_DOMAIN)
            model = orm.ShadowRoleBindingModel(
                binding_id=f"shadow_binding_{binding_hash[:64]}",
                binding_hash=binding_hash,
                role=normalized_role.value,
                role_key=normalized_key,
                account_id=normalized_account,
                sleeve_id=normalized_sleeve,
                experiment_id=normalized_experiment,
                epoch_id=epoch_id,
                active=True,
                bound_at=timestamp,
                unbound_at=None,
                metadata_json=meta,
            )
            session.add(model)
            try:
                session.flush()
            except IntegrityError as exc:
                raise ShadowRoleConflict("shadow role binding conflicts with durable state") from exc
            return self._binding(model)

    def active_binding(
        self,
        *,
        role: ShadowRole | str,
        role_key: str,
    ) -> ShadowRoleBinding | None:
        normalized_role = role if isinstance(role, ShadowRole) else ShadowRole(role)
        normalized_key = _identifier(role_key, name="role_key")
        with self._sessions() as session:
            model = session.scalar(
                select(orm.ShadowRoleBindingModel).where(
                    orm.ShadowRoleBindingModel.role == normalized_role.value,
                    orm.ShadowRoleBindingModel.role_key == normalized_key,
                    orm.ShadowRoleBindingModel.active.is_(True),
                )
            )
            return None if model is None else self._binding(model)

    def active_fleet_bindings(self) -> tuple[ShadowRoleBinding, ...]:
        """Return the code-selected active Champion/Challenger fleet."""

        with self._sessions() as session:
            models = list(
                session.scalars(
                    select(orm.ShadowRoleBindingModel)
                    .where(
                        orm.ShadowRoleBindingModel.role.in_(
                            (
                                ShadowRole.CHAMPION.value,
                                ShadowRole.CHALLENGER.value,
                            )
                        ),
                        orm.ShadowRoleBindingModel.active.is_(True),
                    )
                    .order_by(
                        orm.ShadowRoleBindingModel.role,
                        orm.ShadowRoleBindingModel.role_key,
                        orm.ShadowRoleBindingModel.binding_id,
                    )
                )
            )
            return tuple(self._binding(model) for model in models)

    @staticmethod
    def _fleet_closure_payload(
        *,
        trade_date: date,
        evidence_class: ShadowEvidenceClass,
        epoch_id: str | None,
        evidence_window_hash: str | None,
        members: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        return {
            "schema_version": _FLEET_CLOSURE_SCHEMA,
            "trade_date": trade_date.isoformat(),
            "evidence_class": evidence_class.value,
            "epoch_id": epoch_id,
            "evidence_window_hash": evidence_window_hash,
            "members": [dict(member) for member in members],
        }

    @classmethod
    def _fleet_closure(cls, model: Any) -> ShadowFleetClosure:
        try:
            observed_date = date.fromisoformat(str(model.trade_date))
            evidence_class = ShadowEvidenceClass(str(model.evidence_class))
            raw_members = model.members_json
            if not isinstance(raw_members, list):
                raise ValueError("members must be a list")
            members = tuple(
                sorted(
                    (dict(member) for member in raw_members),
                    key=lambda member: (
                        str(member.get("role") or ""),
                        str(member.get("role_key") or ""),
                        str(member.get("binding_id") or ""),
                    ),
                )
            )
        except Exception as exc:
            raise IncompleteFleetEvidence(
                "stored shadow fleet closure is malformed"
            ) from exc
        if int(model.member_count) != len(members) or not members:
            raise IncompleteFleetEvidence(
                "stored shadow fleet closure member count is invalid"
            )
        binding_ids: set[str] = set()
        account_ids: set[str] = set()
        for member in members:
            required = {
                "binding_id",
                "binding_hash",
                "role",
                "role_key",
                "account_id",
                "session_hash",
                "account_event_hash",
            }
            if set(member) != required:
                raise IncompleteFleetEvidence(
                    "stored shadow fleet closure member schema is invalid"
                )
            binding_id = _identifier(str(member["binding_id"]), name="binding_id")
            account_id = _identifier(str(member["account_id"]), name="account_id")
            _identifier(str(member["role_key"]), name="role_key")
            try:
                ShadowRole(str(member["role"]))
            except ValueError as exc:
                raise IncompleteFleetEvidence(
                    "stored shadow fleet closure role is invalid"
                ) from exc
            _hash(str(member["binding_hash"]), name="binding_hash")
            _hash(str(member["session_hash"]), name="session_hash")
            _hash(str(member["account_event_hash"]), name="account_event_hash")
            if binding_id in binding_ids or account_id in account_ids:
                raise IncompleteFleetEvidence(
                    "stored shadow fleet closure contains duplicate authority"
                )
            binding_ids.add(binding_id)
            account_ids.add(account_id)
        epoch_id = None if model.epoch_id is None else str(model.epoch_id)
        window_hash = (
            None
            if model.evidence_window_hash is None
            else _hash(
                str(model.evidence_window_hash),
                name="evidence_window_hash",
            )
        )
        if evidence_class is ShadowEvidenceClass.FORWARD:
            if epoch_id is None or window_hash is None:
                raise IncompleteFleetEvidence(
                    "forward fleet closure is detached from its epoch"
                )
            _identifier(epoch_id, name="epoch_id")
        elif epoch_id is not None or window_hash is not None:
            raise IncompleteFleetEvidence(
                "engineering fleet closure cannot claim an epoch"
            )
        payload = cls._fleet_closure_payload(
            trade_date=observed_date,
            evidence_class=evidence_class,
            epoch_id=epoch_id,
            evidence_window_hash=window_hash,
            members=members,
        )
        expected_hash = content_fingerprint(
            payload, domain=_FLEET_CLOSURE_HASH_DOMAIN
        )
        closure_hash = _hash(str(model.closure_hash), name="closure_hash")
        if closure_hash != expected_hash:
            raise IncompleteFleetEvidence(
                "stored shadow fleet closure hash is corrupt"
            )
        if str(model.closure_id) != f"fleet_closed_{closure_hash[:64]}":
            raise IncompleteFleetEvidence(
                "stored shadow fleet closure identity is corrupt"
            )
        return ShadowFleetClosure(
            closure_id=str(model.closure_id),
            trade_date=observed_date,
            evidence_class=evidence_class,
            epoch_id=epoch_id,
            evidence_window_hash=window_hash,
            members=members,
            closure_hash=closure_hash,
            closed_at=_db_aware(model.closed_at),
        )

    def fleet_closure(
        self, trade_date: date | str
    ) -> ShadowFleetClosure | None:
        observed_date = (
            trade_date
            if isinstance(trade_date, date)
            else date.fromisoformat(str(trade_date))
        )
        with self._sessions() as session:
            model = session.scalar(
                select(orm.ShadowFleetClosureModel).where(
                    orm.ShadowFleetClosureModel.trade_date
                    == observed_date.isoformat()
                )
            )
            return None if model is None else self._fleet_closure(model)

    def close_fleet_day(
        self, trade_date: date | str
    ) -> ShadowFleetClosure:
        """Atomically seal the exact active Champion/Challenger fleet day.

        No member list, NAV or hash is accepted from the caller.  Every member
        is selected from active role bindings and re-verified against its
        immutable session/event authority before the content-addressed closure
        is inserted.
        """

        observed_date = (
            trade_date
            if isinstance(trade_date, date)
            else date.fromisoformat(str(trade_date))
        )
        with self._sessions.begin() as session:
            if self.engine.dialect.name == "postgresql":
                # Freeze the role set for this transaction.  A row-level lock
                # cannot prevent a newly inserted Challenger key from racing
                # the fleet query; SHARE conflicts with role INSERT/UPDATE's
                # ROW EXCLUSIVE lock without blocking ordinary session reads.
                session.execute(
                    text("LOCK TABLE ros_shadow_role_bindings IN SHARE MODE")
                )
            binding_statement = (
                select(orm.ShadowRoleBindingModel)
                .where(
                    orm.ShadowRoleBindingModel.role.in_(
                        (
                            ShadowRole.CHAMPION.value,
                            ShadowRole.CHALLENGER.value,
                        )
                    ),
                    orm.ShadowRoleBindingModel.active.is_(True),
                )
                .order_by(
                    orm.ShadowRoleBindingModel.role,
                    orm.ShadowRoleBindingModel.role_key,
                    orm.ShadowRoleBindingModel.binding_id,
                )
            )
            if self.engine.dialect.name == "postgresql":
                binding_statement = binding_statement.with_for_update()
            bindings = list(session.scalars(binding_statement))
            if not bindings or not any(
                binding.role == ShadowRole.CHAMPION.value
                for binding in bindings
            ):
                raise IncompleteFleetEvidence(
                    "daily shadow fleet requires an active Champion"
                )
            account_ids = [str(binding.account_id) for binding in bindings]
            if len(account_ids) != len(set(account_ids)):
                raise IncompleteFleetEvidence(
                    "daily shadow fleet role bindings reuse an account"
                )
            binding_ids = [str(binding.binding_id) for binding in bindings]
            rows = list(
                session.scalars(
                    select(orm.ShadowSessionModel).where(
                        orm.ShadowSessionModel.role_binding_id.in_(binding_ids),
                        orm.ShadowSessionModel.trade_date
                        == observed_date.isoformat(),
                    )
                )
            )
            by_binding = {str(row.role_binding_id): row for row in rows}
            if len(rows) != len(bindings) or set(by_binding) != set(binding_ids):
                raise IncompleteFleetEvidence(
                    "daily shadow fleet is missing an active account projection"
                )
            members: list[dict[str, Any]] = []
            classes: set[ShadowEvidenceClass] = set()
            epochs: set[str | None] = set()
            windows: set[str | None] = set()
            for binding in bindings:
                row = by_binding[str(binding.binding_id)]
                if str(row.account_id) != str(binding.account_id):
                    raise IncompleteFleetEvidence(
                        "daily shadow session account differs from its role binding"
                    )
                self._validate_stored_projection(row)
                _, events = self._verify_account_chain(session, str(binding.account_id))
                source_event = next(
                    (
                        event
                        for event in events
                        if event.event_hash == row.account_event_hash
                    ),
                    None,
                )
                if (
                    source_event is None
                    or source_event.event_type != "account_projected"
                    or int(source_event.sequence_number)
                    != int(row.account_event_sequence)
                ):
                    raise IncompleteFleetEvidence(
                        "daily shadow fleet member is detached from its event chain"
                    )
                evidence_class = ShadowEvidenceClass(str(row.evidence_class))
                classes.add(evidence_class)
                epochs.add(None if row.epoch_id is None else str(row.epoch_id))
                windows.add(
                    None
                    if row.evidence_window_hash is None
                    else str(row.evidence_window_hash)
                )
                members.append(
                    {
                        "binding_id": str(binding.binding_id),
                        "binding_hash": str(binding.binding_hash),
                        "role": str(binding.role),
                        "role_key": str(binding.role_key),
                        "account_id": str(binding.account_id),
                        "session_hash": str(row.session_hash),
                        "account_event_hash": str(row.account_event_hash),
                    }
                )
            if len(classes) != 1 or len(epochs) != 1 or len(windows) != 1:
                raise IncompleteFleetEvidence(
                    "daily shadow fleet members do not share one evidence scope"
                )
            evidence_class = next(iter(classes))
            epoch_id = next(iter(epochs))
            window_hash = next(iter(windows))
            ordered_members = tuple(
                sorted(
                    members,
                    key=lambda member: (
                        member["role"],
                        member["role_key"],
                        member["binding_id"],
                    ),
                )
            )
            payload = self._fleet_closure_payload(
                trade_date=observed_date,
                evidence_class=evidence_class,
                epoch_id=epoch_id,
                evidence_window_hash=window_hash,
                members=ordered_members,
            )
            closure_hash = content_fingerprint(
                payload, domain=_FLEET_CLOSURE_HASH_DOMAIN
            )
            existing = session.scalar(
                select(orm.ShadowFleetClosureModel).where(
                    orm.ShadowFleetClosureModel.trade_date
                    == observed_date.isoformat()
                )
            )
            if existing is not None:
                closure = self._fleet_closure(existing)
                if closure.closure_hash != closure_hash:
                    raise IncompleteFleetEvidence(
                        "daily shadow fleet was already closed with different authority"
                    )
                return closure
            model = orm.ShadowFleetClosureModel(
                closure_id=f"fleet_closed_{closure_hash[:64]}",
                trade_date=observed_date.isoformat(),
                evidence_class=evidence_class.value,
                epoch_id=epoch_id,
                evidence_window_hash=window_hash,
                member_count=len(ordered_members),
                members_json=list(ordered_members),
                closure_hash=closure_hash,
                closed_at=self._database_now(session),
            )
            try:
                with session.begin_nested():
                    session.add(model)
                    session.flush()
            except IntegrityError:
                existing = session.scalar(
                    select(orm.ShadowFleetClosureModel).where(
                        orm.ShadowFleetClosureModel.trade_date
                        == observed_date.isoformat()
                    )
                )
                if existing is None:
                    raise
                closure = self._fleet_closure(existing)
                if closure.closure_hash != closure_hash:
                    raise IncompleteFleetEvidence(
                        "concurrent daily fleet closure conflicts with authority"
                    )
                return closure
            return self._fleet_closure(model)

    @staticmethod
    def _verify_account_chain(session: Session, account_id: str) -> tuple[Any, list[Any]]:
        account = session.get(orm.ShadowAccountModel, account_id)
        if account is None:
            raise ShadowSessionRejected("shadow account is not registered")
        events = list(
            session.scalars(
                select(orm.ShadowEventModel)
                .where(orm.ShadowEventModel.account_id == account_id)
                .order_by(orm.ShadowEventModel.sequence_number)
            )
        )
        if not events:
            raise ShadowSessionRejected("shadow account has no event chain")
        previous = _ZERO_EVENT_HASH
        for expected, event in enumerate(events, start=1):
            if (
                int(event.sequence_number) != expected
                or event.previous_event_hash != previous
                or event.event_hash != _event_hash(event)
            ):
                raise ShadowSessionRejected("shadow account event chain is corrupt")
            previous = event.event_hash
        if (
            int(account.last_event_sequence) != len(events)
            or account.last_event_hash != previous
        ):
            raise ShadowSessionRejected("shadow account chain tip differs from events")
        return account, events

    @staticmethod
    def _session_content(values: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "account_id": values["account_id"],
            "trade_date": values["trade_date"],
            "role_binding_id": values["role_binding_id"],
            "epoch_id": values["epoch_id"],
            "evidence_window_hash": values["evidence_window_hash"],
            "evidence_class": values["evidence_class"],
            "decision_snapshot_id": values["decision_snapshot_id"],
            "execution_snapshot_id": values["execution_snapshot_id"],
            "mark_snapshot_id": values["mark_snapshot_id"],
            "rebalanced": bool(values["rebalanced"]),
            "cash": float(values["cash"]),
            "positions_value": float(values["positions_value"]),
            "nav": float(values["nav"]),
            "benchmark_nav": float(values["benchmark_nav"]),
            "position_count": int(values["position_count"]),
            "account_event_hash": values["account_event_hash"],
            "account_event_sequence": int(values["account_event_sequence"]),
        }

    def record_projection(
        self,
        *,
        role_binding_id: str,
        account_event_hash: str,
        trade_date: date | str,
        recorded_at: datetime,
    ) -> ShadowSessionProjection:
        """Admit one event-ledger projection without trusting caller NAV values."""

        return self._record_projection(
            role_binding_id=role_binding_id,
            account_event_hash=account_event_hash,
            trade_date=trade_date,
            recorded_at=recorded_at,
            force_engineering=False,
        )

    def record_engineering_projection(
        self,
        *,
        role_binding_id: str,
        account_event_hash: str,
        trade_date: date | str,
        recorded_at: datetime,
    ) -> ShadowSessionProjection:
        """Persist a verified projection that can never enter a forward epoch.

        Engineering canaries may replay physical historical observations after
        an evidence epoch has already been activated.  Their account values
        still have to be derived from the immutable event chain, but their
        authority row must remain ``engineering`` with no epoch/window binding.
        Keeping this as a separate entry point prevents a canary caller from
        accidentally using the date-based forward classifier.
        """

        return self._record_projection(
            role_binding_id=role_binding_id,
            account_event_hash=account_event_hash,
            trade_date=trade_date,
            recorded_at=recorded_at,
            force_engineering=True,
        )

    def _record_projection(
        self,
        *,
        role_binding_id: str,
        account_event_hash: str,
        trade_date: date | str,
        recorded_at: datetime,
        force_engineering: bool,
    ) -> ShadowSessionProjection:
        """Shared event-chain verification for forward and engineering rows."""

        binding_id = _identifier(role_binding_id, name="role_binding_id")
        event_hash = _hash(account_event_hash, name="account_event_hash")
        observed_date = (
            trade_date if isinstance(trade_date, date) else date.fromisoformat(str(trade_date))
        )
        requested_created_at = _aware(recorded_at, name="recorded_at")

        with self._sessions.begin() as session:
            database_now = self._database_now(session)
            created_at = (
                database_now
                if self._enforce_realtime and not force_engineering
                else requested_created_at
            )
            binding_model = session.get(orm.ShadowRoleBindingModel, binding_id)
            if binding_model is None or not binding_model.active:
                raise ShadowSessionRejected("shadow session requires an active role binding")
            binding = self._binding(binding_model)
            epoch = self._activated_epoch(session)
            calendar_sessions: tuple[date, ...] = ()
            if self._enforce_realtime and not force_engineering:
                if epoch is None:
                    raise ShadowSessionRejected(
                        "formal shadow projection requires an activated epoch"
                    )
                calendar_sessions = self._calendar_sessions(session, epoch)
                first_forward = date.fromisoformat(epoch.first_forward_session)
                if observed_date < first_forward or observed_date not in calendar_sessions:
                    raise ShadowSessionRejected(
                        "formal shadow date is outside the active accepted calendar"
                    )
                completed = tuple(
                    value
                    for value in calendar_sessions
                    if value >= first_forward
                    and self._session_close(value) <= database_now
                )
                if not completed or observed_date != completed[-1]:
                    raise ShadowSessionRejected(
                        "formal shadow accepts only the latest complete trading session"
                    )
                if _db_aware(epoch.activated_at) >= self._session_open(first_forward):
                    raise ShadowSessionRejected(
                        "active epoch was not sealed before its first session opened"
                    )
                if binding.epoch_id != epoch.epoch_id:
                    raise ShadowSessionRejected(
                        "forward session role binding is not bound to the active epoch"
                    )
                if binding.bound_at >= self._session_open(observed_date):
                    raise ShadowSessionRejected(
                        "forward role binding was not active before execution open"
                    )
            account, events = self._verify_account_chain(session, binding.account_id)
            projection = next(
                (event for event in events if event.event_hash == event_hash), None
            )
            if projection is None or projection.event_type != "account_projected":
                raise ShadowSessionRejected(
                    "account_event_hash must name an account_projected event"
                )
            if _db_aware(projection.occurred_at).date() != observed_date:
                raise ShadowSessionRejected("projection event date differs from trade_date")
            if self._enforce_realtime and not force_engineering:
                projection_time = _db_aware(projection.occurred_at)
                if not (
                    self._session_close(observed_date)
                    <= projection_time
                    <= database_now
                ):
                    raise ShadowSessionRejected(
                        "formal account projection was not recorded after the session close"
                    )
            projection_payload = dict(projection.payload_json)
            step = projection_payload.get(_STEP_KEY)
            if not isinstance(step, Mapping) or step.get("kind") != "account_projection":
                raise ShadowSessionRejected("projection lacks an authoritative daily step")
            step_id = str(step.get("step_id") or "")
            if not step_id:
                raise ShadowSessionRejected("projection daily step has no step_id")
            linked = [
                event
                for event in events
                if isinstance(event.payload_json.get(_STEP_KEY), Mapping)
                and event.payload_json[_STEP_KEY].get("step_id") == step_id
            ]
            evidence_events = [
                event for event in linked if event.event_type == "session_evidence"
            ]
            mark_events = [
                event for event in linked if event.event_type == "mark_to_market"
            ]
            if len(evidence_events) != 1 or len(mark_events) != 1:
                raise ShadowSessionRejected(
                    "daily projection needs exactly one session_evidence and mark_to_market event"
                )
            evidence_payload = dict(evidence_events[0].payload_json)
            mark_payload = dict(mark_events[0].payload_json)
            evidence_bindings = evidence_payload.get("snapshot_bindings")
            mark_bindings = mark_payload.get("snapshot_bindings")
            if (
                not isinstance(evidence_bindings, Mapping)
                or not isinstance(mark_bindings, Mapping)
                or canonical_json(dict(evidence_bindings))
                != canonical_json(dict(mark_bindings))
            ):
                raise ShadowSessionRejected("daily snapshot bindings are absent or inconsistent")
            decision_snapshot_id = evidence_bindings.get("decision_snapshot_id")
            decision_snapshot_id = (
                None
                if decision_snapshot_id is None
                else _identifier(str(decision_snapshot_id), name="decision_snapshot_id")
            )
            execution_snapshot_id = _identifier(
                str(evidence_bindings.get("execution_snapshot_id") or ""),
                name="execution_snapshot_id",
            )
            mark_snapshot_id = _identifier(
                str(evidence_bindings.get("mark_snapshot_id") or ""),
                name="mark_snapshot_id",
            )
            rebalanced = bool(evidence_payload.get("rebalanced"))
            if rebalanced != (decision_snapshot_id is not None):
                raise ShadowSessionRejected(
                    "decision snapshot must exist exactly on rebalance sessions"
                )
            role_ids = [execution_snapshot_id, mark_snapshot_id]
            if decision_snapshot_id is not None:
                role_ids.append(decision_snapshot_id)
            if len(role_ids) != len(set(role_ids)):
                raise ShadowSessionRejected(
                    "decision, execution and mark snapshot roles must be distinct"
                )
            execution_model = self._snapshot(
                session,
                execution_snapshot_id,
                role="execution",
                observed_date=observed_date,
                epoch=epoch,
                database_now=database_now,
            )
            mark_model = self._snapshot(
                session,
                mark_snapshot_id,
                role="mark",
                observed_date=observed_date,
                epoch=epoch,
                database_now=database_now,
            )
            if decision_snapshot_id is not None:
                self._snapshot(
                    session,
                    decision_snapshot_id,
                    role="decision",
                    observed_date=observed_date,
                    epoch=epoch,
                    database_now=database_now,
                )
            if self._enforce_realtime and not force_engineering:
                _, execution_manifest, _ = self._reference_parts(execution_model)
                _, mark_manifest, _ = self._reference_parts(mark_model)
                execution_parents = set(
                    map(str, execution_manifest.get("parent_snapshot_ids") or ())
                )
                mark_parents = set(
                    map(str, mark_manifest.get("parent_snapshot_ids") or ())
                )
                if not execution_parents.intersection(mark_parents):
                    raise ShadowSessionRejected(
                        "execution and mark snapshots do not share accepted Gold ancestry"
                    )
                self._typed_partition(
                    session,
                    observed_date=observed_date,
                    execution_snapshot_id=execution_snapshot_id,
                    mark_snapshot_id=mark_snapshot_id,
                )

            projection_state = projection_payload.get("account_state")
            if not isinstance(projection_state, Mapping):
                raise ShadowSessionRejected("account projection state is malformed")
            cash = _finite_nonnegative(mark_payload.get("cash"), name="cash")
            positions_value = _finite_nonnegative(
                mark_payload.get("positions_value"), name="positions_value"
            )
            nav = _finite_nonnegative(mark_payload.get("nav"), name="nav")
            benchmark_nav = _finite_nonnegative(
                mark_payload.get("benchmark_nav"), name="benchmark_nav"
            )
            position_count = int(mark_payload.get("position_count", -1))
            if nav <= 0 or benchmark_nav <= 0 or position_count < 0:
                raise ShadowSessionRejected("daily account values are invalid")
            if not isclose(
                cash + positions_value,
                nav,
                rel_tol=1e-12,
                abs_tol=_ACCOUNT_TOLERANCE,
            ):
                raise ShadowSessionRejected("cash + positions value does not equal NAV")
            for key, expected in (
                ("cash", cash),
                ("nav", nav),
                ("benchmark_nav", benchmark_nav),
            ):
                actual = float(projection_state.get(key, float("nan")))
                if not isfinite(actual) or not isclose(
                    actual, expected, rel_tol=1e-12, abs_tol=_ACCOUNT_TOLERANCE
                ):
                    raise ShadowSessionRejected(
                        f"account projection {key} differs from mark event"
                    )

            is_forward = (
                epoch is not None
                and observed_date >= date.fromisoformat(epoch.first_forward_session)
            )
            if force_engineering:
                evidence_class = ShadowEvidenceClass.ENGINEERING
                epoch_id = None
                window_hash = None
            elif is_forward:
                if binding.epoch_id != epoch.epoch_id:
                    raise ShadowSessionRejected(
                        "forward session role binding is not bound to the active epoch"
                    )
                evidence_class = ShadowEvidenceClass.FORWARD
                epoch_id = epoch.epoch_id
                window_hash = epoch.evidence_window_hash
                if self._enforce_realtime:
                    previous = session.scalar(
                        select(orm.ShadowSessionModel)
                        .where(
                            orm.ShadowSessionModel.role_binding_id
                            == binding.binding_id,
                            orm.ShadowSessionModel.evidence_class
                            == ShadowEvidenceClass.FORWARD.value,
                            orm.ShadowSessionModel.trade_date
                            < observed_date.isoformat(),
                        )
                        .order_by(orm.ShadowSessionModel.trade_date.desc())
                        .limit(1)
                    )
                    first_forward = date.fromisoformat(epoch.first_forward_session)
                    if previous is None:
                        eligible = tuple(
                            value
                            for value in calendar_sessions
                            if value >= first_forward
                            and self._session_open(value) > binding.bound_at
                        )
                        expected_session = eligible[0] if eligible else None
                    else:
                        previous_date = date.fromisoformat(previous.trade_date)
                        later = tuple(
                            value for value in calendar_sessions if value > previous_date
                        )
                        expected_session = later[0] if later else None
                    if expected_session != observed_date:
                        missing = tuple(
                            value
                            for value in calendar_sessions
                            if expected_session is not None
                            and expected_session <= value < observed_date
                        )
                        dispositions = self._gap_incident_dispositions(session, missing)
                        if any(
                            value == "open_blocker"
                            for value in dispositions.values()
                        ):
                            raise ShadowSessionRejected(
                                "formal shadow gap has an exact-date open blocking incident; "
                                "the account remains frozen_data"
                            )
                        if missing and all(
                            dispositions.get(value) == "resolved_requires_replay"
                            for value in missing
                        ):
                            raise ShadowSessionRejected(
                                "resolved data gap still requires trusted session/company-action "
                                "replay or a new evidence segment"
                            )
                        raise ShadowSessionRejected(
                            "formal shadow sessions must append each accepted trading day in order; "
                            "unrelated, superseded, or malformed incidents cannot authorize a gap"
                        )
            else:
                evidence_class = ShadowEvidenceClass.ENGINEERING
                epoch_id = None
                window_hash = None
            values = {
                "account_id": binding.account_id,
                "trade_date": observed_date.isoformat(),
                "role_binding_id": binding.binding_id,
                "epoch_id": epoch_id,
                "evidence_window_hash": window_hash,
                "evidence_class": evidence_class.value,
                "decision_snapshot_id": decision_snapshot_id,
                "execution_snapshot_id": execution_snapshot_id,
                "mark_snapshot_id": mark_snapshot_id,
                "rebalanced": rebalanced,
                "cash": cash,
                "positions_value": positions_value,
                "nav": nav,
                "benchmark_nav": benchmark_nav,
                "position_count": position_count,
                "account_event_hash": projection.event_hash,
                "account_event_sequence": int(projection.sequence_number),
            }
            session_hash = content_fingerprint(
                self._session_content(values), domain=_SESSION_HASH_DOMAIN
            )
            existing = session.get(
                orm.ShadowSessionModel,
                {"account_id": binding.account_id, "trade_date": observed_date.isoformat()},
            )
            if existing is not None:
                if existing.session_hash != session_hash:
                    raise ShadowSessionRejected(
                        "shadow session date already has different authoritative evidence"
                    )
                return self._projection(existing)
            model = orm.ShadowSessionModel(
                **values,
                session_hash=session_hash,
                created_at=created_at,
            )
            session.add(model)
            try:
                session.flush()
            except IntegrityError as exc:
                raise ShadowSessionRejected(
                    "shadow projection conflicts with immutable authority state"
                ) from exc
            # Keep the account-table values honest at the moment the authority
            # row is recorded.  Historical projections remain verified by the
            # linked event and immutable session hash.
            if int(account.last_event_sequence) == int(projection.sequence_number):
                if not (
                    isclose(float(account.cash), cash, abs_tol=_ACCOUNT_TOLERANCE)
                    and isclose(float(account.nav), nav, abs_tol=_ACCOUNT_TOLERANCE)
                    and isclose(
                        float(account.benchmark_nav),
                        benchmark_nav,
                        abs_tol=_ACCOUNT_TOLERANCE,
                    )
                ):
                    raise ShadowSessionRejected(
                        "account table differs from its latest projection"
                    )
                positions = list(
                    session.scalars(
                        select(orm.ShadowPositionModel).where(
                            orm.ShadowPositionModel.account_id == binding.account_id,
                            orm.ShadowPositionModel.quantity > 0,
                        )
                    )
                )
                durable_positions_value = sum(
                    float(position.market_value) for position in positions
                )
                if (
                    len(positions) != position_count
                    or not isclose(
                        durable_positions_value,
                        positions_value,
                        rel_tol=1e-12,
                        abs_tol=_ACCOUNT_TOLERANCE,
                    )
                ):
                    raise ShadowSessionRejected(
                        "position table differs from the latest account projection"
                    )
            return self._projection(model)

    @staticmethod
    def _validate_stored_projection(model: Any) -> None:
        values = {
            "account_id": model.account_id,
            "trade_date": model.trade_date,
            "role_binding_id": model.role_binding_id,
            "epoch_id": model.epoch_id,
            "evidence_window_hash": model.evidence_window_hash,
            "evidence_class": model.evidence_class,
            "decision_snapshot_id": model.decision_snapshot_id,
            "execution_snapshot_id": model.execution_snapshot_id,
            "mark_snapshot_id": model.mark_snapshot_id,
            "rebalanced": bool(model.rebalanced),
            "cash": float(model.cash),
            "positions_value": float(model.positions_value),
            "nav": float(model.nav),
            "benchmark_nav": float(model.benchmark_nav),
            "position_count": int(model.position_count),
            "account_event_hash": model.account_event_hash,
            "account_event_sequence": int(model.account_event_sequence),
        }
        expected = content_fingerprint(
            ShadowEvidenceAuthority._session_content(values),
            domain=_SESSION_HASH_DOMAIN,
        )
        if model.session_hash != expected:
            raise ShadowSessionRejected("stored shadow session hash is corrupt")
        if not isclose(
            float(model.cash) + float(model.positions_value),
            float(model.nav),
            rel_tol=1e-12,
            abs_tol=_ACCOUNT_TOLERANCE,
        ):
            raise ShadowSessionRejected("stored shadow account equation is invalid")
        role_ids = [model.execution_snapshot_id, model.mark_snapshot_id]
        if model.decision_snapshot_id is not None:
            role_ids.append(model.decision_snapshot_id)
        if len(role_ids) != len(set(role_ids)):
            raise ShadowSessionRejected("stored snapshot roles are not distinct")
        if bool(model.rebalanced) != (model.decision_snapshot_id is not None):
            raise ShadowSessionRejected("stored decision snapshot role is inconsistent")

    @staticmethod
    def _returns(session: Session, account_id: str, rows: Sequence[Any]) -> tuple[float, ...]:
        first = rows[0]
        previous = session.scalar(
            select(orm.ShadowSessionModel)
            .where(
                orm.ShadowSessionModel.account_id == account_id,
                orm.ShadowSessionModel.role_binding_id == first.role_binding_id,
                orm.ShadowSessionModel.epoch_id == first.epoch_id,
                orm.ShadowSessionModel.evidence_window_hash
                == first.evidence_window_hash,
                orm.ShadowSessionModel.trade_date < first.trade_date,
            )
            .order_by(orm.ShadowSessionModel.trade_date.desc())
            .limit(1)
        )
        if previous is None:
            older_epoch_row = session.scalar(
                select(orm.ShadowSessionModel.trade_date)
                .where(
                    orm.ShadowSessionModel.account_id == account_id,
                    orm.ShadowSessionModel.trade_date < first.trade_date,
                )
                .limit(1)
            )
            if older_epoch_row is None:
                account = session.get(orm.ShadowAccountModel, account_id)
                if account is None:
                    raise ShadowSessionRejected("shadow account disappeared")
                prior_nav = float(account.initial_capital)
            else:
                # Never splice the final NAV return from a superseded epoch
                # into the new statistical window. Its first observation is a
                # fresh baseline; only within-epoch changes count as evidence.
                prior_nav = float(first.nav)
        else:
            prior_nav = float(previous.nav)
        returns: list[float] = []
        for row in rows:
            nav = float(row.nav)
            value = nav / prior_nav - 1.0
            if not isfinite(value) or value <= -1.0:
                raise ShadowSessionRejected("shadow NAV return is invalid")
            returns.append(value)
            prior_nav = nav
        return tuple(returns)

    def aligned_forward_window(
        self,
        *,
        champion_account_id: str,
        challenger_account_id: str,
        minimum_sessions: int = 60,
        through: date | str | None = None,
    ) -> AlignedForwardEvidence:
        """Return the exact same-date, same-epoch Champion/Challenger window."""

        if minimum_sessions < 60:
            raise ValueError("formal forward evidence cannot use fewer than 60 sessions")
        champion_id = _identifier(champion_account_id, name="champion_account_id")
        challenger_id = _identifier(challenger_account_id, name="challenger_account_id")
        if champion_id == challenger_id:
            raise MisalignedForwardEvidence(
                "Champion and Challenger require distinct shadow accounts"
            )
        through_date = (
            None
            if through is None
            else through
            if isinstance(through, date)
            else date.fromisoformat(str(through))
        )
        with self._sessions() as session:
            epoch = self._activated_epoch(session)
            if epoch is None:
                raise InsufficientForwardEvidence(
                    "formal forward evidence requires an activated epoch"
                )
            account_events: dict[str, dict[str, Any]] = {}
            account_chain_tips: dict[str, Mapping[str, Any]] = {}
            for account_id in (champion_id, challenger_id):
                account, events = self._verify_account_chain(session, account_id)
                account_events[account_id] = {
                    event.event_hash: event for event in events
                }
                account_chain_tips[account_id] = {
                    "last_event_hash": str(account.last_event_hash),
                    "last_event_sequence": int(account.last_event_sequence),
                }
            binding_rows: dict[ShadowRole, Any] = {}
            for role, account_id in (
                (ShadowRole.CHAMPION, champion_id),
                (ShadowRole.CHALLENGER, challenger_id),
            ):
                matches = list(
                    session.scalars(
                        select(orm.ShadowRoleBindingModel).where(
                            orm.ShadowRoleBindingModel.role == role.value,
                            orm.ShadowRoleBindingModel.account_id == account_id,
                            orm.ShadowRoleBindingModel.epoch_id == epoch.epoch_id,
                            orm.ShadowRoleBindingModel.active.is_(True),
                        )
                    )
                )
                if len(matches) != 1:
                    raise ShadowRoleConflict(
                        f"{role.value} account needs exactly one binding in the active epoch"
                    )
                binding_rows[role] = matches[0]

            def rows_for(binding: Any) -> list[Any]:
                statement = select(orm.ShadowSessionModel).where(
                    orm.ShadowSessionModel.role_binding_id == binding.binding_id,
                    orm.ShadowSessionModel.epoch_id == epoch.epoch_id,
                    orm.ShadowSessionModel.evidence_window_hash
                    == epoch.evidence_window_hash,
                    orm.ShadowSessionModel.evidence_class
                    == ShadowEvidenceClass.FORWARD.value,
                )
                if through_date is not None and not self._enforce_realtime:
                    statement = statement.where(
                        orm.ShadowSessionModel.trade_date <= through_date.isoformat()
                    )
                return list(
                    session.scalars(
                        statement.order_by(orm.ShadowSessionModel.trade_date)
                    )
                )

            champion_rows = rows_for(binding_rows[ShadowRole.CHAMPION])
            challenger_rows = rows_for(binding_rows[ShadowRole.CHALLENGER])
            if self._enforce_realtime:
                accepted_sessions = self._calendar_sessions(session, epoch)
                database_now = self._database_now(session)
                completed_scope = tuple(
                    value
                    for value in accepted_sessions
                    if value >= date.fromisoformat(epoch.first_forward_session)
                    and self._session_close(value) <= database_now
                )
                if not completed_scope:
                    raise InsufficientForwardEvidence(
                        "the accepted calendar has no database-clock-complete forward session"
                    )
                latest_complete_session = completed_scope[-1]
                if self._require_fleet_closure:
                    latest_closure_model = session.scalar(
                        select(orm.ShadowFleetClosureModel).where(
                            orm.ShadowFleetClosureModel.trade_date
                            == latest_complete_session.isoformat()
                        )
                    )
                    if latest_closure_model is None:
                        raise IncompleteFleetEvidence(
                            f"database-clock latest complete session "
                            f"{latest_complete_session.isoformat()} has no fleet_closed authority"
                        )
                    latest_closure = self._fleet_closure(latest_closure_model)
                    if (
                        latest_closure.evidence_class
                        is not ShadowEvidenceClass.FORWARD
                        or latest_closure.epoch_id != epoch.epoch_id
                        or latest_closure.evidence_window_hash
                        != epoch.evidence_window_hash
                    ):
                        raise IncompleteFleetEvidence(
                            "database-clock latest fleet closure is outside the active forward window"
                        )
                champion_by_date = {row.trade_date: row for row in champion_rows}
                challenger_by_date = {row.trade_date: row for row in challenger_rows}
                calendar_position = {
                    value: offset for offset, value in enumerate(accepted_sessions)
                }

                def require_unbroken_binding_history(
                    rows: Sequence[Any], *, role: ShadowRole
                ) -> None:
                    """Reject an implicit evidence reset after a skipped ledger day.

                    A new role binding/epoch can deliberately begin a new evidence
                    segment.  Missing sessions *inside* the active binding cannot be
                    erased by taking only the trailing common suffix: the skipped
                    day may contain a split, dividend, delisting, or other account
                    event.  It must be replayed before later projections exist, or
                    the account remains unusable and a fresh binding/segment is
                    required.
                    """

                    if not rows:
                        return
                    observed = tuple(date.fromisoformat(row.trade_date) for row in rows)
                    try:
                        start = calendar_position[observed[0]]
                    except KeyError as exc:
                        raise MisalignedForwardEvidence(
                            f"{role.value} forward projection starts on a non-calendar date"
                        ) from exc
                    if observed[-1] > latest_complete_session:
                        raise MisalignedForwardEvidence(
                            f"{role.value} forward projection extends beyond the "
                            "database-clock latest complete accepted session"
                        )
                    expected = accepted_sessions[
                        start : calendar_position[latest_complete_session] + 1
                    ]
                    if observed == expected:
                        return
                    observed_set = set(observed)
                    missing = tuple(value for value in expected if value not in observed_set)
                    if not missing:
                        raise MisalignedForwardEvidence(
                            f"{role.value} forward projections are not a unique ordered "
                            "accepted-session ledger"
                        )
                    dispositions = self._gap_incident_dispositions(session, missing)
                    if any(
                        disposition == "open_blocker"
                        for disposition in dispositions.values()
                    ):
                        detail = "an exact-date blocking incident remains open"
                    elif dispositions and all(
                        disposition == "resolved_requires_replay"
                        for disposition in dispositions.values()
                    ):
                        detail = (
                            "the blocker was resolved, but the trusted session and "
                            "company actions were not replayed"
                        )
                    else:
                        detail = (
                            "no exact-date resolved blocking incident authorizes the gap"
                        )
                    raise MisalignedForwardEvidence(
                        f"{role.value} shadow ledger skips accepted session(s) "
                        f"{', '.join(value.isoformat() for value in missing)}; {detail}; "
                        "replay the gap before later projections or start a new evidence segment"
                    )

                require_unbroken_binding_history(
                    champion_rows, role=ShadowRole.CHAMPION
                )
                require_unbroken_binding_history(
                    challenger_rows, role=ShadowRole.CHALLENGER
                )
                common = tuple(
                    value
                    for value in accepted_sessions
                    if value.isoformat() in champion_by_date
                    and value.isoformat() in challenger_by_date
                )
                # The Challenger may begin after the Champion, but any later
                # reset must be represented by a fresh binding/segment rather
                # than inferred from a convenient trailing suffix.
                cohort = common
                champion_rows = [
                    champion_by_date[value.isoformat()] for value in cohort
                ]
                challenger_rows = [
                    challenger_by_date[value.isoformat()] for value in cohort
                ]
                champion_dates = tuple(value.isoformat() for value in cohort)
                challenger_dates = champion_dates
                if not cohort or cohort[-1] != latest_complete_session:
                    raise MisalignedForwardEvidence(
                        "formal forward evidence must terminate at the database-clock latest "
                        "complete accepted session"
                    )
            else:
                champion_dates = tuple(row.trade_date for row in champion_rows)
                challenger_dates = tuple(row.trade_date for row in challenger_rows)
                if champion_dates != challenger_dates:
                    raise MisalignedForwardEvidence(
                        "Champion and Challenger forward session dates are not exactly aligned"
                    )
            first_forward = date.fromisoformat(epoch.first_forward_session)
            closures_by_date: dict[str, ShadowFleetClosure] = {}
            if self._require_fleet_closure:
                required_dates = set(champion_dates) | set(challenger_dates)
                if self._enforce_realtime:
                    if completed_scope:
                        required_dates.add(completed_scope[-1].isoformat())
                for session_date in sorted(required_dates):
                    closure_model = session.scalar(
                        select(orm.ShadowFleetClosureModel).where(
                            orm.ShadowFleetClosureModel.trade_date == session_date
                        )
                    )
                    if closure_model is None:
                        raise IncompleteFleetEvidence(
                            f"forward session {session_date} has no fleet_closed authority"
                        )
                    closure = self._fleet_closure(closure_model)
                    if (
                        closure.evidence_class is not ShadowEvidenceClass.FORWARD
                        or closure.epoch_id != epoch.epoch_id
                        or closure.evidence_window_hash
                        != epoch.evidence_window_hash
                    ):
                        raise IncompleteFleetEvidence(
                            f"fleet closure {session_date} is outside the active forward window"
                        )
                    closures_by_date[session_date] = closure
                champion_by_date = {row.trade_date: row for row in champion_rows}
                challenger_by_date = {
                    row.trade_date: row for row in challenger_rows
                }
                for session_date, closure in closures_by_date.items():
                    members = {
                        str(member["binding_id"]): member
                        for member in closure.members
                    }
                    for role, rows_by_date in (
                        (ShadowRole.CHAMPION, champion_by_date),
                        (ShadowRole.CHALLENGER, challenger_by_date),
                    ):
                        row = rows_by_date.get(session_date)
                        if row is None:
                            continue
                        binding_id = str(binding_rows[role].binding_id)
                        member = members.get(binding_id)
                        if (
                            member is None
                            or str(member["session_hash"]) != str(row.session_hash)
                            or str(member["account_event_hash"])
                            != str(row.account_event_hash)
                        ):
                            raise IncompleteFleetEvidence(
                                f"fleet closure {session_date} does not bind the aligned {role.value} session"
                            )
            if len(champion_rows) < minimum_sessions:
                raise InsufficientForwardEvidence(
                    f"formal comparison requires {minimum_sessions} common forward sessions; "
                    f"found {len(champion_rows)}"
                )
            if self._enforce_realtime:
                observed_dates = tuple(
                    date.fromisoformat(value) for value in champion_dates
                )
                try:
                    positions = tuple(
                        accepted_sessions.index(value) for value in observed_dates
                    )
                except ValueError as exc:
                    raise MisalignedForwardEvidence(
                        "forward projections contain a non-calendar date"
                    ) from exc
                if any(
                    current != previous + 1
                    for previous, current in zip(positions, positions[1:])
                ):
                    raise MisalignedForwardEvidence(
                        "forward projections skip an accepted trading session"
                    )
                for binding in binding_rows.values():
                    if (
                        not bool(binding.active)
                        or _db_aware(binding.bound_at)
                        >= self._session_open(observed_dates[0])
                    ):
                        raise ShadowRoleConflict(
                            "formal forward role binding is inactive or late"
                        )
            for champion_row, challenger_row in zip(champion_rows, challenger_rows):
                self._validate_stored_projection(champion_row)
                self._validate_stored_projection(challenger_row)
                for row in (champion_row, challenger_row):
                    source_event = account_events[row.account_id].get(
                        row.account_event_hash
                    )
                    if (
                        source_event is None
                        or source_event.event_type != "account_projected"
                        or int(source_event.sequence_number)
                        != int(row.account_event_sequence)
                    ):
                        raise ShadowSessionRejected(
                            "stored shadow session is detached from its event chain"
                        )
                if (
                    champion_row.epoch_id != challenger_row.epoch_id
                    or champion_row.evidence_window_hash
                    != challenger_row.evidence_window_hash
                    or date.fromisoformat(champion_row.trade_date) < first_forward
                ):
                    raise MisalignedForwardEvidence(
                        "forward projections do not share the activated epoch/window"
                    )
            champion_returns = self._returns(session, champion_id, champion_rows)
            challenger_returns = self._returns(session, challenger_id, challenger_rows)
            dates = tuple(date.fromisoformat(value) for value in champion_dates)
            data_quality_ok = session.scalar(
                select(orm.DataIncidentModel.incident_id)
                .where(orm.DataIncidentModel.status == "open")
                .limit(1)
            ) is None
            payload = {
                "epoch_id": epoch.epoch_id,
                "epoch_hash": epoch.epoch_hash,
                "evidence_window_hash": epoch.evidence_window_hash,
                "first_forward_session": first_forward.isoformat(),
                "champion_binding_hash": binding_rows[
                    ShadowRole.CHAMPION
                ].binding_hash,
                "challenger_binding_hash": binding_rows[
                    ShadowRole.CHALLENGER
                ].binding_hash,
                "sessions": [item.isoformat() for item in dates],
                "champion_session_hashes": [row.session_hash for row in champion_rows],
                "challenger_session_hashes": [
                    row.session_hash for row in challenger_rows
                ],
                "account_chain_tips": dict(sorted(account_chain_tips.items())),
                "fleet_closure_hashes": [
                    closures_by_date[row.trade_date].closure_hash
                    for row in champion_rows
                    if row.trade_date in closures_by_date
                ],
                "champion_returns": champion_returns,
                "challenger_returns": challenger_returns,
                "data_quality_ok": data_quality_ok,
            }
            evidence_hash = content_fingerprint(payload, domain=_WINDOW_HASH_DOMAIN)
            return AlignedForwardEvidence(
                epoch_id=epoch.epoch_id,
                epoch_hash=epoch.epoch_hash,
                evidence_window_hash=epoch.evidence_window_hash,
                first_forward_session=first_forward,
                champion_binding=self._binding(binding_rows[ShadowRole.CHAMPION]),
                challenger_binding=self._binding(binding_rows[ShadowRole.CHALLENGER]),
                sessions=dates,
                champion_returns=champion_returns,
                challenger_returns=challenger_returns,
                champion_session_hashes=tuple(
                    row.session_hash for row in champion_rows
                ),
                challenger_session_hashes=tuple(
                    row.session_hash for row in challenger_rows
                ),
                evidence_hash=evidence_hash,
                data_quality_ok=data_quality_ok,
            )


__all__ = [
    "AlignedForwardEvidence",
    "IncompleteFleetEvidence",
    "InsufficientForwardEvidence",
    "MisalignedForwardEvidence",
    "ShadowAuthorityError",
    "ShadowEvidenceAuthority",
    "ShadowEvidenceClass",
    "ShadowFleetClosure",
    "ShadowRole",
    "ShadowRoleBinding",
    "ShadowRoleConflict",
    "ShadowSessionProjection",
    "ShadowSessionRejected",
]
