"""Durable leases for cross-store data-incident control materialization.

An OPEN ``ros_data_incidents`` row is the immediate, fail-closed risk latch.
Catalog lifecycle/cash-intent materialization is deliberately not performed
inside the PostgreSQL transaction that created that latch.  Instead, this
outbox grants one short-lived, monotonically fenced lease.  The callback runs
after the claim transaction commits and only the current, unexpired token may
publish the terminal action result.

The callback itself must use the catalog's existing idempotency keys.  A worker
can die after committing part (or all) of those effects; an expired lease then
lets another worker replay the callback without changing action authority.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
import json
import re
from typing import Any, Mapping

from sqlalchemy import func, or_, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from . import orm
from .fingerprint import canonical_json, content_fingerprint


_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}$")


class IncidentControlOutboxError(RuntimeError):
    """Base class for durable incident-control action failures."""


class IncidentControlLeaseConflict(IncidentControlOutboxError):
    """The action is leased by another worker or this token lost authority."""


class IncidentControlActionKind(str, Enum):
    FREEZE_FLEET = "freeze_fleet"
    REVALIDATE_INCIDENT = "revalidate_incident"


class IncidentControlActionStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"


@dataclass(frozen=True)
class IncidentControlAction:
    action_id: str
    incident_id: str
    action_kind: IncidentControlActionKind
    status: IncidentControlActionStatus
    attempts: int
    fencing_token: int
    created_at: datetime
    updated_at: datetime
    lease_owner: str | None = None
    lease_token: str | None = None
    lease_expires_at: datetime | None = None
    result_hash: str | None = None
    result: Mapping[str, Any] = field(default_factory=dict)
    last_error_code: str | None = None
    completed_at: datetime | None = None


@dataclass(frozen=True)
class IncidentControlLease:
    action: IncidentControlAction

    @property
    def token(self) -> str:
        token = self.action.lease_token
        if self.action.status is not IncidentControlActionStatus.RUNNING or not token:
            raise IncidentControlLeaseConflict("control action is not actively leased")
        return token

    @property
    def fencing_token(self) -> int:
        return self.action.fencing_token


def _aware(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    return value.astimezone(timezone.utc)


def _db_aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _identifier(value: str, name: str) -> str:
    normalized = str(value or "").strip()
    if not _SAFE_ID.fullmatch(normalized):
        raise ValueError(f"{name} contains unsupported characters or length")
    return normalized


def _action_id(incident_id: str, action_kind: IncidentControlActionKind) -> str:
    digest = content_fingerprint(
        {"incident_id": incident_id, "action_kind": action_kind.value},
        domain="factor-lab/research-os/v1/incident-control-action",
    )
    return f"ica_{digest[:64]}"


class IncidentControlOutbox:
    """Short SQL transactions surrounding an out-of-transaction callback."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._sessions = sessionmaker(
            bind=engine, expire_on_commit=False, class_=Session
        )

    @staticmethod
    def _database_now(session: Session) -> datetime:
        value = session.scalar(select(func.now()))
        if not isinstance(value, datetime):  # pragma: no cover - supported DBs return it.
            raise IncidentControlOutboxError("database clock returned no timestamp")
        return _db_aware(value)  # type: ignore[return-value]

    @classmethod
    def _lease_timestamp(
        cls,
        session: Session,
        supplied: datetime | None,
        *,
        name: str,
    ) -> datetime:
        """Use only the database clock for PostgreSQL lease authority.

        SQLite is test-only and keeps an injectable timestamp for deterministic
        expiry tests.  PostgreSQL callers may supply a timestamp for API
        compatibility, but it is validated and ignored; neither a backdated
        completion nor a future claim can alter production fencing.
        """

        if supplied is not None:
            supplied = _aware(supplied, name)
        database_now = cls._database_now(session)
        if session.get_bind().dialect.name == "postgresql" or supplied is None:
            return database_now
        return supplied

    @staticmethod
    def _lock_incident_then_action(
        session: Session,
        *,
        incident_id: str,
        action_id: str,
    ) -> tuple[Any | None, Any | None]:
        """Acquire every two-row control lock in canonical incident→action order."""

        incident_query = select(orm.DataIncidentModel).where(
            orm.DataIncidentModel.incident_id == incident_id
        )
        action_query = select(orm.IncidentControlActionModel).where(
            orm.IncidentControlActionModel.action_id == action_id
        )
        if session.get_bind().dialect.name == "postgresql":
            incident_query = incident_query.with_for_update()
            action_query = action_query.with_for_update()
        incident = session.scalar(incident_query)
        action = session.scalar(action_query)
        return incident, action

    @staticmethod
    def _record(model: Any) -> IncidentControlAction:
        return IncidentControlAction(
            action_id=model.action_id,
            incident_id=model.incident_id,
            action_kind=IncidentControlActionKind(model.action_kind),
            status=IncidentControlActionStatus(model.status),
            attempts=int(model.attempts),
            fencing_token=int(model.fencing_token),
            lease_owner=model.lease_owner,
            lease_token=model.lease_token,
            lease_expires_at=_db_aware(model.lease_expires_at),
            result_hash=model.result_hash,
            result=dict(model.result_json or {}),
            last_error_code=model.last_error_code,
            created_at=_db_aware(model.created_at),  # type: ignore[arg-type]
            updated_at=_db_aware(model.updated_at),  # type: ignore[arg-type]
            completed_at=_db_aware(model.completed_at),
        )

    @staticmethod
    def _lease_model(
        model: Any,
        *,
        owner: str,
        claimed_at: datetime,
        lease_for: timedelta,
    ) -> IncidentControlLease:
        fence = int(model.fencing_token) + 1
        expires_at = claimed_at + lease_for
        lease_token = content_fingerprint(
            {
                "action_id": model.action_id,
                "owner": owner,
                "fencing_token": fence,
                "claimed_at": claimed_at,
                "expires_at": expires_at,
            },
            domain="factor-lab/research-os/v1/incident-control-lease",
        )
        model.status = IncidentControlActionStatus.RUNNING.value
        model.attempts = int(model.attempts) + 1
        model.fencing_token = fence
        model.lease_owner = owner
        model.lease_token = lease_token
        model.lease_expires_at = expires_at
        model.last_error_code = None
        model.updated_at = claimed_at
        return IncidentControlLease(IncidentControlOutbox._record(model))

    @staticmethod
    def enqueue_in_session(
        session: Session,
        *,
        incident_id: str,
        created_at: datetime,
        action_kind: IncidentControlActionKind | str = (
            IncidentControlActionKind.FREEZE_FLEET
        ),
    ) -> IncidentControlAction:
        """Create one action in the incident reservation transaction."""

        normalized_incident = _identifier(incident_id, "incident_id")
        kind = IncidentControlActionKind(action_kind)
        timestamp = _aware(created_at, "created_at")
        action_id = _action_id(normalized_incident, kind)
        values = {
            "action_id": action_id,
            "incident_id": normalized_incident,
            "action_kind": kind.value,
            "status": IncidentControlActionStatus.PENDING.value,
            "attempts": 0,
            "fencing_token": 0,
            "result_json": {},
            "created_at": timestamp,
            "updated_at": timestamp,
        }
        dialect = session.get_bind().dialect.name
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as dialect_insert

            statement = dialect_insert(orm.IncidentControlActionModel).values(**values)
            session.execute(
                statement.on_conflict_do_nothing(
                    index_elements=[
                        orm.IncidentControlActionModel.incident_id,
                        orm.IncidentControlActionModel.action_kind,
                    ]
                )
            )
        elif dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as dialect_insert

            statement = dialect_insert(orm.IncidentControlActionModel).values(**values)
            session.execute(
                statement.on_conflict_do_nothing(
                    index_elements=[
                        orm.IncidentControlActionModel.incident_id,
                        orm.IncidentControlActionModel.action_kind,
                    ]
                )
            )
        else:  # pragma: no cover - production is PG; unit tests use SQLite.
            try:
                with session.begin_nested():
                    session.add(orm.IncidentControlActionModel(**values))
                    session.flush()
            except IntegrityError:
                pass
        model = session.scalar(
            select(orm.IncidentControlActionModel).where(
                orm.IncidentControlActionModel.incident_id == normalized_incident,
                orm.IncidentControlActionModel.action_kind == kind.value,
            )
        )
        if model is None:
            raise IncidentControlOutboxError(
                "control action conflict did not resolve to an authority row"
            )
        if model.action_id != action_id:
            raise IncidentControlOutboxError("control action identity is inconsistent")
        return IncidentControlOutbox._record(model)

    def get(
        self,
        incident_id: str,
        *,
        action_kind: IncidentControlActionKind | str = (
            IncidentControlActionKind.FREEZE_FLEET
        ),
    ) -> IncidentControlAction | None:
        normalized_incident = _identifier(incident_id, "incident_id")
        kind = IncidentControlActionKind(action_kind)
        with self._sessions() as session:
            model = session.scalar(
                select(orm.IncidentControlActionModel).where(
                    orm.IncidentControlActionModel.incident_id == normalized_incident,
                    orm.IncidentControlActionModel.action_kind == kind.value,
                )
            )
            return None if model is None else self._record(model)

    def claim(
        self,
        incident_id: str,
        *,
        owner: str,
        lease_for: timedelta,
        now: datetime | None = None,
        action_kind: IncidentControlActionKind | str = (
            IncidentControlActionKind.FREEZE_FLEET
        ),
    ) -> IncidentControlLease | None:
        normalized_incident = _identifier(incident_id, "incident_id")
        normalized_owner = _identifier(owner, "owner")
        kind = IncidentControlActionKind(action_kind)
        if lease_for <= timedelta(0) or lease_for > timedelta(hours=1):
            raise ValueError("lease_for must be positive and at most one hour")
        with self._sessions.begin() as session:
            if session.get_bind().dialect.name == "sqlite":
                session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            timestamp = self._lease_timestamp(
                session,
                now,
                name="now",
            )
            incident_query = select(orm.DataIncidentModel).where(
                orm.DataIncidentModel.incident_id == normalized_incident
            )
            action_query = select(orm.IncidentControlActionModel).where(
                orm.IncidentControlActionModel.incident_id == normalized_incident,
                orm.IncidentControlActionModel.action_kind == kind.value,
            )
            if session.get_bind().dialect.name == "postgresql":
                incident_query = incident_query.with_for_update()
                action_query = action_query.with_for_update()
            incident = session.scalar(incident_query)
            if incident is None:
                raise IncidentControlOutboxError("data incident was not found")
            if str(incident.status) != "open":
                raise IncidentControlLeaseConflict(
                    "terminal data incidents cannot materialize controls"
                )
            model = session.scalar(action_query)
            if model is None:
                self.enqueue_in_session(
                    session,
                    incident_id=normalized_incident,
                    created_at=timestamp,
                    action_kind=kind,
                )
                model = session.scalar(action_query)
            if model is None:  # pragma: no cover - enqueue verifies this.
                raise IncidentControlOutboxError("control action was not created")
            status = IncidentControlActionStatus(model.status)
            lease_expires_at = _db_aware(model.lease_expires_at)
            if status is IncidentControlActionStatus.SUCCEEDED:
                return None
            if (
                status is IncidentControlActionStatus.RUNNING
                and lease_expires_at is not None
                and lease_expires_at > timestamp
            ):
                return None
            lease = self._lease_model(
                model,
                owner=normalized_owner,
                claimed_at=timestamp,
                lease_for=lease_for,
            )
            session.flush()
            return lease

    def claim_next(
        self,
        *,
        owner: str,
        lease_for: timedelta,
        now: datetime | None = None,
        action_kind: IncidentControlActionKind | str = (
            IncidentControlActionKind.FREEZE_FLEET
        ),
    ) -> IncidentControlLease | None:
        """Atomically claim the oldest pending or expired OPEN-incident action.

        PostgreSQL workers use ``SKIP LOCKED`` so independent resume workers do
        not wait on each other.  SQLite takes its normal single-writer
        reservation.  SUCCEEDED actions and actions belonging to terminal
        incidents are never rediscovered.
        """

        normalized_owner = _identifier(owner, "owner")
        kind = IncidentControlActionKind(action_kind)
        if lease_for <= timedelta(0) or lease_for > timedelta(hours=1):
            raise ValueError("lease_for must be positive and at most one hour")
        with self._sessions.begin() as session:
            dialect = session.get_bind().dialect.name
            if dialect == "sqlite":
                session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            timestamp = self._lease_timestamp(
                session,
                now,
                name="now",
            )
            query = (
                select(orm.IncidentControlActionModel)
                .join(
                    orm.DataIncidentModel,
                    orm.DataIncidentModel.incident_id
                    == orm.IncidentControlActionModel.incident_id,
                )
                .where(
                    orm.DataIncidentModel.status == "open",
                    orm.IncidentControlActionModel.action_kind == kind.value,
                    or_(
                        orm.IncidentControlActionModel.status
                        == IncidentControlActionStatus.PENDING.value,
                        (
                            orm.IncidentControlActionModel.status
                            == IncidentControlActionStatus.RUNNING.value
                        )
                        & (
                            orm.IncidentControlActionModel.lease_expires_at
                            <= timestamp
                        ),
                    ),
                )
                .order_by(
                    orm.IncidentControlActionModel.created_at,
                    orm.IncidentControlActionModel.action_id,
                )
                .limit(1)
            )
            if dialect == "postgresql":
                query = query.with_for_update(
                    skip_locked=True,
                    of=orm.IncidentControlActionModel,
                )
            model = session.scalar(query)
            if model is None:
                return None
            lease = self._lease_model(
                model,
                owner=normalized_owner,
                claimed_at=timestamp,
                lease_for=lease_for,
            )
            session.flush()
            return lease

    def complete(
        self,
        lease: IncidentControlLease,
        *,
        result: Mapping[str, Any],
        completed_at: datetime | None = None,
    ) -> IncidentControlAction:
        try:
            normalized_result = json.loads(canonical_json(dict(result)))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("result must be canonical JSON evidence") from exc
        if not isinstance(normalized_result, dict):
            raise ValueError("result must normalize to a JSON object")
        result_hash = content_fingerprint(
            {
                "action_id": lease.action.action_id,
                "fencing_token": lease.fencing_token,
                "result": normalized_result,
            },
            domain="factor-lab/research-os/v1/incident-control-result",
        )
        with self._sessions.begin() as session:
            if session.get_bind().dialect.name == "sqlite":
                session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            timestamp = self._lease_timestamp(
                session,
                completed_at,
                name="completed_at",
            )
            incident, model = self._lock_incident_then_action(
                session,
                incident_id=lease.action.incident_id,
                action_id=lease.action.action_id,
            )
            if model is None:
                raise IncidentControlLeaseConflict("control action was not found")
            if not (
                model.status == IncidentControlActionStatus.RUNNING.value
                and model.lease_owner == lease.action.lease_owner
                and model.lease_token == lease.token
                and int(model.fencing_token) == lease.fencing_token
                and _db_aware(model.lease_expires_at) is not None
                and timestamp <= _db_aware(model.lease_expires_at)
            ):
                raise IncidentControlLeaseConflict(
                    "control action lease is stale or expired"
                )
            if incident is None or str(incident.status) != "open":
                raise IncidentControlLeaseConflict(
                    "control action lost its OPEN incident authority"
                )
            model.status = IncidentControlActionStatus.SUCCEEDED.value
            model.lease_owner = None
            model.lease_token = None
            model.lease_expires_at = None
            model.result_hash = result_hash
            model.result_json = normalized_result
            model.last_error_code = None
            model.completed_at = timestamp
            model.updated_at = timestamp
            session.flush()
            return self._record(model)

    def release(
        self,
        lease: IncidentControlLease,
        *,
        error_code: str,
        released_at: datetime | None = None,
    ) -> bool:
        """Return the current token to PENDING; stale workers cannot alter it."""

        code = _identifier(error_code, "error_code")
        with self._sessions.begin() as session:
            if session.get_bind().dialect.name == "sqlite":
                session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            timestamp = self._lease_timestamp(
                session,
                released_at,
                name="released_at",
            )
            query = select(orm.IncidentControlActionModel).where(
                orm.IncidentControlActionModel.action_id == lease.action.action_id
            )
            if session.get_bind().dialect.name == "postgresql":
                query = query.with_for_update()
            model = session.scalar(query)
            if model is None or not (
                model.status == IncidentControlActionStatus.RUNNING.value
                and model.lease_owner == lease.action.lease_owner
                and model.lease_token == lease.token
                and int(model.fencing_token) == lease.fencing_token
            ):
                return False
            model.status = IncidentControlActionStatus.PENDING.value
            model.lease_owner = None
            model.lease_token = None
            model.lease_expires_at = None
            model.last_error_code = code
            model.updated_at = timestamp
            session.flush()
            return True


__all__ = [
    "IncidentControlAction",
    "IncidentControlActionKind",
    "IncidentControlActionStatus",
    "IncidentControlLease",
    "IncidentControlLeaseConflict",
    "IncidentControlOutbox",
    "IncidentControlOutboxError",
]
