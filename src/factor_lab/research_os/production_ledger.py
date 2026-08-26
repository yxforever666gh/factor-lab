"""Authoritative production ledgers for resumable data and shadow operations.

The historical scripts used artifact presence as a progress signal.  This
module deliberately does not inspect the filesystem: PostgreSQL records every
source/dataset/session lease and terminal outcome.  Successful partitions are
immutable, expired leases are safely reclaimable, and all terminal writes use
the lease token acquired by the worker.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
import json
import re
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence

from .fingerprint import canonical_json, content_fingerprint

try:  # Keep lightweight analysis workers importable without infra extras.
    from sqlalchemy import (
        MetaData,
        Table,
        and_,
        create_engine,
        func,
        inspect,
        or_,
        select,
        update,
    )
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.orm import Session, sessionmaker

    from . import orm
    from .incident_control_outbox import (
        IncidentControlActionKind,
        IncidentControlActionStatus,
        IncidentControlLeaseConflict,
        IncidentControlOutbox,
    )
except ImportError:  # pragma: no cover - exercised in minimal environments.
    Engine = Any  # type: ignore[misc,assignment]
    Session = Any  # type: ignore[misc,assignment]
    _SQLALCHEMY_AVAILABLE = False
else:
    _SQLALCHEMY_AVAILABLE = True


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_PARTITION_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,31}$")
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}$")
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(api[_-]?key|token|password|secret|authorization)\s*[:=]\s*([^\s,;]+)"
)
_AUTHORIZATION_BEARER = re.compile(
    r"(?i)\b(authorization\s*[:=]\s*bearer)\s+([^\s,;\"'}]+)"
)
_BARE_BEARER = re.compile(r"(?i)\bbearer\s+([A-Za-z0-9._~+/=-]{8,})")
_QUOTED_SECRET_ASSIGNMENT = re.compile(
    r"(?i)([\"'](?:api[_-]?key|token|password|secret|authorization)[\"']"
    r"\s*:\s*)[\"']([^\"']*)[\"']"
)
_URL_CREDENTIAL = re.compile(r"(https?://[^:/\s]+:)[^@/\s]+@", re.IGNORECASE)
RUNTIME_AUTHORITY_MARKER_KEY = "research_os"
RUNTIME_AUTHORITY_SCHEMA = "research-os/runtime-authority/v1"
RUNTIME_AUTHORITY_HASH_DOMAIN = (
    "factor-lab/research-os/v1/runtime-authority-marker"
)
# Serialize the small, safety-critical incident control plane independently
# from ordinary partition writes.  The two-key PostgreSQL advisory lock is
# transaction scoped; SQLite obtains the equivalent single-writer reservation
# in the mutating methods below.
INCIDENT_CONTROL_LOCK_KEYS = (0x46414354, 0x4F524C42)  # "FACTORLB"


class ProductionLedgerError(RuntimeError):
    pass


class LeaseConflict(ProductionLedgerError):
    pass


class TypedEffectFenceConflict(LeaseConflict):
    """Catalog effects changed before typed incident terminalization."""


class ImmutablePartition(ProductionLedgerError):
    pass


class RuntimeAuthorityError(ProductionLedgerError):
    """The database authority marker is present but invalid or ambiguous."""


class PartitionStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    DISPUTED = "disputed"
    QUARANTINED = "quarantined"
    FAILED = "failed"


class CapabilityStatus(str, Enum):
    ACCEPTED = "accepted"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"
    DISPUTED = "disputed"


class IncidentStatus(str, Enum):
    OPEN = "open"
    RESOLVED = "resolved"
    SUPERSEDED = "superseded"


class IncidentStage(str, Enum):
    SOURCE = "source"
    SILVER = "silver"
    DATA_QUALITY = "data_quality"
    GOLD = "gold"
    SHADOW_EXECUTION = "shadow_execution"


_TERMINAL_PARTITION_STATUSES = {
    PartitionStatus.SUCCEEDED,
    PartitionStatus.DISPUTED,
    PartitionStatus.QUARANTINED,
    PartitionStatus.FAILED,
}

_REPAIR_STAGE_DATASETS: tuple[tuple[str, IncidentStage], ...] = (
    ("stage_source", IncidentStage.SOURCE),
    ("stage_silver", IncidentStage.SILVER),
    ("stage_data_quality", IncidentStage.DATA_QUALITY),
    ("stage_gold", IncidentStage.GOLD),
    ("stage_shadow", IncidentStage.SHADOW_EXECUTION),
)
_REPAIR_STAGE_INDEX = {
    dataset: index for index, (dataset, _stage) in enumerate(_REPAIR_STAGE_DATASETS)
}
_REPAIR_STAGE_FOR_DATASET = dict(_REPAIR_STAGE_DATASETS)
_BASE_PARTITION_GENERATION = "base"


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


def _safe_identifier(value: str, name: str) -> str:
    normalized = str(value or "").strip()
    if not _SAFE_ID.fullmatch(normalized):
        raise ValueError(f"{name} contains unsupported characters or length")
    return normalized


def _safe_partition_key(value: str) -> str:
    normalized = str(value or "").strip()
    if not _PARTITION_KEY.fullmatch(normalized):
        raise ValueError("partition_key contains unsupported characters or length")
    return normalized


def _sha256(value: str | None, name: str, *, required: bool = False) -> str | None:
    normalized = str(value or "").strip().lower()
    if not normalized and not required:
        return None
    if not _SHA256.fullmatch(normalized):
        raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")
    return normalized


def _canonical_json_mapping(
    value: Mapping[str, Any] | None,
    *,
    name: str,
) -> dict[str, Any]:
    """Normalize one mapping to the exact JSON tree persisted by the ORM."""

    try:
        normalized = json.loads(canonical_json(dict(value or {})))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"{name} is not canonical JSON evidence") from exc
    if not isinstance(normalized, dict):  # Defensive: the input contract is a mapping.
        raise ValueError(f"{name} must normalize to a JSON object")
    return normalized


def sanitize_operational_text(
    value: str,
    *,
    sensitive_values: Sequence[str] = (),
    maximum_length: int = 2_000,
) -> str:
    """Redact common credential forms before text reaches PostgreSQL/logs."""

    text = str(value or "").replace("\x00", "")
    for secret in sorted(
        {str(item) for item in sensitive_values if str(item)}, key=len, reverse=True
    ):
        text = text.replace(secret, "***")
    # Apply bearer/quoted forms before the generic assignment expression.  A
    # naive ``Authorization: ...`` replacement otherwise consumes only the
    # word ``Bearer`` and leaves the credential behind.
    text = _AUTHORIZATION_BEARER.sub(lambda match: f"{match.group(1)} ***", text)
    text = _BARE_BEARER.sub("Bearer ***", text)
    text = _QUOTED_SECRET_ASSIGNMENT.sub(
        lambda match: f'{match.group(1)}"***"', text
    )
    text = _SECRET_ASSIGNMENT.sub(lambda match: f"{match.group(1)}=***", text)
    text = _URL_CREDENTIAL.sub(r"\1***@", text)
    return text[:maximum_length]


@dataclass(frozen=True)
class PartitionIdentity:
    source_id: str
    dataset: str
    partition_key: str
    generation: str = _BASE_PARTITION_GENERATION

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_id", _safe_identifier(self.source_id, "source_id"))
        object.__setattr__(self, "dataset", _safe_identifier(self.dataset, "dataset"))
        object.__setattr__(self, "partition_key", _safe_partition_key(self.partition_key))
        object.__setattr__(
            self,
            "generation",
            _safe_identifier(self.generation, "generation"),
        )

    @property
    def partition_run_id(self) -> str:
        payload = {
            "source_id": self.source_id,
            "dataset": self.dataset,
            "partition_key": self.partition_key,
        }
        # Preserve every pre-generation production identity exactly.  Only a
        # server-reserved repair successor enters the new hash domain.
        if self.generation == _BASE_PARTITION_GENERATION:
            domain = "factor-lab/research-os/v1/partition-run"
        else:
            payload["generation"] = self.generation
            domain = "factor-lab/research-os/v2/partition-run-generation"
        digest = content_fingerprint(payload, domain=domain)
        return f"partition_{digest[:64]}"


@dataclass(frozen=True)
class PartitionRecord:
    identity: PartitionIdentity
    status: PartitionStatus
    attempts: int
    created_at: datetime
    updated_at: datetime
    lease_owner: str | None = None
    lease_token: str | None = None
    lease_expires_at: datetime | None = None
    run_id: str | None = None
    output_snapshot_id: str | None = None
    input_hash: str | None = None
    output_hash: str | None = None
    vendor_revision: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)
    error_code: str | None = None
    error: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    repair_incident_id: str | None = None
    repair_parent_partition_run_id: str | None = None
    repair_parent_hash: str | None = None
    repair_fingerprint: str | None = None


@dataclass(frozen=True)
class PartitionRepairAuthority:
    authority_id: str
    scope_key: str
    incident_id: str | None
    identity: PartitionIdentity
    parent_partition_run_id: str
    parent_terminal_hash: str
    repair_fingerprint: str
    created_at: datetime


@dataclass(frozen=True)
class PartitionLease:
    record: PartitionRecord

    @property
    def identity(self) -> PartitionIdentity:
        return self.record.identity

    @property
    def token(self) -> str:
        if self.record.status is not PartitionStatus.RUNNING or not self.record.lease_token:
            raise LeaseConflict("record is not an active lease")
        return self.record.lease_token


@dataclass(frozen=True)
class BackfillProgress:
    total: int
    counts: Mapping[str, int]

    @property
    def completed(self) -> int:
        return int(self.counts.get(PartitionStatus.SUCCEEDED.value, 0))

    @property
    def completion_ratio(self) -> float:
        return 0.0 if self.total == 0 else self.completed / self.total


@dataclass(frozen=True)
class RuntimeAuthorityMarker:
    marker_key: str
    environment: str
    authority_schema: str
    marker_hash: str
    installed_at: datetime
    database_dialect: str

    @property
    def is_production(self) -> bool:
        return (
            self.database_dialect == "postgresql"
            and self.marker_key == RUNTIME_AUTHORITY_MARKER_KEY
            and self.environment == "production"
            and self.authority_schema == RUNTIME_AUTHORITY_SCHEMA
            and self.marker_hash
            == runtime_authority_marker_hash(environment="production")
        )


def runtime_authority_marker_hash(*, environment: str) -> str:
    selected = str(environment or "").strip().lower()
    if selected not in {"production", "test"}:
        raise ValueError("runtime authority environment must be production or test")
    return content_fingerprint(
        {
            "marker_key": RUNTIME_AUTHORITY_MARKER_KEY,
            "environment": selected,
            "authority_schema": RUNTIME_AUTHORITY_SCHEMA,
        },
        domain=RUNTIME_AUTHORITY_HASH_DOMAIN,
    )


@dataclass(frozen=True)
class CapabilityRecord:
    source_id: str
    dataset: str
    status: CapabilityStatus
    contract_hash: str
    fields: tuple[str, ...]
    detail: str
    probed_at: datetime
    probe_hash: str | None = None


@dataclass(frozen=True)
class IncidentRecord:
    incident_id: str
    incident_hash: str
    partition_key: str
    stage: IncidentStage
    status: IncidentStatus
    error_code: str
    message: str
    occurred_at: datetime
    source_ids: tuple[str, ...] = ()
    evidence_hashes: tuple[str, ...] = ()
    payload: Mapping[str, Any] = field(default_factory=dict)
    partition_run_id: str | None = None
    resolved_at: datetime | None = None
    resolution_hash: str | None = None


class ProductionLedger:
    """Transactional SQLAlchemy store shared by CLI, Dagster and WebUI."""

    def __init__(
        self,
        database: str | Engine,
        *,
        connect_args: Mapping[str, Any] | None = None,
    ) -> None:
        if not _SQLALCHEMY_AVAILABLE:
            raise RuntimeError("SQLAlchemy is required for the production ledger")
        self._owns_engine = isinstance(database, str)
        if not self._owns_engine and connect_args:
            raise ValueError("connect_args cannot be supplied with an existing Engine")
        self.engine = (
            create_engine(
                database,
                pool_pre_ping=True,
                connect_args=dict(connect_args or {}),
            )
            if self._owns_engine
            else database
        )
        self._session = sessionmaker(
            bind=self.engine, expire_on_commit=False, class_=Session
        )
        self.incident_controls = IncidentControlOutbox(self.engine)

    def close(self) -> None:
        if self._owns_engine:
            self.engine.dispose()

    def __enter__(self) -> "ProductionLedger":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    @staticmethod
    def _serialize_incident_mutations(session: Session) -> None:
        """Serialize incident reservations and terminal transitions on PG."""

        if session.get_bind().dialect.name == "postgresql":
            connection = session.connection()
            # No Python/catalog callback is allowed under this lock.  It only
            # orders short incident inserts/final CAS writes/role checks, so a
            # small database timeout is sufficient and fail-closed.
            connection.exec_driver_sql("SET LOCAL lock_timeout = '5s'")
            connection.exec_driver_sql("SET LOCAL statement_timeout = '15s'")
            session.execute(
                select(
                    func.pg_advisory_xact_lock(*INCIDENT_CONTROL_LOCK_KEYS)
                )
            )

    @contextmanager
    def incident_control_guard(
        self,
        incident_id: str,
        *,
        owner: str | None = None,
        lease_for: timedelta = timedelta(minutes=5),
    ) -> Iterator[IncidentRecord]:
        """Materialize controls under an outbox lease, never a callback transaction.

        The OPEN incident was committed before this method is entered and is
        therefore already the fail-closed production latch.  Only the claim
        and completion are SQL transactions.  Catalog callbacks execute while
        no ledger transaction/advisory lock is held.  A crash leaves a RUNNING
        action that can be reclaimed after ``lease_for``; the old fencing token
        cannot publish success.

        Completed actions still yield once so callers may verify their
        idempotently materialized catalog evidence.  They do not acquire a new
        action lease and cannot change the stored result authority.
        """

        normalized_owner = owner or f"incident-control-{incident_id[-64:]}"
        with self._session() as session:
            model = session.scalar(
                select(orm.DataIncidentModel).where(
                    orm.DataIncidentModel.incident_id == incident_id
                )
            )
            if model is None:
                raise ProductionLedgerError("data incident was not found")
            authority = self._incident_record(model)
        if authority.status is not IncidentStatus.OPEN:
            raise ImmutablePartition(
                "data incident controls require an OPEN authority"
            )

        try:
            lease = self.incident_controls.claim(
                incident_id,
                owner=normalized_owner,
                lease_for=lease_for,
            )
        except IncidentControlLeaseConflict as exc:
            raise LeaseConflict(str(exc)) from exc

        if lease is None:
            action = self.incident_controls.get(incident_id)
            if (
                action is not None
                and action.status is IncidentControlActionStatus.SUCCEEDED
            ):
                yield authority
                return
            raise LeaseConflict("data incident controls are already leased")

        try:
            yield authority
        except BaseException as exc:
            # Persist only the exception class, never provider/operator text.
            # If the token expired or was reclaimed, release simply returns
            # False and cannot disturb the newer worker.
            try:
                self.incident_controls.release(
                    lease,
                    error_code=type(exc).__name__,
                )
            except Exception:
                pass
            raise
        try:
            self.incident_controls.complete(
                lease,
                result={
                    "incident_id": incident_id,
                    "control_state": "materialized",
                },
            )
        except IncidentControlLeaseConflict as exc:
            raise LeaseConflict(str(exc)) from exc

    def resume_incident_controls(
        self,
        *,
        owner: str,
        apply_effects: Callable[[IncidentRecord], Mapping[str, Any]],
        limit: int = 100,
        lease_for: timedelta = timedelta(minutes=5),
    ) -> tuple[Any, ...]:
        """Drain recoverable incident controls through fenced outbox leases.

        This is the process-restart entry point used by a sensor/monitor.  It
        discovers both PENDING actions and RUNNING actions whose worker lease
        expired.  The catalog callback is outside all ledger transactions and
        must be idempotent; completion is accepted only from the current
        fencing token.
        """

        if not callable(apply_effects):
            raise TypeError("apply_effects must be callable")
        if limit <= 0 or limit > 1_000:
            raise ValueError("limit must be between 1 and 1000")
        completed: list[Any] = []
        for _ in range(limit):
            lease = self.incident_controls.claim_next(
                owner=owner,
                lease_for=lease_for,
            )
            if lease is None:
                break
            with self._session() as session:
                model = session.get(
                    orm.DataIncidentModel, lease.action.incident_id
                )
                if model is None:
                    try:
                        self.incident_controls.release(
                            lease, error_code="IncidentAuthorityMissing"
                        )
                    finally:
                        raise ProductionLedgerError(
                            "control action incident authority was not found"
                        )
                authority = self._incident_record(model)
            try:
                result = apply_effects(authority)
                completed.append(
                    self.incident_controls.complete(lease, result=result)
                )
            except BaseException as exc:
                try:
                    self.incident_controls.release(
                        lease,
                        error_code=type(exc).__name__,
                    )
                except Exception:
                    pass
                raise
        return tuple(completed)

    def runtime_authority_marker(self) -> RuntimeAuthorityMarker | None:
        """Return the Alembic-owned authority marker after strict validation.

        An absent table means the database has not yet been upgraded to the
        authority-marker revision.  Once the table exists, however, every
        ambiguity is an integrity failure: it must contain the one seeded row,
        its content hash must match, and only PostgreSQL may claim production
        authority.  In particular, callers cannot turn a SQLite test catalog
        into a production authority by inserting a convenient row.
        """

        inspector = inspect(self.engine)
        if not inspector.has_table("ros_runtime_authority"):
            return None
        metadata = MetaData()
        table = Table(
            "ros_runtime_authority",
            metadata,
            autoload_with=self.engine,
        )
        required_columns = {
            "marker_key",
            "environment",
            "authority_schema",
            "marker_hash",
            "installed_at",
        }
        actual_columns = set(table.c.keys())
        missing = sorted(required_columns - actual_columns)
        if missing:
            raise RuntimeAuthorityError(
                "runtime authority marker is missing required columns: "
                + ", ".join(missing)
            )
        with self.engine.connect() as connection:
            rows = connection.execute(select(table)).mappings().all()
        if len(rows) != 1:
            raise RuntimeAuthorityError(
                "runtime authority marker must contain exactly one row"
            )
        row = rows[0]
        marker_key = str(row["marker_key"] or "").strip()
        environment = str(row["environment"] or "").strip().lower()
        authority_schema = str(row["authority_schema"] or "").strip()
        marker_hash = str(row["marker_hash"] or "").strip().lower()
        installed_at = _db_aware(row["installed_at"])
        dialect = str(self.engine.dialect.name or "").strip().lower()
        if marker_key != RUNTIME_AUTHORITY_MARKER_KEY:
            raise RuntimeAuthorityError("runtime authority marker key is invalid")
        if environment not in {"production", "test"}:
            raise RuntimeAuthorityError(
                "runtime authority environment is invalid"
            )
        if authority_schema != RUNTIME_AUTHORITY_SCHEMA:
            raise RuntimeAuthorityError(
                "runtime authority schema is unsupported"
            )
        if marker_hash != runtime_authority_marker_hash(environment=environment):
            raise RuntimeAuthorityError("runtime authority marker hash is invalid")
        if installed_at is None:
            raise RuntimeAuthorityError(
                "runtime authority marker installed_at is missing"
            )
        if dialect == "postgresql" and environment != "production":
            raise RuntimeAuthorityError(
                "PostgreSQL authority marker must identify production"
            )
        if dialect != "postgresql" and environment != "test":
            raise RuntimeAuthorityError(
                "non-PostgreSQL databases cannot claim production authority"
            )
        return RuntimeAuthorityMarker(
            marker_key=marker_key,
            environment=environment,
            authority_schema=authority_schema,
            marker_hash=marker_hash,
            installed_at=installed_at,
            database_dialect=dialect,
        )

    @staticmethod
    def _partition_record(model: Any) -> PartitionRecord:
        return PartitionRecord(
            identity=PartitionIdentity(
                source_id=model.source_id,
                dataset=model.dataset,
                partition_key=model.partition_key,
                generation=model.generation,
            ),
            status=PartitionStatus(model.status),
            attempts=int(model.attempts),
            created_at=_db_aware(model.created_at),
            updated_at=_db_aware(model.updated_at),
            lease_owner=model.lease_owner,
            lease_token=model.lease_token,
            lease_expires_at=_db_aware(model.lease_expires_at),
            run_id=model.run_id,
            output_snapshot_id=model.output_snapshot_id,
            input_hash=model.input_hash,
            output_hash=model.output_hash,
            vendor_revision=model.vendor_revision,
            details=dict(model.details_json or {}),
            error_code=model.error_code,
            error=model.error,
            started_at=_db_aware(model.started_at),
            completed_at=_db_aware(model.completed_at),
            repair_incident_id=model.repair_incident_id,
            repair_parent_partition_run_id=model.repair_parent_partition_run_id,
            repair_parent_hash=model.repair_parent_hash,
            repair_fingerprint=model.repair_fingerprint,
        )

    def ensure_partition(
        self,
        identity: PartitionIdentity,
        *,
        created_at: datetime,
        input_hash: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> PartitionRecord:
        now = _aware(created_at, "created_at")
        normalized_input_hash = _sha256(input_hash, "input_hash")
        with self._session.begin() as session:
            model = session.get(orm.PartitionRunModel, identity.partition_run_id)
            if model is None:
                if identity.generation != _BASE_PARTITION_GENERATION:
                    raise ProductionLedgerError(
                        "repair partition generations require incident reservation"
                    )
                model = orm.PartitionRunModel(
                    partition_run_id=identity.partition_run_id,
                    source_id=identity.source_id,
                    dataset=identity.dataset,
                    partition_key=identity.partition_key,
                    generation=identity.generation,
                    status=PartitionStatus.PENDING.value,
                    attempts=0,
                    input_hash=normalized_input_hash,
                    details_json=dict(details or {}),
                    created_at=now,
                    updated_at=now,
                )
                session.add(model)
                try:
                    session.flush()
                except IntegrityError as exc:
                    raise ProductionLedgerError(
                        "partition identity conflicts with an existing row"
                    ) from exc
            else:
                observed = (
                    model.source_id,
                    model.dataset,
                    model.partition_key,
                    model.generation,
                )
                expected = (
                    identity.source_id,
                    identity.dataset,
                    identity.partition_key,
                    identity.generation,
                )
                if observed != expected:
                    raise ProductionLedgerError("partition_run_id identity mismatch")
                if normalized_input_hash and model.input_hash not in {
                    None,
                    normalized_input_hash,
                }:
                    raise ImmutablePartition(
                        "partition input hash changed after registration"
                    )
                if normalized_input_hash and model.input_hash is None:
                    if (
                        model.status != PartitionStatus.PENDING.value
                        or int(model.attempts) != 0
                    ):
                        raise ImmutablePartition(
                            "an attempted partition cannot acquire a late input hash"
                        )
                    # Backfill planners register the authoritative identity
                    # before a worker renders the exact source contract.  The
                    # first worker may bind that hash exactly once, before any
                    # lease or side effect; subsequent changes remain rejected.
                    model.input_hash = normalized_input_hash
                    model.updated_at = now
        return self.get_partition(identity)  # type: ignore[return-value]

    def ensure_partitions(
        self,
        identities: Iterable[PartitionIdentity],
        *,
        created_at: datetime,
    ) -> tuple[PartitionRecord, ...]:
        unique = {
            (item.source_id, item.dataset, item.partition_key, item.generation): item
            for item in identities
        }
        return tuple(
            self.ensure_partition(identity, created_at=created_at)
            for identity in sorted(
                unique.values(),
                key=lambda item: (
                    item.partition_key,
                    item.source_id,
                    item.dataset,
                    item.generation,
                ),
            )
        )

    def get_partition(self, identity: PartitionIdentity) -> PartitionRecord | None:
        with self._session() as session:
            model = session.get(orm.PartitionRunModel, identity.partition_run_id)
            return None if model is None else self._partition_record(model)

    def get_partition_by_run_id(
        self, partition_run_id: str
    ) -> PartitionRecord | None:
        selected = _safe_identifier(partition_run_id, "partition_run_id")
        with self._session() as session:
            model = session.get(orm.PartitionRunModel, selected)
            return None if model is None else self._partition_record(model)

    def get_retry_authority(
        self,
        identity: PartitionIdentity,
        *,
        scope_key: str | None = None,
    ) -> PartitionRepairAuthority | None:
        """Return the CAS-selected generic retry leaf for one logical slot."""

        if identity.generation != _BASE_PARTITION_GENERATION:
            raise ValueError("retry authority lookup requires the base identity")
        selected_scope = _safe_identifier(
            scope_key or f"retry:{identity.partition_run_id}", "scope_key"
        )
        with self._session() as session:
            rows = tuple(
                session.execute(
                    select(orm.PartitionRepairAuthorityModel).where(
                        orm.PartitionRepairAuthorityModel.scope_key
                        == selected_scope,
                        orm.PartitionRepairAuthorityModel.source_id
                        == identity.source_id,
                        orm.PartitionRepairAuthorityModel.dataset
                        == identity.dataset,
                        orm.PartitionRepairAuthorityModel.partition_key
                        == identity.partition_key,
                    )
                ).scalars()
            )
            leaf = self._repair_authority_leaf(rows)
            if leaf is None:
                return None
            authority = self._repair_authority_record(leaf)
            successor = session.get(
                orm.PartitionRunModel, authority.identity.partition_run_id
            )
            parent = session.get(
                orm.PartitionRunModel, authority.parent_partition_run_id
            )
            if successor is None or parent is None:
                raise ProductionLedgerError(
                    "retry authority points to a missing partition"
                )
            if not (
                authority.scope_key == selected_scope
                and authority.incident_id is None
                and authority.parent_terminal_hash
                == self._partition_terminal_hash(parent)
                and successor.source_id == authority.identity.source_id
                and successor.dataset == authority.identity.dataset
                and successor.partition_key == authority.identity.partition_key
                and successor.generation == authority.identity.generation
                and successor.repair_incident_id is None
                and successor.repair_parent_partition_run_id
                == authority.parent_partition_run_id
                and successor.repair_parent_hash
                == authority.parent_terminal_hash
                and successor.repair_fingerprint
                == authority.repair_fingerprint
            ):
                raise ImmutablePartition(
                    "generic retry successor disagrees with its authority"
                )
            return authority

    def get_retry_partition(
        self,
        identity: PartitionIdentity,
    ) -> PartitionRecord | None:
        authority = self.get_retry_authority(identity)
        if authority is None:
            return None
        record = self.get_partition(authority.identity)
        if record is None:
            raise ProductionLedgerError(
                "generic retry authority successor is missing"
            )
        return record

    @staticmethod
    def _partition_terminal_hash(model: Any) -> str:
        status = PartitionStatus(model.status)
        completed_at = _db_aware(model.completed_at)
        if status not in _TERMINAL_PARTITION_STATUSES or completed_at is None:
            raise ImmutablePartition(
                "repair parent must be an immutable terminal partition"
            )
        return content_fingerprint(
            {
                "partition_run_id": model.partition_run_id,
                "source_id": model.source_id,
                "dataset": model.dataset,
                "partition_key": model.partition_key,
                "generation": model.generation,
                "status": status.value,
                "attempts": int(model.attempts),
                "run_id": model.run_id,
                "output_snapshot_id": model.output_snapshot_id,
                "input_hash": model.input_hash,
                "output_hash": model.output_hash,
                "vendor_revision": model.vendor_revision,
                "details": dict(model.details_json or {}),
                "error_code": model.error_code,
                "error": model.error,
                "started_at": _db_aware(model.started_at),
                "completed_at": completed_at,
            },
            domain="factor-lab/research-os/v1/partition-terminal-authority",
        )

    @staticmethod
    def _repair_authority_record(model: Any) -> PartitionRepairAuthority:
        return PartitionRepairAuthority(
            authority_id=model.authority_id,
            scope_key=model.scope_key,
            incident_id=model.incident_id,
            identity=PartitionIdentity(
                source_id=model.source_id,
                dataset=model.dataset,
                partition_key=model.partition_key,
                generation=model.generation,
            ),
            parent_partition_run_id=model.parent_partition_run_id,
            parent_terminal_hash=model.parent_terminal_hash,
            repair_fingerprint=model.repair_fingerprint,
            created_at=_db_aware(model.created_at),
        )

    def get_incident(self, incident_id: str) -> IncidentRecord | None:
        selected = _safe_identifier(incident_id, "incident_id")
        with self._session() as session:
            model = session.get(orm.DataIncidentModel, selected)
            return None if model is None else self._incident_record(model)

    def _terminalize_incident_partition_in_session(
        self,
        session: Session,
        incident_model: Any,
    ) -> PartitionRecord:
        incident = self._incident_record(incident_model)
        if incident.status is not IncidentStatus.OPEN:
            raise ImmutablePartition(
                "incident partition terminalization requires an OPEN incident"
            )
        if not incident.partition_run_id:
            raise ProductionLedgerError(
                "incident is not bound to a production partition"
            )
        partition_query = select(orm.PartitionRunModel).where(
            orm.PartitionRunModel.partition_run_id == incident.partition_run_id
        )
        if session.get_bind().dialect.name == "postgresql":
            partition_query = partition_query.with_for_update()
        model = session.execute(partition_query).scalar_one_or_none()
        if model is None:
            raise ProductionLedgerError(
                "incident-bound production partition is missing"
            )
        expected_stage = _REPAIR_STAGE_FOR_DATASET.get(model.dataset)
        if (
            model.source_id != "research_os"
            or model.partition_key != incident.partition_key
            or expected_stage is not incident.stage
        ):
            raise ImmutablePartition(
                "incident stage does not match its production partition"
            )
        incident_payload = dict(incident.payload)
        incident_payload.pop("resolution", None)
        dagster_run_id = str(
            incident_payload.get("dagster_run_id") or ""
        ).strip()
        failed_step_key = str(
            incident_payload.get("failed_step_key") or ""
        ).strip()
        partition_details = dict(model.details_json or {})
        partition_run_id = str(
            partition_details.get("dagster_run_id") or ""
        ).strip()
        if not (dagster_run_id and failed_step_key):
            raise ImmutablePartition(
                "incident lacks durable Dagster failure lineage"
            )
        if partition_run_id != dagster_run_id:
            raise ImmutablePartition(
                "incident Dagster run differs from its partition lease lineage"
            )
        status = PartitionStatus(model.status)
        if status in {
            PartitionStatus.FAILED,
            PartitionStatus.DISPUTED,
            PartitionStatus.QUARANTINED,
        }:
            # A handler may have reached its own terminal CAS before the
            # failure sensor ran.  That immutable row is already a valid repair
            # parent, but only for the exact same durable Dagster attempt.
            return self._partition_record(model)
        if status is PartitionStatus.SUCCEEDED:
            raise ImmutablePartition(
                "a succeeded partition cannot be terminalized by an incident"
            )
        if status not in {PartitionStatus.PENDING, PartitionStatus.RUNNING}:
            raise ImmutablePartition(
                "incident partition is not an abandonable generation"
            )
        occurred_at = incident.occurred_at
        started_at = _db_aware(model.started_at)
        created_at = _db_aware(model.created_at)
        if (created_at is not None and occurred_at < created_at) or (
            started_at is not None and occurred_at < started_at
        ):
            raise ImmutablePartition(
                "incident predates the partition attempt it would fence"
            )
        if status is PartitionStatus.RUNNING and not (
            model.lease_owner and model.lease_token and model.lease_expires_at
        ):
            raise ImmutablePartition(
                "running incident partition lacks complete lease lineage"
            )
        abandoned_lease_owner = model.lease_owner
        abandoned_lease_hash = (
            None
            if status is PartitionStatus.PENDING
            else content_fingerprint(
                {
                    "partition_run_id": model.partition_run_id,
                    "lease_owner": model.lease_owner,
                    "lease_token": model.lease_token,
                    "lease_expires_at": _db_aware(model.lease_expires_at),
                    "attempt": int(model.attempts),
                    "dagster_run_id": dagster_run_id,
                    "failed_step_key": failed_step_key,
                },
                domain="factor-lab/research-os/v1/abandoned-partition-lease",
            )
        )
        model.status = PartitionStatus.FAILED.value
        model.lease_owner = None
        model.lease_token = None
        model.lease_expires_at = None
        model.details_json = {
            **partition_details,
            "incident_terminalization": {
                "schema_version": "research-os/incident-partition-terminalization/v1",
                "incident_id": incident.incident_id,
                "incident_hash": incident.incident_hash,
                "dagster_run_id": dagster_run_id,
                "failed_step_key": failed_step_key,
                "prior_status": status.value,
                "abandoned_lease_owner": abandoned_lease_owner,
                "abandoned_lease_hash": abandoned_lease_hash,
            },
        }
        model.output_snapshot_id = None
        model.output_hash = None
        model.vendor_revision = None
        model.error_code = incident.error_code
        model.error = incident.message
        model.completed_at = occurred_at
        model.updated_at = occurred_at
        session.flush()
        return self._partition_record(model)

    def terminalize_incident_partition(
        self,
        incident_id: str,
    ) -> PartitionRecord:
        """Fence a crash-left partition from its exact immutable incident."""

        selected_incident = _safe_identifier(incident_id, "incident_id")
        with self._session.begin() as session:
            dialect = session.get_bind().dialect.name
            if dialect == "sqlite":
                session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            else:
                self._serialize_incident_mutations(session)
            incident_query = select(orm.DataIncidentModel).where(
                orm.DataIncidentModel.incident_id == selected_incident
            )
            if dialect == "postgresql":
                incident_query = incident_query.with_for_update()
            incident_model = session.execute(incident_query).scalar_one_or_none()
            if incident_model is None:
                raise ProductionLedgerError(
                    "incident partition terminalization requires an incident"
                )
            return self._terminalize_incident_partition_in_session(
                session, incident_model
            )

    @staticmethod
    def _repair_authority_leaf(rows: Sequence[Any]) -> Any | None:
        if not rows:
            return None
        successor_ids = {row.successor_partition_run_id for row in rows}
        referenced = {
            row.parent_partition_run_id
            for row in rows
            if row.parent_partition_run_id in successor_ids
        }
        leaf_ids = successor_ids - referenced
        if len(leaf_ids) != 1:
            raise ProductionLedgerError(
                "repair successor authority is branched or ambiguous"
            )
        matches = [
            row for row in rows if row.successor_partition_run_id in leaf_ids
        ]
        if len(matches) != 1:
            raise ProductionLedgerError("repair successor leaf is ambiguous")
        return matches[0]

    def _global_repair_lineage_leaf(
        self,
        session: Session,
        *,
        root_partition_run_id: str,
        partition_key: str,
    ) -> Any:
        """Return the sole unconsumed leaf descended from one production root.

        ``parent_partition_run_id`` is deliberately unique across *all* repair
        scopes.  A later incident therefore has to continue after the previous
        incident's complete Source-to-Shadow chain, rather than starting a
        second branch from the same successful Source row.  Walking by parent
        (instead of selecting a per-dataset leaf) also notices that a Source
        successor has already been consumed by Silver.
        """

        current = session.get(
            orm.PartitionRunModel,
            _safe_identifier(root_partition_run_id, "root_partition_run_id"),
        )
        if current is None:
            raise ProductionLedgerError(
                "repair lineage root partition is missing"
            )
        visited: set[str] = set()
        while True:
            if current.partition_run_id in visited:
                raise ProductionLedgerError("global repair lineage contains a cycle")
            visited.add(current.partition_run_id)
            children = tuple(
                session.execute(
                    select(orm.PartitionRepairAuthorityModel).where(
                        orm.PartitionRepairAuthorityModel.parent_partition_run_id
                        == current.partition_run_id
                    )
                ).scalars()
            )
            if not children:
                return current
            if len(children) != 1:
                raise ProductionLedgerError(
                    "global repair lineage is branched or ambiguous"
                )
            child_authority = children[0]
            child = session.get(
                orm.PartitionRunModel,
                child_authority.successor_partition_run_id,
            )
            if child is None:
                raise ProductionLedgerError(
                    "global repair lineage points to a missing successor"
                )
            if not (
                child_authority.partition_key == partition_key
                and child_authority.source_id == "research_os"
                and child_authority.dataset in _REPAIR_STAGE_INDEX
                and child.partition_run_id
                == child_authority.successor_partition_run_id
                and child.source_id == child_authority.source_id
                and child.dataset == child_authority.dataset
                and child.partition_key == child_authority.partition_key
                and child.generation == child_authority.generation
                and child.repair_parent_partition_run_id
                == current.partition_run_id
                and child.repair_parent_hash
                == child_authority.parent_terminal_hash
                and child.repair_fingerprint
                == child_authority.repair_fingerprint
                and child_authority.parent_terminal_hash
                == self._partition_terminal_hash(current)
            ):
                raise ImmutablePartition(
                    "global repair lineage evidence is inconsistent"
                )
            current = child

    def _incident_shadow_repair_succeeded(
        self,
        session: Session,
        *,
        incident_id: str,
        partition_key: str,
    ) -> bool:
        rows = tuple(
            session.execute(
                select(orm.PartitionRepairAuthorityModel).where(
                    orm.PartitionRepairAuthorityModel.scope_key
                    == f"incident:{incident_id}",
                    orm.PartitionRepairAuthorityModel.source_id == "research_os",
                    orm.PartitionRepairAuthorityModel.dataset == "stage_shadow",
                    orm.PartitionRepairAuthorityModel.partition_key == partition_key,
                )
            ).scalars()
        )
        leaf = self._repair_authority_leaf(rows)
        if leaf is None:
            return False
        successor = session.get(
            orm.PartitionRunModel, leaf.successor_partition_run_id
        )
        return (
            successor is not None
            and PartitionStatus(successor.status) is PartitionStatus.SUCCEEDED
        )

    def _require_incident_repair_turn(
        self,
        session: Session,
        *,
        incident: IncidentRecord,
    ) -> None:
        """Deterministically serialize whole repair cohorts per partition."""

        candidates = tuple(
            session.execute(
                select(orm.DataIncidentModel)
                .where(
                    orm.DataIncidentModel.partition_key == incident.partition_key,
                    orm.DataIncidentModel.status == IncidentStatus.OPEN.value,
                    orm.DataIncidentModel.partition_run_id.is_not(None),
                )
                .order_by(
                    orm.DataIncidentModel.occurred_at.asc(),
                    orm.DataIncidentModel.incident_id.asc(),
                )
            ).scalars()
        )
        for candidate in candidates:
            candidate_stage = IncidentStage(candidate.stage)
            expected_dataset = next(
                dataset_name
                for dataset_name, stage in _REPAIR_STAGE_DATASETS
                if stage is candidate_stage
            )
            failed = session.get(
                orm.PartitionRunModel, candidate.partition_run_id
            )
            if not (
                failed is not None
                and failed.source_id == "research_os"
                and failed.dataset == expected_dataset
                and failed.partition_key == incident.partition_key
                and PartitionStatus(failed.status)
                in {
                    PartitionStatus.FAILED,
                    PartitionStatus.DISPUTED,
                    PartitionStatus.QUARANTINED,
                }
            ):
                # Non-production/legacy incidents do not own this repair lane.
                continue
            if self._incident_shadow_repair_succeeded(
                session,
                incident_id=candidate.incident_id,
                partition_key=incident.partition_key,
            ):
                continue
            if candidate.incident_id != incident.incident_id:
                raise ProductionLedgerError(
                    "incident repair is waiting for the earlier OPEN repair "
                    f"cohort {candidate.incident_id} to complete Shadow"
                )
            return
        raise ProductionLedgerError(
            "OPEN incident is not eligible for the production repair lane"
        )

    def get_repair_authority(
        self, incident_id: str, dataset: str
    ) -> PartitionRepairAuthority | None:
        selected_incident = _safe_identifier(incident_id, "incident_id")
        selected_dataset = _safe_identifier(dataset, "dataset")
        with self._session() as session:
            incident = session.get(orm.DataIncidentModel, selected_incident)
            if incident is None:
                return None
            rows = tuple(
                session.execute(
                    select(orm.PartitionRepairAuthorityModel).where(
                        orm.PartitionRepairAuthorityModel.scope_key
                        == f"incident:{selected_incident}",
                        orm.PartitionRepairAuthorityModel.source_id
                        == "research_os",
                        orm.PartitionRepairAuthorityModel.dataset
                        == selected_dataset,
                        orm.PartitionRepairAuthorityModel.partition_key
                        == incident.partition_key,
                    )
                ).scalars()
            )
            leaf = self._repair_authority_leaf(rows)
            return None if leaf is None else self._repair_authority_record(leaf)

    def get_repair_partition(
        self, incident_id: str, dataset: str
    ) -> PartitionRecord | None:
        authority = self.get_repair_authority(incident_id, dataset)
        if authority is None:
            return None
        record = self.get_partition(authority.identity)
        if record is None:
            raise ProductionLedgerError(
                "repair authority points to a missing successor partition"
            )
        if not (
            record.repair_incident_id == authority.incident_id
            and record.repair_parent_partition_run_id
            == authority.parent_partition_run_id
            and record.repair_parent_hash == authority.parent_terminal_hash
            and record.repair_fingerprint == authority.repair_fingerprint
        ):
            raise ImmutablePartition(
                "repair successor disagrees with its selection authority"
            )
        return record

    def get_repair_chain(
        self, incident_id: str, dataset: str
    ) -> tuple[PartitionRepairAuthority, ...]:
        """Return one incident/stage authority chain root-to-leaf, fail closed."""

        selected_incident = _safe_identifier(incident_id, "incident_id")
        selected_dataset = _safe_identifier(dataset, "dataset")
        with self._session() as session:
            incident = session.get(orm.DataIncidentModel, selected_incident)
            if incident is None:
                return ()
            rows = tuple(
                session.execute(
                    select(orm.PartitionRepairAuthorityModel).where(
                        orm.PartitionRepairAuthorityModel.scope_key
                        == f"incident:{selected_incident}",
                        orm.PartitionRepairAuthorityModel.source_id
                        == "research_os",
                        orm.PartitionRepairAuthorityModel.dataset
                        == selected_dataset,
                        orm.PartitionRepairAuthorityModel.partition_key
                        == incident.partition_key,
                    )
                ).scalars()
            )
            if not rows:
                return ()
            successor_ids = {row.successor_partition_run_id for row in rows}
            root_rows = [
                row for row in rows if row.parent_partition_run_id not in successor_ids
            ]
            if len(root_rows) != 1:
                raise ProductionLedgerError(
                    "repair authority has no unique root"
                )
            by_parent: dict[str, list[Any]] = {}
            for row in rows:
                by_parent.setdefault(row.parent_partition_run_id, []).append(row)
            ordered: list[PartitionRepairAuthority] = []
            visited: set[str] = set()
            current = root_rows[0]
            while True:
                if current.authority_id in visited:
                    raise ProductionLedgerError("repair authority chain contains a cycle")
                visited.add(current.authority_id)
                parent_model = session.get(
                    orm.PartitionRunModel, current.parent_partition_run_id
                )
                successor_model = session.get(
                    orm.PartitionRunModel, current.successor_partition_run_id
                )
                if parent_model is None or successor_model is None:
                    raise ProductionLedgerError(
                        "repair authority chain points to a missing partition"
                    )
                parent_hash = self._partition_terminal_hash(parent_model)
                authority = self._repair_authority_record(current)
                expected_authority_hash = content_fingerprint(
                    {
                        "scope_key": authority.scope_key,
                        "incident_id": authority.incident_id,
                        "source_id": authority.identity.source_id,
                        "dataset": authority.identity.dataset,
                        "partition_key": authority.identity.partition_key,
                        "generation": authority.identity.generation,
                        "parent_partition_run_id": authority.parent_partition_run_id,
                        "parent_terminal_hash": authority.parent_terminal_hash,
                        "successor_partition_run_id": authority.identity.partition_run_id,
                        "repair_fingerprint": authority.repair_fingerprint,
                    },
                    domain="factor-lab/research-os/v1/partition-repair-authority",
                )
                if not (
                    authority.authority_id
                    == f"repairauth_{expected_authority_hash[:64]}"
                    and authority.incident_id == selected_incident
                    and authority.parent_terminal_hash == parent_hash
                    and successor_model.partition_run_id
                    == authority.identity.partition_run_id
                    and successor_model.source_id == authority.identity.source_id
                    and successor_model.dataset == authority.identity.dataset
                    and successor_model.partition_key
                    == authority.identity.partition_key
                    and successor_model.generation == authority.identity.generation
                    and successor_model.repair_incident_id == selected_incident
                    and successor_model.repair_parent_partition_run_id
                    == authority.parent_partition_run_id
                    and successor_model.repair_parent_hash
                    == authority.parent_terminal_hash
                    and successor_model.repair_fingerprint
                    == authority.repair_fingerprint
                ):
                    raise ImmutablePartition(
                        "repair authority chain evidence is inconsistent"
                    )
                ordered.append(authority)
                next_rows = by_parent.get(
                    authority.identity.partition_run_id, []
                )
                if not next_rows:
                    break
                if len(next_rows) != 1:
                    raise ProductionLedgerError(
                        "repair authority chain is branched"
                    )
                current = next_rows[0]
            if len(visited) != len(rows):
                raise ProductionLedgerError(
                    "repair authority chain contains disconnected evidence"
                )
            return tuple(ordered)

    def get_incident_repair_predecessor(
        self, incident_id: str
    ) -> PartitionRecord | None:
        """Return the verified global predecessor of an incident's Source root.

        The predecessor may be the original Source base/generic retry or a
        prior incident's successful Shadow leaf.  Callers use this query when
        revalidating a cohort: accepting the per-incident parent fields alone
        would not prove that the cohort was attached to the sole global repair
        lineage instead of a disconnected authority graph.
        """

        selected_incident = _safe_identifier(incident_id, "incident_id")
        with self._session() as session:
            incident = session.get(orm.DataIncidentModel, selected_incident)
            if incident is None:
                return None
            source_rows = tuple(
                session.execute(
                    select(orm.PartitionRepairAuthorityModel).where(
                        orm.PartitionRepairAuthorityModel.scope_key
                        == f"incident:{selected_incident}",
                        orm.PartitionRepairAuthorityModel.source_id
                        == "research_os",
                        orm.PartitionRepairAuthorityModel.dataset
                        == "stage_source",
                        orm.PartitionRepairAuthorityModel.partition_key
                        == incident.partition_key,
                    )
                ).scalars()
            )
            if not source_rows:
                return None
            successor_ids = {
                row.successor_partition_run_id for row in source_rows
            }
            roots = [
                row
                for row in source_rows
                if row.parent_partition_run_id not in successor_ids
            ]
            if len(roots) != 1:
                raise ProductionLedgerError(
                    "incident Source repair authority has no unique root"
                )
            source_root = roots[0]
            current = session.get(
                orm.PartitionRunModel,
                PartitionIdentity(
                    "research_os", "stage_source", incident.partition_key
                ).partition_run_id,
            )
            if current is None:
                raise ProductionLedgerError(
                    "incident repair global Source root is missing"
                )
            visited: set[str] = set()
            while True:
                if current.partition_run_id in visited:
                    raise ProductionLedgerError(
                        "incident repair global lineage contains a cycle"
                    )
                visited.add(current.partition_run_id)
                if current.partition_run_id == source_root.parent_partition_run_id:
                    if (
                        source_root.parent_terminal_hash
                        != self._partition_terminal_hash(current)
                    ):
                        raise ImmutablePartition(
                            "incident repair predecessor terminal hash changed"
                        )
                    return self._partition_record(current)
                children = tuple(
                    session.execute(
                        select(orm.PartitionRepairAuthorityModel).where(
                            orm.PartitionRepairAuthorityModel.parent_partition_run_id
                            == current.partition_run_id
                        )
                    ).scalars()
                )
                if len(children) != 1:
                    raise ProductionLedgerError(
                        "incident Source root is not on one global repair lineage"
                    )
                child_authority = children[0]
                child = session.get(
                    orm.PartitionRunModel,
                    child_authority.successor_partition_run_id,
                )
                if child is None:
                    raise ProductionLedgerError(
                        "incident global repair lineage successor is missing"
                    )
                if not (
                    child_authority.partition_key == incident.partition_key
                    and child_authority.source_id == "research_os"
                    and child_authority.dataset in _REPAIR_STAGE_INDEX
                    and child.repair_parent_partition_run_id
                    == current.partition_run_id
                    and child.repair_parent_hash
                    == child_authority.parent_terminal_hash
                    and child_authority.parent_terminal_hash
                    == self._partition_terminal_hash(current)
                    and child.partition_run_id
                    == child_authority.successor_partition_run_id
                ):
                    raise ImmutablePartition(
                        "incident global repair predecessor evidence is inconsistent"
                    )
                current = child

    def reserve_repair_successor(
        self,
        *,
        incident_id: str,
        dataset: str,
        repair_fingerprint: str,
        input_hash: str | None = None,
        created_at: datetime,
        details: Mapping[str, Any] | None = None,
    ) -> PartitionRepairAuthority:
        """CAS-select one immutable successor in the five-stage repair chain.

        Every incident starts a fresh Source successor, even when the original
        fault was later in the chain.  Silver, DQ, Gold and Shadow can only be
        reserved after the immediately preceding selected successor succeeded.
        This makes one incident/fingerprint a single causal chain and prevents
        old same-day terminal rows from being mistaken for repaired evidence.
        """

        selected_incident = _safe_identifier(incident_id, "incident_id")
        selected_dataset = _safe_identifier(dataset, "dataset")
        if selected_dataset not in _REPAIR_STAGE_INDEX:
            raise ValueError("dataset is not a production repair stage")
        normalized_repair_fingerprint = _sha256(
            repair_fingerprint, "repair_fingerprint", required=True
        )
        normalized_input_hash = _sha256(input_hash, "input_hash")
        when = _aware(created_at, "created_at")
        normalized_details = _canonical_json_mapping(details, name="details")

        with self._session.begin() as session:
            dialect = session.get_bind().dialect.name
            if dialect == "sqlite":
                session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            self._serialize_incident_mutations(session)
            incident_query = select(orm.DataIncidentModel).where(
                orm.DataIncidentModel.incident_id == selected_incident
            )
            if dialect == "postgresql":
                incident_query = incident_query.with_for_update()
            incident_model = session.execute(incident_query).scalar_one_or_none()
            if incident_model is None:
                raise ProductionLedgerError("repair incident was not found")
            incident = self._incident_record(incident_model)
            if incident.status is not IncidentStatus.OPEN:
                raise ImmutablePartition("repair requires an OPEN data incident")
            scope_key = f"incident:{selected_incident}"
            stage_index = _REPAIR_STAGE_INDEX[selected_dataset]
            failed_dataset = next(
                dataset_name
                for dataset_name, stage in _REPAIR_STAGE_DATASETS
                if stage is incident.stage
            )
            failed_model = (
                None
                if not incident.partition_run_id
                else session.get(
                    orm.PartitionRunModel, incident.partition_run_id
                )
            )
            if failed_model is None:
                raise ProductionLedgerError(
                    "repair incident failed partition is missing"
                )
            if not (
                failed_model.source_id == "research_os"
                and failed_model.dataset == failed_dataset
                and failed_model.partition_key == incident.partition_key
                and PartitionStatus(failed_model.status)
                in {
                    PartitionStatus.FAILED,
                    PartitionStatus.DISPUTED,
                    PartitionStatus.QUARANTINED,
                }
            ):
                raise ImmutablePartition(
                    "repair incident is not bound to its exact failed terminal partition"
                )

            existing_query = select(orm.PartitionRepairAuthorityModel).where(
                orm.PartitionRepairAuthorityModel.scope_key == scope_key,
                orm.PartitionRepairAuthorityModel.source_id == "research_os",
                orm.PartitionRepairAuthorityModel.dataset == selected_dataset,
                orm.PartitionRepairAuthorityModel.partition_key
                == incident.partition_key,
            )
            if dialect == "postgresql":
                existing_query = existing_query.with_for_update()
            existing_rows = tuple(session.execute(existing_query).scalars())
            existing = self._repair_authority_leaf(existing_rows)
            if existing is not None:
                existing_authority = self._repair_authority_record(existing)
                if (
                    existing_authority.repair_fingerprint
                    != normalized_repair_fingerprint
                ):
                    raise ImmutablePartition(
                        "incident repair chain has a different fingerprint"
                    )
                selected_model = session.get(
                    orm.PartitionRunModel, existing.successor_partition_run_id
                )
                if selected_model is None:
                    raise ProductionLedgerError(
                        "repair authority successor is missing"
                    )
                if PartitionStatus(selected_model.status) in {
                    PartitionStatus.PENDING,
                    PartitionStatus.RUNNING,
                    PartitionStatus.SUCCEEDED,
                }:
                    if (
                        normalized_input_hash is not None
                        and selected_model.input_hash not in {
                            None,
                            normalized_input_hash,
                        }
                    ):
                        raise ImmutablePartition(
                            "repair successor input hash differs from its authority"
                        )
                    return existing_authority

            if stage_index == 0:
                source_base_identity = PartitionIdentity(
                    "research_os", "stage_source", incident.partition_key
                )
                self._require_incident_repair_turn(
                    session, incident=incident
                )
                parent_model = self._global_repair_lineage_leaf(
                    session,
                    root_partition_run_id=(
                        source_base_identity.partition_run_id
                    ),
                    partition_key=incident.partition_key,
                )
                if parent_model is None:
                    raise ProductionLedgerError(
                        "repair Source authoritative parent is missing"
                    )
                if (
                    PartitionStatus(parent_model.status)
                    not in _TERMINAL_PARTITION_STATUSES
                ):
                    raise ProductionLedgerError(
                        "earlier repair cohort has not terminalized its authority"
                    )
                if (
                    parent_model.repair_incident_id
                    not in {None, incident.incident_id}
                    and not (
                        parent_model.dataset == "stage_shadow"
                        and PartitionStatus(parent_model.status)
                        is PartitionStatus.SUCCEEDED
                    )
                ):
                    raise ProductionLedgerError(
                        "earlier incident repair has not completed Shadow"
                    )
                parent_identity = PartitionIdentity(
                    parent_model.source_id,
                    parent_model.dataset,
                    parent_model.partition_key,
                    parent_model.generation,
                )
            else:
                previous_dataset = _REPAIR_STAGE_DATASETS[stage_index - 1][0]
                previous_rows = tuple(session.execute(
                    select(orm.PartitionRepairAuthorityModel).where(
                        orm.PartitionRepairAuthorityModel.scope_key == scope_key,
                        orm.PartitionRepairAuthorityModel.source_id
                        == "research_os",
                        orm.PartitionRepairAuthorityModel.dataset
                        == previous_dataset,
                        orm.PartitionRepairAuthorityModel.partition_key
                        == incident.partition_key,
                    )
                ).scalars())
                previous_authority = self._repair_authority_leaf(previous_rows)
                if previous_authority is None:
                    raise ProductionLedgerError(
                        "repair stages must be reserved in Source-to-Shadow order"
                    )
                if (
                    previous_authority.repair_fingerprint
                    != normalized_repair_fingerprint
                ):
                    raise ImmutablePartition(
                        "repair stages must share one repair fingerprint"
                    )
                parent_model = session.get(
                    orm.PartitionRunModel,
                    previous_authority.successor_partition_run_id,
                )
                if (
                    parent_model is None
                    or PartitionStatus(parent_model.status)
                    is not PartitionStatus.SUCCEEDED
                ):
                    raise ProductionLedgerError(
                        "preceding repair successor has not succeeded"
                    )
                parent_identity = PartitionIdentity(
                    parent_model.source_id,
                    parent_model.dataset,
                    parent_model.partition_key,
                    parent_model.generation,
                )

            if existing is not None:
                existing_authority = self._repair_authority_record(existing)
                if (
                    existing_authority.repair_fingerprint
                    != normalized_repair_fingerprint
                ):
                    raise ImmutablePartition(
                        "incident repair chain has a different fingerprint"
                    )
                selected_model = session.get(
                    orm.PartitionRunModel, existing.successor_partition_run_id
                )
                if selected_model is None:
                    raise ProductionLedgerError(
                        "repair authority successor is missing"
                    )
                selected_status = PartitionStatus(selected_model.status)
                if selected_status in {
                    PartitionStatus.PENDING,
                    PartitionStatus.RUNNING,
                    PartitionStatus.SUCCEEDED,
                }:
                    if (
                        normalized_input_hash is not None
                        and selected_model.input_hash not in {
                            None,
                            normalized_input_hash,
                        }
                    ):
                        raise ImmutablePartition(
                            "repair successor input hash differs from its authority"
                        )
                    return existing_authority
                parent_model = selected_model
                parent_identity = PartitionIdentity(
                    parent_model.source_id,
                    parent_model.dataset,
                    parent_model.partition_key,
                    parent_model.generation,
                )

            parent_terminal_hash = self._partition_terminal_hash(parent_model)
            generation_hash = content_fingerprint(
                {
                    "scope_key": scope_key,
                    "incident_id": incident.incident_id,
                    "incident_hash": incident.incident_hash,
                    "dataset": selected_dataset,
                    "parent_partition_run_id": parent_identity.partition_run_id,
                    "parent_terminal_hash": parent_terminal_hash,
                    "repair_fingerprint": normalized_repair_fingerprint,
                },
                domain="factor-lab/research-os/v1/partition-repair-generation",
            )
            generation = f"repair_{generation_hash[:64]}"
            successor_identity = PartitionIdentity(
                "research_os",
                selected_dataset,
                incident.partition_key,
                generation,
            )
            authority_hash = content_fingerprint(
                {
                    "scope_key": scope_key,
                    "incident_id": incident.incident_id,
                    "source_id": successor_identity.source_id,
                    "dataset": successor_identity.dataset,
                    "partition_key": successor_identity.partition_key,
                    "generation": successor_identity.generation,
                    "parent_partition_run_id": parent_identity.partition_run_id,
                    "parent_terminal_hash": parent_terminal_hash,
                    "successor_partition_run_id": successor_identity.partition_run_id,
                    "repair_fingerprint": normalized_repair_fingerprint,
                },
                domain="factor-lab/research-os/v1/partition-repair-authority",
            )
            authority_id = f"repairauth_{authority_hash[:64]}"

            if when < incident.occurred_at:
                raise ValueError("repair successor cannot predate its incident")
            if session.get(
                orm.PartitionRunModel, successor_identity.partition_run_id
            ) is not None:
                raise ImmutablePartition(
                    "repair successor identity exists without selection authority"
                )
            successor = orm.PartitionRunModel(
                partition_run_id=successor_identity.partition_run_id,
                source_id=successor_identity.source_id,
                dataset=successor_identity.dataset,
                partition_key=successor_identity.partition_key,
                generation=successor_identity.generation,
                status=PartitionStatus.PENDING.value,
                attempts=0,
                input_hash=normalized_input_hash,
                details_json={
                    **normalized_details,
                    "repair_authority_id": authority_id,
                    "repair_scope_key": scope_key,
                    "repair_incident_id": incident.incident_id,
                    "repair_parent_partition_run_id": parent_identity.partition_run_id,
                    "repair_parent_hash": parent_terminal_hash,
                    "repair_fingerprint": normalized_repair_fingerprint,
                },
                created_at=when,
                updated_at=when,
                repair_incident_id=incident.incident_id,
                repair_parent_partition_run_id=parent_identity.partition_run_id,
                repair_parent_hash=parent_terminal_hash,
                repair_fingerprint=normalized_repair_fingerprint,
            )
            session.add(successor)
            session.flush()
            selected = orm.PartitionRepairAuthorityModel(
                authority_id=authority_id,
                scope_key=scope_key,
                incident_id=incident.incident_id,
                source_id=successor_identity.source_id,
                dataset=successor_identity.dataset,
                partition_key=successor_identity.partition_key,
                generation=successor_identity.generation,
                parent_partition_run_id=parent_identity.partition_run_id,
                parent_terminal_hash=parent_terminal_hash,
                successor_partition_run_id=successor_identity.partition_run_id,
                repair_fingerprint=normalized_repair_fingerprint,
                created_at=when,
            )
            session.add(selected)
            session.flush()
            return self._repair_authority_record(selected)

    def record_shadow_revalidation_rejection(
        self,
        *,
        incident_id: str,
        rejected_partition_run_id: str,
        rejection_evidence_hash: str,
    ) -> PartitionRepairAuthority:
        """Append one immutable FAILED marker after a stale succeeded Shadow leaf.

        The succeeded attempt is never rewritten.  Its typed rejection becomes
        the next authority leaf, allowing a later accepted trading session to
        extend the same incident repair chain without branching.
        """

        selected_incident = _safe_identifier(incident_id, "incident_id")
        selected_rejected = _safe_identifier(
            rejected_partition_run_id, "rejected_partition_run_id"
        )
        normalized_evidence_hash = _sha256(
            rejection_evidence_hash,
            "rejection_evidence_hash",
            required=True,
        )
        with self._session.begin() as session:
            dialect = session.get_bind().dialect.name
            if dialect == "sqlite":
                session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            self._serialize_incident_mutations(session)
            incident_query = select(orm.DataIncidentModel).where(
                orm.DataIncidentModel.incident_id == selected_incident
            )
            if dialect == "postgresql":
                incident_query = incident_query.with_for_update()
            incident_model = session.execute(incident_query).scalar_one_or_none()
            if incident_model is None:
                raise ProductionLedgerError("repair incident was not found")
            incident = self._incident_record(incident_model)
            if incident.status is not IncidentStatus.OPEN:
                raise ImmutablePartition(
                    "Shadow revalidation rejection requires an OPEN incident"
                )

            scope_key = f"incident:{selected_incident}"
            rows_query = select(orm.PartitionRepairAuthorityModel).where(
                orm.PartitionRepairAuthorityModel.scope_key == scope_key,
                orm.PartitionRepairAuthorityModel.source_id == "research_os",
                orm.PartitionRepairAuthorityModel.dataset == "stage_shadow",
                orm.PartitionRepairAuthorityModel.partition_key
                == incident.partition_key,
            )
            if dialect == "postgresql":
                rows_query = rows_query.with_for_update()
            rows = tuple(session.execute(rows_query).scalars())
            existing_child = next(
                (
                    row
                    for row in rows
                    if row.parent_partition_run_id == selected_rejected
                ),
                None,
            )
            if existing_child is not None:
                child = session.get(
                    orm.PartitionRunModel,
                    existing_child.successor_partition_run_id,
                )
                details = {} if child is None else dict(child.details_json or {})
                if not (
                    child is not None
                    and PartitionStatus(child.status) is PartitionStatus.FAILED
                    and child.input_hash == normalized_evidence_hash
                    and details.get("authority_kind")
                    == "typed_shadow_revalidation_rejection"
                    and details.get("rejected_partition_run_id")
                    == selected_rejected
                    and details.get("rejection_evidence_hash")
                    == normalized_evidence_hash
                ):
                    raise ImmutablePartition(
                        "succeeded Shadow parent already has a different successor"
                    )
                return self._repair_authority_record(existing_child)

            leaf = self._repair_authority_leaf(rows)
            if leaf is None or leaf.successor_partition_run_id != selected_rejected:
                raise ImmutablePartition(
                    "rejected Shadow partition is not the current repair authority leaf"
                )
            parent = session.get(orm.PartitionRunModel, selected_rejected)
            if parent is None:
                raise ProductionLedgerError(
                    "rejected Shadow repair partition is missing"
                )
            parent_details = dict(parent.details_json or {})
            if not (
                parent.source_id == "research_os"
                and parent.dataset == "stage_shadow"
                and parent.partition_key == incident.partition_key
                and parent.repair_incident_id == incident.incident_id
                and PartitionStatus(parent.status) is PartitionStatus.SUCCEEDED
                and parent.completed_at is not None
                and isinstance(parent.output_hash, str)
                and _SHA256.fullmatch(parent.output_hash)
                and re.fullmatch(
                    r"repaircohort_[0-9a-f]{64}",
                    str(parent_details.get("repair_cohort_id") or ""),
                )
                and re.fullmatch(
                    r"\d{4}-\d{2}-\d{2}",
                    str(
                        parent_details.get("repair_validation_trade_date") or ""
                    ),
                )
            ):
                raise ImmutablePartition(
                    "only a complete incident-selected Shadow result can be rejected"
                )
            repair_fingerprint = _sha256(
                parent.repair_fingerprint,
                "repair_fingerprint",
                required=True,
            )
            parent_terminal_hash = self._partition_terminal_hash(parent)
            database_now = self.incident_controls._database_now(session)
            if database_now < _db_aware(parent.completed_at):
                raise ImmutablePartition(
                    "Shadow revalidation rejection predates its succeeded parent"
                )
            generation_hash = content_fingerprint(
                {
                    "scope_key": scope_key,
                    "incident_id": incident.incident_id,
                    "incident_hash": incident.incident_hash,
                    "dataset": "stage_shadow",
                    "parent_partition_run_id": selected_rejected,
                    "parent_terminal_hash": parent_terminal_hash,
                    "repair_fingerprint": repair_fingerprint,
                },
                domain="factor-lab/research-os/v1/partition-repair-generation",
            )
            identity = PartitionIdentity(
                "research_os",
                "stage_shadow",
                incident.partition_key,
                f"repair_{generation_hash[:64]}",
            )
            authority_hash = content_fingerprint(
                {
                    "scope_key": scope_key,
                    "incident_id": incident.incident_id,
                    "source_id": identity.source_id,
                    "dataset": identity.dataset,
                    "partition_key": identity.partition_key,
                    "generation": identity.generation,
                    "parent_partition_run_id": selected_rejected,
                    "parent_terminal_hash": parent_terminal_hash,
                    "successor_partition_run_id": identity.partition_run_id,
                    "repair_fingerprint": repair_fingerprint,
                },
                domain="factor-lab/research-os/v1/partition-repair-authority",
            )
            authority_id = f"repairauth_{authority_hash[:64]}"
            rejection_hash = content_fingerprint(
                {
                    "incident_id": incident.incident_id,
                    "rejected_partition_run_id": selected_rejected,
                    "rejected_output_hash": parent.output_hash,
                    "rejection_evidence_hash": normalized_evidence_hash,
                    "repair_validation_trade_date": parent_details[
                        "repair_validation_trade_date"
                    ],
                },
                domain="factor-lab/research-os/v1/shadow-revalidation-rejection",
            )
            details = {
                "operation": "shadow_nav_step",
                "dagster_run_id": f"revalidation-rejection:{rejection_hash[:32]}",
                "repair_cohort_id": parent_details["repair_cohort_id"],
                "repair_validation_trade_date": parent_details[
                    "repair_validation_trade_date"
                ],
                "authority_kind": "typed_shadow_revalidation_rejection",
                "rejected_partition_run_id": selected_rejected,
                "rejected_output_hash": parent.output_hash,
                "rejection_evidence_hash": normalized_evidence_hash,
                "rejection_hash": rejection_hash,
                "repair_authority_id": authority_id,
                "repair_scope_key": scope_key,
                "repair_incident_id": incident.incident_id,
                "repair_parent_partition_run_id": selected_rejected,
                "repair_parent_hash": parent_terminal_hash,
                "repair_fingerprint": repair_fingerprint,
            }
            marker = orm.PartitionRunModel(
                partition_run_id=identity.partition_run_id,
                source_id=identity.source_id,
                dataset=identity.dataset,
                partition_key=identity.partition_key,
                generation=identity.generation,
                status=PartitionStatus.FAILED.value,
                attempts=0,
                input_hash=normalized_evidence_hash,
                details_json=details,
                error_code="shadow_revalidation_stale",
                error="typed Shadow revalidation rejected the prior succeeded result",
                created_at=database_now,
                updated_at=database_now,
                started_at=database_now,
                completed_at=database_now,
                repair_incident_id=incident.incident_id,
                repair_parent_partition_run_id=selected_rejected,
                repair_parent_hash=parent_terminal_hash,
                repair_fingerprint=repair_fingerprint,
            )
            session.add(marker)
            session.flush()
            authority = orm.PartitionRepairAuthorityModel(
                authority_id=authority_id,
                scope_key=scope_key,
                incident_id=incident.incident_id,
                source_id=identity.source_id,
                dataset=identity.dataset,
                partition_key=identity.partition_key,
                generation=identity.generation,
                parent_partition_run_id=selected_rejected,
                parent_terminal_hash=parent_terminal_hash,
                successor_partition_run_id=identity.partition_run_id,
                repair_fingerprint=repair_fingerprint,
                created_at=database_now,
            )
            session.add(authority)
            session.flush()
            return self._repair_authority_record(authority)

    def reserve_retry_successor(
        self,
        identity: PartitionIdentity,
        *,
        repair_fingerprint: str,
        created_at: datetime,
        input_hash: str | None = None,
        details: Mapping[str, Any] | None = None,
        scope_key: str | None = None,
        incident_id: str | None = None,
        allow_succeeded_base: bool = False,
    ) -> PartitionRepairAuthority:
        """Reserve an immutable retry generation for one failed logical slot."""

        if identity.generation != _BASE_PARTITION_GENERATION:
            raise ValueError("retry identity must name the base generation")
        normalized_fingerprint = _sha256(
            repair_fingerprint, "repair_fingerprint", required=True
        )
        normalized_input_hash = _sha256(input_hash, "input_hash")
        normalized_details = _canonical_json_mapping(details, name="details")
        when = _aware(created_at, "created_at")
        selected_scope = _safe_identifier(
            scope_key or f"retry:{identity.partition_run_id}", "scope_key"
        )
        selected_incident = (
            None
            if incident_id is None
            else _safe_identifier(incident_id, "incident_id")
        )
        with self._session.begin() as session:
            dialect = session.get_bind().dialect.name
            if dialect == "sqlite":
                session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            if selected_incident is not None:
                self._serialize_incident_mutations(session)
                incident_query = select(orm.DataIncidentModel).where(
                    orm.DataIncidentModel.incident_id == selected_incident
                )
                if dialect == "postgresql":
                    incident_query = incident_query.with_for_update()
                incident = session.execute(incident_query).scalar_one_or_none()
                if (
                    incident is None
                    or IncidentStatus(incident.status) is not IncidentStatus.OPEN
                ):
                    raise ImmutablePartition(
                        "scoped retry requires its exact OPEN incident"
                    )
            base_query = select(orm.PartitionRunModel).where(
                orm.PartitionRunModel.partition_run_id == identity.partition_run_id
            )
            if dialect == "postgresql":
                base_query = base_query.with_for_update()
            base_model = session.execute(base_query).scalar_one_or_none()
            if base_model is None:
                raise ProductionLedgerError("retry base partition is missing")
            if PartitionStatus(base_model.status) not in {
                PartitionStatus.FAILED,
                PartitionStatus.DISPUTED,
                PartitionStatus.QUARANTINED,
                *(
                    (PartitionStatus.SUCCEEDED,)
                    if allow_succeeded_base
                    else ()
                ),
            }:
                raise ImmutablePartition(
                    "retry generation requires a non-success terminal base"
                )
            authority_query = select(orm.PartitionRepairAuthorityModel).where(
                orm.PartitionRepairAuthorityModel.scope_key == selected_scope,
                orm.PartitionRepairAuthorityModel.source_id == identity.source_id,
                orm.PartitionRepairAuthorityModel.dataset == identity.dataset,
                orm.PartitionRepairAuthorityModel.partition_key
                == identity.partition_key,
            )
            if dialect == "postgresql":
                authority_query = authority_query.with_for_update()
            authority_rows = tuple(session.execute(authority_query).scalars())
            leaf = self._repair_authority_leaf(authority_rows)
            if leaf is None:
                parent_model = base_model
            else:
                leaf_model = session.get(
                    orm.PartitionRunModel, leaf.successor_partition_run_id
                )
                if leaf_model is None:
                    raise ProductionLedgerError(
                        "retry authority successor is missing"
                    )
                leaf_authority = self._repair_authority_record(leaf)
                if selected_incident is not None and (
                    leaf_authority.incident_id != selected_incident
                    or leaf_authority.repair_fingerprint != normalized_fingerprint
                ):
                    raise ImmutablePartition(
                        "incident-scoped retry authority changed"
                    )
                if PartitionStatus(leaf_model.status) in {
                    PartitionStatus.PENDING,
                    PartitionStatus.RUNNING,
                    PartitionStatus.SUCCEEDED,
                }:
                    if (
                        normalized_input_hash is not None
                        and leaf_model.input_hash not in {
                            None,
                            normalized_input_hash,
                        }
                    ):
                        raise ImmutablePartition(
                            "retry successor input hash changed"
                        )
                    return leaf_authority
                parent_model = leaf_model
            parent_hash = self._partition_terminal_hash(parent_model)
            parent_identity = PartitionIdentity(
                parent_model.source_id,
                parent_model.dataset,
                parent_model.partition_key,
                parent_model.generation,
            )
            generation_hash = content_fingerprint(
                {
                    "scope_key": selected_scope,
                    "source_id": identity.source_id,
                    "dataset": identity.dataset,
                    "partition_key": identity.partition_key,
                    "parent_partition_run_id": parent_identity.partition_run_id,
                    "parent_terminal_hash": parent_hash,
                    "repair_fingerprint": normalized_fingerprint,
                },
                domain="factor-lab/research-os/v1/partition-repair-generation",
            )
            successor_identity = PartitionIdentity(
                identity.source_id,
                identity.dataset,
                identity.partition_key,
                f"repair_{generation_hash[:64]}",
            )
            authority_hash = content_fingerprint(
                {
                    "scope_key": selected_scope,
                    "parent_partition_run_id": parent_identity.partition_run_id,
                    "parent_terminal_hash": parent_hash,
                    "successor_partition_run_id": successor_identity.partition_run_id,
                    "repair_fingerprint": normalized_fingerprint,
                },
                domain="factor-lab/research-os/v1/partition-repair-authority",
            )
            authority_id = f"repairauth_{authority_hash[:64]}"
            existing = session.get(
                orm.PartitionRepairAuthorityModel, authority_id
            )
            if existing is not None:
                return self._repair_authority_record(existing)
            if session.get(
                orm.PartitionRunModel, successor_identity.partition_run_id
            ) is not None:
                raise ImmutablePartition(
                    "retry successor exists without exact authority"
                )
            successor = orm.PartitionRunModel(
                partition_run_id=successor_identity.partition_run_id,
                source_id=successor_identity.source_id,
                dataset=successor_identity.dataset,
                partition_key=successor_identity.partition_key,
                generation=successor_identity.generation,
                status=PartitionStatus.PENDING.value,
                attempts=0,
                input_hash=normalized_input_hash,
                details_json={
                    **normalized_details,
                    "repair_authority_id": authority_id,
                    "repair_scope_key": selected_scope,
                    "repair_incident_id": selected_incident,
                    "repair_parent_partition_run_id": parent_identity.partition_run_id,
                    "repair_parent_hash": parent_hash,
                    "repair_fingerprint": normalized_fingerprint,
                },
                created_at=when,
                updated_at=when,
                repair_incident_id=selected_incident,
                repair_parent_partition_run_id=parent_identity.partition_run_id,
                repair_parent_hash=parent_hash,
                repair_fingerprint=normalized_fingerprint,
            )
            session.add(successor)
            session.flush()
            authority_model = orm.PartitionRepairAuthorityModel(
                authority_id=authority_id,
                scope_key=selected_scope,
                incident_id=selected_incident,
                source_id=successor_identity.source_id,
                dataset=successor_identity.dataset,
                partition_key=successor_identity.partition_key,
                generation=successor_identity.generation,
                parent_partition_run_id=parent_identity.partition_run_id,
                parent_terminal_hash=parent_hash,
                successor_partition_run_id=successor_identity.partition_run_id,
                repair_fingerprint=normalized_fingerprint,
                created_at=when,
            )
            session.add(authority_model)
            session.flush()
            return self._repair_authority_record(authority_model)

    def claim(
        self,
        *,
        owner: str,
        now: datetime,
        lease_for: timedelta,
        identity: PartitionIdentity | None = None,
        maximum_attempts: int = 5,
    ) -> PartitionLease | None:
        worker = _safe_identifier(owner, "owner")
        claimed_at = _aware(now, "now")
        if lease_for <= timedelta(0) or maximum_attempts <= 0:
            raise ValueError("lease_for and maximum_attempts must be positive")
        expires_at = claimed_at + lease_for
        with self._session.begin() as session:
            eligible = and_(
                orm.PartitionRunModel.attempts < maximum_attempts,
                or_(
                    orm.PartitionRunModel.status == PartitionStatus.PENDING.value,
                    and_(
                        orm.PartitionRunModel.status == PartitionStatus.RUNNING.value,
                        orm.PartitionRunModel.lease_expires_at < claimed_at,
                    ),
                ),
            )
            query = select(orm.PartitionRunModel).where(eligible)
            if identity is not None:
                query = query.where(
                    orm.PartitionRunModel.partition_run_id
                    == identity.partition_run_id
                )
            query = query.order_by(
                orm.PartitionRunModel.partition_key,
                orm.PartitionRunModel.source_id,
                orm.PartitionRunModel.dataset,
                orm.PartitionRunModel.generation,
            ).limit(1)
            if session.bind.dialect.name == "postgresql":
                query = query.with_for_update(skip_locked=True)
            model = session.execute(query).scalar_one_or_none()
            if model is None:
                return None
            next_attempt = int(model.attempts) + 1
            token = content_fingerprint(
                {
                    "partition_run_id": model.partition_run_id,
                    "owner": worker,
                    "attempt": next_attempt,
                    "claimed_at": claimed_at,
                    "expires_at": expires_at,
                },
                domain="factor-lab/research-os/v1/partition-lease",
            )
            model.status = PartitionStatus.RUNNING.value
            model.lease_owner = worker
            model.lease_token = token
            model.lease_expires_at = expires_at
            model.attempts = next_attempt
            model.started_at = claimed_at
            model.completed_at = None
            model.error_code = None
            model.error = None
            model.updated_at = claimed_at
            session.flush()
            record = self._partition_record(model)
        return PartitionLease(record)

    def renew(
        self,
        lease: PartitionLease,
        *,
        now: datetime,
        lease_for: timedelta,
    ) -> PartitionLease:
        renewed_at = _aware(now, "now")
        if lease_for <= timedelta(0):
            raise ValueError("lease_for must be positive")
        with self._session.begin() as session:
            model = session.get(
                orm.PartitionRunModel, lease.identity.partition_run_id
            )
            if (
                model is None
                or model.status != PartitionStatus.RUNNING.value
                or model.lease_owner != lease.record.lease_owner
                or model.lease_token != lease.token
                or (_db_aware(model.lease_expires_at) or renewed_at) < renewed_at
            ):
                raise LeaseConflict("partition lease is missing, stale or expired")
            model.lease_expires_at = renewed_at + lease_for
            model.updated_at = renewed_at
            session.flush()
            record = self._partition_record(model)
        return PartitionLease(record)

    def finish(
        self,
        lease: PartitionLease,
        *,
        status: PartitionStatus,
        completed_at: datetime,
        run_id: str | None = None,
        output_snapshot_id: str | None = None,
        output_hash: str | None = None,
        vendor_revision: str | None = None,
        details: Mapping[str, Any] | None = None,
        error_code: str | None = None,
        error: str | None = None,
        sensitive_values: Sequence[str] = (),
    ) -> PartitionRecord:
        terminal = PartitionStatus(status)
        if terminal not in {
            PartitionStatus.SUCCEEDED,
            PartitionStatus.DISPUTED,
            PartitionStatus.QUARANTINED,
            PartitionStatus.FAILED,
        }:
            raise ValueError("finish requires a terminal partition status")
        finished_at = _aware(completed_at, "completed_at")
        normalized_output_hash = _sha256(
            output_hash,
            "output_hash",
            required=terminal is PartitionStatus.SUCCEEDED,
        )
        if terminal is PartitionStatus.SUCCEEDED and (error_code or error):
            raise ValueError("successful partition cannot carry an error")
        if terminal is not PartitionStatus.SUCCEEDED and not error_code:
            raise ValueError("non-success terminal partition requires error_code")
        normalized_run_id = str(run_id).strip() if run_id else None
        if not normalized_run_id:
            normalized_run_id = None
        with self._session.begin() as session:
            query = select(orm.PartitionRunModel).where(
                orm.PartitionRunModel.partition_run_id
                == lease.identity.partition_run_id
            )
            if session.bind.dialect.name == "postgresql":
                query = query.with_for_update()
            model = session.execute(query).scalar_one_or_none()
            if model is None:
                raise LeaseConflict("partition row disappeared")
            if model.status in {
                item.value for item in _TERMINAL_PARTITION_STATUSES
            }:
                raise ImmutablePartition(
                    f"terminal partition {model.status!r} cannot be overwritten"
                )
            if (
                model.status != PartitionStatus.RUNNING.value
                or model.lease_owner != lease.record.lease_owner
                or model.lease_token != lease.token
                or (_db_aware(model.lease_expires_at) or finished_at) < finished_at
            ):
                raise LeaseConflict("partition terminal write has a stale lease")
            if (
                normalized_run_id is not None
                and session.get(orm.RunModel, normalized_run_id) is None
            ):
                # Do not rely on the database FK alone: SQLite commonly runs
                # with foreign-key enforcement disabled, and every supported
                # backend must fail closed before mutating the partition row.
                raise ProductionLedgerError(
                    "partition terminal write requires an existing parent run"
                )
            model.status = terminal.value
            model.lease_owner = None
            model.lease_token = None
            model.lease_expires_at = None
            model.run_id = normalized_run_id
            model.output_snapshot_id = (
                str(output_snapshot_id).strip() if output_snapshot_id else None
            )
            model.output_hash = normalized_output_hash
            model.vendor_revision = (
                str(vendor_revision).strip() if vendor_revision else None
            )
            model.details_json = dict(details or {})
            model.error_code = str(error_code).strip() if error_code else None
            model.error = (
                sanitize_operational_text(
                    str(error or ""), sensitive_values=sensitive_values
                )
                if error
                else None
            )
            model.completed_at = finished_at
            model.updated_at = finished_at
            session.flush()
            record = self._partition_record(model)
        return record

    def list_partitions(
        self,
        *,
        statuses: Sequence[PartitionStatus] = (),
        source_id: str | None = None,
        dataset: str | None = None,
        after_partition_key: str | None = None,
        limit: int = 10_000,
    ) -> tuple[PartitionRecord, ...]:
        if limit <= 0 or limit > 100_000:
            raise ValueError("limit must be between 1 and 100000")
        with self._session() as session:
            query = select(orm.PartitionRunModel)
            if statuses:
                query = query.where(
                    orm.PartitionRunModel.status.in_(
                        [PartitionStatus(item).value for item in statuses]
                    )
                )
            if source_id:
                query = query.where(
                    orm.PartitionRunModel.source_id
                    == _safe_identifier(source_id, "source_id")
                )
            if dataset:
                query = query.where(
                    orm.PartitionRunModel.dataset
                    == _safe_identifier(dataset, "dataset")
                )
            if after_partition_key:
                query = query.where(
                    orm.PartitionRunModel.partition_key
                    > _safe_partition_key(after_partition_key)
                )
            models = session.execute(
                query.order_by(
                    orm.PartitionRunModel.partition_key,
                    orm.PartitionRunModel.source_id,
                    orm.PartitionRunModel.dataset,
                    orm.PartitionRunModel.generation,
                ).limit(limit)
            ).scalars()
            return tuple(self._partition_record(model) for model in models)

    def progress(
        self, *, source_id: str | None = None, dataset: str | None = None
    ) -> BackfillProgress:
        with self._session() as session:
            query = select(
                orm.PartitionRunModel.status,
                func.count(orm.PartitionRunModel.partition_run_id),
            )
            if source_id:
                query = query.where(
                    orm.PartitionRunModel.source_id
                    == _safe_identifier(source_id, "source_id")
                )
            if dataset:
                query = query.where(
                    orm.PartitionRunModel.dataset
                    == _safe_identifier(dataset, "dataset")
                )
            rows = session.execute(query.group_by(orm.PartitionRunModel.status)).all()
        counts = {str(status): int(count) for status, count in rows}
        return BackfillProgress(total=sum(counts.values()), counts=counts)

    def accepted_calendar_partitions(
        self, *, after_partition_key: str | None = None, limit: int = 5_000
    ) -> tuple[str, ...]:
        """Return only reconciled canonical exchange-calendar partitions.

        Vendor fetch success is not calendar acceptance.  The static calendar
        bootstrap publishes ``research_os/accepted_trade_calendar`` only after
        all required source observations reconcile and pass DQ; Dagster's
        dynamic trading-day partitions consume that canonical ledger row.
        """

        rows = self.list_partitions(
            statuses=(PartitionStatus.SUCCEEDED,),
            source_id="research_os",
            dataset="accepted_trade_calendar",
            after_partition_key=after_partition_key,
            limit=limit,
        )
        return tuple(sorted({row.identity.partition_key for row in rows}))

    def upsert_capability(self, record: CapabilityRecord) -> CapabilityRecord:
        source_id = _safe_identifier(record.source_id, "source_id")
        dataset = _safe_identifier(record.dataset, "dataset")
        status = CapabilityStatus(record.status)
        contract_hash = _sha256(record.contract_hash, "contract_hash", required=True)
        probe_hash = _sha256(record.probe_hash, "probe_hash")
        probed_at = _aware(record.probed_at, "probed_at")
        fields = tuple(sorted({_safe_identifier(item, "field") for item in record.fields}))
        # Capability details may be a typed, content-addressed authority chain
        # (snapshot ids, partition hashes and physical object hashes).  The
        # generic 2 KiB operational-message limit would truncate that JSON and
        # make the readiness auditor unable to recompute its probe hash.
        detail = sanitize_operational_text(record.detail, maximum_length=16_000)
        with self._session.begin() as session:
            model = session.get(orm.SourceCapabilityModel, (source_id, dataset))
            if model is None:
                model = orm.SourceCapabilityModel(
                    source_id=source_id,
                    dataset=dataset,
                    status=status.value,
                    contract_hash=contract_hash,
                    probe_hash=probe_hash,
                    fields_json=list(fields),
                    detail=detail,
                    probed_at=probed_at,
                    updated_at=probed_at,
                )
                session.add(model)
            else:
                model.status = status.value
                model.contract_hash = contract_hash
                model.probe_hash = probe_hash
                model.fields_json = list(fields)
                model.detail = detail
                model.probed_at = probed_at
                model.updated_at = probed_at
        return CapabilityRecord(
            source_id=source_id,
            dataset=dataset,
            status=status,
            contract_hash=contract_hash or "",
            probe_hash=probe_hash,
            fields=fields,
            detail=detail,
            probed_at=probed_at,
        )

    def record_incident(
        self,
        *,
        partition_key: str,
        stage: IncidentStage,
        error_code: str,
        message: str,
        occurred_at: datetime,
        partition_run_id: str | None = None,
        source_ids: Sequence[str] = (),
        evidence_hashes: Sequence[str] = (),
        payload: Mapping[str, Any] | None = None,
        sensitive_values: Sequence[str] = (),
    ) -> IncidentRecord:
        key = _safe_partition_key(partition_key)
        normalized_stage = IncidentStage(stage)
        code = _safe_identifier(error_code, "error_code")
        when = _aware(occurred_at, "occurred_at")
        sources = tuple(sorted({_safe_identifier(item, "source_id") for item in source_ids}))
        evidence = tuple(
            sorted({_sha256(item, "evidence_hash", required=True) or "" for item in evidence_hashes})
        )
        safe_message = sanitize_operational_text(
            message, sensitive_values=sensitive_values
        )
        origin_payload = _canonical_json_mapping(payload, name="payload")
        if "resolution" in origin_payload:
            raise ValueError("payload cannot contain the reserved resolution field")
        incident_payload = {
            "partition_key": key,
            "stage": normalized_stage.value,
            "error_code": code,
            "occurred_at": when,
            "partition_run_id": partition_run_id,
            "source_ids": sources,
            "evidence_hashes": evidence,
            "payload": origin_payload,
        }
        incident_hash = content_fingerprint(
            incident_payload, domain="factor-lab/research-os/v1/data-incident"
        )
        incident_id = f"incident_{incident_hash[:64]}"
        with self._session.begin() as session:
            # OPEN is the fail-closed latch.  The shared lock orders only this
            # short insert against a short resolution CAS/role-binding check;
            # catalog materialization never runs under it.
            if session.get_bind().dialect.name == "sqlite":
                session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            else:
                self._serialize_incident_mutations(session)
            open_values = {
                "incident_id": incident_id,
                "incident_hash": incident_hash,
                "partition_run_id": partition_run_id,
                "partition_key": key,
                "stage": normalized_stage.value,
                "status": IncidentStatus.OPEN.value,
                "error_code": code,
                "message": safe_message,
                "source_ids_json": list(sources),
                "evidence_hashes_json": list(evidence),
                "payload_json": origin_payload,
                "occurred_at": when,
            }
            dialect = session.get_bind().dialect.name
            if dialect == "postgresql":
                from sqlalchemy.dialects.postgresql import insert as dialect_insert

                statement = dialect_insert(orm.DataIncidentModel).values(**open_values)
                session.execute(
                    statement.on_conflict_do_nothing(
                        index_elements=[orm.DataIncidentModel.incident_id]
                    )
                )
            elif dialect == "sqlite":
                from sqlalchemy.dialects.sqlite import insert as dialect_insert

                statement = dialect_insert(orm.DataIncidentModel).values(**open_values)
                session.execute(
                    statement.on_conflict_do_nothing(
                        index_elements=[orm.DataIncidentModel.incident_id]
                    )
                )
            else:  # pragma: no cover - production is PostgreSQL; tests use SQLite.
                try:
                    with session.begin_nested():
                        session.add(orm.DataIncidentModel(**open_values))
                        session.flush()
                except IntegrityError:
                    pass

            query = select(orm.DataIncidentModel).where(
                orm.DataIncidentModel.incident_id == incident_id
            )
            if dialect == "postgresql":
                query = query.with_for_update()
            model = session.execute(query).scalar_one_or_none()
            if model is None:
                raise ProductionLedgerError(
                    "data incident conflict did not resolve to an authority row"
                )
            existing_origin_payload = dict(model.payload_json or {})
            existing_origin_payload.pop("resolution", None)
            immutable_origin_matches = bool(
                model.incident_hash == incident_hash
                and model.partition_run_id == partition_run_id
                and model.partition_key == key
                and model.stage == normalized_stage.value
                and model.error_code == code
                and model.message == safe_message
                and tuple(model.source_ids_json or ()) == sources
                and tuple(model.evidence_hashes_json or ()) == evidence
                and existing_origin_payload == origin_payload
                and _db_aware(model.occurred_at) == when
            )
            if not immutable_origin_matches:
                raise ImmutablePartition("data incident origin authority changed")
            if (
                model.status == IncidentStatus.OPEN.value
                and model.partition_run_id
                and str(existing_origin_payload.get("dagster_run_id") or "").strip()
                and str(existing_origin_payload.get("failed_step_key") or "").strip()
            ):
                # Reserve the OPEN risk latch and fence its exact crash-left
                # partition in one transaction.  Whichever CAS wins against a
                # late worker terminal write decides the outcome: a succeeded
                # worker rolls this incident insert back; a failure reservation
                # clears the lease before the worker can publish stale output.
                self._terminalize_incident_partition_in_session(session, model)
            if (
                model.status == IncidentStatus.OPEN.value
                and str(existing_origin_payload.get("domain_incident_id") or "").strip()
            ):
                # The risk latch and its one durable control action are one
                # reservation.  A process crash can therefore never leave an
                # OPEN incident that has no reclaimable materialization work.
                self.incident_controls.enqueue_in_session(
                    session,
                    incident_id=incident_id,
                    created_at=when,
                )
            return self._incident_record(model)

    @staticmethod
    def _incident_record(model: Any) -> IncidentRecord:
        return IncidentRecord(
            incident_id=model.incident_id,
            incident_hash=model.incident_hash,
            partition_run_id=model.partition_run_id,
            partition_key=model.partition_key,
            stage=IncidentStage(model.stage),
            status=IncidentStatus(model.status),
            error_code=model.error_code,
            message=model.message,
            source_ids=tuple(model.source_ids_json or ()),
            evidence_hashes=tuple(model.evidence_hashes_json or ()),
            payload=dict(model.payload_json or {}),
            occurred_at=_db_aware(model.occurred_at),
            resolved_at=_db_aware(model.resolved_at),
            resolution_hash=model.resolution_hash,
        )

    def resolve_incident(
        self,
        incident_id: str,
        *,
        resolved_at: datetime,
        evidence: Mapping[str, Any],
        superseded: bool = False,
    ) -> IncidentRecord:
        record, _, _ = self.resolve_incident_with_effects(
            incident_id,
            resolved_at=resolved_at,
            evidence=evidence,
            apply_effects=lambda _incident, _other_open: None,
            superseded=superseded,
        )
        return record

    def resolve_incident_with_effects(
        self,
        incident_id: str,
        *,
        resolved_at: datetime,
        evidence: (
            Mapping[str, Any]
            | Callable[
                [IncidentRecord, tuple[IncidentRecord, ...]], Mapping[str, Any]
            ]
        ),
        apply_effects: Callable[
            [IncidentRecord, tuple[IncidentRecord, ...]], Any
        ],
        superseded: bool = False,
    ) -> tuple[IncidentRecord, Any | None, bool]:
        """Generic incident closure; production domain incidents are rejected."""

        return self._resolve_incident_with_effects(
            incident_id,
            resolved_at=resolved_at,
            evidence=evidence,
            apply_effects=apply_effects,
            superseded=superseded,
            _allow_domain_incident=False,
            _require_typed_effect_fence=False,
        )

    def _resolve_typed_data_incident_with_effects(
        self,
        incident_id: str,
        *,
        resolved_at: datetime,
        evidence: (
            Mapping[str, Any]
            | Callable[
                [IncidentRecord, tuple[IncidentRecord, ...]], Mapping[str, Any]
            ]
        ),
        apply_effects: Callable[
            [IncidentRecord, tuple[IncidentRecord, ...]], Any
        ],
        superseded: bool = False,
        require_effect_fence: bool = False,
    ) -> tuple[IncidentRecord, Any | None, bool]:
        """Internal endpoint reached only after typed five-stage validation."""

        return self._resolve_incident_with_effects(
            incident_id,
            resolved_at=resolved_at,
            evidence=evidence,
            apply_effects=apply_effects,
            superseded=superseded,
            _allow_domain_incident=True,
            _require_typed_effect_fence=bool(require_effect_fence),
        )

    @staticmethod
    def _typed_effect_fence_matches(
        session: Session, effect_result: Any
    ) -> bool:
        """Verify the exact catalog tails while the global incident lock is held."""

        if not isinstance(effect_result, Mapping):
            return False
        fence = effect_result.get("typed_effect_fence")
        if not isinstance(fence, Mapping) or fence.get("schema_version") != (
            "research-os/typed-revalidation-fence/v1"
        ):
            return False
        raw_accounts = fence.get("shadow_account_tails")
        raw_bindings = fence.get("shadow_role_bindings")
        raw_lifecycle = fence.get("lifecycle_tails")
        if not all(
            isinstance(value, Sequence) and not isinstance(value, (str, bytes))
            for value in (raw_accounts, raw_bindings, raw_lifecycle)
        ):
            return False
        try:
            expected_accounts = tuple(
                sorted(
                    (
                        {
                            "account_id": _safe_identifier(
                                str(item["account_id"]), "account_id"
                            ),
                            "last_event_sequence": int(
                                item["last_event_sequence"]
                            ),
                            "last_event_hash": _sha256(
                                str(item["last_event_hash"]),
                                "last_event_hash",
                                required=True,
                            ),
                            "status": _safe_identifier(
                                str(item["status"]), "account_status"
                            ),
                        }
                        for item in raw_accounts
                        if isinstance(item, Mapping)
                    ),
                    key=lambda item: item["account_id"],
                )
            )
            expected_bindings = tuple(
                sorted(
                    (
                        {
                            "binding_id": _safe_identifier(
                                str(item["binding_id"]), "binding_id"
                            ),
                            "binding_hash": _sha256(
                                str(item["binding_hash"]),
                                "binding_hash",
                                required=True,
                            ),
                            "role": _safe_identifier(str(item["role"]), "role"),
                            "role_key": _safe_identifier(
                                str(item["role_key"]), "role_key"
                            ),
                            "account_id": _safe_identifier(
                                str(item["account_id"]), "account_id"
                            ),
                        }
                        for item in raw_bindings
                        if isinstance(item, Mapping)
                    ),
                    key=lambda item: (
                        item["role"],
                        item["role_key"],
                        item["binding_id"],
                    ),
                )
            )
            expected_lifecycle = tuple(
                sorted(
                    (
                        {
                            "sleeve_id": _safe_identifier(
                                str(item["sleeve_id"]), "sleeve_id"
                            ),
                            "event_id": _safe_identifier(
                                str(item["event_id"]), "lifecycle_event_id"
                            ),
                            "idempotency_key": str(item["idempotency_key"]),
                            "to_state": _safe_identifier(
                                str(item["to_state"]), "lifecycle_state"
                            ),
                        }
                        for item in raw_lifecycle
                        if isinstance(item, Mapping)
                    ),
                    key=lambda item: item["sleeve_id"],
                )
            )
        except (KeyError, TypeError, ValueError):
            return False
        if not (
            len(expected_accounts) == len(raw_accounts)
            and len(expected_bindings) == len(raw_bindings)
            and len(expected_lifecycle) == len(raw_lifecycle)
            and len({item["account_id"] for item in expected_accounts})
            == len(expected_accounts)
            and len({item["binding_id"] for item in expected_bindings})
            == len(expected_bindings)
            and len({item["sleeve_id"] for item in expected_lifecycle})
            == len(expected_lifecycle)
        ):
            return False

        observed_accounts: list[Mapping[str, Any]] = []
        for expected in expected_accounts:
            model = session.get(orm.ShadowAccountModel, expected["account_id"])
            if model is None:
                return False
            observed_accounts.append(
                {
                    "account_id": str(model.account_id),
                    "last_event_sequence": int(model.last_event_sequence),
                    "last_event_hash": str(model.last_event_hash),
                    "status": str(model.status),
                }
            )

        observed_bindings = tuple(
            {
                "binding_id": str(model.binding_id),
                "binding_hash": str(model.binding_hash),
                "role": str(model.role),
                "role_key": str(model.role_key),
                "account_id": str(model.account_id),
            }
            for model in session.execute(
                select(orm.ShadowRoleBindingModel)
                .where(
                    orm.ShadowRoleBindingModel.active.is_(True),
                    orm.ShadowRoleBindingModel.role.in_(
                        ("champion", "challenger")
                    ),
                )
                .order_by(
                    orm.ShadowRoleBindingModel.role,
                    orm.ShadowRoleBindingModel.role_key,
                    orm.ShadowRoleBindingModel.binding_id,
                )
            ).scalars()
        )
        observed_lifecycle: list[Mapping[str, Any]] = []
        for expected in expected_lifecycle:
            model = session.scalar(
                select(orm.LifecycleEventModel)
                .where(
                    orm.LifecycleEventModel.sleeve_id
                    == expected["sleeve_id"]
                )
                .order_by(
                    orm.LifecycleEventModel.occurred_at.desc(),
                    orm.LifecycleEventModel.event_id.desc(),
                )
                .limit(1)
            )
            if model is None:
                return False
            observed_lifecycle.append(
                {
                    "sleeve_id": str(model.sleeve_id),
                    "event_id": str(model.event_id),
                    "idempotency_key": str(model.idempotency_key),
                    "to_state": str(model.to_state),
                }
            )
        return (
            canonical_json(observed_accounts) == canonical_json(expected_accounts)
            and canonical_json(observed_bindings)
            == canonical_json(expected_bindings)
            and canonical_json(observed_lifecycle)
            == canonical_json(expected_lifecycle)
        )

    def _resolve_incident_with_effects(
        self,
        incident_id: str,
        *,
        resolved_at: datetime,
        evidence: (
            Mapping[str, Any]
            | Callable[
                [IncidentRecord, tuple[IncidentRecord, ...]], Mapping[str, Any]
            ]
        ),
        apply_effects: Callable[
            [IncidentRecord, tuple[IncidentRecord, ...]], Any
        ],
        superseded: bool = False,
        _allow_domain_incident: bool,
        _require_typed_effect_fence: bool,
    ) -> tuple[IncidentRecord, Any | None, bool]:
        """Apply idempotent effects outside SQL, then atomically fence terminal CAS.

        A durable ``revalidate_incident`` action owns the callback.  Claim and
        finalization are short transactions; no PostgreSQL row/advisory lock is
        retained while Python/catalog code runs.  Before terminalization the
        final transaction verifies both the fencing token and the exact set of
        other OPEN incidents observed by the callback.  A changed scope leaves
        this incident OPEN and the action PENDING for idempotent replay.
        """

        if not callable(apply_effects):
            raise TypeError("apply_effects must be callable")
        when = _aware(resolved_at, "resolved_at")
        terminal_status = (
            IncidentStatus.SUPERSEDED.value
            if superseded
            else IncidentStatus.RESOLVED.value
        )

        def exact_terminal(model: Any) -> tuple[IncidentRecord, None, bool]:
            if callable(evidence):
                raise ImmutablePartition(
                    "terminal incident retry requires persisted resolution evidence"
                )
            terminal_evidence = _canonical_json_mapping(
                evidence, name="resolution evidence"
            )
            terminal_hash = content_fingerprint(
                {
                    "incident_id": incident_id,
                    "resolved_at": when,
                    "evidence": terminal_evidence,
                    "superseded": bool(superseded),
                },
                domain="factor-lab/research-os/v1/data-incident-resolution",
            )
            if (
                model.status == terminal_status
                and _db_aware(model.resolved_at) == when
                and model.resolution_hash == terminal_hash
                and dict(model.payload_json or {}).get("resolution")
                == terminal_evidence
            ):
                return self._incident_record(model), None, False
            raise ImmutablePartition("data incident is already resolved")

        # Register the resolution action while briefly locking only the
        # incident row.  The callback starts after this transaction commits.
        with self._session.begin() as session:
            dialect = session.get_bind().dialect.name
            if dialect == "sqlite":
                session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            else:
                self._serialize_incident_mutations(session)
            query = select(orm.DataIncidentModel).where(
                orm.DataIncidentModel.incident_id == incident_id
            )
            if dialect == "postgresql":
                query = query.with_for_update()
            model = session.execute(query).scalar_one_or_none()
            if model is None:
                raise ProductionLedgerError("data incident was not found")
            authority = self._incident_record(model)
            origin_payload = dict(model.payload_json or {})
            origin_payload.pop("resolution", None)
            if (
                str(origin_payload.get("domain_incident_id") or "").strip()
                and not _allow_domain_incident
            ):
                raise ImmutablePartition(
                    "production domain incidents require typed five-stage revalidation"
                )
            if when < authority.occurred_at:
                raise ValueError("resolved_at cannot precede occurred_at")
            if model.status != IncidentStatus.OPEN.value:
                return exact_terminal(model)
            self.incident_controls.enqueue_in_session(
                session,
                incident_id=incident_id,
                created_at=when,
                action_kind=IncidentControlActionKind.REVALIDATE_INCIDENT,
            )

        try:
            lease = self.incident_controls.claim(
                incident_id,
                owner=f"incident-resolution-{incident_id[-64:]}",
                lease_for=timedelta(minutes=30),
                action_kind=IncidentControlActionKind.REVALIDATE_INCIDENT,
            )
        except IncidentControlLeaseConflict:
            with self._session() as session:
                current = session.get(orm.DataIncidentModel, incident_id)
                if current is not None and current.status != IncidentStatus.OPEN.value:
                    return exact_terminal(current)
            raise ImmutablePartition(
                "data incident is already resolved or being resolved"
            )
        if lease is None:
            with self._session() as session:
                current = session.get(orm.DataIncidentModel, incident_id)
                if current is not None and current.status != IncidentStatus.OPEN.value:
                    return exact_terminal(current)
            raise ImmutablePartition(
                "data incident is already resolved or being resolved"
            )

        # Capture one durable scope after owning the per-incident action.  New
        # incidents may still arrive while effects run; the final CAS detects
        # that phantom and forces an idempotent retry.
        with self._session() as session:
            current = session.get(orm.DataIncidentModel, incident_id)
            if current is None:
                self.incident_controls.release(
                    lease, error_code="IncidentAuthorityMissing"
                )
                raise ProductionLedgerError("data incident was not found")
            if current.status != IncidentStatus.OPEN.value:
                try:
                    self.incident_controls.release(
                        lease, error_code="IncidentAlreadyTerminal"
                    )
                finally:
                    return exact_terminal(current)
            authority = self._incident_record(current)
            other_query = (
                select(orm.DataIncidentModel)
                .where(
                    orm.DataIncidentModel.status == IncidentStatus.OPEN.value,
                    orm.DataIncidentModel.incident_id != incident_id,
                )
                .order_by(orm.DataIncidentModel.incident_id)
            )
            other_open = tuple(
                self._incident_record(item)
                for item in session.execute(other_query).scalars()
            )

        try:
            raw_resolution_evidence = (
                evidence(authority, other_open)
                if callable(evidence)
                else evidence
            )
            resolution_evidence = _canonical_json_mapping(
                raw_resolution_evidence,
                name="resolution evidence",
            )
            resolution_hash = content_fingerprint(
                {
                    "incident_id": incident_id,
                    "resolved_at": when,
                    "evidence": resolution_evidence,
                    "superseded": bool(superseded),
                },
                domain="factor-lab/research-os/v1/data-incident-resolution",
            )
            effect_result = apply_effects(authority, other_open)
        except BaseException as exc:
            try:
                self.incident_controls.release(
                    lease, error_code=type(exc).__name__
                )
            except Exception:
                pass
            raise

        scope_changed = False
        effect_fence_changed = False
        terminal_record: IncidentRecord | None = None
        try:
            with self._session.begin() as session:
                dialect = session.get_bind().dialect.name
                if dialect == "sqlite":
                    session.connection().exec_driver_sql("BEGIN IMMEDIATE")
                else:
                    self._serialize_incident_mutations(session)
                database_now = self.incident_controls._database_now(session)
                incident_model, action_model = (
                    self.incident_controls._lock_incident_then_action(
                        session,
                        incident_id=incident_id,
                        action_id=lease.action.action_id,
                    )
                )
                if action_model is None or incident_model is None:
                    raise IncidentControlLeaseConflict(
                        "resolution action authority disappeared"
                    )
                if not (
                    action_model.status
                    == IncidentControlActionStatus.RUNNING.value
                    and action_model.lease_owner == lease.action.lease_owner
                    and action_model.lease_token == lease.token
                    and int(action_model.fencing_token) == lease.fencing_token
                    and _db_aware(action_model.lease_expires_at) is not None
                    and database_now <= _db_aware(action_model.lease_expires_at)
                ):
                    raise IncidentControlLeaseConflict(
                        "resolution action lease is stale or expired"
                    )
                if incident_model.status != IncidentStatus.OPEN.value:
                    return exact_terminal(incident_model)

                if _require_typed_effect_fence and not (
                    self._typed_effect_fence_matches(session, effect_result)
                ):
                    action_model.status = IncidentControlActionStatus.PENDING.value
                    action_model.lease_owner = None
                    action_model.lease_token = None
                    action_model.lease_expires_at = None
                    action_model.last_error_code = "TypedEffectFenceChanged"
                    action_model.updated_at = database_now
                    effect_fence_changed = True

                locked_other_query = (
                    select(orm.DataIncidentModel)
                    .where(
                        orm.DataIncidentModel.status
                        == IncidentStatus.OPEN.value,
                        orm.DataIncidentModel.incident_id != incident_id,
                    )
                    .order_by(orm.DataIncidentModel.incident_id)
                )
                if dialect == "postgresql":
                    locked_other_query = locked_other_query.with_for_update()
                current_other_ids = tuple(
                    item.incident_id
                    for item in session.execute(locked_other_query).scalars()
                )
                expected_other_ids = tuple(
                    item.incident_id for item in other_open
                )
                if effect_fence_changed:
                    pass
                elif current_other_ids != expected_other_ids:
                    action_model.status = IncidentControlActionStatus.PENDING.value
                    action_model.lease_owner = None
                    action_model.lease_token = None
                    action_model.lease_expires_at = None
                    action_model.last_error_code = "IncidentScopeChanged"
                    action_model.updated_at = database_now
                    scope_changed = True
                else:
                    origin_payload = dict(incident_model.payload_json or {})
                    origin_payload.pop("resolution", None)
                    incident_model.status = terminal_status
                    incident_model.resolved_at = when
                    incident_model.resolution_hash = resolution_hash
                    incident_model.payload_json = {
                        **origin_payload,
                        "resolution": resolution_evidence,
                    }
                    action_result = {
                        "incident_id": incident_id,
                        "resolution_hash": resolution_hash,
                        "other_open_incident_ids": list(expected_other_ids),
                    }
                    action_model.status = (
                        IncidentControlActionStatus.SUCCEEDED.value
                    )
                    action_model.lease_owner = None
                    action_model.lease_token = None
                    action_model.lease_expires_at = None
                    action_model.result_json = action_result
                    action_model.result_hash = content_fingerprint(
                        {
                            "action_id": action_model.action_id,
                            "fencing_token": lease.fencing_token,
                            "result": action_result,
                        },
                        domain=(
                            "factor-lab/research-os/v1/incident-control-result"
                        ),
                    )
                    action_model.last_error_code = None
                    action_model.completed_at = database_now
                    action_model.updated_at = database_now
                    session.flush()
                    terminal_record = self._incident_record(incident_model)
        except IncidentControlLeaseConflict as exc:
            raise LeaseConflict(str(exc)) from exc

        if scope_changed:
            raise LeaseConflict(
                "open incident scope changed during revalidation; retry required"
            )
        if effect_fence_changed:
            raise TypedEffectFenceConflict(
                "typed Shadow/lifecycle effects changed before terminalization"
            )
        if terminal_record is None:
            raise ProductionLedgerError(
                "data incident terminal write produced no authority record"
            )
        return terminal_record, effect_result, True

    def record_resolved_incident(
        self,
        *,
        partition_key: str,
        stage: IncidentStage,
        error_code: str,
        message: str,
        occurred_at: datetime,
        resolved_at: datetime,
        resolution: Mapping[str, Any],
        superseded: bool = False,
        partition_run_id: str | None = None,
        source_ids: Sequence[str] = (),
        evidence_hashes: Sequence[str] = (),
        payload: Mapping[str, Any] | None = None,
        sensitive_values: Sequence[str] = (),
    ) -> IncidentRecord:
        """Create and terminalize one immutable incident in one transaction.

        The insert is conflict-safe across processes.  If an older caller has
        already created the same incident as ``open``, terminalization uses a
        row lock on PostgreSQL plus a compare-and-set update on every dialect.
        Consequently two different resolutions can never overwrite each
        other: the first terminal authority wins and an exact retry is the
        only terminal write accepted afterwards.
        """

        key = _safe_partition_key(partition_key)
        normalized_stage = IncidentStage(stage)
        code = _safe_identifier(error_code, "error_code")
        occurred = _aware(occurred_at, "occurred_at")
        resolved = _aware(resolved_at, "resolved_at")
        if resolved < occurred:
            raise ValueError("resolved_at cannot precede occurred_at")
        sources = tuple(
            sorted({_safe_identifier(item, "source_id") for item in source_ids})
        )
        evidence = tuple(
            sorted(
                {
                    _sha256(item, "evidence_hash", required=True) or ""
                    for item in evidence_hashes
                }
            )
        )
        origin_payload = _canonical_json_mapping(payload, name="payload")
        if "resolution" in origin_payload:
            raise ValueError("payload cannot contain the reserved resolution field")
        if str(origin_payload.get("domain_incident_id") or "").strip():
            raise ImmutablePartition(
                "production domain incidents cannot be atomically closed; "
                "typed five-stage revalidation is required"
            )
        incident_hash = content_fingerprint(
            {
                "partition_key": key,
                "stage": normalized_stage.value,
                "error_code": code,
                "occurred_at": occurred,
                "partition_run_id": partition_run_id,
                "source_ids": sources,
                "evidence_hashes": evidence,
                "payload": origin_payload,
            },
            domain="factor-lab/research-os/v1/data-incident",
        )
        incident_id = f"incident_{incident_hash[:64]}"
        resolution_evidence = _canonical_json_mapping(
            resolution, name="resolution evidence"
        )
        resolution_hash = content_fingerprint(
            {
                "incident_id": incident_id,
                "resolved_at": resolved,
                "evidence": resolution_evidence,
                "superseded": bool(superseded),
            },
            domain="factor-lab/research-os/v1/data-incident-resolution",
        )
        safe_message = sanitize_operational_text(
            message, sensitive_values=sensitive_values
        )
        terminal_status = (
            IncidentStatus.SUPERSEDED.value
            if superseded
            else IncidentStatus.RESOLVED.value
        )
        terminal_values = {
            "incident_id": incident_id,
            "incident_hash": incident_hash,
            "partition_run_id": partition_run_id,
            "partition_key": key,
            "stage": normalized_stage.value,
            "status": terminal_status,
            "error_code": code,
            "message": safe_message,
            "source_ids_json": list(sources),
            "evidence_hashes_json": list(evidence),
            "payload_json": {
                **origin_payload,
                "resolution": resolution_evidence,
            },
            "occurred_at": occurred,
            "resolved_at": resolved,
            "resolution_hash": resolution_hash,
        }

        with self._session.begin() as session:
            self._serialize_incident_mutations(session)
            dialect = session.get_bind().dialect.name
            if dialect == "postgresql":
                from sqlalchemy.dialects.postgresql import insert as dialect_insert

                statement = dialect_insert(orm.DataIncidentModel).values(
                    **terminal_values
                )
                session.execute(
                    statement.on_conflict_do_nothing(
                        index_elements=[orm.DataIncidentModel.incident_id]
                    )
                )
            elif dialect == "sqlite":
                from sqlalchemy.dialects.sqlite import insert as dialect_insert

                statement = dialect_insert(orm.DataIncidentModel).values(
                    **terminal_values
                )
                session.execute(
                    statement.on_conflict_do_nothing(
                        index_elements=[orm.DataIncidentModel.incident_id]
                    )
                )
            else:  # pragma: no cover - production is PostgreSQL; tests use SQLite.
                try:
                    with session.begin_nested():
                        session.add(orm.DataIncidentModel(**terminal_values))
                        session.flush()
                except IntegrityError:
                    pass

            query = select(orm.DataIncidentModel).where(
                orm.DataIncidentModel.incident_id == incident_id
            )
            if dialect == "postgresql":
                query = query.with_for_update()
            model = session.execute(query).scalar_one_or_none()
            if model is None:  # Defensive: an ignored conflict must name a real row.
                raise ProductionLedgerError(
                    "data incident conflict did not resolve to an authority row"
                )
            existing_origin_payload = dict(model.payload_json or {})
            existing_origin_payload.pop("resolution", None)
            immutable_origin_matches = bool(
                model.incident_hash == incident_hash
                and model.partition_run_id == partition_run_id
                and model.partition_key == key
                and model.stage == normalized_stage.value
                and model.error_code == code
                and model.message == safe_message
                and tuple(model.source_ids_json or ()) == sources
                and tuple(model.evidence_hashes_json or ()) == evidence
                and existing_origin_payload == origin_payload
                and _db_aware(model.occurred_at) == occurred
            )
            if not immutable_origin_matches:
                raise ImmutablePartition("data incident origin authority changed")
            if model.status != IncidentStatus.OPEN.value:
                if (
                    model.status == terminal_status
                    and _db_aware(model.resolved_at) == resolved
                    and model.resolution_hash == resolution_hash
                    and dict(model.payload_json or {}).get("resolution")
                    == resolution_evidence
                ):
                    return self._incident_record(model)
                raise ImmutablePartition("data incident is already resolved")

            # Keep the OPEN -> terminal transition a compare-and-set even when
            # the backend supports row locks.  This is the concurrency guard
            # for SQLite and a second line of defence for PostgreSQL workers.
            changed = session.execute(
                update(orm.DataIncidentModel)
                .where(
                    orm.DataIncidentModel.incident_id == incident_id,
                    orm.DataIncidentModel.status == IncidentStatus.OPEN.value,
                    orm.DataIncidentModel.resolved_at.is_(None),
                    orm.DataIncidentModel.resolution_hash.is_(None),
                )
                .values(
                    status=terminal_status,
                    resolved_at=resolved,
                    resolution_hash=resolution_hash,
                    payload_json={
                        **existing_origin_payload,
                        "resolution": resolution_evidence,
                    },
                )
                .execution_options(synchronize_session=False)
            )
            session.expire_all()
            winner = session.execute(query).scalar_one()
            if changed.rowcount == 1:
                return self._incident_record(winner)
            if (
                winner.status == terminal_status
                and _db_aware(winner.resolved_at) == resolved
                and winner.resolution_hash == resolution_hash
                and dict(winner.payload_json or {}).get("resolution")
                == resolution_evidence
            ):
                return self._incident_record(winner)
            raise ImmutablePartition("data incident is already resolved")

    def list_incidents(
        self, *, status: IncidentStatus | None = None, limit: int = 1_000
    ) -> tuple[IncidentRecord, ...]:
        if limit <= 0 or limit > 10_000:
            raise ValueError("limit must be between 1 and 10000")
        with self._session() as session:
            query = select(orm.DataIncidentModel)
            if status is not None:
                query = query.where(
                    orm.DataIncidentModel.status == IncidentStatus(status).value
                )
            models = session.execute(
                query.order_by(
                    orm.DataIncidentModel.occurred_at.desc(),
                    orm.DataIncidentModel.incident_id,
                ).limit(limit)
            ).scalars()
            return tuple(self._incident_record(model) for model in models)

    def iter_incidents(
        self,
        *,
        status: IncidentStatus | None = None,
        batch_size: int = 1_000,
    ) -> Iterator[IncidentRecord]:
        """Stream every incident from one stable, deterministically ordered query.

        Unlike :meth:`list_incidents`, this authority path never truncates the
        ledger.  SQLAlchemy's ``yield_per`` keeps the result bounded in memory
        while the single SELECT/cursor retains one statement snapshot.
        """

        if batch_size <= 0 or batch_size > 10_000:
            raise ValueError("batch_size must be between 1 and 10000")
        normalized_status = (
            None if status is None else IncidentStatus(status)
        )
        query = select(orm.DataIncidentModel)
        if normalized_status is not None:
            query = query.where(
                orm.DataIncidentModel.status == normalized_status.value
            )
        query = query.order_by(
            orm.DataIncidentModel.occurred_at.desc(),
            orm.DataIncidentModel.incident_id,
        ).execution_options(yield_per=batch_size)

        def stream() -> Iterator[IncidentRecord]:
            with self._session.begin() as session:
                models = session.execute(query).scalars()
                for model in models:
                    yield self._incident_record(model)

        return stream()


def load_runtime_authority_marker(
    database: str | Engine,
    *,
    connect_args: Mapping[str, Any] | None = None,
) -> RuntimeAuthorityMarker | None:
    """Read and validate a database's Alembic-owned authority marker."""

    with ProductionLedger(database, connect_args=connect_args) as ledger:
        return ledger.runtime_authority_marker()


__all__ = [
    "BackfillProgress",
    "CapabilityRecord",
    "CapabilityStatus",
    "ImmutablePartition",
    "INCIDENT_CONTROL_LOCK_KEYS",
    "IncidentRecord",
    "IncidentStage",
    "IncidentStatus",
    "LeaseConflict",
    "PartitionIdentity",
    "PartitionLease",
    "PartitionRecord",
    "PartitionStatus",
    "ProductionLedger",
    "ProductionLedgerError",
    "RUNTIME_AUTHORITY_HASH_DOMAIN",
    "RUNTIME_AUTHORITY_MARKER_KEY",
    "RUNTIME_AUTHORITY_SCHEMA",
    "RuntimeAuthorityError",
    "RuntimeAuthorityMarker",
    "TypedEffectFenceConflict",
    "load_runtime_authority_marker",
    "runtime_authority_marker_hash",
    "sanitize_operational_text",
]
