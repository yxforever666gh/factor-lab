"""Authoritative production ledgers for resumable data and shadow operations.

The historical scripts used artifact presence as a progress signal.  This
module deliberately does not inspect the filesystem: PostgreSQL records every
source/dataset/session lease and terminal outcome.  Successful partitions are
immutable, expired leases are safely reclaimable, and all terminal writes use
the lease token acquired by the worker.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
import re
from typing import Any, Iterable, Iterator, Mapping, Sequence

from .fingerprint import content_fingerprint

try:  # Keep lightweight analysis workers importable without infra extras.
    from sqlalchemy import MetaData, Table, and_, create_engine, func, inspect, or_, select
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.orm import Session, sessionmaker

    from . import orm
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


class ProductionLedgerError(RuntimeError):
    pass


class LeaseConflict(ProductionLedgerError):
    pass


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


_TERMINAL_PARTITION_STATUSES = {
    PartitionStatus.SUCCEEDED,
    PartitionStatus.DISPUTED,
    PartitionStatus.QUARANTINED,
}


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

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_id", _safe_identifier(self.source_id, "source_id"))
        object.__setattr__(self, "dataset", _safe_identifier(self.dataset, "dataset"))
        object.__setattr__(self, "partition_key", _safe_partition_key(self.partition_key))

    @property
    def partition_run_id(self) -> str:
        digest = content_fingerprint(
            {
                "source_id": self.source_id,
                "dataset": self.dataset,
                "partition_key": self.partition_key,
            },
            domain="factor-lab/research-os/v1/partition-run",
        )
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

    def close(self) -> None:
        if self._owns_engine:
            self.engine.dispose()

    def __enter__(self) -> "ProductionLedger":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

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
                model = orm.PartitionRunModel(
                    partition_run_id=identity.partition_run_id,
                    source_id=identity.source_id,
                    dataset=identity.dataset,
                    partition_key=identity.partition_key,
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
                observed = (model.source_id, model.dataset, model.partition_key)
                expected = (identity.source_id, identity.dataset, identity.partition_key)
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
            (item.source_id, item.dataset, item.partition_key): item
            for item in identities
        }
        return tuple(
            self.ensure_partition(identity, created_at=created_at)
            for identity in sorted(
                unique.values(),
                key=lambda item: (item.partition_key, item.source_id, item.dataset),
            )
        )

    def get_partition(self, identity: PartitionIdentity) -> PartitionRecord | None:
        with self._session() as session:
            model = session.get(orm.PartitionRunModel, identity.partition_run_id)
            return None if model is None else self._partition_record(model)

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
                    orm.PartitionRunModel.status == PartitionStatus.FAILED.value,
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
        incident_payload = {
            "partition_key": key,
            "stage": normalized_stage.value,
            "error_code": code,
            "occurred_at": when,
            "partition_run_id": partition_run_id,
            "source_ids": sources,
            "evidence_hashes": evidence,
            "payload": dict(payload or {}),
        }
        incident_hash = content_fingerprint(
            incident_payload, domain="factor-lab/research-os/v1/data-incident"
        )
        incident_id = f"incident_{incident_hash[:64]}"
        with self._session.begin() as session:
            model = session.get(orm.DataIncidentModel, incident_id)
            if model is None:
                model = orm.DataIncidentModel(
                    incident_id=incident_id,
                    incident_hash=incident_hash,
                    partition_run_id=partition_run_id,
                    partition_key=key,
                    stage=normalized_stage.value,
                    status=IncidentStatus.OPEN.value,
                    error_code=code,
                    message=safe_message,
                    source_ids_json=list(sources),
                    evidence_hashes_json=list(evidence),
                    payload_json=dict(payload or {}),
                    occurred_at=when,
                )
                session.add(model)
                session.flush()
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
        when = _aware(resolved_at, "resolved_at")
        resolution_hash = content_fingerprint(
            {
                "incident_id": incident_id,
                "resolved_at": when,
                "evidence": dict(evidence),
                "superseded": bool(superseded),
            },
            domain="factor-lab/research-os/v1/data-incident-resolution",
        )
        with self._session.begin() as session:
            model = session.get(orm.DataIncidentModel, incident_id)
            if model is None:
                raise ProductionLedgerError("data incident was not found")
            if model.status != IncidentStatus.OPEN.value:
                if model.resolution_hash == resolution_hash:
                    return self._incident_record(model)
                raise ImmutablePartition("data incident is already resolved")
            model.status = (
                IncidentStatus.SUPERSEDED.value
                if superseded
                else IncidentStatus.RESOLVED.value
            )
            model.resolved_at = when
            model.resolution_hash = resolution_hash
            model.payload_json = {
                **dict(model.payload_json or {}),
                "resolution": dict(evidence),
            }
            session.flush()
            return self._incident_record(model)

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
    "load_runtime_authority_marker",
    "runtime_authority_marker_hash",
    "sanitize_operational_text",
]
