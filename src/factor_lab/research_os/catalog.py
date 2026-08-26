"""Authoritative Research OS catalog and append-only evidence repositories.

PostgreSQL is the production store and is accessed through SQLAlchemy 2.x.
Tests and lightweight local tools use a native SQLite backend with identical
catalog semantics and no optional infrastructure dependencies.
"""

from __future__ import annotations

import json
import hashlib
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Protocol, Sequence
from uuid import uuid4
from zoneinfo import ZoneInfo

from .contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    ExperimentSpec,
    ExperimentStatus,
    LifecycleState,
    RecoveryCase,
    RecoveryCaseStatus,
    SnapshotTier,
    TrialOutcome,
)
from .fingerprint import canonical_json, content_fingerprint, experiment_fingerprint
from .governance import (
    EvidenceClass,
    TrialAdmissionDecision,
    TrialAdmissionStatus,
    TrialKind,
    TrialLedger,
    TrialRegistration,
)
from . import orm


RESEARCH_OS_ALEMBIC_HEAD = "0010_evidence_epoch_versions"

EVIDENCE_EPOCH_SCHEMA_VERSION = "research-os/evidence-epoch/v1"
EVIDENCE_EPOCH_SLOT = "research_os"
_SHADOW_BINDING_HASH_DOMAIN = "factor-lab/research-os/v1/shadow-role-binding"
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_FORMAL_OPEN_TIME = time(9, 30)
_EVIDENCE_EPOCH_LOCK_KEY = 4_604_770_998_302_851_428


class CatalogError(RuntimeError):
    pass


class MissingInfrastructureDependency(CatalogError):
    pass


class UnsupportedEvaluator(CatalogError):
    pass


class CatalogConflict(CatalogError):
    pass


class CatalogNotFound(CatalogError):
    pass


class AuthoritativeResultExists(CatalogConflict):
    pass


@dataclass(frozen=True)
class SnapshotRecord:
    reference: DataSnapshotRef
    created_at: datetime


@dataclass(frozen=True)
class EvidenceEpochRecord:
    """One immutable architecture freeze and its one-way forward window seal.

    ``epoch_hash`` addresses only facts known at architecture freeze time.  A
    pending epoch may later receive exactly one trusted exchange-calendar
    binding.  ``evidence_window_hash`` then addresses that one-way activation;
    neither payload can be overwritten or silently reinterpreted.
    """

    epoch_id: str
    architecture_version: str
    frozen_at: datetime
    code_hash: str
    configuration_hash: str
    dependency_lock_hash: str
    dirty_patch_hash: str
    epoch_hash: str
    first_forward_session: date | None = None
    calendar_snapshot_id: str | None = None
    calendar_snapshot_hash: str | None = None
    calendar_content_hash: str | None = None
    evidence_window_hash: str | None = None
    activated_at: datetime | None = None
    closed_at: datetime | None = None
    superseded_by_epoch_id: str | None = None
    schema_version: str = EVIDENCE_EPOCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "frozen_at", _require_aware(self.frozen_at, "frozen_at"))
        if self.activated_at is not None:
            object.__setattr__(
                self,
                "activated_at",
                _require_aware(self.activated_at, "activated_at"),
            )
        if self.closed_at is not None:
            object.__setattr__(
                self,
                "closed_at",
                _require_aware(self.closed_at, "closed_at"),
            )
        if self.schema_version != EVIDENCE_EPOCH_SCHEMA_VERSION:
            raise ValueError("unsupported evidence epoch schema")
        if not self.epoch_id or not self.architecture_version:
            raise ValueError("epoch_id and architecture_version are required")
        for name in (
            "code_hash",
            "configuration_hash",
            "dependency_lock_hash",
            "dirty_patch_hash",
            "epoch_hash",
        ):
            _require_sha256(str(getattr(self, name)), name=name)
        activation = (
            self.first_forward_session,
            self.calendar_snapshot_id,
            self.calendar_snapshot_hash,
            self.calendar_content_hash,
            self.evidence_window_hash,
            self.activated_at,
        )
        if any(item is not None for item in activation) and not all(
            item is not None for item in activation
        ):
            raise ValueError("evidence window activation fields must be all present or all absent")
        if self.calendar_snapshot_hash is not None:
            _require_sha256(self.calendar_snapshot_hash, name="calendar_snapshot_hash")
            _require_sha256(self.calendar_content_hash or "", name="calendar_content_hash")
            _require_sha256(self.evidence_window_hash or "", name="evidence_window_hash")

        expected_epoch_hash = content_fingerprint(
            self.freeze_payload(),
            domain="factor-lab/research-os/v1/evidence-epoch",
        )
        if expected_epoch_hash != self.epoch_hash:
            raise ValueError("evidence epoch hash differs from immutable freeze payload")
        if not self.epoch_id.endswith(self.epoch_hash[:32]):
            raise ValueError("evidence epoch id is not bound to its hash")
        if self.first_forward_session is not None:
            expected_window_hash = content_fingerprint(
                self.window_payload(),
                domain="factor-lab/research-os/v1/evidence-window",
            )
            if expected_window_hash != self.evidence_window_hash:
                raise ValueError("evidence window hash differs from activation payload")
        closure = (self.closed_at, self.superseded_by_epoch_id)
        if any(item is not None for item in closure) and not all(
            item is not None for item in closure
        ):
            raise ValueError("epoch closure fields must be both present or both absent")
        if self.closed_at is not None:
            if self.closed_at < self.frozen_at:
                raise ValueError("evidence epoch closure predates its freeze")
            if self.superseded_by_epoch_id == self.epoch_id:
                raise ValueError("evidence epoch cannot supersede itself")

    def freeze_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "architecture_version": self.architecture_version,
            "frozen_at": self.frozen_at.isoformat(),
            "code_hash": self.code_hash,
            "configuration_hash": self.configuration_hash,
            "dependency_lock_hash": self.dependency_lock_hash,
            "dirty_patch_hash": self.dirty_patch_hash,
        }

    def window_payload(self) -> dict[str, Any]:
        if self.first_forward_session is None:
            raise ValueError("pending evidence epoch has no forward window payload")
        return {
            "epoch_id": self.epoch_id,
            "epoch_hash": self.epoch_hash,
            "first_forward_session": self.first_forward_session.isoformat(),
            "calendar_snapshot_id": self.calendar_snapshot_id,
            "calendar_snapshot_hash": self.calendar_snapshot_hash,
            "calendar_content_hash": self.calendar_content_hash,
            "activated_at": self.activated_at.isoformat() if self.activated_at else None,
        }

    @property
    def forward_holdout_id(self) -> str | None:
        if self.first_forward_session is None or self.evidence_window_hash is None:
            return None
        return f"forward:{self.epoch_id}:{self.evidence_window_hash}"

    @property
    def lifecycle_status(self) -> str:
        if self.closed_at is not None:
            return "closed"
        if self.activated_at is not None:
            return "active"
        return "pending"


def new_evidence_epoch(
    *,
    architecture_version: str,
    frozen_at: datetime,
    code_hash: str,
    configuration_hash: str,
    dependency_lock_hash: str,
    dirty_patch_hash: str,
) -> EvidenceEpochRecord:
    payload = {
        "schema_version": EVIDENCE_EPOCH_SCHEMA_VERSION,
        "architecture_version": str(architecture_version),
        "frozen_at": _require_aware(frozen_at, "frozen_at").isoformat(),
        "code_hash": _require_sha256(code_hash, name="code_hash"),
        "configuration_hash": _require_sha256(
            configuration_hash, name="configuration_hash"
        ),
        "dependency_lock_hash": _require_sha256(
            dependency_lock_hash, name="dependency_lock_hash"
        ),
        "dirty_patch_hash": _require_sha256(
            dirty_patch_hash, name="dirty_patch_hash"
        ),
    }
    epoch_hash = content_fingerprint(
        payload, domain="factor-lab/research-os/v1/evidence-epoch"
    )
    return EvidenceEpochRecord(
        epoch_id=f"epoch_{epoch_hash[:32]}",
        architecture_version=str(architecture_version),
        frozen_at=frozen_at,
        code_hash=code_hash,
        configuration_hash=configuration_hash,
        dependency_lock_hash=dependency_lock_hash,
        dirty_patch_hash=dirty_patch_hash,
        epoch_hash=epoch_hash,
    )


@dataclass(frozen=True)
class SnapshotPageCursor:
    """Opaque position in the catalog's stable snapshot ordering."""

    as_of: datetime
    created_at: datetime
    snapshot_id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of", _require_aware(self.as_of, "as_of"))
        object.__setattr__(
            self,
            "created_at",
            _require_aware(self.created_at, "created_at"),
        )
        if not self.snapshot_id:
            raise ValueError("snapshot_id is required")

    @classmethod
    def from_record(cls, record: SnapshotRecord) -> "SnapshotPageCursor":
        return cls(
            as_of=record.reference.as_of,
            created_at=record.created_at,
            snapshot_id=record.reference.snapshot_id,
        )


@dataclass(frozen=True)
class SnapshotPage:
    """One bounded keyset page and proof of whether another page exists."""

    records: tuple[SnapshotRecord, ...]
    next_cursor: SnapshotPageCursor | None


@dataclass(frozen=True)
class ExperimentRecord:
    experiment_id: str
    fingerprint: str
    status: ExperimentStatus
    spec: ExperimentSpec
    registered_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ExperimentResultRecord:
    result_id: str
    experiment_id: str
    result_hash: str
    outcome: str
    metrics: dict[str, Any]
    artifact_uri: str | None
    completed_at: datetime
    authoritative: bool = True


@dataclass(frozen=True)
class TrialLedgerEntry:
    family: str
    candidate_id: str
    outcome: TrialOutcome
    reason: str
    occurred_at: datetime
    experiment_id: str | None = None
    p_value: float | None = None
    alpha_spent: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)
    trial_id: str = field(default_factory=lambda: f"trl_{uuid4().hex}")
    admission_status: TrialAdmissionStatus = field(
        default=TrialAdmissionStatus.ADMITTED, compare=False
    )
    experiment_fingerprint: str | None = field(default=None, compare=False)
    research_equivalence_hash: str | None = field(default=None, compare=False)
    completed_at: datetime | None = field(default=None, compare=False)
    updated_at: datetime | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.outcome, TrialOutcome):
            object.__setattr__(self, "outcome", TrialOutcome(self.outcome))
        if not isinstance(self.admission_status, TrialAdmissionStatus):
            object.__setattr__(
                self,
                "admission_status",
                TrialAdmissionStatus(self.admission_status),
            )
        _require_aware(self.occurred_at, "occurred_at")
        if self.completed_at is not None:
            _require_aware(self.completed_at, "completed_at")
        if self.updated_at is not None:
            _require_aware(self.updated_at, "updated_at")
        if self.p_value is not None and not 0.0 <= self.p_value <= 1.0:
            raise ValueError("p_value must be between 0 and 1")
        if not 0.0 <= self.alpha_spent <= 1.0:
            raise ValueError("alpha_spent must be between 0 and 1")
        if not self.family or not self.candidate_id or not self.reason:
            raise ValueError("family, candidate_id, and reason are required")
        if self.research_equivalence_hash is not None:
            _require_sha256(
                self.research_equivalence_hash, name="research_equivalence_hash"
            )


@dataclass(frozen=True)
class TrialReservationRecord:
    """Result of a database-atomic trial budget reservation."""

    entry: TrialLedgerEntry
    admission: TrialAdmissionDecision
    created: bool


@dataclass(frozen=True)
class LifecycleEvent:
    idempotency_key: str
    sleeve_id: str
    to_state: LifecycleState
    cause: str
    occurred_at: datetime
    from_state: LifecycleState | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: f"evt_{uuid4().hex}")

    def __post_init__(self) -> None:
        if self.from_state is not None and not isinstance(self.from_state, LifecycleState):
            object.__setattr__(self, "from_state", LifecycleState(self.from_state))
        if not isinstance(self.to_state, LifecycleState):
            object.__setattr__(self, "to_state", LifecycleState(self.to_state))
        _require_aware(self.occurred_at, "occurred_at")
        if not self.idempotency_key or not self.sleeve_id or not self.cause:
            raise ValueError("idempotency_key, sleeve_id, and cause are required")


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    run_type: str
    status: str
    input_fingerprint: str
    started_at: datetime
    metadata: Mapping[str, Any] = field(default_factory=dict)
    completed_at: datetime | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        _require_aware(self.started_at, "started_at")
        if self.completed_at is not None:
            _require_aware(self.completed_at, "completed_at")
            if self.completed_at < self.started_at:
                raise ValueError("completed_at cannot be before started_at")
        if not self.run_id or not self.run_type or not self.status:
            raise ValueError("run_id, run_type, and status are required")
        if len(self.input_fingerprint) != 64 or any(
            character not in "0123456789abcdef"
            for character in self.input_fingerprint
        ):
            raise ValueError("input_fingerprint must be a lowercase SHA-256 digest")


@dataclass(frozen=True)
class ResearchFamilyRecord:
    family_id: str
    display_name: str
    mechanism_key: str
    cluster_id: str
    allowed_fields: tuple[str, ...]
    field_registry: tuple[Mapping[str, Any], ...]
    created_at: datetime
    active: bool = True
    registry_hash: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "created_at", _require_aware(self.created_at, "created_at"))
        for name in ("family_id", "display_name", "mechanism_key", "cluster_id"):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} is required")
        fields = tuple(sorted(set(map(str, self.allowed_fields))))
        if not fields:
            raise ValueError("a research family requires allowed fields")
        object.__setattr__(self, "allowed_fields", fields)
        registry = tuple(
            sorted(
                (json.loads(canonical_json(item)) for item in self.field_registry),
                key=lambda item: str(item.get("name") or ""),
            )
        )
        if {str(item.get("name") or "") for item in registry} != set(fields):
            raise ValueError("family field registry must exactly cover allowed_fields")
        object.__setattr__(self, "field_registry", registry)
        expected = content_fingerprint(
            self.definition(),
            domain="factor-lab/research-os/v1/family-registry",
        )
        if self.registry_hash is None:
            object.__setattr__(self, "registry_hash", expected)
        elif self.registry_hash != expected:
            raise ValueError("family registry hash differs from its definition")

    def definition(self) -> dict[str, Any]:
        return {
            "family_id": self.family_id,
            "display_name": self.display_name,
            "mechanism_key": self.mechanism_key,
            "cluster_id": self.cluster_id,
            "allowed_fields": list(self.allowed_fields),
            "field_registry": list(self.field_registry),
            "active": self.active,
        }


SUBMISSION_STATUSES = frozenset(
    {"reviewed", "reserved", "running", "completed", "failed", "missing_data"}
)
TERMINAL_SUBMISSION_STATUSES = frozenset({"completed", "failed", "missing_data"})
_RESEARCH_SUBMISSION_LEASE_DOMAIN = "factor-lab/research-os/v1/research-submission-lease"


@dataclass(frozen=True)
class ResearchSubmissionRecord:
    submission_id: str
    proposal_decision_id: str
    family_id: str
    status: str
    research_equivalence_hash: str
    experiment_fingerprint: str
    trial_id: str
    spec: ExperimentSpec
    created_at: datetime
    updated_at: datetime
    recovery_case_id: str | None = None
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    attempts: int = 0
    experiment_id: str | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "created_at", _require_aware(self.created_at, "created_at"))
        object.__setattr__(self, "updated_at", _require_aware(self.updated_at, "updated_at"))
        if self.lease_expires_at is not None:
            object.__setattr__(
                self,
                "lease_expires_at",
                _require_aware(self.lease_expires_at, "lease_expires_at"),
            )
        if self.status not in SUBMISSION_STATUSES:
            raise ValueError(f"unsupported submission status {self.status!r}")
        if not all((self.submission_id, self.proposal_decision_id, self.family_id, self.trial_id)):
            raise ValueError("submission identity fields are required")
        _require_sha256(self.research_equivalence_hash, name="research_equivalence_hash")
        _require_sha256(self.experiment_fingerprint, name="experiment_fingerprint")
        if self.spec.fingerprint() != self.experiment_fingerprint:
            raise ValueError("submission experiment fingerprint is corrupt")
        if self.attempts < 0:
            raise ValueError("submission attempts cannot be negative")
        if self.status == "running" and (
            not self.lease_owner or self.lease_expires_at is None
        ):
            raise ValueError("running submission requires an owner and lease expiry")
        if self.status in TERMINAL_SUBMISSION_STATUSES and (
            self.lease_owner is not None or self.lease_expires_at is not None
        ):
            raise ValueError("terminal submission cannot retain a lease")


def research_submission_lease_token(submission: ResearchSubmissionRecord) -> str:
    """Return the deterministic fencing token for one claimed lease generation.

    ``attempts`` is incremented atomically whenever an expired submission is
    reclaimed. Binding the token to that generation prevents a stale worker
    from finishing work after either another worker, or a new incarnation of
    the same worker id, has taken over the submission.
    """

    if (
        submission.status != "running"
        or not submission.lease_owner
        or submission.lease_expires_at is None
        or submission.attempts < 1
    ):
        raise ValueError("a lease token requires an active research submission lease")
    return content_fingerprint(
        {
            "submission_id": submission.submission_id,
            "experiment_fingerprint": submission.experiment_fingerprint,
            "lease_owner": submission.lease_owner,
            "lease_generation": submission.attempts,
        },
        domain=_RESEARCH_SUBMISSION_LEASE_DOMAIN,
    )


@dataclass(frozen=True)
class LegacyEvidenceRecord:
    source_uri: str
    content_hash: str
    trust_label: str
    reasons: tuple[str, ...]
    imported_at: datetime
    evidence_id: str | None = None

    def __post_init__(self) -> None:
        _require_aware(self.imported_at, "imported_at")
        if not self.source_uri or not self.trust_label or not self.reasons:
            raise ValueError("source_uri, trust_label, and at least one reason are required")
        if len(self.content_hash) != 64 or any(
            character not in "0123456789abcdef" for character in self.content_hash
        ):
            raise ValueError("content_hash must be a lowercase SHA-256 digest")
        if self.evidence_id is None:
            fingerprint = content_fingerprint(
                {"source_uri": self.source_uri, "content_hash": self.content_hash},
                domain="factor-lab/research-os/v1/legacy-evidence",
            )
            object.__setattr__(self, "evidence_id", f"leg_{fingerprint[:32]}")


@dataclass(frozen=True)
class ShadowAccountRecord:
    account_id: str
    name: str
    currency: str
    initial_capital: float
    cash: float
    nav: float
    benchmark_nav: float
    status: str
    as_of: datetime
    last_event_sequence: int
    last_event_hash: str


@dataclass(frozen=True)
class ShadowPositionRecord:
    account_id: str
    ticker: str
    quantity: float
    average_cost: float
    market_price: float
    market_value: float
    updated_at: datetime
    last_event_sequence: int


@dataclass(frozen=True)
class ShadowEvent:
    event_id: str
    account_id: str
    sequence_number: int
    event_type: str
    occurred_at: datetime
    payload: dict[str, Any]
    previous_event_hash: str
    event_hash: str


@dataclass(frozen=True)
class ShadowEventInput:
    event_type: str
    occurred_at: datetime
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_event_type(self.event_type)
        _require_aware(self.occurred_at, "occurred_at")
        _validate_shadow_payload(self.payload)


@dataclass(frozen=True)
class CatalogSummary:
    generated_at: datetime
    totals: dict[str, int]
    experiment_statuses: dict[str, int]
    data_quality_statuses: dict[str, int]
    lifecycle_states: dict[str, int]
    recovery_statuses: dict[str, int]
    latest_run_started_at: datetime | None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _require_aware(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    return value.astimezone(timezone.utc)


def _require_sha256(value: str, *, name: str) -> str:
    normalized = str(value)
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return normalized


def _parse_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _time_text(value: datetime) -> str:
    return _require_aware(value, "timestamp").isoformat(timespec="microseconds")


def _json_tree(value: Any) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", exclude_none=False)
    return json.loads(canonical_json(value))


def _run_is_terminal(run: RunRecord) -> bool:
    """Return whether a durable run has left its sole mutable state."""

    return run.status != "running"


def _run_records_equivalent(left: RunRecord, right: RunRecord) -> bool:
    """Compare the complete authority-bearing representation of two runs."""

    return (
        left.run_id == right.run_id
        and left.run_type == right.run_type
        and left.status == right.status
        and left.input_fingerprint == right.input_fingerprint
        and _require_aware(left.started_at, "started_at")
        == _require_aware(right.started_at, "started_at")
        and (
            None
            if left.completed_at is None
            else _require_aware(left.completed_at, "completed_at")
        )
        == (
            None
            if right.completed_at is None
            else _require_aware(right.completed_at, "completed_at")
        )
        and canonical_json(left.metadata) == canonical_json(right.metadata)
        and left.error == right.error
    )


def _assert_run_identity(existing: RunRecord, proposed: RunRecord) -> None:
    if (
        existing.run_id != proposed.run_id
        or existing.run_type != proposed.run_type
        or existing.input_fingerprint != proposed.input_fingerprint
        or _require_aware(existing.started_at, "started_at")
        != _require_aware(proposed.started_at, "started_at")
    ):
        raise CatalogConflict(f"run identity collision for {proposed.run_id!r}")


def _assert_terminal_run_replay(existing: RunRecord, proposed: RunRecord) -> None:
    """Allow a terminal run to be observed again, never reinterpreted."""

    if not _run_records_equivalent(existing, proposed):
        raise CatalogConflict(
            f"terminal run {proposed.run_id!r} is immutable; "
            f"stored status={existing.status!r}, proposed status={proposed.status!r}"
        )


def _validate_limit(limit: int) -> int:
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
        raise ValueError("limit must be an integer between 1 and 1000")
    return limit


_ZERO_EVENT_HASH = "0" * 64
_FORWARD_ONLY_KEY_FRAGMENTS = (
    "forward_return",
    "forward_label",
    "future_return",
    "future_label",
    "target_return",
    "return_label",
    "label_",
    "next_",
    "fwd_",
    "lead_",
)


def _validate_shadow_payload(value: Any, *, path: str = "payload") -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key)
            normalized = key.lower()
            if any(fragment in normalized for fragment in _FORWARD_ONLY_KEY_FRAGMENTS):
                raise ValueError(f"forward-only field is forbidden in shadow events: {path}.{key}")
            _validate_shadow_payload(child, path=f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _validate_shadow_payload(child, path=f"{path}[{index}]")


def _validate_event_type(event_type: str) -> str:
    if not event_type or len(event_type) > 80:
        raise ValueError("event_type must contain 1 to 80 characters")
    if not all(character.isalnum() or character == "_" for character in event_type):
        raise ValueError("event_type must use only letters, numbers, and underscores")
    return event_type


def _shadow_event_hash(
    *,
    account_id: str,
    sequence_number: int,
    event_type: str,
    occurred_at: datetime,
    payload: Mapping[str, Any],
    previous_event_hash: str,
) -> str:
    return content_fingerprint(
        {
            "account_id": account_id,
            "sequence_number": sequence_number,
            "event_type": event_type,
            "occurred_at": occurred_at,
            "payload": payload,
            "previous_event_hash": previous_event_hash,
        },
        domain="factor-lab/research-os/v1/shadow-event",
    )


def _projection_values(payload: Mapping[str, Any]) -> tuple[dict[str, float], dict[str, Any] | None]:
    account_state_raw = payload.get("account_state", {})
    if account_state_raw is None:
        account_state_raw = {}
    if not isinstance(account_state_raw, Mapping):
        raise ValueError("account_state must be a mapping")
    account_state: dict[str, float] = {}
    for key in ("cash", "nav", "benchmark_nav"):
        if key in account_state_raw:
            number = float(account_state_raw[key])
            if number < -1e-8:
                raise ValueError(f"account_state.{key} cannot be negative")
            account_state[key] = max(number, 0.0)

    position_raw = payload.get("position_state")
    if position_raw is None:
        return account_state, None
    if not isinstance(position_raw, Mapping):
        raise ValueError("position_state must be a mapping")
    required = {"ticker", "quantity", "average_cost", "market_price", "market_value"}
    missing = required - set(position_raw)
    if missing:
        raise ValueError(f"position_state is missing: {', '.join(sorted(missing))}")
    position = {
        "ticker": str(position_raw["ticker"]),
        "quantity": float(position_raw["quantity"]),
        "average_cost": float(position_raw["average_cost"]),
        "market_price": float(position_raw["market_price"]),
        "market_value": float(position_raw["market_value"]),
    }
    if not position["ticker"]:
        raise ValueError("position_state.ticker cannot be empty")
    for key in ("quantity", "average_cost", "market_price", "market_value"):
        if position[key] < -1e-8:
            raise ValueError(f"position_state.{key} cannot be negative in a long-only account")
        position[key] = max(position[key], 0.0)
    return account_state, position


def _normalize_shadow_event_inputs(
    events: Sequence[ShadowEventInput | Mapping[str, Any]],
) -> list[tuple[str, datetime, dict[str, Any], dict[str, float], dict[str, Any] | None]]:
    if not events:
        raise ValueError("an atomic shadow step requires at least one event")
    normalized = []
    for raw_event in events:
        if isinstance(raw_event, ShadowEventInput):
            event = raw_event
        elif isinstance(raw_event, Mapping):
            unknown = set(raw_event) - {"event_type", "occurred_at", "payload"}
            if unknown:
                raise ValueError(
                    f"unknown shadow event input fields: {', '.join(sorted(unknown))}"
                )
            try:
                event = ShadowEventInput(
                    event_type=str(raw_event["event_type"]),
                    occurred_at=raw_event["occurred_at"],
                    payload=raw_event.get("payload", {}),
                )
            except KeyError as exc:
                raise ValueError(f"shadow event input is missing {exc.args[0]}") from exc
        else:
            raise TypeError("shadow events must be ShadowEventInput objects or mappings")
        event_type = _validate_event_type(event.event_type)
        occurred_at = _require_aware(event.occurred_at, "occurred_at")
        _validate_shadow_payload(event.payload)
        payload = json.loads(canonical_json(event.payload))
        account_state, position_state = _projection_values(payload)
        normalized.append(
            (event_type, occurred_at, payload, account_state, position_state)
        )
    return normalized


class _CatalogBackend(Protocol):
    def initialize_schema(self) -> None: ...

    def close(self) -> None: ...

    def database_now(self) -> datetime: ...

    def freeze_evidence_epoch(self, epoch: EvidenceEpochRecord) -> EvidenceEpochRecord: ...

    def get_evidence_epoch(self) -> EvidenceEpochRecord | None: ...

    def get_pending_evidence_epoch(self) -> EvidenceEpochRecord | None: ...

    def list_evidence_epochs(self, *, limit: int) -> list[EvidenceEpochRecord]: ...

    def activate_evidence_epoch(
        self,
        *,
        epoch_id: str,
        expected_epoch_hash: str,
        first_forward_session: date,
        calendar_snapshot_id: str,
        calendar_snapshot_hash: str,
        calendar_content_hash: str,
        activated_at: datetime,
    ) -> EvidenceEpochRecord: ...

    def register_snapshot(self, reference: DataSnapshotRef) -> SnapshotRecord: ...

    def get_snapshot(self, snapshot_id: str) -> SnapshotRecord | None: ...

    def list_snapshots(
        self,
        *,
        limit: int,
        quality_status: DataQualityStatus | None,
        tier: SnapshotTier | None,
    ) -> list[SnapshotRecord]: ...

    def list_snapshot_page(
        self,
        *,
        limit: int,
        quality_status: DataQualityStatus | None,
        tier: SnapshotTier | None,
        after: SnapshotPageCursor | None,
    ) -> SnapshotPage: ...

    def register_experiment(self, spec: ExperimentSpec) -> ExperimentRecord: ...

    def get_experiment(self, experiment_id: str) -> ExperimentRecord | None: ...

    def get_experiment_by_fingerprint(self, fingerprint: str) -> ExperimentRecord | None: ...

    def list_experiments(
        self,
        *,
        limit: int,
        status: ExperimentStatus | None,
        family: str | None,
        candidate_id: str | None,
    ) -> list[ExperimentRecord]: ...

    def set_experiment_status(
        self, experiment_id: str, status: ExperimentStatus
    ) -> ExperimentRecord: ...

    def record_authoritative_result(
        self,
        experiment_id: str,
        *,
        outcome: str,
        metrics: Mapping[str, Any],
        artifact_uri: str | None,
        completed_at: datetime,
    ) -> ExperimentResultRecord: ...

    def get_authoritative_result(
        self, experiment_id: str
    ) -> ExperimentResultRecord | None: ...

    def append_trial(self, entry: TrialLedgerEntry) -> TrialLedgerEntry: ...

    def reserve_trial(
        self,
        registration: TrialRegistration,
        *,
        candidate_id: str,
        experiment_id: str | None,
        maximum_monthly_confirmatory_trials: int,
        maximum_monthly_confirmatory_trials_per_family: int,
        maximum_diagnostic_branches: int,
    ) -> TrialReservationRecord: ...

    def complete_trial(
        self,
        trial_id: str,
        *,
        outcome: TrialOutcome,
        reason: str,
        p_value: float | None,
        alpha_spent: float,
        metadata: Mapping[str, Any],
        experiment_id: str | None,
        completed_at: datetime,
    ) -> TrialLedgerEntry: ...

    def get_trial(self, trial_id: str) -> TrialLedgerEntry | None: ...

    def list_trials(self, *, family: str | None = None) -> list[TrialLedgerEntry]: ...

    def register_research_family(
        self, family: ResearchFamilyRecord
    ) -> ResearchFamilyRecord: ...

    def get_research_family(self, family_id: str) -> ResearchFamilyRecord | None: ...

    def list_research_families(
        self, *, active_only: bool
    ) -> list[ResearchFamilyRecord]: ...

    def create_research_submission(
        self, submission: ResearchSubmissionRecord
    ) -> ResearchSubmissionRecord: ...

    def get_research_submission(
        self, submission_id: str
    ) -> ResearchSubmissionRecord | None: ...

    def list_research_submissions(
        self, *, limit: int, status: str | None, family_id: str | None
    ) -> list[ResearchSubmissionRecord]: ...

    def reserve_research_submission(
        self, submission_id: str, *, reserved_at: datetime
    ) -> ResearchSubmissionRecord: ...

    def claim_research_submission(
        self,
        submission_id: str,
        *,
        worker_id: str,
        claimed_at: datetime,
        lease_expires_at: datetime,
    ) -> tuple[ResearchSubmissionRecord, bool]: ...

    def renew_research_submission(
        self,
        submission_id: str,
        *,
        worker_id: str,
        lease_token: str,
        renewed_at: datetime,
        lease_expires_at: datetime,
    ) -> ResearchSubmissionRecord: ...

    def finish_research_submission(
        self,
        submission_id: str,
        *,
        worker_id: str,
        lease_token: str | None,
        status: str,
        finished_at: datetime,
        experiment_id: str | None,
        error: str | None,
    ) -> ResearchSubmissionRecord: ...

    def append_lifecycle_event(self, event: LifecycleEvent) -> LifecycleEvent: ...

    def list_lifecycle_events(
        self, *, limit: int, sleeve_id: str | None
    ) -> list[LifecycleEvent]: ...

    def iter_lifecycle_events(
        self,
        *,
        sleeve_id: str | None,
        cause: str | None,
        batch_size: int,
    ) -> Iterator[LifecycleEvent]: ...

    def latest_lifecycle_state(self, sleeve_id: str) -> LifecycleState | None: ...

    def save_recovery_case(self, case: RecoveryCase) -> RecoveryCase: ...

    def get_recovery_case(self, recovery_case_id: str) -> RecoveryCase | None: ...

    def list_recovery_cases(
        self,
        *,
        limit: int,
        status: RecoveryCaseStatus | None,
        sleeve_id: str | None,
    ) -> list[RecoveryCase]: ...

    def iter_recovery_cases(
        self,
        *,
        statuses: Sequence[RecoveryCaseStatus] | None,
        sleeve_id: str | None,
        batch_size: int,
    ) -> Iterator[RecoveryCase]: ...

    def save_run(self, run: RunRecord) -> RunRecord: ...

    def claim_run(self, run: RunRecord) -> tuple[RunRecord, bool]: ...

    def get_run(self, run_id: str) -> RunRecord | None: ...

    def list_runs(
        self, *, limit: int, status: str | None, run_type: str | None
    ) -> list[RunRecord]: ...

    def import_legacy_evidence(self, evidence: LegacyEvidenceRecord) -> LegacyEvidenceRecord: ...

    def list_legacy_evidence(
        self, *, limit: int, trust_label: str | None
    ) -> list[LegacyEvidenceRecord]: ...

    def create_shadow_account(
        self,
        *,
        account_id: str,
        name: str,
        initial_capital: float,
        opened_at: datetime,
        currency: str,
    ) -> ShadowAccountRecord: ...

    def get_shadow_account(self, account_id: str) -> ShadowAccountRecord | None: ...

    def list_shadow_accounts(
        self, *, limit: int, status: str | None
    ) -> list[ShadowAccountRecord]: ...

    def iter_shadow_accounts(
        self, *, status: str | None, batch_size: int
    ) -> Iterator[ShadowAccountRecord]: ...

    def append_shadow_event(
        self,
        *,
        account_id: str,
        event_type: str,
        occurred_at: datetime,
        payload: Mapping[str, Any],
        expected_previous_hash: str | None,
    ) -> ShadowEvent: ...

    def append_shadow_events_atomic(
        self,
        *,
        account_id: str,
        events: Sequence[ShadowEventInput | Mapping[str, Any]],
        expected_previous_hash: str | None,
    ) -> list[ShadowEvent]: ...

    def list_shadow_events(self, *, account_id: str, limit: int) -> list[ShadowEvent]: ...

    def list_shadow_events_by_type(
        self,
        *,
        account_id: str,
        event_type: str,
        since: datetime | None,
        through: datetime | None,
        limit: int,
    ) -> list[ShadowEvent]: ...

    def iter_shadow_events_by_type(
        self,
        *,
        account_id: str,
        event_type: str,
        since: datetime | None,
        through: datetime | None,
        batch_size: int,
    ) -> Iterator[ShadowEvent]: ...

    def count_shadow_sessions(
        self, *, account_id: str, since: date, through: date
    ) -> int: ...

    def list_shadow_positions(self, account_id: str) -> list[ShadowPositionRecord]: ...

    def verify_shadow_chain(self, account_id: str) -> bool: ...

    def catalog_summary(self) -> CatalogSummary: ...


_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS ros_schema_metadata (
    component TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    installed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ros_evidence_epochs (
    epoch_slot TEXT PRIMARY KEY,
    epoch_id TEXT NOT NULL UNIQUE,
    schema_version TEXT NOT NULL,
    architecture_version TEXT NOT NULL,
    frozen_at TEXT NOT NULL,
    code_hash TEXT NOT NULL,
    configuration_hash TEXT NOT NULL,
    dependency_lock_hash TEXT NOT NULL,
    dirty_patch_hash TEXT NOT NULL,
    epoch_hash TEXT NOT NULL UNIQUE,
    first_forward_session TEXT,
    calendar_snapshot_id TEXT REFERENCES ros_data_snapshots(snapshot_id),
    calendar_snapshot_hash TEXT,
    calendar_content_hash TEXT,
    evidence_window_hash TEXT UNIQUE,
    activated_at TEXT,
    closed_at TEXT,
    superseded_by_epoch_id TEXT REFERENCES ros_evidence_epochs(epoch_id),
    CHECK (
        (first_forward_session IS NULL AND calendar_snapshot_id IS NULL
         AND calendar_snapshot_hash IS NULL AND calendar_content_hash IS NULL
         AND evidence_window_hash IS NULL AND activated_at IS NULL)
        OR
        (first_forward_session IS NOT NULL AND calendar_snapshot_id IS NOT NULL
         AND calendar_snapshot_hash IS NOT NULL AND calendar_content_hash IS NOT NULL
         AND evidence_window_hash IS NOT NULL AND activated_at IS NOT NULL)
    ),
    CHECK (
        (closed_at IS NULL AND superseded_by_epoch_id IS NULL)
        OR (closed_at IS NOT NULL AND superseded_by_epoch_id IS NOT NULL)
    )
);
CREATE INDEX IF NOT EXISTS ix_ros_evidence_epochs_frozen_at
    ON ros_evidence_epochs(frozen_at);
CREATE UNIQUE INDEX IF NOT EXISTS uq_ros_evidence_epoch_one_active
    ON ros_evidence_epochs((1))
    WHERE activated_at IS NOT NULL AND closed_at IS NULL;
CREATE TABLE IF NOT EXISTS ros_evidence_epoch_active_pointer (
    pointer_key TEXT PRIMARY KEY CHECK(pointer_key = 'research_os'),
    epoch_id TEXT NOT NULL UNIQUE REFERENCES ros_evidence_epochs(epoch_id),
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ros_data_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    tier TEXT NOT NULL,
    uri TEXT NOT NULL,
    content_hash TEXT NOT NULL UNIQUE,
    as_of TEXT NOT NULL,
    quality_status TEXT NOT NULL,
    ref_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_ros_data_snapshots_as_of
    ON ros_data_snapshots(as_of);
CREATE INDEX IF NOT EXISTS ix_ros_data_snapshots_quality
    ON ros_data_snapshots(quality_status);
CREATE TABLE IF NOT EXISTS ros_experiments (
    experiment_id TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL UNIQUE,
    snapshot_id TEXT NOT NULL REFERENCES ros_data_snapshots(snapshot_id),
    candidate_kind TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    family TEXT NOT NULL,
    status TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    registered_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_ros_experiments_snapshot
    ON ros_experiments(snapshot_id);
CREATE INDEX IF NOT EXISTS ix_ros_experiments_candidate
    ON ros_experiments(candidate_kind, candidate_id);
CREATE INDEX IF NOT EXISTS ix_ros_experiments_family
    ON ros_experiments(family);
CREATE TABLE IF NOT EXISTS ros_experiment_results (
    result_id TEXT PRIMARY KEY,
    experiment_id TEXT NOT NULL REFERENCES ros_experiments(experiment_id),
    result_hash TEXT NOT NULL,
    outcome TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    artifact_uri TEXT,
    authoritative INTEGER NOT NULL CHECK(authoritative IN (0, 1)),
    completed_at TEXT NOT NULL,
    UNIQUE(experiment_id, result_hash)
);
CREATE INDEX IF NOT EXISTS ix_ros_results_experiment
    ON ros_experiment_results(experiment_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_ros_results_one_authoritative
    ON ros_experiment_results(experiment_id) WHERE authoritative = 1;
CREATE TABLE IF NOT EXISTS ros_trial_ledger (
    trial_id TEXT PRIMARY KEY,
    experiment_id TEXT REFERENCES ros_experiments(experiment_id),
    family TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    reason TEXT NOT NULL,
    p_value REAL,
    alpha_spent REAL NOT NULL,
    metadata_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    admission_status TEXT NOT NULL DEFAULT 'admitted'
        CHECK(admission_status IN ('admitted', 'rejected')),
    experiment_fingerprint TEXT,
    research_equivalence_hash TEXT,
    completed_at TEXT,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_ros_trials_family_time
    ON ros_trial_ledger(family, occurred_at);
CREATE INDEX IF NOT EXISTS ix_ros_trials_candidate
    ON ros_trial_ledger(candidate_id);
CREATE INDEX IF NOT EXISTS ix_ros_trials_equivalence
    ON ros_trial_ledger(research_equivalence_hash);
CREATE UNIQUE INDEX IF NOT EXISTS uq_ros_trials_admitted_equivalence
    ON ros_trial_ledger(research_equivalence_hash)
    WHERE admission_status = 'admitted' AND research_equivalence_hash IS NOT NULL;
CREATE TABLE IF NOT EXISTS ros_research_families (
    family_id TEXT PRIMARY KEY,
    registry_hash TEXT NOT NULL UNIQUE,
    definition_json TEXT NOT NULL,
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ros_research_submissions (
    submission_id TEXT PRIMARY KEY,
    proposal_decision_id TEXT NOT NULL,
    family_id TEXT NOT NULL REFERENCES ros_research_families(family_id),
    recovery_case_id TEXT REFERENCES ros_recovery_cases(recovery_case_id),
    status TEXT NOT NULL CHECK(status IN (
        'reviewed','reserved','running','completed','failed','missing_data'
    )),
    research_equivalence_hash TEXT NOT NULL,
    experiment_fingerprint TEXT NOT NULL,
    trial_id TEXT NOT NULL UNIQUE,
    spec_json TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    experiment_id TEXT REFERENCES ros_experiments(experiment_id),
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(proposal_decision_id, research_equivalence_hash)
);
CREATE INDEX IF NOT EXISTS ix_ros_submission_status_time
    ON ros_research_submissions(status, updated_at);
CREATE INDEX IF NOT EXISTS ix_ros_submission_family
    ON ros_research_submissions(family_id);
CREATE TABLE IF NOT EXISTS ros_lifecycle_events (
    event_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    sleeve_id TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL,
    cause TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_ros_lifecycle_sleeve_time
    ON ros_lifecycle_events(sleeve_id, occurred_at);
CREATE TABLE IF NOT EXISTS ros_recovery_cases (
    recovery_case_id TEXT PRIMARY KEY,
    sleeve_id TEXT NOT NULL,
    status TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL,
    case_json TEXT NOT NULL,
    triggered_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_ros_recovery_sleeve_status
    ON ros_recovery_cases(sleeve_id, status);
CREATE TABLE IF NOT EXISTS ros_runs (
    run_id TEXT PRIMARY KEY,
    run_type TEXT NOT NULL,
    status TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    error TEXT,
    started_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE INDEX IF NOT EXISTS ix_ros_runs_type_time ON ros_runs(run_type, started_at);
CREATE INDEX IF NOT EXISTS ix_ros_runs_status ON ros_runs(status);
CREATE TABLE IF NOT EXISTS ros_legacy_evidence (
    evidence_id TEXT PRIMARY KEY,
    source_uri TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    trust_label TEXT NOT NULL,
    reasons_json TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    UNIQUE(source_uri, content_hash)
);
CREATE INDEX IF NOT EXISTS ix_ros_legacy_evidence_trust_time
    ON ros_legacy_evidence(trust_label, imported_at);
CREATE TABLE IF NOT EXISTS ros_shadow_accounts (
    account_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    currency TEXT NOT NULL,
    initial_capital REAL NOT NULL CHECK(initial_capital > 0),
    cash REAL NOT NULL,
    nav REAL NOT NULL,
    benchmark_nav REAL NOT NULL,
    status TEXT NOT NULL,
    as_of TEXT NOT NULL,
    last_event_sequence INTEGER NOT NULL,
    last_event_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ros_shadow_events (
    event_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES ros_shadow_accounts(account_id),
    sequence_number INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    previous_event_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    UNIQUE(account_id, sequence_number)
);
CREATE INDEX IF NOT EXISTS ix_ros_shadow_events_account_time
    ON ros_shadow_events(account_id, occurred_at);
CREATE TABLE IF NOT EXISTS ros_shadow_positions (
    account_id TEXT NOT NULL REFERENCES ros_shadow_accounts(account_id),
    ticker TEXT NOT NULL,
    quantity REAL NOT NULL,
    average_cost REAL NOT NULL,
    market_price REAL NOT NULL,
    market_value REAL NOT NULL,
    updated_at TEXT NOT NULL,
    last_event_sequence INTEGER NOT NULL,
    PRIMARY KEY(account_id, ticker)
);
"""


class _SQLiteCatalog:
    def __init__(self, database: str | Path) -> None:
        self._database = str(database)
        if self._database != ":memory:":
            Path(self._database).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(
            self._database,
            isolation_level=None,
            check_same_thread=False,
            timeout=30.0,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        if self._database != ":memory:":
            self._connection.execute("PRAGMA journal_mode = WAL")
        self._lock = threading.RLock()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
            except Exception:
                self._connection.rollback()
                raise
            else:
                self._connection.commit()

    def initialize_schema(self) -> None:
        with self._lock:
            self._connection.executescript(_SQLITE_SCHEMA)
            columns = {
                str(row[1])
                for row in self._connection.execute(
                    "PRAGMA table_info(ros_trial_ledger)"
                ).fetchall()
            }
            additions = {
                "admission_status": (
                    "TEXT NOT NULL DEFAULT 'admitted' "
                    "CHECK(admission_status IN ('admitted', 'rejected'))"
                ),
                "experiment_fingerprint": "TEXT",
                "research_equivalence_hash": "TEXT",
                "completed_at": "TEXT",
                "updated_at": "TEXT",
            }
            for name, definition in additions.items():
                if name not in columns:
                    self._connection.execute(
                        f"ALTER TABLE ros_trial_ledger ADD COLUMN {name} {definition}"
                    )
            rows = self._connection.execute(
                """
                SELECT trial_id, experiment_id, metadata_json, occurred_at,
                       experiment_fingerprint, completed_at, updated_at
                FROM ros_trial_ledger
                """
            ).fetchall()
            for row in rows:
                metadata = json.loads(row["metadata_json"] or "{}")
                fingerprint = row["experiment_fingerprint"] or metadata.get(
                    "experiment_fingerprint"
                )
                if fingerprint is None and row["experiment_id"] is not None:
                    experiment = self._connection.execute(
                        "SELECT fingerprint FROM ros_experiments WHERE experiment_id = ?",
                        (row["experiment_id"],),
                    ).fetchone()
                    fingerprint = None if experiment is None else experiment[0]
                self._connection.execute(
                    """
                    UPDATE ros_trial_ledger
                    SET experiment_fingerprint = ?,
                        completed_at = COALESCE(completed_at, occurred_at),
                        updated_at = COALESCE(updated_at, occurred_at)
                    WHERE trial_id = ?
                    """,
                    (fingerprint, row["trial_id"]),
                )
            self._connection.executescript(
                """
                CREATE INDEX IF NOT EXISTS ix_ros_trials_admission_month_family
                    ON ros_trial_ledger(admission_status, occurred_at, family);
                CREATE INDEX IF NOT EXISTS ix_ros_trials_fingerprint
                    ON ros_trial_ledger(experiment_fingerprint);
                CREATE UNIQUE INDEX IF NOT EXISTS uq_ros_trials_admitted_fingerprint
                    ON ros_trial_ledger(experiment_fingerprint)
                    WHERE admission_status = 'admitted'
                      AND experiment_fingerprint IS NOT NULL;
                CREATE INDEX IF NOT EXISTS ix_ros_trials_equivalence
                    ON ros_trial_ledger(research_equivalence_hash);
                CREATE UNIQUE INDEX IF NOT EXISTS uq_ros_trials_admitted_equivalence
                    ON ros_trial_ledger(research_equivalence_hash)
                    WHERE admission_status = 'admitted'
                      AND research_equivalence_hash IS NOT NULL;
                """
            )
            self._connection.execute(
                """
                INSERT INTO ros_schema_metadata(component, schema_version, installed_at)
                VALUES (?, ?, ?)
                ON CONFLICT(component) DO UPDATE SET schema_version = excluded.schema_version
                """,
                ("research_os", "research-os/v1", _time_text(_utc_now())),
            )

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def database_now(self) -> datetime:
        with self._lock:
            value = self._connection.execute(
                "SELECT strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')"
            ).fetchone()[0]
        return _parse_time(value)

    @staticmethod
    def _evidence_epoch_from_row(row: sqlite3.Row) -> EvidenceEpochRecord:
        return EvidenceEpochRecord(
            epoch_id=str(row["epoch_id"]),
            schema_version=str(row["schema_version"]),
            architecture_version=str(row["architecture_version"]),
            frozen_at=_parse_time(row["frozen_at"]),
            code_hash=str(row["code_hash"]),
            configuration_hash=str(row["configuration_hash"]),
            dependency_lock_hash=str(row["dependency_lock_hash"]),
            dirty_patch_hash=str(row["dirty_patch_hash"]),
            epoch_hash=str(row["epoch_hash"]),
            first_forward_session=(
                None
                if row["first_forward_session"] is None
                else date.fromisoformat(str(row["first_forward_session"]))
            ),
            calendar_snapshot_id=row["calendar_snapshot_id"],
            calendar_snapshot_hash=row["calendar_snapshot_hash"],
            calendar_content_hash=row["calendar_content_hash"],
            evidence_window_hash=row["evidence_window_hash"],
            activated_at=(
                None if row["activated_at"] is None else _parse_time(row["activated_at"])
            ),
            closed_at=(
                None if row["closed_at"] is None else _parse_time(row["closed_at"])
            ),
            superseded_by_epoch_id=row["superseded_by_epoch_id"],
        )

    def get_evidence_epoch(self) -> EvidenceEpochRecord | None:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT epoch.*
                FROM ros_evidence_epoch_active_pointer AS pointer
                JOIN ros_evidence_epochs AS epoch ON epoch.epoch_id = pointer.epoch_id
                WHERE pointer.pointer_key = ?
                  AND epoch.activated_at IS NOT NULL
                  AND epoch.closed_at IS NULL
                """,
                (EVIDENCE_EPOCH_SLOT,),
            ).fetchone()
            if row is None:
                row = self._connection.execute(
                    """
                    SELECT * FROM ros_evidence_epochs
                    WHERE activated_at IS NULL AND closed_at IS NULL
                    ORDER BY frozen_at DESC, epoch_id DESC LIMIT 1
                    """
                ).fetchone()
        return None if row is None else self._evidence_epoch_from_row(row)

    def get_pending_evidence_epoch(self) -> EvidenceEpochRecord | None:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT * FROM ros_evidence_epochs
                WHERE activated_at IS NULL AND closed_at IS NULL
                ORDER BY frozen_at DESC, epoch_id DESC LIMIT 1
                """
            ).fetchone()
        return None if row is None else self._evidence_epoch_from_row(row)

    def list_evidence_epochs(self, *, limit: int) -> list[EvidenceEpochRecord]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM ros_evidence_epochs "
                "ORDER BY frozen_at DESC, epoch_id DESC LIMIT ?",
                (_validate_limit(limit),),
            ).fetchall()
        return [self._evidence_epoch_from_row(row) for row in rows]

    def freeze_evidence_epoch(
        self, epoch: EvidenceEpochRecord
    ) -> EvidenceEpochRecord:
        if epoch.first_forward_session is not None:
            raise ValueError("a new evidence epoch must begin with a pending forward window")
        with self._transaction() as connection:
            existing_row = connection.execute(
                "SELECT * FROM ros_evidence_epochs WHERE epoch_hash = ?",
                (epoch.epoch_hash,),
            ).fetchone()
            if existing_row is not None:
                existing = self._evidence_epoch_from_row(existing_row)
                if existing.freeze_payload() == epoch.freeze_payload():
                    return existing
                raise CatalogConflict("evidence epoch hash collision")
            latest_frozen = connection.execute(
                "SELECT MAX(frozen_at) FROM ros_evidence_epochs"
            ).fetchone()[0]
            if latest_frozen is not None and epoch.frozen_at < _parse_time(latest_frozen):
                raise CatalogConflict("new evidence epoch cannot predate retained history")
            try:
                connection.execute(
                    """
                    INSERT INTO ros_evidence_epochs(
                        epoch_slot, epoch_id, schema_version, architecture_version,
                        frozen_at, code_hash, configuration_hash,
                        dependency_lock_hash, dirty_patch_hash, epoch_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        epoch.epoch_id,
                        epoch.epoch_id,
                        epoch.schema_version,
                        epoch.architecture_version,
                        _time_text(epoch.frozen_at),
                        epoch.code_hash,
                        epoch.configuration_hash,
                        epoch.dependency_lock_hash,
                        epoch.dirty_patch_hash,
                        epoch.epoch_hash,
                    ),
                )
                connection.execute(
                    """
                    UPDATE ros_evidence_epochs
                    SET closed_at = ?, superseded_by_epoch_id = ?
                    WHERE epoch_id <> ? AND activated_at IS NULL
                      AND closed_at IS NULL
                    """,
                    (
                        _time_text(epoch.frozen_at),
                        epoch.epoch_id,
                        epoch.epoch_id,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise CatalogConflict("evidence epoch version could not be frozen") from exc
        return epoch

    def activate_evidence_epoch(
        self,
        *,
        epoch_id: str,
        expected_epoch_hash: str,
        first_forward_session: date,
        calendar_snapshot_id: str,
        calendar_snapshot_hash: str,
        calendar_content_hash: str,
        activated_at: datetime,
    ) -> EvidenceEpochRecord:
        activated_at = _require_aware(activated_at, "activated_at")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM ros_evidence_epochs WHERE epoch_id = ?",
                (epoch_id,),
            ).fetchone()
            if row is None:
                raise CatalogNotFound("the architecture evidence epoch is not frozen")
            existing = self._evidence_epoch_from_row(row)
            if existing.epoch_id != epoch_id or existing.epoch_hash != expected_epoch_hash:
                raise CatalogConflict("evidence epoch identity changed before activation")
            if existing.closed_at is not None:
                raise CatalogConflict("closed evidence epoch cannot be activated")
            if existing.first_forward_session is not None:
                if (
                    existing.first_forward_session == first_forward_session
                    and existing.calendar_snapshot_id == calendar_snapshot_id
                    and existing.calendar_snapshot_hash == calendar_snapshot_hash
                    and existing.calendar_content_hash == calendar_content_hash
                ):
                    connection.execute(
                        """
                        INSERT INTO ros_evidence_epoch_active_pointer(
                            pointer_key, epoch_id, updated_at
                        ) VALUES (?, ?, ?)
                        ON CONFLICT(pointer_key) DO UPDATE SET
                            epoch_id = excluded.epoch_id,
                            updated_at = excluded.updated_at
                        """,
                        (EVIDENCE_EPOCH_SLOT, epoch_id, _time_text(activated_at)),
                    )
                    return existing
                raise CatalogConflict("the forward evidence window is already activated")
            window_payload = {
                "epoch_id": existing.epoch_id,
                "epoch_hash": existing.epoch_hash,
                "first_forward_session": first_forward_session.isoformat(),
                "calendar_snapshot_id": calendar_snapshot_id,
                "calendar_snapshot_hash": calendar_snapshot_hash,
                "calendar_content_hash": calendar_content_hash,
                "activated_at": activated_at.isoformat(),
            }
            window_hash = content_fingerprint(
                window_payload,
                domain="factor-lab/research-os/v1/evidence-window",
            )
            candidate = EvidenceEpochRecord(
                epoch_id=existing.epoch_id,
                architecture_version=existing.architecture_version,
                frozen_at=existing.frozen_at,
                code_hash=existing.code_hash,
                configuration_hash=existing.configuration_hash,
                dependency_lock_hash=existing.dependency_lock_hash,
                dirty_patch_hash=existing.dirty_patch_hash,
                epoch_hash=existing.epoch_hash,
                first_forward_session=first_forward_session,
                calendar_snapshot_id=calendar_snapshot_id,
                calendar_snapshot_hash=calendar_snapshot_hash,
                calendar_content_hash=calendar_content_hash,
                activated_at=activated_at,
                evidence_window_hash=window_hash,
            )
            connection.execute(
                """
                UPDATE ros_evidence_epochs
                SET closed_at = ?, superseded_by_epoch_id = ?
                WHERE epoch_id <> ? AND activated_at IS NOT NULL
                  AND closed_at IS NULL
                """,
                (_time_text(activated_at), epoch_id, epoch_id),
            )
            updated = connection.execute(
                """
                UPDATE ros_evidence_epochs
                SET first_forward_session = ?, calendar_snapshot_id = ?,
                    calendar_snapshot_hash = ?, calendar_content_hash = ?,
                    evidence_window_hash = ?, activated_at = ?
                WHERE epoch_id = ? AND first_forward_session IS NULL
                  AND closed_at IS NULL
                  AND epoch_hash = ?
                """,
                (
                    first_forward_session.isoformat(),
                    calendar_snapshot_id,
                    calendar_snapshot_hash,
                    calendar_content_hash,
                    window_hash,
                    _time_text(activated_at),
                    epoch_id,
                    expected_epoch_hash,
                ),
            )
            if updated.rowcount != 1:
                raise CatalogConflict("evidence window activation lost an atomic race")
            connection.execute(
                """
                INSERT INTO ros_evidence_epoch_active_pointer(
                    pointer_key, epoch_id, updated_at
                ) VALUES (?, ?, ?)
                ON CONFLICT(pointer_key) DO UPDATE SET
                    epoch_id = excluded.epoch_id,
                    updated_at = excluded.updated_at
                """,
                (EVIDENCE_EPOCH_SLOT, epoch_id, _time_text(activated_at)),
            )
            # Role assignments often exist before the one-way forward epoch is
            # activated.  Roll every active assignment into an epoch-bound
            # binding in the *same* transaction as activation; otherwise the
            # first forward session either fails closed or, worse, can remain
            # attached to a pre-epoch identity.
            has_role_bindings = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' "
                "AND name = 'ros_shadow_role_bindings'"
            ).fetchone()
            active_bindings = (
                connection.execute(
                    "SELECT * FROM ros_shadow_role_bindings WHERE active = 1"
                ).fetchall()
                if has_role_bindings is not None
                else ()
            )
            for binding in active_bindings:
                if binding["epoch_id"] == epoch_id:
                    continue
                metadata = json.loads(str(binding["metadata_json"] or "{}"))
                if (
                    metadata.get("formal_epoch_eligible") is False
                    or str(metadata.get("evidence_scope") or "").lower()
                    == "non_forward"
                    or str(metadata.get("evidence_class") or "").lower()
                    == "engineering_canary"
                ):
                    continue
                identity = {
                    "role": str(binding["role"]),
                    "role_key": str(binding["role_key"]),
                    "account_id": str(binding["account_id"]),
                    "sleeve_id": binding["sleeve_id"],
                    "experiment_id": binding["experiment_id"],
                    "epoch_id": epoch_id,
                    "metadata": metadata,
                }
                payload = {**identity, "bound_at": activated_at}
                binding_hash = content_fingerprint(
                    payload, domain=_SHADOW_BINDING_HASH_DOMAIN
                )
                connection.execute(
                    """
                    UPDATE ros_shadow_role_bindings
                    SET active = 0, unbound_at = ?
                    WHERE binding_id = ? AND active = 1
                    """,
                    (_time_text(activated_at), str(binding["binding_id"])),
                )
                connection.execute(
                    """
                    INSERT INTO ros_shadow_role_bindings(
                        binding_id, binding_hash, role, role_key, account_id,
                        sleeve_id, experiment_id, epoch_id, active, bound_at,
                        unbound_at, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, NULL, ?)
                    """,
                    (
                        f"shadow_binding_{binding_hash[:64]}",
                        binding_hash,
                        identity["role"],
                        identity["role_key"],
                        identity["account_id"],
                        identity["sleeve_id"],
                        identity["experiment_id"],
                        epoch_id,
                        _time_text(activated_at),
                        canonical_json(metadata),
                    ),
                )
        return candidate

    def register_snapshot(self, reference: DataSnapshotRef) -> SnapshotRecord:
        now = _utc_now()
        payload = canonical_json(reference)
        try:
            with self._transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO ros_data_snapshots(
                        snapshot_id, schema_version, tier, uri, content_hash, as_of,
                        quality_status, ref_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        reference.snapshot_id,
                        reference.schema_version,
                        reference.tier.value,
                        reference.uri,
                        reference.content_hash,
                        _time_text(reference.as_of),
                        reference.quality_status.value,
                        payload,
                        _time_text(now),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            existing = self.get_snapshot(reference.snapshot_id)
            if existing is not None and canonical_json(existing.reference) == payload:
                return existing
            with self._lock:
                row = self._connection.execute(
                    "SELECT ref_json FROM ros_data_snapshots WHERE content_hash = ?",
                    (reference.content_hash,),
                ).fetchone()
            if row is not None and row["ref_json"] == payload:
                raise CatalogConflict(
                    "snapshot content already exists under a different snapshot_id"
                ) from exc
            raise CatalogConflict(
                f"snapshot identity collision for {reference.snapshot_id}"
            ) from exc
        return SnapshotRecord(reference=reference, created_at=now)

    def get_snapshot(self, snapshot_id: str) -> SnapshotRecord | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT ref_json, created_at FROM ros_data_snapshots WHERE snapshot_id = ?",
                (snapshot_id,),
            ).fetchone()
        if row is None:
            return None
        return SnapshotRecord(
            reference=DataSnapshotRef.model_validate_json(row["ref_json"]),
            created_at=_parse_time(row["created_at"]),
        )

    def list_snapshots(
        self,
        *,
        limit: int,
        quality_status: DataQualityStatus | None,
        tier: SnapshotTier | None,
    ) -> list[SnapshotRecord]:
        return list(
            self.list_snapshot_page(
                limit=limit,
                quality_status=quality_status,
                tier=tier,
                after=None,
            ).records
        )

    def list_snapshot_page(
        self,
        *,
        limit: int,
        quality_status: DataQualityStatus | None,
        tier: SnapshotTier | None,
        after: SnapshotPageCursor | None,
    ) -> SnapshotPage:
        page_size = _validate_limit(limit)
        clauses: list[str] = []
        params: list[Any] = []
        if quality_status is not None:
            clauses.append("quality_status = ?")
            params.append(quality_status.value)
        if tier is not None:
            clauses.append("tier = ?")
            params.append(tier.value)
        if after is not None:
            clauses.append(
                """
                (
                    as_of < ?
                    OR (as_of = ? AND created_at < ?)
                    OR (as_of = ? AND created_at = ? AND snapshot_id > ?)
                )
                """
            )
            cursor_as_of = _time_text(after.as_of)
            cursor_created_at = _time_text(after.created_at)
            params.extend(
                (
                    cursor_as_of,
                    cursor_as_of,
                    cursor_created_at,
                    cursor_as_of,
                    cursor_created_at,
                    after.snapshot_id,
                )
            )
        query = "SELECT ref_json, created_at FROM ros_data_snapshots"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY as_of DESC, created_at DESC, snapshot_id ASC LIMIT ?"
        params.append(page_size + 1)
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        records = tuple(
            SnapshotRecord(
                reference=DataSnapshotRef.model_validate_json(row["ref_json"]),
                created_at=_parse_time(row["created_at"]),
            )
            for row in rows[:page_size]
        )
        next_cursor = (
            SnapshotPageCursor.from_record(records[-1])
            if len(rows) > page_size
            else None
        )
        return SnapshotPage(records=records, next_cursor=next_cursor)

    def register_experiment(self, spec: ExperimentSpec) -> ExperimentRecord:
        fingerprint = experiment_fingerprint(spec)
        experiment_id = f"exp_{fingerprint[:32]}"
        now = _utc_now()
        payload = canonical_json(spec)
        try:
            with self._transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO ros_experiments(
                        experiment_id, fingerprint, snapshot_id, candidate_kind,
                        candidate_id, family, status, spec_json, registered_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        experiment_id,
                        fingerprint,
                        spec.snapshot.snapshot_id,
                        spec.candidate_kind.value,
                        spec.candidate_id,
                        spec.family,
                        ExperimentStatus.PREREGISTERED.value,
                        payload,
                        _time_text(now),
                        _time_text(now),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            existing = self.get_experiment_by_fingerprint(fingerprint)
            if existing is not None and canonical_json(existing.spec) == payload:
                return existing
            if self.get_snapshot(spec.snapshot.snapshot_id) is None:
                raise CatalogNotFound(
                    f"snapshot {spec.snapshot.snapshot_id!r} is not registered"
                ) from exc
            raise CatalogConflict(f"experiment fingerprint collision: {fingerprint}") from exc
        return ExperimentRecord(
            experiment_id=experiment_id,
            fingerprint=fingerprint,
            status=ExperimentStatus.PREREGISTERED,
            spec=spec,
            registered_at=now,
            updated_at=now,
        )

    def _experiment_from_row(self, row: sqlite3.Row) -> ExperimentRecord:
        return ExperimentRecord(
            experiment_id=row["experiment_id"],
            fingerprint=row["fingerprint"],
            status=ExperimentStatus(row["status"]),
            spec=ExperimentSpec.model_validate_json(row["spec_json"]),
            registered_at=_parse_time(row["registered_at"]),
            updated_at=_parse_time(row["updated_at"]),
        )

    def get_experiment(self, experiment_id: str) -> ExperimentRecord | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM ros_experiments WHERE experiment_id = ?", (experiment_id,)
            ).fetchone()
        return None if row is None else self._experiment_from_row(row)

    def get_experiment_by_fingerprint(self, fingerprint: str) -> ExperimentRecord | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM ros_experiments WHERE fingerprint = ?", (fingerprint,)
            ).fetchone()
        return None if row is None else self._experiment_from_row(row)

    def list_experiments(
        self,
        *,
        limit: int,
        status: ExperimentStatus | None,
        family: str | None,
        candidate_id: str | None,
    ) -> list[ExperimentRecord]:
        clauses: list[str] = []
        params: list[Any] = []
        if status is not None:
            clauses.append("status = ?")
            params.append(status.value)
        if family is not None:
            clauses.append("family = ?")
            params.append(family)
        if candidate_id is not None:
            clauses.append("candidate_id = ?")
            params.append(candidate_id)
        query = "SELECT * FROM ros_experiments"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY registered_at DESC, experiment_id LIMIT ?"
        params.append(_validate_limit(limit))
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        return [self._experiment_from_row(row) for row in rows]

    def set_experiment_status(
        self, experiment_id: str, status: ExperimentStatus
    ) -> ExperimentRecord:
        now = _utc_now()
        with self._transaction() as connection:
            cursor = connection.execute(
                "UPDATE ros_experiments SET status = ?, updated_at = ? WHERE experiment_id = ?",
                (status.value, _time_text(now), experiment_id),
            )
            if cursor.rowcount != 1:
                raise CatalogNotFound(f"experiment {experiment_id!r} was not found")
        result = self.get_experiment(experiment_id)
        assert result is not None
        return result

    def record_authoritative_result(
        self,
        experiment_id: str,
        *,
        outcome: str,
        metrics: Mapping[str, Any],
        artifact_uri: str | None = None,
        completed_at: datetime | None = None,
    ) -> ExperimentResultRecord:
        completed_at = _require_aware(completed_at or _utc_now(), "completed_at")
        metrics_dict = json.loads(canonical_json(metrics))
        result_hash = content_fingerprint(
            {
                "experiment_id": experiment_id,
                "outcome": outcome,
                "metrics": metrics_dict,
                "artifact_uri": artifact_uri,
            },
            domain="factor-lab/research-os/v1/authoritative-result",
        )
        result_id = f"res_{result_hash[:32]}"
        existing = self.get_authoritative_result(experiment_id)
        if existing is not None:
            if existing.result_hash == result_hash:
                return existing
            raise AuthoritativeResultExists(
                f"experiment {experiment_id!r} already has an authoritative result"
            )
        try:
            with self._transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO ros_experiment_results(
                        result_id, experiment_id, result_hash, outcome, metrics_json,
                        artifact_uri, authoritative, completed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 1, ?)
                    """,
                    (
                        result_id,
                        experiment_id,
                        result_hash,
                        outcome,
                        canonical_json(metrics_dict),
                        artifact_uri,
                        _time_text(completed_at),
                    ),
                )
                connection.execute(
                    """
                    UPDATE ros_experiments
                    SET status = ?, updated_at = ?
                    WHERE experiment_id = ?
                    """,
                    (
                        ExperimentStatus.COMPLETED.value,
                        _time_text(completed_at),
                        experiment_id,
                    ),
                )
        except sqlite3.IntegrityError as exc:
            existing = self.get_authoritative_result(experiment_id)
            if existing is not None and existing.result_hash == result_hash:
                return existing
            if self.get_experiment(experiment_id) is None:
                raise CatalogNotFound(f"experiment {experiment_id!r} was not found") from exc
            raise AuthoritativeResultExists(
                f"experiment {experiment_id!r} already has an authoritative result"
            ) from exc
        result = self.get_authoritative_result(experiment_id)
        assert result is not None
        return result

    def get_authoritative_result(
        self, experiment_id: str
    ) -> ExperimentResultRecord | None:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT * FROM ros_experiment_results
                WHERE experiment_id = ? AND authoritative = 1
                """,
                (experiment_id,),
            ).fetchone()
        if row is None:
            return None
        return ExperimentResultRecord(
            result_id=row["result_id"],
            experiment_id=row["experiment_id"],
            result_hash=row["result_hash"],
            outcome=row["outcome"],
            metrics=json.loads(row["metrics_json"]),
            artifact_uri=row["artifact_uri"],
            completed_at=_parse_time(row["completed_at"]),
            authoritative=bool(row["authoritative"]),
        )

    @staticmethod
    def _trial_entry(row: sqlite3.Row) -> TrialLedgerEntry:
        metadata = json.loads(row["metadata_json"])
        admission_status = TrialAdmissionStatus(
            row["admission_status"]
            if "admission_status" in row.keys()
            else metadata.get("admission_status", TrialAdmissionStatus.ADMITTED.value)
        )
        return TrialLedgerEntry(
            trial_id=row["trial_id"],
            experiment_id=row["experiment_id"],
            family=row["family"],
            candidate_id=row["candidate_id"],
            outcome=TrialOutcome(row["outcome"]),
            reason=row["reason"],
            p_value=row["p_value"],
            alpha_spent=row["alpha_spent"],
            metadata=metadata,
            occurred_at=_parse_time(row["occurred_at"]),
            admission_status=admission_status,
            experiment_fingerprint=(
                row["experiment_fingerprint"]
                if "experiment_fingerprint" in row.keys()
                else metadata.get("experiment_fingerprint")
            ),
            research_equivalence_hash=(
                row["research_equivalence_hash"]
                if "research_equivalence_hash" in row.keys()
                else metadata.get("research_equivalence_hash")
            ),
            completed_at=(
                _parse_time(row["completed_at"])
                if "completed_at" in row.keys() and row["completed_at"]
                else None
            ),
            updated_at=(
                _parse_time(row["updated_at"])
                if "updated_at" in row.keys() and row["updated_at"]
                else _parse_time(row["occurred_at"])
            ),
        )

    @staticmethod
    def _reservation_decision(entry: TrialLedgerEntry) -> TrialAdmissionDecision:
        return TrialAdmissionDecision(
            allowed=entry.admission_status is TrialAdmissionStatus.ADMITTED,
            evidence_class=EvidenceClass(
                str(entry.metadata.get("evidence_class", EvidenceClass.OBSERVED.value))
            ),
            family_trial_index=int(entry.metadata.get("family_trial_index", 1)),
            reasons=tuple(map(str, entry.metadata.get("admission_reasons", ()))),
        )

    def append_trial(self, entry: TrialLedgerEntry) -> TrialLedgerEntry:
        if entry.experiment_id is not None and self.get_experiment(entry.experiment_id) is None:
            raise CatalogNotFound(
                f"experiment {entry.experiment_id!r} referenced by trial was not found"
            )
        completed_at = entry.completed_at or entry.occurred_at
        updated_at = entry.updated_at or completed_at
        metadata = dict(entry.metadata)
        try:
            with self._transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO ros_trial_ledger(
                        trial_id, experiment_id, family, candidate_id, outcome, reason,
                        p_value, alpha_spent, metadata_json, occurred_at,
                        admission_status, experiment_fingerprint,
                        research_equivalence_hash, completed_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry.trial_id,
                        entry.experiment_id,
                        entry.family,
                        entry.candidate_id,
                        entry.outcome.value,
                        entry.reason,
                        entry.p_value,
                        entry.alpha_spent,
                        canonical_json(metadata),
                        _time_text(entry.occurred_at),
                        entry.admission_status.value,
                        entry.experiment_fingerprint,
                        entry.research_equivalence_hash,
                        _time_text(completed_at),
                        _time_text(updated_at),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise CatalogConflict(f"trial {entry.trial_id!r} already exists") from exc
        return entry

    def reserve_trial(
        self,
        registration: TrialRegistration,
        *,
        candidate_id: str,
        experiment_id: str | None,
        maximum_monthly_confirmatory_trials: int,
        maximum_monthly_confirmatory_trials_per_family: int,
        maximum_diagnostic_branches: int,
    ) -> TrialReservationRecord:
        with self._transaction() as connection:
            if experiment_id is not None and connection.execute(
                "SELECT 1 FROM ros_experiments WHERE experiment_id = ?",
                (experiment_id,),
            ).fetchone() is None:
                raise CatalogNotFound(f"experiment {experiment_id!r} was not found")
            row = connection.execute(
                "SELECT * FROM ros_trial_ledger WHERE trial_id = ?",
                (registration.trial_id,),
            ).fetchone()
            if row is not None:
                entry = self._trial_entry(row)
                if (
                    entry.experiment_fingerprint != registration.experiment_fingerprint
                    or entry.research_equivalence_hash
                    != registration.research_equivalence_hash
                    or entry.family != registration.family
                    or entry.candidate_id != candidate_id
                ):
                    raise CatalogConflict(
                        f"trial {registration.trial_id!r} has a different registration"
                    )
                if experiment_id is not None:
                    if entry.experiment_id not in {None, experiment_id}:
                        raise CatalogConflict(
                            f"trial {registration.trial_id!r} belongs to another experiment"
                        )
                    if entry.experiment_id is None:
                        connection.execute(
                            "UPDATE ros_trial_ledger SET experiment_id = ?, updated_at = ? WHERE trial_id = ?",
                            (experiment_id, _time_text(_utc_now()), registration.trial_id),
                        )
                        row = connection.execute(
                            "SELECT * FROM ros_trial_ledger WHERE trial_id = ?",
                            (registration.trial_id,),
                        ).fetchone()
                        assert row is not None
                        entry = self._trial_entry(row)
                return TrialReservationRecord(
                    entry=entry,
                    admission=self._reservation_decision(entry),
                    created=False,
                )

            rows = connection.execute(
                "SELECT * FROM ros_trial_ledger ORDER BY occurred_at, trial_id"
            ).fetchall()
            admission = TrialLedger.from_catalog_entries(
                self._trial_entry(item) for item in rows
            ).admit(
                registration,
                maximum_monthly_confirmatory_trials=maximum_monthly_confirmatory_trials,
                maximum_monthly_confirmatory_trials_per_family=maximum_monthly_confirmatory_trials_per_family,
                maximum_diagnostic_branches=maximum_diagnostic_branches,
            )
            status = (
                TrialAdmissionStatus.ADMITTED
                if admission.allowed
                else TrialAdmissionStatus.REJECTED
            )
            reason = (
                "confirmatory trial reserved"
                if admission.allowed
                else f"trial admission rejected: {','.join(admission.reasons)}"
            )
            metadata = {
                "trial_kind": registration.kind.value,
                "evidence_class": admission.evidence_class.value,
                "experiment_fingerprint": registration.experiment_fingerprint,
                "research_equivalence_hash": registration.research_equivalence_hash,
                "hypothesis_id": registration.hypothesis_id,
                "holdout_id": registration.holdout_id,
                "variant_id": registration.variant_id,
                "diagnostic_branch": registration.diagnostic_branch,
                "admission_status": status.value,
                "admission_reasons": list(admission.reasons),
                "family_trial_index": admission.family_trial_index,
                "reservation_state": "reserved" if admission.allowed else "rejected",
            }
            completed_at = None if admission.allowed else registration.registered_at
            now = _utc_now()
            connection.execute(
                """
                INSERT INTO ros_trial_ledger(
                    trial_id, experiment_id, family, candidate_id, outcome, reason,
                    p_value, alpha_spent, metadata_json, occurred_at,
                    admission_status, experiment_fingerprint,
                    research_equivalence_hash, completed_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    registration.trial_id,
                    experiment_id,
                    registration.family,
                    candidate_id,
                    (
                        TrialOutcome.MANUAL.value
                        if admission.allowed
                        else TrialOutcome.REJECTED.value
                    ),
                    reason,
                    None,
                    0.0,
                    canonical_json(metadata),
                    _time_text(registration.registered_at),
                    status.value,
                    registration.experiment_fingerprint,
                    registration.research_equivalence_hash,
                    _time_text(completed_at) if completed_at is not None else None,
                    _time_text(now),
                ),
            )
            row = connection.execute(
                "SELECT * FROM ros_trial_ledger WHERE trial_id = ?",
                (registration.trial_id,),
            ).fetchone()
            assert row is not None
            return TrialReservationRecord(
                entry=self._trial_entry(row), admission=admission, created=True
            )

    def complete_trial(
        self,
        trial_id: str,
        *,
        outcome: TrialOutcome,
        reason: str,
        p_value: float | None,
        alpha_spent: float,
        metadata: Mapping[str, Any],
        experiment_id: str | None,
        completed_at: datetime,
    ) -> TrialLedgerEntry:
        _require_aware(completed_at, "completed_at")
        if p_value is not None and not 0.0 <= p_value <= 1.0:
            raise ValueError("p_value must be between 0 and 1")
        if not 0.0 <= alpha_spent <= 1.0:
            raise ValueError("alpha_spent must be between 0 and 1")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM ros_trial_ledger WHERE trial_id = ?", (trial_id,)
            ).fetchone()
            if row is None:
                raise CatalogNotFound(f"trial {trial_id!r} was not found")
            entry = self._trial_entry(row)
            if entry.admission_status is TrialAdmissionStatus.REJECTED:
                if outcome is TrialOutcome.REJECTED:
                    return entry
                raise CatalogConflict(f"rejected trial {trial_id!r} cannot be completed")
            if experiment_id is not None:
                if connection.execute(
                    "SELECT 1 FROM ros_experiments WHERE experiment_id = ?",
                    (experiment_id,),
                ).fetchone() is None:
                    raise CatalogNotFound(f"experiment {experiment_id!r} was not found")
                if entry.experiment_id not in {None, experiment_id}:
                    raise CatalogConflict(f"trial {trial_id!r} belongs to another experiment")
            merged = dict(entry.metadata)
            merged.update(dict(metadata))
            merged.update(
                {
                    "admission_status": TrialAdmissionStatus.ADMITTED.value,
                    "experiment_fingerprint": entry.experiment_fingerprint,
                    "research_equivalence_hash": entry.research_equivalence_hash,
                    "reservation_state": "completed",
                }
            )
            effective_experiment = experiment_id or entry.experiment_id
            if entry.completed_at is not None:
                if (
                    entry.outcome is outcome
                    and entry.reason == reason
                    and entry.p_value == p_value
                    and entry.alpha_spent == alpha_spent
                    and entry.experiment_id == effective_experiment
                    and dict(entry.metadata) == merged
                ):
                    return entry
                raise CatalogConflict(f"trial {trial_id!r} already has an outcome")
            connection.execute(
                """
                UPDATE ros_trial_ledger
                SET experiment_id = ?, outcome = ?, reason = ?, p_value = ?,
                    alpha_spent = ?, metadata_json = ?, completed_at = ?, updated_at = ?
                WHERE trial_id = ? AND completed_at IS NULL
                """,
                (
                    effective_experiment,
                    outcome.value,
                    reason,
                    p_value,
                    alpha_spent,
                    canonical_json(merged),
                    _time_text(completed_at),
                    _time_text(completed_at),
                    trial_id,
                ),
            )
            row = connection.execute(
                "SELECT * FROM ros_trial_ledger WHERE trial_id = ?", (trial_id,)
            ).fetchone()
            assert row is not None
            return self._trial_entry(row)

    def get_trial(self, trial_id: str) -> TrialLedgerEntry | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM ros_trial_ledger WHERE trial_id = ?", (trial_id,)
            ).fetchone()
        return None if row is None else self._trial_entry(row)

    def list_trials(self, *, family: str | None = None) -> list[TrialLedgerEntry]:
        query = "SELECT * FROM ros_trial_ledger"
        params: tuple[Any, ...] = ()
        if family is not None:
            query += " WHERE family = ?"
            params = (family,)
        query += " ORDER BY occurred_at, trial_id"
        with self._lock:
            rows = self._connection.execute(query, params).fetchall()
        return [self._trial_entry(row) for row in rows]

    @staticmethod
    def _research_family(row: sqlite3.Row) -> ResearchFamilyRecord:
        definition = json.loads(row["definition_json"])
        return ResearchFamilyRecord(
            family_id=row["family_id"],
            display_name=str(definition["display_name"]),
            mechanism_key=str(definition["mechanism_key"]),
            cluster_id=str(definition["cluster_id"]),
            allowed_fields=tuple(map(str, definition["allowed_fields"])),
            field_registry=tuple(definition["field_registry"]),
            active=bool(row["active"]),
            created_at=_parse_time(row["created_at"]),
            registry_hash=row["registry_hash"],
        )

    def register_research_family(
        self, family: ResearchFamilyRecord
    ) -> ResearchFamilyRecord:
        try:
            with self._transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO ros_research_families(
                        family_id, registry_hash, definition_json, active, created_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        family.family_id,
                        family.registry_hash,
                        canonical_json(family.definition()),
                        int(family.active),
                        _time_text(family.created_at),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            existing = self.get_research_family(family.family_id)
            if existing is not None and existing.registry_hash == family.registry_hash:
                return existing
            raise CatalogConflict(
                f"research family {family.family_id!r} is immutable and already differs"
            ) from exc
        result = self.get_research_family(family.family_id)
        assert result is not None
        return result

    def get_research_family(self, family_id: str) -> ResearchFamilyRecord | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM ros_research_families WHERE family_id = ?",
                (family_id,),
            ).fetchone()
        return None if row is None else self._research_family(row)

    def list_research_families(
        self, *, active_only: bool
    ) -> list[ResearchFamilyRecord]:
        query = "SELECT * FROM ros_research_families"
        if active_only:
            query += " WHERE active = 1"
        query += " ORDER BY family_id"
        with self._lock:
            rows = self._connection.execute(query).fetchall()
        return [self._research_family(row) for row in rows]

    @staticmethod
    def _research_submission(row: sqlite3.Row) -> ResearchSubmissionRecord:
        return ResearchSubmissionRecord(
            submission_id=row["submission_id"],
            proposal_decision_id=row["proposal_decision_id"],
            family_id=row["family_id"],
            recovery_case_id=row["recovery_case_id"],
            status=row["status"],
            research_equivalence_hash=row["research_equivalence_hash"],
            experiment_fingerprint=row["experiment_fingerprint"],
            trial_id=row["trial_id"],
            spec=ExperimentSpec.model_validate_json(row["spec_json"]),
            lease_owner=row["lease_owner"],
            lease_expires_at=(
                None
                if row["lease_expires_at"] is None
                else _parse_time(row["lease_expires_at"])
            ),
            attempts=int(row["attempts"]),
            experiment_id=row["experiment_id"],
            error=row["error"],
            created_at=_parse_time(row["created_at"]),
            updated_at=_parse_time(row["updated_at"]),
        )

    @staticmethod
    def _same_submission_identity(
        left: ResearchSubmissionRecord, right: ResearchSubmissionRecord
    ) -> bool:
        return (
            left.submission_id == right.submission_id
            and left.proposal_decision_id == right.proposal_decision_id
            and left.family_id == right.family_id
            and left.recovery_case_id == right.recovery_case_id
            and left.research_equivalence_hash == right.research_equivalence_hash
            and left.experiment_fingerprint == right.experiment_fingerprint
            and left.trial_id == right.trial_id
            and left.spec == right.spec
        )

    def create_research_submission(
        self, submission: ResearchSubmissionRecord
    ) -> ResearchSubmissionRecord:
        if submission.status != "reviewed":
            raise ValueError("new research submissions must start in reviewed state")
        try:
            with self._transaction() as connection:
                if connection.execute(
                    "SELECT 1 FROM ros_research_families WHERE family_id = ? AND active = 1",
                    (submission.family_id,),
                ).fetchone() is None:
                    raise CatalogNotFound(
                        f"active research family {submission.family_id!r} was not found"
                    )
                if submission.recovery_case_id is not None and connection.execute(
                    "SELECT 1 FROM ros_recovery_cases WHERE recovery_case_id = ?",
                    (submission.recovery_case_id,),
                ).fetchone() is None:
                    raise CatalogNotFound(
                        f"recovery case {submission.recovery_case_id!r} was not found"
                    )
                connection.execute(
                    """
                    INSERT INTO ros_research_submissions(
                        submission_id, proposal_decision_id, family_id, recovery_case_id,
                        status, research_equivalence_hash, experiment_fingerprint,
                        trial_id, spec_json, lease_owner, lease_expires_at, attempts,
                        experiment_id, error, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        submission.submission_id,
                        submission.proposal_decision_id,
                        submission.family_id,
                        submission.recovery_case_id,
                        submission.status,
                        submission.research_equivalence_hash,
                        submission.experiment_fingerprint,
                        submission.trial_id,
                        submission.spec.model_dump_json(exclude_none=False),
                        submission.lease_owner,
                        None,
                        submission.attempts,
                        submission.experiment_id,
                        submission.error,
                        _time_text(submission.created_at),
                        _time_text(submission.updated_at),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            existing = self.get_research_submission(submission.submission_id)
            if existing is None:
                with self._lock:
                    row = self._connection.execute(
                        """
                        SELECT * FROM ros_research_submissions
                        WHERE proposal_decision_id = ? AND research_equivalence_hash = ?
                        """,
                        (
                            submission.proposal_decision_id,
                            submission.research_equivalence_hash,
                        ),
                    ).fetchone()
                existing = None if row is None else self._research_submission(row)
            if existing is not None and self._same_submission_identity(existing, submission):
                return existing
            raise CatalogConflict("research submission identity conflicts with durable state") from exc
        result = self.get_research_submission(submission.submission_id)
        assert result is not None
        return result

    def get_research_submission(
        self, submission_id: str
    ) -> ResearchSubmissionRecord | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM ros_research_submissions WHERE submission_id = ?",
                (submission_id,),
            ).fetchone()
        return None if row is None else self._research_submission(row)

    def list_research_submissions(
        self, *, limit: int, status: str | None, family_id: str | None
    ) -> list[ResearchSubmissionRecord]:
        clauses: list[str] = []
        parameters: list[Any] = []
        if status is not None:
            clauses.append("status = ?")
            parameters.append(status)
        if family_id is not None:
            clauses.append("family_id = ?")
            parameters.append(family_id)
        query = "SELECT * FROM ros_research_submissions"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at, submission_id LIMIT ?"
        parameters.append(limit)
        with self._lock:
            rows = self._connection.execute(query, tuple(parameters)).fetchall()
        return [self._research_submission(row) for row in rows]

    def reserve_research_submission(
        self, submission_id: str, *, reserved_at: datetime
    ) -> ResearchSubmissionRecord:
        _require_aware(reserved_at, "reserved_at")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM ros_research_submissions WHERE submission_id = ?",
                (submission_id,),
            ).fetchone()
            if row is None:
                raise CatalogNotFound(f"research submission {submission_id!r} was not found")
            current = self._research_submission(row)
            if current.status != "reviewed":
                return current
            trial = connection.execute(
                "SELECT admission_status, reason FROM ros_trial_ledger WHERE trial_id = ?",
                (current.trial_id,),
            ).fetchone()
            if trial is None:
                raise CatalogConflict("submission cannot reserve before its trial ledger row")
            if trial["admission_status"] != TrialAdmissionStatus.ADMITTED.value:
                connection.execute(
                    """
                    UPDATE ros_research_submissions
                    SET status = 'failed', error = ?, lease_owner = NULL,
                        lease_expires_at = NULL, updated_at = ?
                    WHERE submission_id = ? AND status = 'reviewed'
                    """,
                    (str(trial["reason"]), _time_text(reserved_at), submission_id),
                )
            else:
                connection.execute(
                    """
                    UPDATE ros_research_submissions
                    SET status = 'reserved', error = NULL, updated_at = ?
                    WHERE submission_id = ? AND status = 'reviewed'
                    """,
                    (_time_text(reserved_at), submission_id),
                )
            row = connection.execute(
                "SELECT * FROM ros_research_submissions WHERE submission_id = ?",
                (submission_id,),
            ).fetchone()
            assert row is not None
            return self._research_submission(row)

    def claim_research_submission(
        self,
        submission_id: str,
        *,
        worker_id: str,
        claimed_at: datetime,
        lease_expires_at: datetime,
    ) -> tuple[ResearchSubmissionRecord, bool]:
        claimed_at = _require_aware(claimed_at, "claimed_at")
        lease_expires_at = _require_aware(lease_expires_at, "lease_expires_at")
        if not worker_id.strip():
            raise ValueError("worker_id must not be empty")
        if lease_expires_at <= claimed_at:
            raise ValueError("lease expiry must be after claim time")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM ros_research_submissions WHERE submission_id = ?",
                (submission_id,),
            ).fetchone()
            if row is None:
                raise CatalogNotFound(f"research submission {submission_id!r} was not found")
            current = self._research_submission(row)
            if current.status in TERMINAL_SUBMISSION_STATUSES or current.status == "reviewed":
                return current, False
            if current.status == "running":
                assert current.lease_expires_at is not None
                if current.lease_expires_at > claimed_at:
                    return current, False
            connection.execute(
                """
                UPDATE ros_research_submissions
                SET status = 'running', lease_owner = ?, lease_expires_at = ?,
                    attempts = attempts + 1, updated_at = ?
                WHERE submission_id = ?
                """,
                (
                    worker_id,
                    _time_text(lease_expires_at),
                    _time_text(claimed_at),
                    submission_id,
                ),
            )
            row = connection.execute(
                "SELECT * FROM ros_research_submissions WHERE submission_id = ?",
                (submission_id,),
            ).fetchone()
            assert row is not None
            return self._research_submission(row), True

    def renew_research_submission(
        self,
        submission_id: str,
        *,
        worker_id: str,
        lease_token: str,
        renewed_at: datetime,
        lease_expires_at: datetime,
    ) -> ResearchSubmissionRecord:
        renewed_at = _require_aware(renewed_at, "renewed_at")
        lease_expires_at = _require_aware(lease_expires_at, "lease_expires_at")
        lease_token = _require_sha256(lease_token, name="lease_token")
        if not worker_id.strip():
            raise ValueError("worker_id must not be empty")
        if lease_expires_at <= renewed_at:
            raise ValueError("lease expiry must be after renewal time")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM ros_research_submissions WHERE submission_id = ?",
                (submission_id,),
            ).fetchone()
            if row is None:
                raise CatalogNotFound(f"research submission {submission_id!r} was not found")
            current = self._research_submission(row)
            if (
                current.status != "running"
                or current.lease_owner != worker_id
                or current.lease_expires_at is None
                or current.lease_expires_at <= renewed_at
                or research_submission_lease_token(current) != lease_token
            ):
                raise CatalogConflict("research submission lease is missing, stale or expired")
            if lease_expires_at <= current.lease_expires_at:
                # Database clocks (notably SQLite) can have coarser precision
                # than the worker. A same-generation ownership assertion is
                # therefore an idempotent no-op when the existing expiry is
                # already at least as strong as the requested extension.
                return current
            connection.execute(
                """
                UPDATE ros_research_submissions
                SET lease_expires_at = ?, updated_at = ?
                WHERE submission_id = ? AND status = 'running'
                  AND lease_owner = ? AND attempts = ?
                """,
                (
                    _time_text(lease_expires_at),
                    _time_text(renewed_at),
                    submission_id,
                    worker_id,
                    current.attempts,
                ),
            )
            row = connection.execute(
                "SELECT * FROM ros_research_submissions WHERE submission_id = ?",
                (submission_id,),
            ).fetchone()
            assert row is not None
            return self._research_submission(row)

    def finish_research_submission(
        self,
        submission_id: str,
        *,
        worker_id: str,
        lease_token: str | None,
        status: str,
        finished_at: datetime,
        experiment_id: str | None,
        error: str | None,
    ) -> ResearchSubmissionRecord:
        finished_at = _require_aware(finished_at, "finished_at")
        if lease_token is not None:
            lease_token = _require_sha256(lease_token, name="lease_token")
        if status not in TERMINAL_SUBMISSION_STATUSES:
            raise ValueError("submission may only finish in a terminal state")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM ros_research_submissions WHERE submission_id = ?",
                (submission_id,),
            ).fetchone()
            if row is None:
                raise CatalogNotFound(f"research submission {submission_id!r} was not found")
            current = self._research_submission(row)
            if current.status in TERMINAL_SUBMISSION_STATUSES:
                if (
                    current.status == status
                    and current.experiment_id == experiment_id
                    and current.error == error
                ):
                    return current
                raise CatalogConflict("research submission already has a terminal outcome")
            if current.status != "running" or current.lease_owner != worker_id:
                raise CatalogConflict("only the active lease owner may finish a submission")
            if lease_token is not None and (
                current.lease_expires_at is None
                or current.lease_expires_at <= finished_at
                or research_submission_lease_token(current) != lease_token
            ):
                raise CatalogConflict("research submission terminal write has a stale lease")
            if experiment_id is not None and connection.execute(
                "SELECT 1 FROM ros_experiments WHERE experiment_id = ?",
                (experiment_id,),
            ).fetchone() is None:
                raise CatalogNotFound(f"experiment {experiment_id!r} was not found")
            connection.execute(
                """
                UPDATE ros_research_submissions
                SET status = ?, experiment_id = ?, error = ?, lease_owner = NULL,
                    lease_expires_at = NULL, updated_at = ?
                WHERE submission_id = ? AND status = 'running' AND lease_owner = ?
                  AND attempts = ?
                """,
                (
                    status,
                    experiment_id,
                    error,
                    _time_text(finished_at),
                    submission_id,
                    worker_id,
                    current.attempts,
                ),
            )
            row = connection.execute(
                "SELECT * FROM ros_research_submissions WHERE submission_id = ?",
                (submission_id,),
            ).fetchone()
            assert row is not None
            return self._research_submission(row)

    def append_lifecycle_event(self, event: LifecycleEvent) -> LifecycleEvent:
        try:
            with self._transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO ros_lifecycle_events(
                        event_id, idempotency_key, sleeve_id, from_state, to_state,
                        cause, evidence_json, occurred_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.event_id,
                        event.idempotency_key,
                        event.sleeve_id,
                        event.from_state.value if event.from_state is not None else None,
                        event.to_state.value,
                        event.cause,
                        canonical_json(event.evidence),
                        _time_text(event.occurred_at),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            with self._lock:
                row = self._connection.execute(
                    "SELECT * FROM ros_lifecycle_events WHERE idempotency_key = ?",
                    (event.idempotency_key,),
                ).fetchone()
            if row is not None:
                existing = self._lifecycle_from_row(row)
                if (
                    existing.sleeve_id == event.sleeve_id
                    and existing.from_state == event.from_state
                    and existing.to_state == event.to_state
                    and existing.cause == event.cause
                    and canonical_json(existing.evidence) == canonical_json(event.evidence)
                    and existing.occurred_at == _require_aware(event.occurred_at, "occurred_at")
                ):
                    return existing
            raise CatalogConflict(
                f"lifecycle idempotency key {event.idempotency_key!r} was reused"
            ) from exc
        return event

    @staticmethod
    def _lifecycle_from_row(row: sqlite3.Row) -> LifecycleEvent:
        return LifecycleEvent(
            event_id=row["event_id"],
            idempotency_key=row["idempotency_key"],
            sleeve_id=row["sleeve_id"],
            from_state=LifecycleState(row["from_state"]) if row["from_state"] else None,
            to_state=LifecycleState(row["to_state"]),
            cause=row["cause"],
            evidence=json.loads(row["evidence_json"]),
            occurred_at=_parse_time(row["occurred_at"]),
        )

    def list_lifecycle_events(
        self, *, limit: int, sleeve_id: str | None
    ) -> list[LifecycleEvent]:
        query = "SELECT * FROM ros_lifecycle_events"
        params: list[Any] = []
        if sleeve_id is not None:
            query += " WHERE sleeve_id = ?"
            params.append(sleeve_id)
        query += " ORDER BY occurred_at DESC, event_id DESC LIMIT ?"
        params.append(_validate_limit(limit))
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        return [self._lifecycle_from_row(row) for row in rows]

    def iter_lifecycle_events(
        self,
        *,
        sleeve_id: str | None,
        cause: str | None,
        batch_size: int,
    ) -> Iterator[LifecycleEvent]:
        clauses: list[str] = []
        params: list[Any] = []
        if sleeve_id is not None:
            clauses.append("sleeve_id = ?")
            params.append(sleeve_id)
        if cause is not None:
            clauses.append("cause = ?")
            params.append(cause)
        query = "SELECT * FROM ros_lifecycle_events"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY occurred_at DESC, event_id DESC"
        size = _validate_limit(batch_size)

        def stream() -> Iterator[LifecycleEvent]:
            with self._lock:
                cursor = self._connection.execute(query, tuple(params))
                while rows := cursor.fetchmany(size):
                    for row in rows:
                        yield self._lifecycle_from_row(row)

        return stream()

    def latest_lifecycle_state(self, sleeve_id: str) -> LifecycleState | None:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT to_state FROM ros_lifecycle_events
                WHERE sleeve_id = ? ORDER BY occurred_at DESC, event_id DESC LIMIT 1
                """,
                (sleeve_id,),
            ).fetchone()
        return None if row is None else LifecycleState(row["to_state"])

    def save_recovery_case(self, case: RecoveryCase) -> RecoveryCase:
        now = _utc_now()
        with self._transaction() as connection:
            existing_row = connection.execute(
                "SELECT case_json FROM ros_recovery_cases WHERE recovery_case_id = ?",
                (case.recovery_case_id,),
            ).fetchone()
            if existing_row is None:
                connection.execute(
                    """
                    INSERT INTO ros_recovery_cases(
                        recovery_case_id, sleeve_id, status, lifecycle_state, case_json,
                        triggered_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        case.recovery_case_id,
                        case.sleeve_id,
                        case.status.value,
                        case.lifecycle_state.value,
                        canonical_json(case),
                        _time_text(case.triggered_at),
                        _time_text(now),
                    ),
                )
            else:
                existing = RecoveryCase.model_validate_json(existing_row["case_json"])
                immutable_fields = (
                    "sleeve_id",
                    "triggered_at",
                    "drift_event_due_at",
                    "diagnosis_due_at",
                    "earliest_recovery_review_at",
                    "data_integrity_failure",
                    "trigger_evidence",
                )
                if any(getattr(existing, name) != getattr(case, name) for name in immutable_fields):
                    raise CatalogConflict(
                        f"recovery case identity collision for {case.recovery_case_id!r}"
                    )
                connection.execute(
                    """
                    UPDATE ros_recovery_cases
                    SET status = ?, lifecycle_state = ?, case_json = ?, updated_at = ?
                    WHERE recovery_case_id = ?
                    """,
                    (
                        case.status.value,
                        case.lifecycle_state.value,
                        canonical_json(case),
                        _time_text(now),
                        case.recovery_case_id,
                    ),
                )
        return case

    def get_recovery_case(self, recovery_case_id: str) -> RecoveryCase | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT case_json FROM ros_recovery_cases WHERE recovery_case_id = ?",
                (recovery_case_id,),
            ).fetchone()
        return None if row is None else RecoveryCase.model_validate_json(row["case_json"])

    def list_recovery_cases(
        self,
        *,
        limit: int,
        status: RecoveryCaseStatus | None,
        sleeve_id: str | None,
    ) -> list[RecoveryCase]:
        clauses: list[str] = []
        params: list[Any] = []
        if status is not None:
            clauses.append("status = ?")
            params.append(status.value)
        if sleeve_id is not None:
            clauses.append("sleeve_id = ?")
            params.append(sleeve_id)
        query = "SELECT case_json FROM ros_recovery_cases"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY triggered_at DESC, recovery_case_id LIMIT ?"
        params.append(_validate_limit(limit))
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        return [RecoveryCase.model_validate_json(row["case_json"]) for row in rows]

    def iter_recovery_cases(
        self,
        *,
        statuses: Sequence[RecoveryCaseStatus] | None,
        sleeve_id: str | None,
        batch_size: int,
    ) -> Iterator[RecoveryCase]:
        clauses: list[str] = []
        params: list[Any] = []
        if statuses is not None:
            placeholders = ",".join("?" for _status in statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(status.value for status in statuses)
        if sleeve_id is not None:
            clauses.append("sleeve_id = ?")
            params.append(sleeve_id)
        query = "SELECT case_json FROM ros_recovery_cases"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY triggered_at DESC, recovery_case_id"
        size = _validate_limit(batch_size)

        def stream() -> Iterator[RecoveryCase]:
            with self._lock:
                cursor = self._connection.execute(query, tuple(params))
                while rows := cursor.fetchmany(size):
                    for row in rows:
                        yield RecoveryCase.model_validate_json(row["case_json"])

        return stream()

    def save_run(self, run: RunRecord) -> RunRecord:
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM ros_runs WHERE run_id = ?", (run.run_id,)
            ).fetchone()
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO ros_runs(
                        run_id, run_type, status, input_fingerprint, metadata_json,
                        error, started_at, completed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run.run_id,
                        run.run_type,
                        run.status,
                        run.input_fingerprint,
                        canonical_json(run.metadata),
                        run.error,
                        _time_text(run.started_at),
                        _time_text(run.completed_at) if run.completed_at is not None else None,
                    ),
                )
            else:
                persisted = self._run_from_row(existing)
                _assert_run_identity(persisted, run)
                if _run_is_terminal(persisted):
                    _assert_terminal_run_replay(persisted, run)
                    return persisted
                connection.execute(
                    """
                    UPDATE ros_runs SET status = ?, metadata_json = ?, error = ?,
                        completed_at = ? WHERE run_id = ? AND status = 'running'
                    """,
                    (
                        run.status,
                        canonical_json(run.metadata),
                        run.error,
                        _time_text(run.completed_at) if run.completed_at is not None else None,
                        run.run_id,
                    ),
                )
        return run

    def claim_run(self, run: RunRecord) -> tuple[RunRecord, bool]:
        """Insert a running record exactly once and return whether this caller won."""

        try:
            with self._transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO ros_runs(
                        run_id, run_type, status, input_fingerprint, metadata_json,
                        error, started_at, completed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run.run_id,
                        run.run_type,
                        run.status,
                        run.input_fingerprint,
                        canonical_json(run.metadata),
                        run.error,
                        _time_text(run.started_at),
                        _time_text(run.completed_at) if run.completed_at else None,
                    ),
                )
            return run, True
        except sqlite3.IntegrityError as exc:
            existing = self.get_run(run.run_id)
            if existing is None:
                raise
            if (
                existing.run_type != run.run_type
                or existing.input_fingerprint != run.input_fingerprint
            ):
                raise CatalogConflict(f"run identity collision for {run.run_id!r}") from exc
            if _run_is_terminal(existing) and run.status != "running":
                try:
                    _assert_terminal_run_replay(existing, run)
                except CatalogConflict as conflict:
                    raise conflict from exc
            return existing, False

    def get_run(self, run_id: str) -> RunRecord | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM ros_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        return None if row is None else self._run_from_row(row)

    @staticmethod
    def _run_from_row(row: sqlite3.Row) -> RunRecord:
        return RunRecord(
            run_id=row["run_id"],
            run_type=row["run_type"],
            status=row["status"],
            input_fingerprint=row["input_fingerprint"],
            metadata=json.loads(row["metadata_json"]),
            error=row["error"],
            started_at=_parse_time(row["started_at"]),
            completed_at=_parse_time(row["completed_at"]) if row["completed_at"] else None,
        )

    def list_runs(
        self, *, limit: int, status: str | None, run_type: str | None
    ) -> list[RunRecord]:
        clauses: list[str] = []
        params: list[Any] = []
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        if run_type is not None:
            clauses.append("run_type = ?")
            params.append(run_type)
        query = "SELECT * FROM ros_runs"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY started_at DESC, run_id LIMIT ?"
        params.append(_validate_limit(limit))
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        return [self._run_from_row(row) for row in rows]

    def import_legacy_evidence(self, evidence: LegacyEvidenceRecord) -> LegacyEvidenceRecord:
        try:
            with self._transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO ros_legacy_evidence(
                        evidence_id, source_uri, content_hash, trust_label,
                        reasons_json, imported_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        evidence.evidence_id,
                        evidence.source_uri,
                        evidence.content_hash,
                        evidence.trust_label,
                        canonical_json(evidence.reasons),
                        _time_text(evidence.imported_at),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            with self._lock:
                row = self._connection.execute(
                    """
                    SELECT * FROM ros_legacy_evidence
                    WHERE source_uri = ? AND content_hash = ?
                    """,
                    (evidence.source_uri, evidence.content_hash),
                ).fetchone()
            if row is not None:
                existing = self._legacy_from_row(row)
                if (
                    existing.source_uri == evidence.source_uri
                    and existing.content_hash == evidence.content_hash
                    and existing.trust_label == evidence.trust_label
                    and existing.reasons == evidence.reasons
                ):
                    return existing
            raise CatalogConflict(
                "legacy evidence was reimported with conflicting trust metadata"
            ) from exc
        return evidence

    @staticmethod
    def _legacy_from_row(row: sqlite3.Row) -> LegacyEvidenceRecord:
        return LegacyEvidenceRecord(
            evidence_id=row["evidence_id"],
            source_uri=row["source_uri"],
            content_hash=row["content_hash"],
            trust_label=row["trust_label"],
            reasons=tuple(json.loads(row["reasons_json"])),
            imported_at=_parse_time(row["imported_at"]),
        )

    def list_legacy_evidence(
        self, *, limit: int, trust_label: str | None
    ) -> list[LegacyEvidenceRecord]:
        query = "SELECT * FROM ros_legacy_evidence"
        params: list[Any] = []
        if trust_label is not None:
            query += " WHERE trust_label = ?"
            params.append(trust_label)
        query += " ORDER BY imported_at DESC, evidence_id LIMIT ?"
        params.append(_validate_limit(limit))
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        return [self._legacy_from_row(row) for row in rows]

    @staticmethod
    def _shadow_account_from_row(row: sqlite3.Row) -> ShadowAccountRecord:
        return ShadowAccountRecord(
            account_id=row["account_id"],
            name=row["name"],
            currency=row["currency"],
            initial_capital=row["initial_capital"],
            cash=row["cash"],
            nav=row["nav"],
            benchmark_nav=row["benchmark_nav"],
            status=row["status"],
            as_of=_parse_time(row["as_of"]),
            last_event_sequence=row["last_event_sequence"],
            last_event_hash=row["last_event_hash"],
        )

    @staticmethod
    def _shadow_event_from_row(row: sqlite3.Row) -> ShadowEvent:
        return ShadowEvent(
            event_id=row["event_id"],
            account_id=row["account_id"],
            sequence_number=row["sequence_number"],
            event_type=row["event_type"],
            occurred_at=_parse_time(row["occurred_at"]),
            payload=json.loads(row["payload_json"]),
            previous_event_hash=row["previous_event_hash"],
            event_hash=row["event_hash"],
        )

    def create_shadow_account(
        self,
        *,
        account_id: str,
        name: str,
        initial_capital: float,
        opened_at: datetime,
        currency: str,
    ) -> ShadowAccountRecord:
        opened_at = _require_aware(opened_at, "opened_at")
        if not account_id or not name or not currency:
            raise ValueError("account_id, name, and currency are required")
        if initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        payload = {
            "name": name,
            "currency": currency,
            "initial_capital": float(initial_capital),
            "account_state": {
                "cash": float(initial_capital),
                "nav": float(initial_capital),
                "benchmark_nav": float(initial_capital),
            },
        }
        event_hash = _shadow_event_hash(
            account_id=account_id,
            sequence_number=1,
            event_type="account_opened",
            occurred_at=opened_at,
            payload=payload,
            previous_event_hash=_ZERO_EVENT_HASH,
        )
        event_id = f"sev_{event_hash[:32]}"
        try:
            with self._transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO ros_shadow_accounts(
                        account_id, name, currency, initial_capital, cash, nav,
                        benchmark_nav, status, as_of, last_event_sequence,
                        last_event_hash, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, 1, ?, ?)
                    """,
                    (
                        account_id,
                        name,
                        currency,
                        float(initial_capital),
                        float(initial_capital),
                        float(initial_capital),
                        float(initial_capital),
                        _time_text(opened_at),
                        event_hash,
                        _time_text(opened_at),
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO ros_shadow_events(
                        event_id, account_id, sequence_number, event_type, occurred_at,
                        payload_json, previous_event_hash, event_hash
                    ) VALUES (?, ?, 1, 'account_opened', ?, ?, ?, ?)
                    """,
                    (
                        event_id,
                        account_id,
                        _time_text(opened_at),
                        canonical_json(payload),
                        _ZERO_EVENT_HASH,
                        event_hash,
                    ),
                )
        except sqlite3.IntegrityError as exc:
            existing = self.get_shadow_account(account_id)
            with self._lock:
                opening_event = self._connection.execute(
                    """
                    SELECT occurred_at FROM ros_shadow_events
                    WHERE account_id = ? AND sequence_number = 1
                    """,
                    (account_id,),
                ).fetchone()
            if (
                existing is not None
                and existing.name == name
                and existing.currency == currency
                and existing.initial_capital == float(initial_capital)
                and opening_event is not None
                and _parse_time(opening_event["occurred_at"]) == opened_at
            ):
                return existing
            raise CatalogConflict(f"shadow account {account_id!r} already exists") from exc
        result = self.get_shadow_account(account_id)
        assert result is not None
        return result

    def get_shadow_account(self, account_id: str) -> ShadowAccountRecord | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM ros_shadow_accounts WHERE account_id = ?", (account_id,)
            ).fetchone()
        return None if row is None else self._shadow_account_from_row(row)

    def list_shadow_accounts(
        self, *, limit: int, status: str | None
    ) -> list[ShadowAccountRecord]:
        query = "SELECT * FROM ros_shadow_accounts"
        params: list[Any] = []
        if status is not None:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY as_of DESC, account_id LIMIT ?"
        params.append(_validate_limit(limit))
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        return [self._shadow_account_from_row(row) for row in rows]

    def iter_shadow_accounts(
        self, *, status: str | None, batch_size: int
    ) -> Iterator[ShadowAccountRecord]:
        query = "SELECT * FROM ros_shadow_accounts"
        params: list[Any] = []
        if status is not None:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY as_of DESC, account_id"
        size = _validate_limit(batch_size)

        def stream() -> Iterator[ShadowAccountRecord]:
            with self._lock:
                cursor = self._connection.execute(query, tuple(params))
                while rows := cursor.fetchmany(size):
                    for row in rows:
                        yield self._shadow_account_from_row(row)

        return stream()

    def append_shadow_event(
        self,
        *,
        account_id: str,
        event_type: str,
        occurred_at: datetime,
        payload: Mapping[str, Any],
        expected_previous_hash: str | None,
    ) -> ShadowEvent:
        return self.append_shadow_events_atomic(
            account_id=account_id,
            events=(
                ShadowEventInput(
                    event_type=event_type,
                    occurred_at=occurred_at,
                    payload=payload,
                ),
            ),
            expected_previous_hash=expected_previous_hash,
        )[0]

    def append_shadow_events_atomic(
        self,
        *,
        account_id: str,
        events: Sequence[ShadowEventInput | Mapping[str, Any]],
        expected_previous_hash: str | None,
    ) -> list[ShadowEvent]:
        normalized = _normalize_shadow_event_inputs(events)
        committed: list[ShadowEvent] = []
        with self._transaction() as connection:
            account = connection.execute(
                "SELECT * FROM ros_shadow_accounts WHERE account_id = ?", (account_id,)
            ).fetchone()
            if account is None:
                raise CatalogNotFound(f"shadow account {account_id!r} was not found")
            previous_hash = account["last_event_hash"]
            if expected_previous_hash is not None and expected_previous_hash != previous_hash:
                raise CatalogConflict("shadow event optimistic-lock hash does not match")
            sequence = int(account["last_event_sequence"])
            last_time = _parse_time(account["as_of"])
            cash = float(account["cash"])
            nav = float(account["nav"])
            benchmark_nav = float(account["benchmark_nav"])
            status = str(account["status"])
            for event_type, occurred_at, payload_dict, account_state, position_state in normalized:
                if occurred_at < last_time:
                    raise CatalogConflict(
                        "events in an atomic shadow step must be chronological"
                    )
                sequence += 1
                event_hash = _shadow_event_hash(
                    account_id=account_id,
                    sequence_number=sequence,
                    event_type=event_type,
                    occurred_at=occurred_at,
                    payload=payload_dict,
                    previous_event_hash=previous_hash,
                )
                event_id = f"sev_{event_hash[:32]}"
                connection.execute(
                    """
                    INSERT INTO ros_shadow_events(
                        event_id, account_id, sequence_number, event_type, occurred_at,
                        payload_json, previous_event_hash, event_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_id,
                        account_id,
                        sequence,
                        event_type,
                        _time_text(occurred_at),
                        canonical_json(payload_dict),
                        previous_hash,
                        event_hash,
                    ),
                )
                if position_state is not None:
                    if position_state["quantity"] <= 1e-12:
                        connection.execute(
                            "DELETE FROM ros_shadow_positions WHERE account_id = ? AND ticker = ?",
                            (account_id, position_state["ticker"]),
                        )
                    else:
                        connection.execute(
                            """
                            INSERT INTO ros_shadow_positions(
                                account_id, ticker, quantity, average_cost, market_price,
                                market_value, updated_at, last_event_sequence
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(account_id, ticker) DO UPDATE SET
                                quantity = excluded.quantity,
                                average_cost = excluded.average_cost,
                                market_price = excluded.market_price,
                                market_value = excluded.market_value,
                                updated_at = excluded.updated_at,
                                last_event_sequence = excluded.last_event_sequence
                            """,
                            (
                                account_id,
                                position_state["ticker"],
                                position_state["quantity"],
                                position_state["average_cost"],
                                position_state["market_price"],
                                position_state["market_value"],
                                _time_text(occurred_at),
                                sequence,
                            ),
                        )
                cash = account_state.get("cash", cash)
                nav = account_state.get("nav", nav)
                benchmark_nav = account_state.get("benchmark_nav", benchmark_nav)
                status = str(payload_dict.get("account_status", status))
                committed.append(
                    ShadowEvent(
                        event_id=event_id,
                        account_id=account_id,
                        sequence_number=sequence,
                        event_type=event_type,
                        occurred_at=occurred_at,
                        payload=payload_dict,
                        previous_event_hash=previous_hash,
                        event_hash=event_hash,
                    )
                )
                previous_hash = event_hash
                last_time = occurred_at
            connection.execute(
                """
                UPDATE ros_shadow_accounts
                SET cash = ?, nav = ?, benchmark_nav = ?, status = ?, as_of = ?,
                    last_event_sequence = ?, last_event_hash = ?, updated_at = ?
                WHERE account_id = ?
                """,
                (
                    cash,
                    nav,
                    benchmark_nav,
                    status,
                    _time_text(last_time),
                    sequence,
                    previous_hash,
                    _time_text(_utc_now()),
                    account_id,
                ),
            )
        return committed

    def list_shadow_events(self, *, account_id: str, limit: int) -> list[ShadowEvent]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM ros_shadow_events WHERE account_id = ?
                ORDER BY sequence_number DESC LIMIT ?
                """,
                (account_id, _validate_limit(limit)),
            ).fetchall()
        return [self._shadow_event_from_row(row) for row in rows]

    def list_shadow_events_by_type(
        self,
        *,
        account_id: str,
        event_type: str,
        since: datetime | None,
        through: datetime | None,
        limit: int,
    ) -> list[ShadowEvent]:
        normalized_type = _validate_event_type(event_type)
        clauses = ["account_id = ?", "event_type = ?"]
        params: list[Any] = [account_id, normalized_type]
        if since is not None:
            clauses.append("occurred_at >= ?")
            params.append(_time_text(_require_aware(since, "since")))
        if through is not None:
            clauses.append("occurred_at <= ?")
            params.append(_time_text(_require_aware(through, "through")))
        params.append(_validate_limit(limit))
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM ros_shadow_events WHERE "
                + " AND ".join(clauses)
                + " ORDER BY sequence_number DESC LIMIT ?",
                tuple(params),
            ).fetchall()
        return [self._shadow_event_from_row(row) for row in rows]

    def iter_shadow_events_by_type(
        self,
        *,
        account_id: str,
        event_type: str,
        since: datetime | None,
        through: datetime | None,
        batch_size: int,
    ) -> Iterator[ShadowEvent]:
        normalized_type = _validate_event_type(event_type)
        clauses = ["account_id = ?", "event_type = ?"]
        params: list[Any] = [account_id, normalized_type]
        if since is not None:
            clauses.append("occurred_at >= ?")
            params.append(_time_text(_require_aware(since, "since")))
        if through is not None:
            clauses.append("occurred_at <= ?")
            params.append(_time_text(_require_aware(through, "through")))
        size = _validate_limit(batch_size)
        query = (
            "SELECT * FROM ros_shadow_events WHERE "
            + " AND ".join(clauses)
            + " ORDER BY sequence_number DESC"
        )

        def stream() -> Iterator[ShadowEvent]:
            with self._lock:
                cursor = self._connection.execute(query, tuple(params))
                while rows := cursor.fetchmany(size):
                    for row in rows:
                        yield self._shadow_event_from_row(row)

        return stream()

    def count_shadow_sessions(
        self, *, account_id: str, since: date, through: date
    ) -> int:
        if through < since:
            return 0
        with self._lock:
            row = self._connection.execute(
                """
                SELECT COUNT(DISTINCT substr(occurred_at, 1, 10)) AS session_count
                FROM ros_shadow_events
                WHERE account_id = ? AND event_type = 'account_projected'
                  AND occurred_at > ? AND occurred_at <= ?
                """,
                (
                    account_id,
                    f"{since.isoformat()}T23:59:59.999999+00:00",
                    f"{through.isoformat()}T23:59:59.999999+00:00",
                ),
            ).fetchone()
        return int(row["session_count"] or 0)

    def list_shadow_positions(self, account_id: str) -> list[ShadowPositionRecord]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM ros_shadow_positions WHERE account_id = ?
                ORDER BY ticker
                """,
                (account_id,),
            ).fetchall()
        return [
            ShadowPositionRecord(
                account_id=row["account_id"],
                ticker=row["ticker"],
                quantity=row["quantity"],
                average_cost=row["average_cost"],
                market_price=row["market_price"],
                market_value=row["market_value"],
                updated_at=_parse_time(row["updated_at"]),
                last_event_sequence=row["last_event_sequence"],
            )
            for row in rows
        ]

    def verify_shadow_chain(self, account_id: str) -> bool:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM ros_shadow_events WHERE account_id = ?
                ORDER BY sequence_number
                """,
                (account_id,),
            ).fetchall()
            account = self._connection.execute(
                "SELECT * FROM ros_shadow_accounts WHERE account_id = ?", (account_id,)
            ).fetchone()
        if account is None or not rows:
            return False
        previous_hash = _ZERO_EVENT_HASH
        for expected_sequence, row in enumerate(rows, start=1):
            event = self._shadow_event_from_row(row)
            if (
                event.sequence_number != expected_sequence
                or event.previous_event_hash != previous_hash
                or event.event_hash
                != _shadow_event_hash(
                    account_id=event.account_id,
                    sequence_number=event.sequence_number,
                    event_type=event.event_type,
                    occurred_at=event.occurred_at,
                    payload=event.payload,
                    previous_event_hash=event.previous_event_hash,
                )
            ):
                return False
            previous_hash = event.event_hash
        return (
            int(account["last_event_sequence"]) == len(rows)
            and account["last_event_hash"] == previous_hash
        )

    def catalog_summary(self) -> CatalogSummary:
        table_names = {
            "snapshots": "ros_data_snapshots",
            "experiments": "ros_experiments",
            "results": "ros_experiment_results",
            "trials": "ros_trial_ledger",
            "lifecycle_events": "ros_lifecycle_events",
            "recovery_cases": "ros_recovery_cases",
            "runs": "ros_runs",
            "legacy_evidence": "ros_legacy_evidence",
            "shadow_accounts": "ros_shadow_accounts",
            "shadow_events": "ros_shadow_events",
        }
        with self._lock:
            totals = {
                name: int(
                    self._connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                )
                for name, table in table_names.items()
            }

            def grouped(table: str, column: str) -> dict[str, int]:
                return {
                    str(row[0]): int(row[1])
                    for row in self._connection.execute(
                        f"SELECT {column}, COUNT(*) FROM {table} GROUP BY {column}"
                    ).fetchall()
                }

            latest_run = self._connection.execute(
                "SELECT MAX(started_at) FROM ros_runs"
            ).fetchone()[0]
            return CatalogSummary(
                generated_at=_utc_now(),
                totals=totals,
                experiment_statuses=grouped("ros_experiments", "status"),
                data_quality_statuses=grouped(
                    "ros_data_snapshots", "quality_status"
                ),
                lifecycle_states=grouped("ros_lifecycle_events", "to_state"),
                recovery_statuses=grouped("ros_recovery_cases", "status"),
                latest_run_started_at=_parse_time(latest_run) if latest_run else None,
            )


class _SQLAlchemyCatalog:
    def __init__(
        self,
        database_url: str,
        *,
        connect_args: Mapping[str, Any] | None = None,
    ) -> None:
        if not orm.SQLALCHEMY_AVAILABLE:
            raise MissingInfrastructureDependency(
                "PostgreSQL catalog requires SQLAlchemy 2.x and psycopg 3; "
                "install the Research OS infrastructure dependencies"
            )
        try:
            from sqlalchemy import create_engine
            from sqlalchemy.orm import sessionmaker

            self._engine = create_engine(
                database_url,
                pool_pre_ping=True,
                connect_args=dict(connect_args or {}),
            )
            self._sessions = sessionmaker(
                self._engine, expire_on_commit=False, autoflush=False
            )
        except (ImportError, ModuleNotFoundError) as exc:
            raise MissingInfrastructureDependency(
                "PostgreSQL catalog requires SQLAlchemy 2.x and psycopg 3"
            ) from exc

    def initialize_schema(self) -> None:
        """Fail closed unless Alembic already installed the production schema."""

        from sqlalchemy import inspect, text

        assert orm.Base is not None
        if self._engine.dialect.name != "postgresql":
            orm.Base.metadata.create_all(self._engine)
            with self._sessions.begin() as session:
                existing = session.get(orm.SchemaMetadataModel, "research_os")
                if existing is None:
                    session.add(
                        orm.SchemaMetadataModel(
                            component="research_os", schema_version="research-os/v1"
                        )
                    )
                else:
                    existing.schema_version = "research-os/v1"
            return
        inspector = inspect(self._engine)
        table_names = set(inspector.get_table_names())
        required = set(orm.Base.metadata.tables)
        missing = sorted(required - table_names)
        if missing:
            raise CatalogError(
                "PostgreSQL Research OS schema is not migrated; missing tables: "
                + ", ".join(missing)
            )
        if "alembic_version" not in table_names:
            raise CatalogError(
                "PostgreSQL Research OS schema has no Alembic version; run alembic upgrade head"
            )
        with self._engine.connect() as connection:
            heads = {
                str(row[0])
                for row in connection.execute(
                    text("SELECT version_num FROM alembic_version")
                )
            }
            if heads != {RESEARCH_OS_ALEMBIC_HEAD}:
                raise CatalogError(
                    "PostgreSQL Research OS schema is not at Alembic head "
                    f"{RESEARCH_OS_ALEMBIC_HEAD!r}; found {sorted(heads)!r}"
                )
            schema_version = connection.execute(
                text(
                    "SELECT schema_version FROM ros_schema_metadata "
                    "WHERE component = 'research_os'"
                )
            ).scalar_one_or_none()
        if schema_version != "research-os/v1":
            raise CatalogError(
                "PostgreSQL Research OS schema metadata is missing or incompatible"
            )

    def close(self) -> None:
        self._engine.dispose()

    def database_now(self) -> datetime:
        from sqlalchemy import func, select

        with self._sessions() as session:
            value = session.execute(select(func.now())).scalar_one()
        return _parse_time(value)

    @staticmethod
    def _evidence_epoch_from_model(model: Any) -> EvidenceEpochRecord:
        return EvidenceEpochRecord(
            epoch_id=str(model.epoch_id),
            schema_version=str(model.schema_version),
            architecture_version=str(model.architecture_version),
            frozen_at=_parse_time(model.frozen_at),
            code_hash=str(model.code_hash),
            configuration_hash=str(model.configuration_hash),
            dependency_lock_hash=str(model.dependency_lock_hash),
            dirty_patch_hash=str(model.dirty_patch_hash),
            epoch_hash=str(model.epoch_hash),
            first_forward_session=(
                None
                if model.first_forward_session is None
                else date.fromisoformat(str(model.first_forward_session))
            ),
            calendar_snapshot_id=model.calendar_snapshot_id,
            calendar_snapshot_hash=model.calendar_snapshot_hash,
            calendar_content_hash=model.calendar_content_hash,
            evidence_window_hash=model.evidence_window_hash,
            activated_at=(
                None if model.activated_at is None else _parse_time(model.activated_at)
            ),
            closed_at=(
                None if model.closed_at is None else _parse_time(model.closed_at)
            ),
            superseded_by_epoch_id=model.superseded_by_epoch_id,
        )

    def get_evidence_epoch(self) -> EvidenceEpochRecord | None:
        """Return the single active epoch, or the newest pending epoch.

        The pending fallback preserves the pre-versioning API while the
        pointer remains the sole authority once a forward window is active.
        """

        from sqlalchemy import select

        with self._sessions() as session:
            model = session.scalar(
                select(orm.EvidenceEpochModel)
                .join(
                    orm.EvidenceEpochPointerModel,
                    orm.EvidenceEpochPointerModel.epoch_id
                    == orm.EvidenceEpochModel.epoch_id,
                )
                .where(
                    orm.EvidenceEpochPointerModel.pointer_key
                    == EVIDENCE_EPOCH_SLOT,
                    orm.EvidenceEpochModel.activated_at.is_not(None),
                    orm.EvidenceEpochModel.closed_at.is_(None),
                )
            )
            if model is None:
                model = session.scalar(
                    select(orm.EvidenceEpochModel)
                    .where(
                        orm.EvidenceEpochModel.activated_at.is_(None),
                        orm.EvidenceEpochModel.closed_at.is_(None),
                    )
                    .order_by(
                        orm.EvidenceEpochModel.frozen_at.desc(),
                        orm.EvidenceEpochModel.epoch_id.desc(),
                    )
                    .limit(1)
                )
            return None if model is None else self._evidence_epoch_from_model(model)

    def get_pending_evidence_epoch(self) -> EvidenceEpochRecord | None:
        from sqlalchemy import select

        with self._sessions() as session:
            model = session.scalar(
                select(orm.EvidenceEpochModel)
                .where(
                    orm.EvidenceEpochModel.activated_at.is_(None),
                    orm.EvidenceEpochModel.closed_at.is_(None),
                )
                .order_by(
                    orm.EvidenceEpochModel.frozen_at.desc(),
                    orm.EvidenceEpochModel.epoch_id.desc(),
                )
                .limit(1)
            )
        return None if model is None else self._evidence_epoch_from_model(model)

    def list_evidence_epochs(self, *, limit: int) -> list[EvidenceEpochRecord]:
        from sqlalchemy import select

        with self._sessions() as session:
            models = list(
                session.scalars(
                    select(orm.EvidenceEpochModel)
                    .order_by(
                        orm.EvidenceEpochModel.frozen_at.desc(),
                        orm.EvidenceEpochModel.epoch_id.desc(),
                    )
                    .limit(_validate_limit(limit))
                )
            )
        return [self._evidence_epoch_from_model(model) for model in models]

    def freeze_evidence_epoch(
        self, epoch: EvidenceEpochRecord
    ) -> EvidenceEpochRecord:
        from sqlalchemy import func, select, text
        from sqlalchemy.exc import IntegrityError

        if epoch.first_forward_session is not None:
            raise ValueError("a new evidence epoch must begin with a pending forward window")
        try:
            with self._sessions.begin() as session:
                if self._engine.dialect.name == "postgresql":
                    session.execute(
                        text("SELECT pg_advisory_xact_lock(:lock_key)"),
                        {"lock_key": _EVIDENCE_EPOCH_LOCK_KEY},
                    )
                existing_model = session.scalar(
                    select(orm.EvidenceEpochModel)
                    .where(orm.EvidenceEpochModel.epoch_hash == epoch.epoch_hash)
                    .with_for_update()
                )
                if existing_model is not None:
                    existing = self._evidence_epoch_from_model(existing_model)
                    if existing.freeze_payload() == epoch.freeze_payload():
                        return existing
                    raise CatalogConflict("evidence epoch hash collision")
                latest_frozen = session.scalar(
                    select(func.max(orm.EvidenceEpochModel.frozen_at))
                )
                if (
                    latest_frozen is not None
                    and epoch.frozen_at < _parse_time(latest_frozen)
                ):
                    raise CatalogConflict(
                        "new evidence epoch cannot predate retained history"
                    )
                pending = list(
                    session.scalars(
                        select(orm.EvidenceEpochModel)
                        .where(
                            orm.EvidenceEpochModel.activated_at.is_(None),
                            orm.EvidenceEpochModel.closed_at.is_(None),
                        )
                        .with_for_update()
                    )
                )
                model = orm.EvidenceEpochModel(
                    epoch_slot=epoch.epoch_id,
                    epoch_id=epoch.epoch_id,
                    schema_version=epoch.schema_version,
                    architecture_version=epoch.architecture_version,
                    frozen_at=epoch.frozen_at,
                    code_hash=epoch.code_hash,
                    configuration_hash=epoch.configuration_hash,
                    dependency_lock_hash=epoch.dependency_lock_hash,
                    dirty_patch_hash=epoch.dirty_patch_hash,
                    epoch_hash=epoch.epoch_hash,
                )
                session.add(model)
                # The self-reference must point at an inserted row. Flush the
                # immutable successor before closing abandoned pending versions.
                session.flush()
                for old_pending in pending:
                    old_pending.closed_at = epoch.frozen_at
                    old_pending.superseded_by_epoch_id = epoch.epoch_id
        except IntegrityError as exc:
            with self._sessions() as session:
                existing_model = session.scalar(
                    select(orm.EvidenceEpochModel).where(
                        orm.EvidenceEpochModel.epoch_hash == epoch.epoch_hash
                    )
                )
            existing = (
                None
                if existing_model is None
                else self._evidence_epoch_from_model(existing_model)
            )
            if existing is not None and existing.freeze_payload() == epoch.freeze_payload():
                return existing
            raise CatalogConflict(
                "evidence epoch version could not be frozen"
            ) from exc
        return epoch

    def activate_evidence_epoch(
        self,
        *,
        epoch_id: str,
        expected_epoch_hash: str,
        first_forward_session: date,
        calendar_snapshot_id: str,
        calendar_snapshot_hash: str,
        calendar_content_hash: str,
        activated_at: datetime,
    ) -> EvidenceEpochRecord:
        from sqlalchemy import select, text

        activated_at = _require_aware(activated_at, "activated_at")
        with self._sessions.begin() as session:
            if self._engine.dialect.name == "postgresql":
                session.execute(
                    text("SELECT pg_advisory_xact_lock(:lock_key)"),
                    {"lock_key": _EVIDENCE_EPOCH_LOCK_KEY},
                )
            model = session.execute(
                select(orm.EvidenceEpochModel)
                .where(orm.EvidenceEpochModel.epoch_id == epoch_id)
                .with_for_update()
            ).scalar_one_or_none()
            if model is None:
                raise CatalogNotFound("the architecture evidence epoch is not frozen")
            existing = self._evidence_epoch_from_model(model)
            if existing.epoch_id != epoch_id or existing.epoch_hash != expected_epoch_hash:
                raise CatalogConflict("evidence epoch identity changed before activation")
            if existing.closed_at is not None:
                raise CatalogConflict("closed evidence epoch cannot be activated")
            if existing.first_forward_session is not None:
                if (
                    existing.first_forward_session == first_forward_session
                    and existing.calendar_snapshot_id == calendar_snapshot_id
                    and existing.calendar_snapshot_hash == calendar_snapshot_hash
                    and existing.calendar_content_hash == calendar_content_hash
                ):
                    pointer = session.get(
                        orm.EvidenceEpochPointerModel,
                        EVIDENCE_EPOCH_SLOT,
                        with_for_update=True,
                    )
                    if pointer is None:
                        session.add(
                            orm.EvidenceEpochPointerModel(
                                pointer_key=EVIDENCE_EPOCH_SLOT,
                                epoch_id=epoch_id,
                                updated_at=activated_at,
                            )
                        )
                    elif pointer.epoch_id != epoch_id:
                        raise CatalogConflict(
                            "activated epoch differs from the active pointer"
                        )
                    return existing
                raise CatalogConflict("the forward evidence window is already activated")
            window_payload = {
                "epoch_id": existing.epoch_id,
                "epoch_hash": existing.epoch_hash,
                "first_forward_session": first_forward_session.isoformat(),
                "calendar_snapshot_id": calendar_snapshot_id,
                "calendar_snapshot_hash": calendar_snapshot_hash,
                "calendar_content_hash": calendar_content_hash,
                "activated_at": activated_at.isoformat(),
            }
            window_hash = content_fingerprint(
                window_payload,
                domain="factor-lab/research-os/v1/evidence-window",
            )
            candidate = EvidenceEpochRecord(
                epoch_id=existing.epoch_id,
                architecture_version=existing.architecture_version,
                frozen_at=existing.frozen_at,
                code_hash=existing.code_hash,
                configuration_hash=existing.configuration_hash,
                dependency_lock_hash=existing.dependency_lock_hash,
                dirty_patch_hash=existing.dirty_patch_hash,
                epoch_hash=existing.epoch_hash,
                first_forward_session=first_forward_session,
                calendar_snapshot_id=calendar_snapshot_id,
                calendar_snapshot_hash=calendar_snapshot_hash,
                calendar_content_hash=calendar_content_hash,
                evidence_window_hash=window_hash,
                activated_at=activated_at,
            )
            active_models = list(
                session.scalars(
                    select(orm.EvidenceEpochModel)
                    .where(
                        orm.EvidenceEpochModel.epoch_id != epoch_id,
                        orm.EvidenceEpochModel.activated_at.is_not(None),
                        orm.EvidenceEpochModel.closed_at.is_(None),
                    )
                    .with_for_update()
                )
            )
            for active_model in active_models:
                active_model.closed_at = activated_at
                active_model.superseded_by_epoch_id = epoch_id
            # Release the partial one-active uniqueness slot before activating
            # the successor. Pointer and role rebinding still commit atomically.
            session.flush()
            model.first_forward_session = first_forward_session.isoformat()
            model.calendar_snapshot_id = calendar_snapshot_id
            model.calendar_snapshot_hash = calendar_snapshot_hash
            model.calendar_content_hash = calendar_content_hash
            model.evidence_window_hash = window_hash
            model.activated_at = activated_at
            pointer = session.get(
                orm.EvidenceEpochPointerModel,
                EVIDENCE_EPOCH_SLOT,
                with_for_update=True,
            )
            if pointer is None:
                session.add(
                    orm.EvidenceEpochPointerModel(
                        pointer_key=EVIDENCE_EPOCH_SLOT,
                        epoch_id=epoch_id,
                        updated_at=activated_at,
                    )
                )
            else:
                pointer.epoch_id = epoch_id
                pointer.updated_at = activated_at
            active_bindings = list(
                session.scalars(
                    select(orm.ShadowRoleBindingModel)
                    .where(orm.ShadowRoleBindingModel.active.is_(True))
                    .with_for_update()
                )
            )
            for binding in active_bindings:
                if binding.epoch_id == epoch_id:
                    continue
                metadata = dict(binding.metadata_json or {})
                if (
                    metadata.get("formal_epoch_eligible") is False
                    or str(metadata.get("evidence_scope") or "").lower()
                    == "non_forward"
                    or str(metadata.get("evidence_class") or "").lower()
                    == "engineering_canary"
                ):
                    continue
                identity = {
                    "role": str(binding.role),
                    "role_key": str(binding.role_key),
                    "account_id": str(binding.account_id),
                    "sleeve_id": binding.sleeve_id,
                    "experiment_id": binding.experiment_id,
                    "epoch_id": epoch_id,
                    "metadata": metadata,
                }
                payload = {**identity, "bound_at": activated_at}
                binding_hash = content_fingerprint(
                    payload, domain=_SHADOW_BINDING_HASH_DOMAIN
                )
                binding.active = False
                binding.unbound_at = activated_at
                # Flush the partial unique-index update before inserting the
                # replacement active role in this transaction.
                session.flush()
                session.add(
                    orm.ShadowRoleBindingModel(
                        binding_id=f"shadow_binding_{binding_hash[:64]}",
                        binding_hash=binding_hash,
                        role=identity["role"],
                        role_key=identity["role_key"],
                        account_id=identity["account_id"],
                        sleeve_id=identity["sleeve_id"],
                        experiment_id=identity["experiment_id"],
                        epoch_id=epoch_id,
                        active=True,
                        bound_at=activated_at,
                        unbound_at=None,
                        metadata_json=metadata,
                    )
                )
        return candidate

    def register_snapshot(self, reference: DataSnapshotRef) -> SnapshotRecord:
        from sqlalchemy import select
        from sqlalchemy.exc import IntegrityError

        now = _utc_now()
        payload = _json_tree(reference)
        try:
            with self._sessions.begin() as session:
                session.add(
                    orm.DataSnapshotModel(
                        snapshot_id=reference.snapshot_id,
                        schema_version=reference.schema_version,
                        tier=reference.tier.value,
                        uri=reference.uri,
                        content_hash=reference.content_hash,
                        as_of=reference.as_of,
                        quality_status=reference.quality_status.value,
                        ref_json=payload,
                        created_at=now,
                    )
                )
        except IntegrityError as exc:
            existing = self.get_snapshot(reference.snapshot_id)
            if existing is not None and canonical_json(existing.reference) == canonical_json(reference):
                return existing
            with self._sessions() as session:
                content_match = session.scalar(
                    select(orm.DataSnapshotModel).where(
                        orm.DataSnapshotModel.content_hash == reference.content_hash
                    )
                )
            if content_match is not None and canonical_json(content_match.ref_json) == canonical_json(payload):
                raise CatalogConflict(
                    "snapshot content already exists under a different snapshot_id"
                ) from exc
            raise CatalogConflict(
                f"snapshot identity collision for {reference.snapshot_id}"
            ) from exc
        return SnapshotRecord(reference=reference, created_at=now)

    def get_snapshot(self, snapshot_id: str) -> SnapshotRecord | None:
        with self._sessions() as session:
            model = session.get(orm.DataSnapshotModel, snapshot_id)
            if model is None:
                return None
            return SnapshotRecord(
                reference=DataSnapshotRef.model_validate(model.ref_json),
                created_at=_parse_time(model.created_at),
            )

    def list_snapshots(
        self,
        *,
        limit: int,
        quality_status: DataQualityStatus | None,
        tier: SnapshotTier | None,
    ) -> list[SnapshotRecord]:
        return list(
            self.list_snapshot_page(
                limit=limit,
                quality_status=quality_status,
                tier=tier,
                after=None,
            ).records
        )

    def list_snapshot_page(
        self,
        *,
        limit: int,
        quality_status: DataQualityStatus | None,
        tier: SnapshotTier | None,
        after: SnapshotPageCursor | None,
    ) -> SnapshotPage:
        from sqlalchemy import and_, or_, select

        page_size = _validate_limit(limit)
        statement = select(orm.DataSnapshotModel)
        if quality_status is not None:
            statement = statement.where(
                orm.DataSnapshotModel.quality_status == quality_status.value
            )
        if tier is not None:
            statement = statement.where(orm.DataSnapshotModel.tier == tier.value)
        if after is not None:
            statement = statement.where(
                or_(
                    orm.DataSnapshotModel.as_of < after.as_of,
                    and_(
                        orm.DataSnapshotModel.as_of == after.as_of,
                        orm.DataSnapshotModel.created_at < after.created_at,
                    ),
                    and_(
                        orm.DataSnapshotModel.as_of == after.as_of,
                        orm.DataSnapshotModel.created_at == after.created_at,
                        orm.DataSnapshotModel.snapshot_id > after.snapshot_id,
                    ),
                )
            )
        statement = statement.order_by(
            orm.DataSnapshotModel.as_of.desc(),
            orm.DataSnapshotModel.created_at.desc(),
            orm.DataSnapshotModel.snapshot_id.asc(),
        ).limit(page_size + 1)
        with self._sessions() as session:
            rows = session.scalars(statement).all()
        records = tuple(
            SnapshotRecord(
                reference=DataSnapshotRef.model_validate(row.ref_json),
                created_at=_parse_time(row.created_at),
            )
            for row in rows[:page_size]
        )
        next_cursor = (
            SnapshotPageCursor.from_record(records[-1])
            if len(rows) > page_size
            else None
        )
        return SnapshotPage(records=records, next_cursor=next_cursor)

    @staticmethod
    def _experiment_record(model: Any) -> ExperimentRecord:
        return ExperimentRecord(
            experiment_id=model.experiment_id,
            fingerprint=model.fingerprint,
            status=ExperimentStatus(model.status),
            spec=ExperimentSpec.model_validate(model.spec_json),
            registered_at=_parse_time(model.registered_at),
            updated_at=_parse_time(model.updated_at),
        )

    def register_experiment(self, spec: ExperimentSpec) -> ExperimentRecord:
        from sqlalchemy.exc import IntegrityError

        fingerprint = experiment_fingerprint(spec)
        experiment_id = f"exp_{fingerprint[:32]}"
        now = _utc_now()
        payload = _json_tree(spec)
        try:
            with self._sessions.begin() as session:
                session.add(
                    orm.ExperimentModel(
                        experiment_id=experiment_id,
                        fingerprint=fingerprint,
                        snapshot_id=spec.snapshot.snapshot_id,
                        candidate_kind=spec.candidate_kind.value,
                        candidate_id=spec.candidate_id,
                        family=spec.family,
                        status=ExperimentStatus.PREREGISTERED.value,
                        spec_json=payload,
                        registered_at=now,
                        updated_at=now,
                    )
                )
        except IntegrityError as exc:
            existing = self.get_experiment_by_fingerprint(fingerprint)
            if existing is not None and canonical_json(existing.spec) == canonical_json(spec):
                return existing
            if self.get_snapshot(spec.snapshot.snapshot_id) is None:
                raise CatalogNotFound(
                    f"snapshot {spec.snapshot.snapshot_id!r} is not registered"
                ) from exc
            raise CatalogConflict(f"experiment fingerprint collision: {fingerprint}") from exc
        return ExperimentRecord(
            experiment_id=experiment_id,
            fingerprint=fingerprint,
            status=ExperimentStatus.PREREGISTERED,
            spec=spec,
            registered_at=now,
            updated_at=now,
        )

    def get_experiment(self, experiment_id: str) -> ExperimentRecord | None:
        with self._sessions() as session:
            model = session.get(orm.ExperimentModel, experiment_id)
            return None if model is None else self._experiment_record(model)

    def get_experiment_by_fingerprint(self, fingerprint: str) -> ExperimentRecord | None:
        from sqlalchemy import select

        with self._sessions() as session:
            model = session.scalar(
                select(orm.ExperimentModel).where(
                    orm.ExperimentModel.fingerprint == fingerprint
                )
            )
            return None if model is None else self._experiment_record(model)

    def list_experiments(
        self,
        *,
        limit: int,
        status: ExperimentStatus | None,
        family: str | None,
        candidate_id: str | None,
    ) -> list[ExperimentRecord]:
        from sqlalchemy import select

        statement = select(orm.ExperimentModel)
        if status is not None:
            statement = statement.where(orm.ExperimentModel.status == status.value)
        if family is not None:
            statement = statement.where(orm.ExperimentModel.family == family)
        if candidate_id is not None:
            statement = statement.where(
                orm.ExperimentModel.candidate_id == candidate_id
            )
        statement = statement.order_by(
            orm.ExperimentModel.registered_at.desc(),
            orm.ExperimentModel.experiment_id,
        ).limit(_validate_limit(limit))
        with self._sessions() as session:
            rows = session.scalars(statement).all()
        return [self._experiment_record(row) for row in rows]

    def set_experiment_status(
        self, experiment_id: str, status: ExperimentStatus
    ) -> ExperimentRecord:
        with self._sessions.begin() as session:
            model = session.get(orm.ExperimentModel, experiment_id)
            if model is None:
                raise CatalogNotFound(f"experiment {experiment_id!r} was not found")
            model.status = status.value
            model.updated_at = _utc_now()
        result = self.get_experiment(experiment_id)
        assert result is not None
        return result

    @staticmethod
    def _result_record(model: Any) -> ExperimentResultRecord:
        return ExperimentResultRecord(
            result_id=model.result_id,
            experiment_id=model.experiment_id,
            result_hash=model.result_hash,
            outcome=model.outcome,
            metrics=dict(model.metrics_json),
            artifact_uri=model.artifact_uri,
            completed_at=_parse_time(model.completed_at),
            authoritative=model.authoritative,
        )

    def record_authoritative_result(
        self,
        experiment_id: str,
        *,
        outcome: str,
        metrics: Mapping[str, Any],
        artifact_uri: str | None = None,
        completed_at: datetime | None = None,
    ) -> ExperimentResultRecord:
        from sqlalchemy.exc import IntegrityError

        completed_at = _require_aware(completed_at or _utc_now(), "completed_at")
        metrics_dict = json.loads(canonical_json(metrics))
        result_hash = content_fingerprint(
            {
                "experiment_id": experiment_id,
                "outcome": outcome,
                "metrics": metrics_dict,
                "artifact_uri": artifact_uri,
            },
            domain="factor-lab/research-os/v1/authoritative-result",
        )
        result_id = f"res_{result_hash[:32]}"
        existing = self.get_authoritative_result(experiment_id)
        if existing is not None:
            if existing.result_hash == result_hash:
                return existing
            raise AuthoritativeResultExists(
                f"experiment {experiment_id!r} already has an authoritative result"
            )
        try:
            with self._sessions.begin() as session:
                experiment = session.get(orm.ExperimentModel, experiment_id)
                if experiment is None:
                    raise CatalogNotFound(f"experiment {experiment_id!r} was not found")
                session.add(
                    orm.ExperimentResultModel(
                        result_id=result_id,
                        experiment_id=experiment_id,
                        result_hash=result_hash,
                        outcome=outcome,
                        metrics_json=metrics_dict,
                        artifact_uri=artifact_uri,
                        authoritative=True,
                        completed_at=completed_at,
                    )
                )
                experiment.status = ExperimentStatus.COMPLETED.value
                experiment.updated_at = completed_at
        except IntegrityError as exc:
            existing = self.get_authoritative_result(experiment_id)
            if existing is not None and existing.result_hash == result_hash:
                return existing
            raise AuthoritativeResultExists(
                f"experiment {experiment_id!r} already has an authoritative result"
            ) from exc
        result = self.get_authoritative_result(experiment_id)
        assert result is not None
        return result

    def get_authoritative_result(
        self, experiment_id: str
    ) -> ExperimentResultRecord | None:
        from sqlalchemy import select

        with self._sessions() as session:
            model = session.scalar(
                select(orm.ExperimentResultModel).where(
                    orm.ExperimentResultModel.experiment_id == experiment_id,
                    orm.ExperimentResultModel.authoritative.is_(True),
                )
            )
            return None if model is None else self._result_record(model)

    @staticmethod
    def _trial_entry(model: Any) -> TrialLedgerEntry:
        metadata = dict(model.metadata_json)
        return TrialLedgerEntry(
            trial_id=model.trial_id,
            experiment_id=model.experiment_id,
            family=model.family,
            candidate_id=model.candidate_id,
            outcome=TrialOutcome(model.outcome),
            reason=model.reason,
            p_value=model.p_value,
            alpha_spent=model.alpha_spent,
            metadata=metadata,
            occurred_at=_parse_time(model.occurred_at),
            admission_status=TrialAdmissionStatus(model.admission_status),
            experiment_fingerprint=model.experiment_fingerprint,
            research_equivalence_hash=model.research_equivalence_hash,
            completed_at=(
                None if model.completed_at is None else _parse_time(model.completed_at)
            ),
            updated_at=_parse_time(model.updated_at),
        )

    @staticmethod
    def _reservation_decision(entry: TrialLedgerEntry) -> TrialAdmissionDecision:
        return TrialAdmissionDecision(
            allowed=entry.admission_status is TrialAdmissionStatus.ADMITTED,
            evidence_class=EvidenceClass(
                str(entry.metadata.get("evidence_class", EvidenceClass.OBSERVED.value))
            ),
            family_trial_index=int(entry.metadata.get("family_trial_index", 1)),
            reasons=tuple(map(str, entry.metadata.get("admission_reasons", ()))),
        )

    def append_trial(self, entry: TrialLedgerEntry) -> TrialLedgerEntry:
        from sqlalchemy.exc import IntegrityError

        if entry.experiment_id is not None and self.get_experiment(entry.experiment_id) is None:
            raise CatalogNotFound(
                f"experiment {entry.experiment_id!r} referenced by trial was not found"
            )
        completed_at = entry.completed_at or entry.occurred_at
        updated_at = entry.updated_at or completed_at
        metadata = dict(entry.metadata)
        try:
            with self._sessions.begin() as session:
                session.add(
                    orm.TrialLedgerModel(
                        trial_id=entry.trial_id,
                        experiment_id=entry.experiment_id,
                        family=entry.family,
                        candidate_id=entry.candidate_id,
                        outcome=entry.outcome.value,
                        reason=entry.reason,
                        p_value=entry.p_value,
                        alpha_spent=entry.alpha_spent,
                        metadata_json=json.loads(canonical_json(metadata)),
                        occurred_at=entry.occurred_at,
                        admission_status=entry.admission_status.value,
                        experiment_fingerprint=entry.experiment_fingerprint,
                        research_equivalence_hash=entry.research_equivalence_hash,
                        completed_at=completed_at,
                        updated_at=updated_at,
                    )
                )
        except IntegrityError as exc:
            raise CatalogConflict(f"trial {entry.trial_id!r} already exists") from exc
        return entry

    def reserve_trial(
        self,
        registration: TrialRegistration,
        *,
        candidate_id: str,
        experiment_id: str | None,
        maximum_monthly_confirmatory_trials: int,
        maximum_monthly_confirmatory_trials_per_family: int,
        maximum_diagnostic_branches: int,
    ) -> TrialReservationRecord:
        from sqlalchemy import select, text
        from sqlalchemy.exc import IntegrityError

        try:
            with self._sessions.begin() as session:
                month_key = registration.registered_at.astimezone(timezone.utc).strftime(
                    "%Y-%m"
                )
                if self._engine.dialect.name == "postgresql":
                    session.execute(
                        text(
                            "SELECT pg_advisory_xact_lock("
                            "hashtext(:lock_key)::bigint)"
                        ),
                        {"lock_key": f"factor-lab:trial-budget:{month_key}"},
                    )
                if experiment_id is not None and session.get(
                    orm.ExperimentModel, experiment_id
                ) is None:
                    raise CatalogNotFound(f"experiment {experiment_id!r} was not found")
                model = session.get(orm.TrialLedgerModel, registration.trial_id)
                if model is not None:
                    entry = self._trial_entry(model)
                    if (
                        entry.experiment_fingerprint
                        != registration.experiment_fingerprint
                        or entry.research_equivalence_hash
                        != registration.research_equivalence_hash
                        or entry.family != registration.family
                        or entry.candidate_id != candidate_id
                    ):
                        raise CatalogConflict(
                            f"trial {registration.trial_id!r} has a different registration"
                        )
                    if experiment_id is not None:
                        if entry.experiment_id not in {None, experiment_id}:
                            raise CatalogConflict(
                                f"trial {registration.trial_id!r} belongs to another experiment"
                            )
                        if model.experiment_id is None:
                            model.experiment_id = experiment_id
                            model.updated_at = _utc_now()
                            session.flush()
                            entry = self._trial_entry(model)
                    return TrialReservationRecord(
                        entry=entry,
                        admission=self._reservation_decision(entry),
                        created=False,
                    )

                models = session.scalars(
                    select(orm.TrialLedgerModel).order_by(
                        orm.TrialLedgerModel.occurred_at,
                        orm.TrialLedgerModel.trial_id,
                    )
                ).all()
                admission = TrialLedger.from_catalog_entries(
                    self._trial_entry(item) for item in models
                ).admit(
                    registration,
                    maximum_monthly_confirmatory_trials=maximum_monthly_confirmatory_trials,
                    maximum_monthly_confirmatory_trials_per_family=maximum_monthly_confirmatory_trials_per_family,
                    maximum_diagnostic_branches=maximum_diagnostic_branches,
                )
                status = (
                    TrialAdmissionStatus.ADMITTED
                    if admission.allowed
                    else TrialAdmissionStatus.REJECTED
                )
                reason = (
                    "confirmatory trial reserved"
                    if admission.allowed
                    else f"trial admission rejected: {','.join(admission.reasons)}"
                )
                metadata = {
                    "trial_kind": registration.kind.value,
                    "evidence_class": admission.evidence_class.value,
                    "experiment_fingerprint": registration.experiment_fingerprint,
                    "research_equivalence_hash": registration.research_equivalence_hash,
                    "hypothesis_id": registration.hypothesis_id,
                    "holdout_id": registration.holdout_id,
                    "variant_id": registration.variant_id,
                    "diagnostic_branch": registration.diagnostic_branch,
                    "admission_status": status.value,
                    "admission_reasons": list(admission.reasons),
                    "family_trial_index": admission.family_trial_index,
                    "reservation_state": (
                        "reserved" if admission.allowed else "rejected"
                    ),
                }
                model = orm.TrialLedgerModel(
                    trial_id=registration.trial_id,
                    experiment_id=experiment_id,
                    family=registration.family,
                    candidate_id=candidate_id,
                    outcome=(
                        TrialOutcome.MANUAL.value
                        if admission.allowed
                        else TrialOutcome.REJECTED.value
                    ),
                    reason=reason,
                    p_value=None,
                    alpha_spent=0.0,
                    metadata_json=metadata,
                    occurred_at=registration.registered_at,
                    admission_status=status.value,
                    experiment_fingerprint=registration.experiment_fingerprint,
                    research_equivalence_hash=registration.research_equivalence_hash,
                    completed_at=(
                        None if admission.allowed else registration.registered_at
                    ),
                    updated_at=_utc_now(),
                )
                session.add(model)
                session.flush()
                entry = self._trial_entry(model)
                return TrialReservationRecord(
                    entry=entry, admission=admission, created=True
                )
        except IntegrityError as exc:
            raise CatalogConflict(
                f"trial reservation {registration.trial_id!r} conflicted"
            ) from exc

    def complete_trial(
        self,
        trial_id: str,
        *,
        outcome: TrialOutcome,
        reason: str,
        p_value: float | None,
        alpha_spent: float,
        metadata: Mapping[str, Any],
        experiment_id: str | None,
        completed_at: datetime,
    ) -> TrialLedgerEntry:
        from sqlalchemy import select

        _require_aware(completed_at, "completed_at")
        if p_value is not None and not 0.0 <= p_value <= 1.0:
            raise ValueError("p_value must be between 0 and 1")
        if not 0.0 <= alpha_spent <= 1.0:
            raise ValueError("alpha_spent must be between 0 and 1")
        with self._sessions.begin() as session:
            statement = select(orm.TrialLedgerModel).where(
                orm.TrialLedgerModel.trial_id == trial_id
            )
            if self._engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            model = session.scalar(statement)
            if model is None:
                raise CatalogNotFound(f"trial {trial_id!r} was not found")
            entry = self._trial_entry(model)
            if entry.admission_status is TrialAdmissionStatus.REJECTED:
                if outcome is TrialOutcome.REJECTED:
                    return entry
                raise CatalogConflict(f"rejected trial {trial_id!r} cannot be completed")
            if experiment_id is not None:
                if session.get(orm.ExperimentModel, experiment_id) is None:
                    raise CatalogNotFound(f"experiment {experiment_id!r} was not found")
                if entry.experiment_id not in {None, experiment_id}:
                    raise CatalogConflict(f"trial {trial_id!r} belongs to another experiment")
            merged = dict(entry.metadata)
            merged.update(dict(metadata))
            merged.update(
                {
                    "admission_status": TrialAdmissionStatus.ADMITTED.value,
                    "experiment_fingerprint": entry.experiment_fingerprint,
                    "research_equivalence_hash": entry.research_equivalence_hash,
                    "reservation_state": "completed",
                }
            )
            effective_experiment = experiment_id or entry.experiment_id
            if entry.completed_at is not None:
                if (
                    entry.outcome is outcome
                    and entry.reason == reason
                    and entry.p_value == p_value
                    and entry.alpha_spent == alpha_spent
                    and entry.experiment_id == effective_experiment
                    and dict(entry.metadata) == merged
                ):
                    return entry
                raise CatalogConflict(f"trial {trial_id!r} already has an outcome")
            model.experiment_id = effective_experiment
            model.outcome = outcome.value
            model.reason = reason
            model.p_value = p_value
            model.alpha_spent = alpha_spent
            model.metadata_json = json.loads(canonical_json(merged))
            model.completed_at = completed_at
            model.updated_at = completed_at
            session.flush()
            return self._trial_entry(model)

    def get_trial(self, trial_id: str) -> TrialLedgerEntry | None:
        with self._sessions() as session:
            model = session.get(orm.TrialLedgerModel, trial_id)
            return None if model is None else self._trial_entry(model)

    def list_trials(self, *, family: str | None = None) -> list[TrialLedgerEntry]:
        from sqlalchemy import select

        statement = select(orm.TrialLedgerModel)
        if family is not None:
            statement = statement.where(orm.TrialLedgerModel.family == family)
        statement = statement.order_by(
            orm.TrialLedgerModel.occurred_at, orm.TrialLedgerModel.trial_id
        )
        with self._sessions() as session:
            rows = session.scalars(statement).all()
        return [self._trial_entry(row) for row in rows]

    @staticmethod
    def _research_family(model: Any) -> ResearchFamilyRecord:
        definition = dict(model.definition_json)
        return ResearchFamilyRecord(
            family_id=model.family_id,
            display_name=str(definition["display_name"]),
            mechanism_key=str(definition["mechanism_key"]),
            cluster_id=str(definition["cluster_id"]),
            allowed_fields=tuple(map(str, definition["allowed_fields"])),
            field_registry=tuple(definition["field_registry"]),
            active=bool(model.active),
            created_at=_parse_time(model.created_at),
            registry_hash=model.registry_hash,
        )

    def register_research_family(
        self, family: ResearchFamilyRecord
    ) -> ResearchFamilyRecord:
        from sqlalchemy.exc import IntegrityError

        try:
            with self._sessions.begin() as session:
                session.add(
                    orm.ResearchFamilyModel(
                        family_id=family.family_id,
                        registry_hash=family.registry_hash,
                        definition_json=json.loads(canonical_json(family.definition())),
                        active=family.active,
                        created_at=family.created_at,
                    )
                )
        except IntegrityError as exc:
            existing = self.get_research_family(family.family_id)
            if existing is not None and existing.registry_hash == family.registry_hash:
                return existing
            raise CatalogConflict(
                f"research family {family.family_id!r} is immutable and already differs"
            ) from exc
        result = self.get_research_family(family.family_id)
        assert result is not None
        return result

    def get_research_family(self, family_id: str) -> ResearchFamilyRecord | None:
        with self._sessions() as session:
            model = session.get(orm.ResearchFamilyModel, family_id)
            return None if model is None else self._research_family(model)

    def list_research_families(
        self, *, active_only: bool
    ) -> list[ResearchFamilyRecord]:
        from sqlalchemy import select

        statement = select(orm.ResearchFamilyModel)
        if active_only:
            statement = statement.where(orm.ResearchFamilyModel.active.is_(True))
        statement = statement.order_by(orm.ResearchFamilyModel.family_id)
        with self._sessions() as session:
            models = session.scalars(statement).all()
        return [self._research_family(model) for model in models]

    @staticmethod
    def _research_submission(model: Any) -> ResearchSubmissionRecord:
        return ResearchSubmissionRecord(
            submission_id=model.submission_id,
            proposal_decision_id=model.proposal_decision_id,
            family_id=model.family_id,
            recovery_case_id=model.recovery_case_id,
            status=model.status,
            research_equivalence_hash=model.research_equivalence_hash,
            experiment_fingerprint=model.experiment_fingerprint,
            trial_id=model.trial_id,
            spec=ExperimentSpec.model_validate(model.spec_json),
            lease_owner=model.lease_owner,
            lease_expires_at=(
                None
                if model.lease_expires_at is None
                else _parse_time(model.lease_expires_at)
            ),
            attempts=int(model.attempts),
            experiment_id=model.experiment_id,
            error=model.error,
            created_at=_parse_time(model.created_at),
            updated_at=_parse_time(model.updated_at),
        )

    @staticmethod
    def _same_submission_identity(
        left: ResearchSubmissionRecord, right: ResearchSubmissionRecord
    ) -> bool:
        return _SQLiteCatalog._same_submission_identity(left, right)

    def create_research_submission(
        self, submission: ResearchSubmissionRecord
    ) -> ResearchSubmissionRecord:
        from sqlalchemy import select
        from sqlalchemy.exc import IntegrityError

        if submission.status != "reviewed":
            raise ValueError("new research submissions must start in reviewed state")
        try:
            with self._sessions.begin() as session:
                family = session.get(orm.ResearchFamilyModel, submission.family_id)
                if family is None or not family.active:
                    raise CatalogNotFound(
                        f"active research family {submission.family_id!r} was not found"
                    )
                if submission.recovery_case_id is not None and session.get(
                    orm.RecoveryCaseModel, submission.recovery_case_id
                ) is None:
                    raise CatalogNotFound(
                        f"recovery case {submission.recovery_case_id!r} was not found"
                    )
                session.add(
                    orm.ResearchSubmissionModel(
                        submission_id=submission.submission_id,
                        proposal_decision_id=submission.proposal_decision_id,
                        family_id=submission.family_id,
                        recovery_case_id=submission.recovery_case_id,
                        status=submission.status,
                        research_equivalence_hash=submission.research_equivalence_hash,
                        experiment_fingerprint=submission.experiment_fingerprint,
                        trial_id=submission.trial_id,
                        spec_json=submission.spec.model_dump(
                            mode="json", exclude_none=False
                        ),
                        lease_owner=None,
                        lease_expires_at=None,
                        attempts=submission.attempts,
                        experiment_id=submission.experiment_id,
                        error=submission.error,
                        created_at=submission.created_at,
                        updated_at=submission.updated_at,
                    )
                )
        except IntegrityError as exc:
            existing = self.get_research_submission(submission.submission_id)
            if existing is None:
                with self._sessions() as session:
                    model = session.scalar(
                        select(orm.ResearchSubmissionModel).where(
                            orm.ResearchSubmissionModel.proposal_decision_id
                            == submission.proposal_decision_id,
                            orm.ResearchSubmissionModel.research_equivalence_hash
                            == submission.research_equivalence_hash,
                        )
                    )
                    existing = (
                        None if model is None else self._research_submission(model)
                    )
            if existing is not None and self._same_submission_identity(existing, submission):
                return existing
            raise CatalogConflict("research submission identity conflicts with durable state") from exc
        result = self.get_research_submission(submission.submission_id)
        assert result is not None
        return result

    def get_research_submission(
        self, submission_id: str
    ) -> ResearchSubmissionRecord | None:
        with self._sessions() as session:
            model = session.get(orm.ResearchSubmissionModel, submission_id)
            return None if model is None else self._research_submission(model)

    def list_research_submissions(
        self, *, limit: int, status: str | None, family_id: str | None
    ) -> list[ResearchSubmissionRecord]:
        from sqlalchemy import select

        statement = select(orm.ResearchSubmissionModel)
        if status is not None:
            statement = statement.where(orm.ResearchSubmissionModel.status == status)
        if family_id is not None:
            statement = statement.where(
                orm.ResearchSubmissionModel.family_id == family_id
            )
        statement = statement.order_by(
            orm.ResearchSubmissionModel.updated_at,
            orm.ResearchSubmissionModel.submission_id,
        ).limit(limit)
        with self._sessions() as session:
            models = session.scalars(statement).all()
        return [self._research_submission(model) for model in models]

    def reserve_research_submission(
        self, submission_id: str, *, reserved_at: datetime
    ) -> ResearchSubmissionRecord:
        from sqlalchemy import select

        reserved_at = _require_aware(reserved_at, "reserved_at")
        with self._sessions.begin() as session:
            statement = select(orm.ResearchSubmissionModel).where(
                orm.ResearchSubmissionModel.submission_id == submission_id
            )
            if self._engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            model = session.scalar(statement)
            if model is None:
                raise CatalogNotFound(f"research submission {submission_id!r} was not found")
            if model.status != "reviewed":
                return self._research_submission(model)
            trial = session.get(orm.TrialLedgerModel, model.trial_id)
            if trial is None:
                raise CatalogConflict("submission cannot reserve before its trial ledger row")
            if trial.admission_status != TrialAdmissionStatus.ADMITTED.value:
                model.status = "failed"
                model.error = str(trial.reason)
                model.lease_owner = None
                model.lease_expires_at = None
            else:
                model.status = "reserved"
                model.error = None
            model.updated_at = reserved_at
            session.flush()
            return self._research_submission(model)

    def claim_research_submission(
        self,
        submission_id: str,
        *,
        worker_id: str,
        claimed_at: datetime,
        lease_expires_at: datetime,
    ) -> tuple[ResearchSubmissionRecord, bool]:
        from sqlalchemy import select

        claimed_at = _require_aware(claimed_at, "claimed_at")
        lease_expires_at = _require_aware(lease_expires_at, "lease_expires_at")
        if not worker_id.strip():
            raise ValueError("worker_id must not be empty")
        if lease_expires_at <= claimed_at:
            raise ValueError("lease expiry must be after claim time")
        with self._sessions.begin() as session:
            statement = select(orm.ResearchSubmissionModel).where(
                orm.ResearchSubmissionModel.submission_id == submission_id
            )
            if self._engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            model = session.scalar(statement)
            if model is None:
                raise CatalogNotFound(f"research submission {submission_id!r} was not found")
            current = self._research_submission(model)
            if current.status in TERMINAL_SUBMISSION_STATUSES or current.status == "reviewed":
                return current, False
            if current.status == "running":
                assert current.lease_expires_at is not None
                if current.lease_expires_at > claimed_at:
                    return current, False
            model.status = "running"
            model.lease_owner = worker_id
            model.lease_expires_at = lease_expires_at
            model.attempts = int(model.attempts) + 1
            model.updated_at = claimed_at
            session.flush()
            return self._research_submission(model), True

    def renew_research_submission(
        self,
        submission_id: str,
        *,
        worker_id: str,
        lease_token: str,
        renewed_at: datetime,
        lease_expires_at: datetime,
    ) -> ResearchSubmissionRecord:
        from sqlalchemy import select

        renewed_at = _require_aware(renewed_at, "renewed_at")
        lease_expires_at = _require_aware(lease_expires_at, "lease_expires_at")
        lease_token = _require_sha256(lease_token, name="lease_token")
        if not worker_id.strip():
            raise ValueError("worker_id must not be empty")
        if lease_expires_at <= renewed_at:
            raise ValueError("lease expiry must be after renewal time")
        with self._sessions.begin() as session:
            statement = select(orm.ResearchSubmissionModel).where(
                orm.ResearchSubmissionModel.submission_id == submission_id
            )
            if self._engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            model = session.scalar(statement)
            if model is None:
                raise CatalogNotFound(f"research submission {submission_id!r} was not found")
            current = self._research_submission(model)
            if (
                current.status != "running"
                or current.lease_owner != worker_id
                or current.lease_expires_at is None
                or current.lease_expires_at <= renewed_at
                or research_submission_lease_token(current) != lease_token
            ):
                raise CatalogConflict("research submission lease is missing, stale or expired")
            if lease_expires_at <= current.lease_expires_at:
                return current
            model.lease_expires_at = lease_expires_at
            model.updated_at = renewed_at
            session.flush()
            return self._research_submission(model)

    def finish_research_submission(
        self,
        submission_id: str,
        *,
        worker_id: str,
        lease_token: str | None,
        status: str,
        finished_at: datetime,
        experiment_id: str | None,
        error: str | None,
    ) -> ResearchSubmissionRecord:
        from sqlalchemy import select

        finished_at = _require_aware(finished_at, "finished_at")
        if lease_token is not None:
            lease_token = _require_sha256(lease_token, name="lease_token")
        if status not in TERMINAL_SUBMISSION_STATUSES:
            raise ValueError("submission may only finish in a terminal state")
        with self._sessions.begin() as session:
            statement = select(orm.ResearchSubmissionModel).where(
                orm.ResearchSubmissionModel.submission_id == submission_id
            )
            if self._engine.dialect.name == "postgresql":
                statement = statement.with_for_update()
            model = session.scalar(statement)
            if model is None:
                raise CatalogNotFound(f"research submission {submission_id!r} was not found")
            current = self._research_submission(model)
            if current.status in TERMINAL_SUBMISSION_STATUSES:
                if (
                    current.status == status
                    and current.experiment_id == experiment_id
                    and current.error == error
                ):
                    return current
                raise CatalogConflict("research submission already has a terminal outcome")
            if current.status != "running" or current.lease_owner != worker_id:
                raise CatalogConflict("only the active lease owner may finish a submission")
            if lease_token is not None and (
                current.lease_expires_at is None
                or current.lease_expires_at <= finished_at
                or research_submission_lease_token(current) != lease_token
            ):
                raise CatalogConflict("research submission terminal write has a stale lease")
            if experiment_id is not None and session.get(
                orm.ExperimentModel, experiment_id
            ) is None:
                raise CatalogNotFound(f"experiment {experiment_id!r} was not found")
            model.status = status
            model.experiment_id = experiment_id
            model.error = error
            model.lease_owner = None
            model.lease_expires_at = None
            model.updated_at = finished_at
            session.flush()
            return self._research_submission(model)

    def append_lifecycle_event(self, event: LifecycleEvent) -> LifecycleEvent:
        from sqlalchemy import select
        from sqlalchemy.exc import IntegrityError

        try:
            with self._sessions.begin() as session:
                session.add(
                    orm.LifecycleEventModel(
                        event_id=event.event_id,
                        idempotency_key=event.idempotency_key,
                        sleeve_id=event.sleeve_id,
                        from_state=event.from_state.value if event.from_state else None,
                        to_state=event.to_state.value,
                        cause=event.cause,
                        evidence_json=json.loads(canonical_json(event.evidence)),
                        occurred_at=event.occurred_at,
                    )
                )
        except IntegrityError as exc:
            with self._sessions() as session:
                row = session.scalar(
                    select(orm.LifecycleEventModel).where(
                        orm.LifecycleEventModel.idempotency_key == event.idempotency_key
                    )
                )
            if row is not None:
                existing = self._lifecycle_from_model(row)
                if (
                    existing.sleeve_id == event.sleeve_id
                    and existing.from_state == event.from_state
                    and existing.to_state == event.to_state
                    and existing.cause == event.cause
                    and canonical_json(existing.evidence) == canonical_json(event.evidence)
                    and existing.occurred_at == _require_aware(event.occurred_at, "occurred_at")
                ):
                    return existing
            raise CatalogConflict(
                f"lifecycle idempotency key {event.idempotency_key!r} was reused"
            ) from exc
        return event

    @staticmethod
    def _lifecycle_from_model(model: Any) -> LifecycleEvent:
        return LifecycleEvent(
            event_id=model.event_id,
            idempotency_key=model.idempotency_key,
            sleeve_id=model.sleeve_id,
            from_state=LifecycleState(model.from_state) if model.from_state else None,
            to_state=LifecycleState(model.to_state),
            cause=model.cause,
            evidence=dict(model.evidence_json),
            occurred_at=_parse_time(model.occurred_at),
        )

    def list_lifecycle_events(
        self, *, limit: int, sleeve_id: str | None
    ) -> list[LifecycleEvent]:
        from sqlalchemy import select

        statement = select(orm.LifecycleEventModel)
        if sleeve_id is not None:
            statement = statement.where(
                orm.LifecycleEventModel.sleeve_id == sleeve_id
            )
        statement = statement.order_by(
            orm.LifecycleEventModel.occurred_at.desc(),
            orm.LifecycleEventModel.event_id.desc(),
        ).limit(_validate_limit(limit))
        with self._sessions() as session:
            rows = session.scalars(statement).all()
        return [self._lifecycle_from_model(row) for row in rows]

    def iter_lifecycle_events(
        self,
        *,
        sleeve_id: str | None,
        cause: str | None,
        batch_size: int,
    ) -> Iterator[LifecycleEvent]:
        from sqlalchemy import select

        size = _validate_limit(batch_size)
        statement = select(orm.LifecycleEventModel)
        if sleeve_id is not None:
            statement = statement.where(
                orm.LifecycleEventModel.sleeve_id == sleeve_id
            )
        if cause is not None:
            statement = statement.where(orm.LifecycleEventModel.cause == cause)
        statement = statement.order_by(
            orm.LifecycleEventModel.occurred_at.desc(),
            orm.LifecycleEventModel.event_id.desc(),
        ).execution_options(yield_per=size)

        def stream() -> Iterator[LifecycleEvent]:
            with self._sessions() as session:
                rows = session.scalars(statement)
                for row in rows:
                    yield self._lifecycle_from_model(row)

        return stream()

    def latest_lifecycle_state(self, sleeve_id: str) -> LifecycleState | None:
        from sqlalchemy import select

        with self._sessions() as session:
            state = session.scalar(
                select(orm.LifecycleEventModel.to_state)
                .where(orm.LifecycleEventModel.sleeve_id == sleeve_id)
                .order_by(
                    orm.LifecycleEventModel.occurred_at.desc(),
                    orm.LifecycleEventModel.event_id.desc(),
                )
                .limit(1)
            )
        return None if state is None else LifecycleState(state)

    def save_recovery_case(self, case: RecoveryCase) -> RecoveryCase:
        with self._sessions.begin() as session:
            model = session.get(orm.RecoveryCaseModel, case.recovery_case_id)
            if model is None:
                model = orm.RecoveryCaseModel(recovery_case_id=case.recovery_case_id)
                session.add(model)
            else:
                existing = RecoveryCase.model_validate(model.case_json)
                immutable_fields = (
                    "sleeve_id",
                    "triggered_at",
                    "drift_event_due_at",
                    "diagnosis_due_at",
                    "earliest_recovery_review_at",
                    "data_integrity_failure",
                    "trigger_evidence",
                )
                if any(getattr(existing, name) != getattr(case, name) for name in immutable_fields):
                    raise CatalogConflict(
                        f"recovery case identity collision for {case.recovery_case_id!r}"
                    )
            model.sleeve_id = case.sleeve_id
            model.status = case.status.value
            model.lifecycle_state = case.lifecycle_state.value
            model.case_json = _json_tree(case)
            model.triggered_at = case.triggered_at
            model.updated_at = _utc_now()
        return case

    def get_recovery_case(self, recovery_case_id: str) -> RecoveryCase | None:
        with self._sessions() as session:
            model = session.get(orm.RecoveryCaseModel, recovery_case_id)
            return None if model is None else RecoveryCase.model_validate(model.case_json)

    def list_recovery_cases(
        self,
        *,
        limit: int,
        status: RecoveryCaseStatus | None,
        sleeve_id: str | None,
    ) -> list[RecoveryCase]:
        from sqlalchemy import select

        statement = select(orm.RecoveryCaseModel)
        if status is not None:
            statement = statement.where(orm.RecoveryCaseModel.status == status.value)
        if sleeve_id is not None:
            statement = statement.where(orm.RecoveryCaseModel.sleeve_id == sleeve_id)
        statement = statement.order_by(
            orm.RecoveryCaseModel.triggered_at.desc(),
            orm.RecoveryCaseModel.recovery_case_id,
        ).limit(_validate_limit(limit))
        with self._sessions() as session:
            rows = session.scalars(statement).all()
        return [RecoveryCase.model_validate(row.case_json) for row in rows]

    def iter_recovery_cases(
        self,
        *,
        statuses: Sequence[RecoveryCaseStatus] | None,
        sleeve_id: str | None,
        batch_size: int,
    ) -> Iterator[RecoveryCase]:
        from sqlalchemy import select

        size = _validate_limit(batch_size)
        statement = select(orm.RecoveryCaseModel)
        if statuses is not None:
            statement = statement.where(
                orm.RecoveryCaseModel.status.in_(
                    tuple(status.value for status in statuses)
                )
            )
        if sleeve_id is not None:
            statement = statement.where(
                orm.RecoveryCaseModel.sleeve_id == sleeve_id
            )
        statement = statement.order_by(
            orm.RecoveryCaseModel.triggered_at.desc(),
            orm.RecoveryCaseModel.recovery_case_id,
        ).execution_options(yield_per=size)

        def stream() -> Iterator[RecoveryCase]:
            with self._sessions() as session:
                rows = session.scalars(statement)
                for row in rows:
                    yield RecoveryCase.model_validate(row.case_json)

        return stream()

    def save_run(self, run: RunRecord) -> RunRecord:
        from sqlalchemy import select
        from sqlalchemy.exc import IntegrityError

        try:
            with self._sessions.begin() as session:
                # PostgreSQL serializes competing terminal outcomes on this
                # durable row.  The SQLAlchemy SQLite test backend ignores
                # FOR UPDATE but exercises the same state semantics.
                model = session.scalar(
                    select(orm.RunModel)
                    .where(orm.RunModel.run_id == run.run_id)
                    .with_for_update()
                )
                if model is None:
                    session.add(
                        orm.RunModel(
                            run_id=run.run_id,
                            run_type=run.run_type,
                            status=run.status,
                            input_fingerprint=run.input_fingerprint,
                            metadata_json=json.loads(canonical_json(run.metadata)),
                            error=run.error,
                            started_at=run.started_at,
                            completed_at=run.completed_at,
                        )
                    )
                    return run

                persisted = self._run_from_model(model)
                _assert_run_identity(persisted, run)
                if _run_is_terminal(persisted):
                    _assert_terminal_run_replay(persisted, run)
                    return persisted
                model.status = run.status
                model.metadata_json = json.loads(canonical_json(run.metadata))
                model.error = run.error
                model.completed_at = run.completed_at
            return run
        except IntegrityError as exc:
            # Two transactions can both observe an absent primary key.  Once
            # the winner commits, retry against that row so a running winner
            # may still be atomically terminalized and a terminal winner can
            # only receive an exact idempotent replay.
            existing = self.get_run(run.run_id)
            if existing is None:
                raise
            try:
                _assert_run_identity(existing, run)
                if _run_is_terminal(existing):
                    _assert_terminal_run_replay(existing, run)
                    return existing
                return self.save_run(run)
            except CatalogConflict as conflict:
                raise conflict from exc

    def claim_run(self, run: RunRecord) -> tuple[RunRecord, bool]:
        from sqlalchemy.exc import IntegrityError

        try:
            with self._sessions.begin() as session:
                session.add(
                    orm.RunModel(
                        run_id=run.run_id,
                        run_type=run.run_type,
                        status=run.status,
                        input_fingerprint=run.input_fingerprint,
                        metadata_json=json.loads(canonical_json(run.metadata)),
                        error=run.error,
                        started_at=run.started_at,
                        completed_at=run.completed_at,
                    )
                )
            return run, True
        except IntegrityError as exc:
            existing = self.get_run(run.run_id)
            if existing is None:
                raise
            if (
                existing.run_type != run.run_type
                or existing.input_fingerprint != run.input_fingerprint
            ):
                raise CatalogConflict(f"run identity collision for {run.run_id!r}") from exc
            if _run_is_terminal(existing) and run.status != "running":
                try:
                    _assert_terminal_run_replay(existing, run)
                except CatalogConflict as conflict:
                    raise conflict from exc
            return existing, False

    def get_run(self, run_id: str) -> RunRecord | None:
        with self._sessions() as session:
            model = session.get(orm.RunModel, run_id)
            return None if model is None else self._run_from_model(model)

    @staticmethod
    def _run_from_model(model: Any) -> RunRecord:
        return RunRecord(
            run_id=model.run_id,
            run_type=model.run_type,
            status=model.status,
            input_fingerprint=model.input_fingerprint,
            metadata=dict(model.metadata_json),
            error=model.error,
            started_at=_parse_time(model.started_at),
            completed_at=_parse_time(model.completed_at) if model.completed_at else None,
        )

    def list_runs(
        self, *, limit: int, status: str | None, run_type: str | None
    ) -> list[RunRecord]:
        from sqlalchemy import select

        statement = select(orm.RunModel)
        if status is not None:
            statement = statement.where(orm.RunModel.status == status)
        if run_type is not None:
            statement = statement.where(orm.RunModel.run_type == run_type)
        statement = statement.order_by(
            orm.RunModel.started_at.desc(), orm.RunModel.run_id
        ).limit(_validate_limit(limit))
        with self._sessions() as session:
            rows = session.scalars(statement).all()
        return [self._run_from_model(row) for row in rows]

    @staticmethod
    def _legacy_from_model(model: Any) -> LegacyEvidenceRecord:
        return LegacyEvidenceRecord(
            evidence_id=model.evidence_id,
            source_uri=model.source_uri,
            content_hash=model.content_hash,
            trust_label=model.trust_label,
            reasons=tuple(model.reasons_json),
            imported_at=_parse_time(model.imported_at),
        )

    def import_legacy_evidence(self, evidence: LegacyEvidenceRecord) -> LegacyEvidenceRecord:
        from sqlalchemy import select
        from sqlalchemy.exc import IntegrityError

        try:
            with self._sessions.begin() as session:
                session.add(
                    orm.LegacyEvidenceModel(
                        evidence_id=evidence.evidence_id,
                        source_uri=evidence.source_uri,
                        content_hash=evidence.content_hash,
                        trust_label=evidence.trust_label,
                        reasons_json=list(evidence.reasons),
                        imported_at=evidence.imported_at,
                    )
                )
        except IntegrityError as exc:
            with self._sessions() as session:
                model = session.scalar(
                    select(orm.LegacyEvidenceModel).where(
                        orm.LegacyEvidenceModel.source_uri == evidence.source_uri,
                        orm.LegacyEvidenceModel.content_hash == evidence.content_hash,
                    )
                )
            if model is not None:
                existing = self._legacy_from_model(model)
                if (
                    existing.source_uri == evidence.source_uri
                    and existing.content_hash == evidence.content_hash
                    and existing.trust_label == evidence.trust_label
                    and existing.reasons == evidence.reasons
                ):
                    return existing
            raise CatalogConflict(
                "legacy evidence was reimported with conflicting trust metadata"
            ) from exc
        return evidence

    def list_legacy_evidence(
        self, *, limit: int, trust_label: str | None
    ) -> list[LegacyEvidenceRecord]:
        from sqlalchemy import select

        statement = select(orm.LegacyEvidenceModel)
        if trust_label is not None:
            statement = statement.where(
                orm.LegacyEvidenceModel.trust_label == trust_label
            )
        statement = statement.order_by(
            orm.LegacyEvidenceModel.imported_at.desc(),
            orm.LegacyEvidenceModel.evidence_id,
        ).limit(_validate_limit(limit))
        with self._sessions() as session:
            rows = session.scalars(statement).all()
        return [self._legacy_from_model(row) for row in rows]

    @staticmethod
    def _shadow_account_from_model(model: Any) -> ShadowAccountRecord:
        return ShadowAccountRecord(
            account_id=model.account_id,
            name=model.name,
            currency=model.currency,
            initial_capital=model.initial_capital,
            cash=model.cash,
            nav=model.nav,
            benchmark_nav=model.benchmark_nav,
            status=model.status,
            as_of=_parse_time(model.as_of),
            last_event_sequence=model.last_event_sequence,
            last_event_hash=model.last_event_hash,
        )

    @staticmethod
    def _shadow_event_from_model(model: Any) -> ShadowEvent:
        return ShadowEvent(
            event_id=model.event_id,
            account_id=model.account_id,
            sequence_number=model.sequence_number,
            event_type=model.event_type,
            occurred_at=_parse_time(model.occurred_at),
            payload=dict(model.payload_json),
            previous_event_hash=model.previous_event_hash,
            event_hash=model.event_hash,
        )

    def create_shadow_account(
        self,
        *,
        account_id: str,
        name: str,
        initial_capital: float,
        opened_at: datetime,
        currency: str,
    ) -> ShadowAccountRecord:
        from sqlalchemy import select
        from sqlalchemy.exc import IntegrityError

        opened_at = _require_aware(opened_at, "opened_at")
        if not account_id or not name or not currency:
            raise ValueError("account_id, name, and currency are required")
        if initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        payload = {
            "name": name,
            "currency": currency,
            "initial_capital": float(initial_capital),
            "account_state": {
                "cash": float(initial_capital),
                "nav": float(initial_capital),
                "benchmark_nav": float(initial_capital),
            },
        }
        event_hash = _shadow_event_hash(
            account_id=account_id,
            sequence_number=1,
            event_type="account_opened",
            occurred_at=opened_at,
            payload=payload,
            previous_event_hash=_ZERO_EVENT_HASH,
        )
        try:
            with self._sessions.begin() as session:
                session.add(
                    orm.ShadowAccountModel(
                        account_id=account_id,
                        name=name,
                        currency=currency,
                        initial_capital=float(initial_capital),
                        cash=float(initial_capital),
                        nav=float(initial_capital),
                        benchmark_nav=float(initial_capital),
                        status="active",
                        as_of=opened_at,
                        last_event_sequence=1,
                        last_event_hash=event_hash,
                        updated_at=opened_at,
                    )
                )
                session.add(
                    orm.ShadowEventModel(
                        event_id=f"sev_{event_hash[:32]}",
                        account_id=account_id,
                        sequence_number=1,
                        event_type="account_opened",
                        occurred_at=opened_at,
                        payload_json=payload,
                        previous_event_hash=_ZERO_EVENT_HASH,
                        event_hash=event_hash,
                    )
                )
        except IntegrityError as exc:
            existing = self.get_shadow_account(account_id)
            with self._sessions() as session:
                opening_event = session.scalar(
                    select(orm.ShadowEventModel).where(
                        orm.ShadowEventModel.account_id == account_id,
                        orm.ShadowEventModel.sequence_number == 1,
                    )
                )
            if (
                existing is not None
                and existing.name == name
                and existing.currency == currency
                and existing.initial_capital == float(initial_capital)
                and opening_event is not None
                and _parse_time(opening_event.occurred_at) == opened_at
            ):
                return existing
            raise CatalogConflict(f"shadow account {account_id!r} already exists") from exc
        result = self.get_shadow_account(account_id)
        assert result is not None
        return result

    def get_shadow_account(self, account_id: str) -> ShadowAccountRecord | None:
        with self._sessions() as session:
            model = session.get(orm.ShadowAccountModel, account_id)
            return None if model is None else self._shadow_account_from_model(model)

    def list_shadow_accounts(
        self, *, limit: int, status: str | None
    ) -> list[ShadowAccountRecord]:
        from sqlalchemy import select

        statement = select(orm.ShadowAccountModel)
        if status is not None:
            statement = statement.where(orm.ShadowAccountModel.status == status)
        statement = statement.order_by(
            orm.ShadowAccountModel.as_of.desc(), orm.ShadowAccountModel.account_id
        ).limit(_validate_limit(limit))
        with self._sessions() as session:
            rows = session.scalars(statement).all()
        return [self._shadow_account_from_model(row) for row in rows]

    def iter_shadow_accounts(
        self, *, status: str | None, batch_size: int
    ) -> Iterator[ShadowAccountRecord]:
        from sqlalchemy import select

        size = _validate_limit(batch_size)
        statement = select(orm.ShadowAccountModel)
        if status is not None:
            statement = statement.where(orm.ShadowAccountModel.status == status)
        statement = statement.order_by(
            orm.ShadowAccountModel.as_of.desc(), orm.ShadowAccountModel.account_id
        ).execution_options(yield_per=size)

        def stream() -> Iterator[ShadowAccountRecord]:
            with self._sessions() as session:
                rows = session.scalars(statement)
                for row in rows:
                    yield self._shadow_account_from_model(row)

        return stream()

    def append_shadow_event(
        self,
        *,
        account_id: str,
        event_type: str,
        occurred_at: datetime,
        payload: Mapping[str, Any],
        expected_previous_hash: str | None,
    ) -> ShadowEvent:
        return self.append_shadow_events_atomic(
            account_id=account_id,
            events=(
                ShadowEventInput(
                    event_type=event_type,
                    occurred_at=occurred_at,
                    payload=payload,
                ),
            ),
            expected_previous_hash=expected_previous_hash,
        )[0]

    def append_shadow_events_atomic(
        self,
        *,
        account_id: str,
        events: Sequence[ShadowEventInput | Mapping[str, Any]],
        expected_previous_hash: str | None,
    ) -> list[ShadowEvent]:
        from sqlalchemy import select

        normalized = _normalize_shadow_event_inputs(events)
        committed: list[ShadowEvent] = []
        with self._sessions.begin() as session:
            account = session.scalar(
                select(orm.ShadowAccountModel)
                .where(orm.ShadowAccountModel.account_id == account_id)
                .with_for_update()
            )
            if account is None:
                raise CatalogNotFound(f"shadow account {account_id!r} was not found")
            previous_hash = account.last_event_hash
            if expected_previous_hash is not None and expected_previous_hash != previous_hash:
                raise CatalogConflict("shadow event optimistic-lock hash does not match")
            sequence = account.last_event_sequence
            last_time = _parse_time(account.as_of)
            for event_type, occurred_at, payload_dict, account_state, position_state in normalized:
                if occurred_at < last_time:
                    raise CatalogConflict(
                        "events in an atomic shadow step must be chronological"
                    )
                sequence += 1
                event_hash = _shadow_event_hash(
                    account_id=account_id,
                    sequence_number=sequence,
                    event_type=event_type,
                    occurred_at=occurred_at,
                    payload=payload_dict,
                    previous_event_hash=previous_hash,
                )
                event_id = f"sev_{event_hash[:32]}"
                session.add(
                    orm.ShadowEventModel(
                        event_id=event_id,
                        account_id=account_id,
                        sequence_number=sequence,
                        event_type=event_type,
                        occurred_at=occurred_at,
                        payload_json=payload_dict,
                        previous_event_hash=previous_hash,
                        event_hash=event_hash,
                    )
                )
                if position_state is not None:
                    position = session.get(
                        orm.ShadowPositionModel,
                        (account_id, position_state["ticker"]),
                    )
                    if position_state["quantity"] <= 1e-12:
                        if position is not None:
                            session.delete(position)
                    else:
                        if position is None:
                            position = orm.ShadowPositionModel(
                                account_id=account_id,
                                ticker=position_state["ticker"],
                            )
                            session.add(position)
                        position.quantity = position_state["quantity"]
                        position.average_cost = position_state["average_cost"]
                        position.market_price = position_state["market_price"]
                        position.market_value = position_state["market_value"]
                        position.updated_at = occurred_at
                        position.last_event_sequence = sequence
                    # ``autoflush`` is disabled for deterministic repository
                    # boundaries.  Flush here so a later event in the same
                    # atomic step can update the same position identity.
                    session.flush()
                account.cash = account_state.get("cash", account.cash)
                account.nav = account_state.get("nav", account.nav)
                account.benchmark_nav = account_state.get(
                    "benchmark_nav", account.benchmark_nav
                )
                account.status = str(
                    payload_dict.get("account_status", account.status)
                )
                committed.append(
                    ShadowEvent(
                        event_id=event_id,
                        account_id=account_id,
                        sequence_number=sequence,
                        event_type=event_type,
                        occurred_at=occurred_at,
                        payload=payload_dict,
                        previous_event_hash=previous_hash,
                        event_hash=event_hash,
                    )
                )
                previous_hash = event_hash
                last_time = occurred_at
            account.as_of = last_time
            account.last_event_sequence = sequence
            account.last_event_hash = previous_hash
            account.updated_at = _utc_now()
        return committed

    def list_shadow_events(self, *, account_id: str, limit: int) -> list[ShadowEvent]:
        from sqlalchemy import select

        statement = (
            select(orm.ShadowEventModel)
            .where(orm.ShadowEventModel.account_id == account_id)
            .order_by(orm.ShadowEventModel.sequence_number.desc())
            .limit(_validate_limit(limit))
        )
        with self._sessions() as session:
            rows = session.scalars(statement).all()
        return [self._shadow_event_from_model(row) for row in rows]

    def list_shadow_events_by_type(
        self,
        *,
        account_id: str,
        event_type: str,
        since: datetime | None,
        through: datetime | None,
        limit: int,
    ) -> list[ShadowEvent]:
        from sqlalchemy import select

        normalized_type = _validate_event_type(event_type)
        statement = select(orm.ShadowEventModel).where(
            orm.ShadowEventModel.account_id == account_id,
            orm.ShadowEventModel.event_type == normalized_type,
        )
        if since is not None:
            statement = statement.where(
                orm.ShadowEventModel.occurred_at >= _require_aware(since, "since")
            )
        if through is not None:
            statement = statement.where(
                orm.ShadowEventModel.occurred_at <= _require_aware(through, "through")
            )
        statement = statement.order_by(
            orm.ShadowEventModel.sequence_number.desc()
        ).limit(_validate_limit(limit))
        with self._sessions() as session:
            rows = session.scalars(statement).all()
        return [self._shadow_event_from_model(row) for row in rows]

    def iter_shadow_events_by_type(
        self,
        *,
        account_id: str,
        event_type: str,
        since: datetime | None,
        through: datetime | None,
        batch_size: int,
    ) -> Iterator[ShadowEvent]:
        from sqlalchemy import select

        normalized_type = _validate_event_type(event_type)
        size = _validate_limit(batch_size)
        statement = select(orm.ShadowEventModel).where(
            orm.ShadowEventModel.account_id == account_id,
            orm.ShadowEventModel.event_type == normalized_type,
        )
        if since is not None:
            statement = statement.where(
                orm.ShadowEventModel.occurred_at >= _require_aware(since, "since")
            )
        if through is not None:
            statement = statement.where(
                orm.ShadowEventModel.occurred_at <= _require_aware(through, "through")
            )
        statement = statement.order_by(
            orm.ShadowEventModel.sequence_number.desc()
        ).execution_options(yield_per=size)

        def stream() -> Iterator[ShadowEvent]:
            with self._sessions() as session:
                rows = session.scalars(statement)
                for row in rows:
                    yield self._shadow_event_from_model(row)

        return stream()

    def count_shadow_sessions(
        self, *, account_id: str, since: date, through: date
    ) -> int:
        from sqlalchemy import distinct, func, select

        if through < since:
            return 0
        lower = datetime.combine(since, time.max, tzinfo=timezone.utc)
        upper = datetime.combine(through, time.max, tzinfo=timezone.utc)
        statement = select(
            func.count(distinct(func.date(orm.ShadowEventModel.occurred_at)))
        ).where(
            orm.ShadowEventModel.account_id == account_id,
            orm.ShadowEventModel.event_type == "account_projected",
            orm.ShadowEventModel.occurred_at > lower,
            orm.ShadowEventModel.occurred_at <= upper,
        )
        with self._sessions() as session:
            value = session.scalar(statement)
        return int(value or 0)

    def list_shadow_positions(self, account_id: str) -> list[ShadowPositionRecord]:
        from sqlalchemy import select

        statement = (
            select(orm.ShadowPositionModel)
            .where(orm.ShadowPositionModel.account_id == account_id)
            .order_by(orm.ShadowPositionModel.ticker)
        )
        with self._sessions() as session:
            rows = session.scalars(statement).all()
        return [
            ShadowPositionRecord(
                account_id=row.account_id,
                ticker=row.ticker,
                quantity=row.quantity,
                average_cost=row.average_cost,
                market_price=row.market_price,
                market_value=row.market_value,
                updated_at=_parse_time(row.updated_at),
                last_event_sequence=row.last_event_sequence,
            )
            for row in rows
        ]

    def verify_shadow_chain(self, account_id: str) -> bool:
        from sqlalchemy import select

        with self._sessions() as session:
            account = session.get(orm.ShadowAccountModel, account_id)
            rows = session.scalars(
                select(orm.ShadowEventModel)
                .where(orm.ShadowEventModel.account_id == account_id)
                .order_by(orm.ShadowEventModel.sequence_number)
            ).all()
        if account is None or not rows:
            return False
        previous_hash = _ZERO_EVENT_HASH
        for expected_sequence, row in enumerate(rows, start=1):
            event = self._shadow_event_from_model(row)
            if (
                event.sequence_number != expected_sequence
                or event.previous_event_hash != previous_hash
                or event.event_hash
                != _shadow_event_hash(
                    account_id=event.account_id,
                    sequence_number=event.sequence_number,
                    event_type=event.event_type,
                    occurred_at=event.occurred_at,
                    payload=event.payload,
                    previous_event_hash=event.previous_event_hash,
                )
            ):
                return False
            previous_hash = event.event_hash
        return (
            account.last_event_sequence == len(rows)
            and account.last_event_hash == previous_hash
        )

    def catalog_summary(self) -> CatalogSummary:
        from sqlalchemy import func, select

        models = {
            "snapshots": orm.DataSnapshotModel,
            "experiments": orm.ExperimentModel,
            "results": orm.ExperimentResultModel,
            "trials": orm.TrialLedgerModel,
            "lifecycle_events": orm.LifecycleEventModel,
            "recovery_cases": orm.RecoveryCaseModel,
            "runs": orm.RunModel,
            "legacy_evidence": orm.LegacyEvidenceModel,
            "shadow_accounts": orm.ShadowAccountModel,
            "shadow_events": orm.ShadowEventModel,
        }
        with self._sessions() as session:
            totals = {
                name: int(session.scalar(select(func.count()).select_from(model)) or 0)
                for name, model in models.items()
            }

            def grouped(model: Any, column: Any) -> dict[str, int]:
                return {
                    str(key): int(count)
                    for key, count in session.execute(
                        select(column, func.count()).select_from(model).group_by(column)
                    ).all()
                }

            latest_run = session.scalar(select(func.max(orm.RunModel.started_at)))
            return CatalogSummary(
                generated_at=_utc_now(),
                totals=totals,
                experiment_statuses=grouped(
                    orm.ExperimentModel, orm.ExperimentModel.status
                ),
                data_quality_statuses=grouped(
                    orm.DataSnapshotModel, orm.DataSnapshotModel.quality_status
                ),
                lifecycle_states=grouped(
                    orm.LifecycleEventModel, orm.LifecycleEventModel.to_state
                ),
                recovery_statuses=grouped(
                    orm.RecoveryCaseModel, orm.RecoveryCaseModel.status
                ),
                latest_run_started_at=_parse_time(latest_run) if latest_run else None,
            )


def _sqlite_path(database_url: str | Path) -> str | None:
    if isinstance(database_url, Path):
        return str(database_url)
    value = str(database_url)
    if value in {"sqlite://", "sqlite:///:memory:", ":memory:"}:
        return ":memory:"
    if value.startswith("sqlite:///"):
        return value[len("sqlite:///") :]
    if "://" not in value:
        return value
    return None


class ResearchCatalog:
    """Facade with one API across PostgreSQL and the SQLite test fallback."""

    DEFAULT_ALLOWED_EVALUATOR_VERSIONS = frozenset({"research_os.long_only.v2"})

    def __init__(
        self,
        database_url: str | Path,
        *,
        allowed_evaluator_versions: Sequence[str] | None = None,
        connect_args: Mapping[str, Any] | None = None,
    ) -> None:
        sqlite_path = _sqlite_path(database_url)
        allowed = (
            self.DEFAULT_ALLOWED_EVALUATOR_VERSIONS
            if allowed_evaluator_versions is None
            else frozenset(str(item).strip() for item in allowed_evaluator_versions)
        )
        if not allowed or "" in allowed:
            raise ValueError("allowed_evaluator_versions cannot be empty")
        self._allowed_evaluator_versions = allowed
        self._backend: _CatalogBackend
        if sqlite_path is not None:
            if connect_args:
                raise ValueError("connect_args are only supported by infrastructure catalogs")
            self._backend = _SQLiteCatalog(sqlite_path)
        else:
            self._backend = _SQLAlchemyCatalog(
                str(database_url),
                connect_args=connect_args,
            )

    def __enter__(self) -> "ResearchCatalog":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def initialize_schema(self) -> None:
        self._backend.initialize_schema()

    def close(self) -> None:
        self._backend.close()

    def database_now(self) -> datetime:
        """Return the persistence server's UTC clock, never a request timestamp."""

        return self._backend.database_now()

    def freeze_evidence_epoch(
        self,
        *,
        architecture_version: str,
        code_hash: str,
        configuration_hash: str,
        dependency_lock_hash: str,
        dirty_patch_hash: str,
        frozen_at: datetime | None = None,
    ) -> EvidenceEpochRecord:
        epoch = new_evidence_epoch(
            architecture_version=architecture_version,
            frozen_at=frozen_at or self.database_now(),
            code_hash=code_hash,
            configuration_hash=configuration_hash,
            dependency_lock_hash=dependency_lock_hash,
            dirty_patch_hash=dirty_patch_hash,
        )
        return self._backend.freeze_evidence_epoch(epoch)

    def get_evidence_epoch(self) -> EvidenceEpochRecord | None:
        return self._backend.get_evidence_epoch()

    def get_pending_evidence_epoch(self) -> EvidenceEpochRecord | None:
        """Return the newest unactivated version, if one awaits activation."""

        return self._backend.get_pending_evidence_epoch()

    def list_evidence_epochs(self, *, limit: int = 100) -> list[EvidenceEpochRecord]:
        """List retained immutable versions, newest freeze first."""

        return self._backend.list_evidence_epochs(limit=_validate_limit(limit))

    @staticmethod
    def _trusted_forward_calendar(
        reference: DataSnapshotRef,
        *,
        frozen_at: datetime,
        first_forward_session: date,
    ) -> str:
        """Verify that the requested first day is the next manifest-bound session."""

        if (
            reference.tier not in {SnapshotTier.SILVER, SnapshotTier.GOLD}
            or reference.quality_status is not DataQualityStatus.ACCEPTED
        ):
            raise CatalogConflict(
                "forward evidence requires an accepted immutable calendar snapshot"
            )
        blocking = {
            "st_history_unverified",
            "legacy_untrusted_data",
            "legacy_execution_regression_only",
            "disputed",
            "quarantined",
        }
        if blocking & set(reference.trust_labels):
            raise CatalogConflict("forward evidence calendar snapshot has blocking trust labels")
        manifest = dict(reference.manifest or {})
        try:
            content = {
                "schema_version": manifest["schema_version"],
                "tier": manifest["tier"],
                "as_of": manifest["as_of"],
                "parent_snapshot_ids": list(manifest["parent_snapshot_ids"]),
                "environment_hashes": dict(manifest["environment_hashes"]),
                "quality_status": manifest["quality_status"],
                "trust_labels": list(manifest["trust_labels"]),
                "files": list(manifest["files"]),
                "trading_calendar": dict(manifest["trading_calendar"]),
            }
            manifest_hash = hashlib.sha256(
                json.dumps(
                    content,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            calendar = content["trading_calendar"]
            sessions = tuple(
                date.fromisoformat(str(value)) for value in calendar["sessions"]
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise CatalogConflict(
                "forward evidence requires a valid manifest-bound trading calendar"
            ) from exc
        if not (
            manifest_hash
            == manifest.get("snapshot_id")
            == reference.snapshot_id
            == reference.content_hash
        ):
            raise CatalogConflict("trading calendar snapshot identity is corrupt")
        if (
            str(manifest.get("tier")) != reference.tier.value
            or str(manifest.get("quality_status")) != "pass"
            or str(calendar.get("quality_status") or "").lower() != "accepted"
            or not str(calendar.get("source") or "").strip()
        ):
            raise CatalogConflict("trading calendar is not accepted snapshot evidence")
        if not sessions or sessions != tuple(sorted(set(sessions))):
            raise CatalogConflict("trading calendar sessions are not ordered and unique")
        calendar_hash = hashlib.sha256(
            "\n".join(item.isoformat() for item in sessions).encode("ascii")
        ).hexdigest()
        if str(calendar.get("content_hash") or "") != calendar_hash:
            raise CatalogConflict("trading calendar content hash is corrupt")
        freeze_day = frozen_at.astimezone(ZoneInfo("Asia/Shanghai")).date()
        future = tuple(session for session in sessions if session > freeze_day)
        if not future or first_forward_session != future[0]:
            raise CatalogConflict(
                "first forward session must be the trusted calendar's first session after freeze"
            )
        return calendar_hash

    def activate_evidence_epoch(
        self,
        *,
        calendar_snapshot_id: str,
        first_forward_session: date | str,
        activated_at: datetime | None = None,
    ) -> EvidenceEpochRecord:
        # A new release/calendar revision remains pending until activation.
        # Prefer it here while keeping activation of the current version
        # idempotent when no successor exists.
        epoch = self.get_pending_evidence_epoch() or self.get_evidence_epoch()
        if epoch is None:
            raise CatalogNotFound("the architecture evidence epoch is not frozen")
        session = (
            first_forward_session
            if isinstance(first_forward_session, date)
            else date.fromisoformat(str(first_forward_session))
        )
        snapshot = self.get_snapshot(calendar_snapshot_id)
        if snapshot is None:
            raise CatalogNotFound("the trusted calendar Gold snapshot is not cataloged")
        calendar_hash = self._trusted_forward_calendar(
            snapshot.reference,
            frozen_at=epoch.frozen_at,
            first_forward_session=session,
        )
        database_now = self.database_now()
        effective_activation = activated_at or database_now
        # Production PostgreSQL activation is always stamped by the database
        # clock.  The explicit timestamp remains only for deterministic local
        # catalog tests and imports; it cannot be used to backdate a live
        # forward window.
        backend_engine = getattr(self._backend, "_engine", None)
        if (
            getattr(getattr(backend_engine, "dialect", None), "name", None)
            == "postgresql"
        ):
            if activated_at is not None and abs(
                (_require_aware(activated_at, "activated_at") - database_now).total_seconds()
            ) > 5:
                raise CatalogConflict(
                    "production evidence epoch activation cannot be backdated"
                )
            effective_activation = database_now
        effective_activation = _require_aware(
            effective_activation, "activated_at"
        )
        opening_cutoff = datetime.combine(
            session, _FORMAL_OPEN_TIME, tzinfo=_SHANGHAI
        ).astimezone(timezone.utc)
        if effective_activation < epoch.frozen_at:
            raise CatalogConflict("evidence epoch activation predates its freeze")
        if effective_activation >= opening_cutoff:
            raise CatalogConflict(
                "evidence epoch must be activated before the first forward session opens"
            )
        return self._backend.activate_evidence_epoch(
            epoch_id=epoch.epoch_id,
            expected_epoch_hash=epoch.epoch_hash,
            first_forward_session=session,
            calendar_snapshot_id=snapshot.reference.snapshot_id,
            calendar_snapshot_hash=snapshot.reference.content_hash,
            calendar_content_hash=calendar_hash,
            activated_at=effective_activation,
        )

    def register_snapshot(self, reference: DataSnapshotRef) -> SnapshotRecord:
        return self._backend.register_snapshot(reference)

    def get_snapshot(self, snapshot_id: str) -> SnapshotRecord | None:
        return self._backend.get_snapshot(snapshot_id)

    def list_snapshots(
        self,
        *,
        limit: int = 100,
        quality_status: DataQualityStatus | str | None = None,
        tier: SnapshotTier | str | None = None,
    ) -> list[SnapshotRecord]:
        if quality_status is not None and not isinstance(quality_status, DataQualityStatus):
            quality_status = DataQualityStatus(quality_status)
        if tier is not None and not isinstance(tier, SnapshotTier):
            tier = SnapshotTier(tier)
        return self._backend.list_snapshots(
            limit=limit, quality_status=quality_status, tier=tier
        )

    def list_snapshot_page(
        self,
        *,
        limit: int = 100,
        quality_status: DataQualityStatus | str | None = None,
        tier: SnapshotTier | str | None = None,
        after: SnapshotPageCursor | None = None,
    ) -> SnapshotPage:
        """Return a stable keyset page in newest-first catalog order.

        ``next_cursor`` is present only when the filtered catalog has another
        row.  Callers can therefore distinguish an exact page boundary from a
        truncated result without issuing an unbounded query.
        """

        if quality_status is not None and not isinstance(
            quality_status, DataQualityStatus
        ):
            quality_status = DataQualityStatus(quality_status)
        if tier is not None and not isinstance(tier, SnapshotTier):
            tier = SnapshotTier(tier)
        if after is not None and not isinstance(after, SnapshotPageCursor):
            raise TypeError("after must be a SnapshotPageCursor or None")
        return self._backend.list_snapshot_page(
            limit=limit,
            quality_status=quality_status,
            tier=tier,
            after=after,
        )

    def register_experiment(self, spec: ExperimentSpec) -> ExperimentRecord:
        if spec.evaluator_version not in self._allowed_evaluator_versions:
            raise UnsupportedEvaluator(
                f"evaluator {spec.evaluator_version!r} is not promotion-eligible; "
                "legacy runs belong in legacy evidence"
            )
        if spec.environment.evaluator_build != spec.evaluator_version:
            raise UnsupportedEvaluator(
                "environment evaluator_build must exactly match evaluator_version"
            )
        return self._backend.register_experiment(spec)

    def get_experiment(self, experiment_id: str) -> ExperimentRecord | None:
        return self._backend.get_experiment(experiment_id)

    def get_experiment_by_fingerprint(self, fingerprint: str) -> ExperimentRecord | None:
        return self._backend.get_experiment_by_fingerprint(fingerprint)

    def list_experiments(
        self,
        *,
        limit: int = 100,
        status: ExperimentStatus | str | None = None,
        family: str | None = None,
        candidate_id: str | None = None,
    ) -> list[ExperimentRecord]:
        if status is not None and not isinstance(status, ExperimentStatus):
            status = ExperimentStatus(status)
        return self._backend.list_experiments(
            limit=limit,
            status=status,
            family=family,
            candidate_id=candidate_id,
        )

    def set_experiment_status(
        self, experiment_id: str, status: ExperimentStatus | str
    ) -> ExperimentRecord:
        if not isinstance(status, ExperimentStatus):
            status = ExperimentStatus(status)
        return self._backend.set_experiment_status(experiment_id, status)

    def record_authoritative_result(
        self,
        experiment_id: str,
        *,
        outcome: str,
        metrics: Mapping[str, Any],
        artifact_uri: str | None = None,
        completed_at: datetime | None = None,
    ) -> ExperimentResultRecord:
        return self._backend.record_authoritative_result(
            experiment_id,
            outcome=outcome,
            metrics=metrics,
            artifact_uri=artifact_uri,
            completed_at=completed_at or _utc_now(),
        )

    def get_authoritative_result(
        self, experiment_id: str
    ) -> ExperimentResultRecord | None:
        return self._backend.get_authoritative_result(experiment_id)

    def append_trial(self, entry: TrialLedgerEntry) -> TrialLedgerEntry:
        return self._backend.append_trial(entry)

    def reserve_trial(
        self,
        registration: TrialRegistration,
        *,
        candidate_id: str,
        experiment_id: str | None = None,
        maximum_monthly_confirmatory_trials: int = 3,
        maximum_monthly_confirmatory_trials_per_family: int = 1,
        maximum_diagnostic_branches: int = 2,
    ) -> TrialReservationRecord:
        """Atomically decide and persist a trial reservation or rejection."""

        if not candidate_id.strip():
            raise ValueError("candidate_id must not be empty")
        if registration.requested_evidence_class is EvidenceClass.PRISTINE_FORWARD:
            epoch = self.get_evidence_epoch()
            if (
                epoch is None
                or epoch.forward_holdout_id is None
                or registration.holdout_id != epoch.forward_holdout_id
            ):
                registration = replace(
                    registration,
                    requested_evidence_class=EvidenceClass.PSEUDO_OOS,
                )
        return self._backend.reserve_trial(
            registration,
            candidate_id=candidate_id,
            experiment_id=experiment_id,
            maximum_monthly_confirmatory_trials=maximum_monthly_confirmatory_trials,
            maximum_monthly_confirmatory_trials_per_family=maximum_monthly_confirmatory_trials_per_family,
            maximum_diagnostic_branches=maximum_diagnostic_branches,
        )

    def complete_trial(
        self,
        trial_id: str,
        *,
        outcome: TrialOutcome | str,
        reason: str,
        p_value: float | None = None,
        alpha_spent: float = 0.0,
        metadata: Mapping[str, Any] | None = None,
        experiment_id: str | None = None,
        completed_at: datetime | None = None,
    ) -> TrialLedgerEntry:
        """Complete one admitted reservation exactly once, with idempotent replay."""

        if not isinstance(outcome, TrialOutcome):
            outcome = TrialOutcome(outcome)
        if not reason.strip():
            raise ValueError("reason must not be empty")
        return self._backend.complete_trial(
            trial_id,
            outcome=outcome,
            reason=reason,
            p_value=p_value,
            alpha_spent=alpha_spent,
            metadata=metadata or {},
            experiment_id=experiment_id,
            completed_at=completed_at or _utc_now(),
        )

    def get_trial(self, trial_id: str) -> TrialLedgerEntry | None:
        return self._backend.get_trial(trial_id)

    def list_trials(self, *, family: str | None = None) -> list[TrialLedgerEntry]:
        return self._backend.list_trials(family=family)

    def register_research_family(
        self, family: ResearchFamilyRecord
    ) -> ResearchFamilyRecord:
        return self._backend.register_research_family(family)

    def get_research_family(self, family_id: str) -> ResearchFamilyRecord | None:
        return self._backend.get_research_family(family_id)

    def list_research_families(
        self, *, active_only: bool = True
    ) -> list[ResearchFamilyRecord]:
        return self._backend.list_research_families(active_only=active_only)

    def create_research_submission(
        self, submission: ResearchSubmissionRecord
    ) -> ResearchSubmissionRecord:
        return self._backend.create_research_submission(submission)

    def get_research_submission(
        self, submission_id: str
    ) -> ResearchSubmissionRecord | None:
        return self._backend.get_research_submission(submission_id)

    def list_research_submissions(
        self,
        *,
        limit: int = 100,
        status: str | None = None,
        family_id: str | None = None,
    ) -> list[ResearchSubmissionRecord]:
        _validate_limit(limit)
        if status is not None and status not in SUBMISSION_STATUSES:
            raise ValueError(f"unsupported submission status {status!r}")
        return self._backend.list_research_submissions(
            limit=limit, status=status, family_id=family_id
        )

    def reserve_research_submission(
        self,
        submission_id: str,
        *,
        reserved_at: datetime | None = None,
    ) -> ResearchSubmissionRecord:
        return self._backend.reserve_research_submission(
            submission_id, reserved_at=reserved_at or self.database_now()
        )

    def claim_research_submission(
        self,
        submission_id: str,
        *,
        worker_id: str,
        claimed_at: datetime,
        lease_expires_at: datetime,
    ) -> tuple[ResearchSubmissionRecord, bool]:
        return self._backend.claim_research_submission(
            submission_id,
            worker_id=worker_id,
            claimed_at=claimed_at,
            lease_expires_at=lease_expires_at,
        )

    def renew_research_submission(
        self,
        submission_id: str,
        *,
        worker_id: str,
        lease_token: str,
        renewed_at: datetime,
        lease_expires_at: datetime,
    ) -> ResearchSubmissionRecord:
        return self._backend.renew_research_submission(
            submission_id,
            worker_id=worker_id,
            lease_token=lease_token,
            renewed_at=renewed_at,
            lease_expires_at=lease_expires_at,
        )

    def finish_research_submission(
        self,
        submission_id: str,
        *,
        worker_id: str,
        lease_token: str | None = None,
        status: str,
        experiment_id: str | None = None,
        error: str | None = None,
        finished_at: datetime | None = None,
    ) -> ResearchSubmissionRecord:
        return self._backend.finish_research_submission(
            submission_id,
            worker_id=worker_id,
            lease_token=lease_token,
            status=status,
            experiment_id=experiment_id,
            error=error,
            finished_at=finished_at or self.database_now(),
        )

    def append_lifecycle_event(self, event: LifecycleEvent) -> LifecycleEvent:
        return self._backend.append_lifecycle_event(event)

    def list_lifecycle_events(
        self, *, limit: int = 100, sleeve_id: str | None = None
    ) -> list[LifecycleEvent]:
        return self._backend.list_lifecycle_events(limit=limit, sleeve_id=sleeve_id)

    def iter_lifecycle_events(
        self,
        *,
        sleeve_id: str | None = None,
        cause: str | None = None,
        batch_size: int = 1_000,
    ) -> Iterator[LifecycleEvent]:
        return self._backend.iter_lifecycle_events(
            sleeve_id=sleeve_id,
            cause=cause,
            batch_size=batch_size,
        )

    def latest_lifecycle_state(self, sleeve_id: str) -> LifecycleState | None:
        return self._backend.latest_lifecycle_state(sleeve_id)

    def save_recovery_case(self, case: RecoveryCase) -> RecoveryCase:
        return self._backend.save_recovery_case(case)

    def get_recovery_case(self, recovery_case_id: str) -> RecoveryCase | None:
        return self._backend.get_recovery_case(recovery_case_id)

    def list_recovery_cases(
        self,
        *,
        limit: int = 100,
        status: RecoveryCaseStatus | str | None = None,
        sleeve_id: str | None = None,
    ) -> list[RecoveryCase]:
        if status is not None and not isinstance(status, RecoveryCaseStatus):
            status = RecoveryCaseStatus(status)
        return self._backend.list_recovery_cases(
            limit=limit, status=status, sleeve_id=sleeve_id
        )

    def iter_recovery_cases(
        self,
        *,
        statuses: Sequence[RecoveryCaseStatus | str] | None = None,
        sleeve_id: str | None = None,
        batch_size: int = 1_000,
    ) -> Iterator[RecoveryCase]:
        normalized_statuses: tuple[RecoveryCaseStatus, ...] | None = None
        if statuses is not None:
            normalized_statuses = tuple(
                dict.fromkeys(
                    status
                    if isinstance(status, RecoveryCaseStatus)
                    else RecoveryCaseStatus(status)
                    for status in statuses
                )
            )
            if not normalized_statuses:
                raise ValueError("statuses must contain at least one recovery status")
        return self._backend.iter_recovery_cases(
            statuses=normalized_statuses,
            sleeve_id=sleeve_id,
            batch_size=batch_size,
        )

    def save_run(self, run: RunRecord) -> RunRecord:
        return self._backend.save_run(run)

    def claim_run(self, run: RunRecord) -> tuple[RunRecord, bool]:
        return self._backend.claim_run(run)

    def get_run(self, run_id: str) -> RunRecord | None:
        return self._backend.get_run(run_id)

    def list_runs(
        self,
        *,
        limit: int = 100,
        status: str | None = None,
        run_type: str | None = None,
    ) -> list[RunRecord]:
        return self._backend.list_runs(limit=limit, status=status, run_type=run_type)

    def import_legacy_evidence(self, evidence: LegacyEvidenceRecord) -> LegacyEvidenceRecord:
        return self._backend.import_legacy_evidence(evidence)

    def list_legacy_evidence(
        self, *, limit: int = 100, trust_label: str | None = None
    ) -> list[LegacyEvidenceRecord]:
        return self._backend.list_legacy_evidence(
            limit=limit, trust_label=trust_label
        )

    def create_shadow_account(
        self,
        *,
        account_id: str,
        name: str,
        initial_capital: float,
        opened_at: datetime,
        currency: str = "CNY",
    ) -> ShadowAccountRecord:
        return self._backend.create_shadow_account(
            account_id=account_id,
            name=name,
            initial_capital=initial_capital,
            opened_at=opened_at,
            currency=currency,
        )

    def get_shadow_account(self, account_id: str) -> ShadowAccountRecord | None:
        return self._backend.get_shadow_account(account_id)

    def list_shadow_accounts(
        self, *, limit: int = 100, status: str | None = None
    ) -> list[ShadowAccountRecord]:
        return self._backend.list_shadow_accounts(limit=limit, status=status)

    def iter_shadow_accounts(
        self, *, status: str | None = None, batch_size: int = 1_000
    ) -> Iterator[ShadowAccountRecord]:
        return self._backend.iter_shadow_accounts(
            status=status,
            batch_size=batch_size,
        )

    def append_shadow_event(
        self,
        *,
        account_id: str,
        event_type: str,
        occurred_at: datetime,
        payload: Mapping[str, Any],
        expected_previous_hash: str | None = None,
    ) -> ShadowEvent:
        return self._backend.append_shadow_event(
            account_id=account_id,
            event_type=event_type,
            occurred_at=occurred_at,
            payload=payload,
            expected_previous_hash=expected_previous_hash,
        )

    def append_shadow_events_atomic(
        self,
        *,
        account_id: str,
        events: Sequence[ShadowEventInput | Mapping[str, Any]],
        expected_previous_hash: str | None = None,
    ) -> list[ShadowEvent]:
        return self._backend.append_shadow_events_atomic(
            account_id=account_id,
            events=events,
            expected_previous_hash=expected_previous_hash,
        )

    def list_shadow_events(
        self, *, account_id: str, limit: int = 100
    ) -> list[ShadowEvent]:
        return self._backend.list_shadow_events(account_id=account_id, limit=limit)

    def list_shadow_events_by_type(
        self,
        *,
        account_id: str,
        event_type: str,
        since: datetime | None = None,
        through: datetime | None = None,
        limit: int = 1_000,
    ) -> list[ShadowEvent]:
        return self._backend.list_shadow_events_by_type(
            account_id=account_id,
            event_type=event_type,
            since=since,
            through=through,
            limit=limit,
        )

    def iter_shadow_events_by_type(
        self,
        *,
        account_id: str,
        event_type: str,
        since: datetime | None = None,
        through: datetime | None = None,
        batch_size: int = 1_000,
    ) -> Iterator[ShadowEvent]:
        return self._backend.iter_shadow_events_by_type(
            account_id=account_id,
            event_type=event_type,
            since=since,
            through=through,
            batch_size=batch_size,
        )

    def count_shadow_sessions(
        self, *, account_id: str, since: date, through: date
    ) -> int:
        return self._backend.count_shadow_sessions(
            account_id=account_id, since=since, through=through
        )

    def list_shadow_positions(self, account_id: str) -> list[ShadowPositionRecord]:
        return self._backend.list_shadow_positions(account_id)

    def verify_shadow_chain(self, account_id: str) -> bool:
        return self._backend.verify_shadow_chain(account_id)

    def catalog_summary(self) -> CatalogSummary:
        return self._backend.catalog_summary()


__all__ = [
    "AuthoritativeResultExists",
    "CatalogConflict",
    "CatalogError",
    "CatalogNotFound",
    "CatalogSummary",
    "EVIDENCE_EPOCH_SCHEMA_VERSION",
    "EvidenceEpochRecord",
    "ExperimentRecord",
    "ExperimentResultRecord",
    "LifecycleEvent",
    "LegacyEvidenceRecord",
    "MissingInfrastructureDependency",
    "ResearchCatalog",
    "RunRecord",
    "ShadowAccountRecord",
    "ShadowEvent",
    "ShadowEventInput",
    "ShadowPositionRecord",
    "SnapshotPage",
    "SnapshotPageCursor",
    "SnapshotRecord",
    "TrialLedgerEntry",
    "new_evidence_epoch",
    "UnsupportedEvaluator",
]
