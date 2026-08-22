"""Authoritative, point-in-time market bars for production shadow accounting.

This module is deliberately a discovery service rather than a generic frame
builder.  Public production methods accept a trading session only.  Snapshot
identifiers, paths, data frames and capability labels are discovered from the
PostgreSQL catalog/partition ledger and are then verified against physical
objects.  This prevents a workflow payload (or a convenient local parquet)
from self-declaring itself to be forward evidence.

The opening observation and the closing mark are separate immutable Gold
roles.  A third Silver bundle contains the exact, complete frame consumed by
the shadow engine.  Separating the roles is important: a close observed after
15:00 can never be borrowed as a 09:30 execution observation.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import tempfile
from typing import Any, Callable, Mapping, Protocol, Sequence, runtime_checkable
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from . import orm
from .catalog import ResearchCatalog
from .contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    SnapshotTier,
)
from .data_sources import (
    DatasetContract,
    DiemengSourceAdapter,
    FetchRequest,
    ProbeResult,
    SourceAdapter,
    SourceBatch,
    SourceHealth,
    require_tushare_sdk_https_transport,
)
from .execution_open_sources import (
    diemeng_opening_session_request_template,
    TUSHARE_REALTIME_ENDPOINTS,
    TUSHARE_RT_MIN,
    TUSHARE_RT_MIN_MAX_SYMBOLS_PER_REQUEST,
    TushareRealtimeOpenAdapter,
    normalized_open_contract,
)
from .fingerprint import content_fingerprint
from .object_store import ArchivedObject, S3ImmutableArchive
from .production_config import (
    ProductionConfigurationError,
    load_production_config,
    validate_production_config,
)
from .production_ledger import (
    CapabilityRecord,
    CapabilityStatus,
    PartitionIdentity,
    PartitionRecord,
    PartitionStatus,
    ProductionLedger,
)
from .shadow import ShadowSnapshotBindings, assert_point_in_time_columns


SCHEMA_VERSION = "research-os/execution-snapshot/v1"
OPEN_SOURCE_DATASET = "minute_history"
OPEN_DATASET = "execution_open_0930"
OUTPUT_DATASET = "typed_execution_snapshot"
CAPABILITY_DATASET = "shadow_execution_snapshot"
BUNDLE_ROLE = "shadow_typed_bars"
FORMAL_EXECUTION_CAPABILITY_SCHEMA_VERSION = (
    "research-os/formal-execution-capability/v1"
)
FORMAL_EXECUTION_SOURCE_ID = "research_os"

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_HASH = re.compile(r"^[0-9a-f]{64}$")
_BLOCKING_GOLD_LABELS = frozenset(
    {
        "st_history_unverified",
        "historical_st_empty",
        "hash_mismatch",
        "source_disputed",
        "data_quarantined",
        "legacy_untrusted_data",
    }
)
_PRODUCTION_GOLD_LABELS = frozenset(
    {
        "point_in_time",
        "quality_accepted",
        "historical_st_verified",
    }
)
_ROTATION_SOURCE_ID = "security"
_ROTATION_DETAIL = "vendor_confirmed_rotation"
_ROTATION_BASELINE_SCHEMA = "research-os/credential-rotation-baseline/v1"
_ROTATION_CONFIRMATION_SCHEMA = "research-os/vendor-rotation-confirmation/v1"
_ROTATION_SPECS = {
    "diemeng_api_key": ("diemeng", "diemeng_api_key_rotation"),
    "tushare_token": ("tushare", "tushare_token_rotation"),
}


def _add_cleanup_note(error: BaseException, message: str) -> None:
    """Annotate cleanup failures without replacing the causal exception."""

    add_note = getattr(error, "add_note", None)
    if callable(add_note):  # BaseException.add_note is available on Python 3.11+.
        add_note(message)


def _rotation_contract_hash(credential: str) -> str:
    vendor, _ = _ROTATION_SPECS[credential]
    baseline_file = f"{credential}.rotation-baseline.json"
    confirmation_file = f"{credential}.vendor-confirmation.json"
    return content_fingerprint(
        {
            "schema_version": SCHEMA_VERSION,
            "credential": credential,
            "vendor": vendor,
            "authority": "fixed_secret_root_local_evidence_migration",
            "baseline_schema": _ROTATION_BASELINE_SCHEMA,
            "confirmation_schema": _ROTATION_CONFIRMATION_SCHEMA,
            "baseline_file": baseline_file,
            "confirmation_file": confirmation_file,
            "raw_secret_forbidden": True,
            "caller_supplied_reference_forbidden": True,
        },
        domain="factor-lab/research-os/v1/credential-rotation-contract",
    )


_ROTATION_DATASET = _ROTATION_SPECS["diemeng_api_key"][1]
_ROTATION_BASELINE_FILE = "diemeng_api_key.rotation-baseline.json"
_ROTATION_CONFIRMATION_FILE = "diemeng_api_key.vendor-confirmation.json"
_ROTATION_CONTRACT_HASH = _rotation_contract_hash("diemeng_api_key")
_LEGACY_ROTATION_CONTRACT_HASH = content_fingerprint(
    {
        "schema_version": SCHEMA_VERSION,
        "credential": "diemeng_api_key",
        "authority": "fixed_secret_root_local_evidence_migration",
        "baseline_schema": _ROTATION_BASELINE_SCHEMA,
        "confirmation_schema": _ROTATION_CONFIRMATION_SCHEMA,
        "baseline_file": _ROTATION_BASELINE_FILE,
        "confirmation_file": _ROTATION_CONFIRMATION_FILE,
        "raw_secret_forbidden": True,
        "caller_supplied_reference_forbidden": True,
    },
    domain="factor-lab/research-os/v1/credential-rotation-contract",
)
_ROTATION_CONTRACT_HASH = _LEGACY_ROTATION_CONTRACT_HASH


def _credential_rotation_contract_hash(credential: str) -> str:
    return (
        _ROTATION_CONTRACT_HASH
        if credential == "diemeng_api_key"
        else _rotation_contract_hash(credential)
    )
_OUTPUT_CONTRACT_HASH = content_fingerprint(
    {
        "schema_version": SCHEMA_VERSION,
        "execution_time": "09:30:00 Asia/Shanghai",
        "execution_observation_deadline": "09:35:00 Asia/Shanghai",
        "execution_availability": "collector_received_at",
        "mark_time": "15:00:00 Asia/Shanghai",
        "roles": ["execution", "mark", BUNDLE_ROLE],
        "source_authority": "postgresql_catalog_minio",
    },
    domain="factor-lab/research-os/v1/execution-snapshot-contract",
)
OUTPUT_CONTRACT_HASH = _OUTPUT_CONTRACT_HASH
FORMAL_EXECUTION_REQUIRED_FIELDS = (
    "adv_20",
    "benchmark_return",
    "cash_dividend",
    "close_adj",
    "execution_available_at",
    "execution_event_time",
    "execution_snapshot_id",
    "is_delisted",
    "is_one_price_limit_down",
    "is_one_price_limit_up",
    "is_suspended",
    "mark_available_at",
    "mark_event_time",
    "mark_snapshot_id",
    "open_adj",
    "split_ratio",
    "ticker",
    "trade_date",
    "volatility_20",
)


def formal_execution_capability_probe_hash(
    *,
    source_id: str,
    dataset: str,
    contract_hash: str,
    fields: Sequence[str],
    detail: Mapping[str, Any],
) -> str:
    """Hash the exact typed capability row consumed by readiness audit."""

    return content_fingerprint(
        {
            "source_id": str(source_id),
            "dataset": str(dataset),
            "contract_hash": str(contract_hash),
            "fields": sorted(set(map(str, fields))),
            "detail": dict(detail),
        },
        domain=FORMAL_EXECUTION_CAPABILITY_SCHEMA_VERSION,
    )


class ExecutionSnapshotAuthorityError(RuntimeError):
    """Base class for a fail-closed execution snapshot operation."""


class ExecutionEvidenceUnavailable(ExecutionSnapshotAuthorityError):
    """Required accepted PostgreSQL/catalog/physical evidence is absent."""


class ExecutionEvidenceConflict(ExecutionSnapshotAuthorityError):
    """Two authoritative inputs disagree or violate the temporal contract."""


class ExecutionNetworkBlocked(ExecutionSnapshotAuthorityError):
    """A production provider request was denied before any network call."""


class ExecutionLeaseBusy(ExecutionSnapshotAuthorityError):
    """Another worker owns the non-expired partition lease."""


class ExecutionCapabilityDecision(str, Enum):
    ACCEPTED = "accepted"
    NON_FORWARD = "non_forward"
    REJECTED = "rejected"


@dataclass(frozen=True)
class ExecutionSnapshotPolicy:
    """Versioned completeness policy for one A-share session."""

    target_universe_size: int = 500
    minimum_coverage: float = 0.95
    execution_local_time: time = time(9, 30)
    observation_deadline_minutes: int = 5
    mark_local_time: time = time(15, 0)
    lease_minutes: int = 30
    maximum_attempts: int = 5

    def __post_init__(self) -> None:
        if self.target_universe_size <= 0:
            raise ValueError("target_universe_size must be positive")
        if not 0.0 < self.minimum_coverage <= 1.0:
            raise ValueError("minimum_coverage must be in (0, 1]")
        if (
            self.observation_deadline_minutes <= 0
            or self.lease_minutes <= 0
            or self.maximum_attempts <= 0
        ):
            raise ValueError("lease/attempt limits must be positive")

    @property
    def is_formal_default(self) -> bool:
        return self == ExecutionSnapshotPolicy()

    def fingerprint_payload(self) -> dict[str, Any]:
        return {
            "target_universe_size": self.target_universe_size,
            "minimum_coverage": self.minimum_coverage,
            "execution_local_time": self.execution_local_time.isoformat(),
            "observation_deadline_minutes": self.observation_deadline_minutes,
            "mark_local_time": self.mark_local_time.isoformat(),
            "lease_minutes": self.lease_minutes,
            "maximum_attempts": self.maximum_attempts,
        }


@dataclass(frozen=True)
class CredentialRotationAttestation:
    credential: str
    evidence_hash: str
    confirmed_at: datetime


@dataclass(frozen=True)
class ExecutionCapabilityAssessment:
    decision: ExecutionCapabilityDecision
    reasons: tuple[str, ...]
    evidence_hash: str

    @property
    def accepted(self) -> bool:
        return self.decision is ExecutionCapabilityDecision.ACCEPTED


@dataclass(frozen=True)
class TypedExecutionSession:
    trade_date: date
    bars: pd.DataFrame
    benchmark_return: float
    execution_snapshot: DataSnapshotRef
    mark_snapshot: DataSnapshotRef
    bundle_snapshot: DataSnapshotRef
    capability: ExecutionCapabilityAssessment
    reused: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "bars", self.bars.copy(deep=True))
        if self.execution_snapshot.snapshot_id == self.mark_snapshot.snapshot_id:
            raise ValueError("execution and mark snapshot roles must be distinct")
        if not math.isfinite(float(self.benchmark_return)):
            raise ValueError("benchmark_return must be finite")

    @property
    def snapshot_bindings(self) -> ShadowSnapshotBindings:
        return ShadowSnapshotBindings(
            decision_snapshot_id=None,
            execution_snapshot_id=self.execution_snapshot.snapshot_id,
            mark_snapshot_id=self.mark_snapshot.snapshot_id,
        )


@runtime_checkable
class RegisteredGoldSnapshotReader(Protocol):
    """Read one registered immutable Gold reference from its physical store."""

    @property
    def production_attested(self) -> bool: ...

    def read(self, reference: DataSnapshotRef) -> pd.DataFrame: ...


def _snapshot_summary(snapshot: Any) -> Mapping[str, Any]:
    summary = getattr(snapshot, "summary", None)
    if isinstance(summary, Mapping):
        return summary
    additional = getattr(summary, "additional_properties", None)
    return additional if isinstance(additional, Mapping) else {}


class PyIcebergRegisteredGoldReader:
    """Resolve and scan the exact tag encoded by a registered Iceberg URI.

    Supplying a loader is intentionally an engineering/test seam and makes the
    reader non-attested.  Formal capability therefore requires the ordinary
    PyIceberg loader and its configured MinIO catalog.
    """

    def __init__(
        self,
        *,
        catalog_loader: Callable[[str], Any] | None = None,
        expected_catalog_name: str | None = None,
    ) -> None:
        self._catalog_loader = catalog_loader
        expected = str(expected_catalog_name or "").strip()
        if expected and ("/" in expected or "\\" in expected or ".." in expected):
            raise ValueError("expected_catalog_name is invalid")
        self.expected_catalog_name = expected or None

    @property
    def production_attested(self) -> bool:
        return self._catalog_loader is None and self.expected_catalog_name is not None

    def _load_catalog(self, name: str) -> Any:
        if self._catalog_loader is not None:
            return self._catalog_loader(name)
        try:
            from pyiceberg.catalog import load_catalog
        except ImportError as exc:  # pragma: no cover - production image path
            raise ExecutionEvidenceUnavailable(
                "PyIceberg is required to read registered Gold evidence"
            ) from exc
        return load_catalog(name)

    def read(self, reference: DataSnapshotRef) -> pd.DataFrame:
        if (
            reference.tier is not SnapshotTier.GOLD
            or reference.quality_status is not DataQualityStatus.ACCEPTED
        ):
            raise ExecutionEvidenceUnavailable(
                "physical Gold reader requires an accepted Gold reference"
            )
        if not (
            _HASH.fullmatch(reference.snapshot_id)
            and reference.content_hash == reference.snapshot_id
            and str(reference.manifest.get("snapshot_id") or "")
            == reference.snapshot_id
        ):
            raise ExecutionEvidenceConflict(
                "registered Gold reference and immutable manifest identity differ"
            )
        parsed = urlsplit(reference.uri)
        if (
            parsed.scheme != "iceberg"
            or not parsed.netloc
            or not parsed.path.startswith("/")
            or not parsed.fragment
            or parsed.query
        ):
            raise ExecutionEvidenceUnavailable(
                "registered Gold URI must bind an Iceberg catalog/table/tag"
            )
        if self.expected_catalog_name and parsed.netloc != self.expected_catalog_name:
            raise ExecutionEvidenceConflict(
                "registered Gold URI uses a different production Iceberg catalog"
            )
        identifier = parsed.path[1:]
        if not identifier or "/" in identifier or ".." in identifier:
            raise ExecutionEvidenceUnavailable("registered Iceberg table is invalid")
        table = self._load_catalog(parsed.netloc).load_table(identifier)
        refs = getattr(getattr(table, "metadata", None), "refs", None)
        tagged = refs.get(parsed.fragment) if isinstance(refs, Mapping) else None
        raw_snapshot_id = getattr(tagged, "snapshot_id", None)
        if raw_snapshot_id is None:
            raise ExecutionEvidenceUnavailable("registered Iceberg tag is missing")
        try:
            iceberg_snapshot_id = int(raw_snapshot_id)
        except (TypeError, ValueError) as exc:
            raise ExecutionEvidenceUnavailable("Iceberg tag snapshot id is invalid") from exc
        snapshots = getattr(getattr(table, "metadata", None), "snapshots", ()) or ()
        matched = [
            item
            for item in snapshots
            if int(getattr(item, "snapshot_id", -1)) == iceberg_snapshot_id
        ]
        if len(matched) != 1:
            raise ExecutionEvidenceUnavailable(
                "registered Iceberg tag does not resolve to one physical snapshot"
            )
        if (
            str(_snapshot_summary(matched[0]).get("factor_lab.snapshot_key") or "")
            != reference.snapshot_id
        ):
            raise ExecutionEvidenceConflict(
                "Iceberg snapshot key differs from the registered catalog snapshot"
            )
        try:
            arrow = table.scan(snapshot_id=iceberg_snapshot_id).to_arrow()
            frame = arrow.to_pandas()
        except Exception as exc:
            raise ExecutionEvidenceUnavailable(
                "registered Iceberg snapshot could not be scanned"
            ) from exc
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            raise ExecutionEvidenceUnavailable("registered Gold snapshot is physically empty")
        return frame.copy(deep=True)


@dataclass(frozen=True)
class _GoldEvidence:
    session: date
    reference: DataSnapshotRef
    frame: pd.DataFrame
    data_quality: PartitionRecord
    gold_partition: PartitionRecord
    calendar: Mapping[str, Any]
    prior_session: date
    production_attested: bool


@dataclass(frozen=True)
class _OpenEvidence:
    reference: DataSnapshotRef
    frame: pd.DataFrame
    partition: PartitionRecord
    physical_source_attested: bool
    rotation_evidence_hash: str | None
    decision_session: date
    decision_snapshot_id: str
    decision_gold_partition_hash: str
    decision_data_quality_partition_hash: str


def _aware(value: datetime, *, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    return value.astimezone(timezone.utc)


def _session(value: date | str) -> date:
    if isinstance(value, datetime):
        raise ValueError("trade_date must not include a time")
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def _event_timestamp(value: Any) -> pd.Timestamp:
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        parsed = parsed.tz_localize(_SHANGHAI)
    else:
        parsed = parsed.tz_convert(_SHANGHAI)
    return parsed


def _utc_timestamp(day: date, local_time: time) -> pd.Timestamp:
    return pd.Timestamp(datetime.combine(day, local_time, tzinfo=_SHANGHAI)).tz_convert(
        timezone.utc
    )


def _timestamp_column(frame: pd.DataFrame, name: str) -> pd.Series:
    if name not in frame:
        raise ExecutionEvidenceUnavailable(f"Gold execution evidence omits {name}")
    values = pd.to_datetime(frame[name], errors="coerce", utc=True)
    return values


def _bool_column(frame: pd.DataFrame, name: str) -> pd.Series:
    if name not in frame:
        raise ExecutionEvidenceUnavailable(f"Gold execution evidence omits {name}")
    values = frame[name].astype("boolean")
    if values.isna().any():
        raise ExecutionEvidenceConflict(f"Gold execution status {name} contains nulls")
    return values.astype(bool)


def _archived(value: Mapping[str, Any]) -> ArchivedObject:
    try:
        archived = ArchivedObject(
            uri=str(value["uri"]),
            key=str(value["key"]),
            sha256=str(value["sha256"]),
            size_bytes=int(value["size_bytes"]),
            reused=bool(value.get("reused", True)),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ExecutionEvidenceConflict("physical object evidence is malformed") from exc
    if not _HASH.fullmatch(archived.sha256):
        raise ExecutionEvidenceConflict("physical object digest is malformed")
    return archived


def _archive_evidence(value: ArchivedObject) -> dict[str, Any]:
    """Immutable object identity, excluding retry-local ``reused`` state."""

    return {
        "uri": value.uri,
        "key": value.key,
        "sha256": value.sha256,
        "size_bytes": value.size_bytes,
    }


@dataclass(frozen=True)
class _ProductionBinding:
    configuration_hash: str
    execution_contract_hash: str
    iceberg_catalog_name: str
    archive_attested: bool
    real_time_open_capable: bool
    source_id: str = "diemeng"
    source_role: str = "diemeng_open_observation"
    credential_name: str = "diemeng_api_key"
    provider_endpoint: str = "/stock/history"


@dataclass(frozen=True)
class _LocalRotationEvidence:
    evidence_hash: str
    confirmed_at: datetime


_CONFIRMATION_REFERENCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$")


def _is_link_or_reparse(path: Path) -> bool:
    try:
        details = os.lstat(path)
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(details.st_mode):
        return True
    reparse = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(getattr(details, "st_file_attributes", 0) & reparse)


def _fixed_secret_evidence_file(root: Path, name: str) -> tuple[Path, bytes]:
    """Read a small fixed-name file without following a redirection."""

    if root.name in {"", ".", ".."} or _is_link_or_reparse(root):
        raise ExecutionEvidenceUnavailable("credential evidence root is not a plain directory")
    try:
        resolved_root = root.resolve(strict=True)
    except OSError as exc:
        raise ExecutionEvidenceUnavailable("credential evidence root is missing") from exc
    if not resolved_root.is_dir() or _is_link_or_reparse(resolved_root):
        raise ExecutionEvidenceUnavailable("credential evidence root is not a plain directory")
    candidate = resolved_root / name
    if _is_link_or_reparse(candidate) or not candidate.is_file():
        raise ExecutionEvidenceUnavailable(
            f"fixed credential evidence file is missing or redirected: {name}"
        )
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(resolved_root)
        raw = resolved.read_bytes()
    except (OSError, ValueError) as exc:
        raise ExecutionEvidenceUnavailable(
            f"fixed credential evidence file is unreadable: {name}"
        ) from exc
    if not raw or len(raw) > 65_536 or b"\0" in raw:
        raise ExecutionEvidenceConflict(
            f"fixed credential evidence file has an unsafe size/content: {name}"
        )
    return resolved, raw


def _fixed_secret_line(root: Path, name: str) -> str:
    _, raw = _fixed_secret_evidence_file(root, name)
    try:
        value = raw.decode("utf-8").rstrip("\r\n")
    except UnicodeDecodeError as exc:
        raise ExecutionEvidenceConflict(f"fixed secret file is not UTF-8: {name}") from exc
    if not value or "\r" in value or "\n" in value:
        raise ExecutionEvidenceConflict(
            f"fixed secret file must contain one non-empty line: {name}"
        )
    return value


def _strict_json_object(raw: bytes, *, label: str) -> Mapping[str, Any]:
    def _pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise ValueError(f"duplicate key {key!r}")
            result[key] = value
        return result

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ExecutionEvidenceConflict(f"{label} must be strict UTF-8 JSON") from exc
    if not isinstance(value, Mapping):
        raise ExecutionEvidenceConflict(f"{label} must be a JSON object")
    return value


def _parse_evidence_time(value: Any, *, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ExecutionEvidenceConflict(f"{label} is not an ISO timestamp") from exc
    try:
        return _aware(parsed, name=label)
    except ValueError as exc:
        raise ExecutionEvidenceConflict(str(exc)) from exc


def _database_now(ledger: ProductionLedger) -> datetime:
    try:
        from sqlalchemy import func, select
    except ImportError as exc:  # pragma: no cover
        raise ExecutionEvidenceUnavailable("SQLAlchemy is required") from exc
    with ledger.engine.connect() as connection:
        value = connection.execute(select(func.now())).scalar_one()
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise ExecutionEvidenceUnavailable("database clock did not return a timestamp")
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class DiemengRotationEvidenceAuthority:
    """One-way migration of selected-provider rotation evidence into PG.

    There is deliberately no method accepting a receipt path, confirmation
    reference, digest, timestamp or raw secret.  The authority reads two fixed
    files below the already validated production secrets mount and hashes the
    current fixed credential file itself.  Consequently an ordinary CLI or
    workflow payload cannot manufacture the accepted capability row.
    """

    def __init__(
        self,
        *,
        ledger: ProductionLedger,
        secrets_root: Path,
        credential_ref: str,
        execution_contract_hash: str,
        _factory_seal: object,
    ) -> None:
        if _factory_seal is not _ROTATION_FACTORY_SEAL:
            raise TypeError("rotation authority can only be created by the production factory")
        credential = str(credential_ref).removeprefix("secret://")
        if credential_ref != f"secret://{credential}" or credential not in _ROTATION_SPECS:
            raise ProductionConfigurationError(
                "rotation authority requires a supported fixed credential reference"
            )
        if not _HASH.fullmatch(execution_contract_hash):
            raise ValueError("execution_contract_hash must be a SHA-256 digest")
        self.ledger = ledger
        self.secrets_root = Path(secrets_root)
        self.credential_ref = credential_ref
        self.credential = credential
        self.vendor, self.dataset = _ROTATION_SPECS[credential]
        self.contract_hash = _credential_rotation_contract_hash(credential)
        self.baseline_file = f"{credential}.rotation-baseline.json"
        self.confirmation_file = f"{credential}.vendor-confirmation.json"
        self.execution_contract_hash = execution_contract_hash

    def _local_evidence(self) -> _LocalRotationEvidence:
        _, baseline_raw = _fixed_secret_evidence_file(
            self.secrets_root, self.baseline_file
        )
        _, confirmation_raw = _fixed_secret_evidence_file(
            self.secrets_root, self.confirmation_file
        )
        baseline = _strict_json_object(
            baseline_raw, label=f"{self.vendor} rotation baseline"
        )
        confirmation = _strict_json_object(
            confirmation_raw, label=f"{self.vendor} vendor confirmation"
        )
        baseline_fields = {
            "schema_version",
            "credential",
            "previous_credential_sha256",
            "captured_at",
        }
        confirmation_fields = {
            "schema_version",
            "vendor",
            "credential",
            "previous_credential_sha256",
            "current_credential_sha256",
            "confirmed_at",
            "confirmation_reference",
        }
        if set(baseline) != baseline_fields or set(confirmation) != confirmation_fields:
            raise ExecutionEvidenceConflict(
                "credential rotation evidence has unexpected or missing fields"
            )
        if (
            baseline.get("schema_version") != _ROTATION_BASELINE_SCHEMA
            or baseline.get("credential") != self.credential
            or confirmation.get("schema_version") != _ROTATION_CONFIRMATION_SCHEMA
            or confirmation.get("vendor") != self.vendor
            or confirmation.get("credential") != self.credential
        ):
            raise ExecutionEvidenceConflict("credential rotation evidence schema is invalid")
        previous_hash = str(baseline.get("previous_credential_sha256") or "").lower()
        confirmation_previous = str(
            confirmation.get("previous_credential_sha256") or ""
        ).lower()
        confirmation_current = str(
            confirmation.get("current_credential_sha256") or ""
        ).lower()
        if not all(
            _HASH.fullmatch(item)
            for item in (previous_hash, confirmation_previous, confirmation_current)
        ):
            raise ExecutionEvidenceConflict("credential rotation digests are malformed")
        if previous_hash != confirmation_previous:
            raise ExecutionEvidenceConflict(
                "vendor confirmation does not bind the captured previous credential"
            )
        current_secret = _fixed_secret_line(self.secrets_root, self.credential)
        lowered_secret = current_secret.casefold()
        if len(current_secret) < 16 or any(
            marker in lowered_secret
            for marker in ("replace-me", "changeme", "placeholder")
        ):
            raise ExecutionEvidenceConflict(
                f"current {self.vendor} credential is a placeholder"
            )
        current_hash = hashlib.sha256(current_secret.encode("utf-8")).hexdigest()
        if current_hash != confirmation_current or current_hash == previous_hash:
            raise ExecutionEvidenceConflict(
                f"current {self.vendor} credential is not the vendor-confirmed replacement"
            )
        reference = str(confirmation.get("confirmation_reference") or "")
        if not _CONFIRMATION_REFERENCE.fullmatch(reference) or re.search(
            r"(?:replace|change|placeholder)", reference, re.IGNORECASE
        ):
            raise ExecutionEvidenceConflict("vendor confirmation reference is invalid")
        captured_at = _parse_evidence_time(
            baseline.get("captured_at"), label="rotation baseline captured_at"
        )
        confirmed_at = _parse_evidence_time(
            confirmation.get("confirmed_at"), label="vendor confirmation confirmed_at"
        )
        database_now = _database_now(self.ledger)
        if not captured_at < confirmed_at <= database_now + timedelta(minutes=5):
            raise ExecutionEvidenceConflict(
                "credential rotation chronology is invalid or from the future"
            )
        evidence_hash = content_fingerprint(
            {
                "schema_version": _ROTATION_CONFIRMATION_SCHEMA,
                "credential": self.credential,
                "vendor": self.vendor,
                "previous_credential_sha256": previous_hash,
                "current_credential_sha256": current_hash,
                "baseline_file_sha256": hashlib.sha256(baseline_raw).hexdigest(),
                "vendor_confirmation_file_sha256": hashlib.sha256(
                    confirmation_raw
                ).hexdigest(),
                "confirmation_reference_sha256": hashlib.sha256(
                    reference.encode("utf-8")
                ).hexdigest(),
                "captured_at": captured_at,
                "confirmed_at": confirmed_at,
                "execution_contract_hash": self.execution_contract_hash,
            },
            domain="factor-lab/research-os/v1/local-credential-rotation-evidence",
        )
        return _LocalRotationEvidence(evidence_hash, confirmed_at)

    def expected_evidence_hash(self) -> str:
        """Recompute the fixed local evidence hash; accepts no caller values."""

        return self._local_evidence().evidence_hash

    def migrate(self) -> CredentialRotationAttestation:
        """Persist the one immutable accepted row from fixed local evidence."""

        local = self._local_evidence()
        probed_at = _database_now(self.ledger)
        try:
            from sqlalchemy import select
        except ImportError as exc:  # pragma: no cover
            raise ExecutionEvidenceUnavailable("SQLAlchemy is required") from exc
        with self.ledger.engine.begin() as connection:
            query = select(orm.SourceCapabilityModel).where(
                orm.SourceCapabilityModel.source_id == _ROTATION_SOURCE_ID,
                orm.SourceCapabilityModel.dataset == self.dataset,
            )
            if self.ledger.engine.dialect.name == "postgresql":
                query = query.with_for_update()
            existing = connection.execute(query).mappings().one_or_none()
            expected_fields = ["credential_ref", "vendor_confirmation_id"]
            if existing is not None:
                if not (
                    str(existing.get("status")) == CapabilityStatus.ACCEPTED.value
                    and str(existing.get("contract_hash")) == self.contract_hash
                    and str(existing.get("probe_hash")) == local.evidence_hash
                    and sorted(map(str, existing.get("fields_json") or ()))
                    == expected_fields
                    and str(existing.get("detail") or "") == _ROTATION_DETAIL
                ):
                    raise ExecutionEvidenceConflict(
                        "persisted credential rotation authority is immutable and differs"
                    )
                persisted_at = existing.get("probed_at")
                if not isinstance(persisted_at, datetime):
                    raise ExecutionEvidenceConflict(
                        "persisted credential rotation timestamp is malformed"
                    )
                if persisted_at.tzinfo is None or persisted_at.utcoffset() is None:
                    persisted_at = persisted_at.replace(tzinfo=timezone.utc)
                return CredentialRotationAttestation(
                    credential=self.credential,
                    evidence_hash=local.evidence_hash,
                    confirmed_at=persisted_at.astimezone(timezone.utc),
                )
            connection.execute(
                orm.SourceCapabilityModel.__table__.insert().values(
                    source_id=_ROTATION_SOURCE_ID,
                    dataset=self.dataset,
                    status=CapabilityStatus.ACCEPTED.value,
                    contract_hash=self.contract_hash,
                    probe_hash=local.evidence_hash,
                    fields_json=expected_fields,
                    detail=_ROTATION_DETAIL,
                    probed_at=probed_at,
                    updated_at=probed_at,
                )
            )
        return CredentialRotationAttestation(
            credential=self.credential,
            evidence_hash=local.evidence_hash,
            confirmed_at=probed_at,
        )


_ROTATION_FACTORY_SEAL = object()


def _fixed_open_contract() -> DatasetContract:
    return normalized_open_contract()


def _validated_execution_mapping(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    daily = payload.get("daily")
    shadow = daily.get("shadow") if isinstance(daily, Mapping) else None
    execution = (
        shadow.get("execution_market_data") if isinstance(shadow, Mapping) else None
    )
    if not isinstance(execution, Mapping):
        raise ProductionConfigurationError(
            "production config omits daily.shadow.execution_market_data"
        )
    source_id = str(execution.get("source") or "").strip().lower()
    if source_id == "diemeng":
        required_identity = {
            "source": "diemeng",
            "profile_name": "primary-diemeng",
            "credential_ref": "secret://diemeng_api_key",
            "dataset": OPEN_SOURCE_DATASET,
            "endpoint": "/stock/history",
            "method": "POST",
            "response_path": "data.list",
        }
    elif source_id == "tushare":
        endpoint = str(execution.get("endpoint") or "").strip()
        if endpoint not in TUSHARE_REALTIME_ENDPOINTS:
            raise ProductionConfigurationError(
                "Tushare execution endpoint must be rt_min or rt_min_daily"
            )
        required_identity = {
            "source": "tushare",
            "profile_name": "primary-tushare",
            "credential_ref": "secret://tushare_token",
            "dataset": endpoint,
            "endpoint": endpoint,
            "method": "SDK",
        }
    else:
        raise ProductionConfigurationError(
            "execution_market_data.source must be diemeng or tushare"
        )
    for field, expected in required_identity.items():
        if execution.get(field) != expected:
            raise ProductionConfigurationError(
                f"execution_market_data.{field} must be {expected!r}"
            )
    contract = execution.get("contract")
    if not isinstance(contract, Mapping):
        raise ProductionConfigurationError("execution_market_data.contract is required")
    if source_id == "diemeng":
        expected_keys = ("stock_code", "trade_time")
        expected_fields = tuple(item.name for item in _fixed_open_contract().fields)
        event_field = "trade_time"
    else:
        expected_keys = ("ts_code", "time")
        expected_fields = (
            ("ts_code", "time", "open", "close", "high", "low", "vol", "amount")
            if execution["endpoint"] == "rt_min"
            else (
                "ts_code",
                "freq",
                "time",
                "open",
                "close",
                "high",
                "low",
                "vol",
                "amount",
            )
        )
        event_field = "time"
    if tuple(map(str, contract.get("key_fields") or ())) != expected_keys:
        raise ProductionConfigurationError(
            f"{source_id} execution keys are not authoritative"
        )
    if tuple(map(str, contract.get("fields") or ())) != expected_fields:
        raise ProductionConfigurationError(
            f"{source_id} execution fields must exactly match the official contract"
        )
    if str(contract.get("event_time_field") or "") != event_field:
        raise ProductionConfigurationError(
            f"{source_id} execution event time must be {event_field}"
        )
    request = execution.get("request")
    expected_request = (
        diemeng_opening_session_request_template()
        if source_id == "diemeng"
        else {
            "ts_code": (
                "${decision_universe_csv}"
                if execution["endpoint"] == "rt_min"
                else "${ticker}"
            ),
            "freq": "1MIN",
        }
    )
    if not isinstance(request, Mapping) or request != expected_request:
        raise ProductionConfigurationError(
            f"{source_id} execution request must be the bounded session template"
        )
    if source_id == "tushare":
        batching = execution.get("batching")
        expected_batching = (
            {
                "mode": "sorted_deterministic_chunks",
                "maximum_symbols_per_request": (
                    TUSHARE_RT_MIN_MAX_SYMBOLS_PER_REQUEST
                ),
            }
            if execution["endpoint"] == TUSHARE_RT_MIN
            else {
                "mode": "one_ticker_per_request",
                "maximum_symbols_per_request": 1,
            }
        )
        if not isinstance(batching, Mapping) or batching != expected_batching:
            raise ProductionConfigurationError(
                "Tushare realtime request batching exceeds the official capacity"
            )
        availability = execution.get("availability")
        if not isinstance(availability, Mapping) or availability != {
            "mode": "collector_ingested_at",
            "event_time_field": "time",
            "available_at_field": "ingested_at",
            "maximum_delay_minutes": 5,
        }:
            raise ProductionConfigurationError(
                "Tushare realtime availability must use collector ingested_at"
            )
        capability = execution.get("formal_capability")
        if not isinstance(capability, Mapping) or (
            capability.get("status") != "runtime_probe_required"
            or capability.get("formal_shadow_projection") != "runtime_probe_gated"
        ):
            raise ProductionConfigurationError(
                "Tushare realtime capability must remain runtime-probe gated"
            )
    return execution


def _production_archive_attested(archive: S3ImmutableArchive) -> bool:
    filesystem_type = type(archive.filesystem)
    return bool(
        type(archive) is S3ImmutableArchive
        and filesystem_type.__module__.startswith("s3fs")
        and filesystem_type.__name__ == "S3FileSystem"
    )


class ExecutionSnapshotAuthority:
    """Build immutable daily shadow bars from authoritative infrastructure.

    ``observe_open`` and ``build_session`` intentionally accept only a session.
    All other inputs are constructor-bound infrastructure dependencies.  A
    test reader/adapter can exercise the deterministic transformation, but the
    resulting snapshots are quarantined and can never be admitted by the
    production shadow bridge.
    """

    def __init__(
        self,
        *,
        catalog: ResearchCatalog,
        ledger: ProductionLedger,
        archive: S3ImmutableArchive,
        gold_reader: RegisteredGoldSnapshotReader,
        cache_root: str | Path,
        diemeng_adapter: SourceAdapter | None = None,
        open_adapter: SourceAdapter | None = None,
        runtime_mode: str = "production",
        policy: ExecutionSnapshotPolicy | None = None,
        worker_id: str = "execution-snapshot-authority",
        now: Callable[[], datetime] | None = None,
    ) -> None:
        mode = str(runtime_mode).strip().lower()
        if mode not in {"production", "test", "engineering"}:
            raise ValueError("runtime_mode must be production, engineering, or test")
        if not isinstance(gold_reader, RegisteredGoldSnapshotReader):
            raise TypeError("gold_reader must implement RegisteredGoldSnapshotReader")
        self.catalog = catalog
        self.ledger = ledger
        self.archive = archive
        self.gold_reader = gold_reader
        self.cache_root = Path(cache_root).resolve()
        self.cache_root.mkdir(parents=True, exist_ok=True)
        if diemeng_adapter is not None and open_adapter is not None:
            raise ValueError("configure only one opening source adapter")
        self.open_adapter = open_adapter or diemeng_adapter
        # Compatibility for callers/tests that still inspect the former name.
        self.diemeng_adapter = self.open_adapter
        self.runtime_mode = mode
        self.policy = policy or ExecutionSnapshotPolicy()
        self.worker_id = str(worker_id).strip()
        if not self.worker_id:
            raise ValueError("worker_id is required")
        self._now = now or (lambda: datetime.now(timezone.utc))
        # Direct construction is an explicit test/engineering seam.  Formal
        # production admission additionally requires the validated factory,
        # which seals the exact config, adapter, PG ledger and Iceberg catalog.
        self._production_binding: _ProductionBinding | None = None
        self._rotation_evidence_authority: DiemengRotationEvidenceAuthority | None = None

    @classmethod
    def from_production_config(
        cls,
        *,
        config_path: str | Path,
        env: Mapping[str, str],
        catalog: ResearchCatalog,
        ledger: ProductionLedger,
        archive: S3ImmutableArchive,
        cache_root: str | Path,
    ) -> "ExecutionSnapshotAuthority":
        """Construct the only formal production entry from validated roots.

        No frame, path to market data, snapshot id, capability label, policy or
        provider metadata can be supplied here.  The adapter contract is fixed
        in code and merely checked against the validated production document.
        """

        if type(catalog) is not ResearchCatalog:
            raise TypeError("production catalog must be ResearchCatalog")
        if type(ledger) is not ProductionLedger:
            raise TypeError("production ledger must be ProductionLedger")
        if type(archive) is not S3ImmutableArchive:
            raise TypeError("production archive must be S3ImmutableArchive")
        if ledger.engine.dialect.name != "postgresql":
            raise ProductionConfigurationError(
                "formal execution authority requires the PostgreSQL ledger"
            )
        values = dict(env)
        if str(values.get("FACTOR_LAB_PRODUCTION_ROLE") or "").strip().lower() != "worker":
            raise ProductionConfigurationError(
                "typed execution authority may run only in the production worker role"
            )
        # Inspect the credential-free route contract before the general
        # production validator resolves any secret references.  This is a
        # deliberately duplicated, side-effect-free preflight: an HTTP-only
        # Tushare SDK must not cause even a secret-file read.
        resolved_config = Path(config_path).resolve()
        preflight_payload = load_production_config(resolved_config)
        preflight_execution = _validated_execution_mapping(preflight_payload)
        if str(preflight_execution["source"]) == "tushare":
            try:
                require_tushare_sdk_https_transport()
            except Exception:
                raise ProductionConfigurationError(
                    "Tushare HTTPS transport is pending vendor/SDK confirmation"
                ) from None
        preflight_hash = content_fingerprint(
            preflight_payload,
            domain="factor-lab/research-os/v1/production-execution-configuration",
        )
        evidence = validate_production_config(config_path, env=values)
        if Path(evidence.path).resolve() != resolved_config:
            raise ProductionConfigurationError(
                "validated configuration path changed during authority construction"
            )
        payload = load_production_config(evidence.path)
        execution = _validated_execution_mapping(payload)
        configuration_hash = content_fingerprint(
            payload,
            domain="factor-lab/research-os/v1/production-execution-configuration",
        )
        if configuration_hash != preflight_hash:
            raise ProductionConfigurationError(
                "production execution configuration changed after transport preflight"
            )
        execution_contract_hash = content_fingerprint(
            {
                "source": execution["source"],
                "profile_name": execution["profile_name"],
                "credential_ref": execution["credential_ref"],
                "base_url": execution.get("base_url"),
                "dataset": execution["dataset"],
                "endpoint": execution["endpoint"],
                "method": execution["method"],
                "response_path": execution.get("response_path"),
                "request": execution["request"],
                "batching": execution.get("batching"),
                "contract": execution["contract"],
                "availability": execution.get("availability"),
            },
            domain="factor-lab/research-os/v1/production-open-adapter-binding",
        )
        artifact_root = Path(evidence.runtime_artifact_root).resolve()
        selected_cache = Path(cache_root).resolve()
        try:
            selected_cache.relative_to(artifact_root)
        except ValueError as exc:
            raise ProductionConfigurationError(
                "execution cache_root must stay below the validated artifact mount"
            ) from exc
        secrets_root = Path(str(values.get("FACTOR_LAB_SECRETS_ROOT") or ""))
        selected_source = str(execution["source"])
        credential_name = (
            "tushare_token" if selected_source == "tushare" else "diemeng_api_key"
        )
        try:
            api_key = _fixed_secret_line(secrets_root, credential_name)
        except ExecutionSnapshotAuthorityError as exc:
            raise ProductionConfigurationError(
                f"validated fixed {selected_source} credential is no longer readable"
            ) from exc
        if selected_source == "diemeng":
            adapter: SourceAdapter = DiemengSourceAdapter(
                base_url=str(execution.get("base_url") or ""),
                api_key=api_key,
                contracts=(_fixed_open_contract(),),
                endpoint_map={OPEN_SOURCE_DATASET: str(execution["endpoint"])},
                method_map={OPEN_SOURCE_DATASET: str(execution["method"])},
                response_paths={
                    OPEN_SOURCE_DATASET: str(execution["response_path"])
                },
                priority=20,
                timeout_seconds=60.0,
                max_attempts=3,
                probe_dataset=OPEN_SOURCE_DATASET,
                probe_parameters={},
                lineage={
                    "profile_name": "primary-diemeng",
                    "credential_binding": "secret://diemeng_api_key",
                    "production_configuration_hash": configuration_hash,
                    "execution_contract_hash": execution_contract_hash,
                },
            )
        else:
            try:
                import tushare as ts
            except ImportError as exc:  # pragma: no cover - image dependency
                raise ProductionConfigurationError(
                    "Tushare SDK is required for realtime opening collection"
                ) from exc
            adapter = TushareRealtimeOpenAdapter(
                ts.pro_api(api_key),
                endpoint=str(execution["endpoint"]),
                priority=10,
                collection_window_minutes=5,
                max_universe_size=500,
                max_symbols_per_request=int(
                    execution["batching"]["maximum_symbols_per_request"]
                ),
                lineage={
                    "profile_name": "primary-tushare",
                    "credential_binding": "secret://tushare_token",
                    "production_configuration_hash": configuration_hash,
                    "execution_contract_hash": execution_contract_hash,
                },
            )
        iceberg = payload.get("iceberg")
        catalog_name = (
            str(iceberg.get("catalog_name") or "").strip()
            if isinstance(iceberg, Mapping)
            else ""
        )
        if not catalog_name:
            raise ProductionConfigurationError("production Iceberg catalog_name is required")
        authority = cls(
            catalog=catalog,
            ledger=ledger,
            archive=archive,
            gold_reader=PyIcebergRegisteredGoldReader(
                expected_catalog_name=catalog_name
            ),
            cache_root=selected_cache,
            open_adapter=adapter,
            runtime_mode="production",
            policy=ExecutionSnapshotPolicy(),
            worker_id="execution-snapshot-authority",
        )
        authority._production_binding = _ProductionBinding(
            configuration_hash=configuration_hash,
            execution_contract_hash=execution_contract_hash,
            iceberg_catalog_name=catalog_name,
            archive_attested=_production_archive_attested(archive),
            # Diemeng /stock/history remains historical-only.  Tushare's
            # official realtime endpoints are merely structurally capable;
            # the actual session-bound permission/data probe still decides.
            real_time_open_capable=(selected_source == "tushare"),
            source_id=selected_source,
            source_role=f"{selected_source}_open_observation",
            credential_name=credential_name,
            provider_endpoint=str(execution["endpoint"]),
        )
        authority._rotation_evidence_authority = DiemengRotationEvidenceAuthority(
            ledger=ledger,
            secrets_root=secrets_root,
            credential_ref=str(execution["credential_ref"]),
            execution_contract_hash=execution_contract_hash,
            _factory_seal=_ROTATION_FACTORY_SEAL,
        )
        return authority

    @property
    def rotation_contract_hash(self) -> str:
        """Hash expected in the persisted vendor-rotation capability row."""

        authority = self._rotation_evidence_authority
        return _ROTATION_CONTRACT_HASH if authority is None else authority.contract_hash

    @property
    def rotation_capability_identity(self) -> tuple[str, str]:
        """Stable ``ros_source_capabilities`` key for vendor confirmation."""

        authority = self._rotation_evidence_authority
        dataset = _ROTATION_DATASET if authority is None else authority.dataset
        return (_ROTATION_SOURCE_ID, dataset)

    @property
    def production_configuration_hash(self) -> str | None:
        return (
            None
            if self._production_binding is None
            else self._production_binding.configuration_hash
        )

    @property
    def formal_open_collection_capable(self) -> bool:
        """Whether the bound adapter proves a real-time, server-timed open."""

        return bool(
            self._production_binding is not None
            and self._production_binding.real_time_open_capable
        )

    def migrate_rotation_evidence(self) -> CredentialRotationAttestation:
        """Migrate fixed local secret evidence; accepts no caller assertion."""

        authority = self._rotation_evidence_authority
        if authority is None:
            raise ExecutionNetworkBlocked(
                "credential rotation migration requires the validated production factory"
            )
        return authority.migrate()

    def persisted_rotation_attestation(self) -> CredentialRotationAttestation | None:
        """Return only a fully verified persisted rotation attestation."""

        return self._rotation_attestation()

    def _rotation_attestation(self) -> CredentialRotationAttestation | None:
        """Read rotation authority from PostgreSQL/SQLite, never a payload flag."""

        try:
            from sqlalchemy import select
        except ImportError as exc:  # pragma: no cover
            raise ExecutionEvidenceUnavailable("SQLAlchemy is required") from exc
        local_authority = self._rotation_evidence_authority
        dataset = (
            _ROTATION_DATASET if local_authority is None else local_authority.dataset
        )
        contract_hash = (
            _ROTATION_CONTRACT_HASH
            if local_authority is None
            else local_authority.contract_hash
        )
        credential = (
            "diemeng_api_key"
            if local_authority is None
            else local_authority.credential
        )
        with self.ledger.engine.connect() as connection:
            row = connection.execute(
                select(orm.SourceCapabilityModel).where(
                    orm.SourceCapabilityModel.source_id == _ROTATION_SOURCE_ID,
                    orm.SourceCapabilityModel.dataset == dataset,
                )
            ).mappings().one_or_none()
        if row is None:
            return None
        probe_hash = str(row.get("probe_hash") or "")
        fields = tuple(sorted(map(str, row.get("fields_json") or ())))
        if not (
            str(row.get("status")) == CapabilityStatus.ACCEPTED.value
            and str(row.get("contract_hash")) == contract_hash
            and _HASH.fullmatch(probe_hash)
            and str(row.get("detail") or "") == _ROTATION_DETAIL
            and fields == ("credential_ref", "vendor_confirmation_id")
        ):
            return None
        if local_authority is not None:
            try:
                expected_local_hash = local_authority.expected_evidence_hash()
            except ExecutionSnapshotAuthorityError:
                return None
            if probe_hash != expected_local_hash:
                return None
        confirmed = row.get("probed_at")
        if not isinstance(confirmed, datetime):
            return None
        if confirmed.tzinfo is None or confirmed.utcoffset() is None:
            # SQLite drops timezone offsets; PostgreSQL preserves them.  The
            # ledger contract stores UTC, so the test backend is normalized in
            # exactly the same way as ProductionLedger._db_aware.
            confirmed = confirmed.replace(tzinfo=timezone.utc)
        return CredentialRotationAttestation(
            credential=credential,
            evidence_hash=probe_hash,
            confirmed_at=_aware(confirmed, name="credential rotation probed_at"),
        )

    def _real_diemeng_adapter(self) -> bool:
        """Compatibility name for exact selected-provider attestation."""

        adapter = self.open_adapter
        binding = self._production_binding
        common = bool(
            binding is not None
            and adapter is not None
            and adapter.source_id == binding.source_id
            and OPEN_SOURCE_DATASET in adapter.contracts
            and type(self.gold_reader) is PyIcebergRegisteredGoldReader
            and self.gold_reader.production_attested
        )
        if not common:
            return False
        if binding.source_id == "diemeng":
            return bool(
                type(adapter) is DiemengSourceAdapter
                and type(adapter.session) is requests.Session
                and adapter.session.trust_env is False
                and not adapter.session.proxies
                and adapter.session.verify is True
                and adapter.session.cert is None
                and adapter.session.auth is None
                and str(adapter.base_url).lower().startswith(
                    "https://data.diemeng.chat/"
                )
            )
        return bool(
            binding.source_id == "tushare"
            and type(adapter) is TushareRealtimeOpenAdapter
            and adapter.production_attested
            and adapter.endpoint == binding.provider_endpoint
        )

    @property
    def open_source_id(self) -> str:
        binding = self._production_binding
        adapter = self.open_adapter
        return (
            binding.source_id
            if binding is not None
            else str(getattr(adapter, "source_id", "diemeng"))
        )

    @property
    def open_source_role(self) -> str:
        binding = self._production_binding
        return (
            binding.source_role
            if binding is not None
            else f"{self.open_source_id}_open_observation"
        )

    def _require_partition(
        self,
        *,
        source_id: str,
        dataset: str,
        session: date,
    ) -> PartitionRecord:
        identity = PartitionIdentity(source_id, dataset, session.isoformat())
        record = self.ledger.get_partition(identity)
        if record is None or record.status is not PartitionStatus.SUCCEEDED:
            raise ExecutionEvidenceUnavailable(
                f"required accepted partition is absent: {source_id}/{dataset}/{session}"
            )
        if not record.output_hash or not _HASH.fullmatch(record.output_hash):
            raise ExecutionEvidenceConflict(
                f"accepted partition lacks output integrity: {source_id}/{dataset}/{session}"
            )
        return record

    @staticmethod
    def _operation_result(record: PartitionRecord, *, operation: str) -> Mapping[str, Any]:
        value = record.details.get("operation_result")
        if not isinstance(value, Mapping):
            raise ExecutionEvidenceConflict(f"{operation} partition lacks operation result")
        if str(value.get("status") or "") not in {"completed", "skipped"}:
            raise ExecutionEvidenceConflict(f"{operation} operation result is not accepted")
        outputs = value.get("outputs")
        if not isinstance(outputs, Mapping):
            raise ExecutionEvidenceConflict(f"{operation} operation outputs are malformed")
        return outputs

    def _gold_evidence(self, session: date) -> _GoldEvidence:
        self._require_partition(
            source_id="research_os", dataset="accepted_trade_calendar", session=session
        )
        dq = self._require_partition(
            source_id="research_os", dataset="stage_data_quality", session=session
        )
        gold = self._require_partition(
            source_id="research_os", dataset="stage_gold", session=session
        )
        dq_outputs = self._operation_result(dq, operation="data quality")
        report = dq_outputs.get("quality_report")
        if not isinstance(report, Mapping) or str(report.get("status")) != "pass":
            raise ExecutionEvidenceConflict("data-quality partition does not contain a pass report")
        gold_outputs = self._operation_result(gold, operation="Gold publication")
        snapshot_id = str(gold.output_snapshot_id or gold_outputs.get("snapshot_id") or "")
        if not snapshot_id or snapshot_id != str(gold_outputs.get("snapshot_id") or ""):
            raise ExecutionEvidenceConflict("Gold partition snapshot identity is inconsistent")
        snapshot = self.catalog.get_snapshot(snapshot_id)
        if snapshot is None:
            raise ExecutionEvidenceUnavailable("Gold partition snapshot is not registered")
        reference = snapshot.reference
        if (
            reference.tier is not SnapshotTier.GOLD
            or reference.quality_status is not DataQualityStatus.ACCEPTED
        ):
            raise ExecutionEvidenceUnavailable("Gold partition is not accepted Gold")
        labels = {str(item).strip().lower() for item in reference.trust_labels}
        if labels & _BLOCKING_GOLD_LABELS:
            raise ExecutionEvidenceConflict("Gold snapshot carries blocking trust labels")
        if not _PRODUCTION_GOLD_LABELS <= labels:
            raise ExecutionEvidenceUnavailable(
                "Gold snapshot lacks PIT, ST and DQ trust attestations"
            )
        if dq.output_snapshot_id and dq.output_snapshot_id not in reference.parent_snapshot_ids:
            raise ExecutionEvidenceConflict("Gold parent closure omits the accepted DQ snapshot")
        calendar = reference.manifest.get("trading_calendar")
        if not isinstance(calendar, Mapping):
            raise ExecutionEvidenceUnavailable("Gold snapshot lacks accepted trading calendar")
        if str(calendar.get("quality_status") or "").lower() != "accepted":
            raise ExecutionEvidenceConflict("Gold trading calendar is not accepted")
        raw_sessions = calendar.get("sessions")
        if not isinstance(raw_sessions, (list, tuple)):
            raise ExecutionEvidenceConflict("Gold trading calendar sessions are malformed")
        try:
            sessions = tuple(date.fromisoformat(str(item)) for item in raw_sessions)
        except ValueError as exc:
            raise ExecutionEvidenceConflict(
                "Gold trading calendar contains an invalid date"
            ) from exc
        if sessions != tuple(sorted(set(sessions))) or session not in sessions:
            raise ExecutionEvidenceConflict("Gold trading calendar is unordered or omits session")
        encoded = "\n".join(item.isoformat() for item in sessions).encode("ascii")
        calendar_hash = hashlib.sha256(encoded).hexdigest()
        if calendar_hash != str(calendar.get("content_hash") or ""):
            raise ExecutionEvidenceConflict("Gold trading calendar hash is invalid")
        index = sessions.index(session)
        if index == 0:
            raise ExecutionEvidenceUnavailable("Gold calendar has no prior benchmark session")
        frame = self.gold_reader.read(reference)
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            raise ExecutionEvidenceUnavailable("physical Gold reader returned no rows")
        return _GoldEvidence(
            session=session,
            reference=reference,
            frame=frame.copy(deep=True),
            data_quality=dq,
            gold_partition=gold,
            calendar=dict(calendar),
            prior_session=sessions[index - 1],
            production_attested=bool(self.gold_reader.production_attested),
        )

    def _previous_accepted_session(self, session: date) -> date:
        """Resolve the decision date from the canonical PostgreSQL calendar."""

        try:
            accepted = tuple(
                date.fromisoformat(str(value))
                for value in self.ledger.accepted_calendar_partitions()
            )
        except ValueError as exc:
            raise ExecutionEvidenceConflict(
                "accepted trading calendar contains a malformed session"
            ) from exc
        if session not in accepted:
            raise ExecutionEvidenceUnavailable(
                "opening session is absent from the accepted trading calendar"
            )
        previous = tuple(value for value in accepted if value < session)
        if not previous:
            raise ExecutionEvidenceUnavailable(
                "opening session has no prior accepted decision session"
            )
        return max(previous)

    def _decision_gold_evidence(self, session: date) -> _GoldEvidence:
        """Load only the prior close that was immutable before today's open."""

        decision_session = self._previous_accepted_session(session)
        evidence = self._gold_evidence(decision_session)
        open_cutoff = _utc_timestamp(session, self.policy.execution_local_time)
        timestamps = (
            ("decision Gold as_of", evidence.reference.as_of),
            ("decision DQ completion", evidence.data_quality.completed_at),
            ("decision Gold completion", evidence.gold_partition.completed_at),
        )
        for label, raw in timestamps:
            if raw is None or pd.Timestamp(raw).tz_convert(timezone.utc) > open_cutoff:
                raise ExecutionEvidenceConflict(
                    f"{label} was not closed before the 09:30 decision cutoff"
                )
        return evidence

    def _session_rows_for_tickers(
        self,
        evidence: _GoldEvidence,
        *,
        session: date,
        tickers: Sequence[str],
    ) -> pd.DataFrame:
        """Read closing/status rows for the exact prior-close decision universe."""

        frame = evidence.frame.copy(deep=True)
        ticker_column = (
            "ticker"
            if "ticker" in frame
            else "ts_code"
            if "ts_code" in frame
            else None
        )
        if ticker_column is None or "trade_date" not in frame:
            raise ExecutionEvidenceUnavailable(
                "Gold closure lacks ticker/date fields"
            )
        expected = tuple(sorted(set(map(str, tickers))))
        if len(expected) != self.policy.target_universe_size:
            raise ExecutionEvidenceConflict(
                "decision universe size differs from the execution policy"
            )
        frame["_ticker"] = frame[ticker_column].astype("string").str.strip()
        frame["_trade_date"] = pd.to_datetime(
            frame["trade_date"], errors="coerce"
        ).dt.date
        current = frame.loc[
            frame["_trade_date"].eq(session)
            & frame["_ticker"].isin(set(expected))
        ].copy()
        observed = tuple(sorted(map(str, current["_ticker"])))
        if (
            current.duplicated(["_ticker", "_trade_date"]).any()
            or observed != expected
        ):
            raise ExecutionEvidenceConflict(
                "same-day Gold does not exactly close the decision universe"
            )
        return current.sort_values("_ticker", kind="mergesort").reset_index(drop=True)

    def _current_universe(self, evidence: _GoldEvidence, session: date) -> pd.DataFrame:
        frame = evidence.frame.copy(deep=True)
        ticker_column = (
            "ticker"
            if "ticker" in frame
            else "ts_code"
            if "ts_code" in frame
            else None
        )
        if (
            ticker_column is None
            or "trade_date" not in frame
            or "universe_member" not in frame
        ):
            raise ExecutionEvidenceUnavailable("Gold panel lacks ticker/date/universe fields")
        frame["_ticker"] = frame[ticker_column].astype("string").str.strip()
        frame["_trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
        current = frame.loc[
            frame["_trade_date"].eq(session)
            & frame["universe_member"].astype("boolean").fillna(False)
        ].copy()
        if current.empty:
            raise ExecutionEvidenceUnavailable("Gold panel has no current universe members")
        if current["_ticker"].isna().any() or current["_ticker"].eq("").any():
            raise ExecutionEvidenceConflict("Gold universe has empty tickers")
        if current.duplicated(["_ticker", "_trade_date"]).any():
            raise ExecutionEvidenceConflict("Gold universe has duplicate ticker/date rows")
        if len(current) != self.policy.target_universe_size:
            raise ExecutionEvidenceConflict(
                f"Gold universe has {len(current)} rows; "
                f"{self.policy.target_universe_size} required"
            )
        if "benchmark_weight" not in current:
            raise ExecutionEvidenceUnavailable("Gold universe lacks benchmark weights")
        weights = pd.to_numeric(current["benchmark_weight"], errors="coerce")
        expected = 1.0 / self.policy.target_universe_size
        uniform = weights.map(
            lambda value: math.isclose(
                float(value), expected, rel_tol=1e-9, abs_tol=1e-12
            )
        ).all()
        fully_invested = math.isclose(
            float(weights.sum()), 1.0, rel_tol=1e-9, abs_tol=1e-12
        )
        if weights.isna().any() or not uniform or not fully_invested:
            raise ExecutionEvidenceConflict(
                "Gold benchmark is not eligible-universe equal weight"
            )
        return current.sort_values("_ticker", kind="mergesort").reset_index(drop=True)

    def _validate_execution_status(
        self,
        current: pd.DataFrame,
        *,
        session: date,
    ) -> pd.DataFrame:
        open_cutoff = _utc_timestamp(session, self.policy.execution_local_time)
        status_available = _timestamp_column(current, "trade_status_available_at")
        if status_available.isna().any() or status_available.gt(open_cutoff).any():
            raise ExecutionEvidenceConflict(
                "accepted suspension/limit status was not available by 09:30"
            )
        suspended = _bool_column(current, "is_suspended")
        if "is_delisted" in current:
            delisted = _bool_column(current, "is_delisted")
            delist_available = _timestamp_column(
                current, "delist_status_available_at"
            )
        elif "delist_date" in current:
            delist_dates = pd.to_datetime(
                current["delist_date"], errors="coerce"
            ).dt.date
            delisted = delist_dates.notna() & delist_dates.le(session)
            delist_available = _timestamp_column(current, "stock_basic_available_at")
        else:
            raise ExecutionEvidenceUnavailable("Gold panel lacks PIT delisting status")
        if delist_available.isna().any() or delist_available.gt(open_cutoff).any():
            raise ExecutionEvidenceConflict("delisting status was not available by 09:30")
        if delisted.any():
            raise ExecutionEvidenceConflict("current eligible universe contains delisted securities")
        for name in ("up_limit", "down_limit"):
            if name not in current:
                raise ExecutionEvidenceUnavailable(
                    f"Gold execution evidence omits {name}"
                )
            values = pd.to_numeric(current[name], errors="coerce")
            if (
                values.isna().any()
                or (~values.map(math.isfinite)).any()
                or values.le(0).any()
            ):
                raise ExecutionEvidenceConflict(f"accepted {name} status is incomplete")
        if pd.to_numeric(current["down_limit"], errors="coerce").ge(
            pd.to_numeric(current["up_limit"], errors="coerce")
        ).any():
            raise ExecutionEvidenceConflict("accepted price-limit bounds conflict")
        result = current[["_ticker", "up_limit", "down_limit"]].copy()
        result["is_suspended"] = suspended.to_numpy()
        result["is_delisted"] = delisted.to_numpy()
        result["status_available_at"] = status_available.to_numpy()
        result["delist_available_at"] = delist_available.to_numpy()
        return result

    def _write_archive(
        self,
        frame: pd.DataFrame,
        *,
        session: date,
        role: str,
    ) -> ArchivedObject:
        ordered = frame.reset_index(drop=True)
        with tempfile.TemporaryDirectory(
            prefix="execution-authority-", dir=self.cache_root
        ) as directory:
            path = Path(directory) / f"{role}.parquet"
            ordered.to_parquet(path, index=False)
            archived = self.archive.archive_file(
                path,
                logical_path=(
                    f"execution-snapshots/trade_date={session.isoformat()}/{role}"
                ),
            )
            restored_path = Path(directory) / f"restored-{role}.parquet"
            self.archive.restore_file(archived, restored_path)
            restored = pd.read_parquet(restored_path)
        if list(restored.columns) != list(ordered.columns) or not restored.equals(ordered):
            raise ExecutionEvidenceConflict(
                f"physical {role} object differs after MinIO round trip"
            )
        return archived

    @staticmethod
    def _snapshot_manifest(
        *,
        role: str,
        session: date,
        archived: ArchivedObject,
        parent_snapshot_ids: Sequence[str],
        as_of: datetime,
        row_count: int,
        columns: Sequence[str],
        calendar: Mapping[str, Any],
        capability: ExecutionCapabilityDecision,
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "role": role,
            "trade_date": session.isoformat(),
            "tier": "gold" if role in {"execution", "mark"} else "silver",
            "as_of": _aware(as_of, name="snapshot as_of").isoformat(),
            "parent_snapshot_ids": sorted(set(map(str, parent_snapshot_ids))),
            "quality_status": (
                "pass"
                if capability is ExecutionCapabilityDecision.ACCEPTED
                else "blocked"
            ),
            "capability": capability.value,
            "row_count": int(row_count),
            "columns": list(map(str, columns)),
            "physical_object": _archive_evidence(archived),
            "trading_calendar": dict(calendar),
            "price_basis": "raw_cash_accounting",
            "evidence": dict(evidence),
        }

    def _register_role_snapshot(
        self,
        *,
        role: str,
        session: date,
        frame: pd.DataFrame,
        archived: ArchivedObject,
        parent_snapshot_ids: Sequence[str],
        as_of: datetime,
        calendar: Mapping[str, Any],
        capability: ExecutionCapabilityDecision,
        evidence: Mapping[str, Any],
    ) -> DataSnapshotRef:
        manifest = self._snapshot_manifest(
            role=role,
            session=session,
            archived=archived,
            parent_snapshot_ids=parent_snapshot_ids,
            as_of=as_of,
            row_count=len(frame),
            columns=list(frame.columns),
            calendar=calendar,
            capability=capability,
            evidence=evidence,
        )
        content_hash = content_fingerprint(
            manifest,
            domain="factor-lab/research-os/v1/execution-role-snapshot",
        )
        prefix = {"execution": "execution", "mark": "mark", BUNDLE_ROLE: "bars"}[role]
        quality = (
            DataQualityStatus.ACCEPTED
            if capability is ExecutionCapabilityDecision.ACCEPTED
            else DataQualityStatus.QUARANTINED
        )
        tier = SnapshotTier.GOLD if role in {"execution", "mark"} else SnapshotTier.SILVER
        labels = (
            (
                "point_in_time",
                "quality_accepted",
                "physical_object_verified",
                f"shadow_{role}_role",
            )
            if quality is DataQualityStatus.ACCEPTED
            else (
                "non_forward",
                "data_quarantined",
                "synthetic_or_test_source",
                f"shadow_{role}_role",
            )
        )
        reference = DataSnapshotRef(
            snapshot_id=f"{prefix}_{content_hash}",
            tier=tier,
            uri=archived.uri,
            content_hash=content_hash,
            parent_snapshot_ids=tuple(sorted(set(map(str, parent_snapshot_ids)))),
            as_of=_aware(as_of, name="snapshot as_of"),
            quality_status=quality,
            trust_labels=labels,
            manifest=manifest,
        )
        self.catalog.register_snapshot(reference)
        loaded = self.catalog.get_snapshot(reference.snapshot_id)
        if loaded is None or loaded.reference != reference:
            raise ExecutionEvidenceConflict("catalog snapshot differs after registration")
        return reference

    def _load_ref_frame(self, reference: DataSnapshotRef, *, expected_role: str) -> pd.DataFrame:
        if str(reference.manifest.get("role") or "") != expected_role:
            raise ExecutionEvidenceConflict(
                f"snapshot role is {reference.manifest.get('role')!r}, expected {expected_role!r}"
            )
        if expected_role == self.open_source_role:
            domain = (
                f"factor-lab/research-os/v1/{self.open_source_id}-open-snapshot"
            )
            expected_id = (
                f"{self.open_source_id}_open_"
                f"{content_fingerprint(reference.manifest, domain=domain)}"
            )
        else:
            domain = "factor-lab/research-os/v1/execution-role-snapshot"
            digest = content_fingerprint(reference.manifest, domain=domain)
            prefix = {
                "execution": "execution",
                "mark": "mark",
                BUNDLE_ROLE: "bars",
            }.get(expected_role)
            if prefix is None:
                raise ExecutionEvidenceConflict("unsupported snapshot role")
            expected_id = f"{prefix}_{digest}"
        digest = expected_id.rsplit("_", 1)[-1]
        if not (
            reference.snapshot_id == expected_id
            and reference.content_hash == digest
            and tuple(reference.parent_snapshot_ids)
            == tuple(map(str, reference.manifest.get("parent_snapshot_ids") or ()))
            and _aware(reference.as_of, name="snapshot as_of")
            == _aware(
                datetime.fromisoformat(str(reference.manifest.get("as_of"))),
                name="manifest as_of",
            )
        ):
            raise ExecutionEvidenceConflict("snapshot reference identity is corrupt")
        quality_value = str(reference.manifest.get("quality_status") or "")
        if quality_value not in {"pass", "blocked"}:
            raise ExecutionEvidenceConflict("snapshot manifest quality is invalid")
        expected_quality = (
            DataQualityStatus.ACCEPTED
            if quality_value == "pass"
            else DataQualityStatus.QUARANTINED
        )
        expected_tier = (
            SnapshotTier.BRONZE
            if expected_role == self.open_source_role
            else SnapshotTier.GOLD
            if expected_role in {"execution", "mark"}
            else SnapshotTier.SILVER
        )
        if (
            reference.quality_status is not expected_quality
            or reference.tier is not expected_tier
            or str(reference.manifest.get("tier") or "") != expected_tier.value
        ):
            raise ExecutionEvidenceConflict("snapshot reference quality differs from manifest")
        archived_value = reference.manifest.get("physical_object")
        if not isinstance(archived_value, Mapping):
            raise ExecutionEvidenceConflict("snapshot physical object is absent")
        archived = _archived(archived_value)
        if archived.uri != reference.uri:
            raise ExecutionEvidenceConflict("snapshot URI differs from physical object")
        with tempfile.TemporaryDirectory(
            prefix="execution-authority-load-", dir=self.cache_root
        ) as directory:
            path = Path(directory) / f"{expected_role}.parquet"
            self.archive.restore_file(archived, path)
            frame = pd.read_parquet(path)
        if frame.empty or len(frame) != int(reference.manifest.get("row_count", -1)):
            raise ExecutionEvidenceConflict("snapshot row count differs from physical object")
        if list(map(str, frame.columns)) != list(reference.manifest.get("columns") or ()):
            raise ExecutionEvidenceConflict("snapshot columns differ from physical object")
        return frame

    def _source_snapshot(
        self,
        *,
        session: date,
        frame: pd.DataFrame,
        archived: ArchivedObject,
        gold: _GoldEvidence,
        probe: Mapping[str, Any],
        vendor_revision: str,
        source_lineage: Mapping[str, Any],
        physical_source_attested: bool,
        rotation: CredentialRotationAttestation | None,
    ) -> DataSnapshotRef:
        available_values = pd.to_datetime(
            frame["source_available_at"], errors="coerce", utc=True
        )
        if available_values.isna().any():
            raise ExecutionEvidenceConflict("opening source availability is malformed")
        source_as_of = max(available_values).to_pydatetime()
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "role": self.open_source_role,
            "source_id": self.open_source_id,
            "trade_date": session.isoformat(),
            "tier": "bronze",
            "as_of": _aware(source_as_of, name="source as_of").isoformat(),
            "parent_snapshot_ids": [gold.reference.snapshot_id],
            "decision_trade_date": gold.session.isoformat(),
            "decision_snapshot_id": gold.reference.snapshot_id,
            "decision_snapshot_hash": gold.reference.content_hash,
            "decision_gold_partition_hash": gold.gold_partition.output_hash,
            "decision_data_quality_partition_hash": gold.data_quality.output_hash,
            "quality_status": "pass" if physical_source_attested else "blocked",
            "row_count": len(frame),
            "columns": list(map(str, frame.columns)),
            "physical_object": _archive_evidence(archived),
            "vendor_revision": vendor_revision,
            "source_lineage": dict(source_lineage),
            "probe": dict(probe),
            "physical_source_attested": physical_source_attested,
            "rotation_evidence_hash": None if rotation is None else rotation.evidence_hash,
            "production_configuration_hash": self.production_configuration_hash,
        }
        content_hash = content_fingerprint(
            manifest,
            domain=f"factor-lab/research-os/v1/{self.open_source_id}-open-snapshot",
        )
        reference = DataSnapshotRef(
            snapshot_id=f"{self.open_source_id}_open_{content_hash}",
            tier=SnapshotTier.BRONZE,
            uri=archived.uri,
            content_hash=content_hash,
            parent_snapshot_ids=(gold.reference.snapshot_id,),
            as_of=_aware(source_as_of, name="source as_of"),
            quality_status=(
                DataQualityStatus.ACCEPTED
                if physical_source_attested
                else DataQualityStatus.QUARANTINED
            ),
            trust_labels=(
                ("physical_vendor_observation", "point_in_time")
                if physical_source_attested
                else ("non_forward", "synthetic_or_test_source", "data_quarantined")
            ),
            manifest=manifest,
        )
        self.catalog.register_snapshot(reference)
        return reference

    def _load_open_evidence(self, session: date) -> _OpenEvidence:
        partition = self._require_partition(
            source_id=self.open_source_id, dataset=OPEN_DATASET, session=session
        )
        details = partition.details
        if (
            str(details.get("source_id") or self.open_source_id)
            != self.open_source_id
            or str(details.get("source_role") or self.open_source_role)
            != self.open_source_role
        ):
            raise ExecutionEvidenceConflict(
                "opening partition source authority changed"
            )
        expected_hash = content_fingerprint(
            dict(details), domain="factor-lab/research-os/v1/open-partition-result"
        )
        if expected_hash != partition.output_hash:
            raise ExecutionEvidenceConflict("opening partition result hash is invalid")
        snapshot_id = str(partition.output_snapshot_id or details.get("snapshot_id") or "")
        if snapshot_id != str(details.get("snapshot_id") or ""):
            raise ExecutionEvidenceConflict("opening partition snapshot ids disagree")
        record = self.catalog.get_snapshot(snapshot_id)
        if record is None:
            raise ExecutionEvidenceUnavailable("opening observation snapshot is not cataloged")
        reference = record.reference
        if reference.content_hash != str(details.get("snapshot_hash") or ""):
            raise ExecutionEvidenceConflict("opening partition snapshot hash changed")
        if str(reference.manifest.get("role")) != self.open_source_role:
            raise ExecutionEvidenceConflict("opening snapshot role is invalid")
        if str(reference.manifest.get("source_id") or self.open_source_id) != (
            self.open_source_id
        ):
            raise ExecutionEvidenceConflict("opening snapshot source is invalid")
        if dict(reference.manifest.get("source_lineage") or {}) != dict(
            details.get("source_lineage") or {}
        ):
            raise ExecutionEvidenceConflict(
                "opening snapshot and partition source lineage differ"
            )
        decision = self._decision_gold_evidence(session)
        expected_decision = {
            "decision_trade_date": decision.session.isoformat(),
            "decision_snapshot_id": decision.reference.snapshot_id,
            "decision_snapshot_hash": decision.reference.content_hash,
            "decision_gold_partition_hash": decision.gold_partition.output_hash,
            "decision_data_quality_partition_hash": decision.data_quality.output_hash,
        }
        if any(
            str(reference.manifest.get(key) or "") != str(value or "")
            for key, value in expected_decision.items()
        ):
            raise ExecutionEvidenceConflict(
                "opening observation is not bound to the prior accepted decision snapshot"
            )
        if tuple(reference.parent_snapshot_ids) != (decision.reference.snapshot_id,):
            raise ExecutionEvidenceConflict(
                "opening parent is not the prior accepted decision snapshot"
            )
        if any(
            str(details.get(key) or "") != str(value or "")
            for key, value in expected_decision.items()
        ):
            raise ExecutionEvidenceConflict(
                "opening partition decision authority differs from its snapshot"
            )
        frame = self._load_ref_frame(reference, expected_role=self.open_source_role)
        attested = bool(reference.manifest.get("physical_source_attested"))
        rotation_hash = str(reference.manifest.get("rotation_evidence_hash") or "") or None
        if attested and (
            reference.quality_status is not DataQualityStatus.ACCEPTED
            or not rotation_hash
            or not _HASH.fullmatch(rotation_hash)
        ):
            raise ExecutionEvidenceConflict("opening physical attestation is incomplete")
        if attested and (
            self._production_binding is None
            or str(reference.manifest.get("production_configuration_hash") or "")
            != self._production_binding.configuration_hash
        ):
            raise ExecutionEvidenceConflict(
                "opening observation belongs to another production configuration"
            )
        return _OpenEvidence(
            reference=reference,
            frame=frame,
            partition=partition,
            physical_source_attested=attested,
            rotation_evidence_hash=rotation_hash,
            decision_session=decision.session,
            decision_snapshot_id=decision.reference.snapshot_id,
            decision_gold_partition_hash=str(decision.gold_partition.output_hash),
            decision_data_quality_partition_hash=str(
                decision.data_quality.output_hash
            ),
        )

    def _require_live_open_window(self, session: date) -> None:
        """Forbid a new production open partition outside its live event."""

        if self.runtime_mode != "production":
            return
        observed = _database_now(self.ledger).astimezone(_SHANGHAI)
        cutoff = datetime.combine(
            session, self.policy.execution_local_time, tzinfo=_SHANGHAI
        )
        if observed.date() != session or not cutoff <= observed <= cutoff + timedelta(
            minutes=self.policy.observation_deadline_minutes
        ):
            raise ExecutionNetworkBlocked(
                "production opening observation is live-only and cannot be backfilled"
            )

    def _require_live_closure_window(self, session: date) -> None:
        """Forbid closure before the mark, while allowing restart-safe retries."""

        if self.runtime_mode != "production":
            return
        observed = _database_now(self.ledger).astimezone(_SHANGHAI)
        cutoff = datetime.combine(
            session, self.policy.mark_local_time, tzinfo=_SHANGHAI
        )
        if observed < cutoff:
            raise ExecutionEvidenceUnavailable(
                "production execution closure requires the completed closing mark"
            )

    def observe_open(self, trade_date: date | str) -> DataSnapshotRef:
        """Fetch and persist one 09:30 observation partition.

        In production the credential-rotation row is checked *before* probe or
        fetch, so a pending/old credential cannot cause an accidental network
        call.  Engineering/test adapters can exercise the path but always
        publish quarantined, non-forward evidence.
        """

        session = _session(trade_date)
        existing = self.ledger.get_partition(
            PartitionIdentity(self.open_source_id, OPEN_DATASET, session.isoformat())
        )
        if existing is not None and existing.status is PartitionStatus.SUCCEEDED:
            return self._load_open_evidence(session).reference
        adapter = self.open_adapter
        if adapter is None:
            raise ExecutionEvidenceUnavailable("opening source adapter is not configured")
        rotation = self._rotation_attestation()
        if self.runtime_mode == "production" and rotation is None:
            raise ExecutionNetworkBlocked(
                f"{self.open_source_id} credential lacks persisted vendor-confirmed rotation"
            )
        real_adapter = self._real_diemeng_adapter()
        if self.runtime_mode == "production" and not real_adapter:
            raise ExecutionNetworkBlocked(
                "production opening probe requires the concrete selected provider adapter"
            )
        self._require_live_open_window(session)

        decision_gold = self._decision_gold_evidence(session)
        decision_universe = self._current_universe(
            decision_gold, decision_gold.session
        )
        decision_tickers = tuple(map(str, decision_universe["_ticker"]))
        input_hash = content_fingerprint(
            {
                "schema_version": SCHEMA_VERSION,
                "trade_date": session,
                "decision_trade_date": decision_gold.session,
                "decision_snapshot_id": decision_gold.reference.snapshot_id,
                "decision_snapshot_hash": decision_gold.reference.content_hash,
                "decision_gold_partition_hash": decision_gold.gold_partition.output_hash,
                "decision_data_quality_partition_hash": (
                    decision_gold.data_quality.output_hash
                ),
                "tickers": list(decision_tickers),
                "adapter_contract": asdict(adapter.contract_for(OPEN_SOURCE_DATASET)),
                "production_configuration_hash": self.production_configuration_hash,
                "policy": self.policy.fingerprint_payload(),
            },
            domain="factor-lab/research-os/v1/open-partition-input",
        )
        identity = PartitionIdentity(
            self.open_source_id, OPEN_DATASET, session.isoformat()
        )
        self.ledger.ensure_partition(
            identity,
            created_at=_aware(self._now(), name="now"),
            input_hash=input_hash,
            details={"authority": SCHEMA_VERSION},
        )
        lease = self.ledger.claim(
            owner=self.worker_id,
            identity=identity,
            now=_aware(self._now(), name="now"),
            lease_for=timedelta(minutes=self.policy.lease_minutes),
            maximum_attempts=self.policy.maximum_attempts,
        )
        if lease is None:
            raise ExecutionLeaseBusy("opening observation partition is already leased")
        success_committed = False
        try:
            # For the concrete production adapter, the first exact 09:30 fetch
            # is itself the bounded provider probe.  Calling the adapter's
            # generic probe here would require a caller/config-supplied ticker
            # and date, recreating the very self-report seam this authority is
            # intended to remove.
            probe = None if real_adapter else adapter.probe()
            is_tushare_realtime = type(adapter) is TushareRealtimeOpenAdapter
            if (
                probe is not None
                and probe.health is not SourceHealth.HEALTHY
                and not is_tushare_realtime
            ):
                raise ExecutionEvidenceUnavailable(
                    f"{self.open_source_id} bounded probe is not healthy"
                )
            rows: list[dict[str, Any]] = []
            revisions: list[str] = []
            ingested: list[datetime] = []
            expected_open = _utc_timestamp(
                session, self.policy.execution_local_time
            )
            batches: list[SourceBatch] = []
            batch_by_ticker: dict[str, tuple[pd.Series, SourceBatch]] = {}
            source_lineage: dict[str, Any] = {}
            missing_available_at: dict[str, datetime] = {}
            trusted_available_by_batch: dict[int, datetime] = {}
            database_received_at: datetime | None = None
            collector_clock_skew_seconds: float | None = None
            if is_tushare_realtime:
                collection = adapter.fetch_open_batch(decision_tickers, session)
                batches.append(collection.batch)
                source_lineage = dict(collection.batch.lineage)
                trusted_received_at = collection.received_at
                if self.runtime_mode == "production" and real_adapter:
                    database_received_at = _database_now(self.ledger)
                    collector_clock_skew_seconds = abs(
                        (database_received_at - collection.received_at).total_seconds()
                    )
                    if collector_clock_skew_seconds > 30.0:
                        raise ExecutionEvidenceConflict(
                            "collector receive clock differs from PostgreSQL authority"
                        )
                    local_database_received = database_received_at.astimezone(_SHANGHAI)
                    database_deadline = datetime.combine(
                        session,
                        self.policy.execution_local_time,
                        tzinfo=_SHANGHAI,
                    ) + timedelta(
                        minutes=self.policy.observation_deadline_minutes
                    )
                    if not (
                        local_database_received.date() == session
                        and local_database_received <= database_deadline
                    ):
                        raise ExecutionEvidenceConflict(
                            "PostgreSQL observed the realtime response after the live window"
                        )
                    trusted_received_at = max(
                        trusted_received_at, database_received_at
                    )
                trusted_available_by_batch[id(collection.batch)] = trusted_received_at
                probe = ProbeResult(
                    source_id=self.open_source_id,
                    health=SourceHealth.HEALTHY,
                    checked_at=collection.received_at,
                    latency_ms=0.0,
                    datasets=(OPEN_SOURCE_DATASET,),
                    message=(
                        f"session-bound exact 09:30 1MIN response; "
                        f"official_document={collection.doc_id}"
                    ),
                )
                for _, item in collection.batch.frame.iterrows():
                    ticker = str(item.get("stock_code") or "").strip()
                    if ticker in batch_by_ticker:
                        raise ExecutionEvidenceConflict(
                            "opening provider returned a duplicate ticker"
                        )
                    batch_by_ticker[ticker] = (item, collection.batch)
                for ticker in decision_tickers:
                    missing_available_at[ticker] = trusted_received_at
            else:
                for ticker in sorted(decision_tickers):
                    request = FetchRequest(
                        dataset=OPEN_SOURCE_DATASET,
                        parameters={
                            "stock_code": ticker,
                            "level": "1min",
                            "start_time": f"{session.isoformat()} 09:30:00",
                            "end_time": f"{session.isoformat()} 09:30:00",
                            "page": 0,
                            "page_size": 8,
                        },
                        fields=tuple(
                            adapter.contract_for(OPEN_SOURCE_DATASET).field_map
                        ),
                    )
                    batch = adapter.fetch(request)
                    batches.append(batch)
                    trusted_available_by_batch[id(batch)] = _aware(
                        batch.ingested_at, name="batch.ingested_at"
                    )
                    assert_point_in_time_columns(list(batch.frame.columns))
                    event_column = batch.contract.event_time_field
                    events = batch.frame[event_column].map(_event_timestamp)
                    exact = batch.frame.loc[
                        events.map(
                            lambda value: value.date() == session
                            and value.time() == self.policy.execution_local_time
                        )
                    ].copy()
                    missing_available_at[ticker] = _aware(
                        batch.ingested_at, name="batch.ingested_at"
                    )
                    if exact.empty and not batch.frame.empty:
                        raise ExecutionEvidenceUnavailable(
                            f"ticker {ticker} lacks exactly one 09:30 observation"
                        )
                    if len(exact) > 1:
                        raise ExecutionEvidenceUnavailable(
                            f"ticker {ticker} lacks exactly one 09:30 observation"
                        )
                    if len(exact) == 1:
                        batch_by_ticker[ticker] = (exact.iloc[0], batch)
            for batch in batches:
                assert_point_in_time_columns(list(batch.frame.columns))
                revisions.append(batch.vendor_revision)
                ingested.append(batch.ingested_at)
            for ticker in sorted(decision_tickers):
                observed = batch_by_ticker.get(ticker)
                if observed is None:
                    rows.append(
                        {
                            "ticker": ticker,
                            "trade_date": session.isoformat(),
                            "source_event_time": expected_open,
                            "source_available_at": max(
                                expected_open.to_pydatetime(),
                                missing_available_at[ticker],
                            ),
                            "open_raw": float("nan"),
                            "observation_status": "unobserved_0930",
                        }
                    )
                    continue
                row, batch = observed
                event_column = batch.contract.event_time_field
                observed_ticker = str(row.get("stock_code", row.get("ticker", ""))).strip()
                if observed_ticker != ticker:
                    raise ExecutionEvidenceConflict(
                        f"{self.open_source_id} opening ticker differs from request"
                    )
                price = float(row.get("open", float("nan")))
                if not math.isfinite(price) or price <= 0:
                    raise ExecutionEvidenceConflict(
                        f"{self.open_source_id} 09:30 opening price is invalid"
                    )
                event = _event_timestamp(row[event_column]).tz_convert(timezone.utc)
                # The official realtime API does not provide a server-side
                # availability timestamp.  Its collector receive time is the
                # only honest availability.  Historical adapters remain
                # knowable no earlier than their ingestion time as well.
                available = pd.Timestamp(
                    max(
                        event.to_pydatetime(),
                        trusted_available_by_batch[id(batch)],
                    )
                )
                rows.append(
                    {
                        "ticker": ticker,
                        "trade_date": session.isoformat(),
                        "source_event_time": event,
                        # Historical minute responses become knowable only
                        # when ingested.  The event timestamp is never reused
                        # as availability merely because the row describes
                        # 09:30.
                        "source_available_at": available,
                        "open_raw": price,
                        "observation_status": "observed_0930",
                    }
                )
            if probe is None:
                raise ExecutionEvidenceUnavailable(
                    "no decision-universe security reached the 09:30 provider probe"
                )
            frame = pd.DataFrame.from_records(rows).sort_values("ticker").reset_index(drop=True)
            if len(frame) != self.policy.target_universe_size or frame["ticker"].duplicated().any():
                raise ExecutionEvidenceConflict("opening observations do not cover the universe")
            event_values = pd.to_datetime(frame["source_event_time"], errors="coerce", utc=True)
            available_values = pd.to_datetime(
                frame["source_available_at"], errors="coerce", utc=True
            )
            if (
                event_values.isna().any()
                or available_values.isna().any()
                or not event_values.eq(expected_open).all()
                or available_values.lt(event_values).any()
            ):
                raise ExecutionEvidenceConflict(
                    "opening observation event/availability chronology is invalid"
                )
            vendor_revision = content_fingerprint(
                sorted(revisions),
                domain=(
                    f"factor-lab/research-os/v1/{self.open_source_id}-open-"
                    "vendor-revisions"
                ),
            )
            archived = self._write_archive(
                frame, session=session, role=self.open_source_role
            )
            live_deadline = expected_open + pd.Timedelta(
                minutes=self.policy.observation_deadline_minutes
            )
            complete_observed_universe = frame["observation_status"].astype(str).eq(
                "observed_0930"
            ).all()
            physically_attested = bool(
                self.runtime_mode == "production"
                and real_adapter
                and rotation is not None
                and decision_gold.production_attested
                and self._production_binding is not None
                and self._production_binding.archive_attested
                and self._production_binding.real_time_open_capable
                and available_values.le(live_deadline).all()
                and int(source_lineage.get("observed_ticker_count") or 0) > 0
                and database_received_at is not None
                and collector_clock_skew_seconds is not None
                and collector_clock_skew_seconds <= 30.0
            )
            probe_payload = {
                "source_id": probe.source_id,
                "health": probe.health.value,
                "checked_at": _aware(probe.checked_at, name="probe.checked_at").isoformat(),
                "datasets": list(probe.datasets),
                "latency_ms": float(probe.latency_ms),
                "probe_kind": (
                    "session_bound_0930_fetch"
                    if real_adapter
                    else "adapter_capability_probe"
                ),
            }
            reference = self._source_snapshot(
                session=session,
                frame=frame,
                archived=archived,
                gold=decision_gold,
                probe=probe_payload,
                vendor_revision=vendor_revision,
                source_lineage=source_lineage,
                physical_source_attested=physically_attested,
                rotation=rotation,
            )
            details = {
                "schema_version": SCHEMA_VERSION,
                "source_id": self.open_source_id,
                "source_role": self.open_source_role,
                "snapshot_id": reference.snapshot_id,
                "snapshot_hash": reference.content_hash,
                "physical_object": _archive_evidence(archived),
                "decision_trade_date": decision_gold.session.isoformat(),
                "decision_snapshot_id": decision_gold.reference.snapshot_id,
                "decision_snapshot_hash": decision_gold.reference.content_hash,
                "decision_gold_partition_hash": (
                    decision_gold.gold_partition.output_hash
                ),
                "decision_data_quality_partition_hash": (
                    decision_gold.data_quality.output_hash
                ),
                "vendor_revision": vendor_revision,
                "source_lineage": source_lineage,
                "probe": probe_payload,
                "physical_source_attested": physically_attested,
                "complete_observed_universe": bool(complete_observed_universe),
                "rotation_evidence_hash": (
                    None if rotation is None else rotation.evidence_hash
                ),
                "ingested_at_max": (
                    None
                    if not ingested
                    else max(
                        _aware(item, name="batch.ingested_at") for item in ingested
                    ).isoformat()
                ),
                "database_received_at": (
                    None
                    if database_received_at is None
                    else database_received_at.isoformat()
                ),
                "collector_clock_skew_seconds": collector_clock_skew_seconds,
                "collector_clock_verified": bool(
                    self.runtime_mode == "production"
                    and real_adapter
                    and database_received_at is not None
                    and collector_clock_skew_seconds is not None
                    and collector_clock_skew_seconds <= 30.0
                ),
            }
            output_hash = content_fingerprint(
                details, domain="factor-lab/research-os/v1/open-partition-result"
            )
            self.ledger.upsert_capability(
                CapabilityRecord(
                    source_id=self.open_source_id,
                    dataset=OPEN_DATASET,
                    status=CapabilityStatus.DEGRADED,
                    contract_hash=content_fingerprint(
                        asdict(adapter.contract_for(OPEN_SOURCE_DATASET)),
                        domain="factor-lab/research-os/v1/source-contract",
                    ),
                    fields=tuple(map(str, frame.columns)),
                    detail="physical open observed; merged execution DQ pending",
                    probed_at=_aware(self._now(), name="now"),
                    probe_hash=content_fingerprint(
                        probe_payload,
                        domain=(
                            f"factor-lab/research-os/v1/{self.open_source_id}-open-probe"
                        ),
                    ),
                )
            )
            # Capability persistence is part of publishing this partition.  If
            # it fails, leave the partition retryable instead of committing a
            # success that later callers could reuse without its readiness
            # evidence.
            self.ledger.finish(
                lease,
                status=PartitionStatus.SUCCEEDED,
                completed_at=_aware(self._now(), name="now"),
                output_snapshot_id=reference.snapshot_id,
                output_hash=output_hash,
                vendor_revision=vendor_revision,
                details=details,
            )
            success_committed = True
            return reference
        except Exception as exc:
            if not success_committed:
                try:
                    self.ledger.finish(
                        lease,
                        status=PartitionStatus.FAILED,
                        completed_at=_aware(self._now(), name="now"),
                        error_code="execution_open_rejected",
                        # This is a durable PostgreSQL/WebUI boundary.  An
                        # arbitrary provider, archive, or plugin exception may
                        # contain request headers, response bodies, paths, or
                        # credentials even when the known adapters sanitize
                        # their own failures.  Persist only the exception
                        # class; the causal exception is still re-raised to
                        # the in-process caller.
                        error=type(exc).__name__,
                    )
                except Exception as cleanup_exc:
                    _add_cleanup_note(
                        exc,
                        "opening partition failure finalization also failed with "
                        f"{type(cleanup_exc).__name__}",
                    )
            raise

    def _benchmark_return(
        self,
        decision_gold: _GoldEvidence,
        current: pd.DataFrame,
        *,
        session: date,
    ) -> float:
        if decision_gold.session >= session:
            raise ExecutionEvidenceConflict(
                "benchmark decision snapshot is not prior to its return session"
            )
        frame = decision_gold.frame.copy(deep=True)
        ticker_column = "ticker" if "ticker" in frame else "ts_code"
        frame["_ticker"] = frame[ticker_column].astype("string").str.strip()
        frame["_trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
        prior = frame.loc[
            frame["_trade_date"].eq(decision_gold.session)
            & frame["_ticker"].isin(set(map(str, current["_ticker"])))
        ][["_ticker", "close_adj"]].copy()
        today = current[["_ticker", "close_adj"]].copy()
        if prior.duplicated("_ticker").any() or len(prior) != len(today):
            raise ExecutionEvidenceConflict("benchmark lacks complete prior-session closes")
        merged = today.merge(
            prior,
            on="_ticker",
            suffixes=("_today", "_prior"),
            validate="one_to_one",
        )
        current_close = pd.to_numeric(merged["close_adj_today"], errors="coerce")
        prior_close = pd.to_numeric(merged["close_adj_prior"], errors="coerce")
        if (
            current_close.isna().any()
            or prior_close.isna().any()
            or current_close.le(0).any()
            or prior_close.le(0).any()
        ):
            raise ExecutionEvidenceConflict("benchmark adjusted closes are incomplete")
        value = float((current_close / prior_close - 1.0).mean())
        if not math.isfinite(value):
            raise ExecutionEvidenceConflict("benchmark return is not finite")
        return value

    def _decision_execution_risk(
        self,
        evidence: _GoldEvidence,
        decision_universe: pd.DataFrame,
        *,
        session: date,
    ) -> pd.DataFrame:
        """Bind risk/fallback prices to the immutable prior-close snapshot."""

        if evidence.session >= session:
            raise ExecutionEvidenceConflict(
                "decision risk snapshot is not prior to the execution session"
            )
        required = {
            "adv_20",
            "volatility_20",
            "close",
            "daily_available_at",
            "adj_factor_available_at",
        }
        missing = sorted(required - set(decision_universe.columns))
        if missing:
            raise ExecutionEvidenceUnavailable(
                f"decision-snapshot execution risk omits fields: {missing}"
            )
        prior = decision_universe[
            [
                "_ticker",
                "adv_20",
                "volatility_20",
                "close",
                "daily_available_at",
                "adj_factor_available_at",
            ]
        ].copy()
        if (
            prior.duplicated("_ticker").any()
            or len(prior) != self.policy.target_universe_size
        ):
            raise ExecutionEvidenceConflict(
                "decision-snapshot ADV/volatility does not cover the decision universe"
            )
        adv = pd.to_numeric(prior["adv_20"], errors="coerce")
        volatility = pd.to_numeric(prior["volatility_20"], errors="coerce")
        fallback_open = pd.to_numeric(prior["close"], errors="coerce")
        if (
            adv.isna().any()
            or adv.le(0).any()
            or volatility.isna().any()
            or volatility.lt(0).any()
            or fallback_open.isna().any()
            or fallback_open.le(0).any()
        ):
            raise ExecutionEvidenceConflict("prior-session ADV/volatility is invalid")
        available = pd.concat(
            [
                pd.to_datetime(
                    prior["daily_available_at"], errors="coerce", utc=True
                ).rename("daily"),
                pd.to_datetime(
                    prior["adj_factor_available_at"], errors="coerce", utc=True
                ).rename("adjustment"),
            ],
            axis=1,
        ).max(axis=1)
        open_cutoff = _utc_timestamp(session, self.policy.execution_local_time)
        if available.isna().any() or available.gt(open_cutoff).any():
            raise ExecutionEvidenceConflict(
                "prior-session execution risk was unavailable by 09:30"
            )
        return pd.DataFrame(
            {
                "_ticker": prior["_ticker"].astype(str),
                "execution_adv_20": adv,
                "execution_volatility_20": volatility,
                "suspended_fallback_open": fallback_open,
                "risk_available_at": available,
            }
        )

    def _compose_frames(
        self,
        *,
        session: date,
        gold: _GoldEvidence,
        decision_gold: _GoldEvidence,
        opening: _OpenEvidence,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, float]:
        if (
            opening.decision_session != decision_gold.session
            or opening.decision_snapshot_id != decision_gold.reference.snapshot_id
        ):
            raise ExecutionEvidenceConflict(
                "opening decision authority changed before evening closure"
            )
        decision_universe = self._current_universe(
            decision_gold, decision_gold.session
        )
        current = self._session_rows_for_tickers(
            gold,
            session=session,
            tickers=tuple(map(str, decision_universe["_ticker"])),
        )
        status = self._validate_execution_status(current, session=session)
        risk = self._decision_execution_risk(
            decision_gold, decision_universe, session=session
        )
        opening_frame = opening.frame.copy(deep=True)
        assert_point_in_time_columns(list(opening_frame.columns))
        required_open = {
            "ticker",
            "trade_date",
            "source_event_time",
            "source_available_at",
            "open_raw",
            "observation_status",
        }
        missing_open = sorted(required_open - set(opening_frame.columns))
        if missing_open:
            raise ExecutionEvidenceUnavailable(
                f"opening observation omits fields: {missing_open}"
            )
        if opening_frame.duplicated(["ticker", "trade_date"]).any():
            raise ExecutionEvidenceConflict("opening observation has duplicate ticker/date")
        normalized_status_columns = {
            "up_limit",
            "down_limit",
            "is_suspended",
            "is_delisted",
            "status_available_at",
            "delist_available_at",
            "adv_20",
            "volatility_20",
        }
        current_values = current.drop(
            columns=[
                name
                for name in normalized_status_columns
                if name in current.columns
            ],
            errors="ignore",
        )
        joined = current_values.merge(
            status,
            on="_ticker",
            validate="one_to_one",
        ).merge(
            risk,
            on="_ticker",
            validate="one_to_one",
        ).merge(
            opening_frame,
            left_on="_ticker",
            right_on="ticker",
            validate="one_to_one",
        )
        coverage = float(len(joined)) / float(len(current))
        if coverage < self.policy.minimum_coverage or len(joined) != len(current):
            raise ExecutionEvidenceUnavailable("09:30 opening coverage is incomplete")
        observed_session = pd.to_datetime(joined["trade_date_y"], errors="coerce").dt.date
        if observed_session.isna().any() or not observed_session.eq(session).all():
            raise ExecutionEvidenceConflict("opening rows belong to another session")
        open_cutoff = _utc_timestamp(session, self.policy.execution_local_time)
        observation_deadline = open_cutoff + pd.Timedelta(
            minutes=self.policy.observation_deadline_minutes
        )
        source_event = pd.to_datetime(joined["source_event_time"], errors="coerce", utc=True)
        source_available = pd.to_datetime(
            joined["source_available_at"], errors="coerce", utc=True
        )
        if (
            source_event.isna().any()
            or source_available.isna().any()
            or not source_event.eq(open_cutoff).all()
            or source_available.lt(source_event).any()
            or source_available.gt(observation_deadline).any()
        ):
            raise ExecutionEvidenceConflict(
                "opening event/availability leaks after 09:30 live observation window"
            )
        active = ~joined["is_suspended"].astype(bool)
        status_values = joined["observation_status"].astype(str)
        if not status_values.loc[active].eq("observed_0930").all():
            raise ExecutionEvidenceUnavailable("active security lacks a 09:30 observation")
        if not status_values.loc[~active].eq("unobserved_0930").all():
            raise ExecutionEvidenceConflict("suspension and opening observations conflict")
        open_raw = pd.to_numeric(joined["open_raw"], errors="coerce")
        if open_raw.loc[active].isna().any() or open_raw.loc[active].le(0).any():
            raise ExecutionEvidenceConflict("active opening prices are invalid")
        # Formal fills are reconciled against the independently closed daily
        # endpoint.  This does not make the EOD row timely enough to execute;
        # it is an evening integrity cross-check of the live observation.
        if opening.physical_source_attested and "open" not in joined:
            raise ExecutionEvidenceUnavailable(
                "formal opening evidence lacks the closed daily open cross-check"
            )
        if "open" in joined:
            closed_open = pd.to_numeric(joined["open"], errors="coerce")
            if (
                closed_open.loc[active].isna().any()
                or closed_open.loc[active].le(0).any()
                or (closed_open.loc[active] - open_raw.loc[active]).abs().gt(0.01).any()
            ):
                raise ExecutionEvidenceConflict(
                    "live and closed opening price sources conflict above one tick"
                )

        up = pd.to_numeric(joined["up_limit"], errors="coerce")
        down = pd.to_numeric(joined["down_limit"], errors="coerce")
        # Vectorized, tight monetary tolerance; both flags can never be true
        # after the accepted limit-bound conflict check.
        is_up = active & (open_raw - up).abs().le(1e-8)
        is_down = active & (open_raw - down).abs().le(1e-8)
        if (is_up & is_down).any():
            raise ExecutionEvidenceConflict("opening price is both upper and lower limit")

        for name in ("execution_adv_20", "execution_volatility_20", "close", "close_adj"):
            if name not in joined:
                raise ExecutionEvidenceUnavailable(f"Gold mark/risk evidence omits {name}")
        adv = pd.to_numeric(joined["execution_adv_20"], errors="coerce")
        volatility = pd.to_numeric(
            joined["execution_volatility_20"], errors="coerce"
        )
        close_raw = pd.to_numeric(joined["close"], errors="coerce")
        if (
            adv.isna().any()
            or adv.le(0).any()
            or volatility.isna().any()
            or volatility.lt(0).any()
            or close_raw.isna().any()
            or close_raw.le(0).any()
        ):
            raise ExecutionEvidenceConflict("Gold risk/closing mark values are invalid")

        action_present = (
            joined["has_company_action"].astype("boolean").fillna(False)
            if "has_company_action" in joined
            else pd.Series(False, index=joined.index)
        )
        if action_present.any() and not ({"split_ratio", "stk_div"} & set(joined)):
            raise ExecutionEvidenceUnavailable(
                "accepted company action lacks a split-ratio field"
            )
        if action_present.any() and not ({"cash_dividend", "cash_div"} & set(joined)):
            raise ExecutionEvidenceUnavailable(
                "accepted company action lacks a cash-dividend field"
            )
        if "split_ratio" in joined:
            split_ratio = pd.to_numeric(joined["split_ratio"], errors="coerce")
        elif "stk_div" in joined:
            split_ratio = 1.0 + pd.to_numeric(joined["stk_div"], errors="coerce").fillna(0.0)
        else:
            split_ratio = pd.Series(1.0, index=joined.index)
        if "cash_dividend" in joined:
            cash_dividend = pd.to_numeric(joined["cash_dividend"], errors="coerce")
        elif "cash_div" in joined:
            cash_dividend = pd.to_numeric(joined["cash_div"], errors="coerce").fillna(0.0)
        else:
            cash_dividend = pd.Series(0.0, index=joined.index)
        if (
            split_ratio.isna().any()
            or split_ratio.le(0).any()
            or cash_dividend.isna().any()
            or cash_dividend.lt(0).any()
        ):
            raise ExecutionEvidenceConflict("company action values are invalid")
        if action_present.any():
            action_available = _timestamp_column(joined, "company_action_available_at")
            if action_available.loc[action_present].isna().any() or action_available.loc[
                action_present
            ].gt(open_cutoff).any():
                raise ExecutionEvidenceConflict(
                    "company action was not known by the opening execution cutoff"
                )

        mark_event = _utc_timestamp(session, self.policy.mark_local_time)
        mark_available = pd.Series(
            pd.Timestamp(gold.reference.as_of), index=joined.index, dtype="datetime64[ns, UTC]"
        )
        if "daily_available_at" in joined:
            daily_available = pd.to_datetime(
                joined["daily_available_at"], errors="coerce", utc=True
            )
            if daily_available.isna().any():
                raise ExecutionEvidenceConflict("daily closing mark availability is missing")
            mark_available = pd.concat(
                [mark_available.rename("gold"), daily_available.rename("daily")], axis=1
            ).max(axis=1)
        if mark_available.lt(mark_event).any() or mark_available.gt(
            pd.Timestamp(gold.reference.as_of)
        ).any():
            raise ExecutionEvidenceConflict("closing mark availability is temporally invalid")

        execution = pd.DataFrame(
            {
                "ticker": joined["_ticker"].astype(str),
                "trade_date": session.isoformat(),
                # ShadowExecutionConfig retains historical *_adj names.  The
                # account ledger uses real cash/quantities, so these values are
                # deliberately raw and company actions are applied separately.
                "open_adj": open_raw.where(
                    active,
                    pd.to_numeric(joined["suspended_fallback_open"], errors="coerce"),
                ),
                "adv_20": adv,
                "volatility_20": volatility,
                "is_one_price_limit_up": is_up.astype(bool),
                "is_one_price_limit_down": is_down.astype(bool),
                "is_suspended": joined["is_suspended"].astype(bool),
                "is_delisted": joined["is_delisted"].astype(bool),
                "execution_event_time": source_event,
                "execution_available_at": pd.concat(
                    [
                        source_available.rename("source"),
                        pd.to_datetime(joined["status_available_at"], utc=True).rename("status"),
                        pd.to_datetime(joined["delist_available_at"], utc=True).rename("delist"),
                        pd.to_datetime(joined["risk_available_at"], utc=True).rename("risk"),
                    ],
                    axis=1,
                ).max(axis=1),
            }
        )
        if execution["execution_available_at"].gt(observation_deadline).any():
            raise ExecutionEvidenceConflict(
                "combined execution state exceeded the live observation window"
            )
        mark = pd.DataFrame(
            {
                "ticker": joined["_ticker"].astype(str),
                "trade_date": session.isoformat(),
                "close_adj": close_raw,
                "split_ratio": split_ratio,
                "cash_dividend": cash_dividend,
                "mark_event_time": mark_event,
                "mark_available_at": mark_available,
            }
        )
        execution = execution.sort_values("ticker").reset_index(drop=True)
        mark = mark.sort_values("ticker").reset_index(drop=True)
        bars = execution.merge(mark, on=["ticker", "trade_date"], validate="one_to_one")
        assert_point_in_time_columns(list(bars.columns))
        if bars.empty or bars.duplicated(["ticker", "trade_date"]).any():
            raise ExecutionEvidenceConflict("typed execution bars are empty or duplicated")
        benchmark_return = self._benchmark_return(
            decision_gold, current, session=session
        )
        bars["benchmark_return"] = benchmark_return
        return execution, mark, bars, benchmark_return

    def _capability(
        self,
        *,
        session: date,
        gold: _GoldEvidence,
        decision_gold: _GoldEvidence,
        opening: _OpenEvidence,
        physical_hashes: Sequence[str],
    ) -> ExecutionCapabilityAssessment:
        reasons: list[str] = []
        if self.runtime_mode != "production":
            reasons.append("non_production_runtime")
        if self._production_binding is None:
            reasons.append("production_factory_not_attested")
        elif not self._production_binding.archive_attested:
            reasons.append("non_minio_archive")
        elif not self._production_binding.real_time_open_capable:
            reasons.append("historical_minute_history_not_realtime_execution")
        if not self.policy.is_formal_default:
            reasons.append("non_formal_policy")
        if not gold.production_attested:
            reasons.append("non_production_gold_reader")
        if not opening.physical_source_attested:
            reasons.append("opening_source_not_physically_attested")
        rotation = self._rotation_attestation()
        if rotation is None or rotation.evidence_hash != opening.rotation_evidence_hash:
            reasons.append("credential_rotation_not_persisted_or_changed")
        if opening.reference.quality_status is not DataQualityStatus.ACCEPTED:
            reasons.append("opening_snapshot_not_accepted")
        if any(not _HASH.fullmatch(str(item)) for item in physical_hashes):
            reasons.append("physical_object_hash_invalid")
        decision = (
            ExecutionCapabilityDecision.ACCEPTED
            if not reasons
            else ExecutionCapabilityDecision.NON_FORWARD
        )
        evidence_hash = content_fingerprint(
            {
                "schema_version": SCHEMA_VERSION,
                "trade_date": session,
                "decision_trade_date": decision_gold.session,
                "decision_snapshot_id": decision_gold.reference.snapshot_id,
                "decision_snapshot_hash": decision_gold.reference.content_hash,
                "decision_gold_partition_hash": (
                    decision_gold.gold_partition.output_hash
                ),
                "decision_data_quality_partition_hash": (
                    decision_gold.data_quality.output_hash
                ),
                "gold_snapshot_id": gold.reference.snapshot_id,
                "gold_partition_hash": gold.gold_partition.output_hash,
                "dq_partition_hash": gold.data_quality.output_hash,
                "open_snapshot_id": opening.reference.snapshot_id,
                "open_partition_hash": opening.partition.output_hash,
                "physical_hashes": sorted(physical_hashes),
                "production_configuration_hash": self.production_configuration_hash,
                "decision": decision.value,
                "reasons": sorted(reasons),
            },
            domain="factor-lab/research-os/v1/execution-capability-assessment",
        )
        return ExecutionCapabilityAssessment(decision, tuple(sorted(reasons)), evidence_hash)

    def _session_details(
        self,
        *,
        session: date,
        decision_gold: _GoldEvidence,
        execution: DataSnapshotRef,
        mark: DataSnapshotRef,
        bundle: DataSnapshotRef,
        benchmark_return: float,
        capability: ExecutionCapabilityAssessment,
    ) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "trade_date": session.isoformat(),
            "open_source_id": self.open_source_id,
            "open_source_role": self.open_source_role,
            "decision_trade_date": decision_gold.session.isoformat(),
            "decision_snapshot_id": decision_gold.reference.snapshot_id,
            "decision_snapshot_hash": decision_gold.reference.content_hash,
            "decision_gold_partition_hash": decision_gold.gold_partition.output_hash,
            "decision_data_quality_partition_hash": (
                decision_gold.data_quality.output_hash
            ),
            "execution_snapshot_id": execution.snapshot_id,
            "execution_snapshot_hash": execution.content_hash,
            "mark_snapshot_id": mark.snapshot_id,
            "mark_snapshot_hash": mark.content_hash,
            "bundle_snapshot_id": bundle.snapshot_id,
            "bundle_snapshot_hash": bundle.content_hash,
            "benchmark_return": float(benchmark_return),
            "capability": {
                "decision": capability.decision.value,
                "reasons": list(capability.reasons),
                "evidence_hash": capability.evidence_hash,
            },
        }

    def _formal_capability_detail(
        self,
        *,
        session: date,
        gold: _GoldEvidence,
        decision_gold: _GoldEvidence,
        opening: _OpenEvidence,
        execution: DataSnapshotRef,
        mark: DataSnapshotRef,
        bundle: DataSnapshotRef,
        typed_partition_hash: str,
        capability: ExecutionCapabilityAssessment,
    ) -> dict[str, Any]:
        binding = self._production_binding
        probe = opening.reference.manifest.get("probe")
        if not isinstance(probe, Mapping):
            raise ExecutionEvidenceConflict("opening snapshot has no typed provider probe")
        source_probe_hash = content_fingerprint(
            dict(probe),
            domain=(
                f"factor-lab/research-os/v1/{self.open_source_id}-open-probe"
            ),
        )
        realtime_open = bool(
            binding is not None and binding.real_time_open_capable
        )
        physical_hashes: dict[str, str] = {}
        for role, reference in (
            ("source", opening.reference),
            ("execution", execution),
            ("mark", mark),
            ("bundle", bundle),
        ):
            physical = reference.manifest.get("physical_object")
            if not isinstance(physical, Mapping) or not _HASH.fullmatch(
                str(physical.get("sha256") or "")
            ):
                raise ExecutionEvidenceConflict(
                    f"{role} capability lacks physical object integrity"
                )
            physical_hashes[role] = str(physical["sha256"])
        return {
            "schema_version": FORMAL_EXECUTION_CAPABILITY_SCHEMA_VERSION,
            "authority_schema_version": SCHEMA_VERSION,
            "decision": capability.decision.value,
            "reasons": list(capability.reasons),
            "formal_shadow_projection": (
                "allowed" if capability.accepted else "blocked"
            ),
            "real_source_probe": bool(
                realtime_open
                and
                opening.physical_source_attested
                and str(probe.get("probe_kind") or "")
                == "session_bound_0930_fetch"
            ),
            "physical": bool(
                binding is not None
                and binding.archive_attested
                and all(_HASH.fullmatch(value) for value in physical_hashes.values())
            ),
            "point_in_time": True,
            "event_semantics": (
                "official_realtime_current_session_1min_open_09_30"
                if realtime_open
                else "historical_minute_response_non_forward"
            ),
            "collection_mode": (
                "realtime_open" if realtime_open else "historical_query"
            ),
            # Tushare returns the provider event time but no signed server
            # availability timestamp.  Claiming otherwise would recreate the
            # historical-data loophole this authority closes.
            "server_available_at_verified": False,
            "provider_event_time_verified": bool(
                realtime_open and opening.physical_source_attested
            ),
            "collector_received_at_verified": bool(
                realtime_open and opening.physical_source_attested
            ),
            "collector_clock_verified_against_postgresql": bool(
                opening.partition.details.get("collector_clock_verified")
            ),
            "database_received_at": opening.partition.details.get(
                "database_received_at"
            ),
            "tradability_state_verified": bool(
                realtime_open and opening.physical_source_attested
            ),
            "ingested_within_cutoff": bool(
                realtime_open and opening.physical_source_attested
            ),
            "observed_local_time": "09:30:00",
            "observation_deadline_local_time": "09:35:00",
            "timezone": "Asia/Shanghai",
            "event_time_field": "trade_time",
            "available_at_field": (
                "collector_ingested_at" if realtime_open else "ingested_at"
            ),
            "price_field": "open",
            "open_price_reconciled_with_closed_daily": bool(
                realtime_open and opening.physical_source_attested
            ),
            "complete_observed_universe": bool(
                opening.partition.details.get("complete_observed_universe")
            ),
            # Missing provider rows remain explicit and are admitted only
            # after _build_frames reconciles every one against the independent
            # accepted execution-Gold suspension status.  No price is inferred
            # from this fact; suspended rows keep the prior-close fallback.
            "complete_tradable_universe_observed": True,
            "missing_open_explained_by_suspension": bool(
                not opening.partition.details.get("complete_observed_universe")
            ),
            "complete_execution_universe_accounted": True,
            "mark_semantics": "accepted_gold_close_snapshot",
            "execution_mark_roles_separate": (
                execution.snapshot_id != mark.snapshot_id
            ),
            "trade_date": session.isoformat(),
            "decision_trade_date": decision_gold.session.isoformat(),
            "decision_snapshot_id": decision_gold.reference.snapshot_id,
            "decision_snapshot_hash": decision_gold.reference.content_hash,
            "decision_gold_partition_hash": (
                decision_gold.gold_partition.output_hash
            ),
            "decision_data_quality_partition_hash": (
                decision_gold.data_quality.output_hash
            ),
            "production_configuration_hash": self.production_configuration_hash,
            "source_contract_hash": (
                None if binding is None else binding.execution_contract_hash
            ),
            "open_source_id": self.open_source_id,
            "open_source_role": self.open_source_role,
            "provider_endpoint": (
                None if binding is None else binding.provider_endpoint
            ),
            "source_snapshot_id": opening.reference.snapshot_id,
            "source_snapshot_hash": opening.reference.content_hash,
            "source_partition_hash": opening.partition.output_hash,
            "source_probe_hash": source_probe_hash,
            "gold_snapshot_id": gold.reference.snapshot_id,
            "gold_snapshot_hash": gold.reference.content_hash,
            "gold_partition_hash": gold.gold_partition.output_hash,
            "data_quality_partition_hash": gold.data_quality.output_hash,
            "execution_snapshot_id": execution.snapshot_id,
            "execution_snapshot_hash": execution.content_hash,
            "mark_snapshot_id": mark.snapshot_id,
            "mark_snapshot_hash": mark.content_hash,
            "bundle_snapshot_id": bundle.snapshot_id,
            "bundle_snapshot_hash": bundle.content_hash,
            "typed_partition_hash": typed_partition_hash,
            "physical_object_hashes": physical_hashes,
            "capability_evidence_hash": capability.evidence_hash,
        }

    def _load_completed_session(self, session: date) -> TypedExecutionSession:
        partition = self._require_partition(
            source_id="research_os", dataset=OUTPUT_DATASET, session=session
        )
        details = dict(partition.details)
        expected = content_fingerprint(
            details, domain="factor-lab/research-os/v1/typed-execution-partition-result"
        )
        if expected != partition.output_hash:
            raise ExecutionEvidenceConflict("typed execution partition hash is invalid")
        decision_gold = self._decision_gold_evidence(session)
        expected_decision = {
            "decision_trade_date": decision_gold.session.isoformat(),
            "decision_snapshot_id": decision_gold.reference.snapshot_id,
            "decision_snapshot_hash": decision_gold.reference.content_hash,
            "decision_gold_partition_hash": decision_gold.gold_partition.output_hash,
            "decision_data_quality_partition_hash": (
                decision_gold.data_quality.output_hash
            ),
        }
        if any(
            str(details.get(key) or "") != str(value or "")
            for key, value in expected_decision.items()
        ):
            raise ExecutionEvidenceConflict(
                "typed execution partition changed its decision authority"
            )
        ids = {
            role: str(details.get(f"{role}_snapshot_id") or "")
            for role in ("execution", "mark", "bundle")
        }
        if not all(ids.values()) or len(set(ids.values())) != 3:
            raise ExecutionEvidenceConflict("typed execution snapshot roles are missing or reused")
        references: dict[str, DataSnapshotRef] = {}
        for role, snapshot_id in ids.items():
            record = self.catalog.get_snapshot(snapshot_id)
            if record is None:
                raise ExecutionEvidenceUnavailable(f"{role} snapshot is not cataloged")
            expected_role = BUNDLE_ROLE if role == "bundle" else role
            if str(record.reference.manifest.get("role") or "") != expected_role:
                raise ExecutionEvidenceConflict(f"{role} snapshot role binding is invalid")
            if record.reference.content_hash != str(
                details.get(f"{role}_snapshot_hash") or ""
            ):
                raise ExecutionEvidenceConflict(f"{role} snapshot hash changed")
            references[role] = record.reference
        execution_frame = self._load_ref_frame(
            references["execution"], expected_role="execution"
        )
        mark_frame = self._load_ref_frame(references["mark"], expected_role="mark")
        bundle_frame = self._load_ref_frame(
            references["bundle"], expected_role=BUNDLE_ROLE
        )
        recomposed = execution_frame.merge(
            mark_frame, on=["ticker", "trade_date"], validate="one_to_one"
        )
        benchmark = float(details.get("benchmark_return", float("nan")))
        recomposed["benchmark_return"] = benchmark
        recomposed["execution_snapshot_id"] = references["execution"].snapshot_id
        recomposed["mark_snapshot_id"] = references["mark"].snapshot_id
        if list(recomposed.columns) != list(bundle_frame.columns) or not recomposed.equals(
            bundle_frame
        ):
            raise ExecutionEvidenceConflict("typed bundle differs from execution+mark roles")
        for column, expected_id in (
            ("execution_snapshot_id", references["execution"].snapshot_id),
            ("mark_snapshot_id", references["mark"].snapshot_id),
        ):
            observed = set(map(str, bundle_frame[column])) if column in bundle_frame else set()
            if observed != {expected_id}:
                raise ExecutionEvidenceConflict(f"typed bundle has mixed {column} roles")
        capability_value = details.get("capability")
        if not isinstance(capability_value, Mapping):
            raise ExecutionEvidenceConflict("typed execution capability is absent")
        capability = ExecutionCapabilityAssessment(
            decision=ExecutionCapabilityDecision(str(capability_value.get("decision"))),
            reasons=tuple(map(str, capability_value.get("reasons") or ())),
            evidence_hash=str(capability_value.get("evidence_hash") or ""),
        )
        if not _HASH.fullmatch(capability.evidence_hash):
            raise ExecutionEvidenceConflict("typed execution capability hash is invalid")
        accepted = capability.accepted
        expected_quality = (
            DataQualityStatus.ACCEPTED if accepted else DataQualityStatus.QUARANTINED
        )
        if any(
            ref.quality_status is not expected_quality
            for ref in references.values()
        ):
            raise ExecutionEvidenceConflict("snapshot quality disagrees with capability")
        if accepted:
            gold = self._gold_evidence(session)
            opening = self._load_open_evidence(session)
            physical_hashes = tuple(
                str(
                    (references[role].manifest.get("physical_object") or {}).get(
                        "sha256"
                    )
                    or ""
                )
                for role in ("execution", "mark", "bundle")
            )
            recomputed_capability = self._capability(
                session=session,
                gold=gold,
                decision_gold=decision_gold,
                opening=opening,
                physical_hashes=physical_hashes,
            )
            if recomputed_capability != capability:
                raise ExecutionEvidenceConflict(
                    "accepted typed capability no longer matches current authorities"
                )
        self._validate_typed_bars(
            bundle_frame,
            session=session,
            execution_snapshot_id=references["execution"].snapshot_id,
            mark_snapshot_id=references["mark"].snapshot_id,
        )
        return TypedExecutionSession(
            trade_date=session,
            bars=bundle_frame,
            benchmark_return=benchmark,
            execution_snapshot=references["execution"],
            mark_snapshot=references["mark"],
            bundle_snapshot=references["bundle"],
            capability=capability,
            reused=True,
        )

    def _validate_typed_bars(
        self,
        bars: pd.DataFrame,
        *,
        session: date,
        execution_snapshot_id: str,
        mark_snapshot_id: str,
    ) -> None:
        assert_point_in_time_columns(list(bars.columns))
        required = set(FORMAL_EXECUTION_REQUIRED_FIELDS)
        missing = sorted(required - set(bars.columns))
        if missing:
            raise ExecutionEvidenceUnavailable(f"typed bars omit fields: {missing}")
        extra = sorted(set(bars.columns) - required)
        if extra or len(bars.columns) != len(required):
            raise ExecutionEvidenceConflict(
                "typed bars must match the versioned execution schema exactly; "
                f"unexpected or duplicate fields: {extra}"
            )
        if bars.empty or bars.duplicated(["ticker", "trade_date"]).any():
            raise ExecutionEvidenceConflict("typed bars are empty or duplicated")
        if set(pd.to_datetime(bars["trade_date"], errors="coerce").dt.date) != {session}:
            raise ExecutionEvidenceConflict("typed bars contain another session")
        if set(map(str, bars["execution_snapshot_id"])) != {execution_snapshot_id}:
            raise ExecutionEvidenceConflict("typed bars mix execution snapshot roles")
        if set(map(str, bars["mark_snapshot_id"])) != {mark_snapshot_id}:
            raise ExecutionEvidenceConflict("typed bars mix mark snapshot roles")
        if execution_snapshot_id == mark_snapshot_id:
            raise ExecutionEvidenceConflict("execution and mark snapshot roles are identical")
        open_cutoff = _utc_timestamp(session, self.policy.execution_local_time)
        observation_deadline = open_cutoff + pd.Timedelta(
            minutes=self.policy.observation_deadline_minutes
        )
        mark_cutoff = _utc_timestamp(session, self.policy.mark_local_time)
        event = pd.to_datetime(bars["execution_event_time"], errors="coerce", utc=True)
        available = pd.to_datetime(
            bars["execution_available_at"], errors="coerce", utc=True
        )
        mark_event = pd.to_datetime(bars["mark_event_time"], errors="coerce", utc=True)
        mark_available = pd.to_datetime(bars["mark_available_at"], errors="coerce", utc=True)
        if (
            event.isna().any()
            or available.isna().any()
            or not event.eq(open_cutoff).all()
            or available.lt(event).any()
            or available.gt(observation_deadline).any()
            or mark_event.isna().any()
            or not mark_event.eq(mark_cutoff).all()
            or mark_available.isna().any()
            or mark_available.lt(mark_event).any()
        ):
            raise ExecutionEvidenceConflict("typed bars violate execution/mark time cutoffs")
        if (
            bars["is_one_price_limit_up"].astype(bool)
            & bars["is_one_price_limit_down"].astype(bool)
        ).any():
            raise ExecutionEvidenceConflict("typed bars contain conflicting limit states")

    def build_session(self, trade_date: date | str) -> TypedExecutionSession:
        """Build or verify one deterministic typed execution session."""

        session = _session(trade_date)
        identity = PartitionIdentity("research_os", OUTPUT_DATASET, session.isoformat())
        existing = self.ledger.get_partition(identity)
        if existing is not None and existing.status is PartitionStatus.SUCCEEDED:
            return self._load_completed_session(session)
        self._require_live_closure_window(session)
        decision_gold = self._decision_gold_evidence(session)
        gold = self._gold_evidence(session)
        opening = self._load_open_evidence(session)
        input_hash = content_fingerprint(
            {
                "schema_version": SCHEMA_VERSION,
                "trade_date": session,
                "decision_trade_date": decision_gold.session,
                "decision_snapshot_id": decision_gold.reference.snapshot_id,
                "decision_snapshot_hash": decision_gold.reference.content_hash,
                "decision_gold_partition_hash": (
                    decision_gold.gold_partition.output_hash
                ),
                "decision_data_quality_partition_hash": (
                    decision_gold.data_quality.output_hash
                ),
                "gold_snapshot_id": gold.reference.snapshot_id,
                "gold_partition_hash": gold.gold_partition.output_hash,
                "dq_partition_hash": gold.data_quality.output_hash,
                "open_snapshot_id": opening.reference.snapshot_id,
                "open_partition_hash": opening.partition.output_hash,
                "policy": self.policy.fingerprint_payload(),
            },
            domain="factor-lab/research-os/v1/typed-execution-partition-input",
        )
        self.ledger.ensure_partition(
            identity,
            created_at=_aware(self._now(), name="now"),
            input_hash=input_hash,
            details={"authority": SCHEMA_VERSION},
        )
        lease = self.ledger.claim(
            owner=self.worker_id,
            identity=identity,
            now=_aware(self._now(), name="now"),
            lease_for=timedelta(minutes=self.policy.lease_minutes),
            maximum_attempts=self.policy.maximum_attempts,
        )
        if lease is None:
            raise ExecutionLeaseBusy("typed execution partition is already leased")
        success_committed = False
        try:
            execution_frame, mark_frame, bars, benchmark_return = self._compose_frames(
                session=session,
                gold=gold,
                decision_gold=decision_gold,
                opening=opening,
            )
            execution_archive = self._write_archive(
                execution_frame, session=session, role="execution"
            )
            mark_archive = self._write_archive(mark_frame, session=session, role="mark")
            # Capability is evaluated only after every source and output object
            # has survived an archive+restore round trip.
            preliminary = self._capability(
                session=session,
                gold=gold,
                decision_gold=decision_gold,
                opening=opening,
                physical_hashes=(execution_archive.sha256, mark_archive.sha256),
            )
            evidence = {
                "decision_trade_date": decision_gold.session.isoformat(),
                "decision_snapshot_id": decision_gold.reference.snapshot_id,
                "decision_snapshot_hash": decision_gold.reference.content_hash,
                "decision_gold_partition_hash": (
                    decision_gold.gold_partition.output_hash
                ),
                "decision_data_quality_partition_hash": (
                    decision_gold.data_quality.output_hash
                ),
                "gold_snapshot_id": gold.reference.snapshot_id,
                "gold_snapshot_hash": gold.reference.content_hash,
                "gold_partition_hash": gold.gold_partition.output_hash,
                "data_quality_partition_hash": gold.data_quality.output_hash,
                "opening_snapshot_id": opening.reference.snapshot_id,
                "opening_snapshot_hash": opening.reference.content_hash,
                "opening_partition_hash": opening.partition.output_hash,
                "capability_evidence_hash": preliminary.evidence_hash,
            }
            execution_ref = self._register_role_snapshot(
                role="execution",
                session=session,
                frame=execution_frame,
                archived=execution_archive,
                parent_snapshot_ids=(
                    decision_gold.reference.snapshot_id,
                    gold.reference.snapshot_id,
                    opening.reference.snapshot_id,
                ),
                as_of=max(
                    pd.to_datetime(
                        execution_frame["execution_available_at"], utc=True
                    )
                ).to_pydatetime(),
                calendar=gold.calendar,
                capability=preliminary.decision,
                evidence=evidence,
            )
            mark_as_of = max(
                pd.to_datetime(mark_frame["mark_available_at"], utc=True)
            ).to_pydatetime()
            mark_ref = self._register_role_snapshot(
                role="mark",
                session=session,
                frame=mark_frame,
                archived=mark_archive,
                parent_snapshot_ids=(gold.reference.snapshot_id,),
                as_of=mark_as_of,
                calendar=gold.calendar,
                capability=preliminary.decision,
                evidence=evidence,
            )
            if execution_ref.snapshot_id == mark_ref.snapshot_id:
                raise ExecutionEvidenceConflict("role-specific snapshot ids collided")
            bars["execution_snapshot_id"] = execution_ref.snapshot_id
            bars["mark_snapshot_id"] = mark_ref.snapshot_id
            self._validate_typed_bars(
                bars,
                session=session,
                execution_snapshot_id=execution_ref.snapshot_id,
                mark_snapshot_id=mark_ref.snapshot_id,
            )
            bundle_archive = self._write_archive(
                bars, session=session, role=BUNDLE_ROLE
            )
            # Bind the bundle object into the final capability evidence too.
            capability = self._capability(
                session=session,
                gold=gold,
                decision_gold=decision_gold,
                opening=opening,
                physical_hashes=(
                    execution_archive.sha256,
                    mark_archive.sha256,
                    bundle_archive.sha256,
                ),
            )
            if capability.decision is not preliminary.decision:
                raise ExecutionEvidenceConflict("capability changed during immutable publication")
            bundle_evidence = {**evidence, "capability_evidence_hash": capability.evidence_hash}
            bundle_ref = self._register_role_snapshot(
                role=BUNDLE_ROLE,
                session=session,
                frame=bars,
                archived=bundle_archive,
                parent_snapshot_ids=(execution_ref.snapshot_id, mark_ref.snapshot_id),
                as_of=mark_as_of,
                calendar=gold.calendar,
                capability=capability.decision,
                evidence=bundle_evidence,
            )
            # Return bytes that have just been restored from MinIO, never the
            # transformation's in-memory frame.
            restored_bars = self._load_ref_frame(bundle_ref, expected_role=BUNDLE_ROLE)
            details = self._session_details(
                session=session,
                decision_gold=decision_gold,
                execution=execution_ref,
                mark=mark_ref,
                bundle=bundle_ref,
                benchmark_return=benchmark_return,
                capability=capability,
            )
            output_hash = content_fingerprint(
                details,
                domain="factor-lab/research-os/v1/typed-execution-partition-result",
            )
            formal_detail = self._formal_capability_detail(
                session=session,
                gold=gold,
                decision_gold=decision_gold,
                opening=opening,
                execution=execution_ref,
                mark=mark_ref,
                bundle=bundle_ref,
                typed_partition_hash=output_hash,
                capability=capability,
            )
            formal_fields = tuple(map(str, restored_bars.columns))
            formal_probe_hash = formal_execution_capability_probe_hash(
                source_id=FORMAL_EXECUTION_SOURCE_ID,
                dataset=CAPABILITY_DATASET,
                contract_hash=_OUTPUT_CONTRACT_HASH,
                fields=formal_fields,
                detail=formal_detail,
            )
            self.ledger.upsert_capability(
                CapabilityRecord(
                    source_id=FORMAL_EXECUTION_SOURCE_ID,
                    dataset=CAPABILITY_DATASET,
                    status=(
                        CapabilityStatus.ACCEPTED
                        if capability.accepted
                        else CapabilityStatus.DEGRADED
                    ),
                    contract_hash=_OUTPUT_CONTRACT_HASH,
                    fields=formal_fields,
                    detail=json.dumps(
                        formal_detail,
                        ensure_ascii=True,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    probed_at=_aware(self._now(), name="now"),
                    probe_hash=formal_probe_hash,
                )
            )
            # A formal capability row is required readiness evidence.  Persist
            # it before making the immutable output partition successful so a
            # database failure cannot create a reusable but unauthorised
            # execution session.
            self.ledger.finish(
                lease,
                status=PartitionStatus.SUCCEEDED,
                completed_at=_aware(self._now(), name="now"),
                output_snapshot_id=bundle_ref.snapshot_id,
                output_hash=output_hash,
                details=details,
            )
            success_committed = True
            return TypedExecutionSession(
                trade_date=session,
                bars=restored_bars,
                benchmark_return=benchmark_return,
                execution_snapshot=execution_ref,
                mark_snapshot=mark_ref,
                bundle_snapshot=bundle_ref,
                capability=capability,
                reused=False,
            )
        except Exception as exc:
            if not success_committed:
                try:
                    self.ledger.finish(
                        lease,
                        status=PartitionStatus.FAILED,
                        completed_at=_aware(self._now(), name="now"),
                        error_code="typed_execution_rejected",
                        # Never copy arbitrary exception prose into the
                        # durable partition ledger/read model.
                        error=type(exc).__name__,
                    )
                except Exception as cleanup_exc:
                    # finish() may have committed SUCCEEDED and only then lost
                    # its acknowledgement.  Never let cleanup overwrite that
                    # immutable terminal or mask the causal exception.
                    _add_cleanup_note(
                        exc,
                        "typed execution partition failure finalization also failed with "
                        f"{type(cleanup_exc).__name__}",
                    )
            try:
                self.ledger.upsert_capability(
                    CapabilityRecord(
                        source_id=FORMAL_EXECUTION_SOURCE_ID,
                        dataset=CAPABILITY_DATASET,
                        status=CapabilityStatus.UNAVAILABLE,
                        contract_hash=_OUTPUT_CONTRACT_HASH,
                        fields=(),
                        detail=json.dumps(
                            {
                                "schema_version": FORMAL_EXECUTION_CAPABILITY_SCHEMA_VERSION,
                                "authority_schema_version": SCHEMA_VERSION,
                                "decision": ExecutionCapabilityDecision.REJECTED.value,
                                "formal_shadow_projection": "blocked",
                                "error_type": type(exc).__name__,
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        probed_at=_aware(self._now(), name="now"),
                        probe_hash=None,
                    )
                )
            except Exception as cleanup_exc:
                _add_cleanup_note(
                    exc,
                    "formal capability fail-closed update also failed with "
                    f"{type(cleanup_exc).__name__}",
                )
            raise


__all__ = [
    "BUNDLE_ROLE",
    "CAPABILITY_DATASET",
    "CredentialRotationAttestation",
    "DiemengRotationEvidenceAuthority",
    "ExecutionCapabilityAssessment",
    "ExecutionCapabilityDecision",
    "ExecutionEvidenceConflict",
    "ExecutionEvidenceUnavailable",
    "ExecutionLeaseBusy",
    "ExecutionNetworkBlocked",
    "ExecutionSnapshotAuthority",
    "ExecutionSnapshotAuthorityError",
    "ExecutionSnapshotPolicy",
    "FORMAL_EXECUTION_CAPABILITY_SCHEMA_VERSION",
    "FORMAL_EXECUTION_REQUIRED_FIELDS",
    "FORMAL_EXECUTION_SOURCE_ID",
    "OPEN_DATASET",
    "OPEN_SOURCE_DATASET",
    "OUTPUT_DATASET",
    "OUTPUT_CONTRACT_HASH",
    "PyIcebergRegisteredGoldReader",
    "RegisteredGoldSnapshotReader",
    "SCHEMA_VERSION",
    "TypedExecutionSession",
    "formal_execution_capability_probe_hash",
]
