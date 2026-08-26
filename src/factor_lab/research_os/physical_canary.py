"""Physical, restart-safe 50x20 Research OS engineering canary.

This module is intentionally separate from :mod:`engineering_canary`, whose
typed in-memory bundle is useful only as a deterministic plumbing fixture.  A
physical canary selects its sessions from the accepted PostgreSQL calendar,
fetches every observation through reviewed ``SourceAdapter`` contracts, stores
real Bronze/Silver/Gold bytes under an isolated MinIO prefix, and derives daily
shadow authority rows from the event ledger.

Every result remains ``engineering_canary/retrospective_non_forward``.
Historical replay, including replay backed by real provider bytes, is never
formal forward evidence and can never be admitted to an evidence epoch.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
from math import isfinite
import os
from pathlib import Path
import tempfile
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlsplit
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import orm
from .bitemporal import CanonicalizationSpec, canonicalize_batch
from .catalog import CatalogConflict, LifecycleEvent, ResearchCatalog, RunRecord
from .contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    LifecycleState,
    PHYSICAL_CANARY_SNAPSHOT_REFERENCE_SCHEMA,
    SnapshotTier,
)
from .data_sources import (
    AkShareSourceAdapter,
    DatasetContract,
    DiemengSourceAdapter,
    FetchRequest,
    FieldContract,
    ProbeResult,
    SourceAdapter,
    SourceBatch,
    SourceContractError,
    SourceHealth,
    TUSHARE_SEALED_TRANSPORT_POLICY,
    TushareSourceAdapter,
    tushare_client_uses_direct_transport,
    validate_source_frame,
)
from .data_sync import source_adapter_from_mapping
from .docker_attestation import (
    KERNEL_PROCESS_IDENTITY_SCHEME,
    persisted_attestation_binding_errors,
)
from .fingerprint import content_fingerprint
from .execution_open_sources import (
    engineering_canary_execution_contract_hash,
    validate_diemeng_engineering_canary_execution_mapping,
)
from .object_store import ArchivedObject, S3ImmutableArchive
from .production_config import (
    ProductionConfigEvidence,
    ProductionOperation,
    admit_production_operation,
    load_production_config,
    validate_production_config,
)
from .production_ledger import (
    CapabilityRecord,
    CapabilityStatus,
    IncidentRecord,
    IncidentStage,
    IncidentStatus,
    PartitionIdentity,
    PartitionLease,
    PartitionRecord,
    PartitionStatus,
    ProductionLedger,
)
from .reconciliation import (
    RECONCILIATION_EVALUATOR_SCHEMA,
    production_comparison_policies,
    reconcile_observations,
)
from .shadow import ShadowExecutionConfig, ShadowSnapshotBindings, assert_point_in_time_columns
from .shadow_authority import (
    ShadowEvidenceAuthority,
    ShadowEvidenceClass,
    ShadowRole,
    ShadowSessionProjection,
)
from .shadow_catalog import ShadowStepAlreadyApplied, ShadowStepService
from .soak_monitor import DagsterSoakError, local_code_server_runtime_identity


EVIDENCE_SCHEMA = "research-os/physical-engineering-canary/v1"
EVIDENCE_CLASS = "engineering_canary"
EVIDENCE_SCOPE = "retrospective_non_forward"
PHYSICAL_RUN_TYPE = "physical_engineering_canary"
CONTROLLED_TEST_RUN_TYPE = "physical_engineering_canary_test"
CANARY_OBJECT_PREFIX = "research-os/engineering-canary/physical/v1"
SECURITY_COUNT = 50
PROJECTED_SESSION_COUNT = 20
CALENDAR_SESSION_COUNT = PROJECTED_SESSION_COUNT + 1
INITIAL_CAPITAL = 50_000_000.0

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_REAL_ADAPTER_TYPES = (
    TushareSourceAdapter,
    DiemengSourceAdapter,
    AkShareSourceAdapter,
)
_REQUIRED_SINGLE_DATASETS = (
    "daily",
    "adj_factor",
    "historical_st",
    "stock_limit",
    "opening_execution",
)
_CONTROLLED_TEST_MARKER = "physical_canary_controlled_test_adapter"
_EXECUTION_DATASET = "physical_canary_execution"
_EXECUTION_LEASE_FOR = timedelta(hours=24)
_EXECUTION_MAXIMUM_ATTEMPTS = 100
_EXECUTION_RESULT_METADATA = "authoritative_run_metadata"
_EVALUATOR_IDENTITY_SCHEMA = "research-os/physical-canary-evaluator-identity/v1"
_CONTROLLED_TEST_EVALUATOR = "research-os/physical-canary-controlled-test/v1"
_PERSISTED_FRAME_DIGEST_SCHEMA = (
    "research-os/physical-canary-persisted-parquet-frame-digest/v1"
)
_LOGICAL_FRAME_DIGEST_SCHEMA = (
    "research-os/physical-canary-logical-arrow-frame-digest/v1"
)
_SNAPSHOT_FINGERPRINT_DOMAIN = (
    "factor-lab/research-os/v1/physical-canary-snapshot"
)


def physical_canary_generation_source_matches(
    source_identity: str,
    generation_base: str,
) -> bool:
    """Accept exactly one generation family member.

    Production identities historically existed both without a generation
    suffix and with a 12- or 24-character lowercase SHA-256 prefix.  Prefix
    matching is intentionally insufficient: ``engineering_canary_backup`` and
    ``engcan_tushare_old`` are different namespaces, not older generations.
    """

    source = str(source_identity or "")
    base = str(generation_base or "")
    if not source or not base:
        return False
    if source == base[:80]:
        return True
    for generation_length in (12, 24):
        base_budget = 80 - generation_length - 1
        prefix = f"{base[:base_budget]}_"
        generation = source[len(prefix) :] if source.startswith(prefix) else ""
        if len(generation) == generation_length and all(
            character in "0123456789abcdef" for character in generation
        ):
            return True
    return False


def _physical_canary_manifest_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    canonical = parsed.astimezone(timezone.utc)
    return canonical if value == canonical.isoformat() else None


def physical_canary_partition_binding_errors(
    record: PartitionRecord,
    *,
    snapshot_record: Any,
    get_snapshot: Callable[[str], Any],
    audit_now: datetime,
    controlled_test: bool = False,
) -> tuple[str, ...]:
    """Validate one succeeded physical partition as a single strict contract.

    The contract deliberately spans the SQL partition identity, immutable claim
    lineage and catalog snapshot graph.  Both the producer's retry bridge and
    the readiness closure call this function, preventing either side from
    accepting a weaker interpretation of the same evidence.
    """

    errors: list[str] = []
    try:
        now = _aware(audit_now, name="audit now")
    except (TypeError, ValueError):
        return ("physical_partition_audit_time_invalid",)
    completed_at = record.completed_at
    started_at = record.started_at
    if not (
        record.status is PartitionStatus.SUCCEEDED
        and started_at is not None
        and completed_at is not None
        and started_at <= completed_at <= now
        and record.updated_at == completed_at
    ):
        errors.append("physical_partition_timeline_invalid")

    details = record.details
    lineage = details.get("claim_lineage")
    lineage_hash = str(details.get("claim_lineage_hash") or "")
    if not isinstance(lineage, Mapping):
        return tuple(sorted({*errors, "physical_partition_claim_lineage_invalid"}))
    identity = lineage.get("partition_identity")
    stage_lineage = lineage.get("stage_lineage")
    parent_evidence_hashes = lineage.get("parent_evidence_hashes")
    expected_lineage_hash = content_fingerprint(
        dict(lineage),
        domain="factor-lab/research-os/v1/physical-canary-partition-claim-lineage",
    )
    if not (
        isinstance(identity, Mapping)
        and isinstance(stage_lineage, Mapping)
        and isinstance(parent_evidence_hashes, (list, tuple))
        and all(isinstance(item, str) for item in parent_evidence_hashes)
        and tuple(parent_evidence_hashes)
        == tuple(sorted(set(parent_evidence_hashes)))
        and lineage_hash == expected_lineage_hash
        and str(lineage.get("attempted_input_hash") or "")
        == str(record.input_hash or "")
        and identity.get("source_id") == record.identity.source_id
        and identity.get("dataset") == record.identity.dataset
        and identity.get("partition_key") == record.identity.partition_key
        and identity.get("partition_run_id") == record.identity.partition_run_id
    ):
        errors.append("physical_partition_claim_lineage_invalid")

    stage = str(details.get("stage") or "")
    lineage_stage = str(stage_lineage.get("stage") or "")
    expected_incident_stage = {
        "bronze": IncidentStage.SOURCE.value,
        "silver": IncidentStage.SILVER.value,
        "data_quality": IncidentStage.DATA_QUALITY.value,
        "gold": IncidentStage.GOLD.value,
    }.get(stage)
    if not (
        expected_incident_stage
        and lineage_stage == stage
        and lineage.get("incident_stage") == expected_incident_stage
    ):
        errors.append("physical_partition_stage_binding_invalid")

    if snapshot_record is None or not hasattr(snapshot_record, "reference"):
        return tuple(sorted({*errors, "physical_partition_snapshot_missing"}))
    reference = snapshot_record.reference
    manifest = reference.manifest
    if not isinstance(manifest, Mapping):
        return tuple(sorted({*errors, "physical_partition_snapshot_manifest_invalid"}))

    manifest_role = str(manifest.get("role") or "")
    details_role = str(details.get("role") or details.get("gold_role") or "")
    lineage_role = str(
        stage_lineage.get("role") or stage_lineage.get("gold_role") or ""
    )
    expected_tier: SnapshotTier | None = None
    expected_role = ""
    logical_parent_ids: tuple[str, ...] = ()
    if stage == "bronze":
        provider = str(stage_lineage.get("source_id") or "")
        dataset = str(stage_lineage.get("dataset") or "")
        normalized = "".join(
            character if character.isalnum() or character in "_.:-" else "_"
            for character in provider
        )
        expected_source = (
            f"engtest_{normalized}"[:80]
            if controlled_test
            else f"engcan_{normalized}"
        )
        if not (
            provider
            and dataset == record.identity.dataset
            and (
                record.identity.source_id == expected_source
                if controlled_test
                else physical_canary_generation_source_matches(
                    record.identity.source_id, expected_source
                )
            )
        ):
            errors.append("physical_partition_source_namespace_invalid")
        expected_tier = SnapshotTier.BRONZE
        expected_role = f"{provider}_{dataset}"
    elif stage in {"silver", "data_quality", "gold"}:
        expected_stage_source = (
            record.identity.source_id == "engineering_canary_test"
            if controlled_test
            else physical_canary_generation_source_matches(
                record.identity.source_id, "engineering_canary"
            )
        )
        if not expected_stage_source:
            errors.append("physical_partition_source_namespace_invalid")
        if stage == "silver":
            expected_tier = SnapshotTier.SILVER
            expected_role = "accepted_reconciled"
            logical_parent_ids = tuple(reference.parent_snapshot_ids)
            if not (
                record.identity.dataset == "silver_accepted"
                and tuple(map(str, stage_lineage.get("parent_snapshot_ids") or ()))
                == logical_parent_ids
            ):
                errors.append("physical_partition_dataset_parent_binding_invalid")
        elif stage == "data_quality":
            expected_tier = SnapshotTier.SILVER
            expected_role = "accepted_reconciled"
            logical_parent_ids = (str(stage_lineage.get("silver_snapshot_id") or ""),)
            if not (
                record.identity.dataset == "dq_accepted"
                and logical_parent_ids == (reference.snapshot_id,)
            ):
                errors.append("physical_partition_dataset_parent_binding_invalid")
        else:
            role = lineage_role or details_role
            expected_tier = SnapshotTier.GOLD
            expected_role = role
            logical_parent_ids = tuple(reference.parent_snapshot_ids)
            if not (
                role in {"mark", "execution"}
                and record.identity.dataset == f"gold_{role}"
                and len(logical_parent_ids) == 1
                and str(stage_lineage.get("silver_snapshot_id") or "")
                == logical_parent_ids[0]
            ):
                errors.append("physical_partition_dataset_parent_binding_invalid")
    else:
        errors.append("physical_partition_stage_binding_invalid")

    if not (
        expected_tier is not None
        and reference.tier is expected_tier
        and expected_role
        and manifest_role == expected_role
        and (not details_role or details_role == expected_role)
        and (not lineage_role or lineage_role == expected_role)
        and str(manifest.get("run_id") or "") == str(record.run_id or "")
        and str(details.get("run_id") or "") == str(record.run_id or "")
        and str(manifest.get("trade_date") or "")
        == record.identity.partition_key
        and record.output_snapshot_id == reference.snapshot_id
    ):
        errors.append("physical_partition_snapshot_tier_role_invalid")

    published_at = _physical_canary_manifest_time(manifest.get("published_at"))
    reference_as_of = reference.as_of
    if reference_as_of.tzinfo is not None and reference_as_of.utcoffset() is not None:
        reference_as_of = reference_as_of.astimezone(timezone.utc)
    else:
        reference_as_of = None
    try:
        snapshot_created_at = _aware(
            getattr(snapshot_record, "created_at", None),
            name="snapshot created_at",
        )
    except (TypeError, ValueError):
        snapshot_created_at = None
    if not (
        published_at is not None
        and reference_as_of is not None
        and snapshot_created_at is not None
        and completed_at is not None
        and published_at <= snapshot_created_at <= completed_at <= now
        and reference_as_of <= snapshot_created_at
        and published_at <= now
        and reference_as_of <= now
    ):
        errors.append("physical_partition_snapshot_timeline_invalid")

    actual_parent_hashes: dict[str, str] = {}
    for parent_id in reference.parent_snapshot_ids:
        parent_record = get_snapshot(parent_id)
        if parent_record is None or not hasattr(parent_record, "reference"):
            errors.append("physical_partition_parent_snapshot_missing")
            continue
        parent_reference = parent_record.reference
        parent_manifest = parent_reference.manifest
        parent_published_at = (
            _physical_canary_manifest_time(parent_manifest.get("published_at"))
            if isinstance(parent_manifest, Mapping)
            else None
        )
        parent_as_of = parent_reference.as_of
        if parent_as_of.tzinfo is not None and parent_as_of.utcoffset() is not None:
            parent_as_of = parent_as_of.astimezone(timezone.utc)
        else:
            parent_as_of = None
        try:
            parent_created_at = _aware(
                getattr(parent_record, "created_at", None),
                name="parent snapshot created_at",
            )
        except (TypeError, ValueError):
            parent_created_at = None
        if not (
            parent_published_at is not None
            and parent_as_of is not None
            and parent_created_at is not None
            and snapshot_created_at is not None
            and published_at is not None
            and parent_published_at <= parent_created_at <= snapshot_created_at
            and parent_published_at <= published_at
            and completed_at is not None
            and parent_created_at <= completed_at
            and parent_as_of <= parent_created_at
            and parent_created_at <= now
        ):
            errors.append("physical_partition_parent_timeline_invalid")
        actual_parent_hashes[parent_id] = str(parent_reference.content_hash)

    logical_hashes: list[str] = []
    for parent_id in logical_parent_ids:
        logical_parent = (
            snapshot_record
            if parent_id == reference.snapshot_id
            else get_snapshot(parent_id)
        )
        if logical_parent is None or not hasattr(logical_parent, "reference"):
            errors.append("physical_partition_parent_snapshot_missing")
            continue
        logical_hashes.append(str(logical_parent.reference.content_hash))
    if tuple(parent_evidence_hashes) != tuple(sorted(set(logical_hashes))):
        errors.append("physical_partition_parent_hash_binding_invalid")

    if stage == "data_quality":
        report = manifest.get("quality_report")
        expected_output_hash = (
            content_fingerprint(
                dict(report),
                domain="factor-lab/research-os/v1/physical-canary-dq-report",
            )
            if isinstance(report, Mapping)
            else ""
        )
    else:
        expected_output_hash = str(reference.content_hash)
    if record.output_hash != expected_output_hash:
        errors.append("physical_partition_output_hash_binding_invalid")
    return tuple(sorted(set(errors)))


class PhysicalCanaryError(RuntimeError):
    """Base error for physical canary admission, data, or persistence failure."""


class PhysicalCanaryAdmissionError(PhysicalCanaryError):
    """Runtime authority is insufficient for a physical provider operation."""


class PhysicalCanaryDataRejected(PhysicalCanaryError):
    """Fetched physical evidence failed reconciliation or data quality."""


class PhysicalCanaryBusy(PhysicalCanaryError):
    """The deterministic canary fingerprint already has a live executor."""


class PhysicalCanaryFormalEpochDenied(PhysicalCanaryError):
    """A non-forward canary was offered as formal epoch evidence."""


def _labels(*, physical_source_attested: bool, controlled_test: bool) -> dict[str, Any]:
    return {
        "evidence_schema": EVIDENCE_SCHEMA,
        "evidence_class": EVIDENCE_CLASS,
        "evidence_scope": EVIDENCE_SCOPE,
        "formal_epoch_eligible": False,
        "physical_source_attested": bool(physical_source_attested),
        "controlled_test_adapter": bool(controlled_test),
        "readiness_admission": (
            "physical_engineering_prerequisite"
            if physical_source_attested and not controlled_test
            else "rejected_controlled_test_adapter"
        ),
    }


def _aware(value: datetime, *, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    return value.astimezone(timezone.utc)


def _session_time(session: date, value: time) -> datetime:
    return datetime.combine(session, value, tzinfo=_SHANGHAI).astimezone(timezone.utc)


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _calendar_hash(sessions: Sequence[date]) -> str:
    return _sha256_bytes("\n".join(item.isoformat() for item in sessions).encode("ascii"))


def _gold_attempted_input_hash(
    *,
    stage_source: str,
    session: date,
    role: str,
    silver_snapshot_id: str,
    calendar_hash: str,
) -> str:
    return content_fingerprint(
        {
            "stage_source": stage_source,
            "trade_date": session.isoformat(),
            "role": role,
            "silver_snapshot_id": silver_snapshot_id,
            "calendar_hash": calendar_hash,
        },
        domain="factor-lab/research-os/v1/physical-canary-gold-attempted-input",
    )


def _legacy_frame_digest(frame: pd.DataFrame) -> str:
    """Reproduce the digest stored by pre-persisted-boundary manifests."""

    encoded = frame.to_json(
        orient="table",
        date_format="iso",
        date_unit="ns",
        index=False,
        force_ascii=False,
    ).encode("utf-8")
    return _sha256_bytes(encoded)


def _normalize_logical_arrow_table(table: pa.Table) -> pa.Table:
    """Normalize equivalent Arrow string layouts for logical comparisons.

    Pandas 3 can restore the same Parquet string values as ``large_string``
    even when the pre-write object column and the raw Parquet table use
    ``string``.  That presentation difference is irrelevant to the logical
    frame, while persisted digests continue to bind the unmodified raw Arrow
    schema below.
    """

    fields = []
    changed = False
    for field in table.schema:
        field_type = field.type
        if (
            pa.types.is_string(field_type)
            or pa.types.is_large_string(field_type)
            or pa.types.is_string_view(field_type)
        ):
            field_type = pa.large_string()
        changed = changed or field_type != field.type
        fields.append(pa.field(field.name, field_type, nullable=field.nullable))
    normalized = table.replace_schema_metadata(None).combine_chunks()
    if changed:
        normalized = normalized.cast(pa.schema(fields), safe=True)
    return normalized.replace_schema_metadata(None).combine_chunks()


def _canonical_arrow_table(frame: pd.DataFrame) -> pa.Table:
    """Normalize pandas-only dtype presentation without relaxing values."""

    try:
        return _normalize_logical_arrow_table(
            pa.Table.from_pandas(frame, preserve_index=False)
        )
    except Exception as exc:
        raise PhysicalCanaryDataRejected(
            "canary frame cannot be represented as canonical Arrow"
        ) from exc


def _read_persisted_table(path: Path, *, context: str) -> pa.Table:
    if path.is_symlink() or not path.is_file():
        raise PhysicalCanaryDataRejected(
            f"{context} canary Parquet is not a regular file"
        )
    try:
        return pq.read_table(path).replace_schema_metadata(None).combine_chunks()
    except Exception as exc:
        raise PhysicalCanaryDataRejected(
            f"{context} canary Parquet cannot be read as Arrow"
        ) from exc


def _arrow_ipc_sha256(table: pa.Table) -> str:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return _sha256_bytes(sink.getvalue().to_pybytes())


def _persisted_frame_digest(table: pa.Table) -> str:
    """Versioned digest of the exact metadata-free persisted Arrow table."""

    return content_fingerprint(
        {
            "schema_version": _PERSISTED_FRAME_DIGEST_SCHEMA,
            "encoding": "arrow-ipc-stream/no-schema-metadata/combined-chunks",
            "payload_sha256": _arrow_ipc_sha256(table),
        },
        domain=_PERSISTED_FRAME_DIGEST_SCHEMA,
    )


def _logical_frame_digest(frame: pd.DataFrame) -> str:
    """Hash pre-write logical inputs in a domain distinct from storage."""

    table = _canonical_arrow_table(frame)
    return content_fingerprint(
        {
            "schema_version": _LOGICAL_FRAME_DIGEST_SCHEMA,
            "encoding": "arrow-ipc-stream/no-schema-metadata/combined-chunks",
            "payload_sha256": _arrow_ipc_sha256(table),
        },
        domain=_LOGICAL_FRAME_DIGEST_SCHEMA,
    )


def _snapshot_content_hash(manifest: Mapping[str, Any]) -> str:
    return content_fingerprint(
        manifest,
        domain=_SNAPSHOT_FINGERPRINT_DOMAIN,
    )


def _snapshot_id(tier: SnapshotTier, content_hash: str) -> str:
    return f"pec_{tier.value}_{content_hash[:56]}"


def _assert_logically_equivalent(
    expected: pa.Table,
    observed: pa.Table,
    *,
    context: str,
) -> None:
    """Reject any Parquet conversion that changes schema, order, nulls, or values."""

    if not expected.equals(observed):
        raise PhysicalCanaryDataRejected(
            f"{context} canary Parquet changed logical frame values or schema"
        )


def _clean_error(exc: BaseException) -> str:
    return f"physical canary failed closed: {type(exc).__name__}"


def _render_template(value: Any, *, session: date, ticker: str | None) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _render_template(child, session=session, ticker=ticker)
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [_render_template(child, session=session, ticker=ticker) for child in value]
    if not isinstance(value, str):
        return value
    rendered = value.replace("${partition_key}", session.isoformat()).replace(
        "${partition_yyyymmdd}", session.strftime("%Y%m%d")
    )
    if "${ticker}" in rendered:
        if ticker is None:
            raise PhysicalCanaryAdmissionError("ticker-scoped request has no ticker")
        rendered = rendered.replace("${ticker}", ticker)
    return rendered


def _event_timestamp(value: Any) -> pd.Timestamp:
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        parsed = parsed.tz_localize(_SHANGHAI)
    return parsed.tz_convert(timezone.utc)


@dataclass(frozen=True)
class _SourceBinding:
    adapter: SourceAdapter
    dataset: str
    request_parameters: Mapping[str, Any]
    request_fields: tuple[str, ...]
    canonicalization: CanonicalizationSpec
    availability: Mapping[str, Any]
    per_ticker: bool = False

    def descriptor(self) -> dict[str, Any]:
        contract = self.adapter.contract_for(self.dataset)
        return {
            "adapter_type": type(self.adapter).__name__,
            "adapter_contract_identity": self.adapter.public_contract_identity(),
            "source_id": self.adapter.source_id,
            "source_priority": self.adapter.priority,
            "dataset": self.dataset,
            "request_parameters": dict(self.request_parameters),
            "request_fields": list(self.request_fields),
            "contract": asdict(contract),
            "canonicalization": asdict(self.canonicalization),
            "availability": dict(self.availability),
            "per_ticker": self.per_ticker,
        }

    def request(self, session: date, *, ticker: str | None = None) -> FetchRequest:
        return FetchRequest(
            dataset=self.dataset,
            parameters=_render_template(
                self.request_parameters,
                session=session,
                ticker=ticker,
            ),
            fields=self.request_fields,
        )

    def availability_resolver(self, row: pd.Series) -> datetime:
        mode = str(self.availability.get("mode") or "").strip()
        if mode == "event_timestamp":
            field = str(
                self.availability.get("available_at_field")
                or self.canonicalization.event_time_column
            )
            return _event_timestamp(row[field]).to_pydatetime()
        if mode != "session_release_time":
            raise PhysicalCanaryAdmissionError(
                f"{self.dataset} has no reviewed availability mode"
            )
        raw_time = str(self.availability.get("time") or "").strip()
        try:
            release_time = time.fromisoformat(raw_time)
        except ValueError as exc:
            raise PhysicalCanaryAdmissionError(
                f"{self.dataset} has an invalid release time"
            ) from exc
        event = pd.Timestamp(row[self.canonicalization.event_time_column])
        event_date = event.date()
        lag_days = int(self.availability.get("lag_days") or 0)
        return _session_time(event_date + timedelta(days=lag_days), release_time)


@dataclass(frozen=True)
class PhysicalObjectEvidence:
    snapshot_id: str
    tier: str
    role: str
    trade_date: str
    uri: str
    content_hash: str
    object_sha256: str
    size_bytes: int


@dataclass(frozen=True)
class PhysicalCanaryResult:
    evidence_schema: str
    evidence_class: str
    evidence_scope: str
    formal_epoch_eligible: bool
    physical_source_attested: bool
    controlled_test_adapter: bool
    readiness_admission: str
    run_id: str
    run_type: str
    canary_evidence_hash: str
    calendar_sessions: tuple[str, ...]
    security_count: int
    projected_session_count: int
    sleeve_id: str
    sleeve_state: str
    account_id: str
    partition_run_ids: tuple[str, ...]
    source_probe_hashes: Mapping[str, str]
    object_evidence: tuple[PhysicalObjectEvidence, ...]
    shadow_sessions: tuple[ShadowSessionProjection, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["shadow_sessions"] = [
            {
                **asdict(item),
                "trade_date": item.trade_date.isoformat(),
                "evidence_class": item.evidence_class.value,
                "created_at": item.created_at.isoformat(),
            }
            for item in self.shadow_sessions
        ]
        return payload


def deny_physical_canary_formal_epoch(result: PhysicalCanaryResult) -> None:
    """Always reject a physical historical replay from formal epoch admission."""

    raise PhysicalCanaryFormalEpochDenied(
        f"{result.evidence_class}/{result.evidence_scope} is never formal epoch evidence"
    )


def require_physical_canary_credential_rotation(
    evidence: ProductionConfigEvidence,
) -> None:
    """Enforce the non-forward canary admission policy.

    The compatibility name is retained, but rotation is intentionally a
    formal-backfill/forward prerequisite rather than a canary prerequisite.
    Adapter admission separately proves that provider credentials came from
    ``secret://`` file bindings.
    """

    admission = admit_production_operation(
        evidence,
        ProductionOperation.ENGINEERING_CANARY,
    )
    if not admission.allowed:
        raise PhysicalCanaryAdmissionError(
            "physical engineering canary is blocked: "
            + ", ".join(admission.blockers)
        )


def _contract_entity_columns(contract: DatasetContract) -> tuple[str, ...]:
    for candidate in ("ts_code", "ticker", "stock_code", "exchange"):
        if candidate in contract.field_map:
            return (candidate,)
    return tuple(
        item for item in contract.key_fields if item != contract.event_time_field
    ) or (contract.key_fields[0],)


def _controlled_binding(adapter: SourceAdapter, dataset: str) -> _SourceBinding:
    contract = adapter.contract_for(dataset)
    entity = _contract_entity_columns(contract)
    excluded = {*entity, contract.event_time_field, "available_at"}
    values = tuple(name for name in contract.field_map if name not in excluded)
    if dataset == "opening_execution":
        parameters: Mapping[str, Any] = {
            "stock_code": "${ticker}",
            "start_time": "${partition_key} 09:30:00",
            "end_time": "${partition_key} 15:00:00",
        }
        availability = {
            "mode": "event_timestamp",
            "available_at_field": (
                "available_at"
                if "available_at" in contract.field_map
                else contract.event_time_field
            ),
        }
        available_column = (
            "available_at" if "available_at" in contract.field_map else None
        )
        per_ticker = True
    elif dataset == "trade_calendar":
        parameters = {
            "start_date": "${partition_yyyymmdd}",
            "end_date": "${partition_yyyymmdd}",
        }
        availability = {
            "mode": "session_release_time",
            "time": "08:00:00",
            "lag_days": 0,
        }
        available_column = None
        per_ticker = False
    else:
        parameters = {"trade_date": "${partition_yyyymmdd}"}
        availability = {
            "mode": "session_release_time",
            "time": "18:30:00" if dataset != "daily" else "15:30:00",
            "lag_days": 1 if dataset in {"historical_st", "stock_limit"} else 0,
        }
        available_column = None
        per_ticker = False
    return _SourceBinding(
        adapter=adapter,
        dataset=dataset,
        request_parameters=parameters,
        request_fields=tuple(contract.field_map),
        canonicalization=CanonicalizationSpec(
            entity_columns=entity,
            event_time_column=contract.event_time_field,
            available_at_column=available_column,
            value_columns=values,
        ),
        availability=availability,
        per_ticker=per_ticker,
    )


def _production_binding(payload: Mapping[str, Any], env: Mapping[str, str]) -> _SourceBinding:
    adapter = source_adapter_from_mapping(payload, env=env)
    request = payload.get("request")
    canonical = payload.get("canonicalization")
    if not isinstance(request, Mapping) or not isinstance(canonical, Mapping):
        raise PhysicalCanaryAdmissionError("physical canary source contract is incomplete")
    dataset = str(request.get("dataset") or "")
    availability = canonical.get("availability")
    if not isinstance(availability, Mapping):
        raise PhysicalCanaryAdmissionError(
            f"{dataset} has no explicit availability contract"
        )
    return _SourceBinding(
        adapter=adapter,
        dataset=dataset,
        request_parameters=dict(request.get("parameters") or {}),
        request_fields=tuple(map(str, request.get("fields") or ())),
        canonicalization=CanonicalizationSpec(
            entity_columns=tuple(map(str, canonical.get("entity_columns") or ())),
            event_time_column=str(canonical.get("event_time_column") or ""),
            available_at_column=(
                None
                if canonical.get("available_at_column") is None
                else str(canonical["available_at_column"])
            ),
            value_columns=tuple(map(str, canonical.get("value_columns") or ())),
        ),
        availability=dict(availability),
        per_ticker=False,
    )


def _opening_source_payload(canary: Mapping[str, Any]) -> dict[str, Any]:
    """Build only the reviewed retrospective Diemeng canary source.

    The formal shadow source may be Tushare ``rt_min``, but that endpoint is
    current-session only and must never be replayed across the canary's 20
    historical sessions.  This seam therefore reads the independent canary
    mapping and validates its entire closed schema before adapter construction.
    """

    execution = canary.get("execution_market_data")
    if not isinstance(execution, Mapping):
        raise PhysicalCanaryAdmissionError(
            "engineering canary execution market data is absent"
        )
    try:
        execution = validate_diemeng_engineering_canary_execution_mapping(execution)
    except ValueError as exc:
        raise PhysicalCanaryAdmissionError(str(exc)) from None
    contract = execution.get("contract")
    assert isinstance(contract, Mapping)  # closed validator invariant
    fields = tuple(map(str, contract.get("fields") or ()))
    request = execution.get("request")
    assert isinstance(request, Mapping)  # closed validator invariant
    dtype = {
        "stock_code": "string",
        "trade_time": "datetime",
    }
    return {
        "source": str(execution.get("source") or ""),
        "profile_name": str(execution.get("profile_name") or ""),
        "credential_ref": str(execution.get("credential_ref") or ""),
        "priority": 20,
        "base_url": str(execution.get("base_url") or ""),
        "endpoint_map": {"opening_execution": str(execution.get("endpoint") or "")},
        "method_map": {"opening_execution": str(execution.get("method") or "POST")},
        "response_paths": {
            "opening_execution": str(execution.get("response_path") or "")
        },
        "probe_dataset": "opening_execution",
        "probe_parameters": {},
        "request": {
            "dataset": "opening_execution",
            "parameters": deepcopy(request),
            "fields": list(fields),
        },
        "contract": {
            "dataset": "opening_execution",
            "key_fields": list(map(str, contract.get("key_fields") or ())),
            "event_time_field": str(contract.get("event_time_field") or ""),
            "release_timing": "provider event timestamp at the observed minute",
            "allows_empty": False,
            "fields": [
                {
                    "name": name,
                    "dtype": dtype.get(name, "float64"),
                    "nullable": False,
                }
                for name in fields
            ],
        },
        "canonicalization": {
            "entity_columns": ["stock_code"],
            "event_time_column": "trade_time",
            "available_at_column": "trade_time",
            "value_columns": [
                name for name in fields if name not in {"stock_code", "trade_time"}
            ],
            "availability": {
                "mode": "event_timestamp",
                "event_time_field": "trade_time",
                "available_at_field": "trade_time",
            },
        },
    }


@dataclass(frozen=True)
class _PersistedFrame:
    path: Path
    frame_digest: str
    object_sha256: str
    size_bytes: int
    rows: int
    columns: tuple[str, ...]


def _file_integrity(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


@dataclass(frozen=True)
class _PinnedParquetFrame:
    table: pa.Table
    frame: pd.DataFrame
    object_sha256: str
    size_bytes: int


def _read_file_bytes(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except OSError as exc:
        raise PhysicalCanaryDataRejected(
            "restored snapshot Parquet bytes cannot be pinned"
        ) from exc


def _read_pinned_parquet_frame(
    path: Path,
    *,
    expected_sha256: str,
    expected_size_bytes: int,
) -> _PinnedParquetFrame:
    """Bind one immutable byte payload to both Arrow evidence and pandas values."""

    if path.is_symlink() or not path.is_file():
        raise PhysicalCanaryDataRejected(
            "restored snapshot Parquet is not a regular file"
        )
    try:
        integrity_before = _file_integrity(path)
        payload = _read_file_bytes(path)
        payload_integrity = (_sha256_bytes(payload), len(payload))
        integrity_after = _file_integrity(path)
    except PhysicalCanaryDataRejected:
        raise
    except OSError as exc:
        raise PhysicalCanaryDataRejected(
            "restored snapshot Parquet integrity cannot be verified"
        ) from exc
    expected_integrity = (expected_sha256, expected_size_bytes)
    if integrity_before != expected_integrity:
        raise PhysicalCanaryDataRejected(
            "restored snapshot Parquet differs before bytes are pinned"
        )
    if payload_integrity != expected_integrity:
        raise PhysicalCanaryDataRejected(
            "pinned snapshot Parquet bytes differ from archived evidence"
        )
    if integrity_after != expected_integrity:
        raise PhysicalCanaryDataRejected(
            "restored snapshot Parquet changed while bytes were pinned"
        )
    try:
        raw_table = pq.read_table(pa.BufferReader(payload)).combine_chunks()
        frame = raw_table.to_pandas()
    except Exception as exc:
        raise PhysicalCanaryDataRejected(
            "pinned snapshot Parquet cannot be restored as Arrow"
        ) from exc
    return _PinnedParquetFrame(
        table=raw_table.replace_schema_metadata(None).combine_chunks(),
        frame=frame,
        object_sha256=payload_integrity[0],
        size_bytes=payload_integrity[1],
    )


def _persisted_frame(
    path: Path,
    *,
    expected_digest: str,
    expected_table: pa.Table,
) -> _PersistedFrame:
    object_sha256_before, size_bytes_before = _file_integrity(path)
    table = _read_persisted_table(path, context="persisted")
    observed_digest = _persisted_frame_digest(table)
    if observed_digest != expected_digest or not table.equals(expected_table):
        raise PhysicalCanaryDataRejected(
            "persisted canary Parquet frame digest differs"
        )
    object_sha256, size_bytes = _file_integrity(path)
    if (
        object_sha256 != object_sha256_before
        or size_bytes != size_bytes_before
    ):
        raise PhysicalCanaryDataRejected(
            "persisted canary Parquet changed while it was being verified"
        )
    return _PersistedFrame(
        path=path,
        frame_digest=observed_digest,
        object_sha256=object_sha256,
        size_bytes=size_bytes,
        rows=int(table.num_rows),
        columns=tuple(map(str, table.column_names)),
    )


def _write_frame_once(directory: Path, frame: pd.DataFrame) -> _PersistedFrame:
    """Persist, read back, and content-address the exact Parquet candidate.

    The candidate must cross the same Parquet boundary as a later restore
    before it receives an immutable name. Existing and concurrent winners are
    compared to that read-back digest, never to the caller's pre-write dtypes.
    """

    directory.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".candidate-frame.", suffix=".parquet.tmp", dir=directory
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        expected = _canonical_arrow_table(frame)
        storage_frame = frame.copy()
        for field in expected.schema:
            if pa.types.is_large_string(field.type):
                storage_frame[field.name] = storage_frame[field.name].astype(
                    pd.ArrowDtype(pa.large_string())
                )
        storage_table = pa.Table.from_pandas(storage_frame, preserve_index=False)
        pq.write_table(storage_table, temporary)
        candidate = _read_persisted_table(temporary, context="candidate")
        _assert_logically_equivalent(
            expected,
            _normalize_logical_arrow_table(candidate),
            context="candidate",
        )
        candidate_digest = _persisted_frame_digest(candidate)
        path = directory / f"{candidate_digest}.parquet"
        try:
            os.link(temporary, path)
        except FileExistsError:
            existing = _read_persisted_table(path, context="concurrent")
            if (
                _persisted_frame_digest(existing) != candidate_digest
                or not existing.equals(candidate)
            ):
                raise PhysicalCanaryDataRejected(
                    "concurrent immutable canary cache differs"
                )
        return _persisted_frame(
            path,
            expected_digest=candidate_digest,
            expected_table=candidate,
        )
    finally:
        temporary.unlink(missing_ok=True)


def _silver_storage_frame(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()

    def encoded(value: Any) -> str | None:
        if value is None or pd.isna(value):
            return None
        if isinstance(value, (datetime, date, pd.Timestamp)):
            return json.dumps(pd.Timestamp(value).isoformat())
        if hasattr(value, "item"):
            value = value.item()
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)

    result["value"] = result["value"].map(encoded).astype("string")
    return result


class PhysicalEngineeringCanaryService:
    """Code-authoritative physical canary; callers cannot supply market frames."""

    def __init__(
        self,
        *,
        catalog: ResearchCatalog,
        production_ledger: ProductionLedger,
        shadow_authority: ShadowEvidenceAuthority,
        object_store_archive: S3ImmutableArchive,
        local_root: Path,
        bindings: Sequence[_SourceBinding],
        production_evidence: ProductionConfigEvidence | None,
        controlled_test: bool,
        opening_execution_formal_ready: bool,
        now: Callable[[], datetime],
    ) -> None:
        self.catalog = catalog
        self.production_ledger = production_ledger
        self.shadow_authority = shadow_authority
        self.archive = S3ImmutableArchive(
            bucket=object_store_archive.bucket,
            filesystem=object_store_archive.filesystem,
            prefix=CANARY_OBJECT_PREFIX,
        )
        self.local_root = Path(local_root).resolve() / "engineering-canary"
        self.bindings = tuple(bindings)
        self.production_evidence = production_evidence
        self.controlled_test = bool(controlled_test)
        self.physical_source_attested = not self.controlled_test
        if bool(opening_execution_formal_ready):
            raise PhysicalCanaryAdmissionError(
                "retrospective engineering canary execution is never formal-ready"
            )
        self.opening_execution_formal_ready = False
        if self.controlled_test:
            self.canary_execution_contract_hash = content_fingerprint(
                {
                    "evidence_scope": EVIDENCE_SCOPE,
                    "mode": "controlled_test",
                    "bindings": [item.descriptor() for item in self.bindings],
                },
                domain=(
                    "factor-lab/research-os/v1/"
                    "controlled-physical-canary-execution-contract"
                ),
            )
        else:
            contract_hash = str(
                getattr(
                    production_evidence,
                    "engineering_canary_execution_contract_hash",
                    "",
                )
                or ""
            )
            if len(contract_hash) != 64 or any(
                character not in "0123456789abcdef"
                for character in contract_hash
            ):
                raise PhysicalCanaryAdmissionError(
                    "validated engineering canary execution contract hash is absent"
                )
            self.canary_execution_contract_hash = contract_hash
        self._now = now
        self.shadow = ShadowStepService(
            catalog,
            ShadowExecutionConfig(
                max_position_weight=0.02,
                max_adv_participation=0.05,
                lot_size=100,
            ),
        )
        self._validate_binding_shape()

    @classmethod
    def from_production_config(
        cls,
        config_path: str | Path,
        *,
        env: Mapping[str, str],
        catalog: ResearchCatalog,
        production_ledger: ProductionLedger,
        shadow_authority: ShadowEvidenceAuthority,
        object_store_archive: S3ImmutableArchive,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        require_mounts: bool = True,
        mount_checker: Callable[[Path], bool] | None = None,
        image_reference: str | None = None,
    ) -> "PhysicalEngineeringCanaryService":
        """Build the only production-capable canary from reviewed config.

        No adapter, request, frame, or path is accepted from the operation
        caller.  Adapters are selected from the validated immutable production
        config and the local output root is the validated runtime data mount.
        """

        evidence = validate_production_config(
            config_path,
            env=env,
            require_mounts=require_mounts,
            mount_checker=mount_checker,
            image_reference=image_reference,
        )
        payload = load_production_config(evidence.path)
        daily = payload.get("daily")
        if not isinstance(daily, Mapping):
            raise PhysicalCanaryAdmissionError("production daily config is absent")
        raw_sources = daily.get("sources")
        if not isinstance(raw_sources, list):
            raise PhysicalCanaryAdmissionError("production source registry is absent")
        security = payload.get("security")
        source_transport = (
            security.get("source_transport")
            if isinstance(security, Mapping)
            else None
        )
        tushare_transport = (
            source_transport.get("tushare")
            if isinstance(source_transport, Mapping)
            else None
        )
        reviewed_tushare_origin = str(
            tushare_transport.get("api_origin")
            if isinstance(tushare_transport, Mapping)
            else ""
        ).strip()

        def with_reviewed_transport(source: Mapping[str, Any]) -> Mapping[str, Any]:
            if str(source.get("source") or "").lower() != "tushare":
                return source
            return {**dict(source), "api_origin": reviewed_tushare_origin}

        selected: list[Mapping[str, Any]] = []
        required = {"trade_calendar", "daily", "adj_factor", "historical_st", "stock_limit"}
        for source in raw_sources:
            if not isinstance(source, Mapping):
                continue
            request = source.get("request")
            dataset = str(request.get("dataset") or "") if isinstance(request, Mapping) else ""
            if dataset in required:
                selected.append(with_reviewed_transport(source))
        counts = {
            dataset: sum(
                1
                for source in selected
                if str((source.get("request") or {}).get("dataset") or "") == dataset
            )
            for dataset in required
        }
        if counts.get("trade_calendar", 0) < 2 or any(
            counts.get(dataset, 0) != 1 for dataset in required - {"trade_calendar"}
        ):
            raise PhysicalCanaryAdmissionError(
                "physical canary requires two calendars and one reviewed market source per dataset"
            )
        bindings = [_production_binding(source, env) for source in selected]
        engineering_canary = daily.get("engineering_canary")
        if not isinstance(engineering_canary, Mapping) or (
            engineering_canary.get("evidence_scope")
            != "retrospective_non_forward"
        ):
            raise PhysicalCanaryAdmissionError(
                "production engineering canary must be explicitly retrospective and non-forward"
            )
        opening_payload = _opening_source_payload(engineering_canary)
        contract_hash = engineering_canary_execution_contract_hash(
            engineering_canary
        )
        if contract_hash != evidence.engineering_canary_execution_contract_hash:
            raise PhysicalCanaryAdmissionError(
                "validated engineering canary execution contract changed"
            )
        opening_binding = _production_binding(opening_payload, env)
        bindings.append(
            _SourceBinding(
                adapter=opening_binding.adapter,
                dataset=opening_binding.dataset,
                request_parameters=opening_binding.request_parameters,
                request_fields=opening_binding.request_fields,
                canonicalization=opening_binding.canonicalization,
                availability=opening_binding.availability,
                per_ticker=True,
            )
        )
        return cls(
            catalog=catalog,
            production_ledger=production_ledger,
            shadow_authority=shadow_authority,
            object_store_archive=object_store_archive,
            local_root=evidence.runtime_data_root,
            bindings=bindings,
            production_evidence=evidence,
            controlled_test=False,
            opening_execution_formal_ready=False,
            now=now,
        )

    @classmethod
    def for_controlled_test(
        cls,
        *,
        adapters: Sequence[SourceAdapter],
        catalog: ResearchCatalog,
        production_ledger: ProductionLedger,
        shadow_authority: ShadowEvidenceAuthority,
        object_store_archive: S3ImmutableArchive,
        local_root: str | Path,
        now: Callable[[], datetime],
    ) -> "PhysicalEngineeringCanaryService":
        """Exercise the physical code path while producing rejected evidence.

        This is the sole adapter-injection seam.  Every supplied adapter must
        carry the explicit controlled-test marker, and its completed run uses a
        distinct run type that a production readiness audit must reject.
        """

        values = tuple(adapters)
        if not values or any(
            not bool(getattr(adapter, _CONTROLLED_TEST_MARKER, False))
            for adapter in values
        ):
            raise PhysicalCanaryAdmissionError(
                "controlled test factory accepts only marked fake adapters"
            )
        bindings: list[_SourceBinding] = []
        for adapter in values:
            for dataset in adapter.contracts:
                if dataset in {"trade_calendar", *_REQUIRED_SINGLE_DATASETS}:
                    bindings.append(_controlled_binding(adapter, dataset))
        return cls(
            catalog=catalog,
            production_ledger=production_ledger,
            shadow_authority=shadow_authority,
            object_store_archive=object_store_archive,
            local_root=Path(local_root),
            bindings=bindings,
            production_evidence=None,
            controlled_test=True,
            opening_execution_formal_ready=False,
            now=now,
        )

    def _validate_binding_shape(self) -> None:
        by_dataset: dict[str, list[_SourceBinding]] = {}
        for binding in self.bindings:
            by_dataset.setdefault(binding.dataset, []).append(binding)
        if len(by_dataset.get("trade_calendar", ())) < 2:
            raise PhysicalCanaryAdmissionError(
                "physical canary requires at least two calendar adapters"
            )
        calendar_bindings = tuple(by_dataset.get("trade_calendar", ()))
        calendar_source_ids = {
            str(binding.adapter.source_id) for binding in calendar_bindings
        }
        calendar_provider_contracts = {
            content_fingerprint(
                binding.descriptor(),
                domain="factor-lab/research-os/v1/calendar-provider-contract",
            )
            for binding in calendar_bindings
        }
        if (
            len(calendar_source_ids) < 2
            or len(calendar_provider_contracts) != len(calendar_bindings)
        ):
            raise PhysicalCanaryAdmissionError(
                "physical canary requires two independent calendar providers"
            )
        for dataset in _REQUIRED_SINGLE_DATASETS:
            if len(by_dataset.get(dataset, ())) != 1:
                raise PhysicalCanaryAdmissionError(
                    f"physical canary requires exactly one {dataset} adapter"
                )
        if not self.controlled_test:
            for binding in self.bindings:
                adapter = binding.adapter
                if type(adapter) not in _REAL_ADAPTER_TYPES or bool(
                    getattr(adapter, _CONTROLLED_TEST_MARKER, False)
                ):
                    raise PhysicalCanaryAdmissionError(
                        "synthetic, in-memory, subclassed, or test adapters are forbidden in production"
                    )
                credential = str(adapter.lineage.get("credential_binding") or "")
                if adapter.source_id in {"tushare", "diemeng"} and not credential.startswith(
                    "secret://"
                ):
                    raise PhysicalCanaryAdmissionError(
                        "production provider adapter lacks a secret-file profile binding"
                    )
                if isinstance(adapter, TushareSourceAdapter) and not (
                    tushare_client_uses_direct_transport(adapter.client)
                    and isinstance(adapter.base_url, str)
                    and adapter.base_url.startswith("https://")
                    and adapter.transport_policy == TUSHARE_SEALED_TRANSPORT_POLICY
                ):
                    raise PhysicalCanaryAdmissionError(
                        "production Tushare adapter lacks the reviewed sealed HTTPS transport"
                    )

    def _assert_runtime_admission(self) -> None:
        if self.controlled_test:
            return
        evidence = self.production_evidence
        if evidence is None:
            raise PhysicalCanaryAdmissionError("validated production evidence is absent")
        require_physical_canary_credential_rotation(evidence)
        ledger_engine = getattr(self.production_ledger, "engine", None)
        authority_engine = getattr(self.shadow_authority, "engine", None)
        if (
            getattr(getattr(ledger_engine, "dialect", None), "name", None) != "postgresql"
            or getattr(getattr(authority_engine, "dialect", None), "name", None)
            != "postgresql"
        ):
            raise PhysicalCanaryAdmissionError(
                "production physical canary requires PostgreSQL 0007 authorities"
            )
        if type(self.archive.filesystem).__module__.split(".", 1)[0] != "s3fs":
            raise PhysicalCanaryAdmissionError(
                "production physical canary requires a real S3/MinIO filesystem"
            )

    @staticmethod
    def _workload_container_identity() -> str:
        try:
            identity = Path("/etc/hostname").read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError):
            raise PhysicalCanaryAdmissionError(
                "physical canary cannot identify its workload container"
            ) from None
        if not (12 <= len(identity) <= 64) or any(
            character not in "0123456789abcdef" for character in identity
        ):
            raise PhysicalCanaryAdmissionError(
                "physical canary workload container identity is non-canonical"
            )
        return identity

    @staticmethod
    def _workload_process_identity() -> str:
        """Return a wall-clock-independent kernel/PID-1 lifecycle hash."""

        try:
            process_identity = local_code_server_runtime_identity().process_identity
        except DagsterSoakError:
            raise PhysicalCanaryAdmissionError(
                "physical canary cannot measure its container process identity"
            ) from None
        return process_identity

    @staticmethod
    def _assert_workload_root_matches_init() -> None:
        try:
            current_root = os.stat("/")
            init_root = os.stat("/proc/1/root")
        except OSError:
            raise PhysicalCanaryAdmissionError(
                "physical canary cannot verify its init root filesystem"
            ) from None
        if (current_root.st_dev, current_root.st_ino) != (
            init_root.st_dev,
            init_root.st_ino,
        ):
            raise PhysicalCanaryAdmissionError(
                "physical canary does not share the attested init root filesystem"
            )

    def _evaluator_identity(self) -> dict[str, Any]:
        """Return the secret-free build identity that owns this evaluation.

        A completed canary may only be replayed by the same measured source
        bundle and evaluator.  Controlled fixtures intentionally use a fixed,
        visibly non-production identity so unit tests do not depend on the
        developer worktree or container image.
        """

        base: dict[str, Any] = {
            "identity_schema": _EVALUATOR_IDENTITY_SCHEMA,
            "physical_canary_schema": EVIDENCE_SCHEMA,
            "reconciliation_schema": RECONCILIATION_EVALUATOR_SCHEMA,
            "canary_execution_contract_hash": (
                self.canary_execution_contract_hash
            ),
        }
        if self.controlled_test:
            return {
                **base,
                "mode": "controlled_test",
                "evaluator": _CONTROLLED_TEST_EVALUATOR,
            }
        cached = getattr(self, "_cached_evaluator_identity", None)
        if isinstance(cached, Mapping):
            return deepcopy(dict(cached))
        evidence = self.production_evidence
        provenance = None if evidence is None else getattr(evidence, "provenance", None)
        public = getattr(provenance, "public_dict", None)
        if not callable(public):
            raise PhysicalCanaryAdmissionError(
                "physical canary has no measured public build provenance"
            )
        measured = public()
        if not isinstance(measured, Mapping):
            raise PhysicalCanaryAdmissionError(
                "physical canary public build provenance is malformed"
            )
        build_identity_hash = str(measured.get("build_identity_hash") or "")
        if len(build_identity_hash) != 64 or any(
            character not in "0123456789abcdef" for character in build_identity_hash
        ):
            raise PhysicalCanaryAdmissionError(
                "physical canary build identity is not a SHA-256 digest"
            )
        runtime_build = dict(measured)
        runtime_deployment: dict[str, Any] = {
            "controlled_test_backend": True,
            "build_identity_hash": build_identity_hash,
        }
        runtime_attestation: dict[str, Any] = {
            "controlled_test_backend": True,
            "build_identity_hash": build_identity_hash,
        }
        if self.production_ledger.engine.dialect.name == "postgresql":
            # The worker cannot inspect its own host Docker daemon.  Bind the
            # reverified source bundle to the latest independently produced,
            # PostgreSQL-persisted host attestation using the same validator as
            # production readiness.
            from .readiness_audit import (
                ProductionReadinessAuditor,
                ReadinessAuditError,
            )

            try:
                deployment = dict(
                    ProductionReadinessAuditor(
                        self.catalog,
                        self.production_ledger,
                        config=load_production_config(evidence.path),
                        config_evidence=evidence,
                    ).verified_oci_deployment_evidence()
                )
            except ReadinessAuditError as exc:
                raise PhysicalCanaryAdmissionError(
                    "physical canary has no verified current OCI deployment"
                ) from exc
            workload_container_identity = self._workload_container_identity()
            workload_process_identity = self._workload_process_identity()
            self._assert_workload_root_matches_init()
            if not str(deployment.get("container_id") or "").startswith(
                workload_container_identity
            ):
                raise PhysicalCanaryAdmissionError(
                    "physical canary is not running in the attested code-server container"
                )
            epoch_fields = deployment.get("epoch_fields")
            if not isinstance(epoch_fields, Mapping):
                raise PhysicalCanaryAdmissionError(
                    "physical canary OCI deployment has no epoch fields"
                )
            runtime_build = {
                **dict(epoch_fields),
                "provenance_kind": "host_daemon_bound_oci_deployment",
                "build_identity_hash": deployment.get("build_identity_hash"),
                "git_commit": measured.get("git_commit"),
                "image_source_digest": measured.get("image_source_digest"),
                "oci_image_id": deployment.get("oci_image_id"),
                "oci_repo_digests": list(
                    deployment.get("oci_repo_digests") or ()
                ),
                "oci_base_digests": list(
                    deployment.get("oci_base_digests") or ()
                ),
                "formal_epoch_eligible": True,
            }
            # Only stable execution-environment fields belong to the evaluator
            # fingerprint.  Attestation timestamps, proof hashes and container
            # IDs are evidence *about* that environment; including them would
            # force a new experiment after every proof refresh or harmless
            # container restart.
            runtime_deployment = {
                key: deployment.get(key)
                for key in (
                    "controlled_test_backend",
                    "compose_config_hash",
                    "build_identity_hash",
                    "runtime_contract_hash",
                    "oci_image_id",
                    "oci_repo_digests",
                    "oci_base_digests",
                )
            }
            runtime_attestation = {
                key: deployment.get(key)
                for key in (
                    "host_attestation_run_id",
                    "host_attestation_hash",
                    "attested_at",
                    "container_started_at",
                    "container_id",
                    "deployment_identity_hash",
                    "docker_authority_hash",
                    "compose_config_hash",
                    "build_identity_hash",
                    "runtime_contract_hash",
                    "oci_image_id",
                    "oci_repo_digests",
                    "oci_base_digests",
                )
            }
            runtime_attestation["executing_container_identity"] = (
                workload_container_identity
            )
            runtime_attestation["executing_process_identity_scheme"] = (
                KERNEL_PROCESS_IDENTITY_SCHEME
            )
            runtime_attestation["executing_process_identity"] = (
                workload_process_identity
            )
            runtime_attestation["executing_root_matches_init_root"] = True
        identity = {
            **base,
            "mode": "production_image",
            "source_bundle_provenance": dict(measured),
            "build_provenance": runtime_build,
            "runtime_deployment": runtime_deployment,
        }
        self._cached_evaluator_identity = deepcopy(identity)
        self._runtime_attestation_proof = deepcopy(runtime_attestation)
        return identity

    def _runtime_attestation_evidence(self) -> dict[str, Any]:
        if self.controlled_test:
            return {
                "controlled_test_backend": True,
                "evidence": "fixed_controlled_test_evaluator",
            }
        self._evaluator_identity()
        proof = getattr(self, "_runtime_attestation_proof", None)
        if not isinstance(proof, Mapping):
            raise PhysicalCanaryAdmissionError(
                "physical canary runtime attestation proof is absent"
            )
        return deepcopy(dict(proof))

    def _binding(self, dataset: str) -> _SourceBinding:
        matches = [item for item in self.bindings if item.dataset == dataset]
        if len(matches) != 1:
            raise PhysicalCanaryAdmissionError(f"ambiguous binding for {dataset}")
        return matches[0]

    def _calendar_bindings(self) -> tuple[_SourceBinding, ...]:
        return tuple(
            sorted(
                (item for item in self.bindings if item.dataset == "trade_calendar"),
                key=lambda item: (item.adapter.priority, item.adapter.source_id),
            )
        )

    def _verify_external_object(self, uri: str) -> str:
        parsed = urlsplit(str(uri))
        if (
            parsed.scheme != "s3"
            or parsed.netloc != self.archive.bucket
            or not parsed.path.startswith("/")
            or ".." in parsed.path.split("/")
            or "%" in parsed.path
            or "\\" in parsed.path
        ):
            raise PhysicalCanaryAdmissionError(
                "accepted calendar does not reference a safe physical S3 object"
            )
        remote = f"{parsed.netloc}/{parsed.path[1:]}"
        digest_parts = [
            part.removeprefix("sha256=")
            for part in parsed.path.split("/")
            if part.startswith("sha256=")
        ]
        if len(digest_parts) != 1 or len(digest_parts[0]) != 64:
            raise PhysicalCanaryAdmissionError(
                "accepted calendar object URI is not content-addressed"
            )
        expected = digest_parts[0]
        if not self.archive.filesystem.exists(remote):
            raise PhysicalCanaryAdmissionError("accepted calendar physical object is missing")
        digest = hashlib.sha256()
        with self.archive.filesystem.open(remote, "rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
        if digest.hexdigest() != expected:
            raise PhysicalCanaryAdmissionError(
                "accepted calendar physical object hash differs"
            )
        return expected

    def _accepted_sessions(
        self, *, as_of: date | None
    ) -> tuple[tuple[date, ...], tuple[PartitionRecord, ...]]:
        records = self.production_ledger.list_partitions(
            statuses=(PartitionStatus.SUCCEEDED,),
            source_id="research_os",
            dataset="accepted_trade_calendar",
            limit=100_000,
        )
        usable: list[tuple[date, PartitionRecord]] = []
        for record in records:
            try:
                session = date.fromisoformat(record.identity.partition_key)
            except ValueError:
                continue
            if as_of is not None and session > as_of:
                continue
            if not record.output_snapshot_id:
                raise PhysicalCanaryAdmissionError(
                    "accepted calendar partition has no snapshot authority"
                )
            snapshot = self.catalog.get_snapshot(record.output_snapshot_id)
            if snapshot is None or snapshot.reference.quality_status is not DataQualityStatus.ACCEPTED:
                raise PhysicalCanaryAdmissionError(
                    "accepted calendar snapshot is absent or not accepted"
                )
            if snapshot.reference.tier not in {SnapshotTier.SILVER, SnapshotTier.GOLD}:
                raise PhysicalCanaryAdmissionError(
                    "accepted calendar snapshot must be Silver or Gold"
                )
            labels = {str(item).lower() for item in snapshot.reference.trust_labels}
            rejected = {
                "synthetic",
                "in_memory",
                "controlled_test_adapter",
                "legacy_untrusted_data",
            }
            if labels & rejected and not self.controlled_test:
                raise PhysicalCanaryAdmissionError(
                    "accepted calendar is synthetic, test, or legacy evidence"
                )
            self._verify_external_object(snapshot.reference.uri)
            usable.append((session, record))
        usable.sort(key=lambda item: item[0])
        if len(usable) < CALENDAR_SESSION_COUNT:
            raise PhysicalCanaryAdmissionError(
                f"physical canary needs {CALENDAR_SESSION_COUNT} accepted trading sessions"
            )
        selected = usable[-CALENDAR_SESSION_COUNT:]
        sessions = tuple(item[0] for item in selected)
        if sessions != tuple(sorted(set(sessions))):
            raise PhysicalCanaryAdmissionError(
                "accepted trading sessions are not ordered and unique"
            )
        return sessions, tuple(item[1] for item in selected)

    def _probe_sources(self, sessions: Sequence[date]) -> dict[str, str]:
        results: dict[str, str] = {}
        seen: set[tuple[str, str]] = set()
        for binding in self.bindings:
            identity = (binding.adapter.source_id, binding.dataset)
            if identity in seen:
                continue
            seen.add(identity)
            if (
                binding.dataset == "opening_execution"
                and isinstance(binding.adapter, DiemengSourceAdapter)
            ):
                session = sessions[1]
                probe_parameters = dict(
                    binding.request(session, ticker="000001.SZ").parameters
                )
                probe_parameters.update(
                    {
                        "start_time": f"{session.isoformat()} 09:30:00",
                        "end_time": f"{session.isoformat()} 09:31:00",
                    }
                )
                probe = binding.adapter.probe_with_parameters(
                    probe_parameters, dataset=binding.dataset
                )
            else:
                probe = binding.adapter.probe()
            contract_hash = content_fingerprint(
                asdict(binding.adapter.contract_for(binding.dataset)),
                domain="factor-lab/research-os/v1/physical-canary-source-contract",
            )
            probe_hash = content_fingerprint(
                {
                    "source_id": probe.source_id,
                    "health": probe.health.value,
                    "datasets": list(probe.datasets),
                    "message": probe.message,
                    "contract_hash": contract_hash,
                    "adapter_type": type(binding.adapter).__name__,
                },
                domain="factor-lab/research-os/v1/physical-canary-real-probe",
            )
            status = (
                CapabilityStatus.ACCEPTED
                if probe.health is SourceHealth.HEALTHY and not self.controlled_test
                else CapabilityStatus.DEGRADED
                if probe.health is SourceHealth.HEALTHY
                else CapabilityStatus.UNAVAILABLE
            )
            self.production_ledger.upsert_capability(
                CapabilityRecord(
                    source_id=binding.adapter.source_id,
                    dataset=binding.dataset,
                    status=status,
                    contract_hash=contract_hash,
                    probe_hash=probe_hash,
                    fields=tuple(binding.adapter.contract_for(binding.dataset).field_map),
                    detail=(
                        "bounded real SourceAdapter probe passed"
                        if status is CapabilityStatus.ACCEPTED
                        else "controlled test adapter evidence rejected"
                        if self.controlled_test and probe.health is SourceHealth.HEALTHY
                        else "bounded SourceAdapter probe failed closed"
                    ),
                    probed_at=probe.checked_at,
                )
            )
            if probe.health is not SourceHealth.HEALTHY:
                raise PhysicalCanaryAdmissionError(
                    f"source capability probe failed for {identity[0]}/{identity[1]}"
                )
            results[f"{identity[0]}:{identity[1]}"] = probe_hash
        return dict(sorted(results.items()))

    def _source_generation(self, binding: _SourceBinding) -> str:
        provenance = getattr(self.production_evidence, "provenance", None)
        public = getattr(provenance, "public_dict", None)
        measured = public() if callable(public) else {}
        implementation_build = (
            str(measured.get("build_identity_hash") or "")
            if isinstance(measured, Mapping)
            else ""
        )
        if len(implementation_build) != 64 or any(
            character not in "0123456789abcdef"
            for character in implementation_build
        ):
            # Direct unit-test construction can inspect generation behavior,
            # but production run admission rejects absent measured provenance
            # before any partition is touched.
            implementation_build = (
                "controlled_test_adapter"
                if self.controlled_test
                else "unadmitted_nonproduction_build"
            )
        evaluator_identity = self._evaluator_identity()
        evaluator_identity_hash = content_fingerprint(
            evaluator_identity,
            domain="factor-lab/research-os/v1/physical-canary-source-generation-owner",
        )
        return content_fingerprint(
            {
                "binding": binding.descriptor(),
                # Bronze currently stores the adapter-normalized frame, not
                # the untouched wire response. Bind its identity to the source
                # implementation build so parser changes cannot reuse bytes
                # emitted by an older evaluator.
                "adapter_implementation_build_identity": implementation_build,
                # The source-only provenance above is insufficient when the
                # exact same tree/lock is rebuilt on a different base image or
                # executed under a different reviewed runtime contract.  The
                # stable evaluator identity carries the OCI image/base and
                # runtime contract without volatile proof timestamps or
                # container IDs, so those changes force new Bronze bytes while
                # a harmless attestation refresh does not.
                "stable_evaluator_identity_hash": evaluator_identity_hash,
            },
            domain="factor-lab/research-os/v1/physical-canary-source-generation",
        )

    def _partition_source(self, binding: _SourceBinding) -> str:
        prefix = "engtest" if self.controlled_test else "engcan"
        source = "".join(
            character if character.isalnum() or character in "_.:-" else "_"
            for character in binding.adapter.source_id
        )
        if self.controlled_test:
            return f"{prefix}_{source}"[:80]
        contract_generation = self._source_generation(binding)[:24]
        source_budget = 80 - len(prefix) - len(contract_generation) - 2
        return f"{prefix}_{source[:source_budget]}_{contract_generation}"

    def _stage_source(self) -> str:
        if self.controlled_test:
            return "engineering_canary_test"
        generation = content_fingerprint(
            self._evaluator_identity(),
            domain="factor-lab/research-os/v1/physical-canary-stage-generation",
        )[:24]
        return f"engineering_canary_{generation}"

    @staticmethod
    def _provider_generation_source_matches(
        source_identity: str,
        provider_source_id: str,
    ) -> bool:
        """Match one provider identity without accepting nested-name prefixes."""

        normalized = "".join(
            character if character.isalnum() or character in "_.:-" else "_"
            for character in provider_source_id
        )
        return physical_canary_generation_source_matches(
            source_identity,
            f"engcan_{normalized}",
        )

    @staticmethod
    def _legacy_incident_stage(record: PartitionRecord) -> IncidentStage:
        if record.identity.dataset == "silver_accepted":
            return IncidentStage.SILVER
        if record.identity.dataset == "dq_accepted":
            return IncidentStage.DATA_QUALITY
        if record.identity.dataset.startswith("gold_"):
            return IncidentStage.GOLD
        return IncidentStage.SOURCE

    @classmethod
    def _legacy_failure_incident_matches_partition(
        cls,
        incident: IncidentRecord,
        legacy: PartitionRecord,
    ) -> bool:
        """Bind only the terminal failure emitted for this exact partition.

        ``silver_accepted`` is the one deliberate cross-stage case: a
        reconciliation dispute is a Silver incident, while a DQ rejection
        terminalizes that same Silver partition as quarantined and records a
        DATA_QUALITY incident.  The generation-isolation bridge itself remains
        classified by the partition dataset.
        """

        if legacy.identity.dataset == "silver_accepted":
            expected = {
                PartitionStatus.DISPUTED: (
                    IncidentStage.SILVER,
                    "source_reconciliation_disputed",
                ),
                PartitionStatus.QUARANTINED: (
                    IncidentStage.DATA_QUALITY,
                    "data_quality_blocked",
                ),
            }.get(legacy.status)
            return bool(
                expected is not None
                and (incident.stage, incident.error_code) == expected
            )
        return incident.stage is cls._legacy_incident_stage(legacy)

    def _current_generation_for_legacy(
        self,
        record: PartitionRecord,
        *,
        source_generations: Mapping[tuple[str, str], str],
        stage_generation: str,
    ) -> str | None:
        """Return the one current immutable identity that can replace ``record``."""

        identity = record.identity
        if physical_canary_generation_source_matches(
            identity.source_id, "engineering_canary"
        ):
            if identity.dataset in {
                "silver_accepted",
                "dq_accepted",
                "gold_mark",
                "gold_execution",
            }:
                return stage_generation
            return None
        candidates = [
            generation
            for (source_id, dataset), generation in source_generations.items()
            if dataset == identity.dataset
            and self._provider_generation_source_matches(
                identity.source_id, source_id
            )
        ]
        return candidates[0] if len(candidates) == 1 else None

    def _verified_replacement_evidence(
        self, record: PartitionRecord | None
    ) -> Mapping[str, Any] | None:
        """Verify every field the readiness auditor will bind into a bridge."""

        if (
            record is None
            or record.status is not PartitionStatus.SUCCEEDED
            or record.completed_at is None
            or not record.run_id
            or not record.output_snapshot_id
            or not record.output_hash
        ):
            return None
        try:
            reference, _frame = self._load_snapshot_frame(record.output_snapshot_id)
        except Exception:
            # A missing/corrupt object, unreadable Parquet payload or catalog
            # mismatch means there is no admissible replacement evidence.  The
            # incident deliberately remains OPEN for the next audit.
            return None
        snapshot_record = self.catalog.get_snapshot(reference.snapshot_id)
        if physical_canary_partition_binding_errors(
            record,
            snapshot_record=snapshot_record,
            get_snapshot=self.catalog.get_snapshot,
            audit_now=_aware(self._now(), name="now"),
            controlled_test=self.controlled_test,
        ):
            return None
        manifest = reference.manifest
        lineage = record.details.get("claim_lineage")
        lineage_hash = str(record.details.get("claim_lineage_hash") or "")
        if not isinstance(lineage, Mapping):
            return None
        identity = lineage.get("partition_identity")
        stage_lineage = lineage.get("stage_lineage")
        expected_lineage_hash = content_fingerprint(
            dict(lineage),
            domain=(
                "factor-lab/research-os/v1/"
                "physical-canary-partition-claim-lineage"
            ),
        )
        if not (
            isinstance(identity, Mapping)
            and isinstance(stage_lineage, Mapping)
            and lineage_hash == expected_lineage_hash
            and lineage.get("incident_stage")
            == self._legacy_incident_stage(record).value
            and str(lineage.get("attempted_input_hash") or "")
            == str(record.input_hash or "")
            and identity.get("source_id") == record.identity.source_id
            and identity.get("dataset") == record.identity.dataset
            and identity.get("partition_key") == record.identity.partition_key
            and identity.get("partition_run_id")
            == record.identity.partition_run_id
        ):
            return None
        role = str(
            record.details.get("role")
            or record.details.get("gold_role")
            or stage_lineage.get("role")
            or stage_lineage.get("gold_role")
            or ""
        )
        if not role:
            lineage_source = str(stage_lineage.get("source_id") or "")
            lineage_dataset = str(stage_lineage.get("dataset") or "")
            if lineage_source and lineage_dataset:
                role = f"{lineage_source}_{lineage_dataset}"
        if not (
            isinstance(manifest, Mapping)
            and str(manifest.get("run_id") or "") == record.run_id
            and str(manifest.get("trade_date") or "")
            == record.identity.partition_key
            and (
                not record.details.get("run_id")
                or record.details.get("run_id") == record.run_id
            )
            and (not role or str(manifest.get("role") or "") == role)
        ):
            return None
        if record.identity.dataset == "dq_accepted":
            report = reference.manifest.get("quality_report")
            if not isinstance(report, Mapping):
                return None
            expected_hash = content_fingerprint(
                dict(report),
                domain="factor-lab/research-os/v1/physical-canary-dq-report",
            )
        else:
            expected_hash = reference.content_hash
        if record.output_hash != expected_hash:
            return None
        return {
            "replacement_partition_run_id": record.identity.partition_run_id,
            "replacement_output_snapshot_id": reference.snapshot_id,
            "replacement_output_hash": record.output_hash,
        }

    def _resolve_retried_partition_incidents(
        self, replacement: PartitionRecord
    ) -> None:
        """Resolve only retryable FAILED attempts superseded by the same identity."""

        evidence = self._verified_replacement_evidence(replacement)
        completed_at = replacement.completed_at
        if evidence is None or completed_at is None:
            return
        replacement_claim_lineage_hash = str(
            replacement.details.get("claim_lineage_hash") or ""
        )
        replacement_lineage = replacement.details.get("claim_lineage")
        if not isinstance(replacement_lineage, Mapping):
            return
        expected_stage = str(replacement_lineage.get("incident_stage") or "")
        expected_parent_hashes = tuple(
            map(str, replacement_lineage.get("parent_evidence_hashes") or ())
        )
        expected_source_ids = (replacement.identity.source_id,)
        terminal_retry_codes = {
            IncidentStage.SOURCE.value: {"source_fetch_failed"},
            IncidentStage.SILVER.value: set(),
            IncidentStage.DATA_QUALITY.value: set(),
            IncidentStage.GOLD.value: {"gold_publication_failed"},
        }
        candidates = tuple(
            incident
            for incident in self.production_ledger.iter_incidents(
                status=IncidentStatus.OPEN
            )
            if incident.partition_run_id
            == replacement.identity.partition_run_id
        )
        for incident in candidates:
            origin_matches = bool(
                incident.partition_run_id
                == replacement.identity.partition_run_id
                and incident.partition_key == replacement.identity.partition_key
                and incident.stage.value == expected_stage
                and incident.source_ids == expected_source_ids
                and incident.evidence_hashes == expected_parent_hashes
                and incident.payload.get("claim_lineage") == replacement_lineage
                and incident.payload.get("claim_lineage_hash")
                == replacement_claim_lineage_hash
                and replacement_claim_lineage_hash
                and incident.occurred_at <= completed_at
            )
            claim_failure = bool(
                origin_matches
                and incident.error_code == f"{expected_stage}_partition_claim_failed"
                and incident.payload.get("claim_attempt_status")
                == PartitionStatus.FAILED.value
                and incident.payload.get("partition_terminalized") is False
                and incident.payload.get("attempted_input_hash")
                == replacement.input_hash
            )
            terminal_failure = bool(
                origin_matches
                and replacement.attempts >= 2
                and incident.error_code
                in terminal_retry_codes.get(expected_stage, set())
                and incident.payload.get("partition_terminalized") is True
                and incident.payload.get("partition_terminal_status")
                == PartitionStatus.FAILED.value
            )
            if claim_failure:
                self.production_ledger.resolve_incident(
                    incident.incident_id,
                    resolved_at=max(completed_at, incident.occurred_at),
                    evidence={
                        "disposition": "resolved_by_exact_claim_lineage_success",
                        "successful_attempt": replacement.attempts,
                        "claim_lineage_hash": replacement_claim_lineage_hash,
                        **evidence,
                    },
                    superseded=False,
                )
                continue
            if not terminal_failure:
                continue
            self.production_ledger.resolve_incident(
                incident.incident_id,
                resolved_at=max(completed_at, incident.occurred_at),
                evidence={
                    "disposition": "resolved_by_successful_partition_retry",
                    "successful_attempt": replacement.attempts,
                    **evidence,
                },
                superseded=False,
            )

    def _supersede_partition_incidents(
        self,
        *,
        legacy: PartitionRecord,
        replacement: PartitionRecord | None,
        current_source_id: str,
    ) -> None:
        if replacement is not None and (
            replacement.identity.source_id != current_source_id
            or replacement.identity.dataset != legacy.identity.dataset
            or replacement.identity.partition_key != legacy.identity.partition_key
        ):
            return
        evidence = self._verified_replacement_evidence(replacement)
        completed_at = replacement.completed_at if replacement is not None else None
        if (
            evidence is None
            or replacement is None
            or completed_at is None
            or legacy.status
            not in {
                PartitionStatus.SUCCEEDED,
                PartitionStatus.DISPUTED,
                PartitionStatus.QUARANTINED,
                PartitionStatus.FAILED,
            }
            or legacy.updated_at > completed_at
        ):
            return
        output_hash = str(legacy.output_hash or "").lower()
        expected_evidence_hashes = (
            (output_hash,)
            if len(output_hash) == 64
            and all(character in "0123456789abcdef" for character in output_hash)
            else ()
        )
        expected_payload = {
            "legacy_source_id": legacy.identity.source_id,
            "legacy_status": legacy.status.value,
            "current_source_id": current_source_id,
            "dataset": legacy.identity.dataset,
        }
        expected_stage = self._legacy_incident_stage(legacy)
        self.production_ledger.record_resolved_incident(
            partition_key=legacy.identity.partition_key,
            stage=expected_stage,
            error_code="legacy_canary_generation_isolated",
            message=(
                "older physical canary generation is isolated by a verified "
                "same-dataset and same-date replacement"
            ),
            occurred_at=legacy.updated_at,
            resolved_at=completed_at,
            partition_run_id=legacy.identity.partition_run_id,
            source_ids=(legacy.identity.source_id,),
            evidence_hashes=expected_evidence_hashes,
            payload=expected_payload,
            resolution={
                "disposition": "superseded_by_verified_canary_generation",
                "legacy_partition_run_id": legacy.identity.partition_run_id,
                "legacy_source_id": legacy.identity.source_id,
                "current_source_id": current_source_id,
                **evidence,
            },
            superseded=True,
        )
        if legacy.status not in {
            PartitionStatus.FAILED,
            PartitionStatus.DISPUTED,
            PartitionStatus.QUARANTINED,
        }:
            return
        legacy_lineage = legacy.details.get("claim_lineage")
        legacy_lineage_hash = str(legacy.details.get("claim_lineage_hash") or "")
        if not isinstance(legacy_lineage, Mapping) or not legacy_lineage_hash:
            return
        expected_labels = _labels(
            physical_source_attested=self.physical_source_attested,
            controlled_test=self.controlled_test,
        )
        candidates = tuple(
            incident
            for incident in self.production_ledger.iter_incidents(
                status=IncidentStatus.OPEN
            )
            if (
                incident.partition_run_id
                == legacy.identity.partition_run_id
                and incident.partition_key == legacy.identity.partition_key
                and self._legacy_failure_incident_matches_partition(
                    incident, legacy
                )
                and incident.source_ids == (legacy.identity.source_id,)
                and incident.evidence_hashes
                == tuple(
                    map(
                        str,
                        legacy_lineage.get("parent_evidence_hashes") or (),
                    )
                )
                and incident.error_code != "legacy_canary_generation_isolated"
                and incident.occurred_at == legacy.updated_at
                and incident.occurred_at <= completed_at
                and all(
                    incident.payload.get(key) == value
                    for key, value in expected_labels.items()
                )
                and incident.payload.get("run_id") == legacy.run_id
                and incident.payload.get("partition_terminalized") is True
                and incident.payload.get("partition_terminal_status")
                == legacy.status.value
                and incident.payload.get("claim_lineage") == legacy_lineage
                and incident.payload.get("claim_lineage_hash")
                == legacy_lineage_hash
                and not incident.payload.get("domain_incident_id")
            )
        )
        for incident in candidates:
            origin_payload = dict(incident.payload)
            origin_payload.pop("resolution", None)
            self.production_ledger.record_resolved_incident(
                partition_key=incident.partition_key,
                stage=incident.stage,
                error_code=incident.error_code,
                message=incident.message,
                occurred_at=incident.occurred_at,
                resolved_at=completed_at,
                partition_run_id=incident.partition_run_id,
                source_ids=incident.source_ids,
                evidence_hashes=incident.evidence_hashes,
                payload=origin_payload,
                resolution={
                    "disposition": (
                        "superseded_by_verified_canary_generation_failure"
                    ),
                    "legacy_partition_run_id": (
                        legacy.identity.partition_run_id
                    ),
                    "legacy_source_id": legacy.identity.source_id,
                    "current_source_id": current_source_id,
                    **evidence,
                },
                superseded=True,
            )

    def _audit_legacy_source_generations(
        self, *, replacement: PartitionRecord | None = None
    ) -> None:
        """Atomically bind old generations only to an already verified replacement.

        No target-generation incident is pre-created while its replacement is
        absent.  Otherwise an interrupted generation B would leave an OPEN claim
        that generation C could neither satisfy nor safely rewrite.
        """

        if self.controlled_test:
            return
        source_generations: dict[tuple[str, str], str] = {
            (binding.adapter.source_id, binding.dataset): self._partition_source(binding)
            for binding in self.bindings
        }
        records = self.production_ledger.list_partitions(limit=100_000)
        has_stage_generation = any(
            physical_canary_generation_source_matches(
                item.identity.source_id, "engineering_canary"
            )
            for item in records
        )
        stage_generation = self._stage_source() if has_stage_generation else ""
        for record in records:
            identity = record.identity
            current_generation = self._current_generation_for_legacy(
                record,
                source_generations=source_generations,
                stage_generation=stage_generation,
            )
            if current_generation is None or identity.source_id == current_generation:
                continue
            current_identity = PartitionIdentity(
                current_generation, identity.dataset, identity.partition_key
            )
            if replacement is not None and (
                replacement.identity.partition_run_id
                != current_identity.partition_run_id
            ):
                continue
            current = replacement or self.production_ledger.get_partition(
                current_identity
            )
            self._supersede_partition_incidents(
                legacy=record,
                replacement=current,
                current_source_id=current_generation,
            )

    def _audit_prelease_gold_semantics(
        self, *, session: date, role: str, replacement: PartitionRecord
    ) -> None:
        """Close pre-lease Gold semantic failures after causal Gold replacement."""

        if self.controlled_test:
            return
        expected_dataset = f"gold_{role}"
        if (
            replacement.identity.source_id != self._stage_source()
            or replacement.identity.dataset != expected_dataset
            or replacement.identity.partition_key != session.isoformat()
        ):
            return
        evidence = self._verified_replacement_evidence(replacement)
        if evidence is None or replacement.completed_at is None:
            return
        replacement_lineage = {
            "stage_source": replacement.details.get("stage_source"),
            "silver_snapshot_id": replacement.details.get("silver_snapshot_id"),
            "calendar_hash": replacement.details.get("calendar_hash"),
            "gold_role": replacement.details.get("role"),
            "attempted_gold_input_hash": replacement.details.get(
                "attempted_gold_input_hash"
            ),
        }
        if any(not str(value or "") for value in replacement_lineage.values()):
            return
        candidates = tuple(
            incident
            for incident in self.production_ledger.iter_incidents(
                status=IncidentStatus.OPEN
            )
            if (
                incident.partition_run_id is None
                and incident.partition_key == session.isoformat()
                and incident.stage is IncidentStage.GOLD
                and incident.error_code == "gold_market_semantics_rejected"
                and incident.occurred_at < replacement.completed_at
            )
        )
        for incident in candidates:
            projected = bool(incident.payload.get("projected", False))
            if (projected and role != "execution") or (
                not projected and role != "mark"
            ):
                continue
            incident_lineage = {
                key: incident.payload.get(key) for key in replacement_lineage
            }
            if incident_lineage != replacement_lineage:
                continue
            self.production_ledger.resolve_incident(
                incident.incident_id,
                resolved_at=max(replacement.completed_at, incident.occurred_at),
                evidence={
                    "disposition": "superseded_by_verified_gold_partition",
                    "gold_role": role,
                    **evidence,
                },
                superseded=True,
            )

    def _snapshot_cache_path(self, snapshot_id: str) -> Path:
        return self.local_root / "rehydrated" / f"{snapshot_id}.parquet"

    @staticmethod
    def _archived_object(reference: DataSnapshotRef) -> ArchivedObject:
        manifest = reference.manifest
        payload = manifest.get("physical_object") if isinstance(manifest, Mapping) else None
        if not isinstance(payload, Mapping):
            raise PhysicalCanaryDataRejected("snapshot has no physical object evidence")
        try:
            return ArchivedObject(
                uri=str(payload["uri"]),
                key=str(payload["key"]),
                sha256=str(payload["sha256"]),
                size_bytes=int(payload["size_bytes"]),
                reused=bool(payload.get("reused", False)),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise PhysicalCanaryDataRejected(
                "snapshot physical object evidence is malformed"
            ) from exc

    def _object_evidence(self, reference: DataSnapshotRef) -> PhysicalObjectEvidence:
        archived = self._archived_object(reference)
        manifest = reference.manifest
        return PhysicalObjectEvidence(
            snapshot_id=reference.snapshot_id,
            tier=reference.tier.value,
            role=str(manifest.get("role") or ""),
            trade_date=str(manifest.get("trade_date") or ""),
            uri=archived.uri,
            content_hash=reference.content_hash,
            object_sha256=archived.sha256,
            size_bytes=archived.size_bytes,
        )

    def _validate_snapshot_reference(
        self,
        *,
        requested_snapshot_id: str,
        reference: DataSnapshotRef,
        manifest: Mapping[str, Any],
        archived: ArchivedObject,
    ) -> None:
        if reference.snapshot_id != requested_snapshot_id:
            raise PhysicalCanaryDataRejected(
                "catalog snapshot identity differs from requested snapshot"
            )
        expected_content_hash = _snapshot_content_hash(manifest)
        if reference.content_hash != expected_content_hash:
            raise PhysicalCanaryDataRejected(
                "catalog snapshot content hash differs from its manifest"
            )
        if reference.snapshot_id != _snapshot_id(
            reference.tier,
            expected_content_hash,
        ):
            raise PhysicalCanaryDataRejected(
                "catalog snapshot ID differs from its canonical identity"
            )
        if (
            manifest.get("snapshot_reference_schema")
            != PHYSICAL_CANARY_SNAPSHOT_REFERENCE_SCHEMA
        ):
            raise PhysicalCanaryDataRejected(
                "catalog snapshot reference schema is missing or unsupported"
            )
        manifest_as_of = manifest.get("snapshot_as_of")
        if not isinstance(manifest_as_of, str):
            raise PhysicalCanaryDataRejected(
                "catalog snapshot as_of binding is missing or malformed"
            )
        try:
            parsed_as_of = datetime.fromisoformat(manifest_as_of)
            canonical_manifest_as_of = _aware(
                parsed_as_of,
                name="manifest snapshot_as_of",
            ).isoformat()
        except (TypeError, ValueError) as exc:
            raise PhysicalCanaryDataRejected(
                "catalog snapshot as_of binding is missing or malformed"
            ) from exc
        canonical_reference_as_of = _aware(
            reference.as_of,
            name="snapshot reference as_of",
        ).isoformat()
        if manifest_as_of != canonical_manifest_as_of:
            raise PhysicalCanaryDataRejected(
                "catalog snapshot as_of binding is not canonical UTC"
            )
        if reference.as_of.isoformat() != canonical_reference_as_of:
            raise PhysicalCanaryDataRejected(
                "catalog snapshot reference as_of is not canonical UTC"
            )
        if canonical_manifest_as_of != canonical_reference_as_of:
            raise PhysicalCanaryDataRejected(
                "catalog snapshot as_of differs from its manifest"
            )
        if manifest.get("tier") != reference.tier.value:
            raise PhysicalCanaryDataRejected(
                "catalog snapshot tier differs from its manifest"
            )
        manifest_parents = manifest.get("parent_snapshot_ids")
        if (
            not isinstance(manifest_parents, (list, tuple))
            or any(not isinstance(item, str) for item in manifest_parents)
            or tuple(manifest_parents) != reference.parent_snapshot_ids
        ):
            raise PhysicalCanaryDataRejected(
                "catalog snapshot parents differ from its manifest"
            )
        if reference.uri != archived.uri:
            raise PhysicalCanaryDataRejected(
                "catalog snapshot URI differs from its manifest"
            )
        if reference.quality_status is not DataQualityStatus.ACCEPTED:
            raise PhysicalCanaryDataRejected(
                "physical canary snapshot is not accepted"
            )
        expected_trust_labels = (
            "physical_engineering_canary"
            if self.physical_source_attested
            else "controlled_test_adapter",
            EVIDENCE_SCOPE,
            "retrospective_physical_replay",
        )
        if reference.trust_labels != expected_trust_labels:
            raise PhysicalCanaryDataRejected(
                "catalog snapshot trust labels differ from runtime authority"
            )
        if (
            manifest.get("canary_execution_contract_hash")
            != self.canary_execution_contract_hash
        ):
            raise PhysicalCanaryDataRejected(
                "catalog snapshot execution contract differs from runtime authority"
            )

    def _load_snapshot_frame(self, snapshot_id: str) -> tuple[DataSnapshotRef, pd.DataFrame]:
        record = self.catalog.get_snapshot(snapshot_id)
        if record is None:
            raise PhysicalCanaryDataRejected("partition snapshot disappeared from the catalog")
        reference = record.reference
        manifest = dict(reference.manifest or {})
        archived = self._archived_object(reference)
        self._validate_snapshot_reference(
            requested_snapshot_id=snapshot_id,
            reference=reference,
            manifest=manifest,
            archived=archived,
        )
        labels = _labels(
            physical_source_attested=self.physical_source_attested,
            controlled_test=self.controlled_test,
        )
        for key, expected in labels.items():
            if manifest.get(key) != expected:
                raise PhysicalCanaryDataRejected(
                    "persisted canary snapshot labels differ from runtime authority"
                )
        expected_prefix = f"s3://{self.archive.bucket}/{CANARY_OBJECT_PREFIX}/"
        if not archived.uri.startswith(expected_prefix):
            raise PhysicalCanaryDataRejected(
                "canary snapshot escaped the isolated physical object prefix"
            )
        restored = self.archive.restore_file(
            archived,
            self._snapshot_cache_path(reference.snapshot_id),
        )
        if (
            restored.uri != archived.uri
            or restored.sha256 != archived.sha256
            or restored.size_bytes != archived.size_bytes
        ):
            raise PhysicalCanaryDataRejected(
                "restored snapshot physical object evidence differs"
            )
        legacy_digest = "frame_digest_schema" not in manifest
        digest_schema = manifest.get("frame_digest_schema")
        if not legacy_digest and digest_schema != _PERSISTED_FRAME_DIGEST_SCHEMA:
            raise PhysicalCanaryDataRejected(
                "restored snapshot frame digest schema is unsupported"
            )
        pinned = _read_pinned_parquet_frame(
            restored.path,
            expected_sha256=archived.sha256,
            expected_size_bytes=archived.size_bytes,
        )
        frame = pinned.frame
        observed_frame_digest = (
            _legacy_frame_digest(frame)
            if legacy_digest
            else _persisted_frame_digest(pinned.table)
        )
        if observed_frame_digest != str(manifest.get("frame_digest") or ""):
            raise PhysicalCanaryDataRejected("restored snapshot frame digest differs")
        if (
            int(manifest.get("rows", -1)) != pinned.table.num_rows
            or list(map(str, manifest.get("columns") or ()))
            != list(map(str, pinned.table.column_names))
        ):
            raise PhysicalCanaryDataRejected(
                "restored snapshot frame shape differs"
            )
        return reference, frame

    def _publish_frame_snapshot(
        self,
        *,
        run_id: str,
        session: date,
        tier: SnapshotTier,
        role: str,
        frame: pd.DataFrame,
        parent_snapshot_ids: Sequence[str],
        as_of: datetime,
        extra_manifest: Mapping[str, Any] | None = None,
    ) -> DataSnapshotRef:
        local_directory = (
            self.local_root
            / run_id
            / tier.value
            / session.isoformat()
            / role
        )
        persisted = _write_frame_once(local_directory, frame)
        archived = self.archive.archive_file(
            persisted.path,
            logical_path=(
                Path(run_id)
                / tier.value
                / f"trade_date={session.isoformat()}"
                / role
            ).as_posix(),
        )
        if (
            archived.sha256 != persisted.object_sha256
            or archived.size_bytes != persisted.size_bytes
        ):
            raise PhysicalCanaryDataRejected(
                "archived canary Parquet differs from verified persisted bytes"
            )
        canonical_as_of = _aware(as_of, name="snapshot as_of")
        manifest = {
            **_labels(
                physical_source_attested=self.physical_source_attested,
                controlled_test=self.controlled_test,
            ),
            "run_id": run_id,
            "canary_execution_contract_hash": (
                self.canary_execution_contract_hash
            ),
            "tier": tier.value,
            "role": role,
            "trade_date": session.isoformat(),
            "rows": persisted.rows,
            "columns": list(persisted.columns),
            "frame_digest_schema": _PERSISTED_FRAME_DIGEST_SCHEMA,
            "frame_digest": persisted.frame_digest,
            "physical_object": archived.to_dict(),
            "parent_snapshot_ids": list(parent_snapshot_ids),
            "published_at": _aware(self._now(), name="now").isoformat(),
            **dict(extra_manifest or {}),
            "snapshot_reference_schema": (
                PHYSICAL_CANARY_SNAPSHOT_REFERENCE_SCHEMA
            ),
            "snapshot_as_of": canonical_as_of.isoformat(),
        }
        content_hash = _snapshot_content_hash(manifest)
        reference = DataSnapshotRef(
            snapshot_id=_snapshot_id(tier, content_hash),
            tier=tier,
            uri=archived.uri,
            content_hash=content_hash,
            parent_snapshot_ids=tuple(parent_snapshot_ids),
            as_of=canonical_as_of,
            quality_status=DataQualityStatus.ACCEPTED,
            trust_labels=(
                "physical_engineering_canary"
                if self.physical_source_attested
                else "controlled_test_adapter",
                EVIDENCE_SCOPE,
                "retrospective_physical_replay",
            ),
            manifest=manifest,
        )
        self.catalog.register_snapshot(reference)
        loaded = self.catalog.get_snapshot(reference.snapshot_id)
        if loaded is None or loaded.reference != reference:
            raise PhysicalCanaryDataRejected("catalog snapshot differs after publication")
        return reference

    def _fetch_binding(
        self,
        binding: _SourceBinding,
        *,
        session: date,
        tickers: Sequence[str] = (),
        lease_heartbeat: Callable[[], None] | None = None,
    ) -> SourceBatch:
        if binding.per_ticker:
            if len(tickers) != SECURITY_COUNT:
                raise PhysicalCanaryDataRejected(
                    "opening execution fetch requires the fixed 50-security universe"
                )
            children: list[SourceBatch] = []
            rows: list[pd.DataFrame] = []
            for ticker in tickers:
                if lease_heartbeat is not None:
                    lease_heartbeat()
                child = binding.adapter.fetch(binding.request(session, ticker=ticker))
                if lease_heartbeat is not None:
                    lease_heartbeat()
                frame = child.frame.copy()
                event_column = child.contract.event_time_field
                parsed = pd.to_datetime(frame[event_column], errors="coerce")
                if parsed.isna().any():
                    raise SourceContractError("opening execution contains invalid event times")
                local = parsed.map(_event_timestamp).map(
                    lambda value: value.tz_convert(_SHANGHAI)
                )
                frame = frame.loc[
                    local.map(lambda value: value.date() == session and value.time() == time(9, 30))
                ].copy()
                if len(frame) != 1:
                    raise SourceContractError(
                        "opening execution requires exactly one physical 09:30 row per ticker"
                    )
                children.append(child)
                rows.append(frame)
            combined = pd.concat(rows, ignore_index=True)
            validate_source_frame(combined, binding.adapter.contract_for(binding.dataset))
            revision = content_fingerprint(
                [item.vendor_revision for item in children],
                domain="factor-lab/research-os/v1/physical-canary-combined-source",
            )
            batch = SourceBatch(
                source_id=binding.adapter.source_id,
                source_priority=binding.adapter.priority,
                dataset=binding.dataset,
                frame=combined,
                ingested_at=max(item.ingested_at for item in children),
                vendor_revision=revision,
                contract=binding.adapter.contract_for(binding.dataset),
                request=FetchRequest(
                    dataset=binding.dataset,
                    parameters={
                        "trade_date": session.isoformat(),
                        "ticker_set_hash": content_fingerprint(
                            list(tickers),
                            domain="factor-lab/research-os/v1/physical-canary-tickers",
                        ),
                    },
                    fields=binding.request_fields,
                ),
                lineage={
                    "adapter": type(binding.adapter).__name__,
                    "child_vendor_revisions": [item.vendor_revision for item in children],
                },
            )
        else:
            if lease_heartbeat is not None:
                lease_heartbeat()
            batch = binding.adapter.fetch(binding.request(session))
            if lease_heartbeat is not None:
                lease_heartbeat()
        assert_point_in_time_columns(list(batch.frame.columns))
        event_dates = pd.to_datetime(
            batch.frame[batch.contract.event_time_field], errors="coerce"
        )
        if event_dates.isna().any() or {
            pd.Timestamp(value).date() for value in event_dates
        } != {session}:
            raise SourceContractError(
                f"{binding.dataset} returned rows outside the requested partition"
            )
        return batch

    def _batch_from_snapshot(
        self,
        *,
        binding: _SourceBinding,
        reference: DataSnapshotRef,
        frame: pd.DataFrame,
        session: date,
        tickers: Sequence[str],
    ) -> SourceBatch:
        batch = reference.manifest.get("source_batch")
        if not isinstance(batch, Mapping):
            raise PhysicalCanaryDataRejected("Bronze source lineage is absent")
        request = binding.request(session, ticker=(tickers[0] if binding.per_ticker else None))
        if binding.per_ticker:
            request = FetchRequest(
                dataset=binding.dataset,
                parameters={
                    "trade_date": session.isoformat(),
                    "ticker_set_hash": content_fingerprint(
                        list(tickers),
                        domain="factor-lab/research-os/v1/physical-canary-tickers",
                    ),
                },
                fields=binding.request_fields,
            )
        return SourceBatch(
            source_id=str(batch["source_id"]),
            source_priority=int(batch["source_priority"]),
            dataset=binding.dataset,
            frame=frame,
            ingested_at=datetime.fromisoformat(str(batch["ingested_at"])),
            vendor_revision=str(batch["vendor_revision"]),
            contract=binding.adapter.contract_for(binding.dataset),
            request=request,
            lineage=dict(batch.get("lineage") or {}),
        )

    def _claim_partition(
        self,
        identity: PartitionIdentity,
        *,
        input_hash: str,
        run_id: str,
        details: Mapping[str, Any],
    ) -> tuple[PartitionRecord, PartitionLease | None]:
        now = _aware(self._now(), name="now")
        record = self.production_ledger.ensure_partition(
            identity,
            created_at=now,
            input_hash=input_hash,
            details={
                **_labels(
                    physical_source_attested=self.physical_source_attested,
                    controlled_test=self.controlled_test,
                ),
                "run_id": run_id,
                **dict(details),
            },
        )
        if record.status is PartitionStatus.SUCCEEDED:
            return record, None
        if record.status in {PartitionStatus.DISPUTED, PartitionStatus.QUARANTINED}:
            raise PhysicalCanaryDataRejected(
                f"terminal {record.status.value} canary partition blocks continuation"
            )
        lease = self.production_ledger.claim(
            identity=identity,
            owner=f"physical-canary-{run_id[-48:]}",
            now=now,
            lease_for=timedelta(minutes=30),
        )
        if lease is None:
            raise PhysicalCanaryError("canary partition is leased by another worker")
        return record, lease

    @staticmethod
    def _partition_claim_lineage(
        identity: PartitionIdentity,
        *,
        input_hash: str,
        stage: IncidentStage,
        details: Mapping[str, Any],
        evidence_hashes: Sequence[str],
    ) -> dict[str, Any]:
        return {
            "incident_stage": stage.value,
            "partition_identity": {
                "source_id": identity.source_id,
                "dataset": identity.dataset,
                "partition_key": identity.partition_key,
                "partition_run_id": identity.partition_run_id,
            },
            "attempted_input_hash": input_hash,
            "parent_evidence_hashes": sorted(set(evidence_hashes)),
            "stage_lineage": dict(details),
        }

    @staticmethod
    def _claim_lineage_evidence(record: PartitionRecord) -> dict[str, Any]:
        lineage = record.details.get("claim_lineage")
        lineage_hash = record.details.get("claim_lineage_hash")
        if not isinstance(lineage, Mapping) or not str(lineage_hash or ""):
            return {}
        return {
            "claim_lineage": dict(lineage),
            "claim_lineage_hash": str(lineage_hash),
        }

    def _claim_stage_partition(
        self,
        identity: PartitionIdentity,
        *,
        input_hash: str,
        run_id: str,
        stage: IncidentStage,
        details: Mapping[str, Any],
        evidence_hashes: Sequence[str] = (),
    ) -> tuple[PartitionRecord, PartitionLease | None]:
        """Claim a stage partition and persist pre-lease failures independently.

        The claim lineage is immutable promotion evidence.  It is attached to
        both the partition and any claim failure, so a later success can close
        only the incident produced by the exact same attempted inputs.
        """

        lineage = self._partition_claim_lineage(
            identity,
            input_hash=input_hash,
            stage=stage,
            details=details,
            evidence_hashes=evidence_hashes,
        )
        lineage_hash = content_fingerprint(
            lineage,
            domain="factor-lab/research-os/v1/physical-canary-partition-claim-lineage",
        )
        enriched_details = {
            **dict(details),
            "claim_lineage": lineage,
            "claim_lineage_hash": lineage_hash,
        }
        try:
            return self._claim_partition(
                identity,
                input_hash=input_hash,
                run_id=run_id,
                details=enriched_details,
            )
        except Exception as exc:
            now = _aware(self._now(), name="now")
            open_incidents = self.production_ledger.iter_incidents(
                status=IncidentStatus.OPEN
            )
            try:
                already_open = any(
                    incident.partition_run_id == identity.partition_run_id
                    and incident.payload.get("claim_attempt_status")
                    == PartitionStatus.FAILED.value
                    and incident.payload.get("claim_lineage_hash") == lineage_hash
                    for incident in open_incidents
                )
            finally:
                close = getattr(open_incidents, "close", None)
                if close is not None:
                    close()
            if not already_open:
                self.production_ledger.record_incident(
                    partition_key=identity.partition_key,
                    stage=stage,
                    error_code=f"{stage.value}_partition_claim_failed",
                    message=_clean_error(exc),
                    occurred_at=now,
                    partition_run_id=identity.partition_run_id,
                    source_ids=(identity.source_id,),
                    evidence_hashes=evidence_hashes,
                    payload={
                        **_labels(
                            physical_source_attested=self.physical_source_attested,
                            controlled_test=self.controlled_test,
                        ),
                        "run_id": run_id,
                        "failure_type": type(exc).__name__,
                        "claim_attempt_status": PartitionStatus.FAILED.value,
                        "partition_terminalized": False,
                        "partition_terminal_status": None,
                        "attempted_input_hash": input_hash,
                        "claim_lineage_hash": lineage_hash,
                        "claim_lineage": lineage,
                        "partition_identity": dict(lineage["partition_identity"]),
                        "stage_lineage": dict(details),
                    },
                )
            raise

    def _finish_failed_partition(
        self,
        lease: PartitionLease,
        *,
        run_id: str,
        status: PartitionStatus,
        stage: IncidentStage,
        error_code: str,
        exc: BaseException,
        evidence_hashes: Sequence[str] = (),
    ) -> None:
        now = _aware(self._now(), name="now")
        terminalized = False
        terminalization_failure_type: str | None = None
        try:
            self.production_ledger.finish(
                lease,
                status=status,
                completed_at=now,
                run_id=run_id,
                error_code=error_code,
                error=_clean_error(exc),
                details={
                    **_labels(
                        physical_source_attested=self.physical_source_attested,
                        controlled_test=self.controlled_test,
                    ),
                    "run_id": run_id,
                    "failure_type": type(exc).__name__,
                    **self._claim_lineage_evidence(lease.record),
                },
            )
            terminalized = True
        except Exception as terminalization_error:
            # Incident authority must not depend on a still-valid worker lease.
            # An expired lease leaves the partition reclaimable, while this
            # independently persisted incident prevents an orphaned snapshot
            # or failed publication from becoming promotion evidence.
            terminalization_failure_type = type(terminalization_error).__name__
        self.production_ledger.record_incident(
            partition_key=lease.identity.partition_key,
            stage=stage,
            error_code=error_code,
            message=_clean_error(exc),
            occurred_at=now,
            partition_run_id=lease.identity.partition_run_id,
            source_ids=(lease.identity.source_id,),
            evidence_hashes=evidence_hashes,
            payload={
                **_labels(
                    physical_source_attested=self.physical_source_attested,
                    controlled_test=self.controlled_test,
                ),
                "run_id": run_id,
                "partition_terminalized": terminalized,
                "partition_terminal_status": status.value,
                "terminalization_failure_type": terminalization_failure_type,
                **self._claim_lineage_evidence(lease.record),
            },
        )

    def _materialize_bronze(
        self,
        binding: _SourceBinding,
        *,
        run_id: str,
        session: date,
        tickers: Sequence[str] = (),
    ) -> tuple[SourceBatch, DataSnapshotRef, PartitionRecord]:
        source_generation = self._source_generation(binding)
        identity = PartitionIdentity(
            self._partition_source(binding), binding.dataset, session.isoformat()
        )
        input_hash = content_fingerprint(
            {
                "binding": binding.descriptor(),
                "trade_date": session,
                "tickers": list(tickers),
                "evidence_schema": EVIDENCE_SCHEMA,
                "source_generation_hash": source_generation,
            },
            domain="factor-lab/research-os/v1/physical-canary-bronze-input",
        )
        claim_details = {
            "stage": "bronze",
            "source_id": binding.adapter.source_id,
            "dataset": binding.dataset,
            "evidence_schema": EVIDENCE_SCHEMA,
            "source_generation_hash": source_generation,
            "binding_descriptor": binding.descriptor(),
            "tickers": list(tickers),
        }
        record, lease = self._claim_stage_partition(
            identity,
            input_hash=input_hash,
            run_id=run_id,
            stage=IncidentStage.SOURCE,
            details=claim_details,
        )
        if lease is None:
            if not record.output_snapshot_id:
                raise PhysicalCanaryDataRejected(
                    "successful Bronze partition has no snapshot"
                )
            reference, frame = self._load_snapshot_frame(record.output_snapshot_id)
            if reference.manifest.get("source_generation_hash") != source_generation:
                raise PhysicalCanaryDataRejected(
                    "successful Bronze partition belongs to another source build"
                )
            self._resolve_retried_partition_incidents(record)
            self._audit_legacy_source_generations(replacement=record)
            return (
                self._batch_from_snapshot(
                    binding=binding,
                    reference=reference,
                    frame=frame,
                    session=session,
                    tickers=tickers,
                ),
                reference,
                record,
            )
        active_lease = lease

        def renew_lease() -> None:
            nonlocal active_lease
            active_lease = self.production_ledger.renew(
                active_lease,
                now=_aware(self._now(), name="now"),
                lease_for=timedelta(minutes=30),
            )

        try:
            batch = self._fetch_binding(
                binding,
                session=session,
                tickers=tickers,
                lease_heartbeat=renew_lease,
            )
            renew_lease()
            reference = self._publish_frame_snapshot(
                run_id=run_id,
                session=session,
                tier=SnapshotTier.BRONZE,
                role=f"{binding.adapter.source_id}_{binding.dataset}",
                frame=batch.frame,
                parent_snapshot_ids=(),
                as_of=batch.ingested_at,
                extra_manifest={
                    "source_generation_hash": source_generation,
                    "source_batch": {
                        "source_id": batch.source_id,
                        "source_priority": batch.source_priority,
                        "dataset": batch.dataset,
                        "ingested_at": batch.ingested_at.isoformat(),
                        "vendor_revision": batch.vendor_revision,
                        "request": {
                            "dataset": batch.request.dataset,
                            "parameters": dict(batch.request.parameters),
                            "fields": list(batch.request.fields),
                        },
                        "lineage": dict(batch.lineage),
                    },
                    "contract_hash": content_fingerprint(
                        asdict(batch.contract),
                        domain="factor-lab/research-os/v1/physical-canary-source-contract",
                    ),
                },
            )
            renew_lease()
            completed = self.production_ledger.finish(
                active_lease,
                status=PartitionStatus.SUCCEEDED,
                completed_at=_aware(self._now(), name="now"),
                run_id=run_id,
                output_snapshot_id=reference.snapshot_id,
                output_hash=reference.content_hash,
                vendor_revision=batch.vendor_revision,
                details={
                    **_labels(
                        physical_source_attested=self.physical_source_attested,
                        controlled_test=self.controlled_test,
                    ),
                    "run_id": run_id,
                    "stage": "bronze",
                    **self._claim_lineage_evidence(record),
                    "physical_object": self._archived_object(reference).to_dict(),
                },
            )
            self._resolve_retried_partition_incidents(completed)
            self._audit_legacy_source_generations(replacement=completed)
            return batch, reference, completed
        except Exception as exc:
            if isinstance(exc, SourceContractError):
                code = (
                    "historical_st_empty"
                    if binding.dataset == "historical_st" and "no rows" in str(exc).lower()
                    else "source_contract_violation"
                )
                status = PartitionStatus.QUARANTINED
            else:
                code = "source_fetch_failed"
                status = PartitionStatus.FAILED
            self._finish_failed_partition(
                active_lease,
                run_id=run_id,
                status=status,
                stage=IncidentStage.SOURCE,
                error_code=code,
                exc=exc,
            )
            raise

    def _record_dq_acceptance(
        self,
        *,
        run_id: str,
        session: date,
        silver: DataSnapshotRef,
        report: Mapping[str, Any],
    ) -> PartitionRecord:
        identity = PartitionIdentity(
            self._stage_source(), "dq_accepted", session.isoformat()
        )
        report_hash = content_fingerprint(
            dict(report),
            domain="factor-lab/research-os/v1/physical-canary-dq-report",
        )
        dq_input_hash = content_fingerprint(
            {
                "silver_snapshot_id": silver.snapshot_id,
                "quality_report_hash": report_hash,
            },
            domain="factor-lab/research-os/v1/physical-canary-dq-input",
        )
        record, lease = self._claim_stage_partition(
            identity,
            input_hash=dq_input_hash,
            run_id=run_id,
            stage=IncidentStage.DATA_QUALITY,
            details={
                "stage": "data_quality",
                "silver_snapshot_id": silver.snapshot_id,
                "quality_report_hash": report_hash,
            },
            evidence_hashes=(silver.content_hash,),
        )
        if lease is None:
            self._resolve_retried_partition_incidents(record)
            self._audit_legacy_source_generations(replacement=record)
            return record
        completed = self.production_ledger.finish(
            lease,
            status=PartitionStatus.SUCCEEDED,
            completed_at=_aware(self._now(), name="now"),
            run_id=run_id,
            output_snapshot_id=silver.snapshot_id,
            output_hash=report_hash,
            details={
                **_labels(
                    physical_source_attested=self.physical_source_attested,
                    controlled_test=self.controlled_test,
                ),
                "run_id": run_id,
                "stage": "data_quality",
                **self._claim_lineage_evidence(record),
                "quality_status": "accepted",
                "quality_report": dict(report),
            },
        )
        self._resolve_retried_partition_incidents(completed)
        self._audit_legacy_source_generations(replacement=completed)
        return completed

    def _materialize_silver(
        self,
        *,
        run_id: str,
        session: date,
        inputs: Sequence[tuple[_SourceBinding, SourceBatch, DataSnapshotRef]],
        projected: bool,
    ) -> tuple[DataSnapshotRef, PartitionRecord, PartitionRecord]:
        parent_ids = tuple(sorted(item[2].snapshot_id for item in inputs))
        evaluator_identity = self._evaluator_identity()
        identity = PartitionIdentity(
            self._stage_source(), "silver_accepted", session.isoformat()
        )
        comparison_policies = {
            key: asdict(value)
            for key, value in production_comparison_policies().items()
        }
        input_hash = content_fingerprint(
            {
                "parent_snapshot_ids": list(parent_ids),
                "projected": projected,
                "comparison_policies": comparison_policies,
                "evaluator_identity": evaluator_identity,
            },
            domain="factor-lab/research-os/v1/physical-canary-silver-input",
        )
        record, lease = self._claim_stage_partition(
            identity,
            input_hash=input_hash,
            run_id=run_id,
            stage=IncidentStage.SILVER,
            details={
                "stage": "silver",
                "parent_snapshot_ids": list(parent_ids),
                "projected": projected,
                "comparison_policies": comparison_policies,
                "evaluator_identity": evaluator_identity,
            },
            evidence_hashes=tuple(item[2].content_hash for item in inputs),
        )
        if lease is None:
            if not record.output_snapshot_id:
                raise PhysicalCanaryDataRejected(
                    "successful Silver partition has no snapshot"
                )
            reference, _ = self._load_snapshot_frame(record.output_snapshot_id)
            report = dict(reference.manifest.get("quality_report") or {})
            dq = self._record_dq_acceptance(
                run_id=run_id,
                session=session,
                silver=reference,
                report=report,
            )
            self._resolve_retried_partition_incidents(record)
            self._audit_legacy_source_generations(replacement=record)
            return reference, record, dq
        try:
            canonical: list[pd.DataFrame] = []
            raw_datasets: set[str] = set()
            historical_st_rows = 0
            for binding, batch, _reference in inputs:
                raw_datasets.add(batch.dataset)
                if batch.dataset == "historical_st":
                    historical_st_rows += len(batch.frame)
                canonical.append(
                    canonicalize_batch(
                        batch,
                        binding.canonicalization,
                        availability_resolver=(
                            None
                            if binding.canonicalization.available_at_column
                            else binding.availability_resolver
                        ),
                    )
                )
            observations = pd.concat(canonical, ignore_index=True)
            reconciled = reconcile_observations(
                observations,
                policies=production_comparison_policies(),
            )
            if not reconciled.promotion_allowed:
                raise PhysicalCanaryDataRejected(
                    "physical source reconciliation is disputed or quarantined"
                )
            required = {
                "trade_calendar",
                "daily",
                "adj_factor",
                "historical_st",
                "stock_limit",
            }
            if projected:
                required.add("opening_execution")
            missing = sorted(required - raw_datasets)
            if missing:
                raise PhysicalCanaryDataRejected(
                    f"Silver partition omits required datasets: {missing}"
                )
            if historical_st_rows <= 0:
                raise PhysicalCanaryDataRejected(
                    "open session historical ST evidence is empty"
                )
            calendar = reconciled.accepted.loc[
                (reconciled.accepted["dataset"] == "trade_calendar")
                & (reconciled.accepted["field"] == "is_open")
            ]
            if calendar.empty or any(float(value) != 1.0 for value in calendar["value"]):
                raise PhysicalCanaryDataRejected(
                    "calendar reconciliation did not accept this open session"
                )
            try:
                compared_calendar_sources = tuple(
                    {
                        str(source)
                        for source in json.loads(str(raw_sources))
                        if str(source)
                    }
                    for raw_sources in calendar["compared_sources"]
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                raise PhysicalCanaryDataRejected(
                    "calendar reconciliation source evidence is malformed"
                ) from None
            if any(len(sources) < 2 for sources in compared_calendar_sources):
                raise PhysicalCanaryDataRejected(
                    "calendar reconciliation lacks two independent sources"
                )
            report = {
                "schema_version": EVIDENCE_SCHEMA,
                "status": "accepted",
                "trade_date": session.isoformat(),
                "historical_st_rows": historical_st_rows,
                "datasets": sorted(raw_datasets),
                "reconciliation": dict(reconciled.audit),
                "evaluator_identity": evaluator_identity,
                "parent_snapshot_ids": list(parent_ids),
                **_labels(
                    physical_source_attested=self.physical_source_attested,
                    controlled_test=self.controlled_test,
                ),
            }
            stored = _silver_storage_frame(reconciled.accepted)
            reference = self._publish_frame_snapshot(
                run_id=run_id,
                session=session,
                tier=SnapshotTier.SILVER,
                role="accepted_reconciled",
                frame=stored,
                parent_snapshot_ids=parent_ids,
                as_of=max(item[1].ingested_at for item in inputs),
                extra_manifest={
                    "quality_report": report,
                    "value_encoding": "json_scalar_string",
                },
            )
            completed = self.production_ledger.finish(
                lease,
                status=PartitionStatus.SUCCEEDED,
                completed_at=_aware(self._now(), name="now"),
                run_id=run_id,
                output_snapshot_id=reference.snapshot_id,
                output_hash=reference.content_hash,
                details={
                    **_labels(
                        physical_source_attested=self.physical_source_attested,
                        controlled_test=self.controlled_test,
                    ),
                    "run_id": run_id,
                    "stage": "silver",
                    **self._claim_lineage_evidence(record),
                    "quality_status": "accepted",
                    "physical_object": self._archived_object(reference).to_dict(),
                    "reconciliation_audit": dict(reconciled.audit),
                    "evaluator_identity": evaluator_identity,
                },
            )
            self._resolve_retried_partition_incidents(completed)
            self._audit_legacy_source_generations(replacement=completed)
            dq = self._record_dq_acceptance(
                run_id=run_id,
                session=session,
                silver=reference,
                report=report,
            )
            return reference, completed, dq
        except Exception as exc:
            status = (
                PartitionStatus.DISPUTED
                if "reconciliation" in str(exc).lower()
                else PartitionStatus.QUARANTINED
            )
            code = (
                "source_reconciliation_disputed"
                if status is PartitionStatus.DISPUTED
                else "data_quality_blocked"
            )
            self._finish_failed_partition(
                lease,
                run_id=run_id,
                status=status,
                stage=(
                    IncidentStage.SILVER
                    if status is PartitionStatus.DISPUTED
                    else IncidentStage.DATA_QUALITY
                ),
                error_code=code,
                exc=exc,
                evidence_hashes=tuple(item[2].content_hash for item in inputs),
            )
            raise

    @staticmethod
    def _ticker_column(frame: pd.DataFrame) -> str:
        for name in ("ts_code", "ticker", "stock_code"):
            if name in frame.columns:
                return name
        raise PhysicalCanaryDataRejected("market dataset has no security identifier")

    @staticmethod
    def _dataset_frame(
        inputs: Sequence[tuple[_SourceBinding, SourceBatch, DataSnapshotRef]],
        dataset: str,
    ) -> pd.DataFrame:
        matches = [batch.frame.copy() for _binding, batch, _reference in inputs if batch.dataset == dataset]
        if len(matches) != 1:
            raise PhysicalCanaryDataRejected(f"expected exactly one {dataset} source frame")
        return matches[0]

    def _select_tickers(
        self,
        inputs: Sequence[tuple[_SourceBinding, SourceBatch, DataSnapshotRef]],
    ) -> tuple[str, ...]:
        daily = self._dataset_frame(inputs, "daily")
        historical_st = self._dataset_frame(inputs, "historical_st")
        ticker_column = self._ticker_column(daily)
        st_column = self._ticker_column(historical_st)
        if "amount" not in daily.columns:
            raise PhysicalCanaryDataRejected("daily source has no physical amount field")
        daily = daily.copy()
        daily["_ticker"] = daily[ticker_column].astype(str).str.strip()
        daily["_amount"] = pd.to_numeric(daily["amount"], errors="coerce")
        ordinary = daily["_ticker"].map(
            lambda value: (
                len(value.split(".", 1)[0]) == 6
                and value.endswith((".SH", ".SZ"))
                and value.split(".", 1)[0][0] in {"0", "3", "6"}
            )
        )
        st_tickers = {
            str(value).strip() for value in historical_st[st_column] if str(value).strip()
        }
        eligible = daily.loc[
            ordinary
            & daily["_amount"].map(lambda value: isfinite(float(value)) and float(value) > 0)
            & ~daily["_ticker"].isin(st_tickers)
        ].copy()
        eligible = eligible.sort_values(["_amount", "_ticker"], ascending=[False, True])
        tickers = tuple(eligible["_ticker"].drop_duplicates().head(SECURITY_COUNT))
        if len(tickers) != SECURITY_COUNT:
            raise PhysicalCanaryDataRejected(
                "seed session cannot form the fixed 50-security physical universe"
            )
        return tickers

    @staticmethod
    def _indexed_market_frame(frame: pd.DataFrame, tickers: Sequence[str]) -> pd.DataFrame:
        ticker_column = PhysicalEngineeringCanaryService._ticker_column(frame)
        result = frame.copy()
        result["_ticker"] = result[ticker_column].astype(str).str.strip()
        result = result.loc[result["_ticker"].isin(tickers)].copy()
        if result["_ticker"].duplicated().any() or set(result["_ticker"]) != set(tickers):
            raise PhysicalCanaryDataRejected(
                "physical session does not cover each fixed canary security exactly once"
            )
        return result.set_index("_ticker").loc[list(tickers)]

    def _build_gold_bars(
        self,
        *,
        session: date,
        inputs: Sequence[tuple[_SourceBinding, SourceBatch, DataSnapshotRef]],
        tickers: Sequence[str],
        projected: bool,
        amount_history: dict[str, list[float]],
        close_history: dict[str, list[float]],
    ) -> pd.DataFrame:
        daily = self._indexed_market_frame(self._dataset_frame(inputs, "daily"), tickers)
        adjustment = self._indexed_market_frame(
            self._dataset_frame(inputs, "adj_factor"), tickers
        )
        limits = self._indexed_market_frame(
            self._dataset_frame(inputs, "stock_limit"), tickers
        )
        historical_st = self._dataset_frame(inputs, "historical_st")
        st_column = self._ticker_column(historical_st)
        selected_st = sorted(
            set(map(str, historical_st[st_column].tolist())) & set(tickers)
        )
        if selected_st:
            raise PhysicalCanaryDataRejected(
                "a fixed canary security entered historical ST status"
            )
        opening = (
            self._indexed_market_frame(
                self._dataset_frame(inputs, "opening_execution"), tickers
            )
            if projected
            else None
        )
        rows: list[dict[str, Any]] = []
        for ticker in tickers:
            daily_row = daily.loc[ticker]
            adjustment_row = adjustment.loc[ticker]
            limit_row = limits.loc[ticker]
            factor = float(pd.to_numeric(adjustment_row["adj_factor"], errors="coerce"))
            daily_open = float(pd.to_numeric(daily_row["open"], errors="coerce"))
            close = float(pd.to_numeric(daily_row["close"], errors="coerce"))
            high = float(pd.to_numeric(daily_row["high"], errors="coerce"))
            low = float(pd.to_numeric(daily_row["low"], errors="coerce"))
            amount = float(pd.to_numeric(daily_row["amount"], errors="coerce"))
            numbers = (factor, daily_open, close, high, low, amount)
            if any(not isfinite(value) or value <= 0 for value in numbers):
                raise PhysicalCanaryDataRejected(
                    "Gold market inputs must be finite and positive"
                )
            if projected:
                assert opening is not None
                opening_row = opening.loc[ticker]
                physical_open = float(pd.to_numeric(opening_row["open"], errors="coerce"))
                if not isfinite(physical_open) or physical_open <= 0:
                    raise PhysicalCanaryDataRejected(
                        "physical 09:30 opening price is invalid"
                    )
                minute_high = float(
                    pd.to_numeric(opening_row["high"], errors="coerce")
                )
                minute_low = float(
                    pd.to_numeric(opening_row["low"], errors="coerce")
                )
                if not all(
                    isfinite(value) and value > 0
                    for value in (minute_high, minute_low)
                ) or not minute_low - 0.01 <= physical_open <= minute_high + 0.01:
                    raise PhysicalCanaryDataRejected(
                        "physical 09:30 open is outside its minute-bar range"
                    )
                # A session-level daily open and the 09:30 one-minute bar open
                # are related but distinct market events (the former may bind
                # the opening auction).  Do not pretend they are two vendors
                # reporting the same field.  Instead, retain the one-tick
                # difference as audit evidence and fail closed if the minute
                # execution price is outside the accepted daily range.
                if not low - 0.01 <= physical_open <= high + 0.01:
                    raise PhysicalCanaryDataRejected(
                        "physical 09:30 open is outside the accepted daily range"
                    )
                execution_event = _event_timestamp(
                    opening_row["trade_time"]
                    if "trade_time" in opening_row.index
                    else opening_row["event_time"]
                )
                execution_available = _event_timestamp(
                    opening_row["available_at"]
                    if "available_at" in opening_row.index
                    else execution_event
                )
            else:
                physical_open = daily_open
                minute_high = daily_open
                minute_low = daily_open
                execution_event = pd.Timestamp(_session_time(session, time(9, 30)))
                execution_available = execution_event
            if execution_event.tz_convert(_SHANGHAI).time() != time(9, 30):
                raise PhysicalCanaryDataRejected(
                    "opening execution is not the physical 09:30 observation"
                )
            open_adjusted = physical_open * factor
            close_adjusted = close * factor
            amount_history.setdefault(ticker, []).append(amount * 1_000.0)
            close_history.setdefault(ticker, []).append(close_adjusted)
            adv = float(sum(amount_history[ticker][-20:]) / len(amount_history[ticker][-20:]))
            closes = close_history[ticker][-21:]
            returns = [
                closes[index] / closes[index - 1] - 1.0
                for index in range(1, len(closes))
                if closes[index - 1] > 0
            ]
            if len(returns) >= 2:
                volatility = float(pd.Series(returns[-20:], dtype=float).std(ddof=1))
            else:
                volatility = abs(high - low) / close
            volatility = max(float(volatility), 0.0001)
            upper = float(pd.to_numeric(limit_row["up_limit"], errors="coerce"))
            lower = float(pd.to_numeric(limit_row["down_limit"], errors="coerce"))
            if not all(isfinite(value) and value > 0 for value in (upper, lower)):
                raise PhysicalCanaryDataRejected("stock limit evidence is invalid")
            one_price = max(high, daily_open, close) - min(low, daily_open, close) <= 0.01
            daily_open_difference = abs(physical_open - daily_open)
            rows.append(
                {
                    "ticker": ticker,
                    "trade_date": session.isoformat(),
                    "open_adj": open_adjusted,
                    "close_adj": close_adjusted,
                    "daily_session_open_raw": daily_open,
                    "execution_minute_open_raw": physical_open,
                    "execution_minute_low_raw": minute_low,
                    "execution_minute_high_raw": minute_high,
                    "execution_vs_daily_open_abs_diff": daily_open_difference,
                    "execution_vs_daily_open_one_tick_match": bool(
                        daily_open_difference <= 0.01
                    ),
                    "adv_20": adv,
                    "volatility_20": volatility,
                    "execution_event_time": execution_event.isoformat(),
                    "execution_available_at": execution_available.isoformat(),
                    "mark_event_time": _session_time(session, time(15, 0)).isoformat(),
                    "mark_available_at": _session_time(session, time(18, 30)).isoformat(),
                    "is_suspended": False,
                    "is_one_price_limit_up": bool(one_price and abs(close - upper) <= 0.01),
                    "is_one_price_limit_down": bool(one_price and abs(close - lower) <= 0.01),
                    "is_delisted": False,
                }
            )
        frame = pd.DataFrame(rows)
        assert_point_in_time_columns(list(frame.columns))
        if len(frame) != SECURITY_COUNT or frame["ticker"].nunique() != SECURITY_COUNT:
            raise PhysicalCanaryDataRejected("Gold canary frame is not exactly 50 securities")
        return frame

    def _materialize_gold(
        self,
        *,
        run_id: str,
        session: date,
        role: str,
        frame: pd.DataFrame,
        silver: DataSnapshotRef,
        sessions: Sequence[date],
        as_of: datetime,
    ) -> tuple[DataSnapshotRef, PartitionRecord]:
        dataset = f"gold_{role}"
        stage_source = self._stage_source()
        calendar_hash = _calendar_hash(sessions)
        attempted_gold_input_hash = _gold_attempted_input_hash(
            stage_source=stage_source,
            session=session,
            role=role,
            silver_snapshot_id=silver.snapshot_id,
            calendar_hash=calendar_hash,
        )
        identity = PartitionIdentity(stage_source, dataset, session.isoformat())
        frame_digest = _logical_frame_digest(frame)
        input_hash = content_fingerprint(
            {
                "silver_snapshot_id": silver.snapshot_id,
                "frame_digest": frame_digest,
                "role": role,
                "calendar_hash": calendar_hash,
                "attempted_gold_input_hash": attempted_gold_input_hash,
                "canary_execution_contract_hash": (
                    self.canary_execution_contract_hash
                ),
            },
            domain="factor-lab/research-os/v1/physical-canary-gold-input",
        )
        record, lease = self._claim_stage_partition(
            identity,
            input_hash=input_hash,
            run_id=run_id,
            stage=IncidentStage.GOLD,
            details={
                "stage": "gold",
                "role": role,
                "gold_role": role,
                "stage_source": stage_source,
                "silver_snapshot_id": silver.snapshot_id,
                "frame_digest": frame_digest,
                "calendar_hash": calendar_hash,
                "attempted_gold_input_hash": attempted_gold_input_hash,
                "canary_execution_contract_hash": (
                    self.canary_execution_contract_hash
                ),
            },
            evidence_hashes=(silver.content_hash,),
        )
        if lease is None:
            if not record.output_snapshot_id:
                raise PhysicalCanaryDataRejected("successful Gold partition has no snapshot")
            reference, stored = self._load_snapshot_frame(record.output_snapshot_id)
            if not _canonical_arrow_table(stored).equals(
                _canonical_arrow_table(frame)
            ):
                raise PhysicalCanaryDataRejected("persisted Gold frame differs on resume")
            self._resolve_retried_partition_incidents(record)
            self._audit_legacy_source_generations(replacement=record)
            self._audit_prelease_gold_semantics(
                session=session, role=role, replacement=record
            )
            return reference, record
        try:
            opening_audit = (
                {
                    "comparison_semantics": (
                        "daily_session_open_vs_distinct_09_30_minute_bar_open"
                    ),
                    "one_tick_mismatch_count": int(
                        (~frame["execution_vs_daily_open_one_tick_match"]).sum()
                    ),
                    "maximum_absolute_difference": float(
                        frame["execution_vs_daily_open_abs_diff"].max()
                    ),
                    "daily_range_violation_count": 0,
                    "minute_bar_range_violation_count": 0,
                }
                if role == "execution"
                and {
                    "execution_vs_daily_open_one_tick_match",
                    "execution_vs_daily_open_abs_diff",
                }.issubset(frame.columns)
                else None
            )
            reference = self._publish_frame_snapshot(
                run_id=run_id,
                session=session,
                tier=SnapshotTier.GOLD,
                role=role,
                frame=frame,
                parent_snapshot_ids=(silver.snapshot_id,),
                as_of=as_of,
                extra_manifest={
                    "trading_calendar": {
                        "quality_status": "accepted",
                        "source": "postgresql_accepted_calendar_plus_real_source_reconciliation",
                        "sessions": [item.isoformat() for item in sessions],
                        "content_hash": _calendar_hash(sessions),
                    },
                    "opening_execution_formal_ready": self.opening_execution_formal_ready,
                    "canary_execution_contract_hash": (
                        self.canary_execution_contract_hash
                    ),
                    "opening_cross_check": opening_audit,
                },
            )
            completed = self.production_ledger.finish(
                lease,
                status=PartitionStatus.SUCCEEDED,
                completed_at=_aware(self._now(), name="now"),
                run_id=run_id,
                output_snapshot_id=reference.snapshot_id,
                output_hash=reference.content_hash,
                details={
                    **_labels(
                        physical_source_attested=self.physical_source_attested,
                        controlled_test=self.controlled_test,
                    ),
                    "run_id": run_id,
                    "stage": "gold",
                    "role": role,
                    **self._claim_lineage_evidence(record),
                    "stage_source": stage_source,
                    "silver_snapshot_id": silver.snapshot_id,
                    "calendar_hash": calendar_hash,
                    "attempted_gold_input_hash": attempted_gold_input_hash,
                    "canary_execution_contract_hash": (
                        self.canary_execution_contract_hash
                    ),
                    "opening_cross_check": opening_audit,
                    "physical_object": self._archived_object(reference).to_dict(),
                },
            )
            self._resolve_retried_partition_incidents(completed)
            self._audit_legacy_source_generations(replacement=completed)
            self._audit_prelease_gold_semantics(
                session=session, role=role, replacement=completed
            )
            return reference, completed
        except Exception as exc:
            self._finish_failed_partition(
                lease,
                run_id=run_id,
                status=PartitionStatus.FAILED,
                stage=IncidentStage.GOLD,
                error_code="gold_publication_failed",
                exc=exc,
                evidence_hashes=(silver.content_hash,),
            )
            raise

    def _existing_account_projection(self, account_id: str, session: date):
        start = datetime.combine(session, time.min, tzinfo=timezone.utc)
        end = datetime.combine(session, time.max, tzinfo=timezone.utc)
        events = self.catalog.list_shadow_events_by_type(
            account_id=account_id,
            event_type="account_projected",
            since=start,
            through=end,
            limit=10,
        )
        if len(events) != 1:
            raise PhysicalCanaryDataRejected(
                "existing canary daily projection is incomplete or ambiguous"
            )
        return events[0]

    def _append_lifecycle(self, *, run_id: str, sleeve_id: str, sessions: Sequence[date]) -> None:
        states: tuple[tuple[LifecycleState | None, LifecycleState, str], ...] = (
            (None, LifecycleState.PROPOSED, "physical engineering canary registered"),
            (
                LifecycleState.PROPOSED,
                LifecycleState.PREREGISTERED,
                "code-authoritative fixed 50x20 non-forward contract",
            ),
            (
                LifecycleState.PREREGISTERED,
                LifecycleState.CANARY,
                "physical SourceAdapter and object-store evidence accepted for engineering only",
            ),
            (
                LifecycleState.CANARY,
                LifecycleState.WALK_FORWARD,
                "retrospective event-ledger plumbing exercise",
            ),
            (
                LifecycleState.WALK_FORWARD,
                LifecycleState.SHADOW,
                "20 verified daily engineering projections completed",
            ),
        )
        for index, (from_state, to_state, cause) in enumerate(states):
            self.catalog.append_lifecycle_event(
                LifecycleEvent(
                    idempotency_key=f"{run_id}:lifecycle:{to_state.value}",
                    sleeve_id=sleeve_id,
                    from_state=from_state,
                    to_state=to_state,
                    cause=cause,
                    occurred_at=_session_time(sessions[0], time(8, 0))
                    + timedelta(minutes=index),
                    evidence={
                        **_labels(
                            physical_source_attested=self.physical_source_attested,
                            controlled_test=self.controlled_test,
                        ),
                        "run_id": run_id,
                    },
                )
            )

    def _project_shadow_session(
        self,
        *,
        binding_id: str,
        account_id: str,
        trade_date: date,
        decision_date: date,
        bars: pd.DataFrame,
        decision_snapshot_id: str | None,
        execution_snapshot_id: str,
        mark_snapshot_id: str,
        rebalanced: bool,
    ) -> ShadowSessionProjection:
        market = bars.copy()
        market["execution_snapshot_id"] = execution_snapshot_id
        market["mark_snapshot_id"] = mark_snapshot_id
        target = (
            {str(ticker): 1.0 / SECURITY_COUNT for ticker in market["ticker"]}
            if rebalanced
            else None
        )
        account = self.catalog.get_shadow_account(account_id)
        if account is None:
            raise PhysicalCanaryDataRejected("physical canary shadow account disappeared")
        authoritative_date = account.as_of.astimezone(_SHANGHAI).date()
        if authoritative_date >= trade_date:
            event = self._existing_account_projection(account_id, trade_date)
            projection = self.shadow_authority.record_engineering_projection(
                role_binding_id=binding_id,
                account_event_hash=event.event_hash,
                trade_date=trade_date,
                recorded_at=_aware(self._now(), name="now"),
            )
        else:
            try:
                result = self.shadow.project_session(
                    account_id=account_id,
                    trade_date=trade_date,
                    market_bars=market,
                    snapshot_bindings=ShadowSnapshotBindings(
                        decision_snapshot_id=decision_snapshot_id,
                        execution_snapshot_id=execution_snapshot_id,
                        mark_snapshot_id=mark_snapshot_id,
                    ),
                    benchmark_return=float(
                        (market["close_adj"] / market["open_adj"] - 1.0).mean()
                    ),
                    target_weights=target,
                    decision_date=decision_date,
                    model_version=(
                        "physical-engineering-canary-equal-weight-v1"
                        if rebalanced
                        else None
                    ),
                    decision_cutoff=_session_time(decision_date, time(18, 30)),
                    session_metrics={
                        **_labels(
                            physical_source_attested=self.physical_source_attested,
                            controlled_test=self.controlled_test,
                        ),
                        "security_count": SECURITY_COUNT,
                        "opening_execution_formal_ready": self.opening_execution_formal_ready,
                    },
                )
            except ShadowStepAlreadyApplied:
                event = self._existing_account_projection(account_id, trade_date)
                projection = self.shadow_authority.record_engineering_projection(
                    role_binding_id=binding_id,
                    account_event_hash=event.event_hash,
                    trade_date=trade_date,
                    recorded_at=_aware(self._now(), name="now"),
                )
            else:
                projection = self.shadow_authority.record_engineering_projection(
                    role_binding_id=binding_id,
                    account_event_hash=result.last_event_hash,
                    trade_date=trade_date,
                    recorded_at=_aware(self._now(), name="now"),
                )
        if (
            projection.evidence_class is not ShadowEvidenceClass.ENGINEERING
            or projection.epoch_id is not None
            or projection.evidence_window_hash is not None
        ):
            raise PhysicalCanaryDataRejected(
                "physical canary projection was misclassified as forward evidence"
            )
        return projection

    def _execution_identity(self, plan_fingerprint: str) -> PartitionIdentity:
        return PartitionIdentity(
            self._stage_source(),
            _EXECUTION_DATASET,
            plan_fingerprint[:32],
        )

    @staticmethod
    def _authoritative_evidence_hash(metadata: Mapping[str, Any]) -> str:
        payload = dict(metadata)
        for field_name in (
            "canary_evidence_hash",
            "physical_object_count",
            "bronze_object_count",
            "silver_object_count",
            "gold_object_count",
        ):
            payload.pop(field_name, None)
        return content_fingerprint(
            payload,
            domain="factor-lab/research-os/v1/physical-canary-authority",
        )

    def _authoritative_result(
        self,
        *,
        metadata: Mapping[str, Any],
        run_id: str,
        run_type: str,
        plan_fingerprint: str,
        sessions: Sequence[date],
        calendar_records: Sequence[PartitionRecord],
        sleeve_id: str,
        account_id: str,
        labels: Mapping[str, Any],
    ) -> PhysicalCanaryResult:
        """Rehydrate one completed result without invoking a child writer."""

        values = dict(metadata)
        expected_identity: Mapping[str, Any] = {
            **dict(labels),
            "evaluator_identity": self._evaluator_identity(),
            "canary_execution_contract_hash": (
                self.canary_execution_contract_hash
            ),
            "run_id": run_id,
            "run_type": run_type,
            "input_fingerprint": plan_fingerprint,
            "calendar_sessions": [item.isoformat() for item in sessions],
            "accepted_calendar_partition_ids": [
                item.identity.partition_run_id for item in calendar_records
            ],
            "accepted_calendar_output_hashes": [
                item.output_hash for item in calendar_records
            ],
            "security_count": SECURITY_COUNT,
            "projected_session_count": PROJECTED_SESSION_COUNT,
            "account_id": account_id,
            "sleeve_id": sleeve_id,
            "sleeve_state": LifecycleState.SHADOW.value,
        }
        if any(values.get(key) != expected for key, expected in expected_identity.items()):
            raise PhysicalCanaryDataRejected(
                "completed physical canary identity or evidence labels differ"
            )
        runtime_proof = values.get("runtime_attestation_evidence")
        if self.controlled_test:
            if runtime_proof != self._runtime_attestation_evidence():
                raise PhysicalCanaryDataRejected(
                    "completed controlled canary runtime proof differs"
                )
        else:
            evaluator_identity = expected_identity["evaluator_identity"]
            stable_deployment = (
                evaluator_identity.get("runtime_deployment")
                if isinstance(evaluator_identity, Mapping)
                else None
            )
            proof_run_id = (
                str(runtime_proof.get("host_attestation_run_id") or "")
                if isinstance(runtime_proof, Mapping)
                else ""
            )
            proof_errors = persisted_attestation_binding_errors(
                run=self.catalog.get_run(proof_run_id) if proof_run_id else None,
                proof=runtime_proof if isinstance(runtime_proof, Mapping) else None,
                stable_deployment=(
                    stable_deployment
                    if isinstance(stable_deployment, Mapping)
                    else None
                ),
            )
            if proof_errors:
                raise PhysicalCanaryDataRejected(
                    "completed physical canary runtime proof is invalid"
                )
        observed_hash = str(values.get("canary_evidence_hash") or "")
        if observed_hash != self._authoritative_evidence_hash(values):
            raise PhysicalCanaryDataRejected(
                "completed physical canary evidence hash differs"
            )

        raw_objects = values.get("snapshot_evidence")
        if not isinstance(raw_objects, list):
            raise PhysicalCanaryDataRejected(
                "completed physical canary object evidence is absent"
            )
        try:
            objects = tuple(
                PhysicalObjectEvidence(
                    snapshot_id=str(item["snapshot_id"]),
                    tier=str(item["tier"]),
                    role=str(item["role"]),
                    trade_date=str(item["trade_date"]),
                    uri=str(item["uri"]),
                    content_hash=str(item["content_hash"]),
                    object_sha256=str(item["object_sha256"]),
                    size_bytes=int(item["size_bytes"]),
                )
                for item in raw_objects
                if isinstance(item, Mapping)
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise PhysicalCanaryDataRejected(
                "completed physical canary object evidence is malformed"
            ) from exc
        if len(objects) != len(raw_objects):
            raise PhysicalCanaryDataRejected(
                "completed physical canary object evidence is malformed"
            )
        tier_counts = {
            SnapshotTier.BRONZE.value: sum(
                item.tier == SnapshotTier.BRONZE.value for item in objects
            ),
            SnapshotTier.SILVER.value: sum(
                item.tier == SnapshotTier.SILVER.value for item in objects
            ),
            SnapshotTier.GOLD.value: sum(
                item.tier == SnapshotTier.GOLD.value for item in objects
            ),
        }
        if (
            int(values.get("physical_object_count") or -1) != len(objects)
            or int(values.get("bronze_object_count") or -1)
            != tier_counts[SnapshotTier.BRONZE.value]
            or int(values.get("silver_object_count") or -1)
            != tier_counts[SnapshotTier.SILVER.value]
            or int(values.get("gold_object_count") or -1)
            != tier_counts[SnapshotTier.GOLD.value]
        ):
            raise PhysicalCanaryDataRejected(
                "completed physical canary object counts differ"
            )
        for item in objects:
            snapshot = self.catalog.get_snapshot(item.snapshot_id)
            if (
                snapshot is None
                or self._object_evidence(snapshot.reference) != item
                or snapshot.reference.manifest.get(
                    "canary_execution_contract_hash"
                )
                != self.canary_execution_contract_hash
            ):
                raise PhysicalCanaryDataRejected(
                    "completed physical canary snapshot authority differs"
                )

        raw_partitions = values.get("partition_run_ids")
        raw_probes = values.get("source_probe_hashes")
        raw_session_hashes = values.get("shadow_session_hashes")
        raw_event_hashes = values.get("shadow_account_event_hashes")
        binding_id = str(values.get("role_binding_id") or "")
        if (
            not isinstance(raw_partitions, list)
            or not raw_partitions
            or not isinstance(raw_probes, Mapping)
            or not isinstance(raw_session_hashes, list)
            or not isinstance(raw_event_hashes, list)
            or not binding_id
        ):
            raise PhysicalCanaryDataRejected(
                "completed physical canary result evidence is incomplete"
            )
        partition_run_ids = tuple(map(str, raw_partitions))
        if len(set(partition_run_ids)) != len(partition_run_ids):
            raise PhysicalCanaryDataRejected(
                "completed physical canary repeats partition authority"
            )
        all_partition_rows = self.production_ledger.list_partitions(limit=100_000)
        if len(all_partition_rows) >= 100_000:
            raise PhysicalCanaryDataRejected(
                "completed physical canary partition authority is truncated"
            )
        partition_authority = {
            item.identity.partition_run_id: item for item in all_partition_rows
        }
        bound_partitions = tuple(
            partition_authority.get(item) for item in partition_run_ids
        )
        if any(item is None for item in bound_partitions):
            raise PhysicalCanaryDataRejected(
                "completed physical canary partition authority is missing"
            )
        authoritative_partitions = tuple(
            item for item in bound_partitions if item is not None
        )
        used_partition_ids: set[str] = set()
        for object_evidence in objects:
            expected_stage = {
                SnapshotTier.BRONZE.value: "bronze",
                SnapshotTier.SILVER.value: "silver",
                SnapshotTier.GOLD.value: "gold",
            }.get(object_evidence.tier)
            matching = tuple(
                item
                for item in authoritative_partitions
                if item.status is PartitionStatus.SUCCEEDED
                and item.run_id == run_id
                and item.output_snapshot_id == object_evidence.snapshot_id
                and item.output_hash == object_evidence.content_hash
                and item.identity.partition_key == object_evidence.trade_date
                and str(item.details.get("stage") or "") == expected_stage
                and str(item.details.get("run_id") or "") == run_id
                and (
                    expected_stage != "gold"
                    or str(item.details.get("role") or "")
                    == object_evidence.role
                )
            )
            if len(matching) != 1:
                raise PhysicalCanaryDataRejected(
                    "completed physical canary snapshot partition binding differs"
                )
            used_partition_ids.add(matching[0].identity.partition_run_id)
            if object_evidence.tier == SnapshotTier.SILVER.value:
                snapshot = self.catalog.get_snapshot(object_evidence.snapshot_id)
                quality_report = (
                    None
                    if snapshot is None
                    else snapshot.reference.manifest.get("quality_report")
                )
                if not isinstance(quality_report, Mapping):
                    raise PhysicalCanaryDataRejected(
                        "completed physical canary Silver quality report is absent"
                    )
                report_hash = content_fingerprint(
                    dict(quality_report),
                    domain="factor-lab/research-os/v1/physical-canary-dq-report",
                )
                matching_dq = tuple(
                    item
                    for item in authoritative_partitions
                    if item.status is PartitionStatus.SUCCEEDED
                    and item.run_id == run_id
                    and item.output_snapshot_id == object_evidence.snapshot_id
                    and item.output_hash == report_hash
                    and item.identity.partition_key == object_evidence.trade_date
                    and str(item.details.get("stage") or "") == "data_quality"
                    and str(item.details.get("run_id") or "") == run_id
                    and item.details.get("quality_status") == "accepted"
                    and item.details.get("quality_report") == quality_report
                )
                if len(matching_dq) != 1:
                    raise PhysicalCanaryDataRejected(
                        "completed physical canary DQ partition binding differs"
                    )
                used_partition_ids.add(
                    matching_dq[0].identity.partition_run_id
                )
        if used_partition_ids != set(partition_run_ids):
            raise PhysicalCanaryDataRejected(
                "completed physical canary partition set is not exact"
            )
        source_probe_hashes = {
            str(key): str(value) for key, value in raw_probes.items()
        }
        if any(
            len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
            for value in source_probe_hashes.values()
        ):
            raise PhysicalCanaryDataRejected(
                "completed physical canary source probe hash is malformed"
            )

        session_keys = [item.isoformat() for item in sessions[1:]]
        # SQLAlchemy is an optional Research OS infrastructure dependency.  Keep
        # it lazy so importing the core SQLite catalog continues to work in the
        # minimal installation; executing a physical canary already requires the
        # production infrastructure extras.
        from sqlalchemy import select
        from sqlalchemy.orm import Session

        with Session(self.shadow_authority.engine) as session:
            rows = list(
                session.scalars(
                    select(orm.ShadowSessionModel)
                    .where(
                        orm.ShadowSessionModel.account_id == account_id,
                        orm.ShadowSessionModel.trade_date.in_(session_keys),
                    )
                    .order_by(orm.ShadowSessionModel.trade_date)
                )
            )
            for row in rows:
                self.shadow_authority._validate_stored_projection(row)
            projections = tuple(
                self.shadow_authority._projection(row) for row in rows
            )
        if (
            [item.trade_date.isoformat() for item in projections] != session_keys
            or [item.session_hash for item in projections]
            != list(map(str, raw_session_hashes))
            or [item.account_event_hash for item in projections]
            != list(map(str, raw_event_hashes))
            or any(
                item.role_binding_id != binding_id
                or item.evidence_class is not ShadowEvidenceClass.ENGINEERING
                or item.epoch_id is not None
                or item.evidence_window_hash is not None
                for item in projections
            )
        ):
            raise PhysicalCanaryDataRejected(
                "completed physical canary shadow authority differs"
            )
        if (
            len(projections) != PROJECTED_SESSION_COUNT
            or not self.catalog.verify_shadow_chain(account_id)
            or self.catalog.latest_lifecycle_state(sleeve_id)
            is not LifecycleState.SHADOW
        ):
            raise PhysicalCanaryDataRejected(
                "completed physical canary event or lifecycle authority differs"
            )

        return PhysicalCanaryResult(
            **dict(labels),
            run_id=run_id,
            run_type=run_type,
            canary_evidence_hash=observed_hash,
            calendar_sessions=tuple(item.isoformat() for item in sessions),
            security_count=SECURITY_COUNT,
            projected_session_count=PROJECTED_SESSION_COUNT,
            sleeve_id=sleeve_id,
            sleeve_state=LifecycleState.SHADOW.value,
            account_id=account_id,
            partition_run_ids=partition_run_ids,
            source_probe_hashes=source_probe_hashes,
            object_evidence=objects,
            shadow_sessions=projections,
        )

    def _terminalize_failed_parent(
        self,
        *,
        run_id: str,
        run_type: str,
        plan_fingerprint: str,
        started_at: datetime,
        sessions: Sequence[date],
        labels: Mapping[str, Any],
        runtime_attestation_evidence: Mapping[str, Any],
        projected_session_count: int,
        exc: BaseException,
    ) -> None:
        self.catalog.save_run(
            RunRecord(
                run_id=run_id,
                run_type=run_type,
                status="failed",
                input_fingerprint=plan_fingerprint,
                started_at=started_at,
                completed_at=_aware(self._now(), name="now"),
                metadata={
                    **dict(labels),
                    "evaluator_identity": self._evaluator_identity(),
                    "canary_execution_contract_hash": (
                        self.canary_execution_contract_hash
                    ),
                    "runtime_attestation_evidence": dict(
                        runtime_attestation_evidence
                    ),
                    "calendar_sessions": [item.isoformat() for item in sessions],
                    "security_count": SECURITY_COUNT,
                    "projected_session_count": projected_session_count,
                    "failure_type": type(exc).__name__,
                },
                error=_clean_error(exc),
            )
        )

    def _plan_fingerprint(
        self,
        *,
        sessions: Sequence[date],
        calendar_records: Sequence[PartitionRecord],
    ) -> str:
        return content_fingerprint(
            {
                "evidence_schema": EVIDENCE_SCHEMA,
                "evaluator_identity": self._evaluator_identity(),
                "canary_execution_contract_hash": (
                    self.canary_execution_contract_hash
                ),
                "controlled_test": self.controlled_test,
                "calendar_sessions": [item.isoformat() for item in sessions],
                "calendar_partition_evidence": [
                    {
                        "partition_run_id": item.identity.partition_run_id,
                        "output_snapshot_id": item.output_snapshot_id,
                        "output_hash": item.output_hash,
                    }
                    for item in calendar_records
                ],
                "bindings": [
                    item.descriptor()
                    for item in sorted(
                        self.bindings,
                        key=lambda value: (
                            value.dataset,
                            value.adapter.priority,
                            value.adapter.source_id,
                        ),
                    )
                ],
                "security_count": SECURITY_COUNT,
                "projected_session_count": PROJECTED_SESSION_COUNT,
            },
            domain="factor-lab/research-os/v1/physical-canary-run-input",
        )

    def run(self, *, as_of: date | None = None) -> PhysicalCanaryResult:
        """Execute or resume the latest bounded physical 50x20 canary."""

        self._assert_runtime_admission()
        # A service instance may outlive the host-attestation freshness window.
        # Refresh the admission proof at the start of every invocation, then
        # freeze the resulting stable evaluator/proof pair for this run. The
        # stable identity still keeps the same plan across proof refreshes.
        if not self.controlled_test:
            self.__dict__.pop("_cached_evaluator_identity", None)
            self.__dict__.pop("_runtime_attestation_proof", None)
        evaluator_identity = self._evaluator_identity()
        runtime_attestation_evidence = self._runtime_attestation_evidence()
        self._audit_legacy_source_generations()
        sessions, calendar_records = self._accepted_sessions(as_of=as_of)
        plan_fingerprint = self._plan_fingerprint(
            sessions=sessions,
            calendar_records=calendar_records,
        )
        run_id = f"physical_canary_{plan_fingerprint[:48]}"
        run_type = CONTROLLED_TEST_RUN_TYPE if self.controlled_test else PHYSICAL_RUN_TYPE
        sleeve_id = f"physical_canary_sleeve_{plan_fingerprint[:32]}"
        account_id = f"physical_canary_account_{plan_fingerprint[:32]}"
        labels = _labels(
            physical_source_attested=self.physical_source_attested,
            controlled_test=self.controlled_test,
        )
        proposed_started_at = _aware(self._now(), name="now")
        proposed_run = RunRecord(
            run_id=run_id,
            run_type=run_type,
            status="running",
            input_fingerprint=plan_fingerprint,
            started_at=proposed_started_at,
            metadata={
                **labels,
                "evaluator_identity": evaluator_identity,
                "canary_execution_contract_hash": (
                    self.canary_execution_contract_hash
                ),
                "runtime_attestation_evidence": runtime_attestation_evidence,
                "calendar_sessions": [item.isoformat() for item in sessions],
                "security_count": SECURITY_COUNT,
                "projected_session_count": 0,
            },
        )
        try:
            existing_run, parent_claimed = self.catalog.claim_run(proposed_run)
        except CatalogConflict as exc:
            # The catalog is the atomic authority.  Translate its immutable
            # terminal outcome into the canary-specific fail-closed error
            # without attempting any child work.
            terminal = self.catalog.get_run(run_id)
            if (
                terminal is not None
                and terminal.run_type == run_type
                and terminal.input_fingerprint == plan_fingerprint
                and terminal.status != "running"
            ):
                if terminal.status == "succeeded":
                    return self._authoritative_result(
                        metadata=terminal.metadata,
                        run_id=run_id,
                        run_type=run_type,
                        plan_fingerprint=plan_fingerprint,
                        sessions=sessions,
                        calendar_records=calendar_records,
                        sleeve_id=sleeve_id,
                        account_id=account_id,
                        labels=labels,
                    )
                raise PhysicalCanaryDataRejected(
                    "physical canary parent run is already terminal "
                    f"({terminal.status}); a retry requires a new attempt run id"
                ) from exc
            raise
        started_at = existing_run.started_at
        if existing_run.status == "succeeded":
            return self._authoritative_result(
                metadata=existing_run.metadata,
                run_id=run_id,
                run_type=run_type,
                plan_fingerprint=plan_fingerprint,
                sessions=sessions,
                calendar_records=calendar_records,
                sleeve_id=sleeve_id,
                account_id=account_id,
                labels=labels,
            )
        if existing_run.status != "running":
            raise PhysicalCanaryDataRejected(
                "physical canary parent run is already terminal "
                f"({existing_run.status}); a retry requires a new attempt run id"
            )

        execution_identity = self._execution_identity(plan_fingerprint)
        execution_lease: PartitionLease | None = None
        try:
            execution_record = self.production_ledger.ensure_partition(
                execution_identity,
                created_at=proposed_started_at,
                input_hash=plan_fingerprint,
                details={
                    **labels,
                    "evaluator_identity": evaluator_identity,
                    "canary_execution_contract_hash": (
                        self.canary_execution_contract_hash
                    ),
                    "runtime_attestation_evidence": runtime_attestation_evidence,
                    "run_id": run_id,
                    "run_type": run_type,
                    "input_fingerprint": plan_fingerprint,
                },
            )
            if execution_record.status is PartitionStatus.SUCCEEDED:
                recovered_metadata = execution_record.details.get(
                    _EXECUTION_RESULT_METADATA
                )
                if not isinstance(recovered_metadata, Mapping):
                    raise PhysicalCanaryDataRejected(
                        "completed canary execution has no authoritative result"
                    )
                recovered = self._authoritative_result(
                    metadata=recovered_metadata,
                    run_id=run_id,
                    run_type=run_type,
                    plan_fingerprint=plan_fingerprint,
                    sessions=sessions,
                    calendar_records=calendar_records,
                    sleeve_id=sleeve_id,
                    account_id=account_id,
                    labels=labels,
                )
                if execution_record.completed_at is None:
                    raise PhysicalCanaryDataRejected(
                        "completed canary execution has no terminal time"
                    )
                self.catalog.save_run(
                    RunRecord(
                        run_id=run_id,
                        run_type=run_type,
                        status="succeeded",
                        input_fingerprint=plan_fingerprint,
                        started_at=started_at,
                        completed_at=execution_record.completed_at,
                        metadata=dict(recovered_metadata),
                    )
                )
                return recovered
            if execution_record.status in {
                PartitionStatus.DISPUTED,
                PartitionStatus.QUARANTINED,
            }:
                raise PhysicalCanaryDataRejected(
                    "terminal canary execution control blocks continuation"
                )
            execution_lease = self.production_ledger.claim(
                identity=execution_identity,
                owner=f"physical-canary-{os.getpid()}-{uuid4().hex[:16]}",
                now=_aware(self._now(), name="now"),
                lease_for=_EXECUTION_LEASE_FOR,
                maximum_attempts=_EXECUTION_MAXIMUM_ATTEMPTS,
            )
            if execution_lease is None:
                latest_parent = self.catalog.get_run(run_id)
                if latest_parent is not None and latest_parent.status == "succeeded":
                    return self._authoritative_result(
                        metadata=latest_parent.metadata,
                        run_id=run_id,
                        run_type=run_type,
                        plan_fingerprint=plan_fingerprint,
                        sessions=sessions,
                        calendar_records=calendar_records,
                        sleeve_id=sleeve_id,
                        account_id=account_id,
                        labels=labels,
                    )
                latest_execution = self.production_ledger.get_partition(
                    execution_identity
                )
                if (
                    latest_execution is not None
                    and latest_execution.status is PartitionStatus.SUCCEEDED
                ):
                    recovered_metadata = latest_execution.details.get(
                        _EXECUTION_RESULT_METADATA
                    )
                    if not isinstance(recovered_metadata, Mapping):
                        raise PhysicalCanaryDataRejected(
                            "completed canary execution has no authoritative result"
                        )
                    recovered = self._authoritative_result(
                        metadata=recovered_metadata,
                        run_id=run_id,
                        run_type=run_type,
                        plan_fingerprint=plan_fingerprint,
                        sessions=sessions,
                        calendar_records=calendar_records,
                        sleeve_id=sleeve_id,
                        account_id=account_id,
                        labels=labels,
                    )
                    if latest_execution.completed_at is None:
                        raise PhysicalCanaryDataRejected(
                            "completed canary execution has no terminal time"
                        )
                    self.catalog.save_run(
                        RunRecord(
                            run_id=run_id,
                            run_type=run_type,
                            status="succeeded",
                            input_fingerprint=plan_fingerprint,
                            started_at=started_at,
                            completed_at=latest_execution.completed_at,
                            metadata=dict(recovered_metadata),
                        )
                    )
                    return recovered
                raise PhysicalCanaryBusy(
                    "physical canary fingerprint is already running; retry later"
                )

            current_parent = self.catalog.get_run(run_id)
            if current_parent is not None and current_parent.status != "running":
                raise PhysicalCanaryDataRejected(
                    "canary execution lease conflicts with a terminal parent"
                )
            if not parent_claimed:
                self.catalog.save_run(
                    RunRecord(
                        run_id=run_id,
                        run_type=run_type,
                        status="running",
                        input_fingerprint=plan_fingerprint,
                        started_at=started_at,
                        metadata={
                            **labels,
                            "evaluator_identity": evaluator_identity,
                            "canary_execution_contract_hash": (
                                self.canary_execution_contract_hash
                            ),
                            "runtime_attestation_evidence": runtime_attestation_evidence,
                            "calendar_sessions": [
                                item.isoformat() for item in sessions
                            ],
                            "security_count": SECURITY_COUNT,
                            "projected_session_count": 0,
                        },
                    )
                )
        except PhysicalCanaryBusy:
            raise
        except Exception as exc:
            if execution_lease is not None:
                try:
                    self.production_ledger.finish(
                        execution_lease,
                        status=PartitionStatus.FAILED,
                        completed_at=_aware(self._now(), name="now"),
                        run_id=run_id,
                        error_code="physical_canary_claim_failed",
                        error=_clean_error(exc),
                        details={
                            **labels,
                            "evaluator_identity": evaluator_identity,
                            "runtime_attestation_evidence": runtime_attestation_evidence,
                            "run_id": run_id,
                            "run_type": run_type,
                            "input_fingerprint": plan_fingerprint,
                            "failure_type": type(exc).__name__,
                        },
                    )
                except Exception:
                    pass
            if parent_claimed or execution_lease is not None:
                try:
                    self._terminalize_failed_parent(
                        run_id=run_id,
                        run_type=run_type,
                        plan_fingerprint=plan_fingerprint,
                        started_at=started_at,
                        sessions=sessions,
                        labels=labels,
                        runtime_attestation_evidence=runtime_attestation_evidence,
                        projected_session_count=0,
                        exc=exc,
                    )
                except Exception:
                    pass
            raise

        partition_records: dict[str, PartitionRecord] = {}
        snapshots: dict[str, DataSnapshotRef] = {}
        projections: list[ShadowSessionProjection] = []
        amount_history: dict[str, list[float]] = {}
        close_history: dict[str, list[float]] = {}
        selected_tickers: tuple[str, ...] = ()
        previous_mark: DataSnapshotRef | None = None
        try:
            if execution_lease is None:  # pragma: no cover - guarded by claim flow.
                raise PhysicalCanaryError("physical canary execution lease is absent")
            probe_hashes = self._probe_sources(sessions)
            if self.catalog.get_shadow_account(account_id) is None:
                self.catalog.create_shadow_account(
                    account_id=account_id,
                    name="Physical engineering canary / non-forward",
                    initial_capital=INITIAL_CAPITAL,
                    opened_at=_session_time(sessions[0], time(8, 30)),
                )
            binding = self.shadow_authority.bind_role(
                role=ShadowRole.SLEEVE,
                role_key=sleeve_id,
                account_id=account_id,
                sleeve_id=sleeve_id,
                bound_at=_aware(self._now(), name="now"),
                metadata={**labels, "run_id": run_id},
            )

            for index, session in enumerate(sessions):
                execution_lease = self.production_ledger.renew(
                    execution_lease,
                    now=_aware(self._now(), name="now"),
                    lease_for=_EXECUTION_LEASE_FOR,
                )
                inputs: list[tuple[_SourceBinding, SourceBatch, DataSnapshotRef]] = []
                for source_binding in self._calendar_bindings():
                    batch, reference, record = self._materialize_bronze(
                        source_binding,
                        run_id=run_id,
                        session=session,
                    )
                    inputs.append((source_binding, batch, reference))
                    snapshots[reference.snapshot_id] = reference
                    partition_records[record.identity.partition_run_id] = record
                for dataset in ("daily", "adj_factor", "historical_st", "stock_limit"):
                    source_binding = self._binding(dataset)
                    batch, reference, record = self._materialize_bronze(
                        source_binding,
                        run_id=run_id,
                        session=session,
                    )
                    inputs.append((source_binding, batch, reference))
                    snapshots[reference.snapshot_id] = reference
                    partition_records[record.identity.partition_run_id] = record
                if index == 0:
                    selected_tickers = self._select_tickers(inputs)
                else:
                    opening_binding = self._binding("opening_execution")
                    batch, reference, record = self._materialize_bronze(
                        opening_binding,
                        run_id=run_id,
                        session=session,
                        tickers=selected_tickers,
                    )
                    inputs.append((opening_binding, batch, reference))
                    snapshots[reference.snapshot_id] = reference
                    partition_records[record.identity.partition_run_id] = record

                silver, silver_record, dq_record = self._materialize_silver(
                    run_id=run_id,
                    session=session,
                    inputs=inputs,
                    projected=index > 0,
                )
                snapshots[silver.snapshot_id] = silver
                partition_records[silver_record.identity.partition_run_id] = silver_record
                partition_records[dq_record.identity.partition_run_id] = dq_record
                try:
                    bars = self._build_gold_bars(
                        session=session,
                        inputs=inputs,
                        tickers=selected_tickers,
                        projected=index > 0,
                        amount_history=amount_history,
                        close_history=close_history,
                    )
                except Exception as exc:
                    gold_role = "execution" if index > 0 else "mark"
                    stage_source = self._stage_source()
                    calendar_hash = _calendar_hash(sessions)
                    attempted_gold_input_hash = _gold_attempted_input_hash(
                        stage_source=stage_source,
                        session=session,
                        role=gold_role,
                        silver_snapshot_id=silver.snapshot_id,
                        calendar_hash=calendar_hash,
                    )
                    self.production_ledger.record_incident(
                        partition_key=session.isoformat(),
                        stage=IncidentStage.GOLD,
                        error_code="gold_market_semantics_rejected",
                        message=_clean_error(exc),
                        occurred_at=_aware(self._now(), name="now"),
                        source_ids=tuple(
                            sorted({item[0].adapter.source_id for item in inputs})
                        ),
                        evidence_hashes=tuple(
                            sorted(
                                {
                                    silver.content_hash,
                                    *(item[2].content_hash for item in inputs),
                                }
                            )
                        ),
                        payload={
                            **labels,
                            "run_id": run_id,
                            "failure_type": type(exc).__name__,
                            "stage_source": stage_source,
                            "silver_snapshot_id": silver.snapshot_id,
                            "calendar_hash": calendar_hash,
                            "gold_role": gold_role,
                            "attempted_gold_input_hash": attempted_gold_input_hash,
                            "projected": index > 0,
                        },
                    )
                    raise
                mark_frame = bars.loc[
                    :,
                    [
                        "ticker",
                        "trade_date",
                        "close_adj",
                        "mark_event_time",
                        "mark_available_at",
                    ],
                ].copy()
                mark, mark_record = self._materialize_gold(
                    run_id=run_id,
                    session=session,
                    role="mark",
                    frame=mark_frame,
                    silver=silver,
                    sessions=sessions,
                    as_of=_session_time(session, time(18, 30)),
                )
                snapshots[mark.snapshot_id] = mark
                partition_records[mark_record.identity.partition_run_id] = mark_record
                if index == 0:
                    previous_mark = mark
                    continue

                execution_frame = bars.loc[
                    :,
                    [
                        "ticker",
                        "trade_date",
                        "open_adj",
                        "daily_session_open_raw",
                        "execution_minute_open_raw",
                        "execution_minute_low_raw",
                        "execution_minute_high_raw",
                        "execution_vs_daily_open_abs_diff",
                        "execution_vs_daily_open_one_tick_match",
                        "adv_20",
                        "volatility_20",
                        "execution_event_time",
                        "execution_available_at",
                        "is_suspended",
                        "is_one_price_limit_up",
                        "is_one_price_limit_down",
                        "is_delisted",
                    ],
                ].copy()
                execution, execution_record = self._materialize_gold(
                    run_id=run_id,
                    session=session,
                    role="execution",
                    frame=execution_frame,
                    silver=silver,
                    sessions=sessions,
                    as_of=_session_time(session, time(9, 30)),
                )
                snapshots[execution.snapshot_id] = execution
                partition_records[
                    execution_record.identity.partition_run_id
                ] = execution_record
                rebalance = (index - 1) % 5 == 0
                if previous_mark is None:
                    raise PhysicalCanaryDataRejected("decision seed Gold is absent")
                projection = self._project_shadow_session(
                    binding_id=binding.binding_id,
                    account_id=account_id,
                    trade_date=session,
                    decision_date=sessions[index - 1],
                    bars=bars,
                    decision_snapshot_id=(previous_mark.snapshot_id if rebalance else None),
                    execution_snapshot_id=execution.snapshot_id,
                    mark_snapshot_id=mark.snapshot_id,
                    rebalanced=rebalance,
                )
                projections.append(projection)
                previous_mark = mark

            if selected_tickers and len(selected_tickers) != SECURITY_COUNT:
                raise PhysicalCanaryDataRejected("fixed security universe changed")
            if len(projections) != PROJECTED_SESSION_COUNT:
                raise PhysicalCanaryDataRejected(
                    "physical canary did not produce 20 authority projections"
                )
            if len({item.trade_date for item in projections}) != PROJECTED_SESSION_COUNT:
                raise PhysicalCanaryDataRejected(
                    "physical canary daily projections are not continuous and unique"
                )
            if any(
                item.evidence_class is not ShadowEvidenceClass.ENGINEERING
                or item.epoch_id is not None
                or item.evidence_window_hash is not None
                for item in projections
            ):
                raise PhysicalCanaryDataRejected(
                    "physical canary emitted formal forward evidence"
                )
            if not self.catalog.verify_shadow_chain(account_id):
                raise PhysicalCanaryDataRejected("physical canary event chain is invalid")
            if self.catalog.count_shadow_sessions(
                account_id=account_id,
                # ``count_shadow_sessions`` uses a strict lower bound so the
                # prior decision seed includes all 20 projected dates.
                since=sessions[0],
                through=sessions[-1],
            ) != PROJECTED_SESSION_COUNT:
                raise PhysicalCanaryDataRejected(
                    "event ledger does not contain exactly 20 daily projections"
                )
            self._append_lifecycle(run_id=run_id, sleeve_id=sleeve_id, sessions=sessions)
            if self.catalog.latest_lifecycle_state(sleeve_id) is not LifecycleState.SHADOW:
                raise PhysicalCanaryDataRejected("physical canary Sleeve did not reach SHADOW")

            objects = tuple(
                sorted(
                    (self._object_evidence(item) for item in snapshots.values()),
                    key=lambda item: item.snapshot_id,
                )
            )
            partitions = tuple(sorted(partition_records))
            evidence_payload = {
                **labels,
                "evaluator_identity": evaluator_identity,
                "canary_execution_contract_hash": (
                    self.canary_execution_contract_hash
                ),
                "runtime_attestation_evidence": runtime_attestation_evidence,
                "run_id": run_id,
                "run_type": run_type,
                "input_fingerprint": plan_fingerprint,
                "calendar_sessions": [item.isoformat() for item in sessions],
                "accepted_calendar_partition_ids": [
                    item.identity.partition_run_id for item in calendar_records
                ],
                "accepted_calendar_output_hashes": [
                    item.output_hash for item in calendar_records
                ],
                "security_count": SECURITY_COUNT,
                "security_set_hash": content_fingerprint(
                    list(selected_tickers),
                    domain="factor-lab/research-os/v1/physical-canary-tickers",
                ),
                "projected_session_count": PROJECTED_SESSION_COUNT,
                "partition_run_ids": list(partitions),
                "source_probe_hashes": dict(probe_hashes),
                "snapshot_evidence": [asdict(item) for item in objects],
                "shadow_session_hashes": [item.session_hash for item in projections],
                "shadow_account_event_hashes": [
                    item.account_event_hash for item in projections
                ],
                "role_binding_id": binding.binding_id,
                "account_id": account_id,
                "sleeve_id": sleeve_id,
                "sleeve_state": LifecycleState.SHADOW.value,
                "opening_execution_formal_ready": self.opening_execution_formal_ready,
            }
            canary_evidence_hash = content_fingerprint(
                evidence_payload,
                domain="factor-lab/research-os/v1/physical-canary-authority",
            )
            success_metadata = {
                **evidence_payload,
                "canary_evidence_hash": canary_evidence_hash,
                "physical_object_count": len(objects),
                "bronze_object_count": sum(
                    item.tier == SnapshotTier.BRONZE.value for item in objects
                ),
                "silver_object_count": sum(
                    item.tier == SnapshotTier.SILVER.value for item in objects
                ),
                "gold_object_count": sum(
                    item.tier == SnapshotTier.GOLD.value for item in objects
                ),
            }
            result = PhysicalCanaryResult(
                **labels,
                run_id=run_id,
                run_type=run_type,
                canary_evidence_hash=canary_evidence_hash,
                calendar_sessions=tuple(item.isoformat() for item in sessions),
                security_count=SECURITY_COUNT,
                projected_session_count=PROJECTED_SESSION_COUNT,
                sleeve_id=sleeve_id,
                sleeve_state=LifecycleState.SHADOW.value,
                account_id=account_id,
                partition_run_ids=partitions,
                source_probe_hashes=probe_hashes,
                object_evidence=objects,
                shadow_sessions=tuple(projections),
            )
            execution_lease = self.production_ledger.renew(
                execution_lease,
                now=_aware(self._now(), name="now"),
                lease_for=_EXECUTION_LEASE_FOR,
            )
            completed_at = _aware(self._now(), name="now")
            self.production_ledger.finish(
                execution_lease,
                status=PartitionStatus.SUCCEEDED,
                completed_at=completed_at,
                run_id=run_id,
                output_hash=canary_evidence_hash,
                details={
                    **labels,
                    "evaluator_identity": evaluator_identity,
                    "runtime_attestation_evidence": runtime_attestation_evidence,
                    "run_id": run_id,
                    "run_type": run_type,
                    "input_fingerprint": plan_fingerprint,
                    _EXECUTION_RESULT_METADATA: success_metadata,
                },
            )
            execution_lease = None
            self.catalog.save_run(
                RunRecord(
                    run_id=run_id,
                    run_type=run_type,
                    status="succeeded",
                    input_fingerprint=plan_fingerprint,
                    started_at=started_at,
                    completed_at=completed_at,
                    metadata=success_metadata,
                )
            )
            return result
        except Exception as exc:
            if execution_lease is not None:
                try:
                    self.production_ledger.finish(
                        execution_lease,
                        status=PartitionStatus.FAILED,
                        completed_at=_aware(self._now(), name="now"),
                        run_id=run_id,
                        error_code="physical_canary_execution_failed",
                        error=_clean_error(exc),
                        details={
                            **labels,
                            "evaluator_identity": evaluator_identity,
                            "runtime_attestation_evidence": runtime_attestation_evidence,
                            "run_id": run_id,
                            "run_type": run_type,
                            "input_fingerprint": plan_fingerprint,
                            "failure_type": type(exc).__name__,
                            "projected_session_count": len(projections),
                        },
                    )
                except Exception:
                    # Preserve the causal exception.  The still-live/expired
                    # lease remains fail-closed and is reclaimable on retry.
                    pass
            try:
                self._terminalize_failed_parent(
                    run_id=run_id,
                    run_type=run_type,
                    plan_fingerprint=plan_fingerprint,
                    started_at=started_at,
                    sessions=sessions,
                    labels=labels,
                    runtime_attestation_evidence=runtime_attestation_evidence,
                    projected_session_count=len(projections),
                    exc=exc,
                )
            except Exception:
                pass
            raise


__all__ = [
    "CALENDAR_SESSION_COUNT",
    "CANARY_OBJECT_PREFIX",
    "CONTROLLED_TEST_RUN_TYPE",
    "EVIDENCE_CLASS",
    "EVIDENCE_SCHEMA",
    "EVIDENCE_SCOPE",
    "PHYSICAL_RUN_TYPE",
    "PROJECTED_SESSION_COUNT",
    "PhysicalCanaryAdmissionError",
    "PhysicalCanaryBusy",
    "PhysicalCanaryDataRejected",
    "PhysicalCanaryError",
    "PhysicalCanaryFormalEpochDenied",
    "PhysicalCanaryResult",
    "PhysicalEngineeringCanaryService",
    "PhysicalObjectEvidence",
    "SECURITY_COUNT",
    "deny_physical_canary_formal_epoch",
    "require_physical_canary_credential_rotation",
]
