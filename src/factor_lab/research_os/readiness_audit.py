"""PostgreSQL-derived production readiness audit.

Static configuration is necessary but cannot assert that a physical canary,
historical matrix, restore drill, Dagster materialization, or service soak
actually happened.  This module derives those facts from the Research OS and
Dagster PostgreSQL ledgers and appends a content-addressed audit to ``ros_runs``.
No public method accepts caller-supplied check results.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
import hashlib
import json
from math import isfinite
import re
from typing import Any, Mapping, Sequence

from .build_provenance import (
    SourceBundleProvenanceError,
    bind_verified_oci_deployment,
)
from .catalog import RESEARCH_OS_ALEMBIC_HEAD, ResearchCatalog, RunRecord
from .contracts import DataQualityStatus, SnapshotTier
from .fingerprint import canonical_json, content_fingerprint
from .gold_panel import DEFAULT_REQUIRED_DATASETS
from .orchestration import CYCLE_BLUEPRINTS
from .docker_attestation import (
    ATTEMPT_AUTHORITY as HOST_DOCKER_ATTEMPT_AUTHORITY,
    ATTEMPT_RUN_TYPE as HOST_DOCKER_ATTEMPT_RUN_TYPE,
    ATTEMPT_SCHEMA_VERSION as HOST_DOCKER_ATTEMPT_SCHEMA_VERSION,
    COMPOSE_PROJECT as HOST_DOCKER_COMPOSE_PROJECT,
    COMPOSE_SERVICE as HOST_DOCKER_COMPOSE_SERVICE,
    READINESS_ADMISSION as HOST_DOCKER_READINESS_ADMISSION,
    RUN_TYPE as HOST_DOCKER_RUN_TYPE,
    SCHEMA_VERSION as HOST_DOCKER_SCHEMA_VERSION,
    host_docker_attempt_fingerprint,
    persisted_attestation_binding_errors,
)
from .execution_snapshot_authority import (
    CAPABILITY_DATASET as TYPED_EXECUTION_CAPABILITY_DATASET,
    BUNDLE_ROLE as TYPED_EXECUTION_BUNDLE_ROLE,
    FORMAL_EXECUTION_CAPABILITY_SCHEMA_VERSION as TYPED_EXECUTION_CAPABILITY_SCHEMA_VERSION,
    FORMAL_EXECUTION_REQUIRED_FIELDS,
    FORMAL_EXECUTION_SOURCE_ID,
    OPEN_DATASET as TYPED_OPEN_DATASET,
    OUTPUT_CONTRACT_HASH as TYPED_EXECUTION_CONTRACT_HASH,
    OUTPUT_DATASET as TYPED_EXECUTION_OUTPUT_DATASET,
    formal_execution_capability_probe_hash,
)
from .production_config import ProductionConfigEvidence
from .production_ledger import (
    IncidentRecord,
    IncidentStatus,
    PartitionIdentity,
    PartitionRecord,
    PartitionStatus,
    ProductionLedger,
)
from .reconciliation import RECONCILIATION_EVALUATOR_SCHEMA
from .canary_authority import physical_canary_session_errors
from .snapshots import REQUIRED_ENVIRONMENT_HASHES, SNAPSHOT_SCHEMA_VERSION

try:  # ProductionLedger already requires SQLAlchemy; keep import diagnostics clear.
    from sqlalchemy import MetaData, Table, inspect, select, text
except ImportError:  # pragma: no cover - minimal environments cannot construct the auditor.
    MetaData = Table = inspect = select = text = None  # type: ignore[assignment]


READINESS_AUDIT_SCHEMA_VERSION = "research-os/production-readiness-audit/v1"
PHYSICAL_CANARY_SCHEMA_VERSION = "research-os/physical-engineering-canary/v1"
PHYSICAL_CANARY_EVALUATOR_IDENTITY_SCHEMA = (
    "research-os/physical-canary-evaluator-identity/v1"
)
RESTORE_DRILL_SCHEMA_VERSION = "research-os/minio-restore-drill/v1"
DAGSTER_CODE_LOCATION_SOAK_SCHEMA_VERSION = (
    "research-os/dagster-code-location-soak/v1"
)
DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_SCHEMA_VERSION = (
    "research-os/dagster-code-location-health-sample/v1"
)
FORMAL_EXECUTION_CAPABILITY_SCHEMA_VERSION = (
    TYPED_EXECUTION_CAPABILITY_SCHEMA_VERSION
)

PHYSICAL_CANARY_RUN_TYPE = "physical_engineering_canary"
RESTORE_DRILL_RUN_TYPE = "minio_restore_drill"
DAGSTER_CODE_LOCATION_SOAK_RUN_TYPE = "dagster_code_location_soak"
DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_RUN_TYPE = (
    "dagster_code_location_health_sample"
)
READINESS_AUDIT_RUN_TYPE = "production_readiness_audit"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_BLOCKING_TRUST_LABELS = {
    "engineering_canary",
    "non_forward",
    "legacy_untrusted_data",
    "legacy_execution_regression_only",
    "st_history_unverified",
}
_STAGE_DATASETS = {
    "source": "stage_source",
    "silver": "stage_silver",
    "data_quality": "stage_data_quality",
    "gold": "stage_gold",
}
_DATASET_ALIASES = {
    "suspend_d": "trade_status",
    "suspend": "trade_status",
    "suspension": "trade_status",
    "suspension_status": "trade_status",
    "stock_limit": "trade_status",
    "stk_limit": "trade_status",
    "namechange": "historical_st",
    "stock_st": "historical_st",
    "industry": "industry_classification",
    "stock_industry": "industry_classification",
    "dividend": "company_action",
    "corporate_action": "company_action",
    "trade_cal": "trade_calendar",
    "stock_basic_l": "stock_basic",
    "stock_basic_p": "stock_basic",
    "stock_basic_d": "stock_basic",
    "stock_basic_g": "stock_basic",
}
_CANARY_MAX_AGE = timedelta(hours=24)
_RESTORE_MAX_AGE = timedelta(hours=24)
_CALENDAR_MAX_STALENESS = timedelta(days=10)
_SOAK_MINIMUM = timedelta(hours=24)
_HOST_ATTESTATION_MAX_AGE = timedelta(minutes=10)
_HEALTH_SAMPLE_MAX_AGE = timedelta(minutes=10)
_DAGSTER_SERVICE_NAME = "dagster-code-server"
_DAGSTER_CODE_LOCATION = "factor_lab_research_os"
_ROSCAL_BRIDGE_RUN_ID = re.compile(r"^roscal_[0-9a-f]{48}$")
_ROSOP_BRIDGE_RUN_ID = re.compile(r"^rosop_[0-9a-f]{48}$")
_TRUSTED_ROSOP_RUN_TYPES = frozenset(
    f"dagster:{cycle.value}:{operation.value}"
    for cycle, blueprint in CYCLE_BLUEPRINTS.items()
    for operation in blueprint.operations
)
_SHANGHAI = timezone(timedelta(hours=8))
_MARKET_CLOSE = time(15, 0)


def _valid_tushare_batch_lineage(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    try:
        requested = int(value.get("requested_ticker_count"))
        observed = int(value.get("observed_ticker_count"))
        missing = int(value.get("missing_ticker_count"))
        batch_count = int(value.get("request_batch_count"))
        batches = tuple(value.get("request_batches") or ())
        request_hashes = tuple(map(str, value.get("request_hashes") or ()))
        missing_hashes = tuple(map(str, value.get("missing_ticker_hashes") or ()))
    except (TypeError, ValueError):
        return False
    if not (
        requested > 0
        and observed > 0
        and requested == observed + missing
        and batch_count == len(batches) == len(request_hashes)
        and batch_count > 0
        and all(_SHA256.fullmatch(item) for item in request_hashes)
        and len(set(request_hashes)) == len(request_hashes)
        and len(missing_hashes) == missing
        and len(set(missing_hashes)) == len(missing_hashes)
        and all(_SHA256.fullmatch(item) for item in missing_hashes)
        and value.get("coverage_status")
        == ("complete" if missing == 0 else "provisional_missing")
    ):
        return False
    batch_missing_hashes: list[str] = []
    batch_ticker_count = 0
    for index, item in enumerate(batches):
        if not isinstance(item, Mapping):
            return False
        try:
            ticker_count = int(item.get("ticker_count"))
            missing_count = int(item.get("missing_ticker_count"))
            hashes = tuple(map(str, item.get("missing_ticker_hashes") or ()))
        except (TypeError, ValueError):
            return False
        if not (
            item.get("batch_index") == index
            and ticker_count > 0
            and ticker_count <= 300
            and missing_count == len(hashes)
            and missing_count <= ticker_count
            and str(item.get("request_hash") or "") == request_hashes[index]
            and all(_SHA256.fullmatch(digest) for digest in hashes)
        ):
            return False
        batch_ticker_count += ticker_count
        batch_missing_hashes.extend(hashes)
    return bool(
        batch_ticker_count == requested
        and sorted(batch_missing_hashes) == sorted(missing_hashes)
    )


class ReadinessAuditError(RuntimeError):
    """The persisted readiness facts are missing, malformed, or conflicting."""


class ProductionReadinessStatus(str, Enum):
    CONFIG_VALID_CANARY_PENDING = "config_valid_canary_pending"
    CANARY_READY = "canary_ready"
    BACKFILL_COMPLETE = "backfill_complete"
    FORMAL_EPOCH_READY = "formal_epoch_ready"


@dataclass(frozen=True)
class ReadinessCheck:
    code: str
    passed: bool
    blockers: tuple[str, ...] = ()
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.code.strip():
            raise ValueError("readiness check code is required")
        normalized = tuple(sorted(set(map(str, self.blockers))))
        object.__setattr__(self, "blockers", normalized)
        object.__setattr__(self, "evidence", _jsonable(dict(self.evidence)))
        if self.passed == bool(normalized):
            raise ValueError("passed checks have no blockers; failed checks require blockers")

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "evidence": _jsonable(self.evidence),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ReadinessCheck":
        return cls(
            code=str(payload["code"]),
            passed=bool(payload["passed"]),
            blockers=tuple(map(str, payload.get("blockers") or ())),
            evidence=dict(payload.get("evidence") or {}),
        )


@dataclass(frozen=True)
class ProductionReadinessAudit:
    audit_id: str
    fingerprint: str
    status: ProductionReadinessStatus
    audited_at: datetime
    blockers: tuple[str, ...]
    checks: tuple[ReadinessCheck, ...]
    accepted_session_count: int = 0
    source_start: str | None = None
    latest_session: str | None = None

    def __post_init__(self) -> None:
        if self.audited_at.tzinfo is None or self.audited_at.utcoffset() is None:
            raise ValueError("audited_at must include a timezone")
        if not _SHA256.fullmatch(self.fingerprint):
            raise ValueError("readiness audit fingerprint must be SHA-256")
        if self.audit_id != f"readiness_{self.fingerprint}":
            raise ValueError("readiness audit id differs from its fingerprint")
        normalized = tuple(sorted(set(map(str, self.blockers))))
        object.__setattr__(self, "blockers", normalized)
        expected = tuple(
            sorted({item for check in self.checks for item in check.blockers})
        )
        if normalized != expected:
            raise ValueError("readiness audit blockers differ from check blockers")

    @property
    def ready(self) -> bool:
        return self.status is ProductionReadinessStatus.FORMAL_EPOCH_READY

    def _content_payload(self) -> dict[str, Any]:
        return {
            "schema_version": READINESS_AUDIT_SCHEMA_VERSION,
            "status": self.status.value,
            "audited_at": self.audited_at.astimezone(timezone.utc).isoformat(),
            "blockers": list(self.blockers),
            "checks": [item.to_dict() for item in self.checks],
            "accepted_session_count": self.accepted_session_count,
            "source_start": self.source_start,
            "latest_session": self.latest_session,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            **self._content_payload(),
            "audit_id": self.audit_id,
            "fingerprint": self.fingerprint,
            "ready": self.ready,
        }

    @classmethod
    def from_run(cls, run: RunRecord) -> "ProductionReadinessAudit":
        if run.run_type != READINESS_AUDIT_RUN_TYPE or run.status != "completed":
            raise ReadinessAuditError("run is not a completed production readiness audit")
        raw = run.metadata.get("audit")
        if not isinstance(raw, Mapping):
            raise ReadinessAuditError("readiness run has no typed audit payload")
        try:
            audit = cls(
                audit_id=str(raw["audit_id"]),
                fingerprint=str(raw["fingerprint"]),
                status=ProductionReadinessStatus(str(raw["status"])),
                audited_at=_parse_time(raw["audited_at"]),
                blockers=tuple(map(str, raw.get("blockers") or ())),
                checks=tuple(
                    ReadinessCheck.from_dict(item)
                    for item in raw.get("checks") or ()
                ),
                accepted_session_count=int(raw.get("accepted_session_count") or 0),
                source_start=(
                    None if raw.get("source_start") is None else str(raw["source_start"])
                ),
                latest_session=(
                    None
                    if raw.get("latest_session") is None
                    else str(raw["latest_session"])
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ReadinessAuditError("readiness run payload is malformed") from exc
        expected = content_fingerprint(
            audit._content_payload(), domain=READINESS_AUDIT_SCHEMA_VERSION
        )
        if not (
            expected
            == audit.fingerprint
            == run.input_fingerprint
            and run.run_id == audit.audit_id
        ):
            raise ReadinessAuditError("readiness run fingerprint is invalid")
        return audit


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


def _parse_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _parse_dagster_heartbeat_time(value: Any) -> datetime:
    """Parse the storage shape owned by Dagster's heartbeat table.

    Dagster's PostgreSQL schema stores ``daemon_heartbeats.timestamp`` as a
    ``TIMESTAMP WITHOUT TIME ZONE`` even though its value is UTC.  SQLAlchemy
    therefore returns a naive ``datetime``.  That storage exception must stay
    local to this table: all Research OS evidence continues to require an
    explicit timezone through :func:`_parse_time`.
    """

    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return datetime.fromtimestamp(float(value), tz=timezone.utc)


def _valid_hash(value: Any) -> bool:
    return bool(_SHA256.fullmatch(str(value or "")))


def _evidence_hash(payload: Mapping[str, Any], *, field_name: str, domain: str) -> str:
    values = {str(key): _jsonable(value) for key, value in payload.items()}
    values.pop(field_name, None)
    return content_fingerprint(values, domain=domain)


def physical_canary_evidence_hash(payload: Mapping[str, Any]) -> str:
    if payload.get("evidence_schema") == PHYSICAL_CANARY_SCHEMA_VERSION:
        values = {str(key): _jsonable(value) for key, value in payload.items()}
        for field_name in (
            "canary_evidence_hash",
            "physical_object_count",
            "bronze_object_count",
            "silver_object_count",
            "gold_object_count",
        ):
            values.pop(field_name, None)
        return content_fingerprint(
            values,
            domain="factor-lab/research-os/v1/physical-canary-authority",
        )
    return _evidence_hash(
        payload,
        field_name="canary_evidence_hash",
        domain=PHYSICAL_CANARY_SCHEMA_VERSION,
    )


def restore_drill_evidence_hash(payload: Mapping[str, Any]) -> str:
    return _evidence_hash(
        payload,
        field_name="restore_evidence_hash",
        domain=RESTORE_DRILL_SCHEMA_VERSION,
    )


def dagster_code_location_soak_evidence_hash(payload: Mapping[str, Any]) -> str:
    return _evidence_hash(
        payload,
        field_name="soak_evidence_hash",
        domain=DAGSTER_CODE_LOCATION_SOAK_SCHEMA_VERSION,
    )


def dagster_code_location_health_sample_evidence_hash(
    payload: Mapping[str, Any],
) -> str:
    return _evidence_hash(
        payload,
        field_name="sample_evidence_hash",
        domain=DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_SCHEMA_VERSION,
    )


def dagster_code_location_health_series_hash(
    sample_evidence_hashes: Sequence[str],
) -> str:
    hashes = tuple(map(str, sample_evidence_hashes))
    if not hashes or any(not _valid_hash(item) for item in hashes):
        raise ValueError("health sample series requires SHA-256 evidence hashes")
    return content_fingerprint(
        {"sample_evidence_hashes": list(hashes)},
        domain="research-os/dagster-code-location-health-series/v1",
    )


def formal_execution_probe_hash(
    *,
    source_id: str,
    dataset: str,
    contract_hash: str,
    fields: Sequence[str],
    detail: Mapping[str, Any],
) -> str:
    return formal_execution_capability_probe_hash(
        source_id=source_id,
        dataset=dataset,
        contract_hash=contract_hash,
        fields=fields,
        detail=_jsonable(detail),
    )


def _manifest_identity(manifest: Mapping[str, Any]) -> str:
    content = {
        "schema_version": manifest["schema_version"],
        "tier": manifest["tier"],
        "as_of": manifest["as_of"],
        "parent_snapshot_ids": list(manifest["parent_snapshot_ids"]),
        "environment_hashes": dict(manifest["environment_hashes"]),
        "quality_status": manifest["quality_status"],
        "trust_labels": list(manifest["trust_labels"]),
        "files": list(manifest["files"]),
    }
    if "trading_calendar" in manifest:
        content["trading_calendar"] = dict(manifest["trading_calendar"])
    encoded = json.dumps(
        content, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class ProductionReadinessAuditor:
    """Derive and persist one immutable readiness assessment from database facts."""

    def __init__(
        self,
        catalog: ResearchCatalog,
        production_ledger: ProductionLedger,
        *,
        config: Mapping[str, Any],
        config_evidence: ProductionConfigEvidence,
    ) -> None:
        if inspect is None:
            raise RuntimeError("SQLAlchemy is required for production readiness audit")
        self.catalog = catalog
        self.ledger = production_ledger
        self.config = dict(config)
        self.config_evidence = config_evidence
        self._snapshot_cache: dict[str, tuple[str, ...]] = {}

    def _check(self, code: str, blockers: Sequence[str], **evidence: Any) -> ReadinessCheck:
        normalized = tuple(sorted(set(map(str, blockers))))
        return ReadinessCheck(
            code=code,
            passed=not normalized,
            blockers=normalized,
            evidence=evidence,
        )

    def _schema_head_check(self) -> ReadinessCheck:
        blockers: list[str] = []
        heads: tuple[str, ...] = ()
        try:
            inspector = inspect(self.ledger.engine)
            if not inspector.has_table("alembic_version"):
                blockers.append("alembic_version_missing")
            else:
                with self.ledger.engine.connect() as connection:
                    heads = tuple(
                        sorted(
                            str(row[0])
                            for row in connection.execute(
                                text("SELECT version_num FROM alembic_version")
                            )
                        )
                    )
                if heads != (RESEARCH_OS_ALEMBIC_HEAD,):
                    blockers.append("alembic_head_mismatch")
        except Exception:
            blockers.append("alembic_head_query_failed")
        return self._check(
            "alembic_head",
            blockers,
            expected=RESEARCH_OS_ALEMBIC_HEAD,
            observed=list(heads),
        )

    def _configuration_check(self) -> ReadinessCheck:
        ignored = {
            "persisted_production_readiness_audit_missing",
            "daemon_inspected_oci_provenance_missing",
            "formal_execution_adapter_insufficient",
            *tuple(self.config_evidence.credential_rotation_blockers),
        }
        blockers = [
            str(item)
            for item in self.config_evidence.readiness_blockers
            if str(item) not in ignored
        ]
        return self._check(
            "production_configuration",
            blockers,
            config_status=self.config_evidence.status,
        )

    def _latest_host_docker_attempt(
        self,
    ) -> tuple[RunRecord | None, tuple[str, ...], Mapping[str, Any]]:
        """Select the newest formal host-attestation invocation fail-closed.

        Successful attestations and invocation attempts are separate facts.
        The latter is the anti-fallback authority: a fresh old proof cannot be
        reused after a newer invocation starts or fails.  Attempts are ordered
        by immutable invocation ``started_at``; equal-start records are
        ambiguous and therefore never resolved by lexical run-id order.
        """

        attempts = self.catalog.list_runs(
            limit=1_000,
            run_type=HOST_DOCKER_ATTEMPT_RUN_TYPE,
        )
        blockers: list[str] = []
        evidence: dict[str, Any] = {
            "host_attestation_attempt_count": len(attempts),
        }
        if len(attempts) >= 1_000:
            blockers.append("host_docker_attestation_attempt_listing_truncated")
        if not attempts:
            blockers.append("host_docker_attestation_attempt_missing")
            return None, tuple(blockers), evidence
        latest_started_at = max(item.started_at for item in attempts)
        latest = tuple(
            item for item in attempts if item.started_at == latest_started_at
        )
        evidence.update(
            latest_host_attestation_attempt_started_at=latest_started_at,
            latest_host_attestation_attempt_count=len(latest),
            latest_host_attestation_attempt_run_ids=[item.run_id for item in latest],
        )
        if len(latest) != 1:
            blockers.append("host_docker_attestation_latest_attempt_ambiguous")
            return None, tuple(sorted(set(blockers))), evidence
        attempt = latest[0]
        metadata = attempt.metadata
        outcome = str(metadata.get("outcome") or "")
        observed_now = self.catalog.database_now().astimezone(timezone.utc)
        suffix = attempt.run_id.removeprefix("docker_attestation_attempt_")
        try:
            recomputed_fingerprint = host_docker_attempt_fingerprint(
                started_at=attempt.started_at,
                nonce=str(metadata.get("attempt_nonce") or ""),
            )
        except (RuntimeError, TypeError, ValueError):
            recomputed_fingerprint = ""
        terminal = attempt.status in {"succeeded", "failed"}
        contract_valid = bool(
            attempt.run_type == HOST_DOCKER_ATTEMPT_RUN_TYPE
            and re.fullmatch(r"[0-9a-f]{64}", suffix)
            and suffix == attempt.input_fingerprint == recomputed_fingerprint
            and metadata.get("schema_version")
            == HOST_DOCKER_ATTEMPT_SCHEMA_VERSION
            and metadata.get("authority") == HOST_DOCKER_ATTEMPT_AUTHORITY
            and metadata.get("physical") is True
            and outcome == attempt.status
            and attempt.status in {"running", "succeeded", "failed"}
            and attempt.started_at <= observed_now
            and (
                (not terminal and attempt.completed_at is None)
                or (
                    terminal
                    and attempt.completed_at is not None
                    and attempt.completed_at >= attempt.started_at
                    and attempt.completed_at <= observed_now
                )
            )
            and metadata.get("formal_readiness_eligible")
            is (attempt.status == "succeeded")
            and (
                (
                    attempt.status == "failed"
                    and attempt.error
                    in {
                        "docker_attestation_admission_error",
                        "docker_attestation_error",
                        "docker_attestation_internal_error",
                    }
                    and metadata.get("error_type") == attempt.error
                )
                or (
                    attempt.status != "failed"
                    and attempt.error is None
                    and metadata.get("error_type") is None
                )
            )
        )
        evidence.update(
            latest_host_attestation_attempt_run_id=attempt.run_id,
            latest_host_attestation_attempt_status=attempt.status,
            latest_host_attestation_attempt_outcome=outcome or None,
            latest_host_attestation_attempt_completed_at=attempt.completed_at,
        )
        if not contract_valid:
            blockers.append("host_docker_attestation_latest_attempt_invalid")
            return None, tuple(sorted(set(blockers))), evidence
        if attempt.status != "succeeded":
            blockers.append(
                "host_docker_attestation_latest_attempt_"
                + ("running" if attempt.status == "running" else "failed")
            )
            return None, tuple(sorted(set(blockers))), evidence
        attestation_run_id = str(metadata.get("attestation_run_id") or "")
        attestation_hash = str(metadata.get("attestation_hash") or "")
        bound = self.catalog.get_run(attestation_run_id) if attestation_run_id else None
        if not (
            bound is not None
            and bound.run_type == HOST_DOCKER_RUN_TYPE
            and bound.status == "succeeded"
            and bound.run_id == f"docker_attestation_{attestation_hash}"
            and _valid_hash(attestation_hash)
            and bound.input_fingerprint == attestation_hash
            and str(bound.metadata.get("attestation_hash") or "")
            == attestation_hash
            and bound.completed_at is not None
            and attempt.completed_at is not None
            and bound.completed_at <= attempt.completed_at
        ):
            blockers.append("host_docker_attestation_attempt_binding_invalid")
            return None, tuple(sorted(set(blockers))), evidence
        evidence.update(
            latest_host_attestation_bound_run_id=bound.run_id,
            latest_host_attestation_bound_hash=attestation_hash,
        )
        return bound, tuple(sorted(set(blockers))), evidence

    def _oci_provenance_check(self) -> ReadinessCheck:
        provenance = self.config_evidence.provenance
        # SQLite is an explicit controlled-test backend.  It retains the
        # source-bundle-only seam so unit tests can exercise later readiness
        # gates without a Docker daemon.  Formal PostgreSQL readiness accepts
        # only a fresh host-daemon attestation persisted by the fixed host
        # producer in docker_attestation.py.
        if self.ledger.engine.dialect.name != "postgresql":
            epoch_fields_factory = getattr(provenance, "epoch_fields", None)
            epoch_fields = (
                dict(epoch_fields_factory())
                if callable(epoch_fields_factory)
                else {
                    name: getattr(provenance, name, None)
                    for name in (
                        "architecture_version",
                        "code_hash",
                        "configuration_hash",
                        "dependency_lock_hash",
                        "dirty_patch_hash",
                    )
                }
            )
            blockers: list[str] = []
            if not bool(getattr(provenance, "formal_epoch_eligible", False)):
                blockers.append("daemon_inspected_oci_provenance_missing")
            if not str(epoch_fields.get("architecture_version") or "").strip() or any(
                not _valid_hash(epoch_fields.get(name))
                for name in (
                    "code_hash",
                    "configuration_hash",
                    "dependency_lock_hash",
                    "dirty_patch_hash",
                )
            ):
                blockers.append("epoch_build_fields_missing_or_invalid")
            return self._check(
                "daemon_inspected_oci_provenance",
                blockers,
                controlled_test_backend=True,
                epoch_fields=epoch_fields,
                build_identity_hash=getattr(provenance, "build_identity_hash", None),
                oci_image_id=getattr(provenance, "oci_image_id", None),
                oci_repo_digests=list(
                    getattr(provenance, "oci_repo_digests", ()) or ()
                ),
                oci_base_digests=list(
                    getattr(provenance, "oci_base_digests", ()) or ()
                ),
            )

        blockers: list[str] = []
        evidence: dict[str, Any] = {"controlled_test_backend": False}
        run, attempt_blockers, attempt_evidence = (
            self._latest_host_docker_attempt()
        )
        blockers.extend(attempt_blockers)
        evidence.update(attempt_evidence)
        if run is None:
            return self._check(
                "daemon_inspected_oci_provenance",
                blockers,
                **evidence,
            )
        metadata = dict(run.metadata)
        try:
            inspected_at = _parse_time(metadata.get("inspected_at"))
            verified_at = _parse_time(metadata.get("deployment_verified_at"))
            container_started_at = _parse_time(metadata.get("container_started_at"))
            observed_now = self.catalog.database_now().astimezone(timezone.utc)
        except (TypeError, ValueError):
            inspected_at = verified_at = container_started_at = observed_now = (
                datetime.min.replace(tzinfo=timezone.utc)
            )
            blockers.append("host_docker_attestation_time_invalid")
        attestation_payload = dict(metadata)
        attestation_hash = str(attestation_payload.pop("attestation_hash", ""))
        recomputed_attestation = content_fingerprint(
            attestation_payload,
            domain=HOST_DOCKER_SCHEMA_VERSION,
        )
        service_labels = metadata.get("service_labels")
        try:
            health_failing_streak = int(
                metadata.get("health_failing_streak", -1)
            )
        except (TypeError, ValueError):
            health_failing_streak = -1
            blockers.append("host_docker_health_streak_invalid")
        image_id = str(metadata.get("oci_image_id") or "")
        repo_digests = tuple(map(str, metadata.get("oci_repo_digests") or ()))
        base_digest = str(metadata.get("oci_base_digest") or "")
        container_id = str(metadata.get("container_id") or "")
        config_hash = (
            str(service_labels.get("com.docker.compose.config-hash") or "")
            if isinstance(service_labels, Mapping)
            else ""
        )
        deployment_values = {
            "source_bundle_manifest_hash": metadata.get(
                "source_bundle_manifest_hash"
            ),
            "source_tree_hash": metadata.get("source_tree_hash"),
            "configuration_tree_hash": metadata.get("configuration_tree_hash"),
            "runtime_tree_hash": metadata.get("runtime_tree_hash"),
            "dependency_lock_hash": metadata.get("dependency_lock_hash"),
            "source_file_count": metadata.get("source_file_count"),
            "configuration_file_count": metadata.get("configuration_file_count"),
            "runtime_file_count": metadata.get("runtime_file_count"),
            "base_image_name": metadata.get("oci_base_name"),
            "base_image_digest": base_digest,
        }
        recomputed_deployment = content_fingerprint(
            {
                **deployment_values,
                "runtime_image_id": image_id,
                "temporary_container_removed": True,
                "running_container_bundle_verified": True,
                "verified_at": verified_at,
            },
            domain=f"{HOST_DOCKER_SCHEMA_VERSION}/deployment",
        )
        runtime_contract = metadata.get("runtime_contract")
        runtime_contract_hash = str(metadata.get("runtime_contract_hash") or "")
        recomputed_runtime_contract = (
            content_fingerprint(
                runtime_contract,
                domain="research-os/host-docker-runtime-contract/v1",
            )
            if isinstance(runtime_contract, Mapping)
            else ""
        )
        docker_authority = metadata.get("docker_authority")
        docker_authority_hash = str(metadata.get("docker_authority_hash") or "")
        recomputed_docker_authority = (
            content_fingerprint(
                docker_authority,
                domain="research-os/host-docker-local-authority/v1",
            )
            if isinstance(docker_authority, Mapping)
            else ""
        )
        age = observed_now - verified_at
        if age.total_seconds() < 0 or age > _HOST_ATTESTATION_MAX_AGE:
            blockers.append("host_docker_runtime_attestation_stale")
        if not (
            run.status == "succeeded"
            and run.run_type == HOST_DOCKER_RUN_TYPE
            and run.run_id == f"docker_attestation_{attestation_hash}"
            and run.input_fingerprint == attestation_hash
            and run.started_at == inspected_at
            and run.completed_at == verified_at
            and container_started_at <= inspected_at
            and metadata.get("schema_version") == HOST_DOCKER_SCHEMA_VERSION
            and metadata.get("authority")
            == "host_docker_daemon_plus_verified_image_bundle"
            and metadata.get("physical") is True
            and metadata.get("controlled_test_runner") is False
            and metadata.get("readiness_admission")
            == HOST_DOCKER_READINESS_ADMISSION
            and metadata.get("compose_project") == HOST_DOCKER_COMPOSE_PROJECT
            and metadata.get("compose_service") == HOST_DOCKER_COMPOSE_SERVICE
            and metadata.get("container_state") == "running"
            and metadata.get("container_health") == "healthy"
            and health_failing_streak == 0
            and metadata.get("daemon_image_id_verified") is True
            and metadata.get("pinned_base_digest_verified") is True
            and metadata.get("deployment_bundle_verified") is True
            and metadata.get("temporary_container_removed") is True
            and metadata.get("running_container_bundle_verified") is True
            and metadata.get("formal_readiness_eligible") is True
            and isinstance(docker_authority, Mapping)
            and docker_authority.get("authority")
            == "explicit_local_docker_engine_endpoint"
            and docker_authority.get("ambient_docker_routing_rejected") is True
            and docker_authority.get("server_os") == "linux"
            and _valid_hash(docker_authority_hash)
            and docker_authority_hash == recomputed_docker_authority
            and runtime_contract_hash == recomputed_runtime_contract
            and re.fullmatch(r"[0-9a-f]{64}", container_id)
            and re.fullmatch(r"sha256:[0-9a-f]{64}", image_id)
            and re.fullmatch(r"sha256:[0-9a-f]{64}", base_digest)
            and isinstance(service_labels, Mapping)
            and service_labels.get("com.docker.compose.project")
            == HOST_DOCKER_COMPOSE_PROJECT
            and service_labels.get("com.docker.compose.service")
            == HOST_DOCKER_COMPOSE_SERVICE
            and config_hash
            and _valid_hash(attestation_hash)
            and attestation_hash == recomputed_attestation
            and metadata.get("deployment_evidence_hash")
            == recomputed_deployment
        ):
            blockers.append("host_docker_runtime_attestation_invalid")
        if not (
            metadata.get("source_bundle_manifest_hash")
            == getattr(provenance, "image_source_digest", None)
            and metadata.get("source_tree_hash")
            == getattr(provenance, "code_hash", None)
            and metadata.get("configuration_tree_hash")
            == getattr(provenance, "configuration_hash", None)
            and metadata.get("dependency_lock_hash")
            == getattr(provenance, "dependency_lock_hash", None)
        ):
            blockers.append("host_docker_source_bundle_release_mismatch")
        try:
            bound = bind_verified_oci_deployment(
                provenance,
                oci_image_id=image_id,
                oci_repo_digests=repo_digests,
                oci_base_digests=(base_digest,),
            )
        except (SourceBundleProvenanceError, TypeError, ValueError):
            bound = None
            blockers.append("host_docker_epoch_binding_invalid")
        if bound is not None and not bound.formal_epoch_eligible:
            blockers.append("host_docker_epoch_binding_ineligible")
        deployment_identity_hash = (
            None
            if bound is None or not container_id or not config_hash
            else content_fingerprint(
                {
                    "container_id": container_id,
                    "oci_image_id": bound.oci_image_id,
                    "compose_config_hash": config_hash,
                    "build_identity_hash": bound.build_identity_hash,
                    "runtime_contract_hash": runtime_contract_hash,
                },
                domain="research-os/host-docker-deployment-identity/v1",
            )
        )
        evidence.update(
            host_attestation_run_id=run.run_id,
            host_attestation_hash=attestation_hash or None,
            attested_at=verified_at.isoformat(),
            attestation_age_seconds=age.total_seconds(),
            container_started_at=container_started_at.isoformat(),
            container_id=container_id or None,
            compose_config_hash=config_hash or None,
            deployment_identity_hash=deployment_identity_hash,
            docker_authority_hash=docker_authority_hash or None,
            runtime_contract_hash=runtime_contract_hash or None,
            epoch_fields=(None if bound is None else bound.epoch_fields()),
            build_identity_hash=(
                None if bound is None else bound.build_identity_hash
            ),
            oci_image_id=(None if bound is None else bound.oci_image_id),
            oci_repo_digests=(
                [] if bound is None else list(bound.oci_repo_digests)
            ),
            oci_base_digests=(
                [] if bound is None else list(bound.oci_base_digests)
            ),
        )
        return self._check(
            "daemon_inspected_oci_provenance", blockers, **evidence
        )

    def verified_oci_deployment_evidence(self) -> Mapping[str, Any]:
        """Return the current host-daemon deployment only after full validation."""

        check = self._oci_provenance_check()
        if not check.passed:
            raise ReadinessAuditError(
                "verified OCI deployment is unavailable: "
                + ", ".join(check.blockers)
            )
        return dict(check.evidence)

    def _bootstrap_config(self) -> tuple[date, tuple[str, ...]]:
        daily = self.config.get("daily")
        if not isinstance(daily, Mapping):
            raise ReadinessAuditError("daily production configuration is missing")
        bootstrap = daily.get("bootstrap")
        gold = daily.get("gold")
        panel = gold.get("research_panel") if isinstance(gold, Mapping) else None
        if not isinstance(bootstrap, Mapping) or not isinstance(panel, Mapping):
            raise ReadinessAuditError("bootstrap/research panel configuration is missing")
        source_start = date.fromisoformat(str(bootstrap["source_start"]))
        if source_start > date(2016, 6, 1):
            raise ReadinessAuditError(
                "production calendar bootstrap must start no later than 2016-06-01"
            )
        required = tuple(sorted(set(map(str, panel.get("required_datasets") or ()))))
        canonical = {_DATASET_ALIASES.get(item, item) for item in required}
        if not set(DEFAULT_REQUIRED_DATASETS).issubset(canonical):
            raise ReadinessAuditError(
                "configured required datasets do not cover the formal Gold contract"
            )
        return source_start, required

    @staticmethod
    def _partition_result(record: PartitionRecord) -> Mapping[str, Any] | None:
        if record.status is not PartitionStatus.SUCCEEDED or not record.output_hash:
            return None
        result = record.details.get("operation_result")
        if not isinstance(result, Mapping):
            return None
        expected = content_fingerprint(
            result,
            domain="factor-lab/research-os/v1/production-operation-result",
        )
        if record.output_hash != expected or result.get("status") != "completed":
            return None
        return result

    def _snapshot_errors(
        self,
        snapshot_id: str,
        *,
        expected_tier: SnapshotTier | None = None,
        require_physical_uri: bool = True,
        visiting: frozenset[str] = frozenset(),
    ) -> tuple[str, ...]:
        if snapshot_id in self._snapshot_cache and expected_tier is None:
            return self._snapshot_cache[snapshot_id]
        if snapshot_id in visiting:
            return ("snapshot_parent_cycle",)
        record = self.catalog.get_snapshot(snapshot_id)
        if record is None:
            return ("snapshot_missing",)
        reference = record.reference
        errors: list[str] = []
        if expected_tier is not None and reference.tier is not expected_tier:
            errors.append("snapshot_tier_mismatch")
        if reference.quality_status is not DataQualityStatus.ACCEPTED:
            errors.append("snapshot_not_accepted")
        if not (
            reference.snapshot_id == reference.content_hash == snapshot_id
            and _valid_hash(snapshot_id)
        ):
            errors.append("snapshot_reference_hash_mismatch")
        manifest = reference.manifest
        try:
            computed = _manifest_identity(manifest)
        except (KeyError, TypeError, ValueError):
            errors.append("snapshot_manifest_malformed")
        else:
            try:
                manifest_as_of = _parse_time(manifest.get("as_of"))
            except (TypeError, ValueError):
                manifest_as_of = None
                errors.append("snapshot_manifest_as_of_invalid")
            if not (
                manifest.get("schema_version") == SNAPSHOT_SCHEMA_VERSION
                and manifest.get("snapshot_id") == computed == snapshot_id
                and str(manifest.get("tier")) == reference.tier.value
                and str(manifest.get("quality_status")) == "pass"
                and tuple(manifest.get("parent_snapshot_ids") or ())
                == reference.parent_snapshot_ids
                and tuple(manifest.get("trust_labels") or ())
                == reference.trust_labels
            ):
                errors.append("snapshot_manifest_hash_mismatch")
            if manifest_as_of != reference.as_of.astimezone(timezone.utc):
                errors.append("snapshot_manifest_as_of_mismatch")
            environment = manifest.get("environment_hashes")
            if not isinstance(environment, Mapping) or any(
                not _valid_hash(environment.get(name))
                for name in REQUIRED_ENVIRONMENT_HASHES
            ):
                errors.append("snapshot_environment_hash_invalid")
            files = manifest.get("files")
            if not isinstance(files, list) or not files:
                errors.append("snapshot_manifest_files_missing")
            elif any(
                not isinstance(item, Mapping)
                or not str(item.get("path") or "").strip()
                or not _valid_hash(item.get("sha256"))
                or int(item.get("size_bytes") or 0) <= 0
                for item in files
            ):
                errors.append("snapshot_manifest_file_hash_invalid")
            elif len({str(item["path"]) for item in files}) != len(files):
                errors.append("snapshot_manifest_duplicate_file_path")
        if _BLOCKING_TRUST_LABELS.intersection(reference.trust_labels):
            errors.append("snapshot_has_blocking_trust_label")
        if require_physical_uri:
            expected_scheme = "iceberg://" if reference.tier is SnapshotTier.GOLD else "s3://"
            if not reference.uri.startswith(expected_scheme):
                errors.append("snapshot_uri_not_physical")
            if reference.tier is SnapshotTier.GOLD and not reference.uri.endswith(snapshot_id):
                errors.append("gold_uri_not_bound_to_snapshot")
        next_visiting = visiting | {snapshot_id}
        for parent_id in reference.parent_snapshot_ids:
            errors.extend(
                self._snapshot_errors(
                    parent_id,
                    require_physical_uri=require_physical_uri,
                    visiting=next_visiting,
                )
            )
        normalized = tuple(sorted(set(errors)))
        if expected_tier is None:
            self._snapshot_cache[snapshot_id] = normalized
        return normalized

    def _calendar_check(
        self, *, now: datetime, source_start: date
    ) -> tuple[ReadinessCheck, tuple[str, ...]]:
        blockers: list[str] = []
        sessions = self.ledger.accepted_calendar_partitions(limit=5_000)
        bootstrap_rows = self.ledger.list_partitions(
            statuses=(PartitionStatus.SUCCEEDED,),
            source_id="research_os",
            dataset="bootstrap_trade_calendar",
            limit=10_000,
        )
        bootstrap = bootstrap_rows[-1] if bootstrap_rows else None
        bootstrap_result = (
            bootstrap.details.get("bootstrap_result")
            if bootstrap is not None
            else None
        )
        if not sessions:
            blockers.append("accepted_calendar_missing")
        if bootstrap is None or not isinstance(bootstrap_result, Mapping):
            blockers.append("accepted_calendar_bootstrap_missing")
        else:
            expected_hash = content_fingerprint(
                bootstrap_result,
                domain="factor-lab/research-os/v1/calendar-bootstrap-output",
            )
            if bootstrap.output_hash != expected_hash:
                blockers.append("accepted_calendar_bootstrap_hash_mismatch")
            try:
                observed_start = date.fromisoformat(str(bootstrap_result["source_start"]))
                observed_through = date.fromisoformat(str(bootstrap_result["through"]))
            except (KeyError, ValueError):
                blockers.append("accepted_calendar_range_malformed")
            else:
                if observed_start > source_start:
                    blockers.append("accepted_calendar_start_incomplete")
                if sessions and observed_through < date.fromisoformat(sessions[-1]):
                    blockers.append("accepted_calendar_latest_mismatch")
                local_today = now.astimezone(timezone(timedelta(hours=8))).date()
                if local_today - observed_through > _CALENDAR_MAX_STALENESS:
                    blockers.append("accepted_calendar_stale")
            if tuple(map(str, bootstrap_result.get("sessions") or ())) != sessions:
                blockers.append("accepted_calendar_session_set_mismatch")
            snapshot_id = str(bootstrap_result.get("silver_snapshot_id") or "")
            if self._snapshot_errors(snapshot_id, expected_tier=SnapshotTier.SILVER):
                blockers.append("accepted_calendar_snapshot_invalid")
        all_canonical = self.ledger.list_partitions(
            source_id="research_os",
            dataset="accepted_trade_calendar",
            limit=10_000,
        )
        nonaccepted = [
            item.identity.partition_key
            for item in all_canonical
            if item.status is not PartitionStatus.SUCCEEDED
        ]
        if nonaccepted:
            blockers.append("accepted_calendar_has_nonaccepted_rows")
        if sessions and date.fromisoformat(sessions[0]) > source_start:
            blockers.append("accepted_calendar_start_incomplete")
        return (
            self._check(
                "accepted_calendar_range",
                blockers,
                source_start=source_start.isoformat(),
                latest_session=(sessions[-1] if sessions else None),
                session_count=len(sessions),
                bootstrap_partition=(
                    None if bootstrap is None else bootstrap.identity.partition_key
                ),
                nonaccepted_rows=nonaccepted[:100],
            ),
            sessions,
        )

    def _matrix_and_gold_checks(
        self,
        *,
        sessions: Sequence[str],
        required_datasets: Sequence[str],
        now: datetime,
    ) -> tuple[ReadinessCheck, ReadinessCheck]:
        # The accepted exchange calendar is an independent immutable authority
        # and may intentionally include future sessions. Daily source/Silver/DQ/
        # Gold partitions can only be required through the latest session whose
        # close is already behind the PostgreSQL clock; demanding future data
        # makes first-epoch bootstrap logically impossible.
        local_now = now.astimezone(_SHANGHAI)
        completed_sessions = tuple(
            session
            for session in sessions
            if (
                date.fromisoformat(session) < local_now.date()
                or (
                    date.fromisoformat(session) == local_now.date()
                    and local_now.replace(tzinfo=None).time() >= _MARKET_CLOSE
                )
            )
        )
        rows = self.ledger.list_partitions(source_id="research_os", limit=100_000)
        by_identity = {
            (item.identity.dataset, item.identity.partition_key): item for item in rows
        }
        missing_cells: list[dict[str, str]] = []
        invalid_stages: list[dict[str, str]] = []
        silver_ids: set[str] = set()
        gold_ids_by_session: dict[str, str] = {}
        stage_results: dict[tuple[str, str], Mapping[str, Any]] = {}
        for session in completed_sessions:
            results: dict[str, Mapping[str, Any] | None] = {}
            for stage, dataset in _STAGE_DATASETS.items():
                record = by_identity.get((dataset, session))
                result = None if record is None else self._partition_result(record)
                results[stage] = result
                if result is None:
                    invalid_stages.append({"session": session, "stage": stage})
                else:
                    stage_results[(session, stage)] = result
            source = results["source"]
            observed_datasets = {
                str(item.get("dataset") or "")
                for item in (
                    ((source.get("outputs") or {}).get("sources") or ())
                    if isinstance(source, Mapping)
                    else ()
                )
                if isinstance(item, Mapping)
            }
            for dataset in required_datasets:
                if dataset not in observed_datasets:
                    missing_cells.append(
                        {"session": session, "dataset": dataset, "stage": "source"}
                    )
                    continue
                for stage in ("silver", "data_quality", "gold"):
                    if results[stage] is None:
                        missing_cells.append(
                            {"session": session, "dataset": dataset, "stage": stage}
                        )
            silver = results["silver"]
            dq = results["data_quality"]
            gold = results["gold"]
            silver_id = str(
                (((silver or {}).get("outputs") or {}).get("silver_snapshot_id") or "")
            )
            source_ids = tuple(
                map(
                    str,
                    (((source or {}).get("outputs") or {}).get("bronze_snapshot_ids") or ()),
                )
            )
            if silver_id:
                silver_ids.add(silver_id)
                silver_record = self.catalog.get_snapshot(silver_id)
                silver_errors = self._snapshot_errors(
                    silver_id, expected_tier=SnapshotTier.SILVER
                )
                if (
                    silver_errors
                    or silver_record is None
                    or not set(source_ids).issubset(
                        set(silver_record.reference.parent_snapshot_ids)
                    )
                ):
                    invalid_stages.append(
                        {"session": session, "stage": "silver_parent_closure"}
                    )
            if str((((dq or {}).get("outputs") or {}).get("silver_snapshot_id") or "")) != silver_id:
                invalid_stages.append({"session": session, "stage": "dq_silver_binding"})
            quality = ((dq or {}).get("outputs") or {}).get("quality_report")
            if not isinstance(quality, Mapping) or quality.get("status") != "pass":
                invalid_stages.append({"session": session, "stage": "dq_report"})
            gold_id = str(
                (((gold or {}).get("outputs") or {}).get("snapshot_id") or "")
            )
            if gold_id:
                gold_ids_by_session[session] = gold_id

        matrix_blockers: list[str] = []
        if not completed_sessions:
            matrix_blockers.append("required_dataset_stage_matrix_has_no_sessions")
        if missing_cells:
            matrix_blockers.append("required_dataset_stage_matrix_incomplete")
        if invalid_stages:
            matrix_blockers.append("production_stage_closure_invalid")
        expected_cells = (
            len(completed_sessions) * len(required_datasets) * len(_STAGE_DATASETS)
        )
        matrix = self._check(
            "required_dataset_stage_matrix",
            matrix_blockers,
            required_datasets=list(required_datasets),
            expected_cells=expected_cells,
            missing_cell_count=len(missing_cells),
            missing_cells=missing_cells[:100],
            invalid_stage_count=len(invalid_stages),
            invalid_stages=invalid_stages[:100],
            calendar_session_count=len(sessions),
            completed_session_count=len(completed_sessions),
            required_through=(
                completed_sessions[-1] if completed_sessions else None
            ),
            future_calendar_session_count=len(sessions) - len(completed_sessions),
        )

        gold_blockers: list[str] = []
        latest_session = completed_sessions[-1] if completed_sessions else None
        latest_gold_id = (
            None if latest_session is None else gold_ids_by_session.get(latest_session)
        )
        if not latest_gold_id:
            gold_blockers.append("accepted_gold_missing_for_latest_session")
        else:
            latest_record = self.catalog.get_snapshot(latest_gold_id)
            errors = self._snapshot_errors(
                latest_gold_id, expected_tier=SnapshotTier.GOLD
            )
            if errors:
                gold_blockers.append("accepted_gold_hash_or_parent_closure_invalid")
            if latest_record is not None and not silver_ids.issubset(
                set(latest_record.reference.parent_snapshot_ids)
            ):
                gold_blockers.append("accepted_gold_omits_accepted_silver_parents")
            latest_result = stage_results.get((latest_session, "gold"))
            outputs = (
                latest_result.get("outputs")
                if isinstance(latest_result, Mapping)
                else None
            )
            if not isinstance(outputs, Mapping) or (
                outputs.get("research_ready") is not True
                or str(outputs.get("analysis_start")) != "2017-01-01"
                or str(outputs.get("analysis_end")) != latest_session
                or tuple(sorted(map(str, outputs.get("parent_snapshot_ids") or ())))
                != tuple(sorted(latest_record.reference.parent_snapshot_ids))
            ):
                gold_blockers.append("accepted_gold_research_contract_invalid")
        latest_distinct = {
            item
            for session, item in gold_ids_by_session.items()
            if session == latest_session
        }
        if latest_session is not None and len(latest_distinct) != 1:
            gold_blockers.append("accepted_gold_not_unique_for_latest_session")
        accepted_gold_heads: tuple[str, ...] = ()
        accepted_gold_count = 0
        cursor = None
        newest_as_of: datetime | None = None
        head_ids: list[str] = []
        while True:
            page = self.catalog.list_snapshot_page(
                limit=1_000,
                quality_status=DataQualityStatus.ACCEPTED,
                tier=SnapshotTier.GOLD,
                after=cursor,
            )
            for item in page.records:
                # Account execution/mark roles are accepted Gold evidence but
                # are not research-panel heads.  Mixing those namespaces makes
                # every valid typed shadow session look like a second Gold
                # data-lake head.
                if str(item.reference.manifest.get("role") or "") in {
                    "execution",
                    "mark",
                }:
                    continue
                accepted_gold_count += 1
                item_as_of = item.reference.as_of.astimezone(timezone.utc)
                if newest_as_of is None:
                    newest_as_of = item_as_of
                if item_as_of == newest_as_of:
                    head_ids.append(item.reference.snapshot_id)
            if page.next_cursor is None:
                break
            if accepted_gold_count >= 100_000:
                gold_blockers.append("accepted_gold_catalog_scan_truncated")
                break
            cursor = page.next_cursor
        accepted_gold_heads = tuple(sorted(set(head_ids)))
        if accepted_gold_heads != (() if latest_gold_id is None else (latest_gold_id,)):
            gold_blockers.append("accepted_gold_current_head_not_unique_or_unbound")
        gold_check = self._check(
            "accepted_gold_parent_closure",
            gold_blockers,
            latest_session=latest_session,
            latest_gold_snapshot_id=latest_gold_id,
            accepted_gold_head_snapshot_ids=list(accepted_gold_heads),
            accepted_gold_catalog_count=accepted_gold_count,
            accepted_silver_parent_count=len(silver_ids),
            gold_snapshot_count=len(set(gold_ids_by_session.values())),
        )
        return matrix, gold_check

    def _valid_restore_run(self, run: RunRecord, *, now: datetime) -> tuple[bool, str | None]:
        metadata = run.metadata
        expected = restore_drill_evidence_hash(metadata)
        expected_sha = str(metadata.get("expected_sha256") or "")
        try:
            verified_at = _parse_time(metadata.get("verified_at"))
            expected_size = int(metadata.get("expected_size_bytes") or 0)
            first_size = int(metadata.get("first_restore_size_bytes") or 0)
        except (TypeError, ValueError):
            return False, None
        deleted_cache_proof = content_fingerprint(
            {
                "deletion_challenge": metadata.get("deletion_challenge"),
                "source_canary_evidence_hash": metadata.get(
                    "source_canary_evidence_hash"
                ),
                "source_snapshot_id": metadata.get("source_snapshot_id"),
                "object_sha256": expected_sha,
                "size_bytes": expected_size,
                "first_restore_sha256": metadata.get("first_restore_sha256"),
                "first_restore_size_bytes": first_size,
                "cache_existed_after_first_restore": True,
                "cache_absent_before_second_restore": True,
            },
            domain="factor-lab/research-os/v1/restore-drill-deleted-cache-proof",
        )
        if not (
            run.run_type == RESTORE_DRILL_RUN_TYPE
            and run.status == "succeeded"
            and run.run_id == f"restore_{expected}"
            and run.completed_at is not None
            and run.started_at <= run.completed_at <= now
            and timedelta(0) <= now - run.completed_at <= _RESTORE_MAX_AGE
            and verified_at == run.completed_at
            and metadata.get("schema_version") == RESTORE_DRILL_SCHEMA_VERSION
            and metadata.get("authority")
            == "code_selected_physical_canary_twice_hydrated"
            and metadata.get("physical") is True
            and metadata.get("controlled_test_object_store") is False
            and metadata.get("readiness_admission")
            == "physical_minio_restore_drill"
            and str(metadata.get("object_uri") or "").startswith("s3://")
            and f"/sha256={expected_sha}/"
            in str(metadata.get("object_uri") or "")
            and _valid_hash(expected_sha)
            and metadata.get("restored_sha256") == expected_sha
            and expected_size > 0
            and metadata.get("restored_size_bytes")
            == expected_size
            and metadata.get("first_restore_sha256") == expected_sha
            and first_size == expected_size
            and metadata.get("cache_deleted_before_second_restore") is True
            and metadata.get("second_restore_downloaded") is True
            and metadata.get("local_cache_retained") is False
            and _valid_hash(metadata.get("deletion_challenge"))
            and metadata.get("deleted_cache_proof") == deleted_cache_proof
            and str(metadata.get("source_canary_run_id") or "")
            and _valid_hash(metadata.get("source_canary_evidence_hash"))
            and str(metadata.get("source_snapshot_id") or "")
            and metadata.get("source_snapshot_role") == "mark"
            and str(metadata.get("source_snapshot_trade_date") or "")
            and metadata.get("restore_evidence_hash") == expected
            and run.input_fingerprint == expected
        ):
            return False, None
        return True, expected

    def _restore_check(self, *, now: datetime) -> tuple[ReadinessCheck, RunRecord | None]:
        runs = self.catalog.list_runs(limit=1_000, run_type=RESTORE_DRILL_RUN_TYPE)
        for run in runs:
            valid, evidence_hash = self._valid_restore_run(run, now=now)
            if valid:
                return (
                    self._check(
                        "minio_restore_drill",
                        (),
                        run_id=run.run_id,
                        evidence_hash=evidence_hash,
                        completed_at=run.completed_at,
                    ),
                    run,
                )
        return (
            self._check(
                "minio_restore_drill",
                ("physical_minio_restore_drill_missing",),
                inspected_runs=len(runs),
            ),
            None,
        )

    def _physical_canary_snapshot_errors(
        self,
        evidence: Mapping[str, Any],
        *,
        run_id: str,
        evidence_by_snapshot: Mapping[str, Mapping[str, Any]],
        bound_partitions: Sequence[PartitionRecord],
        sessions: Sequence[date],
        visiting: frozenset[str] = frozenset(),
    ) -> tuple[str, ...]:
        snapshot_id = str(evidence.get("snapshot_id") or "")
        if snapshot_id in visiting:
            return ("physical_snapshot_parent_cycle",)
        record = self.catalog.get_snapshot(snapshot_id)
        if record is None:
            return ("physical_snapshot_missing",)
        reference = record.reference
        manifest = reference.manifest
        errors: list[str] = []
        expected_content_hash = content_fingerprint(
            manifest,
            domain="factor-lab/research-os/v1/physical-canary-snapshot",
        )
        physical_object = manifest.get("physical_object")
        if not isinstance(physical_object, Mapping):
            errors.append("physical_snapshot_object_manifest_missing")
            physical_object = {}
        object_sha = str(evidence.get("object_sha256") or "")
        object_uri = str(evidence.get("uri") or "")
        evidence_tier = str(evidence.get("tier") or "")
        evidence_role = str(evidence.get("role") or "")
        evidence_trade_date = str(evidence.get("trade_date") or "")
        evidence_content_hash = str(evidence.get("content_hash") or "")
        expected_stage = {
            SnapshotTier.BRONZE.value: "bronze",
            SnapshotTier.SILVER.value: "silver",
            SnapshotTier.GOLD.value: "gold",
        }.get(evidence_tier)
        matching_partitions = tuple(
            item
            for item in bound_partitions
            if item.status is PartitionStatus.SUCCEEDED
            and item.output_snapshot_id == snapshot_id
            and item.output_hash == evidence_content_hash
            and item.run_id == run_id
            and item.identity.partition_key == evidence_trade_date
            and str(item.details.get("stage") or "") == expected_stage
            and (
                expected_stage != "gold"
                or str(item.details.get("role") or "") == evidence_role
            )
            and (
                not item.details.get("role")
                or str(item.details.get("role")) == evidence_role
            )
            and str(item.details.get("run_id") or "") == run_id
        )
        if len(matching_partitions) != 1:
            errors.append("physical_snapshot_partition_binding_invalid")
            bound_partition = None
        else:
            bound_partition = matching_partitions[0]
        if not (
            reference.quality_status is DataQualityStatus.ACCEPTED
            and reference.snapshot_id == snapshot_id
            and reference.content_hash == expected_content_hash
            and str(evidence.get("content_hash") or "") == expected_content_hash
            and reference.tier.value == str(evidence.get("tier") or "")
            and reference.uri == object_uri
            and object_uri.startswith("s3://")
            and _valid_hash(object_sha)
            and f"/sha256={object_sha}/" in object_uri
            and int(evidence.get("size_bytes") or 0) > 0
            and str(physical_object.get("uri") or "") == object_uri
            and str(physical_object.get("sha256") or "") == object_sha
            and int(physical_object.get("size_bytes") or 0)
            == int(evidence.get("size_bytes") or 0)
            and manifest.get("run_id") == run_id
            and manifest.get("evidence_schema") == PHYSICAL_CANARY_SCHEMA_VERSION
            and manifest.get("evidence_class") == "engineering_canary"
            and manifest.get("evidence_scope") == "non_forward"
            and manifest.get("formal_epoch_eligible") is False
            and manifest.get("physical_source_attested") is True
            and manifest.get("controlled_test_adapter") is False
            and manifest.get("readiness_admission")
            == "physical_engineering_prerequisite"
            and str(manifest.get("tier") or "") == reference.tier.value
            and str(manifest.get("role") or "")
            == str(evidence.get("role") or "")
            and str(manifest.get("trade_date") or "")
            == str(evidence.get("trade_date") or "")
            and tuple(map(str, manifest.get("parent_snapshot_ids") or ()))
            == reference.parent_snapshot_ids
            and set(reference.trust_labels)
            == {
                "physical_engineering_canary",
                "non_forward",
                "retrospective_physical_replay",
            }
        ):
            errors.append("physical_snapshot_contract_or_hash_invalid")

        if reference.tier is SnapshotTier.GOLD:
            opening_audit = manifest.get("opening_cross_check")
            formal_ready = manifest.get("opening_execution_formal_ready")
            if formal_ready is not False:
                errors.append("physical_gold_formal_execution_scope_invalid")
            if evidence_role == "execution":
                required_columns = {
                    "daily_session_open_raw",
                    "execution_minute_open_raw",
                    "execution_minute_low_raw",
                    "execution_minute_high_raw",
                    "execution_vs_daily_open_abs_diff",
                    "execution_vs_daily_open_one_tick_match",
                }
                columns = manifest.get("columns")
                if not (
                    isinstance(columns, Sequence)
                    and not isinstance(columns, (str, bytes, bytearray))
                    and required_columns.issubset(map(str, columns))
                ):
                    errors.append("physical_execution_gold_audit_columns_missing")
                if not isinstance(opening_audit, Mapping):
                    errors.append("physical_execution_opening_cross_check_invalid")
                else:
                    mismatch_count = opening_audit.get("one_tick_mismatch_count")
                    maximum_difference = opening_audit.get(
                        "maximum_absolute_difference"
                    )
                    valid_maximum = (
                        isinstance(maximum_difference, (int, float))
                        and not isinstance(maximum_difference, bool)
                        and isfinite(float(maximum_difference))
                        and float(maximum_difference) >= 0.0
                    )
                    if not (
                        opening_audit.get("comparison_semantics")
                        == "daily_session_open_vs_distinct_09_30_minute_bar_open"
                        and isinstance(mismatch_count, int)
                        and not isinstance(mismatch_count, bool)
                        and mismatch_count >= 0
                        and valid_maximum
                        and opening_audit.get("daily_range_violation_count") == 0
                        and opening_audit.get("minute_bar_range_violation_count")
                        == 0
                    ):
                        errors.append("physical_execution_opening_cross_check_invalid")
                if (
                    bound_partition is not None
                    and bound_partition.details.get("opening_cross_check")
                    != opening_audit
                ):
                    errors.append("physical_execution_opening_audit_binding_invalid")
            elif evidence_role == "mark":
                if opening_audit is not None:
                    errors.append("physical_mark_opening_cross_check_invalid")
                if (
                    bound_partition is not None
                    and bound_partition.details.get("opening_cross_check") is not None
                ):
                    errors.append("physical_mark_opening_audit_binding_invalid")
            else:
                errors.append("physical_gold_role_invalid")
        elif reference.tier is SnapshotTier.SILVER:
            quality_report = manifest.get("quality_report")
            if not isinstance(quality_report, Mapping):
                errors.append("physical_silver_quality_report_invalid")
            else:
                reconciliation = quality_report.get("reconciliation")
                datasets = quality_report.get("datasets")
                required_datasets = {
                    "trade_calendar",
                    "daily",
                    "adj_factor",
                    "historical_st",
                    "stock_limit",
                }
                if sessions and evidence_trade_date != sessions[0].isoformat():
                    required_datasets.add("opening_execution")
                try:
                    historical_st_rows = int(
                        quality_report.get("historical_st_rows") or 0
                    )
                except (TypeError, ValueError):
                    historical_st_rows = 0
                if not (
                    quality_report.get("schema_version")
                    == PHYSICAL_CANARY_SCHEMA_VERSION
                    and quality_report.get("status") == "accepted"
                    and str(quality_report.get("trade_date") or "")
                    == evidence_trade_date
                    and isinstance(datasets, Sequence)
                    and not isinstance(datasets, (str, bytes, bytearray))
                    and required_datasets.issubset(map(str, datasets))
                    and historical_st_rows > 0
                    and isinstance(reconciliation, Mapping)
                    and reconciliation.get("schema_version")
                    == RECONCILIATION_EVALUATOR_SCHEMA
                    and reconciliation.get("status") == "pass"
                    and reconciliation.get("disputed_group_count") == 0
                    and reconciliation.get("quarantined_row_count") == 0
                ):
                    errors.append("physical_silver_quality_report_invalid")
                report_hash = content_fingerprint(
                    dict(quality_report),
                    domain="factor-lab/research-os/v1/physical-canary-dq-report",
                )
                matching_dq = tuple(
                    item
                    for item in bound_partitions
                    if item.status is PartitionStatus.SUCCEEDED
                    and item.output_snapshot_id == snapshot_id
                    and item.output_hash == report_hash
                    and item.run_id == run_id
                    and item.identity.partition_key == evidence_trade_date
                    and str(item.details.get("stage") or "") == "data_quality"
                    and str(item.details.get("run_id") or "") == run_id
                    and item.details.get("quality_status") == "accepted"
                    and item.details.get("quality_report") == quality_report
                )
                if len(matching_dq) != 1:
                    errors.append("physical_silver_dq_partition_binding_invalid")
        next_visiting = visiting | {snapshot_id}
        for parent_id in reference.parent_snapshot_ids:
            parent = evidence_by_snapshot.get(parent_id)
            if parent is None:
                errors.append("physical_snapshot_parent_evidence_missing")
                continue
            errors.extend(
                self._physical_canary_snapshot_errors(
                    parent,
                    run_id=run_id,
                    evidence_by_snapshot=evidence_by_snapshot,
                    bound_partitions=bound_partitions,
                    sessions=sessions,
                    visiting=next_visiting,
                )
            )
        return tuple(sorted(set(errors)))

    def _physical_canary_session_errors(
        self,
        metadata: Mapping[str, Any],
        *,
        sessions: Sequence[date],
        evidence_by_snapshot: Mapping[str, Mapping[str, Any]],
    ) -> tuple[str, ...]:
        return physical_canary_session_errors(
            self.ledger.engine,
            metadata,
            sessions=sessions,
            evidence_by_snapshot=evidence_by_snapshot,
        )

    def _physical_canary_attempt_is_current(
        self,
        run: RunRecord,
        *,
        calendar_window: Sequence[str],
        expected_build_identity: str,
        oci_check: ReadinessCheck | None,
    ) -> tuple[bool, tuple[str, ...]]:
        """Identify the one generation/window that readiness may inspect.

        A succeeded run is not automatically authoritative forever.  The
        physical service fingerprints the accepted calendar window and stable
        evaluator generation; readiness mirrors those immutable dimensions
        before selecting the newest attempt.  Status and completion evidence
        are deliberately *not* part of eligibility: a newer failed/running
        attempt must block fallback to an older success.
        """

        metadata = run.metadata
        reasons: list[str] = []
        raw_sessions = tuple(map(str, metadata.get("calendar_sessions") or ()))
        if not calendar_window or raw_sessions != tuple(calendar_window):
            reasons.append("calendar_window_not_current")
        if not (
            metadata.get("evidence_schema") == PHYSICAL_CANARY_SCHEMA_VERSION
            and metadata.get("evidence_class") == "engineering_canary"
            and metadata.get("evidence_scope") == "non_forward"
            and metadata.get("formal_epoch_eligible") is False
            and metadata.get("physical_source_attested") is True
            and metadata.get("controlled_test_adapter") is False
            and metadata.get("readiness_admission")
            == "physical_engineering_prerequisite"
        ):
            reasons.append("attempt_admission_labels_invalid")
        evaluator_identity = metadata.get("evaluator_identity")
        build_provenance = (
            evaluator_identity.get("build_provenance")
            if isinstance(evaluator_identity, Mapping)
            else None
        )
        if not (
            isinstance(evaluator_identity, Mapping)
            and evaluator_identity.get("identity_schema")
            == PHYSICAL_CANARY_EVALUATOR_IDENTITY_SCHEMA
            and evaluator_identity.get("physical_canary_schema")
            == PHYSICAL_CANARY_SCHEMA_VERSION
            and evaluator_identity.get("reconciliation_schema")
            == RECONCILIATION_EVALUATOR_SCHEMA
            and evaluator_identity.get("mode") == "production_image"
            and isinstance(build_provenance, Mapping)
            and expected_build_identity
            and build_provenance.get("build_identity_hash")
            == expected_build_identity
        ):
            reasons.append("attempt_evaluator_generation_not_current")
        if self.ledger.engine.dialect.name == "postgresql":
            current_oci = {} if oci_check is None else dict(oci_check.evidence)
            runtime_deployment = (
                evaluator_identity.get("runtime_deployment")
                if isinstance(evaluator_identity, Mapping)
                else None
            )
            if not (
                oci_check is not None
                and oci_check.passed
                and isinstance(runtime_deployment, Mapping)
                and runtime_deployment.get("controlled_test_backend") is False
                and runtime_deployment.get("compose_config_hash")
                == current_oci.get("compose_config_hash")
                and runtime_deployment.get("build_identity_hash")
                == current_oci.get("build_identity_hash")
                and runtime_deployment.get("runtime_contract_hash")
                == current_oci.get("runtime_contract_hash")
                and runtime_deployment.get("oci_image_id")
                == current_oci.get("oci_image_id")
                and tuple(runtime_deployment.get("oci_repo_digests") or ())
                == tuple(current_oci.get("oci_repo_digests") or ())
                and tuple(runtime_deployment.get("oci_base_digests") or ())
                == tuple(current_oci.get("oci_base_digests") or ())
            ):
                reasons.append("attempt_runtime_generation_not_current")
        return not reasons, tuple(sorted(set(reasons)))

    @staticmethod
    def _physical_canary_incident_is_related(
        incident: IncidentRecord,
        *,
        run_id: str,
        calendar_window: Sequence[str],
        claimed_partition_ids: frozenset[str],
    ) -> bool:
        """Return whether an incident can invalidate this canary authority."""

        payload = incident.payload
        resolution = payload.get("resolution")
        replacement_partition_id = (
            str(resolution.get("replacement_partition_run_id") or "")
            if isinstance(resolution, Mapping)
            else ""
        )
        directly_bound = bool(
            (incident.partition_run_id or "") in claimed_partition_ids
            or replacement_partition_id in claimed_partition_ids
            or str(payload.get("run_id") or "") == run_id
        )
        if directly_bound:
            return True
        if incident.partition_key not in set(calendar_window):
            return False
        labeled_physical = bool(
            payload.get("evidence_class") == "engineering_canary"
            or payload.get("readiness_admission")
            == "physical_engineering_prerequisite"
            or incident.error_code
            in {"legacy_canary_generation_isolated", "gold_market_semantics_rejected"}
            or any(
                str(source_id).startswith(("engcan_", "engineering_canary_"))
                for source_id in incident.source_ids
            )
        )
        return labeled_physical

    def _physical_canary_incident_errors(
        self,
        *,
        run: RunRecord,
        calendar_window: Sequence[str],
        partitions: Mapping[str, PartitionRecord],
    ) -> tuple[tuple[str, ...], tuple[dict[str, Any], ...]]:
        """Require every related incident to be closed by this exact success.

        Merely flipping an incident to ``resolved`` is insufficient.  Its
        immutable resolution must name a succeeded partition claimed by this
        canary, bind the same snapshot/hash, and occur between the incident and
        the successful parent completion.  This makes recovery causal and
        prevents a stale success from becoming ready again after a later data
        failure.
        """

        completed_at = run.completed_at
        claimed_partition_ids = frozenset(
            map(str, run.metadata.get("partition_run_ids") or ())
        )
        errors: list[str] = []
        inspected: list[dict[str, Any]] = []
        for incident in self.ledger.list_incidents(limit=10_000):
            if not self._physical_canary_incident_is_related(
                incident,
                run_id=run.run_id,
                calendar_window=calendar_window,
                claimed_partition_ids=claimed_partition_ids,
            ):
                continue
            reason: str | None = None
            if incident.status is IncidentStatus.OPEN:
                reason = "related_incident_open"
            else:
                resolution = incident.payload.get("resolution")
                replacement_id = (
                    str(resolution.get("replacement_partition_run_id") or "")
                    if isinstance(resolution, Mapping)
                    else ""
                )
                replacement = partitions.get(replacement_id)
                if not (
                    completed_at is not None
                    and incident.resolved_at is not None
                    and incident.occurred_at <= incident.resolved_at <= completed_at
                    and replacement_id in claimed_partition_ids
                    and replacement is not None
                    and replacement.status is PartitionStatus.SUCCEEDED
                    and replacement.completed_at is not None
                    and incident.occurred_at
                    <= replacement.completed_at
                    <= incident.resolved_at
                    and isinstance(resolution, Mapping)
                    and str(resolution.get("replacement_output_snapshot_id") or "")
                    == str(replacement.output_snapshot_id or "")
                    and str(resolution.get("replacement_output_hash") or "")
                    == str(replacement.output_hash or "")
                ):
                    reason = "related_incident_resolution_not_causal"
            inspected.append(
                {
                    "incident_id": incident.incident_id,
                    "status": incident.status.value,
                    "partition_key": incident.partition_key,
                    "reason": reason,
                }
            )
            if reason is not None:
                errors.append(reason)
        return tuple(sorted(set(errors))), tuple(inspected)

    def _physical_canary_check(
        self,
        *,
        now: datetime,
        restore_run: RunRecord | None,
        oci_check: ReadinessCheck | None = None,
        accepted_sessions: Sequence[str] = (),
    ) -> ReadinessCheck:
        all_runs = self.catalog.list_runs(
            limit=1_000, run_type=PHYSICAL_CANARY_RUN_TYPE
        )
        valid_restore_runs = tuple(
            run
            for run in self.catalog.list_runs(
                limit=1_000, run_type=RESTORE_DRILL_RUN_TYPE
            )
            if self._valid_restore_run(run, now=now)[0]
        )
        synthetic_count = len(
            self.catalog.list_runs(limit=1_000, run_type="engineering_canary")
        )
        partition_rows = self.ledger.list_partitions(limit=100_000)
        partitions = {
            item.identity.partition_run_id: item for item in partition_rows
        }
        inspected: list[dict[str, Any]] = []
        expected_build_identity = (
            str(oci_check.evidence.get("build_identity_hash") or "")
            if oci_check is not None
            else ""
        )
        current_calendar_window = (
            tuple(map(str, accepted_sessions[-21:]))
            if len(accepted_sessions) >= 21
            else ()
        )
        eligible_attempts: list[RunRecord] = []
        rejected_attempts: list[dict[str, Any]] = []
        for candidate in all_runs:
            eligible, rejection_reasons = self._physical_canary_attempt_is_current(
                candidate,
                calendar_window=current_calendar_window,
                expected_build_identity=expected_build_identity,
                oci_check=oci_check,
            )
            if eligible:
                eligible_attempts.append(candidate)
            else:
                rejected_attempts.append(
                    {
                        "run_id": candidate.run_id,
                        "started_at": candidate.started_at,
                        "reasons": list(rejection_reasons),
                    }
                )
        eligible_attempts.sort(
            # Invocation order is authoritative.  A slow older success may
            # complete after a newer attempt has already failed; ordering by
            # completion would then resurrect the old success and bypass the
            # fail-closed latest-attempt rule.
            key=lambda item: (item.started_at, item.run_id),
            reverse=True,
        )
        latest_attempt_started_at = (
            eligible_attempts[0].started_at if eligible_attempts else None
        )
        latest_attempts = tuple(
            item
            for item in eligible_attempts
            if item.started_at == latest_attempt_started_at
        )
        # Equal-start attempts are not ordered by evidence; selecting either
        # would be an arbitrary fallback boundary.  Inspect one for diagnostics
        # but retain an explicit blocker below.
        ambiguous_latest_attempt = len(latest_attempts) > 1
        runs = latest_attempts[:1]
        for run in runs:
            metadata = run.metadata
            reasons: list[str] = []
            if ambiguous_latest_attempt:
                reasons.append("latest_eligible_attempt_ambiguous")
            expected_hash = physical_canary_evidence_hash(metadata)
            completed_at = run.completed_at
            if run.status != "succeeded" or completed_at is None:
                reasons.append("not_succeeded")
            elif now - completed_at > _CANARY_MAX_AGE:
                reasons.append("stale")
            if not (
                metadata.get("evidence_schema") == PHYSICAL_CANARY_SCHEMA_VERSION
                and metadata.get("evidence_class") == "engineering_canary"
                and metadata.get("evidence_scope") == "non_forward"
                and metadata.get("formal_epoch_eligible") is False
                and metadata.get("physical_source_attested") is True
                and metadata.get("controlled_test_adapter") is False
                and metadata.get("readiness_admission")
                == "physical_engineering_prerequisite"
                and metadata.get("run_id") == run.run_id
                and metadata.get("run_type") == PHYSICAL_CANARY_RUN_TYPE
                and _valid_hash(metadata.get("input_fingerprint"))
                and metadata.get("input_fingerprint") == run.input_fingerprint
                and int(metadata.get("security_count") or 0) == 50
                and int(metadata.get("projected_session_count") or 0) == 20
                and metadata.get("sleeve_state") == "shadow"
                and metadata.get("canary_evidence_hash") == expected_hash
            ):
                reasons.append("contract_or_hash_invalid")
            evaluator_identity = metadata.get("evaluator_identity")
            build_provenance = (
                evaluator_identity.get("build_provenance")
                if isinstance(evaluator_identity, Mapping)
                else None
            )
            runtime_deployment = (
                evaluator_identity.get("runtime_deployment")
                if isinstance(evaluator_identity, Mapping)
                else None
            )
            runtime_attestation = metadata.get("runtime_attestation_evidence")
            runtime_attestation_errors: tuple[str, ...] = ()
            stable_deployment_matches = True
            if self.ledger.engine.dialect.name == "postgresql":
                current_oci = {} if oci_check is None else dict(oci_check.evidence)
                stable_deployment_matches = bool(
                    oci_check is not None
                    and oci_check.passed
                    and isinstance(runtime_deployment, Mapping)
                    and runtime_deployment.get("controlled_test_backend") is False
                    and runtime_deployment.get("compose_config_hash")
                    == current_oci.get("compose_config_hash")
                    and runtime_deployment.get("build_identity_hash")
                    == current_oci.get("build_identity_hash")
                    and runtime_deployment.get("runtime_contract_hash")
                    == current_oci.get("runtime_contract_hash")
                    and runtime_deployment.get("oci_image_id")
                    == current_oci.get("oci_image_id")
                    and tuple(runtime_deployment.get("oci_repo_digests") or ())
                    == tuple(current_oci.get("oci_repo_digests") or ())
                    and tuple(runtime_deployment.get("oci_base_digests") or ())
                    == tuple(current_oci.get("oci_base_digests") or ())
                )
                proof_run_id = (
                    str(runtime_attestation.get("host_attestation_run_id") or "")
                    if isinstance(runtime_attestation, Mapping)
                    else ""
                )
                runtime_attestation_errors = persisted_attestation_binding_errors(
                    run=(
                        self.catalog.get_run(proof_run_id)
                        if proof_run_id
                        else None
                    ),
                    proof=(
                        runtime_attestation
                        if isinstance(runtime_attestation, Mapping)
                        else None
                    ),
                    stable_deployment=(
                        runtime_deployment
                        if isinstance(runtime_deployment, Mapping)
                        else None
                    ),
                )
            if not (
                isinstance(evaluator_identity, Mapping)
                and evaluator_identity.get("identity_schema")
                == PHYSICAL_CANARY_EVALUATOR_IDENTITY_SCHEMA
                and evaluator_identity.get("physical_canary_schema")
                == PHYSICAL_CANARY_SCHEMA_VERSION
                and evaluator_identity.get("reconciliation_schema")
                == RECONCILIATION_EVALUATOR_SCHEMA
                and evaluator_identity.get("mode") == "production_image"
                and isinstance(build_provenance, Mapping)
                and _valid_hash(build_provenance.get("build_identity_hash"))
                and expected_build_identity
                and build_provenance.get("build_identity_hash")
                == expected_build_identity
                and stable_deployment_matches
            ):
                reasons.append("physical_canary_evaluator_build_mismatch")
            if runtime_attestation_errors:
                reasons.append("physical_canary_runtime_attestation_invalid")
            source_probe_hashes = metadata.get("source_probe_hashes")
            capability_rows = {
                (str(item.get("source_id") or ""), str(item.get("dataset") or "")): item
                for item in self._capability_rows()
            }
            if not isinstance(source_probe_hashes, Mapping) or not source_probe_hashes:
                reasons.append("physical_source_probe_binding_missing")
            else:
                for raw_identity, raw_probe_hash in source_probe_hashes.items():
                    source_id, separator, dataset = str(raw_identity).rpartition(":")
                    capability = capability_rows.get((source_id, dataset))
                    if not (
                        separator
                        and _valid_hash(raw_probe_hash)
                        and capability is not None
                        and str(capability.get("status") or "") == "accepted"
                        and str(capability.get("probe_hash") or "")
                        == str(raw_probe_hash)
                    ):
                        reasons.append("physical_source_probe_binding_invalid")
                        break
            raw_sessions = tuple(map(str, metadata.get("calendar_sessions") or ()))
            try:
                sessions = tuple(date.fromisoformat(item) for item in raw_sessions)
            except ValueError:
                sessions = ()
            if not (
                len(sessions) == 21
                and len(set(sessions)) == 21
                and tuple(sorted(sessions)) == sessions
                and raw_sessions == current_calendar_window
            ):
                reasons.append("calendar_session_contract_invalid")
            account_id = str(metadata.get("account_id") or "")
            account = self.catalog.get_shadow_account(account_id) if account_id else None
            projected_events = (
                self.catalog.list_shadow_events_by_type(
                    account_id=account_id,
                    event_type="account_projected",
                    limit=1_000,
                )
                if account is not None
                else ()
            )
            expected_event_hashes = tuple(
                map(str, metadata.get("shadow_account_event_hashes") or ())
            )
            observed_event_hashes = tuple(
                item.event_hash
                for item in sorted(
                    projected_events, key=lambda item: item.sequence_number
                )
            )
            shadow_session_hashes = tuple(
                map(str, metadata.get("shadow_session_hashes") or ())
            )
            if not (
                account is not None
                and self.catalog.verify_shadow_chain(account_id)
                and len(expected_event_hashes) == 20
                and observed_event_hashes == expected_event_hashes
                and account.last_event_hash == expected_event_hashes[-1]
                and len(shadow_session_hashes) == 20
                and len(set(shadow_session_hashes)) == 20
                and all(_valid_hash(item) for item in shadow_session_hashes)
            ):
                reasons.append("shadow_event_chain_invalid")
            if sessions:
                if self.catalog.count_shadow_sessions(
                    account_id=account_id,
                    since=sessions[0],
                    through=sessions[-1],
                ) != 20:
                    reasons.append("projected_session_count_unverified")
            sleeve_id = str(metadata.get("sleeve_id") or "")
            if not sleeve_id or str(self.catalog.latest_lifecycle_state(sleeve_id) or "") not in {
                "LifecycleState.SHADOW",
                "shadow",
            }:
                reasons.append("physical_canary_sleeve_state_unverified")

            calendar_partition_ids = tuple(
                map(str, metadata.get("accepted_calendar_partition_ids") or ())
            )
            calendar_output_hashes = tuple(
                map(str, metadata.get("accepted_calendar_output_hashes") or ())
            )
            calendar_rows = tuple(
                partitions.get(item) for item in calendar_partition_ids
            )
            if not (
                len(calendar_partition_ids) == len(calendar_output_hashes) == 21
                and all(item is not None for item in calendar_rows)
                and tuple(item.identity.partition_key for item in calendar_rows if item)
                == raw_sessions
                and all(
                    item is not None
                    and item.identity.source_id == "research_os"
                    and item.identity.dataset == "accepted_trade_calendar"
                    and item.status is PartitionStatus.SUCCEEDED
                    and item.output_hash == output_hash
                    for item, output_hash in zip(
                        calendar_rows, calendar_output_hashes
                    )
                )
            ):
                reasons.append("accepted_calendar_partition_binding_invalid")
            partition_ids = tuple(map(str, metadata.get("partition_run_ids") or ()))
            bound_partitions = tuple(
                partitions[item] for item in partition_ids if item in partitions
            )
            if not partition_ids or len(set(partition_ids)) != len(partition_ids) or any(
                item not in partitions
                or partitions[item].status is not PartitionStatus.SUCCEEDED
                or not partitions[item].output_hash
                for item in partition_ids
            ):
                reasons.append("physical_partition_closure_invalid")
            elif not {"bronze", "silver", "data_quality", "gold"}.issubset(
                {str(item.details.get("stage") or "") for item in bound_partitions}
            ) or any(
                item.run_id != run.run_id
                or item.details.get("physical_source_attested") is not True
                or item.details.get("controlled_test_adapter") is not False
                for item in bound_partitions
            ):
                reasons.append("physical_partition_stage_set_incomplete")

            incident_errors, incident_evidence = (
                self._physical_canary_incident_errors(
                    run=run,
                    calendar_window=current_calendar_window,
                    partitions=partitions,
                )
            )
            if incident_errors:
                reasons.extend(incident_errors)

            raw_snapshot_evidence = metadata.get("snapshot_evidence")
            snapshot_evidence = tuple(
                item
                for item in (raw_snapshot_evidence or ())
                if isinstance(item, Mapping)
            )
            evidence_by_snapshot = {
                str(item.get("snapshot_id") or ""): item
                for item in snapshot_evidence
            }
            session_errors = self._physical_canary_session_errors(
                metadata,
                sessions=sessions,
                evidence_by_snapshot=evidence_by_snapshot,
            )
            if session_errors:
                reasons.extend(session_errors)
            snapshot_errors = tuple(
                error
                for item in snapshot_evidence
                for error in self._physical_canary_snapshot_errors(
                    item,
                    run_id=run.run_id,
                    evidence_by_snapshot=evidence_by_snapshot,
                    bound_partitions=bound_partitions,
                    sessions=sessions,
                )
            )
            tier_counts = {
                tier: sum(str(item.get("tier") or "") == tier for item in snapshot_evidence)
                for tier in ("bronze", "silver", "gold")
            }
            if not (
                snapshot_evidence
                and len(evidence_by_snapshot) == len(snapshot_evidence)
                and not snapshot_errors
                and int(metadata.get("physical_object_count") or 0)
                == len(snapshot_evidence)
                and int(metadata.get("bronze_object_count") or 0)
                == tier_counts["bronze"]
                and int(metadata.get("silver_object_count") or 0)
                == tier_counts["silver"]
                and int(metadata.get("gold_object_count") or 0)
                == tier_counts["gold"]
                and metadata.get("opening_execution_formal_ready") is False
            ):
                reasons.append("physical_snapshot_closure_invalid")
            matching_restore = None
            for candidate in valid_restore_runs:
                restore_metadata = candidate.metadata
                selected = evidence_by_snapshot.get(
                    str(restore_metadata.get("source_snapshot_id") or "")
                )
                if not (
                    completed_at is not None
                    and candidate.started_at >= completed_at
                    and candidate.completed_at is not None
                    and candidate.completed_at >= candidate.started_at
                    and restore_metadata.get("source_canary_run_id") == run.run_id
                    and restore_metadata.get("source_canary_evidence_hash")
                    == expected_hash
                    and selected is not None
                    and str(selected.get("tier") or "") == "gold"
                    and str(selected.get("role") or "") == "mark"
                    and str(selected.get("snapshot_id") or "")
                    == str(restore_metadata.get("source_snapshot_id") or "")
                    and str(selected.get("trade_date") or "")
                    == str(restore_metadata.get("source_snapshot_trade_date") or "")
                    and str(selected.get("uri") or "")
                    == str(restore_metadata.get("object_uri") or "")
                    and str(selected.get("object_sha256") or "")
                    == str(restore_metadata.get("expected_sha256") or "")
                    and int(selected.get("size_bytes") or 0)
                    == int(restore_metadata.get("expected_size_bytes") or 0)
                ):
                    continue
                matching_restore = candidate
                break
            if matching_restore is None:
                reasons.append("restore_drill_binding_invalid")
            inspected.append(
                {
                    "run_id": run.run_id,
                    "status": run.status,
                    "started_at": run.started_at,
                    "reasons": sorted(set(reasons)),
                    "related_incidents": list(incident_evidence),
                }
            )
            if not reasons:
                return self._check(
                    "physical_engineering_canary",
                    (),
                    run_id=run.run_id,
                    canary_evidence_hash=expected_hash,
                    account_id=account_id,
                    completed_at=completed_at,
                    synthetic_runs_rejected=synthetic_count,
                    current_calendar_window=list(current_calendar_window),
                    latest_eligible_attempt_run_id=run.run_id,
                    related_incidents=list(incident_evidence),
                )
        return self._check(
            "physical_engineering_canary",
            ("physical_engineering_canary_missing",),
            inspected_runs=inspected,
            synthetic_runs_rejected=synthetic_count,
            current_calendar_window=list(current_calendar_window),
            latest_eligible_attempt_run_id=(
                None if not runs else runs[0].run_id
            ),
            latest_eligible_attempt_count=len(latest_attempts),
            rejected_attempts=rejected_attempts[:100],
        )

    def _execution_contract(
        self,
    ) -> tuple[str, str, Mapping[str, Any], tuple[str, ...]]:
        daily = self.config.get("daily")
        shadow = daily.get("shadow") if isinstance(daily, Mapping) else None
        execution = (
            shadow.get("execution_market_data") if isinstance(shadow, Mapping) else None
        )
        # Configuration is diagnostic only.  The versioned contract, source
        # identity and event semantics are fixed in execution_snapshot_authority;
        # renaming a configured dataset can neither grant nor revoke formal
        # capability without the exact persisted typed chain below.
        if not isinstance(execution, Mapping):
            execution = {}
        return (
            FORMAL_EXECUTION_SOURCE_ID,
            TYPED_EXECUTION_CAPABILITY_DATASET,
            execution,
            tuple(FORMAL_EXECUTION_REQUIRED_FIELDS),
        )

    def _capability_rows(self) -> list[Mapping[str, Any]]:
        metadata = MetaData()
        table = Table(
            "ros_source_capabilities",
            metadata,
            autoload_with=self.ledger.engine,
        )
        with self.ledger.engine.connect() as connection:
            return [dict(row) for row in connection.execute(select(table)).mappings()]

    def _typed_execution_authority_chain(
        self, detail: Mapping[str, Any]
    ) -> tuple[tuple[str, ...], Mapping[str, Any]]:
        blockers: list[str] = []
        evidence: dict[str, Any] = {}
        try:
            trade_date = date.fromisoformat(str(detail.get("trade_date") or ""))
        except ValueError:
            return ("formal_execution_trade_date_invalid",), evidence
        partition_key = trade_date.isoformat()
        evidence["trade_date"] = partition_key
        open_source_id = str(detail.get("open_source_id") or "diemeng")
        open_source_role = str(
            detail.get("open_source_role") or f"{open_source_id}_open_observation"
        )
        if (
            open_source_id not in {"diemeng", "tushare"}
            or open_source_role != f"{open_source_id}_open_observation"
        ):
            blockers.append("formal_execution_source_identity_invalid")
        evidence["open_source_id"] = open_source_id
        evidence["open_source_role"] = open_source_role
        partition_specs = (
            (
                "typed",
                PartitionIdentity(
                    FORMAL_EXECUTION_SOURCE_ID,
                    TYPED_EXECUTION_OUTPUT_DATASET,
                    partition_key,
                ),
                "typed_partition_hash",
            ),
            (
                "source",
                PartitionIdentity(open_source_id, TYPED_OPEN_DATASET, partition_key),
                "source_partition_hash",
            ),
            (
                "gold",
                PartitionIdentity("research_os", "stage_gold", partition_key),
                "gold_partition_hash",
            ),
            (
                "data_quality",
                PartitionIdentity("research_os", "stage_data_quality", partition_key),
                "data_quality_partition_hash",
            ),
        )
        partitions: dict[str, PartitionRecord | None] = {}
        for role, identity, hash_field in partition_specs:
            record = self.ledger.get_partition(identity)
            partitions[role] = record
            if (
                record is None
                or record.status is not PartitionStatus.SUCCEEDED
                or record.output_hash != str(detail.get(hash_field) or "")
            ):
                blockers.append(f"formal_execution_{role}_partition_invalid")
        typed = partitions.get("typed")
        source = partitions.get("source")
        if typed is not None and typed.status is PartitionStatus.SUCCEEDED:
            typed_detail = typed.details
            capability = typed_detail.get("capability")
            if not (
                typed.output_snapshot_id == str(detail.get("bundle_snapshot_id") or "")
                and str(typed_detail.get("execution_snapshot_id") or "")
                == str(detail.get("execution_snapshot_id") or "")
                and str(typed_detail.get("mark_snapshot_id") or "")
                == str(detail.get("mark_snapshot_id") or "")
                and str(typed_detail.get("bundle_snapshot_id") or "")
                == str(detail.get("bundle_snapshot_id") or "")
                and isinstance(capability, Mapping)
                and capability.get("decision") == "accepted"
                and str(capability.get("evidence_hash") or "")
                == str(detail.get("capability_evidence_hash") or "")
            ):
                blockers.append("formal_execution_typed_partition_binding_invalid")
        if source is not None and source.status is PartitionStatus.SUCCEEDED:
            if source.output_snapshot_id != str(detail.get("source_snapshot_id") or ""):
                blockers.append("formal_execution_source_partition_binding_invalid")
            if open_source_id == "tushare":
                source_detail = source.details
                received_raw = source_detail.get("database_received_at")
                try:
                    received_at = datetime.fromisoformat(str(received_raw))
                    if received_at.tzinfo is None or received_at.utcoffset() is None:
                        raise ValueError("naive database receive time")
                    received_local = received_at.astimezone(_SHANGHAI)
                    opening = datetime.combine(
                        trade_date,
                        time(9, 30),
                        tzinfo=_SHANGHAI,
                    )
                    deadline = opening + timedelta(minutes=5)
                    clock_skew = float(
                        source_detail.get("collector_clock_skew_seconds")
                    )
                except (TypeError, ValueError):
                    blockers.append("formal_execution_collector_clock_invalid")
                else:
                    if not (
                        source_detail.get("collector_clock_verified") is True
                        and (
                            source_detail.get("complete_observed_universe") is True
                            or _valid_tushare_batch_lineage(
                                source_detail.get("source_lineage")
                            )
                        )
                        and 0.0 <= clock_skew <= 30.0
                        and opening <= received_local <= deadline
                        and str(detail.get("database_received_at") or "")
                        == str(received_raw or "")
                    ):
                        blockers.append("formal_execution_collector_clock_invalid")

        physical_hashes = detail.get("physical_object_hashes")
        if not isinstance(physical_hashes, Mapping):
            physical_hashes = {}
        snapshot_specs = (
            (
                "source",
                "source_snapshot_id",
                "source_snapshot_hash",
                open_source_role,
            ),
            ("execution", "execution_snapshot_id", "execution_snapshot_hash", "execution"),
            ("mark", "mark_snapshot_id", "mark_snapshot_hash", "mark"),
            (
                "bundle",
                "bundle_snapshot_id",
                "bundle_snapshot_hash",
                TYPED_EXECUTION_BUNDLE_ROLE,
            ),
        )
        snapshot_ids: dict[str, str] = {}
        snapshots: dict[str, Any] = {}
        for role, id_field, hash_field, expected_role in snapshot_specs:
            snapshot_id = str(detail.get(id_field) or "")
            snapshot_ids[role] = snapshot_id
            record = self.catalog.get_snapshot(snapshot_id) if snapshot_id else None
            snapshots[role] = record
            reference = None if record is None else record.reference
            if not (
                reference is not None
                and reference.snapshot_id == snapshot_id
                and reference.content_hash == str(detail.get(hash_field) or "")
                and reference.quality_status is DataQualityStatus.ACCEPTED
                and str(reference.manifest.get("role") or "") == expected_role
            ):
                blockers.append(f"formal_execution_{role}_snapshot_invalid")
                continue
            physical = reference.manifest.get("physical_object")
            expected_physical = str(physical_hashes.get(role) or "")
            if not (
                isinstance(physical, Mapping)
                and _valid_hash(physical.get("sha256"))
                and str(physical.get("sha256")) == expected_physical
            ):
                blockers.append(f"formal_execution_{role}_physical_object_invalid")
        if len({snapshot_ids.get("execution"), snapshot_ids.get("mark")}) != 2:
            blockers.append("formal_execution_roles_not_separate")
        bundle_record = snapshots.get("bundle")
        if bundle_record is not None:
            expected_parents = {
                snapshot_ids.get("execution"),
                snapshot_ids.get("mark"),
            }
            if set(bundle_record.reference.parent_snapshot_ids) != expected_parents:
                blockers.append("formal_execution_bundle_parent_closure_invalid")
        evidence["snapshot_ids"] = snapshot_ids
        return tuple(sorted(set(blockers))), evidence

    def _formal_execution_check(self) -> ReadinessCheck:
        blockers: list[str] = []
        evidence: dict[str, Any] = {}
        try:
            source_id, dataset, execution, required_fields = self._execution_contract()
            rows = self._capability_rows()
            row = next(
                (
                    item
                    for item in rows
                    if str(item.get("source_id")) == source_id
                    and str(item.get("dataset")) == dataset
                ),
                None,
            )
            expected_contract_hash = TYPED_EXECUTION_CONTRACT_HASH
            evidence.update(
                source_id=source_id,
                dataset=dataset,
                configured_source=execution.get("source"),
                configured_source_dataset=execution.get("dataset"),
                configured_collection_mode=execution.get("collection_mode"),
                configured_declaration_is_authoritative=False,
                required_fields=list(required_fields),
                expected_contract_hash=expected_contract_hash,
            )
            if row is None:
                blockers.append("formal_execution_pg_probe_missing")
            else:
                raw_fields = row.get("fields_json") or ()
                if isinstance(raw_fields, str):
                    raw_fields = json.loads(raw_fields)
                observed_fields = tuple(sorted(set(map(str, raw_fields))))
                detail_value = row.get("detail")
                try:
                    detail = (
                        dict(detail_value)
                        if isinstance(detail_value, Mapping)
                        else json.loads(str(detail_value))
                    )
                except (TypeError, ValueError, json.JSONDecodeError):
                    detail = {}
                probe_hash = str(row.get("probe_hash") or "")
                recomputed = formal_execution_probe_hash(
                    source_id=source_id,
                    dataset=dataset,
                    contract_hash=str(row.get("contract_hash") or ""),
                    fields=observed_fields,
                    detail=detail,
                )
                evidence.update(
                    status=row.get("status"),
                    probe_hash=probe_hash or None,
                    observed_fields=list(observed_fields),
                    probed_at=row.get("probed_at"),
                )
                if str(row.get("status")) != "accepted":
                    blockers.append("formal_execution_capability_not_accepted")
                if str(row.get("contract_hash")) != expected_contract_hash:
                    blockers.append("formal_execution_contract_hash_mismatch")
                if observed_fields != tuple(sorted(required_fields)):
                    blockers.append("formal_execution_schema_not_exact")
                if not (
                    _valid_hash(probe_hash)
                    and probe_hash != "0" * 64
                    and probe_hash == recomputed
                ):
                    blockers.append("formal_execution_real_probe_hash_invalid")
                common_semantics = bool(
                    detail.get("schema_version")
                    == FORMAL_EXECUTION_CAPABILITY_SCHEMA_VERSION
                    and detail.get("authority_schema_version")
                    == "research-os/execution-snapshot/v1"
                    and detail.get("decision") == "accepted"
                    and detail.get("real_source_probe") is True
                    and detail.get("physical") is True
                    and detail.get("point_in_time") is True
                    and detail.get("formal_shadow_projection") == "allowed"
                    and detail.get("collection_mode") == "realtime_open"
                    and detail.get("tradability_state_verified") is True
                    and detail.get("ingested_within_cutoff") is True
                    and detail.get("observed_local_time") == "09:30:00"
                    and detail.get("timezone") == "Asia/Shanghai"
                    and detail.get("event_time_field") == "trade_time"
                    and detail.get("price_field") == "open"
                    and detail.get("mark_semantics")
                    == "accepted_gold_close_snapshot"
                    and detail.get("execution_mark_roles_separate") is True
                )
                legacy_semantics = bool(
                    detail.get("open_source_id") in {None, "diemeng"}
                    and detail.get("event_semantics")
                    == "realtime_server_timed_open_09_30"
                    and detail.get("server_available_at_verified") is True
                    and detail.get("available_at_field") == "trade_time"
                )
                tushare_semantics = bool(
                    detail.get("open_source_id") == "tushare"
                    and detail.get("open_source_role")
                    == "tushare_open_observation"
                    and detail.get("provider_endpoint")
                    in {"rt_min", "rt_min_daily"}
                    and detail.get("event_semantics")
                    == "official_realtime_current_session_1min_open_09_30"
                    and detail.get("server_available_at_verified") is False
                    and detail.get("provider_event_time_verified") is True
                    and detail.get("collector_received_at_verified") is True
                    and detail.get(
                        "collector_clock_verified_against_postgresql"
                    )
                    is True
                    and detail.get("available_at_field")
                    == "collector_ingested_at"
                    and detail.get("observation_deadline_local_time")
                    == "09:35:00"
                    and detail.get("open_price_reconciled_with_closed_daily")
                    is True
                    and (
                        detail.get("complete_observed_universe") is True
                        or (
                            detail.get("complete_tradable_universe_observed")
                            is True
                            and detail.get(
                                "missing_open_explained_by_suspension"
                            )
                            is True
                            and detail.get(
                                "complete_execution_universe_accounted"
                            )
                            is True
                        )
                    )
                )
                configured_source = str(execution.get("source") or "")
                configured_endpoint = str(execution.get("endpoint") or "")
                configured_binding_matches = bool(
                    not detail.get("open_source_id")
                    or (
                        configured_source == str(detail.get("open_source_id"))
                        and (
                            not detail.get("provider_endpoint")
                            or configured_endpoint
                            == str(detail.get("provider_endpoint"))
                        )
                    )
                )
                if not (
                    common_semantics
                    and (legacy_semantics or tushare_semantics)
                    and configured_binding_matches
                ):
                    blockers.append("formal_execution_semantics_unverified")
                chain_blockers, chain_evidence = self._typed_execution_authority_chain(
                    detail
                )
                blockers.extend(chain_blockers)
                evidence["authority_chain"] = chain_evidence
        except Exception as exc:
            blockers.append("formal_execution_capability_query_failed")
            evidence["query_error_type"] = type(exc).__name__
        return self._check("formal_execution_capability", blockers, **evidence)

    def _dagster_materialization_check(self) -> ReadinessCheck:
        blockers: list[str] = []
        linked_partition_runs = tuple(
            sorted(
                {
                    str(item.run_id)
                    for item in self.ledger.list_partitions(
                        statuses=(PartitionStatus.SUCCEEDED,), limit=100_000
                    )
                    if str(item.run_id or "").strip()
                }
            )
        )
        direct_raw_ids: set[str] = set()
        bridge_raw_ids: dict[str, tuple[str, ...]] = {}
        for parent_run_id in linked_partition_runs:
            if parent_run_id.startswith(("roscal_", "rosop_")):
                raw_ids = self._trusted_dagster_bridge_raw_ids(parent_run_id)
                if raw_ids:
                    bridge_raw_ids[parent_run_id] = raw_ids
            else:
                # Compatibility with partitions written before ros_runs bridge
                # parents existed: the FK value was the raw Dagster run id.
                direct_raw_ids.add(parent_run_id)
        eligible_raw_ids = direct_raw_ids | {
            raw_id for values in bridge_raw_ids.values() for raw_id in values
        }
        evidence: dict[str, Any] = {
            "linked_partition_run_ids": list(linked_partition_runs[:100]),
            "matched_raw_dagster_run_ids": [],
            "bridge_parent_run_ids": [],
        }
        try:
            inspector = inspect(self.ledger.engine)
            schema = (
                "dagster"
                if self.ledger.engine.dialect.name == "postgresql"
                and inspector.has_table("event_logs", schema="dagster")
                else None
            )
            if not inspector.has_table("event_logs", schema=schema) or not inspector.has_table(
                "runs", schema=schema
            ):
                blockers.append("dagster_materialization_ledger_missing")
            else:
                metadata = MetaData()
                events = Table(
                    "event_logs", metadata, schema=schema, autoload_with=self.ledger.engine
                )
                runs = Table(
                    "runs", metadata, schema=schema, autoload_with=self.ledger.engine
                )
                required_event_columns = {"run_id", "dagster_event_type"}
                required_run_columns = {"run_id", "status"}
                if not required_event_columns.issubset(
                    set(events.c.keys())
                ) or not required_run_columns.issubset(set(runs.c.keys())):
                    blockers.append("dagster_materialization_schema_invalid")
                else:
                    candidates: tuple[str, ...] = ()
                    if eligible_raw_ids:
                        statement = (
                            select(events.c.run_id)
                            .select_from(
                                events.join(runs, events.c.run_id == runs.c.run_id)
                            )
                            .where(
                                events.c.dagster_event_type
                                == "ASSET_MATERIALIZATION",
                                runs.c.status == "SUCCESS",
                                events.c.run_id.in_(tuple(sorted(eligible_raw_ids))),
                            )
                        )
                        with self.ledger.engine.connect() as connection:
                            candidates = tuple(
                                sorted(
                                    {
                                        str(row[0])
                                        for row in connection.execute(statement)
                                    }
                                )
                            )
                    if not candidates:
                        blockers.append("real_dagster_materialization_missing")
                    else:
                        matched = set(candidates)
                        matched_parents = tuple(
                            sorted(
                                parent_run_id
                                for parent_run_id, raw_ids in bridge_raw_ids.items()
                                if matched.intersection(raw_ids)
                            )
                        )
                        evidence["matched_raw_dagster_run_ids"] = list(
                            candidates[:100]
                        )
                        evidence["bridge_parent_run_ids"] = list(
                            matched_parents[:100]
                        )
                        # Preserve the pre-bridge evidence field for existing
                        # read models while making its raw-id semantics explicit.
                        evidence["dagster_run_ids"] = list(candidates[:100])
        except Exception as exc:
            blockers.append("dagster_materialization_query_failed")
            evidence["query_error_type"] = type(exc).__name__
        return self._check("dagster_materialization", blockers, **evidence)

    def _trusted_dagster_bridge_raw_ids(
        self, parent_run_id: str
    ) -> tuple[str, ...]:
        """Resolve raw Dagster ids only from a terminal, typed bridge parent."""

        parent = self.catalog.get_run(parent_run_id)
        if parent is None or parent.completed_at is None or parent.error is not None:
            return ()
        metadata = parent.metadata
        if _ROSCAL_BRIDGE_RUN_ID.fullmatch(parent_run_id):
            if (
                parent_run_id != f"roscal_{parent.input_fingerprint[:48]}"
                or parent.run_type != "dagster_calendar_bootstrap"
                or parent.status != "succeeded"
            ):
                return ()
            if "dagster_run_ids" in metadata:
                values = metadata.get("dagster_run_ids")
                if not isinstance(values, (list, tuple)) or not values:
                    return ()
                if any(not isinstance(value, str) or not value.strip() for value in values):
                    return ()
                normalized = tuple(value.strip() for value in values)
                if normalized != tuple(sorted(set(normalized))):
                    return ()
                latest = metadata.get("dagster_run_id")
                if latest is not None and (
                    not isinstance(latest, str) or latest.strip() not in normalized
                ):
                    return ()
                return normalized
            # Early calendar bridge rows stored only the current retry id.
            legacy = metadata.get("dagster_run_id")
            if isinstance(legacy, str) and legacy.strip():
                return (legacy.strip(),)
            return ()
        if _ROSOP_BRIDGE_RUN_ID.fullmatch(parent_run_id):
            if (
                parent_run_id != f"rosop_{parent.input_fingerprint[:48]}"
                or parent.run_type not in _TRUSTED_ROSOP_RUN_TYPES
                or parent.status not in {"completed", "skipped"}
            ):
                return ()
            raw_id = metadata.get("dagster_run_id")
            if isinstance(raw_id, str) and raw_id.strip():
                return (raw_id.strip(),)
        return ()

    def _dagster_heartbeat_matches(
        self, *, daemon_type: str, completed_at: datetime, maximum_gap_seconds: float
    ) -> bool:
        inspector = inspect(self.ledger.engine)
        schema = (
            "dagster"
            if self.ledger.engine.dialect.name == "postgresql"
            and inspector.has_table("daemon_heartbeats", schema="dagster")
            else None
        )
        if not inspector.has_table("daemon_heartbeats", schema=schema):
            return False
        metadata = MetaData()
        heartbeats = Table(
            "daemon_heartbeats",
            metadata,
            schema=schema,
            autoload_with=self.ledger.engine,
        )
        if not {"daemon_type", "timestamp"}.issubset(set(heartbeats.c.keys())):
            return False
        statement = select(heartbeats.c.timestamp).where(
            heartbeats.c.daemon_type == daemon_type
        )
        with self.ledger.engine.connect() as connection:
            values = tuple(row[0] for row in connection.execute(statement))
        for value in values:
            try:
                heartbeat = _parse_dagster_heartbeat_time(value)
            except (TypeError, ValueError, OSError, OverflowError):
                continue
            if abs((heartbeat - completed_at).total_seconds()) <= maximum_gap_seconds:
                return True
        return False

    def _dagster_code_location_soak_check(
        self, *, oci_check: ReadinessCheck | None = None
    ) -> ReadinessCheck:
        sample_runs = self.catalog.list_runs(
            limit=1_000,
            run_type=DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_RUN_TYPE,
        )
        newest_sample_time = max(
            (
                item.completed_at or item.started_at
                for item in sample_runs
            ),
            default=None,
        )
        valid_samples: list[tuple[datetime, str, str, RunRecord]] = []
        invalid_sample_count = 0
        provenance = self.config_evidence.provenance
        oci_evidence = (
            dict(oci_check.evidence)
            if oci_check is not None and oci_check.passed
            else {}
        )
        expected_build = (
            oci_evidence.get("build_identity_hash")
            if self.ledger.engine.dialect.name == "postgresql"
            else getattr(provenance, "build_identity_hash", None)
        )
        expected_image = (
            oci_evidence.get("oci_image_id")
            if self.ledger.engine.dialect.name == "postgresql"
            else getattr(provenance, "oci_image_id", None)
        )
        expected_deployment = oci_evidence.get("deployment_identity_hash")
        expected_container = oci_evidence.get("container_id")
        expected_compose = oci_evidence.get("compose_config_hash")
        observed_now = self.catalog.database_now().astimezone(timezone.utc)
        for sample in sample_runs:
            metadata = sample.metadata
            expected = dagster_code_location_health_sample_evidence_hash(metadata)
            try:
                sampled_at = _parse_time(metadata.get("sampled_at"))
                persisted_heartbeat = _parse_time(
                    metadata.get("dagster_heartbeat_at")
                )
            except (TypeError, ValueError):
                invalid_sample_count += 1
                continue
            process_identity = str(metadata.get("process_identity") or "").strip()
            maximum_gap = float(
                metadata.get("maximum_heartbeat_gap_seconds") or 1e12
            )
            if not (
                sample.status == "succeeded"
                and sample.run_id == f"dagster_health_sample_{expected}"
                and sample.started_at == sampled_at
                and sample.completed_at == sampled_at
                and metadata.get("schema_version")
                == DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_SCHEMA_VERSION
                and metadata.get("authority")
                == "dagster_sensor_grpc_roundtrip_plus_pg_heartbeat"
                and metadata.get("physical") is True
                and metadata.get("healthy") is True
                and metadata.get("service_name") == _DAGSTER_SERVICE_NAME
                and metadata.get("code_location") == _DAGSTER_CODE_LOCATION
                and metadata.get("heartbeat_source") == "dagster_postgresql"
                and str(metadata.get("daemon_type") or "")
                in {"SENSOR", "SCHEDULER", "QUEUED_RUN_COORDINATOR"}
                and process_identity
                and maximum_gap <= 600
                and abs((persisted_heartbeat - sampled_at).total_seconds())
                <= maximum_gap
                and metadata.get("build_identity_hash") == expected_build
                and metadata.get("oci_image_id") == expected_image
                and (
                    self.ledger.engine.dialect.name != "postgresql"
                    or (
                        metadata.get("deployment_identity_hash")
                        == expected_deployment
                        and metadata.get("container_id") == expected_container
                        and metadata.get("compose_config_hash") == expected_compose
                        and _valid_hash(metadata.get("host_attestation_hash"))
                    )
                )
                and metadata.get("sample_evidence_hash") == expected
                and sample.input_fingerprint == expected
            ):
                invalid_sample_count += 1
                continue
            valid_samples.append((sampled_at, process_identity, expected, sample))

        runs = self.catalog.list_runs(
            limit=1_000, run_type=DAGSTER_CODE_LOCATION_SOAK_RUN_TYPE
        )
        for run in runs:
            metadata = run.metadata
            expected = dagster_code_location_soak_evidence_hash(metadata)
            if run.completed_at is None:
                continue
            window = sorted(
                (
                    sampled_at,
                    process_identity,
                    evidence_hash,
                    sample,
                )
                for sampled_at, process_identity, evidence_hash, sample in valid_samples
                if run.started_at <= sampled_at <= run.completed_at
            )
            sample_times = tuple(item[0] for item in window)
            process_identities = tuple(item[1] for item in window)
            sample_hashes = tuple(item[2] for item in window)
            sample_run_ids = tuple(item[3].run_id for item in window)
            series_hash = (
                dagster_code_location_health_series_hash(sample_hashes)
                if sample_hashes
                else None
            )
            observed_span = (
                sample_times[-1] - sample_times[0]
                if len(sample_times) >= 2
                else timedelta(0)
            )
            observed_gaps = tuple(
                (later - earlier).total_seconds()
                for earlier, later in zip(sample_times, sample_times[1:])
            )
            maximum_gap = max(observed_gaps, default=float("inf"))
            restart_count = sum(
                current != previous
                for previous, current in zip(
                    process_identities, process_identities[1:]
                )
            )
            declared_maximum_gap = float(
                metadata.get("maximum_sample_gap_seconds") or 1e12
            )
            heartbeat_matches = (
                self._dagster_heartbeat_matches(
                    daemon_type=str(metadata.get("daemon_type") or ""),
                    completed_at=run.completed_at,
                    maximum_gap_seconds=min(declared_maximum_gap, 600),
                )
            )
            if not (
                run.status == "succeeded"
                and sample_times
                and sample_times[0] == run.started_at
                and sample_times[-1] == run.completed_at
                and observed_span >= _SOAK_MINIMUM
                and metadata.get("schema_version")
                == DAGSTER_CODE_LOCATION_SOAK_SCHEMA_VERSION
                and metadata.get("authority")
                == "derived_from_persisted_health_samples"
                and metadata.get("physical") is True
                and metadata.get("service_name") == _DAGSTER_SERVICE_NAME
                and metadata.get("code_location") == _DAGSTER_CODE_LOCATION
                and metadata.get("heartbeat_source") == "dagster_postgresql"
                and str(metadata.get("daemon_type") or "")
                in {"SENSOR", "SCHEDULER", "QUEUED_RUN_COORDINATOR"}
                and len(sample_times) >= 145
                and int(metadata.get("health_sample_count") or 0)
                == len(sample_times)
                and maximum_gap <= 600
                and abs(declared_maximum_gap - maximum_gap) <= 1e-6
                and restart_count == 0
                and len(set(process_identities)) == 1
                and metadata.get("process_identity") == process_identities[0]
                and int(metadata.get("restart_count", -1)) == restart_count
                and metadata.get("health_sample_hash") == series_hash
                and tuple(map(str, metadata.get("health_sample_run_ids") or ()))
                == sample_run_ids
                and metadata.get("build_identity_hash") == expected_build
                and metadata.get("oci_image_id") == expected_image
                and (
                    self.ledger.engine.dialect.name != "postgresql"
                    or (
                        metadata.get("deployment_identity_hash")
                        == expected_deployment
                        and metadata.get("container_id") == expected_container
                        and metadata.get("compose_config_hash") == expected_compose
                    )
                )
                and metadata.get("soak_evidence_hash") == expected
                and run.input_fingerprint == expected
                and heartbeat_matches
                and sample_times[-1] == newest_sample_time
                and (
                    self.ledger.engine.dialect.name != "postgresql"
                    or (
                        0
                        <= (observed_now - sample_times[-1]).total_seconds()
                        <= _HEALTH_SAMPLE_MAX_AGE.total_seconds()
                        and self._dagster_heartbeat_matches(
                            daemon_type=str(metadata.get("daemon_type") or ""),
                            completed_at=observed_now,
                            maximum_gap_seconds=600,
                        )
                    )
                )
            ):
                continue
            return self._check(
                "dagster_code_location_24h_soak",
                (),
                run_id=run.run_id,
                soak_evidence_hash=expected,
                duration_seconds=observed_span.total_seconds(),
                health_sample_count=len(sample_times),
                maximum_sample_gap_seconds=maximum_gap,
                process_identity=process_identities[0],
            )
        return self._check(
            "dagster_code_location_24h_soak",
            ("dagster_code_location_24h_soak_missing",),
            inspected_runs=len(runs),
            inspected_health_samples=len(sample_runs),
            invalid_health_samples=invalid_sample_count,
        )

    def _credential_rotation_check(self) -> ReadinessCheck:
        blockers = tuple(map(str, self.config_evidence.credential_rotation_blockers))
        if not blockers and not self.config_evidence.historical_backfill_allowed:
            blockers = ("credential_rotation_not_verified",)
        return self._check(
            "credential_rotation",
            blockers,
            historical_backfill_allowed=self.config_evidence.historical_backfill_allowed,
        )

    @staticmethod
    def _status(checks: Sequence[ReadinessCheck]) -> ProductionReadinessStatus:
        by_code = {item.code: item.passed for item in checks}
        canary_ready = all(
            by_code.get(item, False)
            for item in (
                "alembic_head",
                "production_configuration",
                "minio_restore_drill",
                "physical_engineering_canary",
            )
        )
        backfill_complete = canary_ready and all(
            by_code.get(item, False)
            for item in (
                "accepted_calendar_range",
                "required_dataset_stage_matrix",
                "accepted_gold_parent_closure",
            )
        )
        if all(item.passed for item in checks):
            return ProductionReadinessStatus.FORMAL_EPOCH_READY
        if backfill_complete:
            return ProductionReadinessStatus.BACKFILL_COMPLETE
        if canary_ready:
            return ProductionReadinessStatus.CANARY_READY
        return ProductionReadinessStatus.CONFIG_VALID_CANARY_PENDING

    def audit(self) -> ProductionReadinessAudit:
        """Append an assessment derived only from persisted facts."""

        observed_now = self.catalog.database_now().astimezone(timezone.utc)
        # Preserve the database clock's full precision.  Truncating to a minute
        # made two state transitions in the same minute indistinguishable and
        # allowed ``latest`` to pick by content hash instead of invocation time.
        audited_at = observed_now
        try:
            source_start, required_datasets = self._bootstrap_config()
            config_shape_error: str | None = None
        except Exception as exc:
            source_start = date(2016, 6, 1)
            required_datasets = ()
            config_shape_error = type(exc).__name__
        schema_check = self._schema_head_check()
        config_check = self._configuration_check()
        if config_shape_error is not None:
            config_check = self._check(
                "production_configuration",
                (*config_check.blockers, "production_data_contract_missing"),
                **dict(config_check.evidence),
                shape_error_type=config_shape_error,
            )
        calendar_check, sessions = self._calendar_check(
            now=observed_now, source_start=source_start
        )
        matrix_check, gold_check = self._matrix_and_gold_checks(
            sessions=sessions,
            required_datasets=required_datasets,
            now=observed_now,
        )
        restore_check, restore_run = self._restore_check(now=observed_now)
        oci_check = self._oci_provenance_check()
        canary_check = self._physical_canary_check(
            now=observed_now,
            restore_run=restore_run,
            oci_check=oci_check,
            accepted_sessions=sessions,
        )
        checks = (
            schema_check,
            config_check,
            calendar_check,
            matrix_check,
            gold_check,
            canary_check,
            restore_check,
            oci_check,
            self._formal_execution_check(),
            self._dagster_materialization_check(),
            self._dagster_code_location_soak_check(oci_check=oci_check),
            self._credential_rotation_check(),
        )
        blockers = tuple(sorted({item for check in checks for item in check.blockers}))
        status = self._status(checks)
        content = {
            "schema_version": READINESS_AUDIT_SCHEMA_VERSION,
            "status": status.value,
            "audited_at": audited_at.isoformat(),
            "blockers": list(blockers),
            "checks": [item.to_dict() for item in checks],
            "accepted_session_count": len(sessions),
            "source_start": source_start.isoformat(),
            "latest_session": sessions[-1] if sessions else None,
        }
        fingerprint = content_fingerprint(
            content, domain=READINESS_AUDIT_SCHEMA_VERSION
        )
        audit = ProductionReadinessAudit(
            audit_id=f"readiness_{fingerprint}",
            fingerprint=fingerprint,
            status=status,
            audited_at=audited_at,
            blockers=blockers,
            checks=checks,
            accepted_session_count=len(sessions),
            source_start=source_start.isoformat(),
            latest_session=(sessions[-1] if sessions else None),
        )
        run = RunRecord(
            run_id=audit.audit_id,
            run_type=READINESS_AUDIT_RUN_TYPE,
            status="completed",
            input_fingerprint=audit.fingerprint,
            started_at=audited_at,
            completed_at=audited_at,
            metadata={
                "schema_version": READINESS_AUDIT_SCHEMA_VERSION,
                "authority": "postgresql_derived_no_caller_assertions",
                "audit": audit.to_dict(),
            },
        )
        stored, won = self.catalog.claim_run(run)
        if not won:
            if (
                stored.run_type != run.run_type
                or stored.status != run.status
                or stored.input_fingerprint != run.input_fingerprint
                or canonical_json(stored.metadata) != canonical_json(run.metadata)
            ):
                raise ReadinessAuditError("readiness audit identity collision")
            return ProductionReadinessAudit.from_run(stored)
        return audit

    def latest(self) -> ProductionReadinessAudit | None:
        """Return the newest immutable audit after revalidating its hash."""

        runs = self.catalog.list_runs(limit=1_000, run_type=READINESS_AUDIT_RUN_TYPE)
        if not runs:
            return None
        audits = tuple(ProductionReadinessAudit.from_run(run) for run in runs)
        newest_at = max(item.audited_at for item in audits)
        newest = tuple(item for item in audits if item.audited_at == newest_at)
        if len(newest) != 1:
            raise ReadinessAuditError(
                "latest readiness audit is ambiguous at the database timestamp"
            )
        return newest[0]


__all__ = [
    "DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_RUN_TYPE",
    "DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_SCHEMA_VERSION",
    "DAGSTER_CODE_LOCATION_SOAK_RUN_TYPE",
    "DAGSTER_CODE_LOCATION_SOAK_SCHEMA_VERSION",
    "FORMAL_EXECUTION_CAPABILITY_SCHEMA_VERSION",
    "PHYSICAL_CANARY_RUN_TYPE",
    "PHYSICAL_CANARY_SCHEMA_VERSION",
    "ProductionReadinessAudit",
    "ProductionReadinessAuditor",
    "ProductionReadinessStatus",
    "READINESS_AUDIT_RUN_TYPE",
    "READINESS_AUDIT_SCHEMA_VERSION",
    "RESTORE_DRILL_RUN_TYPE",
    "RESTORE_DRILL_SCHEMA_VERSION",
    "ReadinessAuditError",
    "ReadinessCheck",
    "dagster_code_location_health_sample_evidence_hash",
    "dagster_code_location_health_series_hash",
    "dagster_code_location_soak_evidence_hash",
    "formal_execution_probe_hash",
    "physical_canary_evidence_hash",
    "restore_drill_evidence_hash",
]
