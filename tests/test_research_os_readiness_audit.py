from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("sqlalchemy")
from sqlalchemy import create_engine, event, text

from factor_lab.research_os.catalog import (
    LifecycleEvent,
    RESEARCH_OS_ALEMBIC_HEAD,
    ResearchCatalog,
    RunRecord,
)
from factor_lab.research_os.contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    LifecycleState,
    SnapshotTier,
)
from factor_lab.research_os.fingerprint import content_fingerprint
from factor_lab.research_os.execution_snapshot_authority import (
    BUNDLE_ROLE as TYPED_EXECUTION_BUNDLE_ROLE,
    CAPABILITY_DATASET as TYPED_EXECUTION_CAPABILITY_DATASET,
    FORMAL_EXECUTION_REQUIRED_FIELDS,
    FORMAL_EXECUTION_SOURCE_ID,
    OPEN_DATASET as TYPED_OPEN_DATASET,
    OUTPUT_CONTRACT_HASH as TYPED_EXECUTION_CONTRACT_HASH,
    OUTPUT_DATASET as TYPED_EXECUTION_OUTPUT_DATASET,
    production_execution_configuration_hash,
    production_formal_source_contract_hash,
)
from factor_lab.research_os.execution_open_sources import (
    diemeng_engineering_canary_execution_mapping,
    diemeng_engineering_canary_opening_contract_hash,
    engineering_canary_execution_contract_hash,
)
from factor_lab.research_os.docker_attestation import (
    ATTEMPT_AUTHORITY as HOST_DOCKER_ATTEMPT_AUTHORITY,
    ATTEMPT_RUN_TYPE as HOST_DOCKER_ATTEMPT_RUN_TYPE,
    ATTEMPT_SCHEMA_VERSION as HOST_DOCKER_ATTEMPT_SCHEMA_VERSION,
    RUN_TYPE as HOST_DOCKER_RUN_TYPE,
    host_docker_attempt_fingerprint,
)
from factor_lab.research_os.orm import Base
from factor_lab.research_os.object_store import S3ImmutableArchive
from factor_lab.research_os.physical_canary import CANARY_OBJECT_PREFIX
from factor_lab.research_os.production_config import ProductionConfigEvidence
from factor_lab.research_os.production_ledger import (
    CapabilityRecord,
    CapabilityStatus,
    IncidentStage,
    IncidentStatus,
    PartitionIdentity,
    PartitionStatus,
    ProductionLedger,
)
from factor_lab.research_os.readiness_audit import (
    DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_RUN_TYPE,
    DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_SCHEMA_VERSION,
    DAGSTER_CODE_LOCATION_SOAK_RUN_TYPE,
    DAGSTER_CODE_LOCATION_SOAK_SCHEMA_VERSION,
    FORMAL_EXECUTION_CAPABILITY_SCHEMA_VERSION,
    PHYSICAL_CANARY_RUN_TYPE,
    PHYSICAL_CANARY_SCHEMA_VERSION,
    RESTORE_DRILL_ATTEMPT_AUTHORITY,
    RESTORE_DRILL_ATTEMPT_SCHEMA_VERSION,
    RESTORE_DRILL_RUN_TYPE,
    RESTORE_DRILL_SCHEMA_VERSION,
    ProductionReadinessAuditor,
    ProductionReadinessStatus,
    ReadinessAuditError,
    dagster_code_location_health_sample_evidence_hash,
    dagster_code_location_health_series_hash,
    dagster_code_location_soak_evidence_hash,
    formal_execution_probe_hash,
    physical_canary_evidence_hash,
    restore_drill_attempt_fingerprint,
    restore_drill_evidence_hash,
)
from factor_lab.research_os.restore_drill import PhysicalMinioRestoreDrillService
from factor_lab.research_os.shadow_authority import (
    ShadowEvidenceAuthority,
    ShadowRole,
)
from factor_lab.research_os.snapshots import SNAPSHOT_SCHEMA_VERSION


NOW = datetime(2026, 8, 22, 12, tzinfo=timezone.utc)
CANARY_SESSIONS = tuple(
    (date(2026, 7, 24) + timedelta(days=index)).isoformat()
    for index in range(29)
    if (date(2026, 7, 24) + timedelta(days=index)).weekday() < 5
)
assert len(CANARY_SESSIONS) == 21 and CANARY_SESSIONS[-1] == "2026-08-21"
SESSIONS = ("2016-06-01", *CANARY_SESSIONS)
REQUIRED_DATASETS = (
    "daily",
    "daily_basic",
    "adj_factor",
    "trade_calendar",
    "stock_basic_l",
    "stock_basic_p",
    "stock_basic_d",
    "stock_basic_g",
    "historical_st",
    "industry_classification",
    "suspension_status",
    "stock_limit",
    "company_action",
)
ENGINEERING_CANARY_CONFIG = {
    "evidence_scope": "retrospective_non_forward",
    "execution_market_data": diemeng_engineering_canary_execution_mapping(),
}
CANARY_EXECUTION_CONTRACT_HASH = engineering_canary_execution_contract_hash(
    ENGINEERING_CANARY_CONFIG
)
CANARY_OPENING_CONTRACT_HASH = diemeng_engineering_canary_opening_contract_hash(
    ENGINEERING_CANARY_CONFIG
)


class FrozenCatalog:
    def __init__(self, catalog: ResearchCatalog, now: datetime = NOW) -> None:
        self._catalog = catalog
        self._now = now

    def database_now(self) -> datetime:
        return self._now

    def __getattr__(self, name):
        return getattr(self._catalog, name)


def _config() -> dict:
    execution = diemeng_engineering_canary_execution_mapping()
    execution["collection_mode"] = "realtime_open"
    # This declaration is not capability evidence, but it is part of the
    # exact production document to which a physical capability must bind.
    execution["formal_capability"] = {
        "status": "accepted",
        "formal_shadow_projection": "allowed",
    }
    return {
        "daily": {
            "bootstrap": {"source_start": "2016-06-01"},
            "engineering_canary": ENGINEERING_CANARY_CONFIG,
            "gold": {
                "research_panel": {
                    "required_datasets": list(REQUIRED_DATASETS),
                }
            },
            "shadow": {
                "execution_market_data": execution,
            },
        }
    }


def _tushare_formal_execution_mapping() -> dict:
    return {
        "source": "tushare",
        "profile_name": "primary-tushare",
        "credential_ref": "secret://tushare_token",
        "dataset": "rt_min",
        "endpoint": "rt_min",
        "method": "SDK",
        "rate_limits": {
            "__account__": {
                "requests": 60,
                "per_seconds": 60,
                "burst": 1,
            }
        },
        "request": {
            "ts_code": "${decision_universe_csv}",
            "freq": "1MIN",
        },
        "batching": {
            "mode": "sorted_deterministic_chunks",
            "maximum_symbols_per_request": 300,
        },
        "contract": {
            "key_fields": ["ts_code", "time"],
            "event_time_field": "time",
            "fields": [
                "ts_code",
                "time",
                "open",
                "close",
                "high",
                "low",
                "vol",
                "amount",
            ],
        },
        "availability": {
            "mode": "collector_ingested_at",
            "event_time_field": "time",
            "available_at_field": "ingested_at",
            "maximum_delay_minutes": 5,
        },
        "formal_capability": {
            "status": "runtime_probe_required",
            "formal_shadow_projection": "runtime_probe_gated",
        },
        "end_of_day_mark": {
            "source": "accepted_gold_close_snapshot",
        },
    }


def _configure_tushare_formal_execution(config: dict) -> None:
    config["daily"]["sources"] = [
        {
            "source": "tushare",
            "rate_limits": {
                "__account__": {
                    "requests": 60,
                    "per_seconds": 60,
                    "burst": 1,
                }
            },
        }
    ]
    config["daily"]["shadow"][
        "execution_market_data"
    ] = _tushare_formal_execution_mapping()
    config["security"] = {
        "source_transport": {
            "tushare": {
                "api_origin": "https://api.waditu.com/dataapi",
            }
        }
    }


def _evidence(*, rotation_blocked: bool = False) -> ProductionConfigEvidence:
    public_provenance = {
        "architecture_version": "research-os/v1",
        "code_hash": "1" * 64,
        "configuration_hash": "2" * 64,
        "dependency_lock_hash": "3" * 64,
        "dirty_patch_hash": "4" * 64,
        "provenance_kind": "daemon_inspected_oci_image",
        "build_identity_hash": "8" * 64,
        "git_commit": None,
        "image_source_digest": "5" * 64,
        "oci_image_id": "sha256:" + "9" * 64,
        "oci_repo_digests": ["factor-lab@sha256:" + "7" * 64],
        "oci_base_digests": ["python@sha256:" + "6" * 64],
        "formal_epoch_eligible": True,
    }
    provenance = SimpleNamespace(
        formal_epoch_eligible=True,
        architecture_version="research-os/v1",
        code_hash="1" * 64,
        configuration_hash="2" * 64,
        dependency_lock_hash="3" * 64,
        dirty_patch_hash="4" * 64,
        build_identity_hash="8" * 64,
        oci_image_id="sha256:" + "9" * 64,
        oci_repo_digests=("factor-lab@sha256:" + "7" * 64,),
        oci_base_digests=("python@sha256:" + "6" * 64,),
        public_dict=lambda: dict(public_provenance),
    )
    rotation = (
        ("tushare_token_post_exposure_rotation_pending",)
        if rotation_blocked
        else ()
    )
    return ProductionConfigEvidence(
        path=Path("production.json"),
        runtime_data_root=Path("/runtime/data"),
        runtime_artifact_root=Path("/runtime/artifacts"),
        credential_refs=("secret://tushare_token",),
        provenance=provenance,
        formal_execution_capable=True,
        historical_backfill_allowed=not rotation_blocked,
        formal_forward_evidence=False,
        readiness_blockers=(
            *rotation,
            "persisted_production_readiness_audit_missing",
        ),
        credential_rotation_blockers=rotation,
        engineering_canary_execution_contract_hash=(
            CANARY_EXECUTION_CONTRACT_HASH
        ),
    )


def _snapshot(
    tier: SnapshotTier,
    *,
    parents=(),
    as_of: datetime = NOW - timedelta(hours=1),
) -> DataSnapshotRef:
    payload = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "tier": tier.value,
        "as_of": as_of.isoformat(),
        "parent_snapshot_ids": sorted(map(str, parents)),
        "environment_hashes": {
            "config_hash": "1" * 64,
            "code_hash": "2" * 64,
            "dirty_patch_hash": "3" * 64,
            "dependency_lock_hash": "4" * 64,
        },
        "quality_status": "pass",
        "trust_labels": ["point_in_time", "quality_accepted"],
        "files": [
            {
                "path": f"{tier.value}.parquet",
                "size_bytes": 128,
                "sha256": "5" * 64,
            }
        ],
    }
    snapshot_id = hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    manifest = {**payload, "snapshot_id": snapshot_id}
    uri = (
        f"iceberg://factorlab/factor_lab.gold#ros_{snapshot_id}"
        if tier is SnapshotTier.GOLD
        else f"s3://factor-lab/manifests/{tier.value}/{snapshot_id}"
    )
    return DataSnapshotRef(
        snapshot_id=snapshot_id,
        tier=tier,
        uri=uri,
        content_hash=snapshot_id,
        parent_snapshot_ids=tuple(payload["parent_snapshot_ids"]),
        as_of=as_of,
        quality_status=DataQualityStatus.ACCEPTED,
        trust_labels=tuple(payload["trust_labels"]),
        manifest=manifest,
    )


def _physical_canary_snapshot(
    tier: SnapshotTier,
    *,
    run_id: str,
    role: str,
    trade_date: str,
    parents=(),
    manifest_variant: str | None = None,
) -> tuple[DataSnapshotRef, dict]:
    object_sha = content_fingerprint(
        {
            "run_id": run_id,
            "tier": tier.value,
            "role": role,
            "trade_date": trade_date,
        },
        domain="test/physical-canary/object",
    )
    uri = (
        "s3://factor-lab/research-os/engineering-canary/physical/v1/"
        f"{run_id}/{tier.value}/sha256={object_sha}/{role}.parquet"
    )
    physical_object = {
        "uri": uri,
        "key": uri.removeprefix("s3://factor-lab/"),
        "sha256": object_sha,
        "size_bytes": 512,
        "reused": False,
    }
    opening_cross_check = (
        {
            "comparison_semantics": (
                "daily_session_open_vs_distinct_09_30_minute_bar_open"
            ),
            # A distinct 09:30 minute-bar open is not required to equal the
            # daily-session open within one tick.  The difference is audited,
            # while range violations remain fail-closed.
            "one_tick_mismatch_count": 3,
            "maximum_absolute_difference": 0.5,
            "daily_range_violation_count": 0,
            "minute_bar_range_violation_count": 0,
        }
        if tier is SnapshotTier.GOLD and role == "execution"
        else None
    )
    columns = ["ticker", "open", "close"]
    if tier is SnapshotTier.GOLD and role == "execution":
        columns.extend(
            [
                "daily_session_open_raw",
                "execution_minute_open_raw",
                "execution_minute_low_raw",
                "execution_minute_high_raw",
                "execution_vs_daily_open_abs_diff",
                "execution_vs_daily_open_one_tick_match",
            ]
        )
    snapshot_as_of = datetime.combine(
        date.fromisoformat(trade_date),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    manifest = {
        "evidence_schema": PHYSICAL_CANARY_SCHEMA_VERSION,
        "evidence_class": "engineering_canary",
        "evidence_scope": "retrospective_non_forward",
        "canary_execution_contract_hash": CANARY_EXECUTION_CONTRACT_HASH,
        "formal_epoch_eligible": False,
        "physical_source_attested": True,
        "controlled_test_adapter": False,
        "readiness_admission": "physical_engineering_prerequisite",
        "run_id": run_id,
        "tier": tier.value,
        "role": role,
        "trade_date": trade_date,
        "rows": 50,
        "columns": columns,
        "frame_digest": "d" * 64,
        "physical_object": physical_object,
        "parent_snapshot_ids": list(map(str, parents)),
        "published_at": (NOW - timedelta(hours=2)).isoformat(),
        "snapshot_reference_schema": (
            "research-os/physical-canary-snapshot-reference/v1"
        ),
        "snapshot_as_of": snapshot_as_of.isoformat(),
    }
    if tier is SnapshotTier.GOLD:
        manifest.update(
            {
                "opening_execution_formal_ready": False,
                "opening_cross_check": opening_cross_check,
            }
        )
    elif tier is SnapshotTier.SILVER:
        manifest["quality_report"] = {
            "schema_version": PHYSICAL_CANARY_SCHEMA_VERSION,
            "status": "accepted",
            "trade_date": trade_date,
            "historical_st_rows": 1,
            "datasets": [
                "trade_calendar",
                "daily",
                "adj_factor",
                "historical_st",
                "stock_limit",
                "opening_execution",
            ],
            "reconciliation": {
                "schema_version": "research-os/data-reconciliation/v1",
                "status": "pass",
                "disputed_group_count": 0,
                "quarantined_row_count": 0,
            },
        }
    if manifest_variant == "missing_execution_audit_column":
        manifest["columns"].remove("execution_minute_low_raw")
    elif manifest_variant == "invalid_opening_audit":
        manifest["opening_cross_check"] = {
            **manifest["opening_cross_check"],
            "one_tick_mismatch_count": -1,
        }
    elif manifest_variant == "invalid_dq_status":
        manifest["quality_report"] = {
            **manifest["quality_report"],
            "status": "blocked",
        }
    elif manifest_variant == "invalid_dq_reconciliation":
        manifest["quality_report"] = {
            **manifest["quality_report"],
            "reconciliation": {
                **manifest["quality_report"]["reconciliation"],
                "status": "blocked",
                "disputed_group_count": 1,
            },
        }
    content_hash = content_fingerprint(
        manifest,
        domain="factor-lab/research-os/v1/physical-canary-snapshot",
    )
    snapshot_id = f"pec_{tier.value}_{content_hash[:56]}"
    reference = DataSnapshotRef(
        snapshot_id=snapshot_id,
        tier=tier,
        uri=uri,
        content_hash=content_hash,
        parent_snapshot_ids=tuple(map(str, parents)),
        as_of=snapshot_as_of,
        quality_status=DataQualityStatus.ACCEPTED,
        trust_labels=(
            "physical_engineering_canary",
            "retrospective_non_forward",
            "retrospective_physical_replay",
        ),
        manifest=manifest,
    )
    evidence = {
        "snapshot_id": snapshot_id,
        "tier": tier.value,
        "role": role,
        "trade_date": trade_date,
        "uri": uri,
        "content_hash": content_hash,
        "object_sha256": object_sha,
        "size_bytes": 512,
    }
    return reference, evidence


def _operation_result(operation: str, outputs: dict) -> dict:
    return {
        "operation": operation,
        "status": "completed",
        "summary": f"{operation} completed",
        "outputs": outputs,
    }


def _finish_partition(
    ledger: ProductionLedger,
    *,
    dataset: str,
    partition_key: str,
    run_id: str,
    result: dict,
    output_snapshot_id: str | None = None,
    source_id: str = "research_os",
):
    identity = PartitionIdentity(source_id, dataset, partition_key)
    ledger.ensure_partition(
        identity,
        created_at=NOW - timedelta(hours=2),
        input_hash=content_fingerprint(
            {"dataset": dataset, "partition_key": partition_key},
            domain="test/readiness/partition-input",
        ),
    )
    lease = ledger.claim(
        identity=identity,
        owner=f"test-{dataset}-{partition_key}",
        now=NOW - timedelta(hours=2),
        lease_for=timedelta(hours=3),
    )
    assert lease is not None
    return ledger.finish(
        lease,
        status=PartitionStatus.SUCCEEDED,
        completed_at=NOW - timedelta(hours=1),
        run_id=run_id,
        output_snapshot_id=output_snapshot_id,
        output_hash=content_fingerprint(
            result,
            domain="factor-lab/research-os/v1/production-operation-result",
        ),
        details={"operation_result": result},
    )


def _physical_partition_claim_lineage(
    identity: PartitionIdentity,
    *,
    input_hash: str,
    stage: str,
    role: str = "",
) -> tuple[dict, str]:
    incident_stage = {
        "bronze": "source",
        "silver": "silver",
        "data_quality": "data_quality",
        "gold": "gold",
    }[stage]
    lineage = {
        "incident_stage": incident_stage,
        "partition_identity": {
            "source_id": identity.source_id,
            "dataset": identity.dataset,
            "partition_key": identity.partition_key,
            "partition_run_id": identity.partition_run_id,
        },
        "attempted_input_hash": input_hash,
        "parent_evidence_hashes": [],
        "stage_lineage": {
            "stage": stage,
            **({"role": role} if role else {}),
        },
    }
    lineage_hash = content_fingerprint(
        lineage,
        domain="factor-lab/research-os/v1/physical-canary-partition-claim-lineage",
    )
    return lineage, lineage_hash


def _save_run(catalog: ResearchCatalog, run_id: str) -> None:
    catalog.save_run(
        RunRecord(
            run_id=run_id,
            run_type="dagster_physical_run_anchor",
            status="succeeded",
            input_fingerprint=content_fingerprint(
                {"run_id": run_id}, domain="test/readiness/run"
            ),
            started_at=NOW - timedelta(hours=2),
            completed_at=NOW - timedelta(hours=1),
            metadata={"physical": True},
        )
    )


def _save_host_attestation_proof(
    catalog: ResearchCatalog,
    *,
    marker: str,
    started_at: datetime,
    completed_at: datetime,
) -> RunRecord:
    attestation_hash = content_fingerprint(
        {"marker": marker},
        domain="test/host-docker-attestation-proof",
    )
    run = RunRecord(
        run_id=f"docker_attestation_{attestation_hash}",
        run_type=HOST_DOCKER_RUN_TYPE,
        status="succeeded",
        input_fingerprint=attestation_hash,
        started_at=started_at,
        completed_at=completed_at,
        metadata={"attestation_hash": attestation_hash},
    )
    catalog.save_run(run)
    return run


def _save_host_attestation_attempt(
    catalog: ResearchCatalog,
    *,
    marker: str,
    status: str,
    started_at: datetime,
    completed_at: datetime | None,
    attestation: RunRecord | None = None,
) -> RunRecord:
    nonce = content_fingerprint(
        marker,
        domain="test/host-docker-attestation-attempt-nonce",
    )[:32]
    fingerprint = host_docker_attempt_fingerprint(
        started_at=started_at,
        nonce=nonce,
    )
    metadata = {
        "schema_version": HOST_DOCKER_ATTEMPT_SCHEMA_VERSION,
        "authority": HOST_DOCKER_ATTEMPT_AUTHORITY,
        "physical": True,
        "attempt_nonce": nonce,
        "outcome": status,
        "formal_readiness_eligible": status == "succeeded",
    }
    if attestation is not None:
        metadata.update(
            attestation_run_id=attestation.run_id,
            attestation_hash=attestation.input_fingerprint,
        )
    if status == "failed":
        metadata["error_type"] = "docker_attestation_error"
    run = RunRecord(
        run_id=f"docker_attestation_attempt_{fingerprint}",
        run_type=HOST_DOCKER_ATTEMPT_RUN_TYPE,
        status=status,
        input_fingerprint=fingerprint,
        started_at=started_at,
        completed_at=completed_at,
        metadata=metadata,
        error=("docker_attestation_error" if status == "failed" else None),
    )
    catalog.save_run(run)
    return run


def _save_restore_attempt(
    catalog: ResearchCatalog,
    *,
    marker: str,
    status: str,
    started_at: datetime,
    completed_at: datetime | None,
    successful_evidence_source: RunRecord | None = None,
    malformed: bool = False,
) -> RunRecord:
    nonce = content_fingerprint(
        marker,
        domain="test/minio-restore-attempt-nonce",
    )[:32]
    fingerprint = restore_drill_attempt_fingerprint(
        started_at=started_at,
        nonce=nonce,
        physical=True,
        controlled_test_object_store=False,
        readiness_admission="physical_minio_restore_drill",
    )
    attempt = {
        "attempt_schema_version": RESTORE_DRILL_ATTEMPT_SCHEMA_VERSION,
        "attempt_authority": RESTORE_DRILL_ATTEMPT_AUTHORITY,
        "attempt_started_at": started_at.isoformat(),
        "attempt_nonce": nonce,
        "attempt_outcome": status,
        "formal_readiness_eligible": status == "succeeded",
        "physical": True,
        "controlled_test_object_store": False,
        "readiness_admission": "physical_minio_restore_drill",
    }
    error = None
    if status == "succeeded":
        assert successful_evidence_source is not None
        assert completed_at is not None
        metadata = dict(successful_evidence_source.metadata)
        metadata.update(attempt)
        metadata["verified_at"] = completed_at.isoformat()
        metadata.pop("error_type", None)
        metadata.pop("failed_at", None)
        metadata.pop("restore_evidence_hash", None)
        metadata["restore_evidence_hash"] = restore_drill_evidence_hash(metadata)
    else:
        metadata = attempt
        if status == "failed":
            error = "restore_drill_internal_error"
            metadata["error_type"] = error
            assert completed_at is not None
            metadata["failed_at"] = completed_at.isoformat()
    if malformed:
        metadata["attempt_authority"] = "caller_supplied_restore_attempt"
    run = RunRecord(
        run_id=f"restore_attempt_{fingerprint}",
        run_type=RESTORE_DRILL_RUN_TYPE,
        status=status,
        input_fingerprint=fingerprint,
        started_at=started_at,
        completed_at=completed_at,
        metadata=metadata,
        error=error,
    )
    catalog.save_run(run)
    return run


def _save_legacy_restore_success(
    catalog: ResearchCatalog,
    *,
    source: RunRecord,
    started_at: datetime,
    completed_at: datetime,
) -> RunRecord:
    metadata = dict(source.metadata)
    for field_name in (
        "attempt_schema_version",
        "attempt_authority",
        "attempt_started_at",
        "attempt_nonce",
        "attempt_outcome",
        "formal_readiness_eligible",
    ):
        metadata.pop(field_name, None)
    metadata["verified_at"] = completed_at.isoformat()
    metadata.pop("restore_evidence_hash", None)
    evidence_hash = restore_drill_evidence_hash(metadata)
    metadata["restore_evidence_hash"] = evidence_hash
    run = RunRecord(
        run_id=f"restore_{evidence_hash}",
        run_type=RESTORE_DRILL_RUN_TYPE,
        status="succeeded",
        input_fingerprint=evidence_hash,
        started_at=started_at,
        completed_at=completed_at,
        metadata=metadata,
    )
    catalog.save_run(run)
    return run


class AuditSystem:
    def __init__(
        self,
        tmp_path: Path,
        *,
        missing: str | None = None,
        canary_variant: str = "valid",
        canary_completed_at: datetime | None = None,
    ) -> None:
        self.url = f"sqlite+pysqlite:///{(tmp_path / 'readiness.db').as_posix()}"
        self.engine = create_engine(self.url)
        Base.metadata.create_all(self.engine)
        self.catalog = ResearchCatalog(self.url)
        self.catalog.initialize_schema()
        self.frozen = FrozenCatalog(self.catalog)
        self.ledger = ProductionLedger(self.engine)
        self.config = _config()
        self.evidence = _evidence(rotation_blocked=missing == "rotation")
        self.canary_completed_at = canary_completed_at
        if missing != "schema_head":
            with self.engine.begin() as connection:
                connection.execute(
                    text("CREATE TABLE IF NOT EXISTS alembic_version (version_num TEXT NOT NULL)")
                )
                connection.execute(
                    text("INSERT INTO alembic_version(version_num) VALUES (:head)"),
                    {"head": RESEARCH_OS_ALEMBIC_HEAD},
                )

        self.bronze = {
            item: _snapshot(SnapshotTier.BRONZE) for item in REQUIRED_DATASETS
        }
        for reference in self.bronze.values():
            self.catalog.register_snapshot(reference)
        bronze_ids = tuple(item.snapshot_id for item in self.bronze.values())
        self.silvers = {
            session: _snapshot(SnapshotTier.SILVER, parents=bronze_ids)
            for session in SESSIONS
        }
        for reference in self.silvers.values():
            self.catalog.register_snapshot(reference)
        self.gold = _snapshot(
            SnapshotTier.GOLD,
            parents=tuple(item.snapshot_id for item in self.silvers.values()),
        )
        self.catalog.register_snapshot(self.gold)
        self.calendar_silver = _snapshot(
            SnapshotTier.SILVER, parents=bronze_ids[:2]
        )
        self.catalog.register_snapshot(self.calendar_silver)

        self.stage_records = {}
        for session in SESSIONS:
            dagster_run_id = f"physical-dagster-{session}"
            _save_run(self.catalog, dagster_run_id)
            source = _operation_result(
                "source_sync",
                {
                    "sources": [
                        {
                            "dataset": dataset,
                            "bronze_snapshot_id": self.bronze[dataset].snapshot_id,
                        }
                        for dataset in REQUIRED_DATASETS
                    ],
                    "bronze_snapshot_ids": list(bronze_ids),
                },
            )
            silver = _operation_result(
                "source_reconciliation",
                {"silver_snapshot_id": self.silvers[session].snapshot_id},
            )
            dq = _operation_result(
                "data_quality_gate",
                {
                    "silver_snapshot_id": self.silvers[session].snapshot_id,
                    "quality_report": {"status": "pass"},
                },
            )
            gold = _operation_result(
                "gold_iceberg_snapshot_publish",
                {
                    "snapshot_id": self.gold.snapshot_id,
                    "research_ready": True,
                    "analysis_start": "2017-01-01",
                    "analysis_end": session,
                    "parent_snapshot_ids": list(self.gold.parent_snapshot_ids),
                },
            )
            for name, result, output_id in (
                ("stage_source", source, None),
                ("stage_silver", silver, self.silvers[session].snapshot_id),
                ("stage_data_quality", dq, self.silvers[session].snapshot_id),
                ("stage_gold", gold, self.gold.snapshot_id),
            ):
                if missing == "matrix" and session == SESSIONS[-1] and name == "stage_data_quality":
                    continue
                if missing == "gold" and session == SESSIONS[-1] and name == "stage_gold":
                    continue
                record = _finish_partition(
                    self.ledger,
                    dataset=name,
                    partition_key=session,
                    run_id=dagster_run_id,
                    result=result,
                    output_snapshot_id=output_id,
                )
                self.stage_records[(session, name)] = record

        if missing != "calendar":
            bootstrap_result = {
                "exchange": "SSE",
                "source_start": "2016-06-01",
                "through": SESSIONS[-1],
                "sessions": list(SESSIONS),
                "silver_snapshot_id": self.calendar_silver.snapshot_id,
            }
            bootstrap_id = PartitionIdentity(
                "research_os", "bootstrap_trade_calendar", SESSIONS[-1]
            )
            self.ledger.ensure_partition(
                bootstrap_id,
                created_at=NOW - timedelta(hours=3),
                input_hash="6" * 64,
            )
            bootstrap_lease = self.ledger.claim(
                identity=bootstrap_id,
                owner="calendar-bootstrap",
                now=NOW - timedelta(hours=3),
                lease_for=timedelta(hours=4),
            )
            assert bootstrap_lease is not None
            self.ledger.finish(
                bootstrap_lease,
                status=PartitionStatus.SUCCEEDED,
                completed_at=NOW - timedelta(hours=1),
                run_id=f"physical-dagster-{SESSIONS[-1]}",
                output_snapshot_id=self.calendar_silver.snapshot_id,
                output_hash=content_fingerprint(
                    bootstrap_result,
                    domain="factor-lab/research-os/v1/calendar-bootstrap-output",
                ),
                details={"bootstrap_result": bootstrap_result},
            )
            for session in SESSIONS:
                identity = PartitionIdentity(
                    "research_os", "accepted_trade_calendar", session
                )
                self.ledger.ensure_partition(
                    identity,
                    created_at=NOW - timedelta(hours=2),
                    input_hash=content_fingerprint(
                        {"session": session}, domain="test/accepted-calendar/input"
                    ),
                )
                lease = self.ledger.claim(
                    identity=identity,
                    owner=f"calendar-{session}",
                    now=NOW - timedelta(hours=2),
                    lease_for=timedelta(hours=3),
                )
                assert lease is not None
                self.ledger.finish(
                    lease,
                    status=PartitionStatus.SUCCEEDED,
                    completed_at=NOW - timedelta(hours=1),
                    run_id=f"physical-dagster-{session}",
                    output_snapshot_id=self.calendar_silver.snapshot_id,
                    output_hash=content_fingerprint(
                        {"session": session}, domain="test/accepted-calendar/output"
                    ),
                    details={"accepted_calendar": {"partition_key": session}},
                )

        self.canary_object_evidence = []
        self._add_canary(missing=missing, variant=canary_variant)
        self.restore_run = None
        if missing != "restore":
            canary_runs = self.catalog.list_runs(
                limit=10, run_type=PHYSICAL_CANARY_RUN_TYPE
            )
            source_canary = canary_runs[0] if canary_runs else None
            mark_objects = [
                item
                for item in self.canary_object_evidence
                if item.get("tier") == "gold" and item.get("role") == "mark"
            ]
            restored_evidence = (
                max(mark_objects, key=lambda item: str(item["trade_date"]))
                if mark_objects
                else {
                    "snapshot_id": "missing-canary-snapshot",
                    "role": "mark",
                    "trade_date": CANARY_SESSIONS[-1],
                    "uri": (
                        "s3://factor-lab/archive/readiness/"
                        f"sha256={'a' * 64}/gold-manifest.json"
                    ),
                    "object_sha256": "a" * 64,
                    "size_bytes": 4096,
                }
            )
            source_canary_hash = (
                str(source_canary.metadata["canary_evidence_hash"])
                if source_canary is not None
                else "d" * 64
            )
            deletion_challenge = "c" * 64
            deleted_cache_proof = content_fingerprint(
                {
                    "deletion_challenge": deletion_challenge,
                    "source_canary_evidence_hash": source_canary_hash,
                    "source_canary_execution_contract_hash": (
                        CANARY_EXECUTION_CONTRACT_HASH
                    ),
                    "source_snapshot_id": restored_evidence["snapshot_id"],
                    "object_sha256": restored_evidence["object_sha256"],
                    "size_bytes": restored_evidence["size_bytes"],
                    "first_restore_sha256": restored_evidence["object_sha256"],
                    "first_restore_size_bytes": restored_evidence["size_bytes"],
                    "cache_existed_after_first_restore": True,
                    "cache_absent_before_second_restore": True,
                },
                domain="factor-lab/research-os/v1/restore-drill-deleted-cache-proof",
            )
            restore_completed_at = NOW - timedelta(minutes=5)
            restore_started_at = NOW - timedelta(minutes=9)
            restore_attempt_nonce = "b" * 32
            restore_attempt_fingerprint = restore_drill_attempt_fingerprint(
                started_at=restore_started_at,
                nonce=restore_attempt_nonce,
                physical=True,
                controlled_test_object_store=False,
                readiness_admission="physical_minio_restore_drill",
            )
            restore = {
                "attempt_schema_version": RESTORE_DRILL_ATTEMPT_SCHEMA_VERSION,
                "attempt_authority": RESTORE_DRILL_ATTEMPT_AUTHORITY,
                "attempt_started_at": restore_started_at.isoformat(),
                "attempt_nonce": restore_attempt_nonce,
                "attempt_outcome": "succeeded",
                "formal_readiness_eligible": True,
                "schema_version": RESTORE_DRILL_SCHEMA_VERSION,
                "authority": "code_selected_physical_canary_twice_hydrated",
                "physical": True,
                "controlled_test_object_store": False,
                "readiness_admission": "physical_minio_restore_drill",
                "object_uri": restored_evidence["uri"],
                "expected_sha256": restored_evidence["object_sha256"],
                "restored_sha256": restored_evidence["object_sha256"],
                "expected_size_bytes": restored_evidence["size_bytes"],
                "restored_size_bytes": restored_evidence["size_bytes"],
                "deleted_cache_proof": deleted_cache_proof,
                "deletion_challenge": deletion_challenge,
                "cache_deleted_before_second_restore": True,
                "first_restore_sha256": restored_evidence["object_sha256"],
                "first_restore_size_bytes": restored_evidence["size_bytes"],
                "second_restore_downloaded": True,
                "local_cache_retained": False,
                "source_canary_run_id": (
                    "different-physical-canary"
                    if canary_variant == "cross_restore"
                    else (
                        source_canary.run_id
                        if source_canary is not None
                        else "missing-canary"
                    )
                ),
                "source_canary_evidence_hash": source_canary_hash,
                "source_canary_execution_contract_hash": (
                    CANARY_EXECUTION_CONTRACT_HASH
                ),
                "source_snapshot_id": restored_evidence["snapshot_id"],
                "source_snapshot_role": "mark",
                "source_snapshot_trade_date": restored_evidence["trade_date"],
                "verified_at": restore_completed_at.isoformat(),
            }
            restore_hash = restore_drill_evidence_hash(restore)
            restore["restore_evidence_hash"] = restore_hash
            self.restore_run = RunRecord(
                run_id=f"restore_attempt_{restore_attempt_fingerprint}",
                run_type=RESTORE_DRILL_RUN_TYPE,
                status="succeeded",
                input_fingerprint=restore_attempt_fingerprint,
                started_at=restore_started_at,
                completed_at=restore_completed_at,
                metadata=restore,
            )
            self.catalog.save_run(self.restore_run)

        if missing != "capability":
            session = SESSIONS[-1]

            def _typed_ref(
                role: str,
                tier: SnapshotTier,
                *,
                parents: tuple[str, ...] = (),
            ) -> DataSnapshotRef:
                physical_hash = content_fingerprint(
                    {"role": role, "session": session},
                    domain="test/readiness/typed-execution-object",
                )
                manifest = {
                    "role": role,
                    "physical_object": {
                        "uri": (
                            "s3://factor-lab/research-os/typed/"
                            f"sha256={physical_hash}/{role}.parquet"
                        ),
                        "key": (
                            "research-os/typed/"
                            f"sha256={physical_hash}/{role}.parquet"
                        ),
                        "sha256": physical_hash,
                        "size_bytes": 2048,
                    },
                }
                content_hash = content_fingerprint(
                    manifest, domain="test/readiness/typed-execution-snapshot"
                )
                return DataSnapshotRef(
                    snapshot_id=f"typed_{role}_{content_hash[:48]}",
                    tier=tier,
                    uri=str(manifest["physical_object"]["uri"]),
                    content_hash=content_hash,
                    parent_snapshot_ids=parents,
                    as_of=NOW - timedelta(hours=1),
                    quality_status=DataQualityStatus.ACCEPTED,
                    trust_labels=("point_in_time", "quality_accepted"),
                    manifest=manifest,
                )

            source_ref = _typed_ref(
                "diemeng_open_observation", SnapshotTier.BRONZE
            )
            execution_ref = _typed_ref(
                "execution", SnapshotTier.GOLD, parents=(source_ref.snapshot_id,)
            )
            mark_ref = _typed_ref("mark", SnapshotTier.GOLD)
            bundle_ref = _typed_ref(
                TYPED_EXECUTION_BUNDLE_ROLE,
                SnapshotTier.SILVER,
                parents=(execution_ref.snapshot_id, mark_ref.snapshot_id),
            )
            for reference in (source_ref, execution_ref, mark_ref, bundle_ref):
                self.catalog.register_snapshot(reference)

            source_partition_hash = content_fingerprint(
                {"source_snapshot_id": source_ref.snapshot_id},
                domain="test/readiness/source-open-partition",
            )
            source_identity = PartitionIdentity(
                "diemeng", TYPED_OPEN_DATASET, session
            )
            self.ledger.ensure_partition(source_identity, created_at=NOW - timedelta(hours=2))
            source_lease = self.ledger.claim(
                identity=source_identity,
                owner="typed-open-fixture",
                now=NOW - timedelta(hours=2),
                lease_for=timedelta(hours=3),
            )
            assert source_lease is not None
            self.ledger.finish(
                source_lease,
                status=PartitionStatus.SUCCEEDED,
                completed_at=NOW - timedelta(hours=1),
                output_snapshot_id=source_ref.snapshot_id,
                output_hash=source_partition_hash,
                details={"physical": True},
            )

            capability_evidence_hash = "e" * 64
            typed_details = {
                "execution_snapshot_id": execution_ref.snapshot_id,
                "execution_snapshot_hash": execution_ref.content_hash,
                "mark_snapshot_id": mark_ref.snapshot_id,
                "mark_snapshot_hash": mark_ref.content_hash,
                "bundle_snapshot_id": bundle_ref.snapshot_id,
                "bundle_snapshot_hash": bundle_ref.content_hash,
                "capability": {
                    "decision": "accepted",
                    "reasons": [],
                    "evidence_hash": capability_evidence_hash,
                },
            }
            typed_partition_hash = content_fingerprint(
                typed_details,
                domain="factor-lab/research-os/v1/typed-execution-partition-result",
            )
            typed_identity = PartitionIdentity(
                FORMAL_EXECUTION_SOURCE_ID,
                TYPED_EXECUTION_OUTPUT_DATASET,
                session,
            )
            self.ledger.ensure_partition(typed_identity, created_at=NOW - timedelta(hours=2))
            typed_lease = self.ledger.claim(
                identity=typed_identity,
                owner="typed-execution-fixture",
                now=NOW - timedelta(hours=2),
                lease_for=timedelta(hours=3),
            )
            assert typed_lease is not None
            self.ledger.finish(
                typed_lease,
                status=PartitionStatus.SUCCEEDED,
                completed_at=NOW - timedelta(hours=1),
                output_snapshot_id=bundle_ref.snapshot_id,
                output_hash=typed_partition_hash,
                details=typed_details,
            )

            gold_record = self.stage_records.get((session, "stage_gold"))
            dq_record = self.stage_records.get((session, "stage_data_quality"))
            detail = {
                "schema_version": FORMAL_EXECUTION_CAPABILITY_SCHEMA_VERSION,
                "authority_schema_version": "research-os/execution-snapshot/v1",
                "decision": "accepted",
                "reasons": [],
                "real_source_probe": True,
                "physical": True,
                "point_in_time": True,
                "formal_shadow_projection": "allowed",
                "event_semantics": "realtime_server_timed_open_09_30",
                "collection_mode": "realtime_open",
                "server_available_at_verified": True,
                "tradability_state_verified": True,
                "ingested_within_cutoff": True,
                "observed_local_time": "09:30:00",
                "timezone": "Asia/Shanghai",
                "event_time_field": "trade_time",
                "available_at_field": "trade_time",
                "price_field": "open",
                "mark_semantics": "accepted_gold_close_snapshot",
                "execution_mark_roles_separate": True,
                "trade_date": session,
                "production_configuration_hash": (
                    production_execution_configuration_hash(self.config)
                ),
                "source_contract_hash": (
                    production_formal_source_contract_hash(self.config)
                ),
                "source_snapshot_id": source_ref.snapshot_id,
                "source_snapshot_hash": source_ref.content_hash,
                "source_partition_hash": source_partition_hash,
                "source_probe_hash": "3" * 64,
                "gold_snapshot_id": self.gold.snapshot_id,
                "gold_snapshot_hash": self.gold.content_hash,
                "gold_partition_hash": (
                    "0" * 64 if gold_record is None else gold_record.output_hash
                ),
                "data_quality_partition_hash": (
                    "0" * 64 if dq_record is None else dq_record.output_hash
                ),
                "execution_snapshot_id": execution_ref.snapshot_id,
                "execution_snapshot_hash": execution_ref.content_hash,
                "mark_snapshot_id": mark_ref.snapshot_id,
                "mark_snapshot_hash": mark_ref.content_hash,
                "bundle_snapshot_id": bundle_ref.snapshot_id,
                "bundle_snapshot_hash": bundle_ref.content_hash,
                "typed_partition_hash": typed_partition_hash,
                "physical_object_hashes": {
                    "source": source_ref.manifest["physical_object"]["sha256"],
                    "execution": execution_ref.manifest["physical_object"]["sha256"],
                    "mark": mark_ref.manifest["physical_object"]["sha256"],
                    "bundle": bundle_ref.manifest["physical_object"]["sha256"],
                },
                "capability_evidence_hash": capability_evidence_hash,
            }
            fields = tuple(FORMAL_EXECUTION_REQUIRED_FIELDS)
            probe_hash = formal_execution_probe_hash(
                source_id=FORMAL_EXECUTION_SOURCE_ID,
                dataset=TYPED_EXECUTION_CAPABILITY_DATASET,
                contract_hash=TYPED_EXECUTION_CONTRACT_HASH,
                fields=fields,
                detail=detail,
            )
            self.ledger.upsert_capability(
                CapabilityRecord(
                    source_id=FORMAL_EXECUTION_SOURCE_ID,
                    dataset=TYPED_EXECUTION_CAPABILITY_DATASET,
                    status=CapabilityStatus.ACCEPTED,
                    contract_hash=TYPED_EXECUTION_CONTRACT_HASH,
                    probe_hash=probe_hash,
                    fields=fields,
                    detail=json.dumps(detail, sort_keys=True, separators=(",", ":")),
                    probed_at=NOW - timedelta(hours=1),
                )
            )

        if missing != "dagster":
            with self.engine.begin() as connection:
                connection.execute(
                    text("CREATE TABLE runs (run_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
                )
                connection.execute(
                    text(
                        "CREATE TABLE event_logs ("
                        "id INTEGER PRIMARY KEY, run_id TEXT NOT NULL, "
                        "dagster_event_type TEXT NOT NULL)"
                    )
                )
                connection.execute(
                    text("INSERT INTO runs(run_id,status) VALUES (:run_id,'SUCCESS')"),
                    {"run_id": f"physical-dagster-{SESSIONS[-1]}"},
                )
                connection.execute(
                    text(
                        "CREATE TABLE daemon_heartbeats ("
                        "daemon_type TEXT PRIMARY KEY, timestamp REAL NOT NULL)"
                    )
                )
                connection.execute(
                    text(
                        "INSERT INTO daemon_heartbeats(daemon_type,timestamp) "
                        "VALUES ('SENSOR',:timestamp)"
                    ),
                    {"timestamp": (NOW - timedelta(minutes=21)).timestamp()},
                )
                connection.execute(
                    text(
                        "INSERT INTO event_logs(id,run_id,dagster_event_type) "
                        "VALUES (1,:run_id,'ASSET_MATERIALIZATION')"
                    ),
                    {"run_id": f"physical-dagster-{SESSIONS[-1]}"},
                )

        if missing != "soak":
            soak_started_at = NOW - timedelta(hours=24, minutes=20)
            soak_completed_at = NOW - timedelta(minutes=20)
            sample_hashes = []
            sample_run_ids = []
            process_identity = "dagster-code-server-instance-01"
            for index in range(145):
                sampled_at = soak_started_at + timedelta(minutes=10 * index)
                sample = {
                    "schema_version": (
                        DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_SCHEMA_VERSION
                    ),
                    "authority": (
                        "dagster_sensor_grpc_roundtrip_plus_pg_heartbeat"
                    ),
                    "physical": True,
                    "healthy": True,
                    "service_name": "dagster-code-server",
                    "code_location": "factor_lab_research_os",
                    "heartbeat_source": "dagster_postgresql",
                    "daemon_type": "SENSOR",
                    "sampled_at": sampled_at.isoformat(),
                    "dagster_heartbeat_at": sampled_at.isoformat(),
                    "maximum_heartbeat_gap_seconds": 600,
                    "process_identity": process_identity,
                    "build_identity_hash": (
                        self.evidence.provenance.build_identity_hash
                    ),
                    "oci_image_id": self.evidence.provenance.oci_image_id,
                }
                sample_hash = (
                    dagster_code_location_health_sample_evidence_hash(sample)
                )
                sample["sample_evidence_hash"] = sample_hash
                sample_hashes.append(sample_hash)
                sample_run_ids.append(f"dagster_health_sample_{sample_hash}")
                self.catalog.save_run(
                    RunRecord(
                        run_id=f"dagster_health_sample_{sample_hash}",
                        run_type=DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_RUN_TYPE,
                        status="succeeded",
                        input_fingerprint=sample_hash,
                        started_at=sampled_at,
                        completed_at=sampled_at,
                        metadata=sample,
                    )
                )
            soak = {
                "schema_version": DAGSTER_CODE_LOCATION_SOAK_SCHEMA_VERSION,
                "authority": "derived_from_persisted_health_samples",
                "physical": True,
                "service_name": "dagster-code-server",
                "code_location": "factor_lab_research_os",
                "heartbeat_source": "dagster_postgresql",
                "daemon_type": "SENSOR",
                "health_sample_count": len(sample_hashes),
                "maximum_sample_gap_seconds": 600,
                "restart_count": 0,
                "process_identity": process_identity,
                "health_sample_hash": (
                    dagster_code_location_health_series_hash(sample_hashes)
                ),
                "health_sample_run_ids": sample_run_ids,
                "build_identity_hash": self.evidence.provenance.build_identity_hash,
                "oci_image_id": self.evidence.provenance.oci_image_id,
            }
            soak_hash = dagster_code_location_soak_evidence_hash(soak)
            soak["soak_evidence_hash"] = soak_hash
            self.catalog.save_run(
                RunRecord(
                    run_id=f"soak_{soak_hash}",
                    run_type=DAGSTER_CODE_LOCATION_SOAK_RUN_TYPE,
                    status="succeeded",
                    input_fingerprint=soak_hash,
                    started_at=soak_started_at,
                    completed_at=soak_completed_at,
                    metadata=soak,
                )
            )

    def _add_canary(self, *, missing: str | None, variant: str) -> None:
        if variant == "synthetic":
            self.catalog.save_run(
                RunRecord(
                    run_id="engcan_synthetic",
                    run_type="engineering_canary",
                    status="succeeded",
                    input_fingerprint="d" * 64,
                    started_at=NOW - timedelta(hours=2),
                    completed_at=NOW - timedelta(hours=1),
                    metadata={
                        "evidence_class": "engineering_canary",
                        "evidence_scope": "retrospective_non_forward",
                        "formal_epoch_eligible": False,
                    },
                )
            )
            return
        if missing == "canary":
            return
        run_id = "physical-canary-real-01"
        plan_fingerprint = content_fingerprint(
            {"run_id": run_id, "sessions": list(CANARY_SESSIONS)},
            domain="test/physical-canary/plan",
        )
        completed_at = self.canary_completed_at or (
            NOW - timedelta(days=2)
            if variant == "stale"
            else NOW - timedelta(minutes=10)
        )
        started_at = completed_at - timedelta(hours=2)
        self.catalog.save_run(
            RunRecord(
                run_id=run_id,
                run_type=PHYSICAL_CANARY_RUN_TYPE,
                status="running",
                input_fingerprint=plan_fingerprint,
                started_at=started_at,
                metadata={
                    "evidence_schema": PHYSICAL_CANARY_SCHEMA_VERSION,
                    "physical_source_attested": True,
                    "controlled_test_adapter": False,
                },
            )
        )
        account_id = "physical-canary-account"
        self.catalog.create_shadow_account(
            account_id=account_id,
            name="Physical engineering canary",
            initial_capital=50_000_000,
            opened_at=NOW - timedelta(days=100),
        )
        projection_dates = tuple(map(date.fromisoformat, CANARY_SESSIONS[1:]))
        projected_events = self.catalog.append_shadow_events_atomic(
            account_id=account_id,
            events=tuple(
                {
                    "event_type": "account_projected",
                    "occurred_at": datetime.combine(
                        item, datetime.min.time(), tzinfo=timezone.utc
                    )
                    + timedelta(hours=15),
                    "payload": {"account_state": {"nav": 50_000_000 + index}},
                }
                for index, item in enumerate(projection_dates)
            ),
        )
        account = self.catalog.get_shadow_account(account_id)
        assert account is not None
        sleeve_id = "physical-canary-sleeve"
        self.catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key="physical-canary-shadow",
                sleeve_id=sleeve_id,
                to_state=LifecycleState.SHADOW,
                cause="physical engineering canary",
                occurred_at=NOW - timedelta(minutes=20),
            )
        )

        bronze, bronze_evidence = _physical_canary_snapshot(
            SnapshotTier.BRONZE,
            run_id=run_id,
            role="official_daily",
            trade_date=CANARY_SESSIONS[-1],
        )
        silver, silver_evidence = _physical_canary_snapshot(
            SnapshotTier.SILVER,
            run_id=run_id,
            role="accepted",
            trade_date=CANARY_SESSIONS[-1],
            parents=(bronze.snapshot_id,),
            manifest_variant=(
                variant
                if variant in {"invalid_dq_status", "invalid_dq_reconciliation"}
                else None
            ),
        )
        mark_snapshots = {}
        mark_evidence = {}
        execution_snapshots = {}
        execution_evidence = {}
        for session in CANARY_SESSIONS:
            mark_snapshots[session], mark_evidence[session] = (
                _physical_canary_snapshot(
                    SnapshotTier.GOLD,
                    run_id=run_id,
                    role="mark",
                    trade_date=session,
                    parents=(silver.snapshot_id,),
                )
            )
        for session in CANARY_SESSIONS[1:]:
            execution_snapshots[session], execution_evidence[session] = (
                _physical_canary_snapshot(
                    SnapshotTier.GOLD,
                    run_id=run_id,
                    role="execution",
                    trade_date=session,
                    parents=(silver.snapshot_id,),
                    manifest_variant=(
                        "missing_execution_audit_column"
                        if variant == "missing_execution_audit_column"
                        and session == CANARY_SESSIONS[1]
                        else "invalid_opening_audit"
                        if variant == "invalid_opening_audit"
                        and session == CANARY_SESSIONS[1]
                        else None
                    ),
                )
            )
        gold = mark_snapshots[CANARY_SESSIONS[-1]]
        for reference in (
            bronze,
            silver,
            *mark_snapshots.values(),
            *execution_snapshots.values(),
        ):
            self.catalog.register_snapshot(reference)
        self.canary_object_evidence = [
            bronze_evidence,
            silver_evidence,
            *mark_evidence.values(),
            *execution_evidence.values(),
        ]

        authority = ShadowEvidenceAuthority(
            self.engine,
            enforce_realtime=False,
            require_fleet_closure=False,
        )
        binding = authority.bind_role(
            role=ShadowRole.CHAMPION,
            role_key="physical-engineering-canary",
            account_id=account_id,
            sleeve_id=sleeve_id,
            bound_at=NOW - timedelta(days=90),
            metadata={"evidence_class": "engineering_canary"},
        )
        session_rows = []
        for index, (session, event) in enumerate(
            zip(CANARY_SESSIONS[1:], projected_events), start=1
        ):
            rebalanced = (index - 1) % 5 == 0
            values = {
                "account_id": account_id,
                "trade_date": session,
                "role_binding_id": binding.binding_id,
                "epoch_id": None,
                "evidence_window_hash": None,
                "evidence_class": "engineering",
                "decision_snapshot_id": (
                    mark_snapshots[CANARY_SESSIONS[index - 1]].snapshot_id
                    if rebalanced
                    else None
                ),
                "execution_snapshot_id": execution_snapshots[session].snapshot_id,
                "mark_snapshot_id": mark_snapshots[session].snapshot_id,
                "rebalanced": rebalanced,
                "cash": 50_000_000.0,
                "positions_value": 0.0,
                "nav": 50_000_000.0,
                "benchmark_nav": 50_000_000.0 + index,
                "position_count": 0,
                "account_event_hash": event.event_hash,
                "account_event_sequence": event.sequence_number,
            }
            session_rows.append(
                {
                    **values,
                    "session_hash": content_fingerprint(
                        ShadowEvidenceAuthority._session_content(values),
                        domain="factor-lab/research-os/v1/shadow-session",
                    ),
                    "created_at": NOW - timedelta(minutes=30) + timedelta(seconds=index),
                }
            )
        with self.engine.begin() as connection:
            connection.execute(
                Base.metadata.tables["ros_shadow_sessions"].insert(),
                session_rows,
            )

        canary_partition_ids = []
        stage_bindings = [
            ("bronze_daily", "bronze", bronze),
            ("silver_accepted", "silver", silver),
            ("dq_accepted", "data_quality", silver),
        ]
        stage_bindings.extend(
            (f"gold_mark_{session}", "gold", reference)
            for session, reference in mark_snapshots.items()
        )
        stage_bindings.extend(
            (f"gold_execution_{session}", "gold", reference)
            for session, reference in execution_snapshots.items()
        )
        first_execution_id = execution_snapshots[CANARY_SESSIONS[1]].snapshot_id
        for dataset, stage, reference in stage_bindings:
            if variant == "missing_stage" and stage == "gold":
                continue
            if (
                variant == "missing_snapshot_partition"
                and reference.snapshot_id == first_execution_id
            ):
                continue
            identity = PartitionIdentity(
                "engineering_canary",
                dataset,
                str(reference.manifest["trade_date"]),
            )
            partition_input_hash = content_fingerprint(
                {"dataset": dataset, "run_id": run_id},
                domain="test/physical-canary/partition-input",
            )
            self.ledger.ensure_partition(
                identity,
                created_at=NOW - timedelta(hours=2),
                input_hash=partition_input_hash,
            )
            lease = self.ledger.claim(
                identity=identity,
                owner=f"canary-{dataset}",
                now=NOW - timedelta(hours=2),
                lease_for=timedelta(hours=3),
            )
            assert lease is not None
            role = str(reference.manifest["role"])
            claim_lineage, claim_lineage_hash = _physical_partition_claim_lineage(
                identity,
                input_hash=partition_input_hash,
                stage=stage,
                role=role,
            )
            opening_audit = reference.manifest.get("opening_cross_check")
            quality_report = reference.manifest.get("quality_report")
            partition_opening_audit = opening_audit
            if (
                variant == "tampered_opening_audit"
                and reference.snapshot_id == first_execution_id
            ):
                partition_opening_audit = {
                    **opening_audit,
                    "maximum_absolute_difference": 0.75,
                }
            partition_role = role
            if (
                variant == "wrong_snapshot_role"
                and reference.snapshot_id == first_execution_id
            ):
                partition_role = "mark"
            output_hash = reference.content_hash
            if stage == "data_quality":
                output_hash = content_fingerprint(
                    dict(quality_report),
                    domain="factor-lab/research-os/v1/physical-canary-dq-report",
                )
                if variant == "wrong_dq_hash":
                    output_hash = "8" * 64
            partition_quality_report = quality_report
            if variant == "tampered_dq_report" and stage == "data_quality":
                partition_quality_report = {
                    **quality_report,
                    "historical_st_rows": 2,
                }
            record = self.ledger.finish(
                lease,
                status=PartitionStatus.SUCCEEDED,
                completed_at=NOW - timedelta(hours=1),
                run_id=run_id,
                output_snapshot_id=reference.snapshot_id,
                output_hash=(
                    "f" * 64
                    if variant == "wrong_snapshot_hash"
                    and reference.snapshot_id == first_execution_id
                    else output_hash
                ),
                details={
                    "evidence_schema": PHYSICAL_CANARY_SCHEMA_VERSION,
                    "physical_source_attested": True,
                    "controlled_test_adapter": False,
                    "run_id": run_id,
                    "stage": stage,
                    "role": partition_role,
                    "claim_lineage": claim_lineage,
                    "claim_lineage_hash": claim_lineage_hash,
                    "canary_execution_contract_hash": (
                        CANARY_EXECUTION_CONTRACT_HASH
                        if stage == "gold"
                        else None
                    ),
                    "opening_cross_check": partition_opening_audit,
                    "quality_status": (
                        "accepted" if stage == "data_quality" else None
                    ),
                    "quality_report": partition_quality_report,
                },
            )
            canary_partition_ids.append(record.identity.partition_run_id)

            if (
                variant == "duplicate_snapshot_partition"
                and reference.snapshot_id == first_execution_id
            ):
                duplicate_identity = PartitionIdentity(
                    "engineering_canary",
                    f"{dataset}_duplicate",
                    str(reference.manifest["trade_date"]),
                )
                self.ledger.ensure_partition(
                    duplicate_identity,
                    created_at=NOW - timedelta(hours=2),
                    input_hash="9" * 64,
                )
                duplicate_lease = self.ledger.claim(
                    identity=duplicate_identity,
                    owner="canary-duplicate",
                    now=NOW - timedelta(hours=2),
                    lease_for=timedelta(hours=3),
                )
                assert duplicate_lease is not None
                duplicate = self.ledger.finish(
                    duplicate_lease,
                    status=PartitionStatus.SUCCEEDED,
                    completed_at=NOW - timedelta(hours=1),
                    run_id=run_id,
                    output_snapshot_id=reference.snapshot_id,
                    output_hash=reference.content_hash,
                    details={
                        "evidence_schema": PHYSICAL_CANARY_SCHEMA_VERSION,
                        "physical_source_attested": True,
                        "controlled_test_adapter": False,
                        "run_id": run_id,
                        "stage": stage,
                        "role": role,
                        "canary_execution_contract_hash": (
                            CANARY_EXECUTION_CONTRACT_HASH
                        ),
                        "opening_cross_check": opening_audit,
                    },
                )
                canary_partition_ids.append(
                    duplicate.identity.partition_run_id
                )

        calendar_rows = {
            item.identity.partition_key: item
            for item in self.ledger.list_partitions(
                statuses=(PartitionStatus.SUCCEEDED,),
                source_id="research_os",
                dataset="accepted_trade_calendar",
                limit=1_000,
            )
        }
        bound_calendar = tuple(
            calendar_rows[item]
            for item in CANARY_SESSIONS
            if item in calendar_rows
        )
        canary_probe_hash = "b" * 64
        self.ledger.upsert_capability(
            CapabilityRecord(
                source_id="official-canary",
                dataset="daily",
                status=CapabilityStatus.ACCEPTED,
                contract_hash="c" * 64,
                probe_hash=canary_probe_hash,
                fields=("ticker", "trade_date", "close"),
                detail="bounded real SourceAdapter probe passed",
                probed_at=NOW - timedelta(hours=1),
            )
        )
        opening_probe_hash = "d" * 64
        self.ledger.upsert_capability(
            CapabilityRecord(
                source_id="diemeng",
                dataset="opening_execution",
                status=CapabilityStatus.ACCEPTED,
                contract_hash=(
                    "e" * 64
                    if variant == "wrong_opening_probe_contract"
                    else CANARY_OPENING_CONTRACT_HASH
                ),
                probe_hash=opening_probe_hash,
                fields=("stock_code", "trade_time", "open"),
                detail="bounded real SourceAdapter probe passed",
                probed_at=NOW - timedelta(hours=1),
            )
        )
        claimed_session_hashes = [
            str(item["session_hash"]) for item in session_rows
        ]
        if variant == "forged_session_hash":
            claimed_session_hashes[0] = "f" * 64
        evaluator_build = self.evidence.provenance.public_dict()
        if variant == "wrong_evaluator_build":
            evaluator_build = {
                **evaluator_build,
                "build_identity_hash": "f" * 64,
            }
        evidence_payload = {
            "evidence_schema": PHYSICAL_CANARY_SCHEMA_VERSION,
            "evaluator_identity": {
                "identity_schema": (
                    "research-os/physical-canary-evaluator-identity/v1"
                ),
                "physical_canary_schema": PHYSICAL_CANARY_SCHEMA_VERSION,
                "reconciliation_schema": "research-os/data-reconciliation/v1",
                "canary_execution_contract_hash": (
                    CANARY_EXECUTION_CONTRACT_HASH
                ),
                "mode": "production_image",
                "build_provenance": evaluator_build,
            },
            "evidence_class": "engineering_canary",
            "evidence_scope": "retrospective_non_forward",
            "canary_execution_contract_hash": CANARY_EXECUTION_CONTRACT_HASH,
            "formal_epoch_eligible": False,
            "physical_source_attested": True,
            "controlled_test_adapter": False,
            "readiness_admission": "physical_engineering_prerequisite",
            "run_id": run_id,
            "run_type": PHYSICAL_CANARY_RUN_TYPE,
            "input_fingerprint": plan_fingerprint,
            "calendar_sessions": list(CANARY_SESSIONS),
            "accepted_calendar_partition_ids": [
                item.identity.partition_run_id for item in bound_calendar
            ],
            "accepted_calendar_output_hashes": [
                item.output_hash for item in bound_calendar
            ],
            "security_count": 50,
            "security_set_hash": "a" * 64,
            "projected_session_count": 20,
            "partition_run_ids": sorted(canary_partition_ids),
            "source_probe_hashes": {
                "official-canary:daily": canary_probe_hash,
                **(
                    {}
                    if variant == "missing_opening_probe"
                    else {"diemeng:opening_execution": opening_probe_hash}
                ),
            },
            "snapshot_evidence": self.canary_object_evidence,
            "shadow_session_hashes": claimed_session_hashes,
            "shadow_account_event_hashes": [
                item.event_hash for item in projected_events
            ],
            "role_binding_id": binding.binding_id,
            "account_id": account_id,
            "sleeve_id": sleeve_id,
            "sleeve_state": "shadow",
            "opening_execution_formal_ready": False,
        }
        canary_hash = physical_canary_evidence_hash(evidence_payload)
        canary = {
            **evidence_payload,
            "canary_evidence_hash": (
            "e" * 64 if variant == "fake_hash" else canary_hash
            ),
            "physical_object_count": len(self.canary_object_evidence),
            "bronze_object_count": 1,
            "silver_object_count": 1,
            "gold_object_count": len(mark_snapshots) + len(execution_snapshots),
        }
        self.catalog.save_run(
            RunRecord(
                run_id=run_id,
                run_type=PHYSICAL_CANARY_RUN_TYPE,
                status="succeeded",
                input_fingerprint=plan_fingerprint,
                started_at=started_at,
                completed_at=completed_at,
                metadata=canary,
            )
        )

    def auditor(self) -> ProductionReadinessAuditor:
        return ProductionReadinessAuditor(
            self.frozen,
            self.ledger,
            config=self.config,
            config_evidence=self.evidence,
        )

    def close(self) -> None:
        self.ledger.close()
        self.catalog.close()
        self.engine.dispose()


def test_all_persisted_physical_facts_reach_formal_ready_and_are_idempotent(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        auditor = system.auditor()
        first = auditor.audit()
        second = auditor.audit()

        assert first.status is ProductionReadinessStatus.FORMAL_EPOCH_READY
        assert first.ready and first.blockers == ()
        oci_check = next(
            item
            for item in first.checks
            if item.code == "daemon_inspected_oci_provenance"
        )
        assert oci_check.evidence["epoch_fields"] == {
            "architecture_version": "research-os/v1",
            "code_hash": "1" * 64,
            "configuration_hash": "2" * 64,
            "dependency_lock_hash": "3" * 64,
            "dirty_patch_hash": "4" * 64,
        }
        assert second == first
        assert auditor.latest() == first
        rows = system.catalog.list_runs(run_type="production_readiness_audit")
        assert len(rows) == 1
        assert rows[0].metadata["authority"] == (
            "postgresql_derived_no_caller_assertions"
        )

        system.frozen._now = NOW + timedelta(seconds=50)
        later = auditor.audit()
        assert later.audit_id != first.audit_id
        assert later.audited_at == NOW + timedelta(seconds=50)
        assert auditor.latest() == later
        assert len(
            system.catalog.list_runs(run_type="production_readiness_audit")
        ) == 2
    finally:
        system.close()


def test_latest_failed_restore_attempt_blocks_old_success_and_canary_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        assert system.restore_run is not None
        archive = S3ImmutableArchive(
            bucket="factor-lab",
            filesystem=SimpleNamespace(),  # type: ignore[arg-type]
            prefix=CANARY_OBJECT_PREFIX,
        )
        service = PhysicalMinioRestoreDrillService(
            catalog=system.catalog,
            object_store_archive=archive,
            production_evidence=SimpleNamespace(
                engineering_canary_execution_contract_hash=(
                    CANARY_EXECUTION_CONTRACT_HASH
                ),
                runtime_data_root=tmp_path,
            ),  # type: ignore[arg-type]
            controlled_test=False,
        )
        failure_times = iter(
            (NOW - timedelta(minutes=4), NOW - timedelta(minutes=3))
        )
        monkeypatch.setattr(service, "_assert_runtime_admission", lambda: None)
        monkeypatch.setattr(service, "_database_now", lambda: next(failure_times))

        def fail_selection(*, observed_at: datetime) -> None:
            raise RuntimeError(f"MinIO secret must not persist at {observed_at}")

        monkeypatch.setattr(service, "_select_object", fail_selection)

        with pytest.raises(RuntimeError, match="secret must not persist"):
            service.run()
        failed = system.catalog.list_runs(
            limit=1,
            run_type=RESTORE_DRILL_RUN_TYPE,
        )[0]
        assert failed.status == "failed"
        assert "secret" not in json.dumps(failed.metadata, sort_keys=True)

        audit = system.auditor().audit()
        restore_check = next(
            item for item in audit.checks if item.code == "minio_restore_drill"
        )
        canary_check = next(
            item for item in audit.checks if item.code == "physical_engineering_canary"
        )

        assert not restore_check.passed
        assert restore_check.blockers == (
            "physical_minio_restore_drill_latest_attempt_failed",
        )
        assert restore_check.evidence["latest_run_id"] == failed.run_id
        assert any(
            "restore_drill_binding_invalid" in item["reasons"]
            for item in canary_check.evidence["inspected_runs"]
        )
        assert audit.status is not ProductionReadinessStatus.FORMAL_EPOCH_READY
    finally:
        system.close()


def test_latest_malformed_restore_attempt_never_falls_back_to_old_success(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        malformed = _save_restore_attempt(
            system.catalog,
            marker="malformed-after-valid-success",
            status="failed",
            started_at=NOW - timedelta(minutes=4),
            completed_at=NOW - timedelta(minutes=3),
            malformed=True,
        )

        check, selected = system.auditor()._restore_check(now=NOW)

        assert selected is None
        assert check.blockers == (
            "physical_minio_restore_drill_latest_attempt_invalid",
        )
        assert check.evidence["latest_run_id"] == malformed.run_id
    finally:
        system.close()


def test_newest_v2_restore_success_supersedes_prior_failed_attempt(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        assert system.restore_run is not None
        _save_restore_attempt(
            system.catalog,
            marker="prior-failed-attempt",
            status="failed",
            started_at=NOW - timedelta(minutes=4),
            completed_at=NOW - timedelta(minutes=3),
        )
        recovered = _save_restore_attempt(
            system.catalog,
            marker="new-v2-success",
            status="succeeded",
            started_at=NOW - timedelta(minutes=2),
            completed_at=NOW - timedelta(minutes=1),
            successful_evidence_source=system.restore_run,
        )

        check, selected = system.auditor()._restore_check(now=NOW)

        assert check.passed and check.blockers == ()
        assert selected == recovered
        assert check.evidence["run_id"] == recovered.run_id
    finally:
        system.close()


def test_latest_running_restore_attempt_after_crash_blocks_old_success(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        running = _save_restore_attempt(
            system.catalog,
            marker="crashed-running-attempt",
            status="running",
            started_at=NOW - timedelta(minutes=4),
            completed_at=None,
        )

        check, selected = system.auditor()._restore_check(now=NOW)

        assert selected is None
        assert check.blockers == (
            "physical_minio_restore_drill_latest_attempt_running",
        )
        assert check.evidence["latest_run_id"] == running.run_id
    finally:
        system.close()


def test_equal_started_restore_attempts_are_ambiguous(tmp_path: Path) -> None:
    system = AuditSystem(tmp_path)
    try:
        shared_started_at = NOW - timedelta(minutes=4)
        _save_restore_attempt(
            system.catalog,
            marker="equal-start-failed",
            status="failed",
            started_at=shared_started_at,
            completed_at=NOW - timedelta(minutes=3),
        )
        _save_restore_attempt(
            system.catalog,
            marker="equal-start-running",
            status="running",
            started_at=shared_started_at,
            completed_at=None,
        )

        check, selected = system.auditor()._restore_check(now=NOW)

        assert selected is None
        assert check.blockers == (
            "physical_minio_restore_drill_latest_attempt_ambiguous",
        )
        assert len(check.evidence["latest_run_ids"]) == 2
    finally:
        system.close()


def test_legacy_restore_success_requires_a_new_versioned_drill(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        assert system.restore_run is not None
        legacy = _save_legacy_restore_success(
            system.catalog,
            source=system.restore_run,
            started_at=NOW - timedelta(minutes=4),
            completed_at=NOW - timedelta(minutes=3),
        )

        check, selected = system.auditor()._restore_check(now=NOW)

        assert selected is None
        assert check.blockers == (
            "physical_minio_restore_drill_new_attempt_required",
        )
        assert check.evidence["latest_run_id"] == legacy.run_id
    finally:
        system.close()


def test_credential_check_discloses_operator_retention_without_claiming_rotation(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        system.evidence = replace(
            system.evidence,
            credential_retention_waivers=("tushare_token", "diemeng_api_key"),
        )

        check = system.auditor()._credential_rotation_check()

        assert check.passed
        assert check.evidence["credential_retention_waivers"] == [
            "tushare_token",
            "diemeng_api_key",
        ]
        assert check.evidence["all_required_credentials_vendor_rotated"] is False
        assert check.evidence["credential_use_decision"] == (
            "retained_unrotated_operator_accepted"
        )
    finally:
        system.close()


def test_dagster_postgresql_naive_heartbeat_timestamp_is_utc_only_for_dagster(
    tmp_path: Path,
) -> None:
    """Mirror PostgreSQL TIMESTAMP WITHOUT TIME ZONE result processing."""

    system = AuditSystem(tmp_path)
    completed_at = NOW - timedelta(minutes=21)
    try:
        with system.engine.begin() as connection:
            connection.execute(text("DROP TABLE daemon_heartbeats"))
            connection.execute(
                text(
                    "CREATE TABLE daemon_heartbeats ("
                    "daemon_type TEXT PRIMARY KEY, timestamp TIMESTAMP NOT NULL)"
                )
            )
            connection.execute(
                text(
                    "INSERT INTO daemon_heartbeats(daemon_type,timestamp) "
                    "VALUES ('SENSOR',:timestamp)"
                ),
                # PostgreSQL/Dagster returns this UTC value without tzinfo.
                {"timestamp": completed_at.replace(tzinfo=None)},
            )

        assert system.auditor()._dagster_heartbeat_matches(
            daemon_type="SENSOR",
            completed_at=completed_at,
            maximum_gap_seconds=1.0,
        )

        # The exception is not generalized to Research OS evidence parsing.
        from factor_lab.research_os import readiness_audit as readiness_module

        with pytest.raises(ValueError, match="timezone"):
            readiness_module._parse_time(completed_at.replace(tzinfo=None))
    finally:
        system.close()


def test_latest_readiness_audit_fails_closed_on_equal_database_timestamps(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        healthy = system.auditor().audit()
        assert healthy.ready

        system.evidence = _evidence(rotation_blocked=True)
        blocked = system.auditor().audit()
        assert blocked.audit_id != healthy.audit_id
        assert blocked.audited_at == healthy.audited_at
        with pytest.raises(ReadinessAuditError, match="ambiguous"):
            system.auditor().latest()
    finally:
        system.close()


def test_bootstrap_calendar_may_include_future_sessions_without_future_gold(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    future_sessions = ("2026-08-24", "2026-08-25", "2026-08-26")
    try:
        matrix, gold = system.auditor()._matrix_and_gold_checks(
            sessions=(*SESSIONS, *future_sessions),
            required_datasets=REQUIRED_DATASETS,
            now=NOW,
        )
        assert matrix.passed is True
        assert gold.passed is True
        assert matrix.evidence["required_through"] == SESSIONS[-1]
        assert matrix.evidence["completed_session_count"] == len(SESSIONS)
        assert matrix.evidence["future_calendar_session_count"] == len(
            future_sessions
        )
        assert matrix.evidence["missing_cell_count"] == 0
    finally:
        system.close()


@pytest.mark.parametrize(
    ("missing", "expected_status"),
    (
        ("canary", ProductionReadinessStatus.CONFIG_VALID_CANARY_PENDING),
        ("matrix", ProductionReadinessStatus.CANARY_READY),
        ("capability", ProductionReadinessStatus.BACKFILL_COMPLETE),
    ),
)
def test_readiness_status_ladder(
    tmp_path: Path,
    missing: str,
    expected_status: ProductionReadinessStatus,
) -> None:
    system = AuditSystem(tmp_path, missing=missing)
    try:
        assert system.auditor().audit().status is expected_status
    finally:
        system.close()


def test_missing_formal_oci_provenance_does_not_block_canary_stage(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path, missing="matrix")
    try:
        system.evidence = ProductionConfigEvidence(
            **{
                **system.evidence.__dict__,
                "provenance": SimpleNamespace(
                    formal_epoch_eligible=False,
                    architecture_version="research-os/v1",
                    code_hash="1" * 64,
                    configuration_hash="2" * 64,
                    dependency_lock_hash="3" * 64,
                    dirty_patch_hash="4" * 64,
                    build_identity_hash="8" * 64,
                    oci_image_id=None,
                    oci_repo_digests=(),
                    oci_base_digests=(),
                ),
                "readiness_blockers": (
                    "daemon_inspected_oci_provenance_missing",
                    "persisted_production_readiness_audit_missing",
                ),
            }
        )
        audit = system.auditor().audit()
        assert audit.status is ProductionReadinessStatus.CANARY_READY
        assert "daemon_inspected_oci_provenance_missing" in audit.blockers
    finally:
        system.close()


def test_latest_failed_host_attestation_attempt_blocks_fresh_old_success(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        proof = _save_host_attestation_proof(
            system.catalog,
            marker="old-fresh-success",
            started_at=NOW - timedelta(minutes=9),
            completed_at=NOW - timedelta(minutes=8),
        )
        _save_host_attestation_attempt(
            system.catalog,
            marker="old-success-attempt",
            status="succeeded",
            started_at=NOW - timedelta(minutes=9),
            completed_at=NOW - timedelta(minutes=7),
            attestation=proof,
        )
        failed = _save_host_attestation_attempt(
            system.catalog,
            marker="new-failure-without-runtime-fingerprint",
            status="failed",
            started_at=NOW - timedelta(minutes=2),
            completed_at=NOW - timedelta(minutes=1),
        )

        bound, blockers, evidence = (
            system.auditor()._latest_host_docker_attempt()
        )

        assert bound is None
        assert blockers == ("host_docker_attestation_latest_attempt_failed",)
        assert evidence["latest_host_attestation_attempt_run_id"] == failed.run_id
        assert "attestation_hash" not in failed.metadata
    finally:
        system.close()


def test_equal_started_host_attestation_attempts_are_ambiguous(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        started_at = NOW - timedelta(minutes=2)
        _save_host_attestation_attempt(
            system.catalog,
            marker="same-time-running",
            status="running",
            started_at=started_at,
            completed_at=None,
        )
        _save_host_attestation_attempt(
            system.catalog,
            marker="same-time-failed",
            status="failed",
            started_at=started_at,
            completed_at=NOW - timedelta(minutes=1),
        )

        bound, blockers, evidence = (
            system.auditor()._latest_host_docker_attempt()
        )

        assert bound is None
        assert blockers == ("host_docker_attestation_latest_attempt_ambiguous",)
        assert evidence["latest_host_attestation_attempt_count"] == 2
    finally:
        system.close()


def test_new_successful_host_attempt_restores_only_its_bound_attestation(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        _save_host_attestation_attempt(
            system.catalog,
            marker="prior-failure",
            status="failed",
            started_at=NOW - timedelta(minutes=5),
            completed_at=NOW - timedelta(minutes=4),
        )
        recovered_proof = _save_host_attestation_proof(
            system.catalog,
            marker="recovered-proof",
            started_at=NOW - timedelta(minutes=3),
            completed_at=NOW - timedelta(minutes=2),
        )
        recovered = _save_host_attestation_attempt(
            system.catalog,
            marker="recovered-success",
            status="succeeded",
            started_at=NOW - timedelta(minutes=3),
            completed_at=NOW - timedelta(minutes=1),
            attestation=recovered_proof,
        )

        bound, blockers, evidence = (
            system.auditor()._latest_host_docker_attempt()
        )

        assert blockers == ()
        assert bound == recovered_proof
        assert evidence["latest_host_attestation_attempt_run_id"] == recovered.run_id
        assert evidence["latest_host_attestation_bound_run_id"] == recovered_proof.run_id
    finally:
        system.close()


def test_postgresql_oci_check_cannot_bypass_latest_attempt_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        proof = _save_host_attestation_proof(
            system.catalog,
            marker="fresh-proof-that-must-not-fallback",
            started_at=NOW - timedelta(minutes=4),
            completed_at=NOW - timedelta(minutes=3),
        )
        auditor = system.auditor()
        monkeypatch.setattr(system.ledger.engine.dialect, "name", "postgresql")
        monkeypatch.setattr(
            auditor,
            "_latest_host_docker_attempt",
            lambda: (
                None,
                ("host_docker_attestation_latest_attempt_failed",),
                {"older_fresh_proof_run_id": proof.run_id},
            ),
        )

        check = auditor._oci_provenance_check()

        assert not check.passed
        assert check.blockers == (
            "host_docker_attestation_latest_attempt_failed",
        )
        assert check.evidence["older_fresh_proof_run_id"] == proof.run_id
        assert "host_attestation_run_id" not in check.evidence
    finally:
        system.close()


def test_audit_api_has_no_caller_pass_or_fact_override(tmp_path: Path) -> None:
    system = AuditSystem(tmp_path, missing="capability")
    try:
        auditor = system.auditor()
        with pytest.raises(TypeError):
            auditor.audit({"formal_execution_capability": "pass"})
        audit = auditor.audit()
        assert "formal_execution_pg_probe_missing" in audit.blockers
    finally:
        system.close()


@pytest.mark.parametrize(
    ("missing", "expected_blocker"),
    (
        ("schema_head", "alembic_version_missing"),
        ("calendar", "accepted_calendar_missing"),
        ("matrix", "required_dataset_stage_matrix_incomplete"),
        ("gold", "accepted_gold_missing_for_latest_session"),
        ("canary", "physical_engineering_canary_missing"),
        ("restore", "physical_minio_restore_drill_missing"),
        ("capability", "formal_execution_pg_probe_missing"),
        ("dagster", "dagster_materialization_ledger_missing"),
        ("soak", "dagster_code_location_24h_soak_missing"),
        ("rotation", "tushare_token_post_exposure_rotation_pending"),
    ),
)
def test_each_missing_persisted_fact_blocks_readiness(
    tmp_path: Path, missing: str, expected_blocker: str
) -> None:
    system = AuditSystem(tmp_path, missing=missing)
    try:
        audit = system.auditor().audit()
        assert audit.status is not ProductionReadinessStatus.FORMAL_EPOCH_READY
        assert expected_blocker in audit.blockers
    finally:
        system.close()


def test_config_declared_accepted_without_pg_probe_is_not_capability_evidence(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path, missing="capability")
    try:
        assert (
            system.config["daily"]["shadow"]["execution_market_data"][
                "formal_capability"
            ]["status"]
            == "accepted"
        )
        audit = system.auditor().audit()
        check = next(
            item for item in audit.checks if item.code == "formal_execution_capability"
        )
        assert not check.passed
        assert "formal_execution_pg_probe_missing" in check.blockers
    finally:
        system.close()


def test_legacy_missing_source_identity_is_compatibility_only_for_diemeng_config(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        check = next(
            item
            for item in system.auditor().audit().checks
            if item.code == "formal_execution_capability"
        )

        assert check.passed
        assert check.evidence["configured_semantics_source"] == "diemeng"
        assert check.evidence["legacy_identity_compatibility_used"] is True
    finally:
        system.close()


def test_current_config_change_invalidates_stale_typed_execution_capability(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        execution = system.config["daily"]["shadow"]["execution_market_data"]
        execution.update(
            {
                "source": "diemeng",
                "dataset": "minute_history",
                "collection_mode": "historical_query",
                "formal_capability": {
                    "status": "insufficient",
                    "formal_shadow_projection": "blocked",
                },
            }
        )

        audit = system.auditor().audit()
        check = next(
            item for item in audit.checks if item.code == "formal_execution_capability"
        )

        assert not check.passed
        assert (
            "formal_execution_production_configuration_hash_mismatch"
            in check.blockers
        )
        assert check.evidence["configured_source_dataset"] == "minute_history"
        assert check.evidence["configured_binding_is_authoritative"] is True
    finally:
        system.close()


def test_invalid_current_formal_source_mapping_fails_closed(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        request = system.config["daily"]["shadow"]["execution_market_data"][
            "request"
        ]
        request["page_size"] = 9_999

        check = next(
            item
            for item in system.auditor().audit().checks
            if item.code == "formal_execution_capability"
        )

        assert not check.passed
        assert "formal_execution_current_source_contract_invalid" in check.blockers
        assert "formal_execution_source_contract_hash_mismatch" in check.blockers
    finally:
        system.close()


@pytest.mark.parametrize(
    ("field", "replacement", "blocker"),
    (
        (
            "production_configuration_hash",
            None,
            "formal_execution_production_configuration_hash_mismatch",
        ),
        (
            "production_configuration_hash",
            "f" * 64,
            "formal_execution_production_configuration_hash_mismatch",
        ),
        (
            "source_contract_hash",
            None,
            "formal_execution_source_contract_hash_mismatch",
        ),
        (
            "source_contract_hash",
            "f" * 64,
            "formal_execution_source_contract_hash_mismatch",
        ),
    ),
)
def test_formal_capability_self_hash_cannot_authorize_missing_or_stale_binding(
    tmp_path: Path,
    field: str,
    replacement: str | None,
    blocker: str,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        with system.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT detail, fields_json, contract_hash "
                    "FROM ros_source_capabilities "
                    "WHERE source_id=:source_id AND dataset=:dataset"
                ),
                {
                    "source_id": FORMAL_EXECUTION_SOURCE_ID,
                    "dataset": TYPED_EXECUTION_CAPABILITY_DATASET,
                },
            ).mappings().one()
        detail = json.loads(str(row["detail"]))
        detail[field] = replacement
        raw_fields = row["fields_json"]
        fields = tuple(
            map(
                str,
                json.loads(raw_fields) if isinstance(raw_fields, str) else raw_fields,
            )
        )
        probe_hash = formal_execution_probe_hash(
            source_id=FORMAL_EXECUTION_SOURCE_ID,
            dataset=TYPED_EXECUTION_CAPABILITY_DATASET,
            contract_hash=str(row["contract_hash"]),
            fields=fields,
            detail=detail,
        )
        with system.engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE ros_source_capabilities SET detail=:detail, "
                    "probe_hash=:probe_hash WHERE source_id=:source_id "
                    "AND dataset=:dataset"
                ),
                {
                    "detail": json.dumps(
                        detail,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "probe_hash": probe_hash,
                    "source_id": FORMAL_EXECUTION_SOURCE_ID,
                    "dataset": TYPED_EXECUTION_CAPABILITY_DATASET,
                },
            )

        check = next(
            item
            for item in system.auditor().audit().checks
            if item.code == "formal_execution_capability"
        )

        assert not check.passed
        assert blocker in check.blockers
        assert "formal_execution_real_probe_hash_invalid" not in check.blockers
    finally:
        system.close()


def test_tushare_config_cannot_admit_rehashed_legacy_diemeng_identity(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        # Preserve the old, source-id-free Diemeng capability and its valid
        # physical authority chain, but switch the current configuration to
        # the reviewed Tushare route.  Rehashing every caller-visible field
        # must not let the legacy semantics impersonate the configured source.
        _configure_tushare_formal_execution(system.config)
        with system.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT detail, fields_json, contract_hash "
                    "FROM ros_source_capabilities "
                    "WHERE source_id=:source_id AND dataset=:dataset"
                ),
                {
                    "source_id": FORMAL_EXECUTION_SOURCE_ID,
                    "dataset": TYPED_EXECUTION_CAPABILITY_DATASET,
                },
            ).mappings().one()
        detail = json.loads(str(row["detail"]))
        assert detail.get("open_source_id") is None
        assert detail["event_semantics"] == "realtime_server_timed_open_09_30"
        detail["production_configuration_hash"] = (
            production_execution_configuration_hash(system.config)
        )
        detail["source_contract_hash"] = (
            production_formal_source_contract_hash(system.config)
        )
        raw_fields = row["fields_json"]
        fields = tuple(
            map(
                str,
                json.loads(raw_fields) if isinstance(raw_fields, str) else raw_fields,
            )
        )
        probe_hash = formal_execution_probe_hash(
            source_id=FORMAL_EXECUTION_SOURCE_ID,
            dataset=TYPED_EXECUTION_CAPABILITY_DATASET,
            contract_hash=str(row["contract_hash"]),
            fields=fields,
            detail=detail,
        )
        with system.engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE ros_source_capabilities SET detail=:detail, "
                    "probe_hash=:probe_hash WHERE source_id=:source_id "
                    "AND dataset=:dataset"
                ),
                {
                    "detail": json.dumps(
                        detail,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "probe_hash": probe_hash,
                    "source_id": FORMAL_EXECUTION_SOURCE_ID,
                    "dataset": TYPED_EXECUTION_CAPABILITY_DATASET,
                },
            )

        check = next(
            item
            for item in system.auditor().audit().checks
            if item.code == "formal_execution_capability"
        )

        assert not check.passed
        assert "formal_execution_semantics_unverified" in check.blockers
        assert (
            "formal_execution_production_configuration_hash_mismatch"
            not in check.blockers
        )
        assert "formal_execution_source_contract_hash_mismatch" not in check.blockers
        assert check.evidence["configured_semantics_source"] == "tushare"
        assert check.evidence["legacy_identity_compatibility_used"] is False
    finally:
        system.close()


@pytest.mark.parametrize(
    ("provider_endpoint", "expected_pass"),
    (("rt_min", True), ("rt_min_daily", False)),
)
def test_tushare_runtime_probe_chain_uses_only_formal_rt_min_semantics(
    tmp_path: Path,
    provider_endpoint: str,
    expected_pass: bool,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        _configure_tushare_formal_execution(system.config)
        with system.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT detail, fields_json, contract_hash "
                    "FROM ros_source_capabilities "
                    "WHERE source_id=:source_id AND dataset=:dataset"
                ),
                {
                    "source_id": FORMAL_EXECUTION_SOURCE_ID,
                    "dataset": TYPED_EXECUTION_CAPABILITY_DATASET,
                },
            ).mappings().one()
        detail = json.loads(str(row["detail"]))
        old_source = system.catalog.get_snapshot(detail["source_snapshot_id"])
        assert old_source is not None
        manifest = {
            **old_source.reference.manifest,
            "role": "tushare_open_observation",
            "source_id": "tushare",
        }
        source_hash = content_fingerprint(
            manifest, domain="test/readiness/tushare-open-snapshot"
        )
        source_ref = DataSnapshotRef(
            snapshot_id=f"tushare_open_{source_hash[:48]}",
            tier=SnapshotTier.BRONZE,
            uri=old_source.reference.uri.replace("diemeng", "tushare"),
            content_hash=source_hash,
            parent_snapshot_ids=old_source.reference.parent_snapshot_ids,
            as_of=old_source.reference.as_of,
            quality_status=DataQualityStatus.ACCEPTED,
            trust_labels=old_source.reference.trust_labels,
            manifest=manifest,
        )
        system.catalog.register_snapshot(source_ref)

        session = SESSIONS[-1]
        source_partition_hash = content_fingerprint(
            {"source_snapshot_id": source_ref.snapshot_id},
            domain="test/readiness/tushare-source-open-partition",
        )
        identity = PartitionIdentity("tushare", TYPED_OPEN_DATASET, session)
        system.ledger.ensure_partition(
            identity,
            created_at=NOW - timedelta(hours=2),
            input_hash="1" * 64,
        )
        lease = system.ledger.claim(
            identity=identity,
            owner="tushare-live-open-fixture",
            now=NOW - timedelta(hours=2),
            lease_for=timedelta(hours=3),
        )
        assert lease is not None
        system.ledger.finish(
            lease,
            status=PartitionStatus.SUCCEEDED,
            completed_at=NOW - timedelta(hours=1),
            output_snapshot_id=source_ref.snapshot_id,
            output_hash=source_partition_hash,
            details={
                "physical": True,
                "source_id": "tushare",
                "database_received_at": "2026-08-21T01:31:00+00:00",
                "collector_clock_skew_seconds": 0.25,
                "collector_clock_verified": True,
                "complete_observed_universe": True,
            },
        )

        detail.update(
            {
                "open_source_id": "tushare",
                "open_source_role": "tushare_open_observation",
                "provider_endpoint": provider_endpoint,
                "event_semantics": (
                    "official_realtime_current_session_1min_open_09_30"
                ),
                "server_available_at_verified": False,
                "provider_event_time_verified": True,
                "collector_received_at_verified": True,
                "collector_clock_verified_against_postgresql": True,
                "database_received_at": "2026-08-21T01:31:00+00:00",
                "available_at_field": "collector_ingested_at",
                "observation_deadline_local_time": "09:35:00",
                "open_price_reconciled_with_closed_daily": True,
                "complete_observed_universe": True,
                "source_snapshot_id": source_ref.snapshot_id,
                "source_snapshot_hash": source_ref.content_hash,
                "source_partition_hash": source_partition_hash,
                "production_configuration_hash": (
                    production_execution_configuration_hash(system.config)
                ),
                "source_contract_hash": (
                    production_formal_source_contract_hash(system.config)
                ),
            }
        )
        fields = tuple(FORMAL_EXECUTION_REQUIRED_FIELDS)
        probe_hash = formal_execution_probe_hash(
            source_id=FORMAL_EXECUTION_SOURCE_ID,
            dataset=TYPED_EXECUTION_CAPABILITY_DATASET,
            contract_hash=TYPED_EXECUTION_CONTRACT_HASH,
            fields=fields,
            detail=detail,
        )
        with system.engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE ros_source_capabilities SET detail=:detail, "
                    "probe_hash=:probe_hash WHERE source_id=:source_id "
                    "AND dataset=:dataset"
                ),
                {
                    "detail": json.dumps(detail, sort_keys=True, separators=(",", ":")),
                    "probe_hash": probe_hash,
                    "source_id": FORMAL_EXECUTION_SOURCE_ID,
                    "dataset": TYPED_EXECUTION_CAPABILITY_DATASET,
                },
            )
        check = next(
            item
            for item in system.auditor().audit().checks
            if item.code == "formal_execution_capability"
        )
        assert check.passed is expected_pass
        if expected_pass:
            assert check.evidence["authority_chain"]["open_source_id"] == "tushare"
        else:
            assert "formal_execution_semantics_unverified" in check.blockers
    finally:
        system.close()


@pytest.mark.parametrize(
    "variant",
    (
        "synthetic",
        "stale",
        "fake_hash",
        "missing_stage",
        "forged_session_hash",
        "wrong_evaluator_build",
        "cross_restore",
        "missing_snapshot_partition",
        "wrong_snapshot_hash",
        "wrong_snapshot_role",
        "duplicate_snapshot_partition",
        "missing_execution_audit_column",
        "invalid_opening_audit",
        "tampered_opening_audit",
        "wrong_dq_hash",
        "tampered_dq_report",
        "invalid_dq_status",
        "invalid_dq_reconciliation",
        "missing_opening_probe",
        "wrong_opening_probe_contract",
    ),
)
def test_synthetic_stale_or_forged_canary_never_counts_as_physical(
    tmp_path: Path, variant: str
) -> None:
    system = AuditSystem(tmp_path, canary_variant=variant)
    try:
        audit = system.auditor().audit()
        check = next(
            item for item in audit.checks if item.code == "physical_engineering_canary"
        )
        assert not check.passed
        assert check.blockers == ("physical_engineering_canary_missing",)
        if variant == "synthetic":
            assert check.evidence["synthetic_runs_rejected"] == 1
    finally:
        system.close()


def test_newer_current_window_failed_attempt_blocks_older_success(
    tmp_path: Path,
) -> None:
    # The older invocation starts first but legitimately completes after the
    # newer failed attempt.  Model that timing at initial creation so the test
    # never mutates an immutable terminal run.
    system = AuditSystem(
        tmp_path,
        canary_completed_at=NOW - timedelta(minutes=1),
    )
    try:
        older = system.catalog.get_run("physical-canary-real-01")
        assert older is not None and older.status == "succeeded"
        failed_run_id = "physical-canary-current-window-failed"
        failed_fingerprint = content_fingerprint(
            {"run_id": failed_run_id, "attempt": 2},
            domain="test/physical-canary/failed-attempt",
        )
        system.catalog.save_run(
            RunRecord(
                run_id=failed_run_id,
                run_type=PHYSICAL_CANARY_RUN_TYPE,
                status="failed",
                input_fingerprint=failed_fingerprint,
                started_at=NOW - timedelta(minutes=5),
                completed_at=NOW - timedelta(minutes=4),
                metadata={
                    "evidence_schema": PHYSICAL_CANARY_SCHEMA_VERSION,
                    "evaluator_identity": older.metadata["evaluator_identity"],
                    "evidence_class": "engineering_canary",
                    "evidence_scope": "retrospective_non_forward",
                    "canary_execution_contract_hash": (
                        CANARY_EXECUTION_CONTRACT_HASH
                    ),
                    "formal_epoch_eligible": False,
                    "physical_source_attested": True,
                    "controlled_test_adapter": False,
                    "readiness_admission": "physical_engineering_prerequisite",
                    "run_id": failed_run_id,
                    "run_type": PHYSICAL_CANARY_RUN_TYPE,
                    "input_fingerprint": failed_fingerprint,
                    "calendar_sessions": list(CANARY_SESSIONS),
                    "failure_type": "PhysicalCanaryDataRejected",
                },
                error="data rejected",
            )
        )

        check = next(
            item
            for item in system.auditor().audit().checks
            if item.code == "physical_engineering_canary"
        )

        assert not check.passed
        assert check.evidence["latest_eligible_attempt_run_id"] == failed_run_id
        assert len(check.evidence["inspected_runs"]) == 1
        assert check.evidence["inspected_runs"][0]["run_id"] == failed_run_id
        assert "not_succeeded" in check.evidence["inspected_runs"][0]["reasons"]
    finally:
        system.close()


def test_related_open_incident_after_success_prevents_stale_success_fallback(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        run = system.catalog.get_run("physical-canary-real-01")
        assert run is not None and run.completed_at is not None
        partition_id = str(run.metadata["partition_run_ids"][0])
        system.ledger.record_incident(
            partition_key=CANARY_SESSIONS[-1],
            stage=IncidentStage.GOLD,
            error_code="gold_market_semantics_rejected",
            message="new failure after the prior canary",
            occurred_at=NOW - timedelta(minutes=5),
            partition_run_id=partition_id,
            payload={
                "evidence_class": "engineering_canary",
                "readiness_admission": "physical_engineering_prerequisite",
                "run_id": run.run_id,
            },
        )

        check = next(
            item
            for item in system.auditor().audit().checks
            if item.code == "physical_engineering_canary"
        )

        assert not check.passed
        inspected = check.evidence["inspected_runs"][0]
        assert "related_incident_open" in inspected["reasons"]
        assert inspected["related_incidents"][0]["status"] == "open"
    finally:
        system.close()


def test_older_related_open_incident_cannot_be_hidden_by_ten_thousand_newer_rows(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        run = system.catalog.get_run("physical-canary-real-01")
        assert run is not None
        partition_id = str(run.metadata["partition_run_ids"][0])
        related = system.ledger.record_incident(
            partition_key=CANARY_SESSIONS[-1],
            stage=IncidentStage.GOLD,
            error_code="gold_market_semantics_rejected",
            message="older directly bound failure must remain visible",
            occurred_at=run.started_at - timedelta(hours=1),
            partition_run_id=partition_id,
            payload={
                "evidence_class": "engineering_canary",
                "readiness_admission": "physical_engineering_prerequisite",
                "run_id": run.run_id,
            },
        )
        incident_table = Base.metadata.tables["ros_data_incidents"]
        noise_rows = []
        for index in range(10_000):
            incident_hash = f"{index:064x}"
            noise_rows.append(
                {
                    "incident_id": f"incident_{incident_hash}",
                    "incident_hash": incident_hash,
                    "partition_run_id": None,
                    "partition_key": "2099-01-01",
                    "stage": IncidentStage.SOURCE.value,
                    "status": IncidentStatus.OPEN.value,
                    "error_code": "unrelated_source_noise",
                    "message": "unrelated newer incident",
                    "source_ids_json": ["unrelated_source"],
                    "evidence_hashes_json": [],
                    "payload_json": {"sequence": index},
                    "occurred_at": NOW + timedelta(seconds=index),
                    "resolved_at": None,
                    "resolution_hash": None,
                }
            )
        with system.engine.begin() as connection:
            connection.execute(incident_table.insert(), noise_rows)
        incident_selects = 0

        def count_incident_selects(
            _connection,
            _cursor,
            statement,
            _parameters,
            _context,
            _executemany,
        ) -> None:
            nonlocal incident_selects
            if "FROM ros_data_incidents" in statement:
                incident_selects += 1

        event.listen(system.engine, "before_cursor_execute", count_incident_selects)
        try:
            check = next(
                item
                for item in system.auditor().audit().checks
                if item.code == "physical_engineering_canary"
            )
        finally:
            event.remove(
                system.engine,
                "before_cursor_execute",
                count_incident_selects,
            )

        assert not check.passed
        inspected_run = check.evidence["inspected_runs"][0]
        assert "related_incident_open" in inspected_run["reasons"]
        assert inspected_run["related_incidents"] == [
            {
                "incident_id": related.incident_id,
                "status": IncidentStatus.OPEN.value,
                "partition_key": related.partition_key,
                "reason": "related_incident_open",
            }
        ]
        assert incident_selects == 1
    finally:
        system.close()


def test_canary_window_must_equal_latest_21_accepted_calendar_sessions(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        next_session = "2026-08-24"
        _save_run(system.catalog, "physical-dagster-2026-08-24")
        identity = PartitionIdentity(
            "research_os", "accepted_trade_calendar", next_session
        )
        system.ledger.ensure_partition(
            identity,
            created_at=NOW - timedelta(minutes=4),
            input_hash="e" * 64,
        )
        lease = system.ledger.claim(
            identity=identity,
            owner="calendar-new-session",
            now=NOW - timedelta(minutes=4),
            lease_for=timedelta(minutes=10),
        )
        assert lease is not None
        system.ledger.finish(
            lease,
            status=PartitionStatus.SUCCEEDED,
            completed_at=NOW - timedelta(minutes=3),
            run_id="physical-dagster-2026-08-24",
            output_snapshot_id=system.calendar_silver.snapshot_id,
            output_hash="f" * 64,
            details={"accepted_calendar": {"partition_key": next_session}},
        )

        check = next(
            item
            for item in system.auditor().audit().checks
            if item.code == "physical_engineering_canary"
        )

        assert not check.passed
        assert check.evidence["current_calendar_window"][-1] == next_session
        assert check.evidence["latest_eligible_attempt_run_id"] is None
        rejected = {
            item["run_id"]: item["reasons"]
            for item in check.evidence["rejected_attempts"]
        }
        assert "calendar_window_not_current" in rejected["physical-canary-real-01"]
    finally:
        system.close()


def _canary_partition(
    system: AuditSystem,
    *,
    dataset: str,
    partition_key: str,
    role: str = "",
):
    run = system.catalog.get_run("physical-canary-real-01")
    assert run is not None
    claimed = set(map(str, run.metadata["partition_run_ids"]))
    matches = [
        item
        for item in system.ledger.list_partitions(limit=100_000)
        if item.identity.partition_run_id in claimed
        and item.identity.dataset == dataset
        and item.identity.partition_key == partition_key
        and (not role or item.details.get("role") == role)
    ]
    assert len(matches) == 1
    return matches[0]


def _historical_generation_partition(
    system: AuditSystem,
    *,
    source_id: str,
    dataset: str,
    partition_key: str,
    role: str,
    run_id: str,
    completed_at: datetime,
):
    system.catalog.save_run(
        RunRecord(
            run_id=run_id,
            run_type=PHYSICAL_CANARY_RUN_TYPE,
            status="succeeded",
            input_fingerprint=content_fingerprint(
                {"run_id": run_id}, domain="test/readiness/historical-generation-run"
            ),
            started_at=completed_at - timedelta(hours=1),
            completed_at=completed_at,
            metadata={"physical_source_attested": True},
        )
    )
    tier = SnapshotTier.GOLD if role in {"mark", "execution"} else SnapshotTier.SILVER
    reference, _evidence = _physical_canary_snapshot(
        tier,
        run_id=run_id,
        role=role,
        trade_date=partition_key,
    )
    system.catalog.register_snapshot(reference)
    identity = PartitionIdentity(source_id, dataset, partition_key)
    partition_input_hash = content_fingerprint(
        {"source_id": source_id, "dataset": dataset, "partition_key": partition_key},
        domain="test/readiness/historical-generation-input",
    )
    stage = "gold" if tier is SnapshotTier.GOLD else "silver"
    claim_lineage, claim_lineage_hash = _physical_partition_claim_lineage(
        identity,
        input_hash=partition_input_hash,
        stage=stage,
        role=role,
    )
    system.ledger.ensure_partition(
        identity,
        created_at=completed_at - timedelta(hours=1),
        input_hash=partition_input_hash,
    )
    lease = system.ledger.claim(
        identity=identity,
        owner=f"historical-{source_id}",
        now=completed_at - timedelta(minutes=30),
        lease_for=timedelta(hours=1),
    )
    assert lease is not None
    return system.ledger.finish(
        lease,
        status=PartitionStatus.SUCCEEDED,
        completed_at=completed_at,
        run_id=run_id,
        output_snapshot_id=reference.snapshot_id,
        output_hash=reference.content_hash,
        details={
            "evidence_schema": PHYSICAL_CANARY_SCHEMA_VERSION,
            "physical_source_attested": True,
            "controlled_test_adapter": False,
            "run_id": run_id,
            "stage": stage,
            "role": role,
            "claim_lineage": claim_lineage,
            "claim_lineage_hash": claim_lineage_hash,
        },
    )


def _historical_generation_incident(
    system: AuditSystem,
    *,
    original: PartitionIdentity,
    replacement,
    resolved_at: datetime,
):
    original_record = system.ledger.get_partition(original)
    assert original_record is not None
    stage = {
        "bronze": IncidentStage.SOURCE,
        "silver": IncidentStage.SILVER,
        "data_quality": IncidentStage.DATA_QUALITY,
        "gold": IncidentStage.GOLD,
    }.get(str(original_record.details.get("stage") or ""), IncidentStage.SILVER)
    incident = system.ledger.record_incident(
        partition_key=original.partition_key,
        stage=stage,
        error_code="legacy_canary_generation_isolated",
        message="historical generation replaced by verified successor",
        occurred_at=original_record.updated_at,
        partition_run_id=original.partition_run_id,
        source_ids=(original.source_id,),
        payload={
            "legacy_source_id": original.source_id,
            "legacy_status": original_record.status.value,
            "current_source_id": replacement.identity.source_id,
            "dataset": original.dataset,
        },
    )
    return system.ledger.resolve_incident(
        incident.incident_id,
        resolved_at=resolved_at,
        evidence={
            "disposition": "superseded_by_verified_canary_generation",
            "legacy_partition_run_id": original.partition_run_id,
            "legacy_source_id": original.source_id,
            "current_source_id": replacement.identity.source_id,
            "replacement_partition_run_id": replacement.identity.partition_run_id,
            "replacement_output_snapshot_id": replacement.output_snapshot_id,
            "replacement_output_hash": replacement.output_hash,
        },
        superseded=True,
    )


def _tamper_catalog_snapshot_reference(
    system: AuditSystem,
    monkeypatch,
    *,
    snapshot_id: str,
    field: str,
) -> None:
    get_snapshot = system.catalog.get_snapshot

    def tampered_get_snapshot(requested_snapshot_id: str):
        record = get_snapshot(requested_snapshot_id)
        if record is None or requested_snapshot_id != snapshot_id:
            return record
        if field == "as_of":
            reference = record.reference.model_copy(
                update={"as_of": record.reference.as_of + timedelta(microseconds=1)}
            )
        elif field == "trust_labels":
            reference = record.reference.model_copy(
                update={
                    "trust_labels": (
                        "controlled_test_adapter",
                        "retrospective_non_forward",
                        "retrospective_physical_replay",
                    )
                }
            )
        else:  # pragma: no cover - test helper contract
            raise AssertionError(f"unsupported snapshot tamper: {field}")
        return replace(record, reference=reference)

    monkeypatch.setattr(system.catalog, "get_snapshot", tampered_get_snapshot)


def test_historical_a_to_b_resolution_accepts_unique_current_c_successor(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        run = system.catalog.get_run("physical-canary-real-01")
        assert run is not None
        dataset = "silver_accepted"
        partition_key = CANARY_SESSIONS[-1]
        original = PartitionIdentity("engineering_canary_generation_a", dataset, partition_key)
        system.ledger.ensure_partition(
            original,
            created_at=run.started_at - timedelta(hours=2),
            input_hash="a" * 64,
            details={"stage": "silver"},
        )
        generation_b = _historical_generation_partition(
            system,
            source_id="engineering_canary_generation_b",
            dataset=dataset,
            partition_key=partition_key,
            role="accepted",
            run_id="historical-generation-b",
            completed_at=run.started_at - timedelta(minutes=30),
        )
        closed = _historical_generation_incident(
            system,
            original=original,
            replacement=generation_b,
            resolved_at=run.started_at - timedelta(minutes=20),
        )

        check = next(
            item
            for item in system.auditor().audit().checks
            if item.code == "physical_engineering_canary"
        )

        assert check.passed
        inspected = next(
            item
            for item in check.evidence["related_incidents"]
            if item["incident_id"] == closed.incident_id
        )
        assert inspected["reason"] is None
    finally:
        system.close()


@pytest.mark.parametrize("tampered_field", ["as_of", "trust_labels"])
def test_historical_bridge_rejects_tampered_generation_b_reference_binding(
    tmp_path: Path,
    monkeypatch,
    tampered_field: str,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        run = system.catalog.get_run("physical-canary-real-01")
        assert run is not None
        dataset = "silver_accepted"
        partition_key = CANARY_SESSIONS[-1]
        original = PartitionIdentity(
            "engineering_canary_generation_a", dataset, partition_key
        )
        system.ledger.ensure_partition(
            original,
            created_at=run.started_at - timedelta(hours=2),
            input_hash="a" * 64,
            details={"stage": "silver"},
        )
        generation_b = _historical_generation_partition(
            system,
            source_id="engineering_canary_generation_b",
            dataset=dataset,
            partition_key=partition_key,
            role="accepted",
            run_id="historical-generation-b",
            completed_at=run.started_at - timedelta(minutes=30),
        )
        _historical_generation_incident(
            system,
            original=original,
            replacement=generation_b,
            resolved_at=run.started_at - timedelta(minutes=20),
        )
        assert generation_b.output_snapshot_id is not None
        _tamper_catalog_snapshot_reference(
            system,
            monkeypatch,
            snapshot_id=generation_b.output_snapshot_id,
            field=tampered_field,
        )

        check = next(
            item
            for item in system.auditor().audit().checks
            if item.code == "physical_engineering_canary"
        )

        assert not check.passed
        assert "related_incident_resolution_not_causal" in check.evidence[
            "inspected_runs"
        ][0]["reasons"]
    finally:
        system.close()


@pytest.mark.parametrize("tampered_field", ["as_of", "trust_labels"])
def test_current_canary_output_rejects_tampered_reference_binding(
    tmp_path: Path,
    monkeypatch,
    tampered_field: str,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        current = _canary_partition(
            system,
            dataset=f"gold_execution_{CANARY_SESSIONS[1]}",
            partition_key=CANARY_SESSIONS[1],
            role="execution",
        )
        assert current.output_snapshot_id is not None
        _tamper_catalog_snapshot_reference(
            system,
            monkeypatch,
            snapshot_id=current.output_snapshot_id,
            field=tampered_field,
        )
        auditor = system.auditor()

        assert not auditor._physical_canary_partition_output_is_valid(current)
        check = next(
            item
            for item in auditor.audit().checks
            if item.code == "physical_engineering_canary"
        )
        assert not check.passed
        assert "physical_snapshot_closure_invalid" in check.evidence[
            "inspected_runs"
        ][0]["reasons"]
    finally:
        system.close()


def test_historical_resolution_without_current_successor_remains_blocking(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        run = system.catalog.get_run("physical-canary-real-01")
        assert run is not None
        original = PartitionIdentity(
            "engineering_canary_generation_a", "obsolete_silver", CANARY_SESSIONS[-1]
        )
        system.ledger.ensure_partition(
            original,
            created_at=run.started_at - timedelta(hours=2),
            input_hash="a" * 64,
            details={"stage": "silver"},
        )
        generation_b = _historical_generation_partition(
            system,
            source_id="engineering_canary_generation_b",
            dataset=original.dataset,
            partition_key=original.partition_key,
            role="accepted",
            run_id="historical-obsolete-generation-b",
            completed_at=run.started_at - timedelta(minutes=30),
        )
        _historical_generation_incident(
            system,
            original=original,
            replacement=generation_b,
            resolved_at=run.started_at - timedelta(minutes=20),
        )

        check = next(
            item
            for item in system.auditor().audit().checks
            if item.code == "physical_engineering_canary"
        )

        assert not check.passed
        assert "related_incident_resolution_not_causal" in check.evidence[
            "inspected_runs"
        ][0]["reasons"]
    finally:
        system.close()


def test_historical_resolution_with_bad_current_successor_remains_blocking(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path, canary_variant="wrong_snapshot_hash")
    try:
        run = system.catalog.get_run("physical-canary-real-01")
        assert run is not None
        current = next(
            item
            for item in system.ledger.list_partitions(limit=100_000)
            if item.run_id == run.run_id
            and item.details.get("role") == "execution"
            and item.identity.partition_key == CANARY_SESSIONS[1]
        )
        original = PartitionIdentity(
            "engineering_canary_generation_a",
            current.identity.dataset,
            current.identity.partition_key,
        )
        system.ledger.ensure_partition(
            original,
            created_at=run.started_at - timedelta(hours=2),
            input_hash="a" * 64,
            details={"stage": "gold", "role": "execution"},
        )
        generation_b = _historical_generation_partition(
            system,
            source_id="engineering_canary_generation_b",
            dataset=original.dataset,
            partition_key=original.partition_key,
            role="execution",
            run_id="historical-execution-generation-b",
            completed_at=run.started_at - timedelta(minutes=30),
        )
        _historical_generation_incident(
            system,
            original=original,
            replacement=generation_b,
            resolved_at=run.started_at - timedelta(minutes=20),
        )

        check = next(
            item
            for item in system.auditor().audit().checks
            if item.code == "physical_engineering_canary"
        )

        assert not check.passed
        assert "related_incident_resolution_not_causal" in check.evidence[
            "inspected_runs"
        ][0]["reasons"]
    finally:
        system.close()


def test_invalid_duplicate_current_successor_cannot_be_filtered_out(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        run = system.catalog.get_run("physical-canary-real-01")
        assert run is not None and run.completed_at is not None
        current = _canary_partition(
            system,
            dataset="silver_accepted",
            partition_key=CANARY_SESSIONS[-1],
        )
        original = PartitionIdentity(
            "engineering_canary_generation_a",
            current.identity.dataset,
            current.identity.partition_key,
        )
        system.ledger.ensure_partition(
            original,
            created_at=run.started_at - timedelta(hours=2),
            input_hash="a" * 64,
            details={"stage": "silver"},
        )
        generation_b = _historical_generation_partition(
            system,
            source_id="engineering_canary_generation_b",
            dataset=original.dataset,
            partition_key=original.partition_key,
            role="accepted",
            run_id="historical-duplicate-generation-b",
            completed_at=run.started_at - timedelta(minutes=30),
        )
        _historical_generation_incident(
            system,
            original=original,
            replacement=generation_b,
            resolved_at=run.started_at - timedelta(minutes=20),
        )

        duplicate_identity = PartitionIdentity(
            "engineering_canary_duplicate_current",
            current.identity.dataset,
            current.identity.partition_key,
        )
        duplicate_input_hash = "b" * 64
        duplicate_lineage, duplicate_lineage_hash = _physical_partition_claim_lineage(
            duplicate_identity,
            input_hash=duplicate_input_hash,
            stage="silver",
        )
        system.ledger.ensure_partition(
            duplicate_identity,
            created_at=run.started_at + timedelta(minutes=1),
            input_hash=duplicate_input_hash,
        )
        lease = system.ledger.claim(
            identity=duplicate_identity,
            owner="duplicate-current-successor",
            now=run.started_at + timedelta(minutes=2),
            lease_for=timedelta(hours=2),
        )
        assert lease is not None
        duplicate = system.ledger.finish(
            lease,
            status=PartitionStatus.SUCCEEDED,
            completed_at=run.started_at + timedelta(minutes=3),
            run_id=run.run_id,
            output_snapshot_id=current.output_snapshot_id,
            output_hash="f" * 64,
            details={
                "run_id": run.run_id,
                "stage": "silver",
                "claim_lineage": duplicate_lineage,
                "claim_lineage_hash": duplicate_lineage_hash,
            },
        )
        synthetic_run = replace(
            run,
            metadata={
                **run.metadata,
                "partition_run_ids": [
                    *run.metadata["partition_run_ids"],
                    duplicate.identity.partition_run_id,
                ],
            },
        )
        partitions = {
            item.identity.partition_run_id: item
            for item in system.ledger.list_partitions(limit=100_000)
        }

        errors, _inspected = system.auditor()._physical_canary_incident_errors(
            run=synthetic_run,
            calendar_window=CANARY_SESSIONS,
            partitions=partitions,
        )

        assert errors == ("related_incident_resolution_not_causal",)
    finally:
        system.close()


def _legacy_bridge_fixture(system: AuditSystem, *, canonical: bool):
    run = system.catalog.get_run("physical-canary-real-01")
    assert run is not None and run.completed_at is not None
    current = _canary_partition(
        system,
        dataset="silver_accepted",
        partition_key=CANARY_SESSIONS[-1],
    )
    original = PartitionIdentity(
        "engineering_canary_generation_a",
        current.identity.dataset,
        current.identity.partition_key,
    )
    original_record = system.ledger.ensure_partition(
        original,
        created_at=run.started_at - timedelta(hours=2),
        input_hash="a" * 64,
        details={"stage": "silver"},
    )
    old = system.ledger.record_incident(
        partition_key=original.partition_key,
        stage=IncidentStage.SILVER,
        error_code="legacy_canary_generation_isolated",
        message="legacy contract generation",
        occurred_at=run.started_at - timedelta(hours=1),
        partition_run_id=original.partition_run_id,
        source_ids=(original.source_id,),
        payload={"dataset": original.dataset, "current_source_id": "generation_b"},
    )
    old = system.ledger.resolve_incident(
        old.incident_id,
        resolved_at=run.started_at - timedelta(minutes=30),
        evidence={"disposition": "superseded_contract_generation"},
        superseded=True,
    )
    bridge = system.ledger.record_incident(
        partition_key=original.partition_key,
        stage=IncidentStage.SILVER,
        error_code="legacy_canary_generation_isolated",
        message="current exact generation bridge",
        occurred_at=original_record.updated_at,
        partition_run_id=original.partition_run_id,
        source_ids=(original.source_id,),
        payload={
            "legacy_source_id": original.source_id,
            "legacy_status": original_record.status.value,
            "current_source_id": current.identity.source_id,
            "dataset": original.dataset,
        },
    )
    assert current.completed_at is not None
    resolution = {
        "disposition": (
            "superseded_by_verified_canary_generation"
            if canonical
            else "manual_exact_generation_bridge"
        ),
        "replacement_partition_run_id": current.identity.partition_run_id,
        "replacement_output_snapshot_id": current.output_snapshot_id,
        "replacement_output_hash": current.output_hash,
    }
    if canonical:
        resolution.update(
            {
                "legacy_partition_run_id": original.partition_run_id,
                "legacy_source_id": original.source_id,
                "current_source_id": current.identity.source_id,
            }
        )
    bridge = system.ledger.resolve_incident(
        bridge.incident_id,
        resolved_at=current.completed_at + timedelta(minutes=1),
        evidence=resolution,
        superseded=True,
    )
    return run, current, original, old, bridge


def test_historical_missing_replacement_can_use_immutable_current_bridge(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        _legacy_bridge_fixture(system, canonical=True)

        check = next(
            item
            for item in system.auditor().audit().checks
            if item.code == "physical_engineering_canary"
        )

        assert check.passed
        assert all(
            item["reason"] is None for item in check.evidence["related_incidents"]
        )
    finally:
        system.close()


def test_manual_exact_bridge_cannot_supersede_historical_incident(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        _legacy_bridge_fixture(system, canonical=False)

        check = next(
            item
            for item in system.auditor().audit().checks
            if item.code == "physical_engineering_canary"
        )

        assert not check.passed
        assert "related_incident_resolution_not_causal" in check.evidence[
            "inspected_runs"
        ][0]["reasons"]
    finally:
        system.close()


@pytest.mark.parametrize("hash_field", ["incident_hash", "resolution_hash"])
def test_historical_bridge_rejects_tampered_authority_hash(
    tmp_path: Path,
    monkeypatch,
    hash_field: str,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        run, _current, _original, old, bridge = _legacy_bridge_fixture(
            system, canonical=True
        )
        tampered_bridge = replace(bridge, **{hash_field: "0" * 64})
        monkeypatch.setattr(
            system.ledger,
            "iter_incidents",
            lambda **_kwargs: (old, tampered_bridge),
        )
        partitions = {
            item.identity.partition_run_id: item
            for item in system.ledger.list_partitions(limit=100_000)
        }

        errors, _inspected = system.auditor()._physical_canary_incident_errors(
            run=run,
            calendar_window=CANARY_SESSIONS,
            partitions=partitions,
        )

        assert errors == ("related_incident_resolution_not_causal",)
    finally:
        system.close()


def test_incident_index_scales_linearly_for_656_historical_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        run, _current, _original, old, bridge = _legacy_bridge_fixture(
            system, canonical=True
        )
        incidents = (*((old,) * 655), bridge)
        monkeypatch.setattr(
            system.ledger,
            "iter_incidents",
            lambda **_kwargs: incidents,
        )
        get_snapshot = system.catalog.get_snapshot
        lookup_count = 0

        def counted_get_snapshot(snapshot_id):
            nonlocal lookup_count
            lookup_count += 1
            return get_snapshot(snapshot_id)

        monkeypatch.setattr(system.catalog, "get_snapshot", counted_get_snapshot)
        partitions = {
            item.identity.partition_run_id: item
            for item in system.ledger.list_partitions(limit=100_000)
        }

        errors, inspected = system.auditor()._physical_canary_incident_errors(
            run=run,
            calendar_window=CANARY_SESSIONS,
            partitions=partitions,
        )

        claimed_count = len(set(map(str, run.metadata["partition_run_ids"])))
        assert errors == ()
        assert len(inspected) == 656
        assert lookup_count <= claimed_count + 1
    finally:
        system.close()


def test_incident_resolved_after_canary_completion_cannot_revalidate_stale_run(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        run = system.catalog.get_run("physical-canary-real-01")
        assert run is not None and run.completed_at is not None
        replacement = _canary_partition(
            system,
            dataset="silver_accepted",
            partition_key=CANARY_SESSIONS[-1],
        )
        incident = system.ledger.record_incident(
            partition_key=replacement.identity.partition_key,
            stage=IncidentStage.SILVER,
            error_code="legacy_canary_generation_isolated",
            message="failure after the canary was already complete",
            occurred_at=run.completed_at + timedelta(minutes=1),
            partition_run_id=replacement.identity.partition_run_id,
            source_ids=(replacement.identity.source_id,),
            payload={
                "evidence_class": "engineering_canary",
                "dataset": replacement.identity.dataset,
                "run_id": run.run_id,
            },
        )
        system.ledger.resolve_incident(
            incident.incident_id,
            resolved_at=run.completed_at + timedelta(minutes=2),
            evidence={
                "disposition": "late_manual_revalidation",
                "replacement_partition_run_id": replacement.identity.partition_run_id,
                "replacement_output_snapshot_id": replacement.output_snapshot_id,
                "replacement_output_hash": replacement.output_hash,
            },
            superseded=True,
        )

        check = next(
            item
            for item in system.auditor().audit().checks
            if item.code == "physical_engineering_canary"
        )

        assert not check.passed
        assert "related_incident_resolution_not_causal" in check.evidence[
            "inspected_runs"
        ][0]["reasons"]
    finally:
        system.close()


def test_causal_partition_recovery_restores_current_canary_readiness(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        run = system.catalog.get_run("physical-canary-real-01")
        assert run is not None and run.completed_at is not None
        partition_id = str(run.metadata["partition_run_ids"][0])
        partitions = {
            item.identity.partition_run_id: item
            for item in system.ledger.list_partitions(limit=100_000)
        }
        replacement = partitions[partition_id]
        assert replacement.completed_at is not None
        original = PartitionIdentity(
            "engineering_canary_generation_before_current",
            replacement.identity.dataset,
            replacement.identity.partition_key,
        )
        system.ledger.ensure_partition(
            original,
            created_at=NOW - timedelta(minutes=90),
            input_hash="a" * 64,
            details={
                "stage": replacement.details.get("stage"),
                "role": replacement.details.get("role"),
            },
        )
        incident = _historical_generation_incident(
            system,
            original=original,
            replacement=replacement,
            resolved_at=NOW - timedelta(minutes=59),
        )

        check = next(
            item
            for item in system.auditor().audit().checks
            if item.code == "physical_engineering_canary"
        )

        assert check.passed
        assert check.evidence["related_incidents"] == [
            {
                "incident_id": incident.incident_id,
                "status": "superseded",
                "partition_key": replacement.identity.partition_key,
                "reason": None,
            }
        ]
    finally:
        system.close()


def test_noncausal_incident_resolution_cannot_restore_old_success(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path)
    try:
        run = system.catalog.get_run("physical-canary-real-01")
        assert run is not None
        partition_id = str(run.metadata["partition_run_ids"][0])
        incident = system.ledger.record_incident(
            partition_key=CANARY_SESSIONS[-1],
            stage=IncidentStage.GOLD,
            error_code="gold_market_semantics_rejected",
            message="failure cannot be closed by unrelated evidence",
            occurred_at=NOW - timedelta(minutes=90),
            partition_run_id=partition_id,
            payload={
                "evidence_class": "engineering_canary",
                "readiness_admission": "physical_engineering_prerequisite",
                "run_id": run.run_id,
            },
        )
        system.ledger.resolve_incident(
            incident.incident_id,
            resolved_at=NOW - timedelta(minutes=59),
            evidence={
                "disposition": "manual_resolution_without_replacement",
                "replacement_partition_run_id": "partition_" + "0" * 64,
                "replacement_output_snapshot_id": "unrelated",
                "replacement_output_hash": "0" * 64,
            },
        )

        check = next(
            item
            for item in system.auditor().audit().checks
            if item.code == "physical_engineering_canary"
        )

        assert not check.passed
        inspected = check.evidence["inspected_runs"][0]
        assert "related_incident_resolution_not_causal" in inspected["reasons"]
    finally:
        system.close()


def test_soak_aggregate_without_independent_health_samples_is_rejected(
    tmp_path: Path,
) -> None:
    system = AuditSystem(tmp_path, missing="soak")
    try:
        soak = {
            "schema_version": DAGSTER_CODE_LOCATION_SOAK_SCHEMA_VERSION,
            "authority": "caller_asserted_aggregate",
            "physical": True,
            "service_name": "dagster-code-server",
            "code_location": "factor_lab_research_os",
            "heartbeat_source": "dagster_postgresql",
            "daemon_type": "SENSOR",
            "health_sample_count": 145,
            "maximum_sample_gap_seconds": 600,
            "restart_count": 0,
            "process_identity": "caller-asserted-process",
            "health_sample_hash": "c" * 64,
            "build_identity_hash": system.evidence.provenance.build_identity_hash,
            "oci_image_id": system.evidence.provenance.oci_image_id,
        }
        soak_hash = dagster_code_location_soak_evidence_hash(soak)
        soak["soak_evidence_hash"] = soak_hash
        system.catalog.save_run(
            RunRecord(
                run_id=f"soak_{soak_hash}",
                run_type=DAGSTER_CODE_LOCATION_SOAK_RUN_TYPE,
                status="succeeded",
                input_fingerprint=soak_hash,
                started_at=NOW - timedelta(hours=24, minutes=20),
                completed_at=NOW - timedelta(minutes=20),
                metadata=soak,
            )
        )

        audit = system.auditor().audit()

        assert "dagster_code_location_24h_soak_missing" in audit.blockers
    finally:
        system.close()


def test_multiple_current_accepted_gold_heads_fail_closed(tmp_path: Path) -> None:
    system = AuditSystem(tmp_path)
    try:
        alternate = _snapshot(
            SnapshotTier.GOLD,
            parents=(system.silvers[SESSIONS[-1]].snapshot_id,),
            as_of=system.gold.as_of,
        )
        assert alternate.snapshot_id != system.gold.snapshot_id
        system.catalog.register_snapshot(alternate)

        audit = system.auditor().audit()

        assert "accepted_gold_current_head_not_unique_or_unbound" in audit.blockers
    finally:
        system.close()


@pytest.fixture
def dagster_materialization_factory(tmp_path: Path):
    resources = []

    def factory():
        database = tmp_path / f"dagster-materialization-{len(resources)}.db"
        url = f"sqlite+pysqlite:///{database.as_posix()}"
        engine = create_engine(url)
        Base.metadata.create_all(engine)
        catalog = ResearchCatalog(url)
        catalog.initialize_schema()
        ledger = ProductionLedger(engine)
        with engine.begin() as connection:
            connection.execute(
                text("CREATE TABLE runs (run_id TEXT PRIMARY KEY, status TEXT NOT NULL)")
            )
            connection.execute(
                text(
                    "CREATE TABLE event_logs ("
                    "id INTEGER PRIMARY KEY, run_id TEXT NOT NULL, "
                    "dagster_event_type TEXT NOT NULL)"
                )
            )
        auditor = ProductionReadinessAuditor(
            catalog,
            ledger,
            config=_config(),
            config_evidence=_evidence(),
        )
        resources.append((catalog, ledger, engine))
        return auditor, catalog, ledger, engine

    yield factory
    for catalog, ledger, engine in resources:
        ledger.close()
        catalog.close()
        engine.dispose()


def _link_materialization_parent(
    catalog: ResearchCatalog,
    ledger: ProductionLedger,
    *,
    parent_run_id: str,
    run_type: str,
    status: str,
    metadata: dict,
) -> None:
    prefix, _, suffix = parent_run_id.partition("_")
    if (
        prefix in {"roscal", "rosop"}
        and len(suffix) == 48
        and all(character in "0123456789abcdef" for character in suffix)
    ):
        input_fingerprint = f"{suffix}{'0' * 16}"
    else:
        input_fingerprint = content_fingerprint(
            {"parent_run_id": parent_run_id},
            domain="test/readiness/dagster-bridge-parent",
        )
    catalog.save_run(
        RunRecord(
            run_id=parent_run_id,
            run_type=run_type,
            status=status,
            input_fingerprint=input_fingerprint,
            started_at=NOW - timedelta(hours=2),
            completed_at=NOW - timedelta(hours=1),
            metadata=metadata,
        )
    )
    _finish_partition(
        ledger,
        dataset="stage_source",
        partition_key="2026-08-21",
        run_id=parent_run_id,
        result=_operation_result("source_sync", {"source_count": 1}),
    )


def _insert_dagster_fact(
    engine,
    *,
    raw_run_id: str,
    status: str = "SUCCESS",
    event_type: str | None = "ASSET_MATERIALIZATION",
) -> None:
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO runs(run_id,status) VALUES (:run_id,:status)"),
            {"run_id": raw_run_id, "status": status},
        )
        if event_type is not None:
            connection.execute(
                text(
                    "INSERT INTO event_logs(run_id,dagster_event_type) "
                    "VALUES (:run_id,:event_type)"
                ),
                {"run_id": raw_run_id, "event_type": event_type},
            )


def test_dagster_materialization_accepts_legacy_raw_uuid_link(
    dagster_materialization_factory,
) -> None:
    auditor, catalog, ledger, engine = dagster_materialization_factory()
    raw_run_id = "legacy-raw-dagster-uuid"
    _link_materialization_parent(
        catalog,
        ledger,
        parent_run_id=raw_run_id,
        run_type="dagster_physical_run_anchor",
        status="succeeded",
        metadata={"physical": True},
    )
    _insert_dagster_fact(engine, raw_run_id=raw_run_id)

    check = auditor._dagster_materialization_check()

    assert check.passed
    assert check.evidence["matched_raw_dagster_run_ids"] == [raw_run_id]
    assert check.evidence["bridge_parent_run_ids"] == []


def test_dagster_materialization_resolves_roscal_retry_list(
    dagster_materialization_factory,
) -> None:
    auditor, catalog, ledger, engine = dagster_materialization_factory()
    parent_run_id = f"roscal_{'a' * 48}"
    failed_raw_id = "calendar-retry-failed"
    succeeded_raw_id = "calendar-retry-succeeded"
    _link_materialization_parent(
        catalog,
        ledger,
        parent_run_id=parent_run_id,
        run_type="dagster_calendar_bootstrap",
        status="succeeded",
        metadata={
            "dagster_run_id": succeeded_raw_id,
            "dagster_run_ids": sorted([failed_raw_id, succeeded_raw_id]),
        },
    )
    _insert_dagster_fact(
        engine,
        raw_run_id=failed_raw_id,
        status="FAILURE",
    )
    _insert_dagster_fact(engine, raw_run_id=succeeded_raw_id)

    check = auditor._dagster_materialization_check()

    assert check.passed
    assert check.evidence["matched_raw_dagster_run_ids"] == [succeeded_raw_id]
    assert check.evidence["bridge_parent_run_ids"] == [parent_run_id]


def test_dagster_materialization_resolves_rosop_scalar(
    dagster_materialization_factory,
) -> None:
    auditor, catalog, ledger, engine = dagster_materialization_factory()
    parent_run_id = f"rosop_{'b' * 48}"
    raw_run_id = "daily-source-materialization"
    _link_materialization_parent(
        catalog,
        ledger,
        parent_run_id=parent_run_id,
        run_type="dagster:daily:source_sync",
        status="completed",
        metadata={"dagster_run_id": raw_run_id},
    )
    _insert_dagster_fact(engine, raw_run_id=raw_run_id)

    check = auditor._dagster_materialization_check()

    assert check.passed
    assert check.evidence["matched_raw_dagster_run_ids"] == [raw_run_id]
    assert check.evidence["bridge_parent_run_ids"] == [parent_run_id]


@pytest.mark.parametrize(
    ("parent_run_id", "run_type", "status"),
    (
        (f"roscal_{'c' * 48}", "dagster_calendar_bootstrap", "failed"),
        (f"roscal_{'d' * 48}", "dagster:daily:source_sync", "succeeded"),
        (f"untrusted_{'e' * 48}", "dagster_calendar_bootstrap", "succeeded"),
    ),
    ids=("wrong-status", "wrong-type", "wrong-prefix"),
)
def test_dagster_bridge_rejects_untrusted_parent_contract(
    dagster_materialization_factory,
    parent_run_id: str,
    run_type: str,
    status: str,
) -> None:
    auditor, catalog, ledger, engine = dagster_materialization_factory()
    raw_run_id = "otherwise-valid-materialization"
    _link_materialization_parent(
        catalog,
        ledger,
        parent_run_id=parent_run_id,
        run_type=run_type,
        status=status,
        metadata={
            "dagster_run_id": raw_run_id,
            "dagster_run_ids": [raw_run_id],
        },
    )
    _insert_dagster_fact(engine, raw_run_id=raw_run_id)

    check = auditor._dagster_materialization_check()

    assert not check.passed
    assert check.blockers == ("real_dagster_materialization_missing",)
    assert check.evidence["matched_raw_dagster_run_ids"] == []
    assert check.evidence["bridge_parent_run_ids"] == []


@pytest.mark.parametrize(
    ("dagster_status", "event_type"),
    (
        ("FAILURE", "ASSET_MATERIALIZATION"),
        ("SUCCESS", "STEP_SUCCESS"),
    ),
    ids=("failed-run", "no-materialization"),
)
def test_dagster_materialization_requires_success_and_materialization_event(
    dagster_materialization_factory,
    dagster_status: str,
    event_type: str,
) -> None:
    auditor, catalog, ledger, engine = dagster_materialization_factory()
    raw_run_id = "legacy-raw-rejected"
    _link_materialization_parent(
        catalog,
        ledger,
        parent_run_id=raw_run_id,
        run_type="dagster_physical_run_anchor",
        status="succeeded",
        metadata={"physical": True},
    )
    _insert_dagster_fact(
        engine,
        raw_run_id=raw_run_id,
        status=dagster_status,
        event_type=event_type,
    )

    check = auditor._dagster_materialization_check()

    assert not check.passed
    assert check.blockers == ("real_dagster_materialization_missing",)


def test_dagster_bridge_rejects_nonexistent_raw_uuid(
    dagster_materialization_factory,
) -> None:
    auditor, catalog, ledger, _ = dagster_materialization_factory()
    parent_run_id = f"rosop_{'f' * 48}"
    _link_materialization_parent(
        catalog,
        ledger,
        parent_run_id=parent_run_id,
        run_type="dagster:daily:source_sync",
        status="completed",
        metadata={"dagster_run_id": "missing-from-dagster-ledger"},
    )

    check = auditor._dagster_materialization_check()

    assert not check.passed
    assert check.blockers == ("real_dagster_materialization_missing",)
    assert check.evidence["bridge_parent_run_ids"] == []
