"""`factor-lab` command line interface for the research-only operating system."""

from __future__ import annotations

import argparse
from dataclasses import asdict, fields
from datetime import date, datetime, time, timedelta, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence
from uuid import uuid4

from .catalog import LifecycleEvent, ResearchCatalog
from .build_provenance import (
    SOURCE_BUNDLE_MANIFEST_ENV,
    EpochBuildProvenance,
    SourceBundleProvenanceError,
    bind_verified_oci_deployment,
    capture_epoch_provenance,
)
from .contracts import (
    ExperimentSpec,
    LifecycleState,
    RecoveryCaseStatus,
)
from .coordinator_cli import (
    coordinator_propose,
    coordinator_resume,
    coordinator_status,
    coordinator_submit,
)
from .cycle import HistoricalResearchCycle, field_specs_from_mapping
from .data_sync import read_frame, sync_bronze
from .legacy import import_legacy_evidence
from .legacy_bronze_seed import LegacyExpandedBronzeSeeder
from .lifecycle import (
    LifecycleTransition,
    SleeveHealthObservation,
    SleeveLifecycleRecord,
    SleeveState,
)
from .monitor import LifecycleMonitor
from .object_store import S3ImmutableArchive
from .production_config import (
    ORCHESTRATION_CONFIG_ENV,
    PRODUCTION_DATA_ROOT,
    ProductionConfigurationError,
    ProductionOperation,
    admit_production_operation,
    validate_production_config,
)
from .production_ledger import ProductionLedger, load_runtime_authority_marker
from .readiness_audit import (
    READINESS_AUDIT_RUN_TYPE,
    ProductionReadinessAudit,
    ProductionReadinessAuditor,
    ReadinessAuditError,
)
from .orchestration import CycleName, OperationName, OperationRequest
from .runtime import DoctorCheck, DoctorReport, ResearchOSSettings, RunCoordinator, doctor
from .snapshot_service import publish_cataloged_snapshot


def _json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _emit(payload: Any) -> None:
    if hasattr(payload, "to_dict"):
        payload = payload.to_dict()
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str, sort_keys=True))


def _settings(args: argparse.Namespace) -> ResearchOSSettings:
    settings = ResearchOSSettings.from_env()
    if args.database_url:
        settings = type(settings)(
            **{**settings.__dict__, "database_url": args.database_url}
        )
    return settings


def _database_connect_args(settings: ResearchOSSettings) -> dict[str, Any] | None:
    resolver = getattr(settings, "database_connect_args", None)
    if not callable(resolver):
        return None
    return resolver() or None


def _new_catalog(settings: ResearchOSSettings) -> ResearchCatalog:
    connect_args = _database_connect_args(settings)
    if connect_args is None:
        return ResearchCatalog(settings.database_url)
    return ResearchCatalog(settings.database_url, connect_args=connect_args)


def _new_production_ledger(settings: ResearchOSSettings) -> ProductionLedger:
    connect_args = _database_connect_args(settings)
    if connect_args is None:
        return ProductionLedger(settings.database_url)
    return ProductionLedger(settings.database_url, connect_args=connect_args)


def _catalog(settings: ResearchOSSettings) -> ResearchCatalog:
    catalog = _new_catalog(settings)
    catalog.initialize_schema()
    return catalog


def _object_store_archive(settings: ResearchOSSettings) -> S3ImmutableArchive:
    return S3ImmutableArchive.from_connection(
        endpoint=settings.object_store_endpoint,
        bucket=settings.object_store_bucket,
        access_key=settings.object_store_access_key,
        secret_key=settings.object_store_secret_key,
    )


def _is_production(settings: ResearchOSSettings) -> bool:
    if settings.environment.strip().lower() == "production":
        return True
    if not settings.uses_postgresql:
        return False
    connect_args = _database_connect_args(settings)
    marker = (
        load_runtime_authority_marker(settings.database_url)
        if connect_args is None
        else load_runtime_authority_marker(
            settings.database_url,
            connect_args=connect_args,
        )
    )
    return bool(marker is not None and marker.is_production)


def _require_production(settings: ResearchOSSettings, command: str) -> None:
    if not _is_production(settings):
        raise ValueError(
            f"{command} is an authoritative command; "
            "set FACTOR_LAB_ENVIRONMENT=production"
        )


def _reject_file_input_in_production(
    settings: ResearchOSSettings,
    *,
    command: str,
) -> None:
    if _is_production(settings):
        raise ValueError(
            f"production {command} rejects caller-supplied experiment, data, "
            "statistics, monitor, and market-bar files"
        )


def _authoritative_services():
    # Import lazily so parser/help and explicit legacy/test commands do not
    # initialize the production object-store or orchestration stack.
    from .application_services import create_services

    return create_services()


def _close_services(services: Any) -> None:
    close = getattr(getattr(services, "catalog", None), "close", None)
    if callable(close):
        close()


def _daily_request(
    partition_key: str,
    operation: OperationName,
) -> OperationRequest:
    return OperationRequest(
        operation=operation,
        cycle=CycleName.DAILY,
        partition_key=partition_key,
        # A stable run identity lets independently invoked sync/publish/shadow
        # commands hydrate the same immutable operation chain.  The PG
        # partition ledger still owns leases and terminal de-duplication.
        run_id=f"production-cli-daily:{partition_key}",
        metadata={"authority": "production_cli"},
    )


def _accepted_range(services: Any, start: date, end: date) -> tuple[str, ...]:
    if start > end:
        raise ValueError("--from cannot be after --to")
    ledger = getattr(services, "production_ledger", None)
    if ledger is None:
        raise ValueError("authoritative backfill requires the PostgreSQL partition ledger")
    accepted = tuple(
        value
        for value in ledger.accepted_calendar_partitions()
        if start <= date.fromisoformat(value) <= end
    )
    if not accepted:
        raise ValueError(
            "no reconciled accepted-calendar sessions exist in the requested range; "
            "run the calendar bootstrap first"
        )
    return accepted


def _execute_daily_chain(
    services: Any,
    partition_key: str,
    operations: Sequence[OperationName],
    *,
    executor_name: str = "execute",
) -> list[dict[str, Any]]:
    executor = getattr(services, executor_name, None)
    if not callable(executor):
        raise ValueError(f"authoritative services do not expose {executor_name}")
    results: list[dict[str, Any]] = []
    for operation in operations:
        result = executor(_daily_request(partition_key, operation))
        outputs = dict(result.outputs)
        compact_outputs = {
            key: outputs[key]
            for key in (
                "snapshot_id",
                "silver_snapshot_id",
                "iceberg_snapshot_id",
                "iceberg_tag",
                "research_ready",
                "generation_reason",
                "error_type",
            )
            if key in outputs
        }
        if isinstance(outputs.get("sources"), Sequence) and not isinstance(
            outputs.get("sources"), (str, bytes)
        ):
            compact_outputs["source_count"] = len(outputs["sources"])
        results.append(
            {
                "operation": result.operation.value,
                "status": result.status,
                "summary": result.summary,
                "outputs": compact_outputs,
            }
        )
        if result.status != "completed":
            break
    return results


def _require_authoritative_backfill_admission(
    settings: ResearchOSSettings,
) -> Any:
    _require_production(settings, "authoritative historical backfill")
    config_value = str(os.environ.get(ORCHESTRATION_CONFIG_ENV) or "").strip()
    if not config_value:
        raise ValueError(
            f"set {ORCHESTRATION_CONFIG_ENV} for authoritative historical backfill"
        )
    evidence = validate_production_config(config_value)
    admission = admit_production_operation(
        evidence,
        ProductionOperation.AUTHORITATIVE_HISTORICAL_BACKFILL,
    )
    if not admission.allowed:
        raise ValueError(
            "authoritative historical backfill is blocked by production readiness: "
            + ", ".join(admission.blockers)
        )
    return evidence


def _prepare_legacy_bronze_seed(
    services: Any, *, through: date
) -> Mapping[str, Any]:
    """Prepare the fixed Bronze-only seed and canonical vendor gap ledger.

    No source path crosses this boundary.  The seeder resolves the reviewed
    production config against ``PRODUCTION_DATA_ROOT`` and therefore cannot be
    redirected by a CLI argument.
    """

    ledger = getattr(services, "production_ledger", None)
    archive = getattr(services, "object_store_archive", None)
    if ledger is None or archive is None:
        raise ValueError(
            "legacy Bronze seed preparation requires PostgreSQL and immutable MinIO"
        )
    environment_hashes = getattr(services, "_environment_hashes", None)
    if not isinstance(environment_hashes, Mapping):
        raise ValueError("legacy Bronze seed lacks measured environment hashes")
    settings = getattr(services, "settings", None)
    if settings is None:
        raise ValueError("legacy Bronze seed lacks authoritative runtime settings")
    preparation = LegacyExpandedBronzeSeeder(
        catalog=services.catalog,
        ledger=ledger,
        archive=archive,
        config=services.config,
        runtime_data_root=Path(PRODUCTION_DATA_ROOT),
        lake_root=settings.lake_root,
        snapshot_root=settings.snapshot_root,
        environment_hashes=environment_hashes,
    ).prepare(
        through=through,
        now=services.catalog.database_now(),
    )
    return preparation.to_dict()


def _authoritative_data_sync(args: argparse.Namespace, settings: ResearchOSSettings) -> int:
    _require_authoritative_backfill_admission(settings)
    if not args.date_from or not args.date_to:
        raise ValueError("authoritative data sync requires --from and --to")
    start = date.fromisoformat(str(args.date_from))
    end = date.fromisoformat(str(args.date_to))
    if start > end:
        raise ValueError("--from cannot be after --to")
    services = _authoritative_services()
    try:
        bootstrap = getattr(services, "bootstrap_accepted_calendar", None)
        if not callable(bootstrap):
            raise ValueError(
                "authoritative services do not expose the dual-source calendar bootstrap"
            )
        bootstrap_poll = bootstrap(
            exchange="SSE",
            source_start=date(2016, 6, 1),
            through=end,
            dagster_run_id=f"production-cli-calendar:{end.isoformat()}",
        )
        seed_preparation = _prepare_legacy_bronze_seed(services, through=end)
        partitions = _accepted_range(services, start, end)
        rows = [
            {
                "partition_key": partition,
                "operations": _execute_daily_chain(
                    services,
                    partition,
                    (OperationName.SOURCE_SYNC,),
                    executor_name="execute_authoritative_backfill",
                ),
            }
            for partition in partitions
        ]
    finally:
        _close_services(services)
    completed = all(
        row["operations"]
        and row["operations"][-1]["status"] == "completed"
        for row in rows
    )
    _emit(
        {
            "status": "completed" if completed else "blocked",
            "mode": "authoritative_postgresql_resume",
            "requested_resume": bool(args.resume),
            "resume_enforced": True,
            "calendar_bootstrap": {
                "message": str(getattr(bootstrap_poll, "message", "")),
                "cursor": getattr(bootstrap_poll, "cursor", None),
            },
            "legacy_bronze_seed": dict(seed_preparation),
            "partition_count": len(rows),
            "partitions": rows,
        }
    )
    return 0 if completed else 2


def _coordinated(
    catalog: ResearchCatalog,
    run_type: str,
    inputs: Mapping[str, Any],
    action,
):
    return RunCoordinator(catalog).execute(
        run_type,
        {**dict(inputs), "invocation_id": uuid4().hex},
        action,
    )


def _data_sync(args: argparse.Namespace) -> int:
    settings = _settings(args)
    if not args.spec:
        return _authoritative_data_sync(args, settings)
    _reject_file_input_in_production(settings, command="data sync --spec")
    spec = _json(args.spec)
    with _catalog(settings) as catalog:
        result = _coordinated(
            catalog,
            "data_sync",
            {"spec": spec},
            lambda: sync_bronze(
                spec,
                lake_root=args.lake_root or settings.lake_root,
                object_store_archive=_object_store_archive(settings),
            ),
        )
    _emit(result)
    return 0


def _snapshot_publish(args: argparse.Namespace) -> int:
    settings = _settings(args)
    if not args.spec:
        _require_authoritative_backfill_admission(settings)
        if not args.as_of:
            raise ValueError("authoritative snapshot publish requires --as-of")
        partition = date.fromisoformat(str(args.as_of)).isoformat()
        services = _authoritative_services()
        try:
            if partition not in _accepted_range(
                services,
                date.fromisoformat(partition),
                date.fromisoformat(partition),
            ):
                raise ValueError("--as-of is not an accepted trading session")
            results = _execute_daily_chain(
                services,
                partition,
                (
                    OperationName.SOURCE_SYNC,
                    OperationName.SOURCE_RECONCILIATION,
                    OperationName.DATA_QUALITY_GATE,
                    OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
                ),
                executor_name="execute_authoritative_backfill",
            )
        finally:
            _close_services(services)
        completed = bool(results) and results[-1]["status"] == "completed"
        _emit(
            {
                "status": "completed" if completed else "blocked",
                "mode": "authoritative_postgresql_gold_publish",
                "partition_key": partition,
                "operations": results,
            }
        )
        return 0 if completed else 2
    _reject_file_input_in_production(settings, command="snapshot publish --spec")
    spec = _json(args.spec)
    repository = Path(spec.get("repository") or Path.cwd()).resolve()
    with _catalog(settings) as catalog:
        result = _coordinated(
            catalog,
            "snapshot_publish",
            {"spec": spec},
            lambda: publish_cataloged_snapshot(
                catalog,
                paths=spec["paths"],
                base_dir=spec["base_dir"],
                snapshot_root=spec.get("snapshot_root") or settings.snapshot_root,
                tier=spec["tier"],
                as_of=spec["as_of"],
                parent_snapshot_ids=spec.get("parent_snapshot_ids") or (),
                quality_report=spec["quality_report"],
                trust_labels=spec.get("trust_labels") or (),
                repository=repository,
                dependency_lock=spec.get("dependency_lock")
                or repository / "uv.lock",
                configuration=spec.get("configuration") or spec,
                uri=spec.get("uri"),
                production=_is_production(settings),
            ),
        )
    _emit(result)
    return 0


def _research_cycle(args: argparse.Namespace) -> int:
    settings = _settings(args)
    _reject_file_input_in_production(settings, command="research cycle")
    experiment_payload = _json(args.experiment)
    spec = ExperimentSpec.model_validate(experiment_payload)
    frame = read_frame(args.data)
    field_payload = _json(args.fields) if args.fields else []
    controls = _json(args.negative_controls) if args.negative_controls else []
    with _catalog(settings) as catalog:
        result = _coordinated(
            catalog,
            "research_cycle",
            {"experiment_fingerprint": spec.fingerprint()},
            lambda: HistoricalResearchCycle(catalog).run(
                spec,
                frame,
                field_specs=field_specs_from_mapping(field_payload),
                sleeve_signal=args.sleeve_signal,
                negative_controls=controls,
                bootstrap_resamples=args.bootstrap_resamples,
                seed=args.seed,
            ),
        )
    _emit(result)
    return 0


class _CliProposalPort:
    """Narrow adapter for an LLM/process that emits only hypothesis + DSL."""

    def __init__(self, payload: Mapping[str, Any]) -> None:
        self.payload = dict(payload)

    def propose(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        # The deterministic coordinator owns and fingerprints ``context``;
        # this port cannot supply snapshots, statistics, weights or verdicts.
        del context
        return dict(self.payload)


def _proposal_payload(value: str) -> Mapping[str, Any]:
    if value == "-":
        payload = json.load(sys.stdin)
    else:
        payload = _json(value)
    if not isinstance(payload, Mapping):
        raise ValueError("research proposal must be a JSON object")
    return payload


def _coordinator_services(args: argparse.Namespace):
    settings = _settings(args)
    _require_production(settings, f"research {args.research_command}")
    if args.database_url:
        raise ValueError(
            "authoritative research coordinator rejects --database-url overrides; "
            "use the configured PostgreSQL fact source"
        )
    services = _authoritative_services()
    coordinator = getattr(services, "monthly_research", None)
    if coordinator is None:
        _close_services(services)
        raise ValueError("production services do not expose MonthlyResearchCoordinator")
    return services, coordinator


def _research_propose(args: argparse.Namespace) -> int:
    proposal = _proposal_payload(args.proposal)
    services, coordinator = _coordinator_services(args)
    try:
        result = coordinator_propose(
            coordinator,
            _CliProposalPort(proposal),
            family_id=args.family,
            recovery_case_id=args.recovery_case,
        )
    finally:
        _close_services(services)
    _emit(result)
    return 0 if result["accepted"] else 2


def _research_submit(args: argparse.Namespace) -> int:
    proposal = _proposal_payload(args.proposal)
    services, coordinator = _coordinator_services(args)
    try:
        result = coordinator_submit(
            coordinator,
            proposal,
            family_id=args.family,
            recovery_case_id=args.recovery_case,
        )
    finally:
        _close_services(services)
    _emit(result)
    return 0 if result["accepted"] else 2


def _research_status(args: argparse.Namespace) -> int:
    services, coordinator = _coordinator_services(args)
    try:
        result = coordinator_status(coordinator, args.submission_id)
    finally:
        _close_services(services)
    _emit(result)
    return 0


def _research_resume(args: argparse.Namespace) -> int:
    services, coordinator = _coordinator_services(args)
    try:
        result = coordinator_resume(
            coordinator,
            worker_id=args.worker_id,
            limit=args.limit,
            lease_seconds=args.lease_seconds,
        )
    finally:
        _close_services(services)
    _emit(result)
    terminal_failures = {
        "failed",
        "missing_data",
    }.intersection(
        str(item.get("status") or "") for item in result["submissions"]
    )
    return 2 if terminal_failures else 0


def _measured_epoch_context(
    args: argparse.Namespace, settings: ResearchOSSettings
) -> tuple[EpochBuildProvenance, Any | None]:
    config_value = str(
        getattr(args, "config", None)
        or os.environ.get(ORCHESTRATION_CONFIG_ENV)
        or ""
    ).strip()
    if not config_value:
        raise ValueError(
            f"set {ORCHESTRATION_CONFIG_ENV} or pass --config for epoch provenance"
        )
    config_path = Path(config_value).resolve()
    if _is_production(settings):
        if str(getattr(args, "image", None) or "").strip():
            raise ValueError(
                "production epoch/readiness commands reject caller-selected --image; "
                "use readiness attest-runtime on the deployment host"
            )
        evidence = validate_production_config(config_path)
        return evidence.provenance, evidence
    repository = Path(
        getattr(args, "repository", None) or Path.cwd()
    ).resolve()
    manifest = str(
        getattr(args, "manifest", None)
        or os.environ.get(SOURCE_BUNDLE_MANIFEST_ENV)
        or ""
    ).strip()
    return (
        capture_epoch_provenance(
            configuration_path=config_path,
            repository=repository,
            manifest_path=manifest or None,
        ),
        None,
    )


def _measured_epoch_provenance(
    args: argparse.Namespace, settings: ResearchOSSettings
) -> EpochBuildProvenance:
    """Compatibility seam retained for tests and non-production callers."""

    provenance, _ = _measured_epoch_context(args, settings)
    return provenance


def _latest_verified_readiness(
    settings: ResearchOSSettings,
) -> ProductionReadinessAudit | None:
    """Read the latest content-addressed PG audit without mutating schemas."""

    try:
        with _new_catalog(settings) as catalog:
            runs = catalog.list_runs(limit=1_000, run_type=READINESS_AUDIT_RUN_TYPE)
    except Exception:
        # Doctor/status must fail closed while a database is unavailable or
        # before its schema has been migrated; it must not initialize tables.
        return None
    audits: list[ProductionReadinessAudit] = []
    for run in runs:
        try:
            audits.append(ProductionReadinessAudit.from_run(run))
        except ReadinessAuditError:
            # A malformed newest row cannot manufacture readiness.  Older
            # independently valid audits remain visible for diagnosis.
            continue
    return max(audits, key=lambda item: (item.audited_at, item.audit_id), default=None)


def _readiness_check(
    audit: ProductionReadinessAudit | None,
    code: str,
) -> Any | None:
    if audit is None:
        return None
    return next((item for item in audit.checks if item.code == code), None)


def _readiness_matches_provenance(
    audit: ProductionReadinessAudit | None,
    provenance: EpochBuildProvenance,
) -> bool:
    """Bind a persisted verdict to the daemon-inspected release being frozen."""

    check = _readiness_check(audit, "daemon_inspected_oci_provenance")
    if audit is None or check is None or not check.passed:
        return False
    evidence = dict(check.evidence)
    expected_epoch = {
        "architecture_version": provenance.architecture_version,
        "code_hash": provenance.code_hash,
        "configuration_hash": provenance.configuration_hash,
        "dependency_lock_hash": provenance.dependency_lock_hash,
        "dirty_patch_hash": provenance.dirty_patch_hash,
    }
    observed_epoch = evidence.get("epoch_fields")
    return bool(
        isinstance(observed_epoch, Mapping)
        and all(observed_epoch.get(key) == value for key, value in expected_epoch.items())
        and evidence.get("build_identity_hash") == provenance.build_identity_hash
        and evidence.get("oci_image_id") == provenance.oci_image_id
    )


def _bind_readiness_deployment(
    source_provenance: EpochBuildProvenance,
    audit: ProductionReadinessAudit | None,
) -> EpochBuildProvenance:
    """Derive epoch provenance only from a verified readiness attestation."""

    check = _readiness_check(audit, "daemon_inspected_oci_provenance")
    if audit is None or check is None or not check.passed:
        raise ValueError("fresh host Docker readiness attestation is unavailable")
    observed = dict(check.evidence)
    epoch_fields = observed.get("epoch_fields")
    if not isinstance(epoch_fields, Mapping) or any(
        epoch_fields.get(name) != getattr(source_provenance, name)
        for name in (
            "architecture_version",
            "code_hash",
            "configuration_hash",
            "dependency_lock_hash",
        )
    ):
        raise ValueError("host Docker attestation belongs to another source release")
    try:
        bound = bind_verified_oci_deployment(
            source_provenance,
            oci_image_id=str(observed.get("oci_image_id") or ""),
            oci_repo_digests=tuple(
                map(str, observed.get("oci_repo_digests") or ())
            ),
            oci_base_digests=tuple(
                map(str, observed.get("oci_base_digests") or ())
            ),
        )
    except SourceBundleProvenanceError as exc:
        raise ValueError("host Docker deployment binding is invalid") from exc
    if (
        not bound.formal_epoch_eligible
        or observed.get("build_identity_hash") != bound.build_identity_hash
        or epoch_fields != bound.epoch_fields()
    ):
        raise ValueError("host Docker deployment identity is internally inconsistent")
    return bound


def _readiness_payload(
    evidence: Any | None,
    audit: ProductionReadinessAudit | None = None,
) -> dict[str, Any]:
    if evidence is None and audit is None:
        return {}
    static_blockers = list(getattr(evidence, "readiness_blockers", ()) or ())
    audit_blockers = list(audit.blockers) if audit is not None else [
        "persisted_production_readiness_audit_missing"
    ]
    execution_check = _readiness_check(audit, "formal_execution_capability")
    return {
        "readiness_status": (
            audit.status.value
            if audit is not None
            else getattr(evidence, "status", "config_valid_canary_pending")
        ),
        "readiness_audit_id": None if audit is None else audit.audit_id,
        "readiness_audited_at": (
            None if audit is None else audit.audited_at.isoformat()
        ),
        "formal_execution_capable": bool(
            execution_check is not None and execution_check.passed
        ),
        "historical_backfill_allowed": bool(
            getattr(evidence, "historical_backfill_allowed", False)
        ),
        "formal_forward_evidence": bool(audit is not None and audit.ready),
        "readiness_blockers": list(
            dict.fromkeys(audit_blockers if audit is not None else [*static_blockers, *audit_blockers])
        ),
    }


def _production_readiness_audit(args: argparse.Namespace) -> int:
    """Derive and persist readiness from PG/MinIO/Dagster facts only."""

    settings = _settings(args)
    _require_production(settings, "production readiness audit")
    _provenance, evidence = _measured_epoch_context(args, settings)
    if evidence is None:
        raise ValueError("production readiness audit requires validated production config")
    config = _json(evidence.path)
    catalog = _new_catalog(settings)
    ledger = _new_production_ledger(settings)
    try:
        audit = ProductionReadinessAuditor(
            catalog,
            ledger,
            config=config,
            config_evidence=evidence,
        ).audit()
    finally:
        ledger.close()
        catalog.close()
    _emit(audit)
    return 0 if audit.ready else 2


def _production_readiness_status(args: argparse.Namespace) -> int:
    settings = _settings(args)
    _require_production(settings, "production readiness status")
    audit = _latest_verified_readiness(settings)
    if audit is None:
        _emit(
            {
                "status": "config_valid_canary_pending",
                "ready": False,
                "audit_id": None,
                "blockers": ["persisted_production_readiness_audit_missing"],
            }
        )
        return 2
    _emit(audit)
    return 0 if audit.ready else 2


def _production_credential_use_attestation(args: argparse.Namespace) -> int:
    """Persist the validated execution credential decision without exposing it."""

    settings = _settings(args)
    _require_production(settings, "execution credential-use attestation")
    if args.database_url:
        raise ValueError(
            "execution credential-use attestation rejects --database-url overrides; "
            "use the configured production PostgreSQL authority"
        )
    services = _authoritative_services()
    try:
        authority = services._production_execution_snapshot_authority()
        attestation = authority.migrate_credential_use_evidence()
        source_id, dataset = authority.rotation_capability_identity
    finally:
        _close_services(services)
    _emit(
        {
            "status": "completed",
            "credential": attestation.credential,
            "disposition": attestation.disposition,
            "evidence_hash": attestation.evidence_hash,
            "recorded_at": attestation.confirmed_at.isoformat(),
            "capability": {"source_id": source_id, "dataset": dataset},
            "credential_material_recorded": False,
        }
    )
    return 0


def _host_runtime_attestation(args: argparse.Namespace) -> int:
    """Persist Docker facts selected only by the host daemon and fixed labels."""

    settings = _settings(args)
    _require_production(settings, "host Docker runtime attestation")
    if args.database_url:
        raise ValueError(
            "host runtime attestation rejects --database-url overrides; "
            "use the configured production PostgreSQL authority"
        )
    from .docker_attestation import HostDockerRuntimeAttestor

    with _catalog(settings) as catalog:
        result = HostDockerRuntimeAttestor.from_host(catalog=catalog).attest()
    _emit(result)
    return 0


def _physical_engineering_canary(args: argparse.Namespace) -> int:
    settings = _settings(args)
    _require_production(settings, "physical engineering canary")
    services = _authoritative_services()
    try:
        runner = getattr(services, "run_physical_engineering_canary", None)
        if not callable(runner):
            raise ValueError("authoritative services do not expose the physical canary")
        result = runner(
            as_of=(None if args.as_of is None else date.fromisoformat(args.as_of))
        )
    finally:
        _close_services(services)
    _emit(result)
    return 0


def _physical_minio_restore_drill(args: argparse.Namespace) -> int:
    settings = _settings(args)
    _require_production(settings, "physical MinIO restore drill")
    services = _authoritative_services()
    try:
        runner = getattr(services, "run_physical_minio_restore_drill", None)
        if not callable(runner):
            raise ValueError(
                "authoritative services do not expose the physical MinIO restore drill"
            )
        result = runner()
    finally:
        _close_services(services)
    _emit(result)
    return 0


def _epoch_payload(
    epoch: Any,
    provenance: EpochBuildProvenance,
    evidence: Any | None = None,
    audit: ProductionReadinessAudit | None = None,
) -> dict[str, Any]:
    actual = provenance.epoch_fields()
    persisted = {key: getattr(epoch, key) for key in actual}
    matches = actual == persisted
    return {
        **epoch.__dict__,
        "frozen_at": epoch.frozen_at.isoformat(),
        "first_forward_session": (
            None
            if epoch.first_forward_session is None
            else epoch.first_forward_session.isoformat()
        ),
        "activated_at": (
            None if epoch.activated_at is None else epoch.activated_at.isoformat()
        ),
        "closed_at": (
            None if epoch.closed_at is None else epoch.closed_at.isoformat()
        ),
        "lifecycle_status": epoch.lifecycle_status,
        "status": (
            "provenance_mismatch"
            if not matches
            else (
                "pending_trusted_forward_session"
                if epoch.first_forward_session is None
                else "forward_window_activated"
            )
        ),
        "current_provenance_matches": matches,
        "current_provenance": provenance.public_dict(),
        "readiness_matches_current_release": _readiness_matches_provenance(
            audit, provenance
        ),
        **_readiness_payload(evidence, audit),
    }


def _research_epoch_status(args: argparse.Namespace) -> int:
    settings = _settings(args)
    provenance, evidence = _measured_epoch_context(args, settings)
    audit = _latest_verified_readiness(settings) if evidence is not None else None
    if evidence is not None:
        try:
            provenance = _bind_readiness_deployment(provenance, audit)
        except ValueError:
            # Status remains read-only and reports the release mismatch below.
            pass
    with _catalog(settings) as catalog:
        epoch = catalog.get_evidence_epoch()
    if epoch is None:
        _emit(
            {
                "status": "not_frozen",
                "current_provenance_matches": None,
                "current_provenance": provenance.public_dict(),
                "warning": "no pristine forward evidence window exists",
                "readiness_matches_current_release": _readiness_matches_provenance(
                    audit, provenance
                ),
                **_readiness_payload(evidence, audit),
            }
        )
        return 2
    payload = _epoch_payload(epoch, provenance, evidence, audit)
    _emit(payload)
    ready = bool(payload["current_provenance_matches"])
    if evidence is not None:
        ready = ready and bool(
            audit is not None
            and audit.ready
            and _readiness_matches_provenance(audit, provenance)
        )
    return 0 if ready else 2


def _research_freeze_epoch(args: argparse.Namespace) -> int:
    """Freeze architecture provenance and optionally seal its forward day.

    The first forward session is never calculated from weekdays.  Activation
    delegates to the catalog verifier, which requires the first post-freeze
    session in an accepted, content-addressed immutable calendar authority
    snapshot. That authority may include future sessions without pretending
    that future daily/Gold partitions already exist.
    """

    settings = _settings(args)
    provenance, evidence = _measured_epoch_context(args, settings)
    if evidence is not None and str(getattr(args, "image", None) or "").strip():
        raise ValueError(
            "formal epoch freeze rejects caller-selected --image; "
            "run readiness attest-runtime on the deployment host"
        )
    if evidence is not None and hasattr(evidence, "path"):
        config = _json(evidence.path)
        catalog = _new_catalog(settings)
        ledger = _new_production_ledger(settings)
        try:
            audit = ProductionReadinessAuditor(
                catalog,
                ledger,
                config=config,
                config_evidence=evidence,
            ).audit()
            observed_now = catalog.database_now().astimezone(timezone.utc)
        finally:
            ledger.close()
            catalog.close()
        age = observed_now - audit.audited_at.astimezone(timezone.utc)
        if age.total_seconds() < 0 or age > timedelta(minutes=5):
            raise ValueError(
                "fresh production readiness audit is older than five minutes"
            )
        provenance = _bind_readiness_deployment(provenance, audit)
    elif evidence is not None:
        # Compatibility for diagnostic/test evidence objects.  They can never
        # manufacture a fresh audit, so the ordinary readiness gate below
        # remains closed.
        audit = _latest_verified_readiness(settings)
    else:
        audit = None
    if _is_production(settings) and not provenance.formal_epoch_eligible:
        raise ValueError(
            "formal production epoch freeze requires fresh host-attested OCI provenance"
        )
    if evidence is not None and not (
        audit is not None
        and audit.ready
        and _readiness_matches_provenance(audit, provenance)
    ):
        blockers = ", ".join(
            (audit.blockers if audit is not None else evidence.readiness_blockers)
        ) or "persisted production readiness audit does not match this release"
        raise ValueError(
            "formal production epoch freeze is blocked by production readiness: "
            + blockers
        )
    if bool(args.calendar_snapshot_id) != bool(args.first_forward_session):
        raise ValueError(
            "--calendar-snapshot-id and --first-forward-session must be supplied together"
        )
    measured = provenance.epoch_fields()
    with _catalog(settings) as catalog:
        active = catalog.get_evidence_epoch()
        pending = catalog.get_pending_evidence_epoch()

        def _matches(candidate: Any | None) -> bool:
            return candidate is not None and measured == {
                key: getattr(candidate, key) for key in measured
            }

        same_active_window = bool(
            args.calendar_snapshot_id
            and _matches(active)
            and active is not None
            and active.lifecycle_status == "active"
            and active.calendar_snapshot_id == args.calendar_snapshot_id
            and active.first_forward_session.isoformat()
            == str(args.first_forward_session)
        )
        if same_active_window:
            epoch = active
        elif _matches(pending):
            epoch = pending
        elif not args.calendar_snapshot_id and _matches(active):
            epoch = active
        else:
            # A code/config/image change, or a new immutable calendar
            # horizon/revision, begins a fresh append-only epoch. The active
            # pointer moves only after the successor window is verified.
            epoch = catalog.freeze_evidence_epoch(**measured)
        if args.calendar_snapshot_id:
            epoch = catalog.activate_evidence_epoch(
                calendar_snapshot_id=args.calendar_snapshot_id,
                first_forward_session=args.first_forward_session,
            )
    _emit(_epoch_payload(epoch, provenance, evidence, audit))
    return 0


def _lifecycle_record(payload: Mapping[str, Any]) -> SleeveLifecycleRecord:
    transitions = tuple(
        LifecycleTransition(
            from_state=SleeveState(str(row["from_state"])),
            to_state=SleeveState(str(row["to_state"])),
            as_of_date=date.fromisoformat(str(row["as_of_date"])),
            reasons=tuple(row.get("reasons") or ()),
        )
        for row in payload.get("transitions") or ()
    )
    names = {item.name for item in fields(SleeveLifecycleRecord)}
    values = {key: value for key, value in payload.items() if key in names}
    values["state"] = SleeveState(str(payload.get("state") or "proposed"))
    values["transitions"] = transitions
    if values.get("dormant_since"):
        values["dormant_since"] = date.fromisoformat(str(values["dormant_since"]))
    return SleeveLifecycleRecord(**values)


def _health_observation(payload: Mapping[str, Any]) -> SleeveHealthObservation:
    names = {item.name for item in fields(SleeveHealthObservation)}
    values = {key: value for key, value in payload.items() if key in names}
    values.pop("new_sessions_since_dormant", None)
    values["as_of_date"] = date.fromisoformat(str(payload["as_of_date"]))
    return SleeveHealthObservation(**values)


def _monitor_tick(args: argparse.Namespace) -> int:
    settings = _settings(args)
    if not args.input:
        _require_production(settings, "monitor tick")
        partition = date.fromisoformat(
            str(args.as_of or datetime.now(timezone.utc).date().isoformat())
        ).isoformat()
        services = _authoritative_services()
        try:
            resume_result = services.resume_pending_incident_controls(
                worker_id=f"monitor-tick:{partition}",
                limit=100,
            )
            results: list[dict[str, Any]] = [dict(resume_result)]
            for operation in (
                OperationName.SLEEVE_HEALTH_CHECK,
                OperationName.DRIFT_DETECTION,
                OperationName.LIFECYCLE_TRANSITION,
                OperationName.RECOVERY_SLA_CHECK,
            ):
                result = services.execute(
                    OperationRequest(
                        operation=operation,
                        cycle=CycleName.WEEKLY,
                        partition_key=partition,
                        run_id=f"production-cli-monitor:{partition}",
                        metadata={"authority": "production_cli"},
                    )
                )
                results.append(result.to_dict())
                if result.status not in {"completed", "skipped"}:
                    break
        finally:
            _close_services(services)
        completed = bool(results) and results[-1]["status"] in {
            "completed",
            "skipped",
        }
        _emit(
            {
                "status": "completed" if completed else "blocked",
                "mode": "authoritative_postgresql_event_chain",
                "as_of": partition,
                "operations": results,
            }
        )
        return 0 if completed else 2
    _reject_file_input_in_production(settings, command="monitor tick --input")
    payload = _json(args.input)
    with _catalog(settings) as catalog:
        seed = _lifecycle_record(payload["record"])
        record = next(
            (
                _lifecycle_record(event.evidence["record"])
                for event in catalog.list_lifecycle_events(
                    sleeve_id=seed.sleeve_id, limit=1000
                )
                if isinstance(event.evidence.get("record"), Mapping)
            ),
            SleeveLifecycleRecord(
                sleeve_id=seed.sleeve_id,
                state=seed.state,
                target_weight=seed.target_weight,
                effective_weight=seed.effective_weight,
            ),
        )
        observation = _health_observation(payload["observation"])
        shadow_account_id = (
            str(payload["shadow_account_id"])
            if payload.get("shadow_account_id")
            else None
        )
        shadow_sessions = 0
        if shadow_account_id and record.dormant_since is not None:
            shadow_sessions = catalog.count_shadow_sessions(
                account_id=shadow_account_id,
                since=record.dormant_since,
                through=observation.as_of_date,
            )
        observation = SleeveHealthObservation(
            **{
                **asdict(observation),
                "new_sessions_since_dormant": shadow_sessions,
            }
        )
        snapshot_id = str(payload["snapshot_id"])
        catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key=(
                    f"health:{record.sleeve_id}:{observation.as_of_date.isoformat()}:"
                    f"{snapshot_id}"
                ),
                sleeve_id=record.sleeve_id,
                from_state=LifecycleState(record.state.value),
                to_state=LifecycleState(record.state.value),
                cause="health_measurement_recorded",
                occurred_at=datetime.combine(
                    observation.as_of_date, time(14, 59), tzinfo=timezone.utc
                ),
                evidence={
                    "snapshot_id": snapshot_id,
                    "shadow_account_id": shadow_account_id,
                    "measurement": {
                        key: value
                        for key, value in asdict(observation).items()
                        if key != "new_sessions_since_dormant"
                    },
                    "measurement_kind": "raw_point_in_time",
                },
            )
        )
        active_recovery_cases = tuple(
            catalog.iter_recovery_cases(
                statuses=(
                    RecoveryCaseStatus.OPEN,
                    RecoveryCaseStatus.DIAGNOSING,
                    RecoveryCaseStatus.OBSERVING,
                ),
                sleeve_id=record.sleeve_id,
                batch_size=1_000,
            )
        )
        if len(active_recovery_cases) > 1:
            raise RuntimeError(
                f"multiple active recovery cases exist for Sleeve {record.sleeve_id!r}"
            )
        recovery = active_recovery_cases[0] if active_recovery_cases else None
        result = _coordinated(
            catalog,
            "monitor_tick",
            {
                "sleeve_id": record.sleeve_id,
                "as_of_date": observation.as_of_date,
                "snapshot_id": snapshot_id,
            },
            lambda: LifecycleMonitor(catalog).tick(
                record,
                observation,
                snapshot_id=snapshot_id,
                active_recovery_case=recovery,
                shadow_account_id=shadow_account_id,
                allow_projected_deadlines=True,
            ),
        )
    _emit(result)
    return 0


def _shadow_step(args: argparse.Namespace) -> int:
    from .shadow_catalog import ShadowStepService

    settings = _settings(args)
    if not args.input:
        _require_production(settings, "shadow step")
        partition = date.fromisoformat(
            str(args.date or datetime.now(timezone.utc).date().isoformat())
        ).isoformat()
        services = _authoritative_services()
        try:
            _accepted_range(
                services,
                date.fromisoformat(partition),
                date.fromisoformat(partition),
            )
            results = _execute_daily_chain(
                services,
                partition,
                (
                    OperationName.SOURCE_SYNC,
                    OperationName.SOURCE_RECONCILIATION,
                    OperationName.DATA_QUALITY_GATE,
                    OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
                    OperationName.SHADOW_NAV_STEP,
                ),
            )
        finally:
            _close_services(services)
        completed = bool(results) and results[-1]["status"] in {
            "completed",
            "skipped",
        }
        _emit(
            {
                "status": "completed" if completed else "blocked",
                "mode": "authoritative_postgresql_shadow",
                "partition_key": partition,
                "operations": results,
            }
        )
        return 0 if completed else 2
    _reject_file_input_in_production(settings, command="shadow step --input")
    payload = _json(args.input)
    bars = read_frame(payload["market_bars"])
    with _catalog(settings) as catalog:
        service = ShadowStepService(catalog)

        def execute_step() -> dict[str, Any]:
            account_id = str(payload["account_id"])
            if catalog.get_shadow_account(account_id) is None:
                opened_value = payload.get("opened_at") or (
                    f"{payload['decision_date']}T00:00:00+00:00"
                )
                opened_at = datetime.fromisoformat(
                    str(opened_value).replace("Z", "+00:00")
                )
                if opened_at.tzinfo is None:
                    opened_at = opened_at.replace(tzinfo=timezone.utc)
                catalog.create_shadow_account(
                    account_id=account_id,
                    name=str(payload.get("account_name") or "Research shadow"),
                    initial_capital=float(payload.get("initial_capital", 50_000_000)),
                    opened_at=opened_at,
                    currency=str(payload.get("currency") or "CNY"),
                )
            return service.step(
                account_id=account_id,
                decision_date=str(payload["decision_date"]),
                trade_date=str(payload["trade_date"]),
                target_weights={
                    str(key): float(value)
                    for key, value in payload["target_weights"].items()
                },
                market_bars=bars,
                snapshot_id=str(payload["snapshot_id"]),
                model_version=str(payload["model_version"]),
                benchmark_return=(
                    None
                    if payload.get("benchmark_return") is None
                    else float(payload["benchmark_return"])
                ),
                expected_next_session=payload.get("expected_next_session"),
            ).as_dict()

        result = _coordinated(
            catalog,
            "shadow_step",
            {
                "account_id": payload["account_id"],
                "decision_date": payload["decision_date"],
                "trade_date": payload["trade_date"],
                "snapshot_id": payload["snapshot_id"],
            },
            execute_step,
        )
    _emit(result)
    return 0


def _incident_revalidate(args: argparse.Namespace) -> int:
    settings = _settings(args)
    _require_production(settings, "incident revalidate")
    services = _authoritative_services()
    try:
        revalidate = getattr(services, "revalidate_ready_data_incident", None)
        if not callable(revalidate):
            raise ValueError(
                "authoritative services do not expose incident revalidation"
            )
        result = revalidate(incident_id=str(args.incident_id))
    finally:
        _close_services(services)
    _emit(result)
    return 0


def _legacy_import(args: argparse.Namespace) -> int:
    settings = _settings(args)
    _reject_file_input_in_production(settings, command="legacy import")
    with _catalog(settings) as catalog:
        result = _coordinated(
            catalog,
            "legacy_import",
            {"root": str(Path(args.root).resolve())},
            lambda: import_legacy_evidence(
                catalog,
                args.root,
                seal_sqlite=args.seal,
            ),
        )
    _emit(result)
    return 0


def _doctor(args: argparse.Namespace) -> int:
    settings = _settings(args)
    result = doctor(settings, check_network=not args.no_network)
    if args.production:
        try:
            if not _is_production(settings):
                raise ProductionConfigurationError(
                    "effective runtime authority must be production for doctor --production"
                )
            config_value = str(
                args.config or os.environ.get(ORCHESTRATION_CONFIG_ENV) or ""
            ).strip()
            if not config_value:
                raise ProductionConfigurationError(
                    f"set {ORCHESTRATION_CONFIG_ENV} or pass --config"
                )
            evidence = validate_production_config(
                config_value,
                require_mounts=not args.no_mount_check,
                image_reference=(str(args.image or "").strip() or None),
            )
            readiness_audit = _latest_verified_readiness(settings)
            readiness_matches = bool(
                readiness_audit is not None
                and _readiness_matches_provenance(
                    readiness_audit, evidence.provenance
                )
            )
            execution_check = _readiness_check(
                readiness_audit, "formal_execution_capability"
            )
            formal_execution_capable = bool(
                execution_check is not None and execution_check.passed
            )
            formal_forward_ready = bool(
                readiness_audit is not None
                and readiness_audit.ready
                and readiness_matches
            )
            production_checks = (
                DoctorCheck(
                    name="production_bootstrap",
                    status="pass",
                    detail=(
                        f"verified {evidence.path.name}; "
                        f"build={evidence.provenance.build_identity_hash[:16]}; "
                        f"readiness={readiness_audit.status.value if readiness_audit else evidence.status}"
                    ),
                ),
                DoctorCheck(
                    name="historical_backfill_authority",
                    status=("pass" if evidence.historical_backfill_allowed else "fail"),
                    detail=(
                        "reviewed credential disposition permits authoritative historical backfill"
                        if evidence.historical_backfill_allowed
                        else "authoritative historical backfill is blocked until every credential "
                        "has either vendor-confirmed rotation or an explicit operator-accepted "
                        "retention decision"
                    ),
                    blocking=not evidence.historical_backfill_allowed,
                ),
                DoctorCheck(
                    name="formal_execution_capability",
                    status=("pass" if formal_execution_capable else "fail"),
                    detail=(
                        "accepted point-in-time opening execution capability is persisted"
                        if formal_execution_capable
                        else "PostgreSQL has no accepted hash-verified opening execution capability"
                    ),
                    blocking=not formal_execution_capable,
                ),
                DoctorCheck(
                    name="persisted_production_readiness",
                    status=("pass" if formal_forward_ready else "fail"),
                    detail=(
                        f"{readiness_audit.audit_id} matches the inspected release"
                        if formal_forward_ready
                        else (
                            "no hash-verified PostgreSQL readiness audit exists"
                            if readiness_audit is None
                            else "latest readiness audit is blocked or belongs to another release"
                        )
                    ),
                    blocking=not formal_forward_ready,
                ),
                DoctorCheck(
                    name="formal_forward_evidence",
                    status=("pass" if formal_forward_ready else "fail"),
                    detail=(
                        "formal forward evidence may be activated"
                        if formal_forward_ready
                        else "blocked: "
                        + ", ".join(
                            readiness_audit.blockers
                            if readiness_audit is not None
                            else evidence.readiness_blockers
                        )
                    ),
                    blocking=not formal_forward_ready,
                ),
            )
        except Exception as exc:
            production_checks = (
                DoctorCheck(
                    name="production_bootstrap",
                    status="fail",
                    detail=f"{type(exc).__name__}: {exc}",
                    blocking=True,
                ),
            )
        checks = (*result.checks, *production_checks)
        blocked = any(item.status == "fail" and item.blocking for item in checks)
        degraded = any(item.status != "pass" for item in checks)
        result = DoctorReport(
            status="blocked" if blocked else ("degraded" if degraded else "ready"),
            checks=checks,
            settings=result.settings,
        )
    _emit(result)
    return 0 if result.ready else 2


def _add_epoch_provenance_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config",
        help=f"orchestration JSON; defaults to {ORCHESTRATION_CONFIG_ENV}",
    )
    parser.add_argument(
        "--repository",
        help="source checkout root for local capture; ignored in production image mode",
    )
    parser.add_argument(
        "--manifest",
        help=f"immutable source-bundle manifest; defaults to {SOURCE_BUNDLE_MANIFEST_ENV}",
    )
    parser.add_argument(
        "--image",
        help="local OCI image reference to inspect; required for formal production epoch freeze",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="factor-lab",
        description="Factor Lab Research OS (historical research and shadow only)",
    )
    parser.add_argument(
        "--database-url",
        help="Authoritative PostgreSQL URL; SQLite is an explicit test-only override",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    data = commands.add_parser("data")
    data_commands = data.add_subparsers(dest="data_command", required=True)
    sync = data_commands.add_parser("sync")
    sync.add_argument("--spec", help="test/legacy-only SourceAdapter JSON")
    sync.add_argument("--from", dest="date_from")
    sync.add_argument("--to", dest="date_to")
    sync.add_argument("--resume", action="store_true")
    sync.add_argument("--lake-root")
    sync.set_defaults(handler=_data_sync)

    snapshot = commands.add_parser("snapshot")
    snapshot_commands = snapshot.add_subparsers(
        dest="snapshot_command", required=True
    )
    publish = snapshot_commands.add_parser("publish")
    publish.add_argument("--spec", help="test/legacy-only snapshot JSON")
    publish.add_argument("--as-of")
    publish.set_defaults(handler=_snapshot_publish)

    research = commands.add_parser("research")
    research_commands = research.add_subparsers(
        dest="research_command", required=True
    )
    cycle = research_commands.add_parser("cycle")
    cycle.add_argument("--experiment", required=True)
    cycle.add_argument("--data", required=True)
    cycle.add_argument("--fields")
    cycle.add_argument("--negative-controls")
    cycle.add_argument("--sleeve-signal")
    cycle.add_argument("--bootstrap-resamples", type=int, default=2_000)
    cycle.add_argument("--seed", type=int, default=0)
    cycle.set_defaults(handler=_research_cycle)

    def add_proposal_arguments(command: argparse.ArgumentParser) -> None:
        command.add_argument("--family", required=True)
        command.add_argument(
            "--proposal",
            required=True,
            help="hypothesis + Factor DSL JSON path, or '-' for stdin",
        )
        command.add_argument("--recovery-case")

    propose = research_commands.add_parser("propose")
    add_proposal_arguments(propose)
    propose.set_defaults(handler=_research_propose)
    submit = research_commands.add_parser("submit")
    add_proposal_arguments(submit)
    submit.set_defaults(handler=_research_submit)
    research_status = research_commands.add_parser("status")
    research_status.add_argument("--submission-id", required=True)
    research_status.set_defaults(handler=_research_status)
    resume = research_commands.add_parser("resume")
    resume.add_argument("--worker-id", required=True)
    resume.add_argument("--limit", type=int, default=100)
    resume.add_argument("--lease-seconds", type=int, default=1_800)
    resume.set_defaults(handler=_research_resume)
    epoch = research_commands.add_parser("epoch")
    epoch_commands = epoch.add_subparsers(dest="epoch_command", required=True)
    epoch_status = epoch_commands.add_parser("status")
    _add_epoch_provenance_options(epoch_status)
    epoch_status.set_defaults(handler=_research_epoch_status)
    epoch_freeze = epoch_commands.add_parser("freeze")
    _add_epoch_provenance_options(epoch_freeze)
    epoch_freeze.add_argument("--calendar-snapshot-id")
    epoch_freeze.add_argument("--first-forward-session")
    epoch_freeze.set_defaults(handler=_research_freeze_epoch)

    # Compatibility spelling retained without the unsafe caller-supplied hash
    # arguments.  Both paths measure the running package and configuration.
    freeze_epoch = research_commands.add_parser("freeze-epoch")
    _add_epoch_provenance_options(freeze_epoch)
    freeze_epoch.add_argument("--calendar-snapshot-id")
    freeze_epoch.add_argument("--first-forward-session")
    freeze_epoch.set_defaults(handler=_research_freeze_epoch)

    # Public spelling.  The nested research epoch spelling above remains a
    # compatibility alias, with identical measured-provenance behavior.
    public_epoch = commands.add_parser("epoch")
    public_epoch_commands = public_epoch.add_subparsers(
        dest="epoch_command", required=True
    )
    public_epoch_status = public_epoch_commands.add_parser("status")
    _add_epoch_provenance_options(public_epoch_status)
    public_epoch_status.set_defaults(handler=_research_epoch_status)
    public_epoch_freeze = public_epoch_commands.add_parser("freeze")
    _add_epoch_provenance_options(public_epoch_freeze)
    public_epoch_freeze.add_argument("--calendar-snapshot-id")
    public_epoch_freeze.add_argument("--first-forward-session")
    public_epoch_freeze.set_defaults(handler=_research_freeze_epoch)

    readiness = commands.add_parser("readiness")
    readiness_commands = readiness.add_subparsers(
        dest="readiness_command", required=True
    )
    readiness_audit = readiness_commands.add_parser("audit")
    _add_epoch_provenance_options(readiness_audit)
    readiness_audit.set_defaults(handler=_production_readiness_audit)
    readiness_status = readiness_commands.add_parser("status")
    readiness_status.set_defaults(handler=_production_readiness_status)
    readiness_attest = readiness_commands.add_parser("attest-runtime")
    readiness_attest.set_defaults(handler=_host_runtime_attestation)
    readiness_credential = readiness_commands.add_parser("attest-credential-use")
    readiness_credential.set_defaults(handler=_production_credential_use_attestation)

    canary = commands.add_parser("canary")
    canary_commands = canary.add_subparsers(dest="canary_command", required=True)
    canary_run = canary_commands.add_parser("run")
    canary_run.add_argument("--as-of")
    canary_run.set_defaults(handler=_physical_engineering_canary)
    canary_restore = canary_commands.add_parser("restore")
    canary_restore.set_defaults(handler=_physical_minio_restore_drill)

    monitor = commands.add_parser("monitor")
    monitor_commands = monitor.add_subparsers(dest="monitor_command", required=True)
    tick = monitor_commands.add_parser("tick")
    tick.add_argument("--input", help="test/legacy-only monitor JSON")
    tick.add_argument("--as-of")
    tick.set_defaults(handler=_monitor_tick)

    shadow = commands.add_parser("shadow")
    shadow_commands = shadow.add_subparsers(dest="shadow_command", required=True)
    step = shadow_commands.add_parser("step")
    step.add_argument("--input", help="test/legacy-only shadow JSON")
    step.add_argument("--date")
    step.set_defaults(handler=_shadow_step)

    incident = commands.add_parser("incident")
    incident_commands = incident.add_subparsers(
        dest="incident_command", required=True
    )
    incident_revalidate = incident_commands.add_parser("revalidate")
    incident_revalidate.add_argument("--incident-id", required=True)
    incident_revalidate.set_defaults(handler=_incident_revalidate)

    legacy = commands.add_parser("legacy")
    legacy_commands = legacy.add_subparsers(dest="legacy_command", required=True)
    legacy_import = legacy_commands.add_parser("import")
    legacy_import.add_argument("--root", default="artifacts")
    legacy_import.add_argument(
        "--seal",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="remove write bits from imported legacy SQLite files",
    )
    legacy_import.set_defaults(handler=_legacy_import)

    doctor_command = commands.add_parser("doctor")
    doctor_command.add_argument("--no-network", action="store_true")
    doctor_command.add_argument("--production", action="store_true")
    doctor_command.add_argument("--config")
    doctor_command.add_argument(
        "--image",
        help="local OCI image reference inspected for formal production readiness",
    )
    doctor_command.add_argument(
        "--no-mount-check",
        action="store_true",
        help="test-only: validate production config without checking bind mounts",
    )
    doctor_command.set_defaults(handler=_doctor)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args))
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = ["build_parser", "main"]
