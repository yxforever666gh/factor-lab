from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, delete, insert, update

import factor_lab.research_os.application_services as application_services
from factor_lab.research_os import orm
from factor_lab.research_os.application_services import (
    APPLICATION_SERVICES_SCHEMA_VERSION,
    WEBUI_ENV_FILE_ENV,
    ApplicationServices,
    _runtime_environment,
)
from factor_lab.research_os.catalog import LifecycleEvent, ResearchCatalog
from factor_lab.research_os.contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    LifecycleState,
    SnapshotTier,
)
from factor_lab.research_os.data_incidents import (
    DataIncidentCoordinator,
    DataRevalidation,
)
from factor_lab.research_os.fingerprint import content_fingerprint
from factor_lab.research_os.orchestration import (
    CycleName,
    OperationName,
    OperationRequest,
    OperationResult,
    OrchestrationFailure,
    ServiceNotConfigured,
)
from factor_lab.research_os.production_ledger import (
    ImmutablePartition,
    IncidentStage,
    IncidentStatus,
    PartitionIdentity,
    PartitionStatus,
    ProductionLedger,
)
from factor_lab.research_os.shadow_authority import ShadowEvidenceAuthority, ShadowRole
from factor_lab.research_os.sleeve_registry import load_sleeve_roster
from factor_lab.research_os.snapshots import build_immutable_snapshot_manifest


def _set_snapshot_created_at(
    catalog: ResearchCatalog, snapshot_id: str, created_at: datetime
) -> None:
    """Bind fixture registration time to its simulated durable stage clock."""

    backend = catalog._backend  # noqa: SLF001 - bounded authority fixture
    connection = getattr(backend, "_connection", None)
    if connection is not None:
        with backend._transaction() as transaction:  # noqa: SLF001
            transaction.execute(
                "UPDATE ros_data_snapshots SET created_at = ? WHERE snapshot_id = ?",
                (created_at.isoformat(timespec="microseconds"), snapshot_id),
            )
        return
    with backend._engine.begin() as transaction:  # noqa: SLF001
        transaction.execute(
            update(orm.DataSnapshotModel)
            .where(orm.DataSnapshotModel.snapshot_id == snapshot_id)
            .values(created_at=created_at)
        )


def _revalidation_repair_fingerprint(
    *, partition_key: str, suffix: str, incident_id: str | None
) -> str:
    return content_fingerprint(
        {
            "partition_key": partition_key,
            "suffix": suffix,
            "incident_id": incident_id,
            "run_id": f"fixture-repair-{suffix}",
        },
        domain="test/revalidation-repair-fingerprint",
    )


def _register_revalidation_gold(
    *,
    catalog: ResearchCatalog,
    ledger: ProductionLedger,
    lake_root: Path,
    partition_key: str,
    incident_at: datetime,
    suffix: str = "primary",
    bind_stage: bool = True,
    repair_incident_id: str | None = None,
    repair_fingerprint_override: str | None = None,
    gold_manifest_labels: tuple[str, ...] | None = None,
    gold_reference_labels: tuple[str, ...] | None = None,
) -> DataSnapshotRef:
    """Publish a physically verifiable Bronze -> Silver -> DQ -> Gold repair."""

    lake_root.mkdir(parents=True, exist_ok=True)
    repair_base = incident_at
    environment_hashes = {
        "config_hash": "1" * 64,
        "code_hash": "2" * 64,
        "dirty_patch_hash": "3" * 64,
        "dependency_lock_hash": "4" * 64,
    }
    bronze_artifact = lake_root / f"revalidation-bronze-{suffix}.json"
    bronze_artifact.write_text(
        json.dumps({"partition_key": partition_key, "suffix": suffix}),
        encoding="utf-8",
    )
    bronze_manifest = build_immutable_snapshot_manifest(
        (bronze_artifact,),
        base_dir=lake_root,
        tier="bronze",
        as_of=repair_base + timedelta(minutes=1),
        parent_snapshot_ids=(),
        environment_hashes=environment_hashes,
        quality_report={"status": "pass"},
        trust_labels=("raw_vendor_response", "source:fixture"),
    )
    parent_id = bronze_manifest.snapshot_id
    catalog.register_snapshot(
        bronze_manifest.to_snapshot_ref(uri=f"s3://fixture/{parent_id}")
    )
    _set_snapshot_created_at(
        catalog, parent_id, repair_base + timedelta(minutes=1)
    )
    silver_artifact = lake_root / f"revalidation-silver-{suffix}.json"
    silver_artifact.write_text(
        json.dumps({"partition_key": partition_key, "suffix": suffix}),
        encoding="utf-8",
    )
    silver_manifest = build_immutable_snapshot_manifest(
        (silver_artifact,),
        base_dir=lake_root,
        tier="silver",
        as_of=repair_base + timedelta(minutes=4),
        parent_snapshot_ids=(parent_id,),
        environment_hashes=environment_hashes,
        quality_report={"status": "pass"},
        trust_labels=(
            "point_in_time",
            "field_reconciled",
        ),
    )
    silver_reference = silver_manifest.to_snapshot_ref(
        uri=f"s3://fixture/silver/{silver_manifest.snapshot_id}"
    )
    catalog.register_snapshot(silver_reference)
    _set_snapshot_created_at(
        catalog,
        silver_reference.snapshot_id,
        repair_base + timedelta(minutes=4),
    )
    gold_artifact = lake_root / f"revalidation-gold-{suffix}.json"
    gold_artifact.write_text(
        json.dumps({"silver_snapshot_id": silver_reference.snapshot_id}),
        encoding="utf-8",
    )
    gold_manifest = build_immutable_snapshot_manifest(
        (gold_artifact,),
        base_dir=lake_root,
        tier="gold",
        as_of=repair_base + timedelta(minutes=10),
        parent_snapshot_ids=(silver_reference.snapshot_id,),
        environment_hashes=environment_hashes,
        quality_report={"status": "pass"},
        trust_labels=(
            gold_manifest_labels
            if gold_manifest_labels is not None
            else (
                "point_in_time",
                "field_reconciled",
                "quality_accepted",
            )
        ),
    )
    gold_uri = f"iceberg://factorlab/research.gold#ros_{gold_manifest.snapshot_id}"
    if gold_reference_labels is None:
        reference = gold_manifest.to_snapshot_ref(uri=gold_uri)
    else:
        reference = DataSnapshotRef(
            snapshot_id=gold_manifest.snapshot_id,
            tier=SnapshotTier.GOLD,
            uri=gold_uri,
            content_hash=gold_manifest.snapshot_id,
            parent_snapshot_ids=gold_manifest.parent_snapshot_ids,
            as_of=gold_manifest.as_of,
            quality_status=DataQualityStatus.ACCEPTED,
            trust_labels=gold_reference_labels,
            manifest=gold_manifest.to_dict(),
        )
    catalog.register_snapshot(reference)
    _set_snapshot_created_at(
        catalog,
        reference.snapshot_id,
        repair_base + timedelta(minutes=10),
    )
    if not bind_stage:
        return reference

    repair_run_id = f"fixture-repair-{suffix}"
    repair_fingerprint = repair_fingerprint_override or (
        _revalidation_repair_fingerprint(
            partition_key=partition_key,
            suffix=suffix,
            incident_id=repair_incident_id,
        )
    )
    repair_cohort_id: str | None = None
    if repair_incident_id is not None:
        repair_incident = ledger.get_incident(repair_incident_id)
        assert repair_incident is not None
        repair_cohort_id = ApplicationServices._repair_cohort_id(
            repair_incident, repair_fingerprint
        )

    def finish_stage(
        *,
        dataset: str,
        operation: OperationName,
        outputs: dict[str, object],
        minute: int,
        output_snapshot_id: str | None,
    ) -> None:
        base_identity = PartitionIdentity("research_os", dataset, partition_key)
        current = ledger.get_partition(base_identity)
        input_hash = (
            current.input_hash
            if current is not None and current.input_hash is not None
            else content_fingerprint(
                {"partition_key": partition_key, "dataset": dataset, "suffix": suffix},
                domain="test/revalidation-stage-input",
            )
        )
        if repair_incident_id is None:
            identity = base_identity
            ledger.ensure_partition(
                identity,
                created_at=repair_base + timedelta(minutes=max(0, minute - 1)),
                input_hash=input_hash,
                details={"dagster_run_id": repair_run_id},
            )
        else:
            identity = ledger.reserve_repair_successor(
                incident_id=repair_incident_id,
                dataset=dataset,
                repair_fingerprint=repair_fingerprint,
                input_hash=input_hash,
                created_at=repair_base
                + timedelta(minutes=max(0, minute - 1)),
                details={
                    "operation": operation.value,
                    "dagster_run_id": repair_run_id,
                    "repair_cohort_id": repair_cohort_id,
                },
            ).identity
        lease = ledger.claim(
            identity=identity,
            owner=f"fixture-{dataset}-repair",
            now=repair_base + timedelta(minutes=max(0, minute - 1)),
            lease_for=timedelta(minutes=5),
        )
        assert lease is not None
        operation_payload = OperationResult(
            operation=operation,
            status="completed",
            summary=f"completed repaired fixture {dataset}",
            outputs=outputs,
        ).to_dict()
        ledger.finish(
            lease,
            status=PartitionStatus.SUCCEEDED,
            completed_at=repair_base + timedelta(minutes=minute),
            output_snapshot_id=output_snapshot_id,
            output_hash=content_fingerprint(
                operation_payload,
                domain="factor-lab/research-os/v1/production-operation-result",
            ),
            details={
                "operation": operation.value,
                "dagster_run_id": repair_run_id,
                "operation_result": operation_payload,
                **(
                    {}
                    if repair_cohort_id is None
                    else {"repair_cohort_id": repair_cohort_id}
                ),
            },
        )

    finish_stage(
        dataset="stage_source",
        operation=OperationName.SOURCE_SYNC,
        outputs={"bronze_snapshot_ids": [parent_id]},
        minute=2,
        output_snapshot_id=None,
    )
    finish_stage(
        dataset="stage_silver",
        operation=OperationName.SOURCE_RECONCILIATION,
        outputs={"silver_snapshot_id": silver_reference.snapshot_id},
        minute=4,
        output_snapshot_id=silver_reference.snapshot_id,
    )
    finish_stage(
        dataset="stage_data_quality",
        operation=OperationName.DATA_QUALITY_GATE,
        outputs={
            "silver_snapshot_id": silver_reference.snapshot_id,
            "quality_report": {"status": "pass"},
        },
        minute=6,
        output_snapshot_id=silver_reference.snapshot_id,
    )
    finish_stage(
        dataset="stage_gold",
        operation=OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
        outputs={
            "snapshot_id": reference.snapshot_id,
            "uri": reference.uri,
            "parent_snapshot_ids": list(reference.parent_snapshot_ids),
        },
        minute=10,
        output_snapshot_id=reference.snapshot_id,
    )
    return reference


def _finish_revalidation_shadow(
    *,
    catalog: ResearchCatalog,
    ledger: ProductionLedger,
    repair_incident_id: str,
    partition_key: str,
    incident_at: datetime,
    gold: DataSnapshotRef,
    account_ids: tuple[str, ...],
    suffix: str = "primary",
    validation_trade_date: str | None = None,
    repair_fingerprint_override: str | None = None,
) -> tuple[PartitionIdentity, tuple[dict[str, object], ...]]:
    """Append real account projections and finish the selected Shadow successor."""

    if not account_ids:
        raise AssertionError("repair fixture requires at least the Champion account")
    repair_run_id = f"fixture-repair-{suffix}"
    repair_fingerprint = repair_fingerprint_override or (
        _revalidation_repair_fingerprint(
            partition_key=partition_key,
            suffix=suffix,
            incident_id=repair_incident_id,
        )
    )
    repair_incident = ledger.get_incident(repair_incident_id)
    assert repair_incident is not None
    repair_cohort_id = ApplicationServices._repair_cohort_id(
        repair_incident, repair_fingerprint
    )
    validation_date = date.fromisoformat(
        validation_trade_date or partition_key
    )
    validation_key = validation_date.isoformat()
    validation_close = datetime.combine(
        validation_date,
        application_services.time(15, 30),
        tzinfo=application_services._SHANGHAI,
    ).astimezone(timezone.utc)
    projection_time = max(
        incident_at + timedelta(minutes=12), validation_close
    )
    shadow_started_at = max(
        incident_at + timedelta(minutes=11),
        projection_time - timedelta(minutes=1),
    )
    shadow_completed_at = projection_time + timedelta(minutes=2)
    authority = ledger.reserve_repair_successor(
        incident_id=repair_incident_id,
        dataset="stage_shadow",
        repair_fingerprint=repair_fingerprint,
        input_hash=content_fingerprint(
            {
                "partition_key": partition_key,
                "gold_snapshot_id": gold.snapshot_id,
                "accounts": account_ids,
            },
            domain="test/revalidation-shadow-input",
        ),
        created_at=shadow_started_at,
        details={
            "operation": OperationName.SHADOW_NAV_STEP.value,
            "dagster_run_id": repair_run_id,
            "repair_cohort_id": repair_cohort_id,
            "repair_validation_trade_date": validation_key,
        },
    )
    lease = ledger.claim(
        identity=authority.identity,
        owner=f"fixture-stage-shadow-{suffix}",
        now=shadow_started_at,
        lease_for=timedelta(minutes=10),
    )
    assert lease is not None
    projections: list[dict[str, object]] = []
    for index, account_id in enumerate(account_ids):
        account = catalog.get_shadow_account(account_id)
        assert account is not None
        nav = 50_000_000.0 - index * 100_000.0
        step_id = f"repair-{suffix}-{account_id}"
        event = catalog.append_shadow_event(
            account_id=account_id,
            event_type="account_projected",
            occurred_at=projection_time,
            payload={
                "research_os_shadow_step": {
                    "step_id": step_id,
                    "kind": "account_projection",
                },
                "account_status": "active",
                "account_state": {
                    "cash": nav,
                    "nav": nav,
                    "benchmark_nav": 50_000_000.0,
                },
            },
            expected_previous_hash=account.last_event_hash,
        )
        projections.append(
            {
                "step_id": step_id,
                "account_id": account_id,
                "decision_date": validation_key,
                "trade_date": validation_key,
                "snapshot_id": gold.snapshot_id,
                "model_version": "repair-v1",
                "decision_snapshot_id": None,
                "execution_snapshot_id": f"execution-{validation_key}",
                "mark_snapshot_id": f"mark-{validation_key}",
                "rebalanced": False,
                "cash": nav,
                "nav": nav,
                "benchmark_nav": 50_000_000.0,
                "position_count": 0,
                "domain_event_count": 1,
                "persisted_event_count": 1,
                "first_event_sequence": event.sequence_number,
                "last_event_sequence": event.sequence_number,
                "last_event_hash": event.event_hash,
                "chain_verified": True,
            }
        )
    operation_payload = OperationResult(
        operation=OperationName.SHADOW_NAV_STEP,
        status="completed",
        summary="completed repaired fixture stage_shadow",
        outputs={
            "input_mode": "authoritative_pg",
            "incident_partition_key": partition_key,
            "validation_trade_date": validation_key,
            "executed": {
                "projections": projections,
                "champion_account_id": account_ids[0],
                "challenger_account_ids": list(account_ids[1:]),
            },
        },
    ).to_dict()
    ledger.finish(
        lease,
        status=PartitionStatus.SUCCEEDED,
        completed_at=shadow_completed_at,
        output_hash=content_fingerprint(
            operation_payload,
            domain="factor-lab/research-os/v1/production-operation-result",
        ),
        details={
            "operation": OperationName.SHADOW_NAV_STEP.value,
            "dagster_run_id": repair_run_id,
            "operation_result": operation_payload,
            "repair_cohort_id": repair_cohort_id,
            "repair_validation_trade_date": validation_key,
        },
    )
    return authority.identity, tuple(projections)


def _install_fixture_shadow_roles(
    service: ApplicationServices,
    account_ids: tuple[str, ...],
) -> tuple[SimpleNamespace, ...]:
    """Install one deterministic production-role selection for bridge tests."""

    if not account_ids:
        raise AssertionError("fixture production fleet requires a Champion")
    bindings = tuple(
        SimpleNamespace(
            binding_id=f"fixture-binding-{account_id}",
            binding_hash=content_fingerprint(
                {
                    "role": (
                        ShadowRole.CHAMPION.value
                        if index == 0
                        else ShadowRole.CHALLENGER.value
                    ),
                    "account_id": account_id,
                },
                domain="test/revalidation-role-binding",
            ),
            role=(
                ShadowRole.CHAMPION
                if index == 0
                else ShadowRole.CHALLENGER
            ),
            role_key=(
                "static_champion"
                if index == 0
                else f"fixture-challenger-{account_id}"
            ),
            account_id=account_id,
            active=True,
        )
        for index, account_id in enumerate(account_ids)
    )
    service.shadow_authority = SimpleNamespace(
        active_fleet_bindings=lambda: bindings
    )
    return bindings


def test_runtime_env_file_loads_only_profile_selection_and_process_wins(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "webui.env"
    env_file.write_text(
        "\n".join(
            (
                "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON=[]",
                "FACTOR_LAB_DATA_SOURCE_ORDER=file-order",
                "FACTOR_LAB_PRIMARY_DATA_SOURCE=tushare",
                "FACTOR_LAB_LLM_MODEL=file-model",
                "FACTOR_LAB_LLM_API_KEY_REF=secret://llm-primary",
                "FACTOR_LAB_LLM_PROFILES_JSON="
                + json.dumps(
                    [
                        {
                            "name": "primary",
                            "model": "file-model",
                            "credential_ref": "secret://llm-primary",
                        }
                    ]
                ),
                "FACTOR_LAB_LLM_FALLBACK_ORDER=primary",
                "TUSHARE_TOKEN=",
            )
        ),
        encoding="utf-8",
    )
    merged = _runtime_environment(
        {
            WEBUI_ENV_FILE_ENV: str(env_file),
            "FACTOR_LAB_DATA_SOURCE_ORDER": "process-order",
        }
    )
    assert merged["FACTOR_LAB_DATA_SOURCE_PROFILES_JSON"] == "[]"
    assert merged["FACTOR_LAB_DATA_SOURCE_ORDER"] == "process-order"
    assert merged["FACTOR_LAB_PRIMARY_DATA_SOURCE"] == "tushare"
    assert merged["FACTOR_LAB_LLM_MODEL"] == "file-model"
    assert merged["FACTOR_LAB_LLM_API_KEY_REF"] == "secret://llm-primary"
    assert merged["FACTOR_LAB_LLM_FALLBACK_ORDER"] == "primary"
    assert "TUSHARE_TOKEN" not in merged


@pytest.mark.parametrize(
    "line",
    (
        "TUSHARE_TOKEN=raw-secret",
        "DIEMENG_API_KEY=raw-secret",
        "FACTOR_LAB_LLM_API_KEY=raw-secret",
        "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON="
        + json.dumps(
            [
                {
                    "name": "bad",
                    "source_type": "tushare",
                    "api_key": "raw-secret",
                }
            ]
        ),
    ),
)
def test_runtime_env_file_rejects_inline_data_source_secrets(
    tmp_path: Path, line: str
) -> None:
    env_file = tmp_path / "webui.env"
    env_file.write_text(line + "\n", encoding="utf-8")
    with pytest.raises(ServiceNotConfigured, match="forbidden"):
        _runtime_environment({WEBUI_ENV_FILE_ENV: str(env_file)})


def test_create_services_rereads_atomically_replaced_webui_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / "webui.env"
    config_path = tmp_path / "orchestration.json"
    config_path.write_text("{}", encoding="utf-8")

    def publish(model: str) -> None:
        replacement = tmp_path / "webui.env.next"
        replacement.write_text(
            "\n".join(
                (
                    f"FACTOR_LAB_LLM_MODEL={model}",
                    "FACTOR_LAB_LLM_API_KEY_REF=secret://llm-primary",
                    "FACTOR_LAB_LLM_PROFILES_JSON="
                    + json.dumps(
                        [
                            {
                                "name": "primary",
                                "model": model,
                                "credential_ref": "secret://llm-primary",
                            }
                        ]
                    ),
                    "FACTOR_LAB_LLM_FALLBACK_ORDER=primary",
                    "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON=[]",
                )
            )
            + "\n",
            encoding="utf-8",
        )
        replacement.replace(env_file)

    settings = SimpleNamespace(
        database_url="sqlite+pysqlite:///:memory:",
        object_store_endpoint="http://object-store.invalid",
        object_store_bucket="factor-lab",
        object_store_access_key="ref-only",
        object_store_secret_key="ref-only",
    )
    monkeypatch.setattr(
        application_services.ResearchOSSettings,
        "from_env",
        staticmethod(lambda _env: settings),
    )
    monkeypatch.setattr(application_services, "ResearchCatalog", lambda _url: object())
    monkeypatch.setattr(
        application_services, "PyIcebergGoldPublisher", lambda **_kwargs: object()
    )
    monkeypatch.setattr(
        application_services.S3ImmutableArchive,
        "from_connection",
        staticmethod(lambda **_kwargs: object()),
    )
    monkeypatch.setattr(
        application_services,
        "ApplicationServices",
        lambda _config, **kwargs: SimpleNamespace(env=kwargs["env"]),
    )
    for key in (
        "FACTOR_LAB_LLM_MODEL",
        "FACTOR_LAB_LLM_API_KEY_REF",
        "FACTOR_LAB_LLM_PROFILES_JSON",
        "FACTOR_LAB_LLM_FALLBACK_ORDER",
        "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv(WEBUI_ENV_FILE_ENV, str(env_file))
    monkeypatch.setenv(application_services.ORCHESTRATION_CONFIG_ENV, str(config_path))

    publish("model-v1")
    first = application_services.create_services()
    publish("model-v2")
    second = application_services.create_services()

    assert first.env["FACTOR_LAB_LLM_MODEL"] == "model-v1"
    assert second.env["FACTOR_LAB_LLM_MODEL"] == "model-v2"


def test_database_authority_marker_overrides_accidental_local_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = SimpleNamespace(
        environment="local",
        uses_postgresql=True,
        database_url="postgresql+psycopg://factor_lab.invalid/factor_lab",
    )
    monkeypatch.setattr(
        application_services,
        "load_runtime_authority_marker",
        lambda _database: SimpleNamespace(is_production=True),
    )

    assert application_services._effective_production_authority(settings) is True


def test_environment_cannot_promote_unmarked_postgresql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = SimpleNamespace(
        environment="production",
        uses_postgresql=True,
        database_url="postgresql+psycopg://factor_lab.invalid/factor_lab",
    )
    monkeypatch.setattr(
        application_services,
        "load_runtime_authority_marker",
        lambda _database: None,
    )

    with pytest.raises(ServiceNotConfigured, match="Alembic-owned authority"):
        application_services._effective_production_authority(settings)


def test_partition_compact_alias_is_rendered() -> None:
    service = object.__new__(ApplicationServices)
    request = OperationRequest(
        operation=OperationName.SOURCE_SYNC,
        cycle=CycleName.DAILY,
        partition_key="2026-08-23",
        run_id="tokens",
    )
    tokens = service._tokens(request)
    assert tokens["partition_compact"] == "20260823"
    assert tokens["partition_yyyymmdd"] == "20260823"


def test_typed_capability_can_close_before_epoch_but_shadow_cannot_project() -> None:
    service = object.__new__(ApplicationServices)
    epoch = None
    service.catalog = SimpleNamespace(get_evidence_epoch=lambda: epoch)

    assert service.formal_shadow_projection_allowed(date(2026, 8, 24)) is False

    epoch = SimpleNamespace(
        first_forward_session=date(2026, 8, 25),
        forward_holdout_id="holdout-2026-08-25",
    )
    assert service.formal_shadow_projection_allowed(date(2026, 8, 24)) is False
    assert service.formal_shadow_projection_allowed(date(2026, 8, 25)) is True


def test_automatic_monthly_proposals_are_one_per_family_three_total_and_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = application_services.datetime(
        2026, 8, 23, 8, tzinfo=application_services.timezone.utc
    )

    class Catalog:
        def __init__(self) -> None:
            self.runs = {}
            self.active_case = SimpleNamespace(
                recovery_case_id="older-active-family-0",
                sleeve_id="family_0",
                status=application_services.RecoveryCaseStatus.OPEN,
                triggered_at=now - timedelta(days=1),
            )
            self.recovery_cases = [
                *(
                    SimpleNamespace(
                        recovery_case_id=f"newer-terminal-{index}",
                        sleeve_id="family_0",
                        status=application_services.RecoveryCaseStatus.CLOSED,
                        triggered_at=now + timedelta(minutes=index),
                    )
                    for index in range(1_001)
                ),
                self.active_case,
            ]

        def database_now(self):
            return now

        def list_runs(self, *, limit, run_type=None, **_kwargs):
            assert limit == 1_000
            return [row for row in self.runs.values() if row.run_type == run_type]

        def list_research_submissions(self, *, limit, **_kwargs):
            assert limit == 1_000
            return []

        def iter_recovery_cases(
            self, *, statuses, sleeve_id=None, batch_size
        ):
            assert batch_size == 1_000
            allowed = set(statuses)
            return iter(
                case
                for case in self.recovery_cases
                if case.status in allowed
                and (sleeve_id is None or case.sleeve_id == sleeve_id)
            )

        def list_research_families(self, *, active_only):
            assert active_only
            return [
                SimpleNamespace(family_id=f"family_{index}", registry_hash=str(index) * 64)
                for index in range(4)
            ]

        def get_run(self, run_id):
            return self.runs.get(run_id)

        def claim_run(self, run):
            existing = self.runs.get(run.run_id)
            if existing is not None:
                return existing, False
            self.runs[run.run_id] = run
            return run, True

    class Coordinator:
        def __init__(self) -> None:
            self.calls = []

        def propose(self, _port, *, family_id, recovery_case_id):
            expected_recovery = (
                "older-active-family-0" if family_id == "family_0" else None
            )
            assert recovery_case_id == expected_recovery
            self.calls.append((family_id, recovery_case_id))
            decision = SimpleNamespace(
                decision_id=f"decision_{family_id}",
                raw_proposal_hash=(family_id[-1] or "0") * 64,
            )
            return SimpleNamespace(
                decision=decision,
                accepted=False,
                violations=("deterministic_test_rejection",),
                submission=None,
            )

    port = SimpleNamespace(public_identity={"model": "test-direct-model"})
    monkeypatch.setattr(
        application_services,
        "proposal_port_from_config",
        lambda *_args, **_kwargs: port,
    )
    service = object.__new__(ApplicationServices)
    service.catalog = Catalog()
    service.monthly_research = Coordinator()
    service.env = {}
    request = OperationRequest(
        operation=OperationName.CONFIRMATORY_BUDGET_GATE,
        cycle=CycleName.MONTHLY,
        partition_key="2026-08",
        run_id="automatic-proposals",
    )

    first = service._automatic_monthly_proposals(
        request, section={"proposal": {"provider": "direct_model"}}
    )
    second = service._automatic_monthly_proposals(
        request, section={"proposal": {"provider": "direct_model"}}
    )

    assert len(first) == 3
    assert second == ()
    assert service.monthly_research.calls == [
        ("family_0", "older-active-family-0"),
        ("family_1", None),
        ("family_2", None),
    ]
    assert len({family for family, _case in service.monthly_research.calls}) == 3
    assert len(service.catalog.runs) == 3


def test_recovery_sla_sensor_sees_older_active_case_beyond_terminal_window() -> None:
    now = datetime(2026, 8, 23, 8, tzinfo=timezone.utc)
    active = SimpleNamespace(
        recovery_case_id="older-active-recovery",
        sleeve_id="value_quality",
        status=application_services.RecoveryCaseStatus.OBSERVING,
        drift_event_due_at=now - timedelta(days=2),
        diagnosis_due_at=now - timedelta(days=1),
        earliest_recovery_review_at=now - timedelta(hours=1),
    )
    cases = [
        *(
            SimpleNamespace(
                recovery_case_id=f"newer-terminal-{index}",
                sleeve_id="value_quality",
                status=application_services.RecoveryCaseStatus.CLOSED,
            )
            for index in range(1_001)
        ),
        active,
    ]

    class Catalog:
        def iter_recovery_cases(
            self, *, statuses, sleeve_id=None, batch_size
        ):
            assert batch_size == 1_000
            allowed = set(statuses)
            return iter(
                case
                for case in cases
                if case.status in allowed
                and (sleeve_id is None or case.sleeve_id == sleeve_id)
            )

    service = object.__new__(ApplicationServices)
    service.catalog = Catalog()
    service._now = lambda: now

    poll = service._poll_recovery_sla(None)

    assert len(poll.triggers) == 3
    assert {trigger.metadata["recovery_case_id"] for trigger in poll.triggers} == {
        active.recovery_case_id
    }


def test_recovery_safety_paths_fail_closed_on_duplicate_active_cases() -> None:
    now = datetime(2026, 8, 23, 8, tzinfo=timezone.utc)
    cases = tuple(
        SimpleNamespace(
            recovery_case_id=f"duplicate-active-{index}",
            sleeve_id="value_quality",
            status=application_services.RecoveryCaseStatus.OPEN,
            drift_event_due_at=now,
            diagnosis_due_at=now,
            earliest_recovery_review_at=now,
        )
        for index in range(2)
    )

    class Catalog:
        def iter_recovery_cases(self, **_kwargs):
            return iter(cases)

    service = object.__new__(ApplicationServices)
    service.catalog = Catalog()
    service._now = lambda: now

    with pytest.raises(OrchestrationFailure, match="multiple active recovery cases"):
        service._poll_recovery_sla(None)


def test_missing_direct_model_config_is_blocked_and_recorded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = application_services.datetime(
        2026, 8, 23, 8, tzinfo=application_services.timezone.utc
    )

    class Catalog:
        def __init__(self) -> None:
            self.runs = {}

        def database_now(self):
            return now

        def claim_run(self, run):
            existing = self.runs.get(run.run_id)
            if existing is not None:
                return existing, False
            self.runs[run.run_id] = run
            return run, True

    monkeypatch.setattr(
        application_services,
        "proposal_port_from_config",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            application_services.ProposalPortError("no direct-model profile configured")
        ),
    )
    service = object.__new__(ApplicationServices)
    service.catalog = Catalog()
    service.env = {}
    request = OperationRequest(
        operation=OperationName.CONFIRMATORY_BUDGET_GATE,
        cycle=CycleName.MONTHLY,
        partition_key="2026-08",
        run_id="missing-model",
    )

    first = service._automatic_monthly_proposals(
        request,
        section={
            "proposal": {
                "provider": "direct_model",
                "max_proposals_per_month": 3,
                "max_proposals_per_family": 1,
            }
        },
    )
    second = service._automatic_monthly_proposals(
        request,
        section={
            "proposal": {
                "provider": "direct_model",
                "max_proposals_per_month": 3,
                "max_proposals_per_family": 1,
            }
        },
    )

    assert first == second
    assert first[0]["status"] == "blocked"
    assert first[0]["configuration_available"] is False
    assert first[0]["accepted"] is False
    assert len(service.catalog.runs) == 1
    record = next(iter(service.catalog.runs.values()))
    assert record.run_type == "monthly_model_proposal"
    assert record.status == "blocked"
    assert record.error == (
        "automatic monthly direct-model proposal is unavailable: "
        "no direct-model profile configured"
    )


def test_production_monthly_rejects_file_or_caller_statistics(tmp_path: Path) -> None:
    service = object.__new__(ApplicationServices)
    service.config = {
        "schema_version": APPLICATION_SERVICES_SCHEMA_VERSION,
        "monthly": {
            "input_mode": "authoritative_pg",
            "research_inputs": {"frame_path": "untrusted.parquet"},
        },
    }
    service._state_root = tmp_path
    request = OperationRequest(
        operation=OperationName.CONFIRMATORY_BUDGET_GATE,
        cycle=CycleName.MONTHLY,
        partition_key="2026-08",
        run_id="monthly",
    )
    with pytest.raises(OrchestrationFailure, match="rejects caller/file evidence"):
        service._monthly_payload(request)


def test_production_operation_admission_ignores_operator_metadata() -> None:
    service = object.__new__(ApplicationServices)
    service.settings = SimpleNamespace(environment="production")
    service.production_config_evidence = SimpleNamespace(
        historical_backfill_allowed=False,
        formal_execution_capable=False,
        provenance=SimpleNamespace(formal_epoch_eligible=False),
    )
    service._execute_admitted = lambda request: SimpleNamespace(
        operation=request.operation, status="completed"
    )
    disguised = OperationRequest(
        operation=OperationName.SOURCE_SYNC,
        cycle=CycleName.DAILY,
        partition_key="2026-08-23",
        run_id="disguised-canary",
        metadata={"operation": "engineering_canary", "evidence_class": "canary"},
    )

    with pytest.raises(OrchestrationFailure, match="formal_forward_activation"):
        service.execute(disguised)
    with pytest.raises(
        OrchestrationFailure, match="tushare_token_post_exposure_rotation_pending"
    ):
        service.execute_authoritative_backfill(disguised)
    with pytest.raises(
        OrchestrationFailure, match="tushare_token_post_exposure_rotation_pending"
    ):
        service.execute_engineering_canary(disguised)


def test_formal_runtime_gate_binds_ready_audit_to_host_deployment_and_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 8, 23, 12, tzinfo=timezone.utc)
    provenance = SimpleNamespace(
        architecture_version="research-os/application-services/v1",
        code_hash="a" * 64,
        configuration_hash="b" * 64,
        dependency_lock_hash="c" * 64,
    )
    oci = SimpleNamespace(
        code="daemon_inspected_oci_provenance",
        passed=True,
        evidence={
            "epoch_fields": {
                "architecture_version": provenance.architecture_version,
                "code_hash": provenance.code_hash,
                "configuration_hash": provenance.configuration_hash,
                "dependency_lock_hash": provenance.dependency_lock_hash,
                "dirty_patch_hash": "d" * 64,
            },
            "build_identity_hash": "e" * 64,
            "oci_image_id": "sha256:" + "f" * 64,
            "host_attestation_hash": "1" * 64,
            "attested_at": (now - timedelta(minutes=1)).isoformat(),
            "container_id": "2" * 64,
            "compose_config_hash": "compose-v1",
            "deployment_identity_hash": "3" * 64,
        },
    )
    soak = SimpleNamespace(
        code="dagster_code_location_24h_soak",
        passed=True,
        evidence={"process_identity": "4" * 64},
    )
    audit = SimpleNamespace(
        ready=True,
        blockers=(),
        audited_at=now - timedelta(minutes=5),
        checks=(oci, soak),
    )
    service = object.__new__(ApplicationServices)
    service.catalog = SimpleNamespace(database_now=lambda: now)
    service.production_ledger = SimpleNamespace(list_incidents=lambda **_kwargs: ())
    service.production_config_evidence = SimpleNamespace(
        provenance=provenance,
        historical_backfill_allowed=True,
        credential_rotation_blockers=(),
    )
    service.latest_production_readiness_audit = lambda: audit
    monkeypatch.setattr(
        "factor_lab.research_os.soak_monitor.local_code_server_process_identity",
        lambda: "4" * 64,
    )

    assert service._runtime_readiness_blockers() == ()

    oci.evidence["attested_at"] = (now - timedelta(minutes=11)).isoformat()
    assert "production_readiness_host_attestation_stale" in (
        service._runtime_readiness_blockers()
    )


def test_soak_sampling_accepts_old_attestation_only_for_same_verified_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 8, 23, 12, tzinfo=timezone.utc)
    host_run = SimpleNamespace(
        run_id="docker_attestation_" + "1" * 64,
        # Deliberately older than the ten-minute formal-admission window.  The
        # local binder below represents the independently verified unchanged
        # container/PID-1/source continuity needed by the soak.
        completed_at=now - timedelta(hours=2),
    )
    deployment = SimpleNamespace(
        build_identity_hash="2" * 64,
        oci_image_id="sha256:" + "3" * 64,
        deployment_identity_hash="4" * 64,
        host_attestation_hash="5" * 64,
        host_attestation_run_id=host_run.run_id,
        container_id="6" * 64,
        compose_config_hash="compose-v1",
        process_identity="7" * 64,
    )
    observed: dict[str, object] = {}

    class Auditor:
        @staticmethod
        def _latest_host_docker_attempt():
            return host_run, (), {}

    class Monitor:
        def __init__(self, _catalog, _ledger, **kwargs):
            observed.update(kwargs)

        @staticmethod
        def record_sample():
            return SimpleNamespace(
                run_id="dagster_health_sample_" + "8" * 64,
                metadata={
                    "sampled_at": now.isoformat(),
                    "sample_evidence_hash": "8" * 64,
                    "process_identity": deployment.process_identity,
                },
            )

        @staticmethod
        def finalize(*, provenance):
            assert provenance.build_identity_hash == deployment.build_identity_hash
            raise application_services.DagsterSoakIncomplete("still accumulating")

    service = object.__new__(ApplicationServices)
    service.catalog = object()
    service.production_ledger = object()
    service.production_config_evidence = SimpleNamespace(provenance=object())
    service._production_readiness_auditor = lambda: Auditor()
    # A soak sample must not recursively create/read a fresh all-green audit;
    # the 24-hour soak check is itself part of that audit.
    service.audit_production_readiness = lambda: pytest.fail(
        "soak sampling must not require a fresh readiness audit"
    )
    monkeypatch.setattr(
        application_services,
        "bind_current_code_server_to_host_attestation",
        lambda run, *, provenance: (
            deployment
            if run is host_run and provenance is service.production_config_evidence.provenance
            else pytest.fail("unexpected host binding inputs")
        ),
    )
    monkeypatch.setattr(
        application_services, "DagsterCodeLocationSoakMonitor", Monitor
    )

    result = service.record_dagster_code_location_health()

    assert result["status"] == "recorded"
    assert result["soak_status"] == "accumulating"
    assert observed["container_id"] == deployment.container_id
    assert observed["process_identity"] == deployment.process_identity
    assert observed["build_identity_hash"] == deployment.build_identity_hash


def test_soak_sampling_stops_after_later_failed_host_attempt() -> None:
    class Auditor:
        @staticmethod
        def _latest_host_docker_attempt():
            return None, ("host_docker_attestation_latest_attempt_failed",), {}

    service = object.__new__(ApplicationServices)
    service.catalog = object()
    service.production_ledger = object()
    service.production_config_evidence = SimpleNamespace(provenance=object())
    service._production_readiness_auditor = lambda: Auditor()

    assert service.record_dagster_code_location_health() == {
        "status": "skipped",
        "reason": "matching daemon-inspected OCI readiness attestation is unavailable",
    }


def test_trading_partition_poll_reads_only_canonical_ledger() -> None:
    class Ledger:
        def accepted_calendar_partitions(self, *, after_partition_key=None):
            assert after_partition_key == "2026-08-21"
            return ("2026-08-24", "2026-08-25")

    service = object.__new__(ApplicationServices)
    service.production_ledger = Ledger()
    poll = service._poll_trading_partitions("2026-08-21")
    assert [item.partition_key for item in poll.triggers] == [
        "2026-08-24",
        "2026-08-25",
    ]
    assert poll.cursor == "2026-08-25"


def test_open_schedule_selects_only_today_from_accepted_calendar() -> None:
    shanghai = application_services.ZoneInfo("Asia/Shanghai")

    class Ledger:
        @staticmethod
        def accepted_calendar_partitions():
            return ("2026-08-24",)

    service = object.__new__(ApplicationServices)
    service.production_ledger = Ledger()
    service.catalog = SimpleNamespace(
        database_now=lambda: datetime(
            2026, 8, 24, 9, 30, 1, tzinfo=shanghai
        )
    )

    scheduled = datetime(2026, 8, 24, 9, 30, tzinfo=shanghai)
    assert service.accepted_execution_open_partition(scheduled) == "2026-08-24"

    service.catalog = SimpleNamespace(
        database_now=lambda: datetime(2026, 8, 25, 9, 30, tzinfo=shanghai)
    )
    assert service.accepted_execution_open_partition(scheduled) is None


def test_failure_stage_is_derived_from_durable_partition_progress() -> None:
    class Ledger:
        def get_partition(self, identity: PartitionIdentity):
            if identity.dataset in {"stage_source", "stage_silver"}:
                return SimpleNamespace(
                    status=PartitionStatus.SUCCEEDED,
                    output_hash=("a" if identity.dataset == "stage_source" else "b")
                    * 64,
                )
            return None

    service = object.__new__(ApplicationServices)
    service.production_ledger = Ledger()
    incident_stage, pipeline_stage, partition_run_id, evidence = (
        service._infer_failed_data_stage("2026-08-23")
    )
    assert incident_stage is IncidentStage.DATA_QUALITY
    assert pipeline_stage.value == "data_quality"
    assert partition_run_id == PartitionIdentity(
        "research_os", "stage_data_quality", "2026-08-23"
    ).partition_run_id
    assert evidence == ("a" * 64, "b" * 64)


def test_blocked_shadow_envelope_cannot_override_all_green_stage_facts() -> None:
    class Ledger:
        def get_partition(self, identity: PartitionIdentity):
            return SimpleNamespace(
                status=PartitionStatus.SUCCEEDED,
                output_hash={
                    "stage_source": "a",
                    "stage_silver": "b",
                    "stage_data_quality": "c",
                    "stage_gold": "d",
                    "stage_shadow": "e",
                }[identity.dataset]
                * 64,
            )

    service = object.__new__(ApplicationServices)
    service.production_ledger = Ledger()

    with pytest.raises(OrchestrationFailure, match="every durable stage succeeded"):
        service._infer_failed_data_stage(
            "2026-08-23", require_incomplete=True
        )


def test_same_day_open_decoy_cannot_bypass_all_green_stage_authority() -> None:
    class Ledger:
        def get_partition(self, identity: PartitionIdentity):
            return SimpleNamespace(
                status=PartitionStatus.SUCCEEDED,
                output_hash={
                    "stage_source": "a",
                    "stage_silver": "b",
                    "stage_data_quality": "c",
                    "stage_gold": "d",
                    "stage_shadow": "e",
                }[identity.dataset]
                * 64,
            )

        @staticmethod
        def iter_incidents(**_kwargs):
            return iter(
                (
                    SimpleNamespace(
                        incident_id="incident_canary_decoy",
                        partition_key="2026-08-23",
                        stage=IncidentStage.SOURCE,
                        payload={},
                    ),
                )
            )

    service = object.__new__(ApplicationServices)
    service.production_ledger = Ledger()

    with pytest.raises(OrchestrationFailure, match="every durable stage succeeded"):
        service.report_unexpected_data_failure(
            "2026-08-23",
            message="caller claimed a source failure",
            occurred_at=datetime(2026, 8, 23, 10, tzinfo=timezone.utc),
            dagster_run_id="all-green-decoy-regression",
            failed_step_key="source_sync",
            error_code="SourceUnavailable",
            expected_failure_stage="source",
        )


def test_source_failure_shadow_branch_immediately_freezes_and_persists_cash_intent(
    tmp_path: Path,
) -> None:
    database = tmp_path / "direct-daily-incident.db"
    catalog = ResearchCatalog(database)
    catalog.initialize_schema()
    engine = create_engine(f"sqlite:///{database}")
    orm.Base.metadata.create_all(engine)
    ledger = ProductionLedger(engine)
    occurred_at = datetime(2026, 8, 23, 10, tzinfo=timezone.utc)
    identity = PartitionIdentity(
        "research_os", "stage_source", "2026-08-23"
    )
    ledger.ensure_partition(
        identity,
        created_at=occurred_at,
        details={"dagster_run_id": "daily-source-failure"},
    )
    lease = ledger.claim(
        identity=identity,
        owner="source-worker",
        now=occurred_at,
        lease_for=timedelta(minutes=5),
    )
    assert lease is not None
    ledger.finish(
        lease,
        status=PartitionStatus.FAILED,
        completed_at=occurred_at,
        details={"dagster_run_id": "daily-source-failure"},
        error_code="SourceUnavailable",
        error="provider unavailable",
    )
    catalog.create_shadow_account(
        account_id="champion-shadow",
        name="Champion shadow",
        initial_capital=50_000_000,
        opened_at=occurred_at - timedelta(days=1),
    )
    service = object.__new__(ApplicationServices)
    service.catalog = catalog
    service.production_ledger = ledger
    service.shadow_authority = None
    service.sleeve_roster = load_sleeve_roster(
        Path(__file__).resolve().parents[1]
        / "configs"
        / "research_os_initial_sleeves.json"
    )
    service._now = lambda: occurred_at
    request = OperationRequest(
        operation=OperationName.SHADOW_NAV_STEP,
        cycle=CycleName.DAILY,
        partition_key="2026-08-23",
        run_id="daily-source-failure",
        metadata={
            "daily_data_outcome": {
                "partition_key": "2026-08-23",
                "status": "blocked",
                "failure_stage": "source",
                "error_code": "SourceUnavailable",
                "message": "source chain is unavailable",
                "occurred_at": occurred_at.isoformat(),
            }
        },
    )

    try:
        first = service._shadow_nav_step(request)
        second = service._shadow_nav_step(request)

        assert first.status == second.status == "completed"
        assert first.outputs["risk_guard"] == "cash_target_intent"
        assert first.outputs["incident"]["reused"] is False
        incidents = ledger.list_incidents(limit=100)
        assert len(incidents) == 1
        assert incidents[0].stage is IncidentStage.SOURCE
        assert second.outputs["incident"]["reused"] is True
        for entry in service.sleeve_roster.entries:
            state = catalog.latest_lifecycle_state(entry.sleeve.sleeve_id)
            assert state is not None and state.value == "frozen_data"
        events = catalog.list_shadow_events(
            account_id="champion-shadow", limit=100
        )
        assert sum(event.event_type == "data_incident" for event in events) == 1
        assert sum(event.event_type == "cash_target_intent" for event in events) == 1
        assert not any(event.event_type == "fill" for event in events)
        account = catalog.get_shadow_account("champion-shadow")
        assert account is not None
        assert account.cash == account.nav == 50_000_000
    finally:
        ledger.close()
        catalog.close()
        engine.dispose()


def test_same_day_canary_open_cannot_swallow_exact_domain_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "canary-open-domain-incident.db"
    catalog = ResearchCatalog(database)
    catalog.initialize_schema()
    engine = create_engine(f"sqlite:///{database}")
    orm.Base.metadata.create_all(engine)
    ledger = ProductionLedger(engine)
    occurred_at = datetime(2026, 8, 23, 10, tzinfo=timezone.utc)
    partition_key = "2026-08-23"
    identity = PartitionIdentity("research_os", "stage_source", partition_key)
    ledger.ensure_partition(
        identity,
        created_at=occurred_at,
        details={"dagster_run_id": "domain-failure-not-canary"},
    )
    lease = ledger.claim(
        identity=identity,
        owner="source-worker",
        now=occurred_at,
        lease_for=timedelta(minutes=5),
    )
    assert lease is not None
    ledger.finish(
        lease,
        status=PartitionStatus.FAILED,
        completed_at=occurred_at,
        details={"dagster_run_id": "domain-failure-not-canary"},
        error_code="SourceUnavailable",
        error="provider unavailable",
    )
    canary = ledger.record_incident(
        partition_key=partition_key,
        stage=IncidentStage.SOURCE,
        error_code="source_contract_violation",
        message="legacy physical canary contract failed",
        occurred_at=occurred_at - timedelta(minutes=1),
        source_ids=("engineering_canary_legacy",),
        payload={
            "evidence_class": "engineering_canary",
            "legacy_source_id": "engineering_canary_legacy",
        },
    )
    catalog.create_shadow_account(
        account_id="champion-shadow",
        name="Champion shadow",
        initial_capital=50_000_000,
        opened_at=occurred_at - timedelta(days=1),
    )
    service = object.__new__(ApplicationServices)
    service.catalog = catalog
    service.production_ledger = ledger
    service.shadow_authority = None
    service.sleeve_roster = load_sleeve_roster(
        Path(__file__).resolve().parents[1]
        / "configs"
        / "research_os_initial_sleeves.json"
    )
    try:
        request = {
            "message": "source chain is unavailable",
            "occurred_at": occurred_at,
            "dagster_run_id": "domain-failure-not-canary",
            "failed_step_key": "source_sync",
            "error_code": "SourceUnavailable",
            "expected_failure_stage": "source",
        }
        original_run = application_services.ProductionDailyControl.run

        def crash_after_reservation(*_args, **_kwargs):
            raise RuntimeError("simulated crash after durable incident reservation")

        monkeypatch.setattr(
            application_services.ProductionDailyControl,
            "run",
            crash_after_reservation,
        )
        with pytest.raises(
            RuntimeError,
            match="simulated crash after durable incident reservation",
        ):
            service.report_unexpected_data_failure(partition_key, **request)
        reserved = [
            item
            for item in ledger.list_incidents(limit=100)
            if item.incident_id != canary.incident_id
        ]
        assert len(reserved) == 1
        assert reserved[0].status is IncidentStatus.OPEN
        assert catalog.list_lifecycle_events(limit=100) == []
        assert not any(
            event.event_type in {"data_incident", "cash_target_intent"}
            for event in catalog.list_shadow_events(
                account_id="champion-shadow", limit=100
            )
        )

        monkeypatch.setattr(
            application_services.ProductionDailyControl,
            "run",
            original_run,
        )
        first = service.report_unexpected_data_failure(partition_key, **request)
        repeated = service.report_unexpected_data_failure(partition_key, **request)

        assert first["reused"] is True
        assert repeated["reused"] is True
        assert first["incident_id"] == repeated["incident_id"] != canary.incident_id
        assert first["cash_intent_accounts"] == ["champion-shadow"]
        assert repeated["cash_intent_accounts"] == ["champion-shadow"]
        incidents = ledger.list_incidents(limit=100)
        assert len(incidents) == 2
        assert {item.incident_id for item in incidents} == {
            canary.incident_id,
            first["incident_id"],
        }
        domain = next(
            item for item in incidents if item.incident_id == first["incident_id"]
        )
        assert domain.status is IncidentStatus.OPEN
        assert domain.payload == {
            "dagster_run_id": "domain-failure-not-canary",
            "failed_step_key": "source_sync",
            "domain_incident_id": first["domain_incident_id"],
            "failed_partition_input_hash": None,
        }
        assert str(first["domain_incident_id"]).startswith("dinc_")
        for entry in service.sleeve_roster.entries:
            state = catalog.latest_lifecycle_state(entry.sleeve.sleeve_id)
            assert state is not None and state.value == "frozen_data"
        events = catalog.list_shadow_events(
            account_id="champion-shadow", limit=100
        )
        assert sum(event.event_type == "data_incident" for event in events) == 1
        assert sum(event.event_type == "cash_target_intent" for event in events) == 1
        with pytest.raises(
            ImmutablePartition,
            match="typed five-stage revalidation",
        ):
            ledger.resolve_incident(
                first["incident_id"],
                resolved_at=occurred_at + timedelta(minutes=1),
                evidence={"fixture": "direct domain close is forbidden"},
            )

        catalog.create_shadow_account(
            account_id="challenger-shadow",
            name="Challenger shadow",
            initial_capital=50_000_000,
            opened_at=occurred_at - timedelta(days=1),
        )
        partial_partition_key = "2026-08-24"
        partial_at = occurred_at + timedelta(days=1)
        partial_run_id = "domain-failure-partial-replay"
        partial_identity = PartitionIdentity(
            "research_os", "stage_source", partial_partition_key
        )
        ledger.ensure_partition(
            partial_identity,
            created_at=partial_at,
            details={"dagster_run_id": partial_run_id},
        )
        partial_lease = ledger.claim(
            identity=partial_identity,
            owner="partial-source-worker",
            now=partial_at,
            lease_for=timedelta(minutes=5),
        )
        assert partial_lease is not None
        ledger.finish(
            partial_lease,
            status=PartitionStatus.FAILED,
            completed_at=partial_at,
            details={"dagster_run_id": partial_run_id},
            error_code="SourceUnavailable",
            error="provider unavailable",
        )
        partial_request = {
            **request,
            "message": "second source failure exercises partial replay",
            "occurred_at": partial_at,
            "dagster_run_id": partial_run_id,
            "failed_step_key": "source_sync_partial_replay",
        }

        def crash_after_partial_controls(
            control,
            *,
            outcome,
            lifecycle_records,
            shadow_accounts,
            **_kwargs,
        ):
            first_record = lifecycle_records[0]
            application_services.DataIncidentCoordinator(control.catalog).report(
                outcome.to_incident(),
                lifecycle_records=(first_record,),
                shadow_accounts={
                    first_record.sleeve_id: ("champion-shadow",)
                },
            )
            raise RuntimeError("simulated crash after partial incident controls")

        monkeypatch.setattr(
            application_services.ProductionDailyControl,
            "run",
            crash_after_partial_controls,
        )
        with pytest.raises(
            RuntimeError,
            match="simulated crash after partial incident controls",
        ):
            service.report_unexpected_data_failure(
                partial_partition_key, **partial_request
            )
        partial_authority = next(
            item
            for item in ledger.list_incidents(limit=100)
            if item.payload.get("dagster_run_id")
            == "domain-failure-partial-replay"
        )
        partial_domain_id = str(
            partial_authority.payload["domain_incident_id"]
        )
        partial_lifecycle = [
            event
            for event in catalog.list_lifecycle_events(limit=100)
            if event.evidence.get("data_incident", {}).get("incident_id")
            == partial_domain_id
        ]
        assert len(partial_lifecycle) == 1
        assert any(
            event.payload.get("incident_id") == partial_domain_id
            for event in catalog.list_shadow_events(
                account_id="champion-shadow", limit=100
            )
            if event.event_type == "cash_target_intent"
        )
        assert not any(
            event.payload.get("incident_id") == partial_domain_id
            for event in catalog.list_shadow_events(
                account_id="challenger-shadow", limit=100
            )
            if event.event_type == "cash_target_intent"
        )

        monkeypatch.setattr(
            application_services.ProductionDailyControl,
            "run",
            original_run,
        )
        recovered = service.report_unexpected_data_failure(
            partial_partition_key, **partial_request
        )
        assert recovered["reused"] is True
        assert recovered["cash_intent_accounts"] == [
            "challenger-shadow",
            "champion-shadow",
        ]
        partial_lifecycle = [
            event
            for event in catalog.list_lifecycle_events(limit=100)
            if event.evidence.get("data_incident", {}).get("incident_id")
            == partial_domain_id
        ]
        assert len(partial_lifecycle) == len(service.sleeve_roster.entries)
        for account_id in ("champion-shadow", "challenger-shadow"):
            assert sum(
                event.payload.get("incident_id") == partial_domain_id
                for event in catalog.list_shadow_events(
                    account_id=account_id, limit=100
                )
                if event.event_type == "cash_target_intent"
            ) == 1

        lifecycle_event = partial_lifecycle[0]
        malformed_evidence = {
            **lifecycle_event.evidence,
            "cash_target_intent": {
                **lifecycle_event.evidence["cash_target_intent"],
                "cash_weight": 0.5,
            },
        }
        with engine.begin() as connection:
            connection.execute(
                update(orm.LifecycleEventModel)
                .where(
                    orm.LifecycleEventModel.event_id
                    == lifecycle_event.event_id
                )
                .values(evidence_json=malformed_evidence)
            )
        with pytest.raises(
            OrchestrationFailure,
            match="lifecycle evidence is inconsistent",
        ):
            service.report_unexpected_data_failure(
                partial_partition_key, **partial_request
            )
        with engine.begin() as connection:
            connection.execute(
                update(orm.LifecycleEventModel)
                .where(
                    orm.LifecycleEventModel.event_id
                    == lifecycle_event.event_id
                )
                .values(evidence_json=lifecycle_event.evidence)
            )

        malformed_event = next(
            event
            for event in catalog.list_shadow_events(
                account_id="challenger-shadow", limit=100
            )
            if event.event_type == "cash_target_intent"
            and event.payload.get("incident_id") == partial_domain_id
        )
        malformed_payload = {
            **malformed_event.payload,
            "cash_weight": 0.5,
        }
        with engine.begin() as connection:
            connection.execute(
                update(orm.ShadowEventModel)
                .where(
                    orm.ShadowEventModel.event_id
                    == malformed_event.event_id
                )
                .values(payload_json=malformed_payload)
            )
        with pytest.raises(
            OrchestrationFailure,
            match="lacks current fleet cash intents",
        ):
            service.report_unexpected_data_failure(
                partial_partition_key, **partial_request
            )
    finally:
        ledger.close()
        catalog.close()
        engine.dispose()


@pytest.mark.parametrize(
    "use_failed_generic_successor",
    (False, True),
    ids=("failed-base", "failed-generic-successor"),
)
def test_revalidation_recovers_catalog_commit_before_incident_cas_and_is_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    use_failed_generic_successor: bool,
) -> None:
    catalog_database = tmp_path / "revalidation-catalog.db"
    ledger_database = tmp_path / "revalidation-ledger.db"
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(catalog_database)
    catalog.initialize_schema()
    ledger_engine = create_engine(f"sqlite:///{ledger_database.as_posix()}")
    orm.Base.metadata.create_all(ledger_engine)
    ledger = ProductionLedger(ledger_engine)
    incident_at = datetime(2026, 8, 23, 10, tzinfo=timezone.utc)
    partition_key = "2026-08-23"
    identity = PartitionIdentity("research_os", "stage_source", partition_key)
    ledger.ensure_partition(
        identity,
        created_at=incident_at,
        input_hash=content_fingerprint(
            {"partition_key": partition_key, "dataset": "stage_source"},
            domain="test/revalidation-original-input",
        ),
        details={"dagster_run_id": "revalidation-crash-source"},
    )
    lease = ledger.claim(
        identity=identity,
        owner="source-worker",
        now=incident_at,
        lease_for=timedelta(minutes=5),
    )
    assert lease is not None
    ledger.finish(
        lease,
        status=PartitionStatus.FAILED,
        completed_at=incident_at,
        details={"dagster_run_id": "revalidation-crash-source"},
        error_code="SourceUnavailable",
        error="provider unavailable",
    )
    failure_at = incident_at
    if use_failed_generic_successor:
        failure_at = incident_at + timedelta(minutes=2)
        retry_authority = ledger.reserve_retry_successor(
            identity,
            repair_fingerprint=content_fingerprint(
                {"partition_key": partition_key, "attempt": "generic-retry"},
                domain="test/revalidation-generic-retry",
            ),
            created_at=incident_at + timedelta(minutes=1),
            input_hash=content_fingerprint(
                {"partition_key": partition_key, "dataset": "stage_source"},
                domain="test/revalidation-original-input",
            ),
            details={"dagster_run_id": "revalidation-crash-source"},
        )
        retry_lease = ledger.claim(
            identity=retry_authority.identity,
            owner="source-retry-worker",
            now=incident_at + timedelta(minutes=1),
            lease_for=timedelta(minutes=5),
        )
        assert retry_lease is not None
        ledger.finish(
            retry_lease,
            status=PartitionStatus.FAILED,
            completed_at=failure_at,
            details={"dagster_run_id": "revalidation-crash-source"},
            error_code="SourceUnavailable",
            error="provider remains unavailable",
        )
    for account_id in ("champion-shadow", "challenger-shadow"):
        catalog.create_shadow_account(
            account_id=account_id,
            name=account_id,
            initial_capital=50_000_000,
            opened_at=incident_at - timedelta(days=1),
        )
    service = object.__new__(ApplicationServices)
    service.catalog = catalog
    service.production_ledger = ledger
    service.shadow_authority = None
    service.settings = SimpleNamespace(lake_root=lake_root)
    service.sleeve_roster = load_sleeve_roster(
        Path(__file__).resolve().parents[1]
        / "configs"
        / "research_os_initial_sleeves.json"
    )
    repair_accounts = ("champion-shadow", "challenger-shadow")
    _install_fixture_shadow_roles(service, repair_accounts)

    try:
        reported = service.report_unexpected_data_failure(
            partition_key,
            message="source chain is unavailable",
            occurred_at=failure_at,
            dagster_run_id="revalidation-crash-source",
            failed_step_key="source_sync",
            error_code="SourceUnavailable",
            expected_failure_stage="source",
        )
        gold = _register_revalidation_gold(
            catalog=catalog,
            ledger=ledger,
            lake_root=lake_root,
            partition_key=partition_key,
            incident_at=failure_at,
            repair_incident_id=reported["incident_id"],
        )
        _finish_revalidation_shadow(
            catalog=catalog,
            ledger=ledger,
            repair_incident_id=reported["incident_id"],
            partition_key=partition_key,
            incident_at=failure_at,
            gold=gold,
            account_ids=repair_accounts,
        )
        revalidated_at = gold.as_of + timedelta(minutes=10)
        original_resolve = ledger._resolve_typed_data_incident_with_effects

        def crash_after_catalog_commit(
            requested_incident_id,
            *,
            resolved_at,
            evidence,
            apply_effects,
            superseded=False,
        ):
            assert requested_incident_id == reported["incident_id"]
            assert resolved_at == revalidated_at
            assert superseded is False
            open_rows = tuple(ledger.iter_incidents(status=IncidentStatus.OPEN))
            authority = next(
                item
                for item in open_rows
                if item.incident_id == requested_incident_id
            )
            other_open = tuple(
                item
                for item in open_rows
                if item.incident_id != requested_incident_id
            )
            assert callable(evidence)
            evidence(authority, other_open)
            apply_effects(authority, other_open)
            raise RuntimeError("simulated crash after catalog effects")

        monkeypatch.setattr(
            ledger,
            "_resolve_typed_data_incident_with_effects",
            crash_after_catalog_commit,
        )
        with pytest.raises(
            RuntimeError, match="simulated crash after catalog effects"
        ):
            service.revalidate_data_incident(
                incident_id=reported["incident_id"],
                snapshot_id=gold.snapshot_id,
                occurred_at=revalidated_at,
            )
        authority = next(
            item
            for item in ledger.iter_incidents()
            if item.incident_id == reported["incident_id"]
        )
        assert authority.status is IncidentStatus.OPEN
        for entry in service.sleeve_roster.entries:
            assert (
                catalog.latest_lifecycle_state(entry.sleeve.sleeve_id).value
                == "dormant"
            )
        effective_records, _ = service._production_lifecycle_fleet()
        assert effective_records
        assert all(record.state.value == "frozen_data" for record in effective_records)
        lifecycle_count = sum(
            event.cause == "data_revalidation_passed"
            for event in catalog.list_lifecycle_events(limit=100)
        )
        shadow_counts = {
            account_id: sum(
                event.event_type == "data_revalidated"
                for event in catalog.list_shadow_events(
                    account_id=account_id, limit=100
                )
            )
            for account_id in ("champion-shadow", "challenger-shadow")
        }

        monkeypatch.setattr(
            ledger,
            "_resolve_typed_data_incident_with_effects",
            original_resolve,
        )
        recovered = service.revalidate_data_incident(
            incident_id=reported["incident_id"],
            snapshot_id=gold.snapshot_id,
            occurred_at=revalidated_at,
        )
        repeated = service.revalidate_data_incident(
            incident_id=reported["incident_id"],
            snapshot_id=gold.snapshot_id,
            occurred_at=revalidated_at,
        )

        assert recovered["status"] == repeated["status"] == "resolved"
        assert recovered["fleet_action"] == "restored_to_dormant"
        assert recovered["effects_applied"] is True
        assert repeated["effects_applied"] is False
        assert recovered["restored_sleeves"] == sorted(
            entry.sleeve.sleeve_id for entry in service.sleeve_roster.entries
        )
        assert recovered["revalidated_accounts"] == [
            "challenger-shadow",
            "champion-shadow",
        ]
        assert (
            sum(
                event.cause == "data_revalidation_passed"
                for event in catalog.list_lifecycle_events(limit=100)
            )
            == lifecycle_count
        )
        assert {
            account_id: sum(
                event.event_type == "data_revalidated"
                for event in catalog.list_shadow_events(
                    account_id=account_id, limit=100
                )
            )
            for account_id in ("champion-shadow", "challenger-shadow")
        } == shadow_counts

        with pytest.raises(
            OrchestrationFailure,
            match="timestamp differs from authority",
        ):
            service.revalidate_data_incident(
                incident_id=reported["incident_id"],
                snapshot_id=gold.snapshot_id,
                occurred_at=revalidated_at + timedelta(seconds=1),
            )
    finally:
        ledger.close()
        catalog.close()
        ledger_engine.dispose()


def test_revalidating_one_of_two_open_domain_incidents_keeps_fleet_frozen(
    tmp_path: Path,
) -> None:
    catalog_database = tmp_path / "multi-incident-catalog.db"
    ledger_database = tmp_path / "multi-incident-ledger.db"
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(catalog_database)
    catalog.initialize_schema()
    ledger_engine = create_engine(f"sqlite:///{ledger_database.as_posix()}")
    orm.Base.metadata.create_all(ledger_engine)
    ledger = ProductionLedger(ledger_engine)
    first_at = datetime(2026, 8, 22, 10, tzinfo=timezone.utc)
    second_at = datetime(2026, 8, 23, 10, tzinfo=timezone.utc)
    first_partition_key = "2026-08-22"
    second_partition_key = "2026-08-23"
    for occurred_at, selected_partition, run_id in (
        (first_at, first_partition_key, "first-domain-failure"),
        (second_at, second_partition_key, "second-domain-failure"),
    ):
        identity = PartitionIdentity(
            "research_os", "stage_source", selected_partition
        )
        ledger.ensure_partition(
            identity,
            created_at=occurred_at,
            input_hash=content_fingerprint(
                {
                    "partition_key": selected_partition,
                    "dataset": "stage_source",
                },
                domain="test/revalidation-original-input",
            ),
            details={"dagster_run_id": run_id},
        )
        lease = ledger.claim(
            identity=identity,
            owner=f"source-worker-{selected_partition}",
            now=occurred_at,
            lease_for=timedelta(minutes=5),
        )
        assert lease is not None
        ledger.finish(
            lease,
            status=PartitionStatus.FAILED,
            completed_at=occurred_at,
            details={"dagster_run_id": run_id},
            error_code="SourceUnavailable",
            error="provider unavailable",
        )
    catalog.create_shadow_account(
        account_id="champion-shadow",
        name="Champion shadow",
        initial_capital=50_000_000,
        opened_at=first_at - timedelta(days=1),
    )
    service = object.__new__(ApplicationServices)
    service.catalog = catalog
    service.production_ledger = ledger
    service.shadow_authority = None
    service.settings = SimpleNamespace(lake_root=lake_root)
    service.sleeve_roster = load_sleeve_roster(
        Path(__file__).resolve().parents[1]
        / "configs"
        / "research_os_initial_sleeves.json"
    )
    repair_accounts = ("champion-shadow",)
    _install_fixture_shadow_roles(service, repair_accounts)

    try:
        first = service.report_unexpected_data_failure(
            first_partition_key,
            message="first independent source failure",
            occurred_at=first_at,
            dagster_run_id="first-domain-failure",
            failed_step_key="source_sync:first",
            error_code="SourceUnavailable",
            expected_failure_stage="source",
        )
        second = service.report_unexpected_data_failure(
            second_partition_key,
            message="second independent source failure",
            occurred_at=second_at,
            dagster_run_id="second-domain-failure",
            failed_step_key="source_sync:second",
            error_code="SourceUnavailable",
            expected_failure_stage="source",
        )
        gold = _register_revalidation_gold(
            catalog=catalog,
            ledger=ledger,
            lake_root=lake_root,
            partition_key=first_partition_key,
            incident_at=second_at + timedelta(minutes=1),
            suffix="first-incident",
            repair_incident_id=first["incident_id"],
        )
        _finish_revalidation_shadow(
            catalog=catalog,
            ledger=ledger,
            repair_incident_id=first["incident_id"],
            partition_key=first_partition_key,
            incident_at=second_at + timedelta(minutes=1),
            gold=gold,
            account_ids=repair_accounts,
            suffix="first-incident",
            validation_trade_date=second_partition_key,
        )
        first_resolution_at = gold.as_of + timedelta(minutes=10)
        first_result = service.revalidate_data_incident(
            incident_id=first["incident_id"],
            snapshot_id=gold.snapshot_id,
            occurred_at=first_resolution_at,
        )

        assert first_result["fleet_action"] == "remained_frozen"
        assert first_result["blocking_incident_ids"] == [second["incident_id"]]
        assert first_result["restored_sleeves"] == []
        assert not any(
            event.event_type == "data_revalidated"
            for event in catalog.list_shadow_events(
                account_id="champion-shadow", limit=100
            )
        )
        for entry in service.sleeve_roster.entries:
            assert (
                catalog.latest_lifecycle_state(entry.sleeve.sleeve_id).value
                == "frozen_data"
            )

        second_gold = _register_revalidation_gold(
            catalog=catalog,
            ledger=ledger,
            lake_root=lake_root,
            partition_key=second_partition_key,
            incident_at=second_at + timedelta(minutes=30),
            suffix="second-incident",
            repair_incident_id=second["incident_id"],
        )
        _finish_revalidation_shadow(
            catalog=catalog,
            ledger=ledger,
            repair_incident_id=second["incident_id"],
            partition_key=second_partition_key,
            incident_at=second_at + timedelta(minutes=30),
            gold=second_gold,
            account_ids=repair_accounts,
            suffix="second-incident",
        )
        second_result = service.revalidate_data_incident(
            incident_id=second["incident_id"],
            snapshot_id=second_gold.snapshot_id,
            occurred_at=second_gold.as_of + timedelta(minutes=10),
        )
        assert second_result["fleet_action"] == "restored_to_dormant"
        assert second_result["blocking_incident_ids"] == []
        for entry in service.sleeve_roster.entries:
            assert (
                catalog.latest_lifecycle_state(entry.sleeve.sleeve_id).value
                == "dormant"
            )
        repeated_first = service.revalidate_data_incident(
            incident_id=first["incident_id"],
            snapshot_id=gold.snapshot_id,
            occurred_at=first_resolution_at,
        )
        assert repeated_first["fleet_action"] == "remained_frozen"
        assert repeated_first["effects_applied"] is False
    finally:
        ledger.close()
        catalog.close()
        ledger_engine.dispose()


def test_revalidation_rejects_canary_unrelated_gold_and_tampered_origin(
    tmp_path: Path,
) -> None:
    catalog_database = tmp_path / "revalidation-rejection-catalog.db"
    ledger_database = tmp_path / "revalidation-rejection-ledger.db"
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(catalog_database)
    catalog.initialize_schema()
    ledger_engine = create_engine(f"sqlite:///{ledger_database.as_posix()}")
    orm.Base.metadata.create_all(ledger_engine)
    ledger = ProductionLedger(ledger_engine)
    incident_at = datetime(2026, 8, 23, 10, tzinfo=timezone.utc)
    partition_key = "2026-08-23"
    identity = PartitionIdentity("research_os", "stage_source", partition_key)
    ledger.ensure_partition(
        identity,
        created_at=incident_at,
        input_hash=content_fingerprint(
            {"partition_key": partition_key, "dataset": "stage_source"},
            domain="test/revalidation-original-input",
        ),
        details={"dagster_run_id": "revalidation-reject-source"},
    )
    lease = ledger.claim(
        identity=identity,
        owner="source-worker",
        now=incident_at,
        lease_for=timedelta(minutes=5),
    )
    assert lease is not None
    ledger.finish(
        lease,
        status=PartitionStatus.FAILED,
        completed_at=incident_at,
        details={"dagster_run_id": "revalidation-reject-source"},
        error_code="SourceUnavailable",
        error="provider unavailable",
    )
    catalog.create_shadow_account(
        account_id="champion-shadow",
        name="Champion shadow",
        initial_capital=50_000_000,
        opened_at=incident_at - timedelta(days=1),
    )
    service = object.__new__(ApplicationServices)
    service.catalog = catalog
    service.production_ledger = ledger
    service.shadow_authority = None
    service.settings = SimpleNamespace(lake_root=lake_root)
    service.sleeve_roster = load_sleeve_roster(
        Path(__file__).resolve().parents[1]
        / "configs"
        / "research_os_initial_sleeves.json"
    )

    try:
        reported = service.report_unexpected_data_failure(
            partition_key,
            message="source chain is unavailable",
            occurred_at=incident_at,
            dagster_run_id="revalidation-reject-source",
            failed_step_key="source_sync",
            error_code="SourceUnavailable",
            expected_failure_stage="source",
        )
        canary = DataSnapshotRef(
            snapshot_id="retrospective-canary-gold",
            tier=SnapshotTier.GOLD,
            uri="iceberg://factorlab/canary#retrospective-canary-gold",
            content_hash="a" * 64,
            as_of=incident_at + timedelta(minutes=5),
            quality_status=DataQualityStatus.ACCEPTED,
            trust_labels=(
                "physical_engineering_canary",
                "retrospective_non_forward",
            ),
        )
        catalog.register_snapshot(canary)
        with pytest.raises(
            OrchestrationFailure,
            match="formal forward-eligible",
        ):
            service.revalidate_data_incident(
                incident_id=reported["incident_id"],
                snapshot_id=canary.snapshot_id,
                occurred_at=incident_at + timedelta(minutes=20),
            )

        unrelated = _register_revalidation_gold(
            catalog=catalog,
            ledger=ledger,
            lake_root=lake_root,
            partition_key=partition_key,
            incident_at=incident_at,
            suffix="unrelated",
            bind_stage=False,
        )
        with pytest.raises(
            OrchestrationFailure,
            match="no complete durable repaired partition chain",
        ):
            service.revalidate_data_incident(
                incident_id=reported["incident_id"],
                snapshot_id=unrelated.snapshot_id,
                occurred_at=unrelated.as_of + timedelta(minutes=10),
            )

        gold = _register_revalidation_gold(
            catalog=catalog,
            ledger=ledger,
            lake_root=lake_root,
            partition_key=partition_key,
            incident_at=incident_at,
            repair_incident_id=reported["incident_id"],
        )
        with ledger_engine.begin() as connection:
            model = connection.execute(
                orm.DataIncidentModel.__table__.select().where(
                    orm.DataIncidentModel.incident_id == reported["incident_id"]
                )
            ).mappings().one()
            payload = dict(model["payload_json"])
            payload["domain_incident_id"] = "dinc_tampered"
            connection.execute(
                update(orm.DataIncidentModel)
                .where(
                    orm.DataIncidentModel.incident_id == reported["incident_id"]
                )
                .values(payload_json=payload)
            )
        with pytest.raises(
            OrchestrationFailure,
            match="origin or terminal envelope",
        ):
            service.revalidate_data_incident(
                incident_id=reported["incident_id"],
                snapshot_id=gold.snapshot_id,
                occurred_at=gold.as_of + timedelta(minutes=10),
            )
        assert not any(
            event.event_type == "data_revalidated"
            for event in catalog.list_shadow_events(
                account_id="champion-shadow", limit=100
            )
        )
    finally:
        ledger.close()
        catalog.close()
        ledger_engine.dispose()


@pytest.mark.parametrize(
    ("tamper", "expected_error"),
    (
        ("manifest_reference_laundering", "parent or immutable manifest"),
        ("gold_operation_lineage", "repair authority chain is inconsistent"),
        ("bronze_file_hash", "Bronze manifest/reference binding"),
    ),
)
def test_revalidation_rejects_laundered_manifest_and_semantic_stage_lineage(
    tmp_path: Path,
    tamper: str,
    expected_error: str,
) -> None:
    catalog_database = tmp_path / f"{tamper}-catalog.db"
    ledger_database = tmp_path / f"{tamper}-ledger.db"
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(catalog_database)
    catalog.initialize_schema()
    ledger_engine = create_engine(f"sqlite:///{ledger_database.as_posix()}")
    orm.Base.metadata.create_all(ledger_engine)
    ledger = ProductionLedger(ledger_engine)
    incident_at = datetime(2026, 8, 23, 10, tzinfo=timezone.utc)
    partition_key = "2026-08-23"
    source_identity = PartitionIdentity("research_os", "stage_source", partition_key)
    ledger.ensure_partition(
        source_identity,
        created_at=incident_at,
        input_hash=content_fingerprint(
            {"partition_key": partition_key, "dataset": "stage_source"},
            domain="test/revalidation-original-input",
        ),
        details={"dagster_run_id": f"{tamper}-failure"},
    )
    source_lease = ledger.claim(
        identity=source_identity,
        owner="source-worker",
        now=incident_at,
        lease_for=timedelta(minutes=5),
    )
    assert source_lease is not None
    ledger.finish(
        source_lease,
        status=PartitionStatus.FAILED,
        completed_at=incident_at,
        details={"dagster_run_id": f"{tamper}-failure"},
        error_code="SourceUnavailable",
        error="provider unavailable",
    )
    catalog.create_shadow_account(
        account_id="champion-shadow",
        name="Champion shadow",
        initial_capital=50_000_000,
        opened_at=incident_at - timedelta(days=1),
    )
    service = object.__new__(ApplicationServices)
    service.catalog = catalog
    service.production_ledger = ledger
    service.shadow_authority = None
    service.settings = SimpleNamespace(lake_root=lake_root)
    service.sleeve_roster = load_sleeve_roster(
        Path(__file__).resolve().parents[1]
        / "configs"
        / "research_os_initial_sleeves.json"
    )
    repair_accounts = ("champion-shadow",)
    _install_fixture_shadow_roles(service, repair_accounts)

    try:
        reported = service.report_unexpected_data_failure(
            partition_key,
            message="source chain is unavailable",
            occurred_at=incident_at,
            dagster_run_id=f"{tamper}-failure",
            failed_step_key="source_sync",
            error_code="SourceUnavailable",
            expected_failure_stage="source",
        )
        gold = _register_revalidation_gold(
            catalog=catalog,
            ledger=ledger,
            lake_root=lake_root,
            partition_key=partition_key,
            incident_at=incident_at,
            suffix=tamper,
            repair_incident_id=reported["incident_id"],
            gold_manifest_labels=(
                (
                    "point_in_time",
                    "physical_engineering_canary",
                    "retrospective_non_forward",
                )
                if tamper == "manifest_reference_laundering"
                else None
            ),
            gold_reference_labels=(
                ("point_in_time", "field_reconciled", "quality_accepted")
                if tamper == "manifest_reference_laundering"
                else None
            ),
        )
        _finish_revalidation_shadow(
            catalog=catalog,
            ledger=ledger,
            repair_incident_id=reported["incident_id"],
            partition_key=partition_key,
            incident_at=incident_at,
            gold=gold,
            account_ids=repair_accounts,
            suffix=tamper,
        )
        if tamper == "gold_operation_lineage":
            gold_authority = ledger.get_repair_authority(
                reported["incident_id"], "stage_gold"
            )
            assert gold_authority is not None
            gold_identity = gold_authority.identity
            with ledger_engine.begin() as connection:
                row = connection.execute(
                    orm.PartitionRunModel.__table__.select().where(
                        orm.PartitionRunModel.partition_run_id
                        == gold_identity.partition_run_id
                    )
                ).mappings().one()
                details = dict(row["details_json"])
                operation_result = dict(details["operation_result"])
                outputs = dict(operation_result["outputs"])
                outputs["uri"] = "iceberg://factorlab/unrelated#ros_unrelated"
                operation_result["outputs"] = outputs
                details["operation_result"] = operation_result
                connection.execute(
                    update(orm.PartitionRunModel)
                    .where(
                        orm.PartitionRunModel.partition_run_id
                        == gold_identity.partition_run_id
                    )
                    .values(
                        details_json=details,
                        output_hash=content_fingerprint(
                            operation_result,
                            domain=(
                                "factor-lab/research-os/v1/production-operation-result"
                            ),
                        ),
                    )
                )
        elif tamper == "bronze_file_hash":
            (lake_root / f"revalidation-bronze-{tamper}.json").write_text(
                json.dumps(
                    {"partition_key": partition_key, "suffix": "tampered"}
                ),
                encoding="utf-8",
            )

        with pytest.raises(OrchestrationFailure, match=expected_error):
            service.revalidate_data_incident(
                incident_id=reported["incident_id"],
                snapshot_id=gold.snapshot_id,
                occurred_at=gold.as_of + timedelta(minutes=10),
            )
        authority = next(
            item
            for item in ledger.iter_incidents()
            if item.incident_id == reported["incident_id"]
        )
        assert authority.status is IncidentStatus.OPEN
        effective_records, _ = service._production_lifecycle_fleet()
        assert all(record.state.value == "frozen_data" for record in effective_records)
    finally:
        ledger.close()
        catalog.close()
        ledger_engine.dispose()


def _finish_accepted_calendar_partition(
    ledger: ProductionLedger,
    *,
    partition_key: str,
) -> PartitionIdentity:
    session = date.fromisoformat(partition_key)
    completed_at = datetime.combine(
        session,
        application_services.time(15, 1),
        tzinfo=application_services._SHANGHAI,
    ).astimezone(timezone.utc)
    identity = PartitionIdentity(
        "research_os", "accepted_trade_calendar", partition_key
    )
    dagster_run_id = f"fixture-accepted-calendar-{partition_key}"
    ledger.ensure_partition(
        identity,
        created_at=completed_at - timedelta(minutes=1),
        input_hash=content_fingerprint(
            {"partition_key": partition_key},
            domain="test/accepted-calendar-partition",
        ),
        details={"dagster_run_id": dagster_run_id},
    )
    lease = ledger.claim(
        identity=identity,
        owner=f"fixture-accepted-calendar-{partition_key}",
        now=completed_at - timedelta(minutes=1),
        lease_for=timedelta(minutes=5),
    )
    assert lease is not None
    ledger.finish(
        lease,
        status=PartitionStatus.SUCCEEDED,
        completed_at=completed_at,
        output_hash=content_fingerprint(
            {"partition_key": partition_key, "quality_status": "accepted"},
            domain="test/accepted-calendar-result",
        ),
        details={"dagster_run_id": dagster_run_id},
    )
    return identity


def _shadow_revalidation_case(
    tmp_path: Path,
    *,
    partition_key: str = "2026-08-23",
    validation_trade_date: str | None = None,
    finish_shadow: bool = True,
    shared_authority_database: bool = False,
) -> dict[str, object]:
    catalog_database = tmp_path / "shadow-revalidation-catalog.db"
    catalog_database_url = (
        f"sqlite+pysqlite:///{catalog_database.as_posix()}"
    )
    ledger_database = tmp_path / "shadow-revalidation-ledger.db"
    ledger_database_url = (
        catalog_database_url
        if shared_authority_database
        else f"sqlite+pysqlite:///{ledger_database.as_posix()}"
    )
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(catalog_database_url)
    catalog.initialize_schema()
    catalog_engine = create_engine(catalog_database_url)
    orm.Base.metadata.create_all(catalog_engine)
    ledger_engine = create_engine(ledger_database_url)
    orm.Base.metadata.create_all(ledger_engine)
    ledger = ProductionLedger(ledger_engine)
    shadow_authority = ShadowEvidenceAuthority(
        catalog_engine,
        enforce_realtime=False,
        require_fleet_closure=False,
    )
    partition_date = date.fromisoformat(partition_key)
    seed_at = datetime.combine(
        partition_date,
        application_services.time(10),
        tzinfo=timezone.utc,
    )
    service = object.__new__(ApplicationServices)
    service.catalog = catalog
    service.production_ledger = ledger
    service.shadow_authority = shadow_authority
    service._configuration_hash = content_fingerprint(
        {"fixture": "shadow-revalidation"},
        domain="test/shadow-revalidation-configuration",
    )
    service._environment_hashes = {}
    service.settings = SimpleNamespace(lake_root=lake_root)
    service.sleeve_roster = load_sleeve_roster(
        Path(__file__).resolve().parents[1]
        / "configs"
        / "research_os_initial_sleeves.json"
    )
    for account_id, name in (
        ("champion-shadow", "Champion shadow"),
        ("challenger-shadow", "Challenger shadow"),
    ):
        catalog.create_shadow_account(
            account_id=account_id,
            name=name,
            initial_capital=50_000_000,
            opened_at=seed_at - timedelta(days=1),
        )

    gold = _register_revalidation_gold(
        catalog=catalog,
        ledger=ledger,
        lake_root=lake_root,
        partition_key=partition_key,
        incident_at=seed_at,
        suffix="shadow-execution",
    )
    with catalog_engine.begin() as connection:
        connection.execute(
            insert(orm.ExperimentModel).values(
                experiment_id="shadow-repair-challenger-exp",
                fingerprint="8" * 64,
                snapshot_id=gold.snapshot_id,
                candidate_kind="sleeve",
                candidate_id="value-quality-repair",
                family="value_quality",
                status="completed",
                spec_json={"candidate_kind": "sleeve"},
                registered_at=seed_at,
                updated_at=seed_at,
            )
        )
        connection.execute(
            insert(orm.ExperimentResultModel).values(
                result_id="shadow-repair-challenger-result",
                experiment_id="shadow-repair-challenger-exp",
                result_hash="9" * 64,
                outcome="promoted_to_shadow",
                metrics_json={"promotion_verdict": "promote"},
                artifact_uri=None,
                authoritative=True,
                completed_at=seed_at,
            )
        )
    shadow_authority.bind_role(
        role=ShadowRole.CHAMPION,
        role_key="static_champion",
        account_id="champion-shadow",
        bound_at=seed_at,
    )
    catalog.append_lifecycle_event(
        LifecycleEvent(
            idempotency_key="shadow-revalidation-fixture-state",
            sleeve_id="value_quality",
            to_state=LifecycleState.SHADOW,
            cause="fixture shadow state",
            occurred_at=seed_at - timedelta(microseconds=1),
        )
    )
    shadow_authority.bind_role(
        role=ShadowRole.CHALLENGER,
        role_key="shadow-repair-challenger-exp",
        account_id="challenger-shadow",
        sleeve_id="value_quality",
        experiment_id="shadow-repair-challenger-exp",
        bound_at=seed_at,
    )

    # The domain account commit may succeed before the Dagster stage envelope
    # crashes.  Persist those real account_projected events first; incident
    # handling then appends its cash intent without erasing this repaired-step
    # evidence from the append-only chain.
    projection_time = datetime.combine(
        partition_date,
        application_services.time(15),
        tzinfo=application_services._SHANGHAI,
    ).astimezone(timezone.utc)
    projections: list[dict[str, object]] = []
    projection_events = {}
    for index, account_id in enumerate(
        ("champion-shadow", "challenger-shadow")
    ):
        nav = 50_000_000.0 - index * 100_000.0
        account = catalog.get_shadow_account(account_id)
        assert account is not None
        event = catalog.append_shadow_event(
            account_id=account_id,
            event_type="account_projected",
            occurred_at=projection_time,
            payload={
                "research_os_shadow_step": {
                    "step_id": f"shadow-step-repaired-{account_id}",
                    "kind": "account_projection",
                },
                "account_status": "active",
                "account_state": {
                    "cash": nav,
                    "nav": nav,
                    "benchmark_nav": 50_000_000.0,
                },
            },
            expected_previous_hash=account.last_event_hash,
        )
        projection_events[account_id] = event
        projections.append(
            {
                "step_id": f"shadow-step-repaired-{account_id}",
                "account_id": account_id,
                "decision_date": partition_key,
                "trade_date": partition_key,
                "snapshot_id": gold.snapshot_id,
                "model_version": "repair-v1",
                "cash": nav,
                "nav": nav,
                "benchmark_nav": 50_000_000.0,
                "position_count": 0,
                "domain_event_count": 1,
                "persisted_event_count": 1,
                "first_event_sequence": event.sequence_number,
                "last_event_sequence": event.sequence_number,
                "last_event_hash": event.event_hash,
                "chain_verified": True,
            }
        )

    failure_at = gold.as_of + timedelta(minutes=1)
    shadow_identity = PartitionIdentity(
        "research_os", "stage_shadow", partition_key
    )
    shadow_input_hash = content_fingerprint(
        {"partition_key": partition_key, "operation": "shadow_nav_step"},
        domain="test/shadow-revalidation-input",
    )
    ledger.ensure_partition(
        shadow_identity,
        created_at=failure_at,
        input_hash=shadow_input_hash,
        details={
            "operation": OperationName.SHADOW_NAV_STEP.value,
            "dagster_run_id": "failed-shadow-run",
        },
    )
    failed_lease = ledger.claim(
        identity=shadow_identity,
        owner="failed-shadow-worker",
        now=failure_at,
        lease_for=timedelta(minutes=5),
    )
    assert failed_lease is not None

    reported = service.report_unexpected_data_failure(
        partition_key,
        message="typed shadow event-chain closure failed",
        occurred_at=failure_at,
        dagster_run_id="failed-shadow-run",
        failed_step_key="shadow_account_nav:typed_execution_closure",
        error_code="shadow_typed_execution_failed",
    )
    authority = next(
        item
        for item in ledger.iter_incidents()
        if item.incident_id == reported["incident_id"]
    )
    assert authority.stage is IncidentStage.SHADOW_EXECUTION
    assert authority.partition_run_id == shadow_identity.partition_run_id
    assert len(authority.evidence_hashes) == 4

    failed_record = ledger.get_partition(shadow_identity)
    assert failed_record is not None
    if failed_record.status is PartitionStatus.RUNNING:
        ledger.finish(
            failed_lease,
            status=PartitionStatus.FAILED,
            completed_at=failure_at + timedelta(minutes=1),
            error_code="shadow_typed_execution_failed",
            error="typed shadow event-chain closure failed",
        )
    repair_fingerprint = service._server_repair_fingerprint(
        OperationRequest(
            operation=OperationName.SOURCE_SYNC,
            cycle=CycleName.DAILY,
            partition_key=partition_key,
            run_id="fixture-repair-shadow-execution-repair",
        ),
        incident=authority,
    )
    repair_gold = _register_revalidation_gold(
        catalog=catalog,
        ledger=ledger,
        lake_root=lake_root,
        partition_key=partition_key,
        incident_at=failure_at,
        suffix="shadow-execution-repair",
        repair_incident_id=reported["incident_id"],
        repair_fingerprint_override=repair_fingerprint,
    )
    if validation_trade_date is not None:
        _finish_accepted_calendar_partition(
            ledger,
            partition_key=validation_trade_date,
        )
    repaired_shadow_identity = None
    repaired_projections: tuple[dict[str, object], ...] = ()
    if finish_shadow:
        repaired_shadow_identity, repaired_projections = _finish_revalidation_shadow(
            catalog=catalog,
            ledger=ledger,
            repair_incident_id=reported["incident_id"],
            partition_key=partition_key,
            incident_at=failure_at,
            gold=repair_gold,
            account_ids=("champion-shadow", "challenger-shadow"),
            suffix="shadow-execution-repair",
            validation_trade_date=validation_trade_date,
            repair_fingerprint_override=repair_fingerprint,
        )
    return {
        "catalog": catalog,
        "ledger": ledger,
        "ledger_engine": ledger_engine,
        "catalog_engine": catalog_engine,
        "shadow_authority": shadow_authority,
        "service": service,
        "gold": repair_gold,
        "failure_at": failure_at,
        "reported": reported,
        "partition_key": partition_key,
        "validation_trade_date": validation_trade_date or partition_key,
        "projection_events": projection_events,
        "repaired_projections": repaired_projections,
        "shadow_identity": repaired_shadow_identity,
    }


def _close_shadow_revalidation_case(case: dict[str, object]) -> None:
    case["shadow_authority"].close()
    case["ledger"].close()
    case["catalog"].close()
    case["catalog_engine"].dispose()
    case["ledger_engine"].dispose()


def _install_fixture_formal_shadow_authority(
    case: dict[str, object],
    *,
    stale: bool = False,
    missing_account_id: str | None = None,
) -> SimpleNamespace:
    """Bind repaired projections to deterministic formal session/closure readers."""

    durable_authority = case["shadow_authority"]
    bindings = tuple(durable_authority.active_fleet_bindings())
    binding_by_account = {
        str(binding.account_id): binding for binding in bindings
    }
    projections = tuple(case["repaired_projections"])
    projection_by_account = {
        str(projection["account_id"]): projection for projection in projections
    }
    shadow_record = case["ledger"].get_partition(case["shadow_identity"])
    assert shadow_record is not None
    assert shadow_record.completed_at is not None
    formal_by_account: dict[str, SimpleNamespace] = {}
    for account_id, projection in projection_by_account.items():
        projected_events = case["catalog"].list_shadow_events_by_type(
            account_id=account_id,
            event_type="account_projected",
            since=None,
            through=None,
            limit=1_000,
        )
        tail = next(
            event
            for event in projected_events
            if event.event_hash == projection["last_event_hash"]
        )
        created_at = tail.occurred_at + timedelta(seconds=1)
        if stale:
            created_at = tail.occurred_at - timedelta(seconds=1)
        formal_by_account[account_id] = SimpleNamespace(
            account_id=account_id,
            trade_date=date.fromisoformat(str(projection["trade_date"])),
            role_binding_id=str(binding_by_account[account_id].binding_id),
            account_event_sequence=int(projection["last_event_sequence"]),
            account_event_hash=str(projection["last_event_hash"]),
            decision_snapshot_id=projection.get("decision_snapshot_id"),
            execution_snapshot_id=projection.get("execution_snapshot_id"),
            mark_snapshot_id=projection.get("mark_snapshot_id"),
            rebalanced=bool(projection["rebalanced"]),
            cash=float(projection["cash"]),
            nav=float(projection["nav"]),
            benchmark_nav=float(projection["benchmark_nav"]),
            position_count=int(projection["position_count"]),
            created_at=created_at,
            session_hash=content_fingerprint(
                {
                    "account_id": account_id,
                    "trade_date": projection["trade_date"],
                    "event_hash": projection["last_event_hash"],
                },
                domain="test/formal-shadow-session",
            ),
        )
    trade_date = date.fromisoformat(str(projections[0]["trade_date"]))
    members = tuple(
        sorted(
            (
                {
                    "binding_id": str(binding.binding_id),
                    "binding_hash": str(binding.binding_hash),
                    "role": binding.role.value,
                    "role_key": str(binding.role_key),
                    "account_id": str(binding.account_id),
                    "session_hash": formal_by_account[
                        str(binding.account_id)
                    ].session_hash,
                    "account_event_hash": formal_by_account[
                        str(binding.account_id)
                    ].account_event_hash,
                }
                for binding in bindings
            ),
            key=lambda member: (
                member["role"],
                member["role_key"],
                member["binding_id"],
            ),
        )
    )
    closure = SimpleNamespace(
        trade_date=trade_date,
        members=members,
        member_count=len(members),
        closed_at=min(
            shadow_record.completed_at,
            max(item.created_at for item in formal_by_account.values())
            + timedelta(seconds=1),
        ),
    )

    def session_projection(*, account_id, trade_date):
        if str(account_id) == missing_account_id:
            return None
        formal = formal_by_account.get(str(account_id))
        if formal is None or formal.trade_date != trade_date:
            return None
        return formal

    proxy = SimpleNamespace(
        active_fleet_bindings=lambda: bindings,
        session_projection=session_projection,
        fleet_closure=lambda selected: closure if selected == trade_date else None,
    )
    case["service"].shadow_authority = proxy
    case["service"]._production_authority = True
    return proxy


def _rewrite_shadow_repair_result(
    case: dict[str, object],
    mutate: object,
) -> None:
    engine = case["ledger_engine"]
    identity = case["shadow_identity"]
    with engine.begin() as connection:
        row = connection.execute(
            orm.PartitionRunModel.__table__.select().where(
                orm.PartitionRunModel.partition_run_id == identity.partition_run_id
            )
        ).mappings().one()
        details = dict(row["details_json"])
        operation_result = dict(details["operation_result"])
        outputs = dict(operation_result["outputs"])
        executed = dict(outputs["executed"])
        mutate(executed)
        outputs["executed"] = executed
        operation_result["outputs"] = outputs
        details["operation_result"] = operation_result
        connection.execute(
            update(orm.PartitionRunModel)
            .where(
                orm.PartitionRunModel.partition_run_id == identity.partition_run_id
            )
            .values(
                details_json=details,
                output_hash=content_fingerprint(
                    operation_result,
                    domain="factor-lab/research-os/v1/production-operation-result",
                ),
            )
        )


def test_shadow_repair_revalidates_on_fresh_session_twenty_days_after_incident(
    tmp_path: Path,
) -> None:
    incident_partition = "2026-08-04"
    validation_trade_date = "2026-08-24"
    assert (
        date.fromisoformat(validation_trade_date)
        - date.fromisoformat(incident_partition)
    ).days == 20
    case = _shadow_revalidation_case(
        tmp_path,
        partition_key=incident_partition,
        validation_trade_date=validation_trade_date,
    )
    try:
        shadow_identity = case["shadow_identity"]
        assert isinstance(shadow_identity, PartitionIdentity)
        assert shadow_identity.partition_key == incident_partition
        assert shadow_identity.generation != "base"
        assert validation_trade_date in case["ledger"].accepted_calendar_partitions()

        shadow_record = case["ledger"].get_partition(shadow_identity)
        assert shadow_record is not None
        assert shadow_record.status is PartitionStatus.SUCCEEDED
        assert (
            shadow_record.details["repair_validation_trade_date"]
            == validation_trade_date
        )
        operation_result = shadow_record.details["operation_result"]
        outputs = operation_result["outputs"]
        assert outputs["incident_partition_key"] == incident_partition
        assert outputs["validation_trade_date"] == validation_trade_date
        projections = tuple(outputs["executed"]["projections"])
        assert projections == case["repaired_projections"]
        assert {item["decision_date"] for item in projections} == {
            validation_trade_date
        }
        assert {item["trade_date"] for item in projections} == {
            validation_trade_date
        }

        recovered = case["service"].revalidate_data_incident(
            incident_id=case["reported"]["incident_id"],
            snapshot_id=case["gold"].snapshot_id,
            occurred_at=case["failure_at"] + timedelta(days=20, hours=1),
        )
        assert recovered["status"] == "resolved"
        assert recovered["fleet_action"] == "restored_to_dormant"
        resolved = case["ledger"].get_incident(case["reported"]["incident_id"])
        assert resolved is not None
        resolution = resolved.payload["resolution"]
        assert resolution["validation_trade_date"] == validation_trade_date
        assert resolution["repaired_partition_key"] == incident_partition
        assert (
            resolution["repaired_partition_run_id"]
            == shadow_identity.partition_run_id
        )
    finally:
        _close_shadow_revalidation_case(case)


def test_shadow_repair_preserves_first_server_selected_validation_session(
    tmp_path: Path,
) -> None:
    incident_partition = "2026-08-04"
    first_validation_session = "2026-08-24"
    later_validation_session = "2026-08-25"
    case = _shadow_revalidation_case(
        tmp_path,
        partition_key=incident_partition,
        validation_trade_date=first_validation_session,
        finish_shadow=False,
    )
    try:
        service = case["service"]
        service._now = lambda: datetime(2026, 8, 26, 1, tzinfo=timezone.utc)
        admitted: list[OperationRequest] = []

        def capture(request: OperationRequest) -> OperationResult:
            admitted.append(request)
            return OperationResult(
                operation=request.operation,
                status="completed",
                summary="captured server-selected Shadow repair session",
            )

        service._execute_admitted = capture
        first = service.execute_data_incident_repair(
            OperationRequest(
                operation=OperationName.SHADOW_NAV_STEP,
                cycle=CycleName.DAILY,
                partition_key=incident_partition,
                run_id="fresh-shadow-repair-attempt-1",
            ),
            incident_id=case["reported"]["incident_id"],
        )
        assert first.status == "completed"
        assert (
            admitted[-1].metadata["repair_validation_trade_date"]
            == first_validation_session
        )

        _finish_accepted_calendar_partition(
            case["ledger"],
            partition_key=later_validation_session,
        )
        second = service.execute_data_incident_repair(
            OperationRequest(
                operation=OperationName.SHADOW_NAV_STEP,
                cycle=CycleName.DAILY,
                partition_key=incident_partition,
                run_id="fresh-shadow-repair-attempt-2",
            ),
            incident_id=case["reported"]["incident_id"],
        )
        assert second.status == "completed"
        assert case["ledger"].accepted_calendar_partitions() == (
            first_validation_session,
            later_validation_session,
        )
        assert [
            request.metadata["repair_validation_trade_date"]
            for request in admitted
        ] == [first_validation_session, first_validation_session]

        shadow_authority = case["ledger"].get_repair_authority(
            case["reported"]["incident_id"],
            "stage_shadow",
        )
        assert shadow_authority is not None
        assert shadow_authority.identity.partition_key == incident_partition
        shadow_record = case["ledger"].get_partition(shadow_authority.identity)
        assert shadow_record is not None
        assert shadow_record.status is PartitionStatus.PENDING
        assert (
            shadow_record.details["repair_validation_trade_date"]
            == first_validation_session
        )
    finally:
        _close_shadow_revalidation_case(case)


def test_post_gold_shadow_execution_revalidation_uses_real_account_tails(
    tmp_path: Path,
) -> None:
    case = _shadow_revalidation_case(
        tmp_path, shared_authority_database=True
    )
    try:
        recovered = case["service"].revalidate_data_incident(
            incident_id=case["reported"]["incident_id"],
            snapshot_id=case["gold"].snapshot_id,
            occurred_at=case["failure_at"] + timedelta(minutes=20),
        )
        assert recovered["status"] == "resolved"
        assert recovered["fleet_action"] == "restored_to_dormant"
        assert recovered["effects_applied"] is True
        assert not tuple(case["ledger"].iter_incidents(status=IncidentStatus.OPEN))
    finally:
        _close_shadow_revalidation_case(case)


def test_production_revalidation_requires_exact_formal_session_and_closure(
    tmp_path: Path,
) -> None:
    case = _shadow_revalidation_case(
        tmp_path, shared_authority_database=True
    )
    try:
        _install_fixture_formal_shadow_authority(case)
        recovered = case["service"].revalidate_ready_data_incident(
            incident_id=case["reported"]["incident_id"]
        )
        assert recovered["status"] == "resolved"
        assert recovered["fleet_action"] == "restored_to_dormant"
    finally:
        _close_shadow_revalidation_case(case)


def test_terminal_revalidation_replay_ignores_later_role_rotation(
    tmp_path: Path,
) -> None:
    case = _shadow_revalidation_case(
        tmp_path, shared_authority_database=True
    )
    try:
        _install_fixture_formal_shadow_authority(case)
        service = case["service"]
        incident_id = case["reported"]["incident_id"]
        first = service.revalidate_ready_data_incident(
            incident_id=incident_id
        )
        assert first["status"] == "resolved"
        resolved = case["ledger"].get_incident(incident_id)
        assert resolved is not None
        assert resolved.resolved_at is not None
        original_resolution_hash = resolved.resolution_hash
        lifecycle_count = len(case["catalog"].list_lifecycle_events(limit=100))
        revalidation_counts = {
            account_id: len(
                case["catalog"].list_shadow_events_by_type(
                    account_id=account_id,
                    event_type="data_revalidated",
                    since=None,
                    through=None,
                    limit=100,
                )
            )
            for account_id in ("champion-shadow", "challenger-shadow")
        }

        rotation_at = resolved.resolved_at + timedelta(seconds=1)
        case["catalog"].create_shadow_account(
            account_id="champion-shadow-v2",
            name="Rotated Champion shadow",
            initial_capital=50_000_000,
            opened_at=rotation_at,
        )
        case["shadow_authority"].bind_role(
            role=ShadowRole.CHAMPION,
            role_key="static_champion",
            account_id="champion-shadow-v2",
            bound_at=rotation_at,
        )
        assert {
            str(binding.account_id)
            for binding in case["shadow_authority"].active_fleet_bindings()
        } == {"champion-shadow-v2", "challenger-shadow"}

        def mutable_authority_must_not_be_read(*_args, **_kwargs):
            raise AssertionError(
                "terminal replay read mutable Shadow role/session authority"
            )

        service.shadow_authority = SimpleNamespace(
            active_fleet_bindings=mutable_authority_must_not_be_read,
            session_projection=mutable_authority_must_not_be_read,
            fleet_closure=mutable_authority_must_not_be_read,
        )
        direct = service.revalidate_data_incident(
            incident_id=incident_id,
            snapshot_id=case["gold"].snapshot_id,
            occurred_at=resolved.resolved_at,
        )
        ready = service.revalidate_ready_data_incident(
            incident_id=incident_id
        )
        assert direct["status"] == ready["status"] == "resolved"
        assert direct["effects_applied"] is False
        assert ready["effects_applied"] is False
        assert direct["revalidation_id"] == ready["revalidation_id"]
        replayed = case["ledger"].get_incident(incident_id)
        assert replayed is not None
        assert replayed.resolution_hash == original_resolution_hash
        assert len(case["catalog"].list_lifecycle_events(limit=100)) == (
            lifecycle_count
        )
        assert {
            account_id: len(
                case["catalog"].list_shadow_events_by_type(
                    account_id=account_id,
                    event_type="data_revalidated",
                    since=None,
                    through=None,
                    limit=100,
                )
            )
            for account_id in ("champion-shadow", "challenger-shadow")
        } == revalidation_counts

        with pytest.raises(
            OrchestrationFailure, match="differs from authority evidence"
        ):
            service.revalidate_data_incident(
                incident_id=incident_id,
                snapshot_id="f" * 64,
                occurred_at=resolved.resolved_at,
            )
    finally:
        _close_shadow_revalidation_case(case)


def test_final_typed_effect_fence_rejects_account_append_before_terminal_cas(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _shadow_revalidation_case(
        tmp_path, shared_authority_database=True
    )
    try:
        _install_fixture_formal_shadow_authority(case)
        ledger = case["ledger"]
        original_fence = ledger._typed_effect_fence_matches
        injected = False

        def append_before_final_fence(session, effect_result):
            nonlocal injected
            assert injected is False
            injected = True
            account = session.get(
                orm.ShadowAccountModel, "champion-shadow"
            )
            assert account is not None
            sequence_number = int(account.last_event_sequence) + 1
            previous_event_hash = str(account.last_event_hash)
            occurred_at = account.as_of
            if occurred_at.tzinfo is None:
                occurred_at = occurred_at.replace(tzinfo=timezone.utc)
            payload = {
                "reason": "concurrent append after typed effects"
            }
            event_hash = content_fingerprint(
                {
                    "account_id": "champion-shadow",
                    "sequence_number": sequence_number,
                    "event_type": "concurrent_account_observation",
                    "occurred_at": occurred_at,
                    "payload": payload,
                    "previous_event_hash": previous_event_hash,
                },
                domain="factor-lab/research-os/v1/shadow-event",
            )
            session.add(
                orm.ShadowEventModel(
                    event_id=f"sev_{event_hash[:32]}",
                    account_id="champion-shadow",
                    sequence_number=sequence_number,
                    event_type="concurrent_account_observation",
                    occurred_at=occurred_at,
                    payload_json=payload,
                    previous_event_hash=previous_event_hash,
                    event_hash=event_hash,
                )
            )
            account.last_event_sequence = sequence_number
            account.last_event_hash = event_hash
            account.updated_at = occurred_at
            return original_fence(session, effect_result)

        monkeypatch.setattr(
            ledger,
            "_typed_effect_fence_matches",
            append_before_final_fence,
        )
        incident_id = case["reported"]["incident_id"]
        original_identity = case["shadow_identity"]
        original_partition = ledger.get_partition(original_identity)
        assert original_partition is not None

        with pytest.raises(
            OrchestrationFailure, match="waiting for a newer accepted session"
        ):
            case["service"].revalidate_ready_data_incident(
                incident_id=incident_id
            )

        assert injected is True
        incident = ledger.get_incident(incident_id)
        assert incident is not None
        assert incident.status is IncidentStatus.OPEN
        assert ledger.get_partition(original_identity) == original_partition
        chain = ledger.get_repair_chain(incident_id, "stage_shadow")
        assert len(chain) == 2
        rejection = ledger.get_partition(chain[-1].identity)
        assert rejection is not None
        assert rejection.status is PartitionStatus.FAILED
        assert (
            rejection.details["authority_kind"]
            == "typed_shadow_revalidation_rejection"
        )
        concurrent_events = case["catalog"].list_shadow_events_by_type(
            account_id="champion-shadow",
            event_type="concurrent_account_observation",
            since=None,
            through=None,
            limit=10,
        )
        assert len(concurrent_events) == 1
    finally:
        _close_shadow_revalidation_case(case)


def test_missing_formal_session_records_typed_shadow_rejection(
    tmp_path: Path,
) -> None:
    case = _shadow_revalidation_case(tmp_path)
    try:
        original_identity = case["shadow_identity"]
        original = case["ledger"].get_partition(original_identity)
        assert original is not None
        _install_fixture_formal_shadow_authority(
            case, missing_account_id="challenger-shadow"
        )
        with pytest.raises(
            OrchestrationFailure, match="waiting for a newer accepted session"
        ):
            case["service"].revalidate_ready_data_incident(
                incident_id=case["reported"]["incident_id"]
            )
        assert case["ledger"].get_partition(original_identity) == original
        chain = case["ledger"].get_repair_chain(
            case["reported"]["incident_id"], "stage_shadow"
        )
        assert len(chain) == 2
        rejection = case["ledger"].get_partition(chain[-1].identity)
        assert rejection is not None
        assert rejection.status is PartitionStatus.FAILED
        assert (
            rejection.details["authority_kind"]
            == "typed_shadow_revalidation_rejection"
        )
        assert tuple(case["ledger"].iter_incidents(status=IncidentStatus.OPEN))
    finally:
        _close_shadow_revalidation_case(case)


def test_stale_formal_shadow_is_rejected_then_new_session_resolves(
    tmp_path: Path,
) -> None:
    incident_partition = "2026-08-04"
    first_session = "2026-08-24"
    next_session = "2026-08-25"
    case = _shadow_revalidation_case(
        tmp_path,
        partition_key=incident_partition,
        validation_trade_date=first_session,
        shared_authority_database=True,
    )
    try:
        incident_id = case["reported"]["incident_id"]
        original_identity = case["shadow_identity"]
        original = case["ledger"].get_partition(original_identity)
        assert original is not None
        _install_fixture_formal_shadow_authority(case, stale=True)
        with pytest.raises(
            OrchestrationFailure, match="waiting for a newer accepted session"
        ):
            case["service"].revalidate_ready_data_incident(
                incident_id=incident_id
            )

        first_chain = case["ledger"].get_repair_chain(
            incident_id, "stage_shadow"
        )
        assert [
            case["ledger"].get_partition(item.identity).status
            for item in first_chain
        ] == [PartitionStatus.SUCCEEDED, PartitionStatus.FAILED]
        assert case["ledger"].get_partition(original_identity) == original

        _finish_accepted_calendar_partition(
            case["ledger"], partition_key=next_session
        )
        source_authority = case["ledger"].get_repair_authority(
            incident_id, "stage_source"
        )
        assert source_authority is not None
        next_identity, next_projections = _finish_revalidation_shadow(
            catalog=case["catalog"],
            ledger=case["ledger"],
            repair_incident_id=incident_id,
            partition_key=incident_partition,
            incident_at=case["failure_at"],
            gold=case["gold"],
            account_ids=("champion-shadow", "challenger-shadow"),
            suffix="shadow-execution-repair-next-session",
            validation_trade_date=next_session,
            repair_fingerprint_override=source_authority.repair_fingerprint,
        )
        case["shadow_identity"] = next_identity
        case["repaired_projections"] = next_projections
        _install_fixture_formal_shadow_authority(case)

        recovered = case["service"].revalidate_ready_data_incident(
            incident_id=incident_id
        )
        assert recovered["status"] == "resolved"
        resolved = case["ledger"].get_incident(incident_id)
        assert resolved is not None
        assert resolved.payload["resolution"]["validation_trade_date"] == next_session
        final_chain = case["ledger"].get_repair_chain(
            incident_id, "stage_shadow"
        )
        assert [
            case["ledger"].get_partition(item.identity).status
            for item in final_chain
        ] == [
            PartitionStatus.SUCCEEDED,
            PartitionStatus.FAILED,
            PartitionStatus.SUCCEEDED,
        ]
    finally:
        _close_shadow_revalidation_case(case)


def test_partial_lifecycle_and_account_effect_compensate_on_next_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incident_partition = "2026-08-04"
    first_session = "2026-08-24"
    next_session = "2026-08-25"
    first_revalidated_at = datetime(2026, 8, 24, 8, tzinfo=timezone.utc)
    next_revalidated_at = datetime(2026, 8, 25, 8, tzinfo=timezone.utc)
    case = _shadow_revalidation_case(
        tmp_path,
        partition_key=incident_partition,
        validation_trade_date=first_session,
    )
    try:
        service = case["service"]
        incident_id = case["reported"]["incident_id"]
        incident = case["ledger"].get_incident(incident_id)
        assert incident is not None
        domain_incident_id = str(incident.payload["domain_incident_id"])
        lifecycle_records, _ = service._production_lifecycle_fleet(
            overlay_open_incidents=False
        )
        first_account = "challenger-shadow"
        second_account = "champion-shadow"
        first_projection = next(
            item
            for item in case["repaired_projections"]
            if item["account_id"] == first_account
        )
        old_evidence = DataRevalidation(
            incident_id=domain_incident_id,
            snapshot_id=case["gold"].snapshot_id,
            snapshot_content_hash=case["gold"].content_hash,
            occurred_at=first_revalidated_at,
        )
        # Model the legacy/mid-transaction state explicitly: every Sleeve was
        # advanced, only the first account effect committed, and the second
        # account then moved outside the validated projection tail.
        DataIncidentCoordinator(case["catalog"]).revalidate(
            old_evidence,
            lifecycle_records=lifecycle_records,
            shadow_accounts={
                lifecycle_records[0].sleeve_id: (first_account,)
            },
            expected_shadow_tails={
                first_account: str(first_projection["last_event_hash"])
            },
        )
        second = case["catalog"].get_shadow_account(second_account)
        assert second is not None
        case["catalog"].append_shadow_event(
            account_id=second_account,
            event_type="concurrent_account_observation",
            occurred_at=first_revalidated_at,
            payload={"reason": "red-team append after repaired projection"},
            expected_previous_hash=second.last_event_hash,
        )
        monkeypatch.setattr(
            case["catalog"], "database_now", lambda: first_revalidated_at
        )
        monkeypatch.setattr(
            case["ledger"].incident_controls,
            "_database_now",
            lambda _session: first_revalidated_at + timedelta(minutes=1),
        )
        with pytest.raises(
            OrchestrationFailure, match="waiting for a newer accepted session"
        ):
            service.revalidate_ready_data_incident(incident_id=incident_id)

        _finish_accepted_calendar_partition(
            case["ledger"], partition_key=next_session
        )
        source_authority = case["ledger"].get_repair_authority(
            incident_id, "stage_source"
        )
        assert source_authority is not None
        next_identity, next_projections = _finish_revalidation_shadow(
            catalog=case["catalog"],
            ledger=case["ledger"],
            repair_incident_id=incident_id,
            partition_key=incident_partition,
            incident_at=case["failure_at"],
            gold=case["gold"],
            account_ids=("champion-shadow", "challenger-shadow"),
            suffix="shadow-execution-repair-compensation",
            validation_trade_date=next_session,
            repair_fingerprint_override=source_authority.repair_fingerprint,
        )
        case["shadow_identity"] = next_identity
        case["repaired_projections"] = next_projections
        monkeypatch.setattr(
            case["catalog"], "database_now", lambda: next_revalidated_at
        )

        recovered = service.revalidate_ready_data_incident(
            incident_id=incident_id
        )
        assert recovered["status"] == "resolved"
        resolved = case["ledger"].get_incident(incident_id)
        assert resolved is not None
        assert (
            resolved.payload["resolution"]["validation_trade_date"]
            == next_session
        )
        new_revalidation_id = str(
            resolved.payload["resolution"]["revalidation_id"]
        )
        assert new_revalidation_id != old_evidence.revalidation_id
        for record in lifecycle_records:
            events = case["catalog"].list_lifecycle_events(
                sleeve_id=record.sleeve_id, limit=100
            )
            assert any(
                event.cause == "data_revalidation_attempt_superseded"
                and event.evidence["replacement_revalidation"][
                    "revalidation_id"
                ]
                == new_revalidation_id
                for event in events
            )
            assert (
                case["catalog"].latest_lifecycle_state(record.sleeve_id)
                is LifecycleState.DORMANT
            )
        first_effects = [
            event
            for event in case["catalog"].list_shadow_events(
                account_id=first_account, limit=100
            )
            if event.event_type == "data_revalidated"
        ]
        assert {
            str(event.payload["revalidation_id"]) for event in first_effects
        } == {old_evidence.revalidation_id, new_revalidation_id}
    finally:
        _close_shadow_revalidation_case(case)


def test_shadow_execution_revalidation_rejects_missing_durable_chain(
    tmp_path: Path,
) -> None:
    case = _shadow_revalidation_case(tmp_path)
    try:
        with case["catalog_engine"].begin() as connection:
            connection.execute(
                delete(orm.ShadowEventModel).where(
                    orm.ShadowEventModel.account_id == "challenger-shadow"
                )
            )
        with pytest.raises(OrchestrationFailure, match="account chain is corrupt"):
            case["service"].revalidate_data_incident(
                incident_id=case["reported"]["incident_id"],
                snapshot_id=case["gold"].snapshot_id,
                occurred_at=case["failure_at"] + timedelta(minutes=20),
            )
        assert tuple(case["ledger"].iter_incidents(status=IncidentStatus.OPEN))
    finally:
        _close_shadow_revalidation_case(case)


def test_shadow_execution_revalidation_rejects_tampered_projection_tail(
    tmp_path: Path,
) -> None:
    case = _shadow_revalidation_case(tmp_path)
    try:
        def tamper(executed):
            projections = [dict(item) for item in executed["projections"]]
            projections[0]["last_event_hash"] = "f" * 64
            executed["projections"] = projections

        _rewrite_shadow_repair_result(case, tamper)
        with pytest.raises(
            OrchestrationFailure,
            match="no unique durable projection event",
        ):
            case["service"].revalidate_data_incident(
                incident_id=case["reported"]["incident_id"],
                snapshot_id=case["gold"].snapshot_id,
                occurred_at=case["failure_at"] + timedelta(minutes=20),
            )
        assert tuple(case["ledger"].iter_incidents(status=IncidentStatus.OPEN))
    finally:
        _close_shadow_revalidation_case(case)


def test_shadow_execution_revalidation_rejects_partial_active_fleet_coverage(
    tmp_path: Path,
) -> None:
    case = _shadow_revalidation_case(tmp_path)
    try:
        def omit_challenger(executed):
            executed["projections"] = [
                item
                for item in executed["projections"]
                if item["account_id"] == "champion-shadow"
            ]

        _rewrite_shadow_repair_result(case, omit_challenger)
        with pytest.raises(OrchestrationFailure, match="do not cover the active fleet"):
            case["service"].revalidate_data_incident(
                incident_id=case["reported"]["incident_id"],
                snapshot_id=case["gold"].snapshot_id,
                occurred_at=case["failure_at"] + timedelta(minutes=20),
            )
        assert tuple(case["ledger"].iter_incidents(status=IncidentStatus.OPEN))
    finally:
        _close_shadow_revalidation_case(case)


def test_data_failure_freezes_oldest_of_more_than_one_thousand_active_accounts(
    tmp_path: Path,
) -> None:
    database = tmp_path / "full-active-fleet-incident.db"
    catalog = ResearchCatalog(database)
    catalog.initialize_schema()
    engine = create_engine(f"sqlite:///{database}")
    orm.Base.metadata.create_all(engine)
    ledger = ProductionLedger(engine)
    occurred_at = datetime(2026, 8, 23, 10, tzinfo=timezone.utc)
    identity = PartitionIdentity("research_os", "stage_source", "2026-08-23")
    ledger.ensure_partition(
        identity,
        created_at=occurred_at,
        details={"dagster_run_id": "full-active-fleet-failure"},
    )
    lease = ledger.claim(
        identity=identity,
        owner="source-worker",
        now=occurred_at,
        lease_for=timedelta(minutes=5),
    )
    assert lease is not None
    ledger.finish(
        lease,
        status=PartitionStatus.FAILED,
        completed_at=occurred_at,
        details={"dagster_run_id": "full-active-fleet-failure"},
        error_code="SourceUnavailable",
        error="provider unavailable",
    )
    oldest_account_id = "fleet-account-0000"
    opened_from = occurred_at - timedelta(days=2)
    for index in range(1_001):
        catalog.create_shadow_account(
            account_id=f"fleet-account-{index:04d}",
            name=f"Fleet account {index:04d}",
            initial_capital=50_000_000,
            opened_at=opened_from + timedelta(seconds=index),
        )
    truncated = catalog.list_shadow_accounts(limit=1_000, status="active")
    assert oldest_account_id not in {account.account_id for account in truncated}

    service = object.__new__(ApplicationServices)
    service.catalog = catalog
    service.production_ledger = ledger
    service.shadow_authority = None
    service.sleeve_roster = load_sleeve_roster(
        Path(__file__).resolve().parents[1]
        / "configs"
        / "research_os_initial_sleeves.json"
    )
    try:
        result = service.report_unexpected_data_failure(
            "2026-08-23",
            message="source chain is unavailable",
            occurred_at=occurred_at,
            dagster_run_id="full-active-fleet-failure",
            failed_step_key="source_sync",
            error_code="SourceUnavailable",
            expected_failure_stage="source",
        )

        assert len(result["cash_intent_accounts"]) == 1_001
        assert oldest_account_id in result["cash_intent_accounts"]
        oldest_events = catalog.list_shadow_events(
            account_id=oldest_account_id,
            limit=10,
        )
        assert sum(event.event_type == "data_incident" for event in oldest_events) == 1
        assert sum(
            event.event_type == "cash_target_intent" for event in oldest_events
        ) == 1
        for entry in service.sleeve_roster.entries:
            state = catalog.latest_lifecycle_state(entry.sleeve.sleeve_id)
            assert state is not None and state.value == "frozen_data"
    finally:
        ledger.close()
        catalog.close()
        engine.dispose()


def test_pending_cash_intent_survives_ten_thousand_newer_open_incidents(
    tmp_path: Path,
) -> None:
    database = tmp_path / "pending-cash-incident-depth.db"
    catalog = ResearchCatalog(database)
    catalog.initialize_schema()
    engine = create_engine(f"sqlite:///{database}")
    orm.Base.metadata.create_all(engine)
    ledger = ProductionLedger(engine)
    incident_at = datetime(2026, 8, 23, 10, tzinfo=timezone.utc)
    domain_incident_id = "domain-incident-beyond-truncated-window"
    account_id = "cash-intent-depth-shadow"
    account = catalog.create_shadow_account(
        account_id=account_id,
        name="Cash intent depth shadow",
        initial_capital=50_000_000,
        opened_at=incident_at - timedelta(hours=2),
    )
    position = catalog.append_shadow_event(
        account_id=account_id,
        event_type="position_seeded",
        occurred_at=incident_at - timedelta(hours=1),
        payload={
            "account_state": {
                "cash": 49_000_000,
                "nav": 50_000_000,
                "benchmark_nav": 50_000_000,
            },
            "position_state": {
                "ticker": "000001.SZ",
                "quantity": 100_000,
                "average_cost": 10.0,
                "market_price": 10.0,
                "market_value": 1_000_000,
            },
        },
        expected_previous_hash=account.last_event_hash,
    )
    cash_intent = catalog.append_shadow_event(
        account_id=account_id,
        event_type="cash_target_intent",
        occurred_at=incident_at,
        payload={
            "incident_id": domain_incident_id,
            "cash_weight": 1.0,
            "execution_state": "awaiting_trusted_execution",
        },
        expected_previous_hash=position.event_hash,
    )
    catalog.append_shadow_events_atomic(
        account_id=account_id,
        events=tuple(
            {
                "event_type": "cash_target_intent",
                "occurred_at": incident_at + timedelta(seconds=index + 1),
                "payload": {
                    "incident_id": f"closed-domain-noise-{index}",
                    "cash_weight": 1.0,
                    "execution_state": "already_superseded",
                },
            }
            for index in range(1_001)
        ),
        expected_previous_hash=cash_intent.event_hash,
    )
    related = ledger.record_incident(
        partition_key="2026-08-23",
        stage=IncidentStage.SOURCE,
        error_code="fixture_pending_cash_incident",
        message="older open incident still governs cash intent",
        occurred_at=incident_at,
        payload={"domain_incident_id": domain_incident_id},
    )
    decoys = [
        {
            "incident_id": f"incident_cash_decoy_{index:05d}",
            "incident_hash": f"{index + 1:064x}",
            "partition_run_id": None,
            "partition_key": "2026-08-24",
            "stage": IncidentStage.SOURCE.value,
            "status": IncidentStatus.OPEN.value,
            "error_code": "fixture_unrelated_open",
            "message": "newer unrelated open incident",
            "source_ids_json": [],
            "evidence_hashes_json": [],
            "payload_json": {"domain_incident_id": f"noise-{index}"},
            "occurred_at": incident_at + timedelta(minutes=1),
            "resolved_at": None,
            "resolution_hash": None,
        }
        for index in range(10_001)
    ]
    with engine.begin() as connection:
        connection.execute(insert(orm.DataIncidentModel), decoys)

    service = object.__new__(ApplicationServices)
    service.catalog = catalog
    service.production_ledger = ledger
    try:
        truncated = ledger.list_incidents(status=IncidentStatus.OPEN, limit=10_000)
        assert related.incident_id not in {item.incident_id for item in truncated}
        truncated_cash_events = catalog.list_shadow_events_by_type(
            account_id=account_id,
            event_type="cash_target_intent",
            limit=1_000,
        )
        assert cash_intent.event_id not in {
            event.event_id for event in truncated_cash_events
        }

        pending = service._pending_cash_intent_for_account(
            account_id,
            superseding_target_generated_at=None,
        )

        assert pending is not None
        assert pending["incident_id"] == domain_incident_id
        assert pending["cash_weight"] == 1.0
        assert catalog.list_shadow_positions(account_id)
    finally:
        ledger.close()
        catalog.close()
        engine.dispose()


def test_typed_shadow_closure_failure_uses_direct_incident_bridge() -> None:
    service = object.__new__(ApplicationServices)
    service.config = {
        "daily": {"shadow": {"input_mode": "authoritative_pg"}}
    }
    service._is_production_runtime = lambda: True
    service._now = lambda: datetime(2026, 8, 23, 10, tzinfo=timezone.utc)
    service._durable_stage_failure_time = (
        lambda *_args, **_kwargs: datetime(
            2026, 8, 23, 10, tzinfo=timezone.utc
        )
    )
    service._authoritative_shadow_nav_step = lambda *_args, **_kwargs: (
        (_ for _ in ()).throw(RuntimeError("vendor payload must stay hidden"))
    )
    reports: list[dict[str, object]] = []

    def report(partition_key, **kwargs):
        reports.append({"partition_key": partition_key, **kwargs})
        return {"incident_id": "incident-1", "partition_key": partition_key}

    service.report_unexpected_data_failure = report
    request = OperationRequest(
        operation=OperationName.SHADOW_NAV_STEP,
        cycle=CycleName.DAILY,
        partition_key="2026-08-23",
        run_id="typed-closure-failure",
    )

    result = service._shadow_nav_step(request)

    assert result.status == "failed"
    assert result.outputs["risk_guard"] == "frozen_data"
    assert reports[0]["error_code"] == "shadow_typed_execution_failed"
    assert "vendor payload" not in result.summary
    assert "vendor payload" not in str(reports)
