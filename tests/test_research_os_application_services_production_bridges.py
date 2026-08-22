from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine

import factor_lab.research_os.application_services as application_services
from factor_lab.research_os import orm
from factor_lab.research_os.application_services import (
    APPLICATION_SERVICES_SCHEMA_VERSION,
    WEBUI_ENV_FILE_ENV,
    ApplicationServices,
    _runtime_environment,
)
from factor_lab.research_os.catalog import ResearchCatalog
from factor_lab.research_os.orchestration import (
    CycleName,
    OperationName,
    OperationRequest,
    OrchestrationFailure,
    ServiceNotConfigured,
)
from factor_lab.research_os.production_ledger import (
    IncidentStage,
    PartitionIdentity,
    PartitionStatus,
    ProductionLedger,
)
from factor_lab.research_os.sleeve_registry import load_sleeve_roster


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

        def database_now(self):
            return now

        def list_runs(self, *, limit, run_type=None, **_kwargs):
            assert limit == 1_000
            return [row for row in self.runs.values() if row.run_type == run_type]

        def list_research_submissions(self, *, limit, **_kwargs):
            assert limit == 1_000
            return []

        def list_recovery_cases(self, *, limit, **_kwargs):
            assert limit == 1_000
            return []

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
            assert recovery_case_id is None
            self.calls.append(family_id)
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
    assert service.monthly_research.calls == ["family_0", "family_1", "family_2"]
    assert len(set(service.monthly_research.calls)) == 3
    assert len(service.catalog.runs) == 3


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
    assert partition_run_id is None
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
                }[identity.dataset]
                * 64,
            )

    service = object.__new__(ApplicationServices)
    service.production_ledger = Ledger()

    with pytest.raises(OrchestrationFailure, match="every durable stage succeeded"):
        service._infer_failed_data_stage(
            "2026-08-23", require_incomplete=True
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
    ledger.ensure_partition(identity, created_at=occurred_at)
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


def test_typed_shadow_closure_failure_uses_direct_incident_bridge() -> None:
    service = object.__new__(ApplicationServices)
    service.config = {
        "daily": {"shadow": {"input_mode": "authoritative_pg"}}
    }
    service._is_production_runtime = lambda: True
    service._now = lambda: datetime(2026, 8, 23, 10, tzinfo=timezone.utc)
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
