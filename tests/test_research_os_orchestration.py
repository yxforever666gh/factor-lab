from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest
import yaml

import factor_lab.research_os.dagster_defs as dagster_defs_module
from factor_lab.research_os.dagster_defs import (
    DATA_INCIDENT_REPAIR_JOB_NAME,
    DAGSTER_AVAILABLE,
    HISTORICAL_BACKFILL_JOB_NAME,
    defs,
    failure_event_occurred_at,
    partition_key_for,
)
from factor_lab.research_os.contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    SnapshotTier,
)
from factor_lab.research_os.execution_snapshot_authority import (
    ExecutionCapabilityAssessment,
    ExecutionCapabilityDecision,
    TypedExecutionSession,
)
from factor_lab.research_os.orchestration import (
    CYCLE_BLUEPRINTS,
    CycleName,
    HandlerResearchOSServices,
    OperationName,
    OperationResult,
    OrchestrationFailure,
    ServiceNotConfigured,
    Trigger,
    TriggerPoll,
    run_cycle,
    services_from_environment,
)


def _success(operation: OperationName) -> OperationResult:
    outputs = {}
    if operation is OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH:
        outputs = {
            "iceberg_table": "factor_lab.gold.market_panel",
            "iceberg_snapshot_id": 901,
            "iceberg_tag": "gold-2026-08-22-deadbeef",
        }
    return OperationResult(operation, "completed", "ok", outputs)


@pytest.mark.skipif(not DAGSTER_AVAILABLE, reason="Dagster is an orchestration extra")
def test_failure_timestamp_comes_from_real_dagster_event_log() -> None:
    """A DagsterEvent has no timestamp; the durable EventLogRecord does."""

    from types import SimpleNamespace

    from dagster import DagsterEventType, DagsterInstance, job, op

    @op
    def fail_after_start() -> None:
        raise RuntimeError("deliberate durable failure")

    @job
    def failing_job() -> None:
        fail_after_start()

    instance = DagsterInstance.ephemeral()
    result = failing_job.execute_in_process(
        instance=instance,
        raise_on_error=False,
    )
    assert result.success is False
    dagster_run = instance.get_run_by_id(result.run_id)
    assert dagster_run is not None
    records = instance.get_records_for_run(
        result.run_id,
        of_type=DagsterEventType.RUN_FAILURE,
        limit=1,
        ascending=False,
    ).records
    assert len(records) == 1
    assert not hasattr(records[0].event_log_entry.dagster_event, "timestamp")

    resolved = failure_event_occurred_at(
        SimpleNamespace(instance=instance, dagster_run=dagster_run)
    )

    assert resolved == datetime.fromtimestamp(records[0].timestamp, tz=timezone.utc)


@pytest.mark.skipif(not DAGSTER_AVAILABLE, reason="Dagster is an orchestration extra")
def test_daily_failure_sensor_reuses_durable_run_incident_and_closes_services(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sensor replay delegates identity to durable authority, never process memory."""

    from dagster import (
        DagsterEventType,
        DagsterInstance,
        build_run_status_sensor_context,
        job,
        op,
    )

    partition_key = "2026-08-22"

    @op
    def fail_daily_run() -> None:
        raise RuntimeError("deliberate daily failure")

    @job(tags={"factor_lab/partition_key": partition_key})
    def failed_daily_job() -> None:
        fail_daily_run()

    instance = DagsterInstance.ephemeral()
    result = failed_daily_job.execute_in_process(
        instance=instance,
        raise_on_error=False,
    )
    assert result.success is False
    dagster_run = instance.get_run_by_id(result.run_id)
    assert dagster_run is not None
    failure_records = instance.get_records_for_run(
        result.run_id,
        of_type=DagsterEventType.RUN_FAILURE,
        limit=1,
        ascending=False,
    ).records
    assert len(failure_records) == 1
    failure_event = failure_records[0].event_log_entry.dagster_event
    assert failure_event is not None

    # This object stands in for PostgreSQL and deliberately outlives every
    # transient ApplicationServices instance created by the sensor.  A replay
    # must rediscover the same incident/cohort through this authority.
    durable_by_run: dict[str, tuple[str, str]] = {}
    report_calls: list[dict[str, object]] = []
    closed_service_ids: list[int] = []
    created_service_ids: list[int] = []

    class TransientCatalog:
        def __init__(self, service_id: int) -> None:
            self.service_id = service_id

        def close(self) -> None:
            closed_service_ids.append(self.service_id)

    class TransientServices:
        def __init__(self, service_id: int) -> None:
            self.catalog = TransientCatalog(service_id)

        def report_unexpected_data_failure(self, **kwargs):
            report_calls.append(dict(kwargs))
            run_id = str(kwargs["dagster_run_id"])
            reused = run_id in durable_by_run
            durable_by_run.setdefault(
                run_id,
                ("incident_" + "a" * 64, "repaircohort_" + "b" * 64),
            )
            incident_id, cohort_id = durable_by_run[run_id]
            return {
                "incident_id": incident_id,
                "repair_cohort_id": cohort_id,
                "reused": reused,
            }

    def create_transient_services():
        service_id = len(created_service_ids) + 1
        created_service_ids.append(service_id)
        return TransientServices(service_id)

    monkeypatch.setattr(
        dagster_defs_module,
        "services_from_environment",
        create_transient_services,
    )
    sensor_def = defs.get_sensor_def(
        "research_os_unexpected_daily_failure_sensor"
    )

    def evaluate_once():
        context = build_run_status_sensor_context(
            sensor_name=sensor_def.name,
            dagster_event=failure_event,
            dagster_instance=instance,
            dagster_run=dagster_run,
            repository_def=defs.get_repository_def(),
        )
        return sensor_def(context)

    first = evaluate_once()
    second = evaluate_once()

    assert "registered unexpected daily failure" in first.skip_message
    assert "reused durable unexpected daily failure" in second.skip_message
    assert len(durable_by_run) == 1
    assert len(set(durable_by_run.values())) == 1
    assert [call["dagster_run_id"] for call in report_calls] == [
        result.run_id,
        result.run_id,
    ]
    assert report_calls[0]["occurred_at"] == report_calls[1]["occurred_at"]
    assert report_calls[0]["failed_step_key"] == report_calls[1]["failed_step_key"]
    assert created_service_ids == [1, 2]
    assert closed_service_ids == [1, 2]


@pytest.mark.skipif(not DAGSTER_AVAILABLE, reason="Dagster is an orchestration extra")
def test_daily_failure_sensor_closes_services_when_bridge_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace
    from dagster import (
        DagsterEventType,
        DagsterInstance,
        build_run_status_sensor_context,
        job,
        op,
    )

    @op
    def fail_without_bridge() -> None:
        raise RuntimeError("deliberate bridge-less failure")

    @job(tags={"factor_lab/partition_key": "2026-08-22"})
    def failed_daily_job() -> None:
        fail_without_bridge()

    instance = DagsterInstance.ephemeral()
    result = failed_daily_job.execute_in_process(
        instance=instance,
        raise_on_error=False,
    )
    dagster_run = instance.get_run_by_id(result.run_id)
    assert dagster_run is not None
    failure_records = instance.get_records_for_run(
        result.run_id,
        of_type=DagsterEventType.RUN_FAILURE,
        limit=1,
        ascending=False,
    ).records
    assert len(failure_records) == 1
    failure_event = failure_records[0].event_log_entry.dagster_event
    assert failure_event is not None

    closed: list[bool] = []

    class MissingBridge:
        catalog = SimpleNamespace(close=lambda: closed.append(True))

    monkeypatch.setattr(
        dagster_defs_module,
        "services_from_environment",
        lambda: MissingBridge(),
    )
    sensor_def = defs.get_sensor_def(
        "research_os_unexpected_daily_failure_sensor"
    )
    context = build_run_status_sensor_context(
        sensor_name=sensor_def.name,
        dagster_event=failure_event,
        dagster_instance=instance,
        dagster_run=dagster_run,
        repository_def=defs.get_repository_def(),
    )

    with pytest.raises(RuntimeError, match="do not expose"):
        sensor_def(context)

    assert closed == [True]


def _typed_session(partition_key: str, *, reused: bool = False) -> TypedExecutionSession:
    trade_date = date.fromisoformat(partition_key)

    def reference(role: str, digest: str, tier: SnapshotTier) -> DataSnapshotRef:
        return DataSnapshotRef(
            snapshot_id=f"{role}_{digest}",
            tier=tier,
            uri=f"s3://factor-lab/typed/{partition_key}/{role}.parquet",
            content_hash=digest,
            as_of=datetime.combine(
                trade_date,
                datetime.min.time(),
                tzinfo=timezone.utc,
            ),
            quality_status=DataQualityStatus.ACCEPTED,
            manifest={"role": role, "trade_date": partition_key},
        )

    return TypedExecutionSession(
        trade_date=trade_date,
        bars=pd.DataFrame({"ticker": ["000001.SZ"]}),
        benchmark_return=0.0,
        execution_snapshot=reference("execution", "a" * 64, SnapshotTier.GOLD),
        mark_snapshot=reference("mark", "b" * 64, SnapshotTier.GOLD),
        bundle_snapshot=reference("bundle", "c" * 64, SnapshotTier.SILVER),
        capability=ExecutionCapabilityAssessment(
            decision=ExecutionCapabilityDecision.ACCEPTED,
            reasons=(),
            evidence_hash="d" * 64,
        ),
        reused=reused,
    )


def test_cycle_blueprints_cover_only_bounded_research_and_shadow_work() -> None:
    assert set(CYCLE_BLUEPRINTS) == set(CycleName)
    assert CYCLE_BLUEPRINTS[CycleName.DAILY].cron_schedule == "30 18 * * 1-5"
    assert CYCLE_BLUEPRINTS[CycleName.WEEKLY].cron_schedule == "0 20 * * 5"
    assert CYCLE_BLUEPRINTS[CycleName.MONTHLY].cron_schedule == "0 20 1 * *"
    assert CYCLE_BLUEPRINTS[CycleName.QUARTERLY].cron_schedule == "0 10 1 1,4,7,10 *"

    operations = {item.value for item in OperationName}
    assert "gold_iceberg_snapshot_publish" in operations
    assert "shadow_nav_step" in operations
    assert not any(
        forbidden in operation
        for operation in operations
        for forbidden in ("broker", "live_trade", "submit_order", "real_money")
    )


def test_injected_cycle_is_ordered_and_stops_at_first_failed_gate() -> None:
    called: list[OperationName] = []

    def handler(request):
        called.append(request.operation)
        if request.operation is OperationName.DATA_QUALITY_GATE:
            return OperationResult(request.operation, "blocked", "disputed source values")
        return _success(request.operation)

    services = HandlerResearchOSServices(
        {operation: handler for operation in OperationName}
    )
    with pytest.raises(OrchestrationFailure, match="disputed source values"):
        run_cycle(
            CycleName.DAILY,
            services,
            partition_key="2026-08-22",
            run_id="test-run",
        )
    assert called == [
        OperationName.SOURCE_SYNC,
        OperationName.SOURCE_RECONCILIATION,
        OperationName.DATA_QUALITY_GATE,
    ]


def test_gold_boundary_requires_iceberg_snapshot_and_tag() -> None:
    services = HandlerResearchOSServices(
        {
            OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH: lambda request: OperationResult(
                request.operation,
                "completed",
                "wrote a local manifest only",
                {"manifest_path": "snapshot.json"},
            )
        }
    )
    from factor_lab.research_os.orchestration import OperationRequest

    request = OperationRequest(
        operation=OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
        cycle=CycleName.DAILY,
        partition_key="2026-08-22",
        run_id="gold-test",
    )
    with pytest.raises(OrchestrationFailure, match="Iceberg catalog snapshot"):
        services.execute(request)


def test_unconfigured_service_fails_closed() -> None:
    services = services_from_environment({})
    from factor_lab.research_os.orchestration import OperationRequest

    with pytest.raises(ServiceNotConfigured, match="FACTOR_LAB_ORCHESTRATION_FACTORY"):
        services.execute(
            OperationRequest(
                operation=OperationName.SLEEVE_HEALTH_CHECK,
                cycle=CycleName.WEEKLY,
                partition_key="2026-08-22",
                run_id="missing-factory",
            )
        )


def test_partition_keys_use_shanghai_calendar_boundaries() -> None:
    value = datetime(2026, 3, 31, 16, 30, tzinfo=timezone.utc)
    assert partition_key_for(CycleName.DAILY, value) == "2026-04-01"
    assert partition_key_for(CycleName.MONTHLY, value) == "2026-04"
    assert partition_key_for(CycleName.QUARTERLY, value) == "2026-Q2"


def test_dagster_module_stays_collectable_without_optional_dependency() -> None:
    if DAGSTER_AVAILABLE:
        assert defs is not None
        repository = defs.get_repository_def()
        job_names = {
            "research_os_calendar_registry_job",
            "research_os_open_observation_job",
            "research_os_daily_job",
            HISTORICAL_BACKFILL_JOB_NAME,
            DATA_INCIDENT_REPAIR_JOB_NAME,
            "research_os_weekly_job",
            "research_os_monthly_job",
            "research_os_quarterly_job",
        }
        assert job_names <= {definition.name for definition in repository.get_all_jobs()}
        assert len(repository.schedule_defs) == 5
        assert len(repository.sensor_defs) == 5
        assert all(
            definition.default_status.value == "STOPPED"
            for definition in repository.schedule_defs
        )
        sensor_statuses = {
            definition.name: definition.default_status.value
            for definition in repository.sensor_defs
        }
        assert all(status == "STOPPED" for status in sensor_statuses.values())
    else:
        assert defs is None


@pytest.mark.skipif(not DAGSTER_AVAILABLE, reason="Dagster is an orchestration extra")
def test_code_defined_historical_backfill_uses_only_authoritative_data_chain() -> None:
    assert defs is not None
    from dagster import DagsterInstance

    class AuthorityServices:
        def __init__(self) -> None:
            self.ordinary: list[OperationName] = []
            self.backfill: list[OperationName] = []

        def execute(self, request):
            self.ordinary.append(request.operation)
            return _success(request.operation)

        def execute_authoritative_backfill(self, request):
            self.backfill.append(request.operation)
            return _success(request.operation)

        @staticmethod
        def build_execution_session(trade_date):
            return _typed_session(trade_date.isoformat())

        @staticmethod
        def formal_shadow_projection_allowed(_trade_date):
            return True

    services = AuthorityServices()
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("a_share_trading_days", ["2026-08-22"])

    result = defs.resolve_job_def(HISTORICAL_BACKFILL_JOB_NAME).execute_in_process(
        resources={"research_os_services": services},
        partition_key="2026-08-22",
        instance=instance,
    )

    assert result.success
    assert services.ordinary == []
    assert services.backfill == [
        OperationName.SOURCE_SYNC,
        OperationName.SOURCE_RECONCILIATION,
        OperationName.DATA_QUALITY_GATE,
        OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
    ]
    assert OperationName.SHADOW_NAV_STEP not in services.backfill


@pytest.mark.skipif(not DAGSTER_AVAILABLE, reason="Dagster is an orchestration extra")
def test_data_incident_repair_job_runs_five_stages_then_revalidates() -> None:
    assert defs is not None
    from dagster import DagsterInstance

    incident_id = "incident_" + "a" * 64

    class RepairServices:
        def __init__(self) -> None:
            self.calls: list[tuple[OperationName, str, str]] = []
            self.revalidated: list[str] = []

        def execute_data_incident_repair(self, request, *, incident_id):
            self.calls.append(
                (request.operation, request.run_id, incident_id)
            )
            return _success(request.operation)

        def revalidate_ready_data_incident(self, *, incident_id):
            self.revalidated.append(incident_id)
            return {
                "incident_id": incident_id,
                "status": "resolved",
                "revalidation_id": "dreval_" + "b" * 64,
                "fleet_action": "restored_to_dormant",
            }

    services = RepairServices()
    result = defs.resolve_job_def(
        DATA_INCIDENT_REPAIR_JOB_NAME
    ).execute_in_process(
        resources={"research_os_services": services},
        instance=DagsterInstance.ephemeral(),
        tags={
            "factor_lab/partition_key": "2026-08-22",
            "factor_lab/repair_incident_id": incident_id,
            "factor_lab/repair_cohort_id": "repaircohort_" + "c" * 64,
        },
    )

    assert result.success
    assert [item[0] for item in services.calls] == [
        OperationName.SOURCE_SYNC,
        OperationName.SOURCE_RECONCILIATION,
        OperationName.DATA_QUALITY_GATE,
        OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
        OperationName.SHADOW_NAV_STEP,
    ]
    assert len({item[1] for item in services.calls}) == 1
    assert all(item[2] == incident_id for item in services.calls)
    assert services.revalidated == [incident_id]


@pytest.mark.skipif(not DAGSTER_AVAILABLE, reason="Dagster is an orchestration extra")
def test_data_incident_repair_sensor_drains_controls_and_suppresses_active_run() -> None:
    assert defs is not None
    from dagster import DagsterInstance, build_sensor_context

    incident_id = "incident_" + "d" * 64
    cohort_id = "repaircohort_" + "e" * 64

    class CoordinatorServices:
        def __init__(self) -> None:
            self.resume_calls: list[dict[str, object]] = []

        def resume_pending_incident_controls(self, **kwargs):
            self.resume_calls.append(dict(kwargs))
            return {"status": "completed"}

        @staticmethod
        def pending_data_incident_repairs():
            return (
                {
                    "incident_id": incident_id,
                    "partition_key": "2026-08-22",
                    "repair_cohort_id": cohort_id,
                },
            )

    services = CoordinatorServices()
    instance = DagsterInstance.ephemeral()
    sensor_def = defs.get_sensor_def(
        "research_os_data_incident_repair_sensor"
    )
    with build_sensor_context(
        instance=instance,
        definitions=defs,
        sensor_name=sensor_def.name,
        resources={"research_os_services": services},
    ) as context:
        first = sensor_def.evaluate_tick(context)
    assert len(first.run_requests) == 1
    request = first.run_requests[0]
    assert request.run_key is None
    assert request.tags["factor_lab/repair_incident_id"] == incident_id
    assert request.tags["factor_lab/repair_cohort_id"] == cohort_id

    instance.create_run_for_job(
        defs.resolve_job_def(DATA_INCIDENT_REPAIR_JOB_NAME),
        tags={"factor_lab/repair_incident_id": incident_id},
    )
    with build_sensor_context(
        instance=instance,
        definitions=defs,
        sensor_name=sensor_def.name,
        resources={"research_os_services": services},
    ) as context:
        second = sensor_def.evaluate_tick(context)
    assert second.run_requests == []
    assert "active repair run" in str(second.skip_message)
    assert len(services.resume_calls) == 2


@pytest.mark.skipif(not DAGSTER_AVAILABLE, reason="Dagster is an orchestration extra")
def test_daily_job_tags_cannot_spoof_historical_backfill_authority() -> None:
    assert defs is not None
    from dagster import DagsterInstance

    class AuthorityServices:
        def __init__(self) -> None:
            self.ordinary: list[OperationName] = []
            self.backfill: list[OperationName] = []

        def execute(self, request):
            self.ordinary.append(request.operation)
            return _success(request.operation)

        def execute_authoritative_backfill(self, request):
            self.backfill.append(request.operation)
            return _success(request.operation)

        @staticmethod
        def build_execution_session(trade_date):
            return _typed_session(trade_date.isoformat())

        @staticmethod
        def formal_shadow_projection_allowed(_trade_date):
            return True

    services = AuthorityServices()
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("a_share_trading_days", ["2026-08-22"])

    result = defs.resolve_job_def("research_os_daily_job").execute_in_process(
        resources={"research_os_services": services},
        partition_key="2026-08-22",
        instance=instance,
        tags={
            "factor_lab/partition_key": "2026-08-22",
            "factor_lab/job_name": HISTORICAL_BACKFILL_JOB_NAME,
            "factor_lab/authority": "authoritative_historical_backfill",
        },
    )

    assert result.success
    assert services.backfill == []
    assert services.ordinary == [
        OperationName.SOURCE_SYNC,
        OperationName.SOURCE_RECONCILIATION,
        OperationName.DATA_QUALITY_GATE,
        OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
        OperationName.SHADOW_NAV_STEP,
    ]


@pytest.mark.skipif(not DAGSTER_AVAILABLE, reason="Dagster is an orchestration extra")
def test_fresh_catalog_bootstraps_first_dynamic_sessions_without_guessing_weekdays() -> None:
    assert defs is not None
    from dagster import DagsterInstance

    class BootstrapServices:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def bootstrap_accepted_calendar(self, **kwargs) -> TriggerPoll:
            self.calls.append(dict(kwargs))
            return TriggerPoll(
                triggers=(
                    Trigger(
                        partition_key="2016-06-01",
                        run_key="daily:2016-06-01",
                        metadata={"calendar_snapshot_id": "silver-calendar-1"},
                    ),
                    Trigger(
                        partition_key="2016-06-03",
                        run_key="daily:2016-06-03",
                        metadata={"calendar_snapshot_id": "silver-calendar-1"},
                    ),
                ),
                cursor="2016-06-03",
                message="two mutually accepted sessions",
            )

        @staticmethod
        def poll(_sensor_name, _cursor):
            raise AssertionError("fresh-volume bootstrap must not depend on a PG poll")

    services = BootstrapServices()
    instance = DagsterInstance.ephemeral()
    assert instance.get_dynamic_partitions("a_share_trading_days") == []

    result = defs.resolve_job_def(
        "research_os_calendar_registry_job"
    ).execute_in_process(
        resources={"research_os_services": services},
        partition_key="SSE",
        instance=instance,
    )

    assert result.success
    assert instance.get_dynamic_partitions("a_share_trading_days") == [
        "2016-06-01",
        "2016-06-03",
    ]
    assert len(services.calls) == 1
    call = services.calls[0]
    assert call["exchange"] == "SSE"
    assert call["source_start"].isoformat() == "2016-06-01"
    assert call["through"].isoformat() >= "2016-06-03"
    assert str(call["dagster_run_id"]).strip()


@pytest.mark.skipif(not DAGSTER_AVAILABLE, reason="Dagster is an orchestration extra")
def test_dagster_daily_assets_execute_through_fake_service() -> None:
    assert defs is not None
    from dagster import DagsterInstance

    class DailyServices:
        def __init__(self) -> None:
            self.delegate = HandlerResearchOSServices(
                {
                    operation: (lambda request: _success(request.operation))
                    for operation in OperationName
                }
            )

        def execute(self, request):
            return self.delegate.execute(request)

        @staticmethod
        def build_execution_session(trade_date):
            return _typed_session(trade_date.isoformat())

        @staticmethod
        def formal_shadow_projection_allowed(_trade_date):
            return True

    services = DailyServices()
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("a_share_trading_days", ["2026-08-22"])
    result = defs.resolve_job_def("research_os_daily_job").execute_in_process(
        resources={"research_os_services": services},
        tags={"factor_lab/partition_key": "2026-08-22"},
        partition_key="2026-08-22",
        instance=instance,
    )
    assert result.success
    assets = {"/".join(spec.key.path) for spec in defs.resolve_all_asset_specs()}
    assert "gold_iceberg_snapshot" in assets
    assert "shadow_account_nav" in assets


@pytest.mark.skipif(not DAGSTER_AVAILABLE, reason="Dagster is an orchestration extra")
def test_open_job_is_independent_idempotent_and_required_by_evening_build() -> None:
    assert defs is not None
    from dagster import DagsterInstance

    partition_key = "2026-08-22"

    class OpenAwareServices:
        def __init__(self) -> None:
            self.open_refs: dict[str, DataSnapshotRef] = {}
            self.physical_open_calls = 0
            self.typed_builds: list[str] = []

        def observe_execution_open(self, trade_date):
            key = trade_date.isoformat()
            existing = self.open_refs.get(key)
            if existing is not None:
                return existing
            self.physical_open_calls += 1
            reference = DataSnapshotRef(
                snapshot_id="open_" + "a" * 64,
                tier=SnapshotTier.BRONZE,
                uri="s3://factor-lab/open/2026-08-22.parquet",
                content_hash="a" * 64,
                parent_snapshot_ids=("decision_" + "b" * 64,),
                as_of=datetime(2026, 8, 22, 1, 30, tzinfo=timezone.utc),
                quality_status=DataQualityStatus.ACCEPTED,
                manifest={
                    "decision_snapshot_id": "decision_" + "b" * 64,
                    "decision_trade_date": "2026-08-21",
                },
            )
            self.open_refs[key] = reference
            return reference

        def build_execution_session(self, trade_date):
            key = trade_date.isoformat()
            if key not in self.open_refs:
                raise RuntimeError("persisted open observation is missing")
            self.typed_builds.append(key)
            return _typed_session(key, reused=len(self.typed_builds) > 1)

        @staticmethod
        def formal_shadow_projection_allowed(_trade_date):
            return True

        @staticmethod
        def report_unexpected_data_failure(**_kwargs):
            return {"incident_id": "incident-open-missing"}

        def execute(self, request):
            return _success(request.operation)

    services = OpenAwareServices()
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("a_share_trading_days", [partition_key])

    without_open = defs.resolve_job_def("research_os_daily_job").execute_in_process(
        resources={"research_os_services": services},
        partition_key=partition_key,
        instance=instance,
        raise_on_error=False,
    )
    assert without_open.success is False
    assert services.typed_builds == []

    for _ in range(2):
        opened = defs.resolve_job_def(
            "research_os_open_observation_job"
        ).execute_in_process(
            resources={"research_os_services": services},
            partition_key=partition_key,
            instance=instance,
        )
        assert opened.success
    assert services.physical_open_calls == 1

    with_open = defs.resolve_job_def("research_os_daily_job").execute_in_process(
        resources={"research_os_services": services},
        partition_key=partition_key,
        instance=instance,
    )
    assert with_open.success
    assert services.typed_builds == [partition_key]


@pytest.mark.skipif(not DAGSTER_AVAILABLE, reason="Dagster is an orchestration extra")
def test_pre_epoch_typed_capability_closes_without_forward_projection() -> None:
    """Break the readiness cycle without manufacturing account evidence."""

    assert defs is not None
    from dagster import DagsterInstance

    partition_key = "2026-08-22"

    class EpochAwareServices:
        def __init__(self) -> None:
            self.epoch_active = False
            self.typed_builds = 0
            self.shadow_projections = 0

        def execute(self, request):
            if request.operation is OperationName.SHADOW_NAV_STEP:
                self.shadow_projections += 1
            return _success(request.operation)

        def build_execution_session(self, trade_date):
            self.typed_builds += 1
            return _typed_session(
                trade_date.isoformat(), reused=self.typed_builds > 1
            )

        def formal_shadow_projection_allowed(self, _trade_date):
            return self.epoch_active

    services = EpochAwareServices()
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("a_share_trading_days", [partition_key])

    pre_epoch = defs.resolve_job_def("research_os_daily_job").execute_in_process(
        resources={"research_os_services": services},
        partition_key=partition_key,
        instance=instance,
    )
    assert pre_epoch.success
    assert services.typed_builds == 1
    assert services.shadow_projections == 0
    closure = pre_epoch.output_for_node("typed_execution_session_closure")
    shadow = pre_epoch.output_for_node("shadow_account_nav")
    assert closure["status"] == "completed"
    assert closure["capability_decision"] == "accepted"
    assert shadow["status"] == "skipped"
    assert shadow["forward_projection"] == "blocked_pre_epoch"

    services.epoch_active = True
    forward = defs.resolve_job_def("research_os_daily_job").execute_in_process(
        resources={"research_os_services": services},
        partition_key=partition_key,
        instance=instance,
    )
    assert forward.success
    assert services.typed_builds == 2
    assert services.shadow_projections == 1


@pytest.mark.skipif(not DAGSTER_AVAILABLE, reason="Dagster is an orchestration extra")
def test_expected_source_failure_reaches_shadow_guard_before_terminal_failure() -> None:
    assert defs is not None
    from dagster import DagsterInstance

    class RawOutcomeServices:
        def __init__(self) -> None:
            self.called: list[OperationName] = []
            self.shadow_data_outcome = None
            self.direct_settlement_calls = 0

        def execute(self, request):
            self.called.append(request.operation)
            if request.operation is OperationName.SHADOW_NAV_STEP:
                self.shadow_data_outcome = request.metadata.get(
                    "daily_data_outcome"
                )
                return OperationResult(
                    request.operation,
                    "completed",
                    "persisted all-cash risk intent",
                    {"risk_guard": "all_cash"},
                )
            return OperationResult(
                request.operation,
                "failed",
                "expected source chain block",
                {"error_type": "SourceUnavailable"},
            )

        def execute_daily_failure_settlement(self, request):
            self.direct_settlement_calls += 1
            return self.execute(request)

        @staticmethod
        def poll(_sensor_name, cursor):
            from factor_lab.research_os.orchestration import TriggerPoll

            return TriggerPoll(cursor=cursor, message="none")

    services = RawOutcomeServices()
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("a_share_trading_days", ["2026-08-22"])
    result = defs.resolve_job_def("research_os_daily_job").execute_in_process(
        resources={"research_os_services": services},
        tags={"factor_lab/partition_key": "2026-08-22"},
        partition_key="2026-08-22",
        instance=instance,
        raise_on_error=False,
    )

    assert result.success is False
    assert OperationName.SHADOW_NAV_STEP in services.called
    assert services.called.index(OperationName.SHADOW_NAV_STEP) > services.called.index(
        OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH
    )
    assert services.direct_settlement_calls == 1
    assert services.shadow_data_outcome is not None
    occurred_at = services.shadow_data_outcome.pop("occurred_at")
    assert datetime.fromisoformat(occurred_at).tzinfo is not None
    assert services.shadow_data_outcome == {
        "partition_key": "2026-08-22",
        "status": "blocked",
        "failure_stage": "source",
        "error_code": "SourceUnavailable",
        "message": "expected source chain block",
    }


def test_local_compose_has_only_research_infrastructure() -> None:
    root = Path(__file__).resolve().parents[1]
    compose = yaml.safe_load(
        (root / "infra" / "research_os" / "docker-compose.yml").read_text(
            encoding="utf-8"
        )
    )
    assert set(compose["services"]) == {
        "credential-check",
        "postgres",
        "minio",
        "minio-init",
        "catalog-migrate",
        "webui-db-bootstrap",
        "dagster-code-server",
        "dagster-webserver",
        "dagster-daemon",
        "research-os-webui",
    }
    assert compose["services"]["postgres"]["image"].startswith("postgres:16")
    environment = compose["x-research-os-environment"]
    assert environment["PYICEBERG_CATALOG__FACTORLAB__TYPE"] == "sql"
    assert environment["PYICEBERG_CATALOG__FACTORLAB__WAREHOUSE"].startswith("s3://")
    assert "DAGSTER_POSTGRES_URL" in environment
