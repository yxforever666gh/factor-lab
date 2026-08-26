"""Dagster definitions for the local, research-only Factor Lab deployment.

The schedule blueprint remains importable without Dagster.  The Docker image
installs Dagster and exposes a separate opening-observation job in addition to
the close/data jobs.  All work crosses an injectable application-service
boundary.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from math import isfinite
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from .contracts import DataQualityStatus, DataSnapshotRef
from .application_services import NonDataPipelineFailure
from .execution_snapshot_authority import (
    ExecutionCapabilityDecision,
    TypedExecutionSession,
)
from .orchestration import (
    CYCLE_BLUEPRINTS,
    ORCHESTRATION_TIMEZONE,
    CycleName,
    OperationName,
    OperationRequest,
    OperationResult,
    TriggerPoll,
    cycle_tags,
    execute_operation,
    services_from_environment,
    validate_operation_result,
)


HISTORICAL_BACKFILL_JOB_NAME = "research_os_historical_backfill_job"
DATA_INCIDENT_REPAIR_JOB_NAME = "research_os_data_incident_repair_job"


def _durable_utc_timestamp(raw: Any, *, source: str) -> datetime:
    """Normalize a timestamp that has already been persisted by Dagster."""

    if isinstance(raw, datetime):
        if raw.tzinfo is None or raw.utcoffset() is None:
            raise RuntimeError(f"{source} timestamp is timezone-naive")
        return raw.astimezone(timezone.utc)
    if isinstance(raw, (int, float)) and isfinite(float(raw)) and float(raw) > 0:
        return datetime.fromtimestamp(float(raw), tz=timezone.utc)
    if isinstance(raw, str) and raw.strip():
        text_value = raw.strip()
        try:
            numeric = float(text_value)
        except ValueError:
            parsed = datetime.fromisoformat(text_value.replace("Z", "+00:00"))
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise RuntimeError(f"{source} timestamp is timezone-naive")
            return parsed.astimezone(timezone.utc)
        if isfinite(numeric) and numeric > 0:
            return datetime.fromtimestamp(numeric, tz=timezone.utc)
    raise RuntimeError(f"{source} has no stable timestamp")


def failure_event_occurred_at(context: Any) -> datetime:
    """Read a retry-stable failure time from Dagster's durable event storage.

    ``RunFailureSensorContext.failure_event`` is a ``DagsterEvent`` and does not
    itself carry the event-log timestamp.  Looking at it directly therefore
    either fails or tempts callers to substitute wall-clock time.  Resolve the
    matching run's persisted ``RUN_FAILURE`` row instead, with the persisted run
    end time as a compatibility fallback for older Dagster storage backends.
    """

    instance = getattr(context, "instance", None)
    dagster_run = getattr(context, "dagster_run", None)
    run_id = str(getattr(dagster_run, "run_id", "") or "").strip()
    if instance is None or not run_id:
        raise RuntimeError("Dagster failure context has no durable run identity")

    try:
        from dagster import DagsterEventType

        connection = instance.get_records_for_run(
            run_id,
            of_type=DagsterEventType.RUN_FAILURE,
            limit=1,
            ascending=False,
        )
    except Exception as exc:
        raise RuntimeError(
            f"could not read durable Dagster failure event for run {run_id}"
        ) from exc

    records = tuple(getattr(connection, "records", ()) or ())
    if records:
        return _durable_utc_timestamp(
            getattr(records[0], "timestamp", None),
            source="Dagster RUN_FAILURE event",
        )

    try:
        stats = instance.get_run_stats(run_id)
    except Exception as exc:
        raise RuntimeError(
            f"could not read durable Dagster run stats for run {run_id}"
        ) from exc
    return _durable_utc_timestamp(
        getattr(stats, "end_time", None),
        source="Dagster run end",
    )


def partition_key_for(cycle: CycleName, value: datetime) -> str:
    """Build stable Shanghai-time keys used for idempotent run de-duplication."""

    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=timezone.utc)
    local = value.astimezone(ZoneInfo(ORCHESTRATION_TIMEZONE))
    if cycle is CycleName.MONTHLY:
        return local.strftime("%Y-%m")
    if cycle is CycleName.QUARTERLY:
        return f"{local.year}-Q{((local.month - 1) // 3) + 1}"
    return local.date().isoformat()


try:
    from dagster import (
        AssetSelection,
        DynamicPartitionsDefinition,
        DefaultScheduleStatus,
        DefaultSensorStatus,
        Definitions,
        DagsterRunStatus,
        Out,
        RetryPolicy,
        RunRequest,
        RunsFilter,
        SkipReason,
        StaticPartitionsDefinition,
        asset,
        define_asset_job,
        job,
        multiprocess_executor,
        op,
        resource,
        run_failure_sensor,
        schedule,
        sensor,
    )
except ImportError:  # pragma: no cover - expected outside the orchestration image.
    DAGSTER_AVAILABLE = False
    defs = None
    research_os_daily_job = None
    research_os_open_observation_job = None
    research_os_historical_backfill_job = None
    research_os_data_incident_repair_job = None
    research_os_weekly_job = None
    research_os_monthly_job = None
    research_os_quarterly_job = None
    research_os_calendar_registry_job = None
else:
    DAGSTER_AVAILABLE = True

    ACCEPTED_CALENDAR_PARTITIONS = StaticPartitionsDefinition(["SSE"])
    A_SHARE_TRADING_DAY_PARTITIONS = DynamicPartitionsDefinition(
        name="a_share_trading_days"
    )

    @resource(description="Injected Factor Lab Research OS application services")
    def research_os_services_resource(_context) -> Any:
        return services_from_environment()


    def _run_tags(context: Any) -> dict[str, str]:
        run = getattr(context, "run", None)
        return dict(getattr(run, "tags", {}) or {})


    def _run_id(context: Any) -> str:
        run = getattr(context, "run", None)
        value = str(getattr(run, "run_id", "") or "").strip()
        if not value:
            raise RuntimeError("Dagster execution context has no run identity")
        return value


    def _partition_from_context(context: Any, cycle: CycleName) -> str:
        try:
            native = str(context.partition_key or "").strip()
        except Exception:
            native = ""
        if native:
            return native
        tagged = str(_run_tags(context).get("factor_lab/partition_key") or "").strip()
        return tagged or partition_key_for(cycle, datetime.now(timezone.utc))


    def _is_authoritative_backfill_job(context: Any) -> bool:
        """Bind historical authority to a code-defined Dagster job identity.

        Run tags and operation metadata are deliberately ignored.  They are
        operator-controlled and therefore cannot upgrade an ordinary daily run
        to the separately admitted historical-backfill path.
        """

        return str(getattr(context, "job_name", "") or "").strip() == (
            HISTORICAL_BACKFILL_JOB_NAME
        )


    def _execute_for_job(context: Any, request: OperationRequest):
        services = context.resources.research_os_services
        if not _is_authoritative_backfill_job(context):
            if (
                request.operation is OperationName.SHADOW_NAV_STEP
                and isinstance(
                    request.metadata.get("daily_data_outcome"), Mapping
                )
            ):
                settler = getattr(
                    services, "execute_daily_failure_settlement", None
                )
                if callable(settler):
                    return settler(request)
            return services.execute(request)
        executor = getattr(services, "execute_authoritative_backfill", None)
        if not callable(executor):
            raise RuntimeError(
                "historical backfill job requires execute_authoritative_backfill"
            )
        return executor(request)


    def _invoke(context: Any, cycle: CycleName, operation: OperationName) -> dict[str, Any]:
        request = OperationRequest(
            operation=operation,
            cycle=cycle,
            partition_key=_partition_from_context(context, cycle),
            run_id=_run_id(context),
            metadata={"dagster_tags": _run_tags(context)},
        )
        if _is_authoritative_backfill_job(context):
            result = validate_operation_result(
                request, _execute_for_job(context, request)
            )
        else:
            result = execute_operation(context.resources.research_os_services, request)
        context.add_output_metadata(
            {
                "operation": operation.value,
                "status": result.status,
                "summary": result.summary,
                "partition_key": request.partition_key,
            }
        )
        return result.to_dict()


    def _repair_incident_id(context: Any) -> str:
        incident_id = str(
            _run_tags(context).get("factor_lab/repair_incident_id") or ""
        ).strip()
        if not re.fullmatch(r"incident_[0-9a-f]{64}", incident_id):
            raise RuntimeError(
                "incident repair run has no canonical durable incident identity"
            )
        return incident_id


    def _invoke_incident_repair(
        context: Any, operation: OperationName
    ) -> dict[str, Any]:
        services = context.resources.research_os_services
        executor = getattr(services, "execute_data_incident_repair", None)
        if not callable(executor):
            raise RuntimeError(
                "production services do not expose execute_data_incident_repair"
            )
        request = OperationRequest(
            operation=operation,
            cycle=CycleName.DAILY,
            partition_key=_partition_from_context(context, CycleName.DAILY),
            run_id=_run_id(context),
            metadata={"dagster_tags": _run_tags(context)},
        )
        result = executor(request, incident_id=_repair_incident_id(context))
        if not isinstance(result, OperationResult):
            raise TypeError(
                "execute_data_incident_repair must return OperationResult"
            )
        result = validate_operation_result(request, result)
        context.add_output_metadata(
            {
                "operation": operation.value,
                "status": result.status,
                "summary": result.summary,
                "partition_key": request.partition_key,
                "repair_incident_id": _repair_incident_id(context),
            }
        )
        return result.to_dict()


    def _blocked_data_outcome(
        payload: Mapping[str, Any] | None, *, partition_key: str
    ) -> dict[str, str] | None:
        if not isinstance(payload, Mapping):
            return None
        raw = payload.get("daily_data_outcome")
        if not isinstance(raw, Mapping):
            return None
        stage = str(raw.get("failure_stage") or "").strip()
        code = str(raw.get("error_code") or "").strip()
        message = str(raw.get("message") or "").strip()
        occurred_at = str(raw.get("occurred_at") or "").strip()
        if (
            str(raw.get("partition_key") or "") != partition_key
            or str(raw.get("status") or "") != "blocked"
            or stage not in {"source", "silver", "data_quality", "gold"}
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}", code)
            or not message
            or not occurred_at
        ):
            raise RuntimeError("daily data outcome has an invalid typed envelope")
        try:
            parsed = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise RuntimeError("daily data outcome has an invalid timestamp") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise RuntimeError("daily data outcome timestamp must be timezone-aware")
        return {
            "partition_key": partition_key,
            "status": "blocked",
            "failure_stage": stage,
            "error_code": code,
            "message": message[:2_000],
            "occurred_at": parsed.astimezone(timezone.utc).isoformat(),
        }


    def _invoke_data_outcome(
        context: Any,
        cycle: CycleName,
        operation: OperationName,
        *,
        upstream: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Materialize a blocked data outcome so shadow can first de-risk.

        A terminal daily asset raises after the shadow branch has persisted its
        all-cash target, preserving both visible run failure and fail-closed
        portfolio behavior.
        """

        partition_key = _partition_from_context(context, cycle)
        inherited = _blocked_data_outcome(upstream, partition_key=partition_key)
        metadata: dict[str, Any] = {"dagster_tags": _run_tags(context)}
        if operation is OperationName.SHADOW_NAV_STEP and inherited is not None:
            # The envelope only wakes the deterministic incident bridge.  The
            # application service re-derives the failed stage from PostgreSQL
            # and will reject a forged/all-green envelope.
            metadata["daily_data_outcome"] = inherited
        request = OperationRequest(
            operation=operation,
            cycle=cycle,
            partition_key=partition_key,
            run_id=_run_id(context),
            metadata=metadata,
        )
        try:
            result = _execute_for_job(context, request)
        except Exception as exc:
            if _is_authoritative_backfill_job(context):
                raise
            # Synchronous application/gate failures still take the in-run
            # de-risk branch.  A hard worker/process loss cannot be caught
            # here and remains the RUNNING failure sensor's fallback case.
            result = OperationResult(
                operation=operation,
                status="failed",
                summary=(
                    f"{operation.value} raised a synchronous "
                    f"{type(exc).__name__}"
                ),
                outputs={"error_type": type(exc).__name__},
            )
        if result.operation is not operation:
            raise RuntimeError("application service returned a mismatched data outcome")
        if _is_authoritative_backfill_job(context):
            # Unlike the ordinary daily run, a backfill has no shadow de-risk
            # branch to reach before terminal failure.  It therefore stops at
            # the first blocked partition and cannot publish a later Gold step.
            result = validate_operation_result(request, result)
        context.add_output_metadata(
            {
                "operation": operation.value,
                "status": result.status,
                "summary": result.summary,
                "partition_key": request.partition_key,
            }
        )
        payload = result.to_dict()
        if result.status != "completed" and inherited is None:
            stages = {
                OperationName.SOURCE_SYNC: "source",
                OperationName.SOURCE_RECONCILIATION: "silver",
                OperationName.DATA_QUALITY_GATE: "data_quality",
                OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH: "gold",
            }
            stage = stages.get(operation)
            if stage:
                error_code = re.sub(
                    r"[^A-Za-z0-9_.:-]+",
                    "_",
                    str(
                        result.outputs.get("error_type")
                        or f"{operation.value}_{result.status}"
                    ),
                )[:160].strip("_.:-") or "daily_data_blocked"
                inherited = {
                    "partition_key": request.partition_key,
                    "status": "blocked",
                    "failure_stage": stage,
                    "error_code": error_code,
                    "message": str(result.summary)[:2_000],
                    "occurred_at": datetime.now(timezone.utc).isoformat(),
                }
        if inherited is not None:
            payload["daily_data_outcome"] = inherited
        return payload


    @asset(
        group_name="accepted_calendar",
        required_resource_keys={"research_os_services"},
        partitions_def=ACCEPTED_CALENDAR_PARTITIONS,
    )
    def accepted_calendar_partition_registry(context) -> dict[str, Any]:
        """Bootstrap and register only mutually accepted vendor sessions.

        A fresh catalog has no dynamic partitions yet, so it cannot rely on a
        daily asset to create its first accepted-calendar ledger rows.  This
        static SSE asset calls the dual-source bootstrap seam first.  That seam
        reconciles Tushare and Diemeng calendars, persists accepted Silver and
        PostgreSQL partition-ledger evidence, and returns only proven open
        sessions.  Dagster never guesses weekdays.
        """

        exchange = str(context.partition_key or "").strip().upper()
        if exchange != "SSE":
            raise RuntimeError("calendar bootstrap only supports the SSE partition")
        through = datetime.now(ZoneInfo(ORCHESTRATION_TIMEZONE)).date()
        poll: TriggerPoll = (
            context.resources.research_os_services.bootstrap_accepted_calendar(
                exchange=exchange,
                source_start=date(2016, 6, 1),
                through=through,
                dagster_run_id=_run_id(context),
            )
        )
        sessions: list[str] = []
        for trigger in poll.triggers:
            try:
                parsed = date.fromisoformat(trigger.partition_key)
            except ValueError as exc:
                raise RuntimeError(
                    "accepted calendar poller emitted a non-ISO trading session"
                ) from exc
            if parsed < date(2016, 6, 1):
                continue
            sessions.append(parsed.isoformat())
        sessions = sorted(set(sessions))
        if sessions:
            context.instance.add_dynamic_partitions(
                A_SHARE_TRADING_DAY_PARTITIONS.name,
                sessions,
            )
        context.add_output_metadata(
            {
                "registered_session_count": len(sessions),
                "partition_definition": A_SHARE_TRADING_DAY_PARTITIONS.name,
                "source": "dual_source_bootstrap_then_postgresql_ledger",
                "bootstrap_through": through.isoformat(),
            }
        )
        return {
            "status": "completed",
            "registered_sessions": sessions,
            "message": poll.message,
        }


    @asset(
        group_name="daily_execution_open",
        required_resource_keys={"research_os_services"},
        partitions_def=A_SHARE_TRADING_DAY_PARTITIONS,
        retry_policy=RetryPolicy(max_retries=1, delay=15),
        metadata={
            "event_time": "09:30 Asia/Shanghai",
            "authority": "prior accepted Gold decision universe",
            "backfillable": False,
        },
    )
    def execution_open_observation(context) -> dict[str, Any]:
        """Persist the live open independently from the evening Gold closure."""

        partition_key = _partition_from_context(context, CycleName.DAILY)
        observer = getattr(
            context.resources.research_os_services,
            "observe_execution_open",
            None,
        )
        if not callable(observer):
            raise RuntimeError(
                "production services do not expose observe_execution_open"
            )
        try:
            reference = observer(date.fromisoformat(partition_key))
        except Exception as exc:
            reporter = getattr(
                context.resources.research_os_services,
                "report_unexpected_data_failure",
                None,
            )
            if callable(reporter):
                reporter(
                    partition_key=partition_key,
                    error_code="execution_open_observation_failed",
                    message=(
                        "execution open observation failed: "
                        f"{type(exc).__name__}"
                    ),
                    occurred_at=datetime.now(timezone.utc),
                    dagster_run_id=_run_id(context),
                    failed_step_key="execution_open_observation",
                )
            raise RuntimeError(
                "execution open observation failed: " f"{type(exc).__name__}"
            ) from None
        dump = getattr(reference, "model_dump", None)
        if not callable(dump):
            raise RuntimeError(
                "observe_execution_open must return a typed DataSnapshotRef"
            )
        payload = dict(dump(mode="json"))
        required = {
            "snapshot_id",
            "content_hash",
            "uri",
            "tier",
            "quality_status",
        }
        missing = sorted(required - set(payload))
        if missing:
            raise RuntimeError(
                "opening observation reference is incomplete: " + ",".join(missing)
            )
        context.add_output_metadata(
            {
                "partition_key": partition_key,
                "snapshot_id": str(payload["snapshot_id"]),
                "content_hash": str(payload["content_hash"]),
                "quality_status": str(payload["quality_status"]),
                "decision_snapshot_id": str(
                    (payload.get("manifest") or {}).get("decision_snapshot_id") or ""
                ),
            }
        )
        return {"status": "completed", "partition_key": partition_key, **payload}


    @asset(
        group_name="daily_data_plane",
        required_resource_keys={"research_os_services"},
        partitions_def=A_SHARE_TRADING_DAY_PARTITIONS,
        retry_policy=RetryPolicy(max_retries=3, delay=5),
    )
    def bronze_source_partition(context) -> dict[str, Any]:
        """SourceAdapter responses persisted at the immutable Bronze boundary."""

        return _invoke_data_outcome(
            context, CycleName.DAILY, OperationName.SOURCE_SYNC
        )


    @asset(
        group_name="daily_data_plane",
        required_resource_keys={"research_os_services"},
        partitions_def=A_SHARE_TRADING_DAY_PARTITIONS,
        retry_policy=RetryPolicy(max_retries=3, delay=5),
    )
    def silver_reconciled_partition(
        context, bronze_source_partition: dict[str, Any]
    ) -> dict[str, Any]:
        return _invoke_data_outcome(
            context,
            CycleName.DAILY,
            OperationName.SOURCE_RECONCILIATION,
            upstream=bronze_source_partition,
        )


    @asset(
        group_name="daily_data_plane",
        required_resource_keys={"research_os_services"},
        partitions_def=A_SHARE_TRADING_DAY_PARTITIONS,
    )
    def accepted_data_quality_gate(
        context, silver_reconciled_partition: dict[str, Any]
    ) -> dict[str, Any]:
        """Fail-closed gate: disputed or quarantined data stops Gold publication."""

        return _invoke_data_outcome(
            context,
            CycleName.DAILY,
            OperationName.DATA_QUALITY_GATE,
            upstream=silver_reconciled_partition,
        )


    @asset(
        group_name="daily_data_plane",
        required_resource_keys={"research_os_services"},
        partitions_def=A_SHARE_TRADING_DAY_PARTITIONS,
        metadata={
            "storage": "Apache Iceberg on MinIO",
            "catalog": "PostgreSQL-backed PyIceberg SQL catalog",
            "immutability": "catalog snapshot plus named tag",
            "research_panel": "PIT ticker/date panel with manifest-bound Silver parent closure",
            "labels": "research-only; never consumed by shadow execution",
        },
    )
    def gold_iceberg_snapshot(
        context, accepted_data_quality_gate: dict[str, Any]
    ) -> dict[str, Any]:
        """Commit the research-ready Gold panel and immutable Iceberg snapshot tag.

        The service result is rejected unless it reports ``iceberg_table``,
        ``iceberg_snapshot_id`` and ``iceberg_tag``.  When the formal panel
        configuration is enabled the service assembles all cataloged Silver
        history, verifies the Bronze parent closure, and full-history-overwrites
        the current view while preserving every earlier tagged Iceberg snapshot.
        """

        return _invoke_data_outcome(
            context,
            CycleName.DAILY,
            OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
            upstream=accepted_data_quality_gate,
        )


    def _typed_execution_failure(
        context: Any,
        *,
        partition_key: str,
        error_code: str,
        error_type: str,
    ) -> dict[str, Any]:
        """Synchronously freeze the fleet without serializing vendor errors."""

        reporter = getattr(
            context.resources.research_os_services,
            "report_unexpected_data_failure",
            None,
        )
        if not callable(reporter):
            raise RuntimeError(
                "typed execution closure requires the production incident bridge"
            )
        try:
            incident = reporter(
                partition_key=partition_key,
                error_code=error_code,
                message=f"typed execution closure failed: {error_type}",
                occurred_at=datetime.now(timezone.utc),
                dagster_run_id=_run_id(context),
                failed_step_key="typed_execution_session_closure",
            )
        except Exception as exc:
            raise RuntimeError(
                "typed execution closure and synchronous incident persistence "
                f"failed: {type(exc).__name__}"
            ) from None
        return {
            "status": "failed",
            "partition_key": partition_key,
            "summary": f"typed execution closure failed: {error_type}",
            "error_code": error_code,
            "incident": dict(incident),
            "risk_guard": "frozen_data",
        }


    @asset(
        group_name="daily_execution_closure",
        required_resource_keys={"research_os_services"},
        partitions_def=A_SHARE_TRADING_DAY_PARTITIONS,
        retry_policy=RetryPolicy(max_retries=1, delay=15),
        metadata={
            "timing": "after accepted same-session Gold and closing mark",
            "open_authority": "persisted immutable 09:30 observation",
            "forward_projection": "not emitted by this asset",
        },
    )
    def typed_execution_session_closure(
        context, gold_iceberg_snapshot: dict[str, Any]
    ) -> dict[str, Any]:
        """Close the two-stage execution bundle before any shadow projection.

        The morning observation is deliberately outside the daily job.  This
        asset can build and attest an accepted capability before an evidence
        epoch exists, but it never writes an account projection itself.
        """

        partition_key = _partition_from_context(context, CycleName.DAILY)
        blocked = _blocked_data_outcome(
            gold_iceberg_snapshot, partition_key=partition_key
        )
        if blocked is not None:
            return {
                "status": "blocked",
                "partition_key": partition_key,
                "summary": "typed execution closure blocked by daily data",
                "daily_data_outcome": blocked,
            }
        if str(gold_iceberg_snapshot.get("status") or "") != "completed":
            return _typed_execution_failure(
                context,
                partition_key=partition_key,
                error_code="typed_execution_gold_not_completed",
                error_type="GoldNotCompleted",
            )

        builder = getattr(
            context.resources.research_os_services,
            "build_execution_session",
            None,
        )
        if not callable(builder):
            return _typed_execution_failure(
                context,
                partition_key=partition_key,
                error_code="typed_execution_builder_missing",
                error_type="BuilderMissing",
            )
        try:
            session = builder(date.fromisoformat(partition_key))
            if not isinstance(session, TypedExecutionSession):
                raise TypeError("builder returned an untyped execution session")
            if session.trade_date.isoformat() != partition_key:
                raise ValueError("typed execution session date differs from partition")
            references = {
                "execution": session.execution_snapshot,
                "mark": session.mark_snapshot,
                "bundle": session.bundle_snapshot,
            }
            if not all(
                isinstance(reference, DataSnapshotRef)
                for reference in references.values()
            ):
                raise TypeError("typed execution session contains an untyped snapshot")
            if len({ref.snapshot_id for ref in references.values()}) != 3:
                raise ValueError("typed execution snapshot roles are not distinct")
            if any(
                ref.quality_status is not DataQualityStatus.ACCEPTED
                for ref in references.values()
            ):
                raise ValueError("typed execution snapshot is not accepted")
            if (
                session.capability.decision
                is not ExecutionCapabilityDecision.ACCEPTED
                or not session.capability.accepted
            ):
                raise ValueError("typed execution capability is not accepted")
        except Exception as exc:
            return _typed_execution_failure(
                context,
                partition_key=partition_key,
                error_code="typed_execution_closure_failed",
                error_type=type(exc).__name__,
            )

        payload = {
            "status": "completed",
            "partition_key": partition_key,
            "summary": "accepted typed execution session closed",
            "trade_date": session.trade_date.isoformat(),
            "execution_snapshot_id": session.execution_snapshot.snapshot_id,
            "execution_snapshot_hash": session.execution_snapshot.content_hash,
            "mark_snapshot_id": session.mark_snapshot.snapshot_id,
            "mark_snapshot_hash": session.mark_snapshot.content_hash,
            "bundle_snapshot_id": session.bundle_snapshot.snapshot_id,
            "bundle_snapshot_hash": session.bundle_snapshot.content_hash,
            "capability_decision": session.capability.decision.value,
            "capability_evidence_hash": session.capability.evidence_hash,
            "reused": bool(session.reused),
        }
        context.add_output_metadata(
            {
                "status": "completed",
                "partition_key": partition_key,
                "capability_decision": session.capability.decision.value,
                "capability_evidence_hash": session.capability.evidence_hash,
                "reused": bool(session.reused),
            }
        )
        return payload


    @asset(
        group_name="daily_shadow",
        required_resource_keys={"research_os_services"},
        partitions_def=A_SHARE_TRADING_DAY_PARTITIONS,
    )
    def shadow_account_nav(
        context, typed_execution_session_closure: dict[str, Any]
    ) -> dict[str, Any]:
        partition_key = _partition_from_context(context, CycleName.DAILY)
        blocked = _blocked_data_outcome(
            typed_execution_session_closure, partition_key=partition_key
        )
        if blocked is None:
            typed_status = str(
                typed_execution_session_closure.get("status") or "failed"
            )
            if typed_status != "completed":
                # The closure asset already persisted the incident and cash
                # intent.  Do not call the shadow service a second time.
                return {
                    "status": "completed",
                    "partition_key": partition_key,
                    "summary": "shadow projection withheld after typed closure failure",
                    "risk_guard": str(
                        typed_execution_session_closure.get("risk_guard")
                        or "frozen_data"
                    ),
                }
            admission = getattr(
                context.resources.research_os_services,
                "formal_shadow_projection_allowed",
                None,
            )
            if not callable(admission) or not bool(
                admission(date.fromisoformat(partition_key))
            ):
                # Capability closure is useful engineering evidence before
                # activation, but a pre-epoch account row would falsely look
                # forward.  Absence of the admission seam is fail-closed.
                return {
                    "status": "skipped",
                    "partition_key": partition_key,
                    "summary": "typed capability closed; forward epoch not active",
                    "forward_projection": "blocked_pre_epoch",
                    "capability_evidence_hash": str(
                        typed_execution_session_closure.get(
                            "capability_evidence_hash"
                        )
                        or ""
                    ),
                }
        return _invoke_data_outcome(
            context,
            CycleName.DAILY,
            OperationName.SHADOW_NAV_STEP,
            upstream=typed_execution_session_closure,
        )


    @asset(
        group_name="daily_integrity",
        required_resource_keys={"research_os_services"},
        partitions_def=A_SHARE_TRADING_DAY_PARTITIONS,
    )
    def daily_integrity_outcome(
        context,
        bronze_source_partition: dict[str, Any],
        silver_reconciled_partition: dict[str, Any],
        accepted_data_quality_gate: dict[str, Any],
        gold_iceberg_snapshot: dict[str, Any],
        typed_execution_session_closure: dict[str, Any],
        shadow_account_nav: dict[str, Any],
    ) -> dict[str, Any]:
        del context
        statuses = {
            "source": str(bronze_source_partition.get("status") or "failed"),
            "silver": str(silver_reconciled_partition.get("status") or "failed"),
            "data_quality": str(accepted_data_quality_gate.get("status") or "failed"),
            "gold": str(gold_iceberg_snapshot.get("status") or "failed"),
            "typed_execution": str(
                typed_execution_session_closure.get("status") or "failed"
            ),
            "shadow": str(shadow_account_nav.get("status") or "failed"),
        }
        blocked = {
            key: value
            for key, value in statuses.items()
            if key != "shadow" and value != "completed"
        }
        if blocked:
            raise RuntimeError(
                "daily data failed or was blocked after the shadow de-risk path completed: "
                + ", ".join(f"{key}={value}" for key, value in blocked.items())
            )
        if statuses["shadow"] not in {"completed", "skipped"}:
            raise RuntimeError(
                "daily shadow risk guard failed after accepted Gold: "
                + statuses["shadow"]
            )
        return {"status": "completed", "stage_statuses": statuses}


    research_os_calendar_registry_job = define_asset_job(
        "research_os_calendar_registry_job",
        selection=AssetSelection.assets(accepted_calendar_partition_registry),
        description="Register PostgreSQL accepted-calendar sessions as native partitions",
    )

    research_os_open_observation_job = define_asset_job(
        "research_os_open_observation_job",
        selection=AssetSelection.assets(execution_open_observation),
        description=(
            "Collect the non-backfillable 09:30 observation against the prior "
            "accepted decision universe"
        ),
    )

    _daily_assets = [
        bronze_source_partition,
        silver_reconciled_partition,
        accepted_data_quality_gate,
        gold_iceberg_snapshot,
        typed_execution_session_closure,
        shadow_account_nav,
        daily_integrity_outcome,
    ]
    research_os_daily_job = define_asset_job(
        "research_os_daily_job",
        selection=AssetSelection.assets(*_daily_assets),
        executor_def=multiprocess_executor,
        description=(
            "PIT data through tagged Iceberg Gold, typed execution closure, "
            "and epoch-admitted shadow NAV"
        ),
    )
    research_os_historical_backfill_job = define_asset_job(
        HISTORICAL_BACKFILL_JOB_NAME,
        selection=AssetSelection.assets(
            bronze_source_partition,
            silver_reconciled_partition,
            accepted_data_quality_gate,
            gold_iceberg_snapshot,
        ),
        executor_def=multiprocess_executor,
        description=(
            "Authoritative historical Source through immutable Gold; no shadow, "
            "monitoring, discovery, or portfolio operation"
        ),
    )


    @op(
        required_resource_keys={"research_os_services"},
        out=Out(dict),
        retry_policy=RetryPolicy(max_retries=1, delay=15),
    )
    def repair_incident_source(context) -> dict[str, Any]:
        return _invoke_incident_repair(context, OperationName.SOURCE_SYNC)


    @op(
        required_resource_keys={"research_os_services"},
        out=Out(dict),
        retry_policy=RetryPolicy(max_retries=1, delay=15),
    )
    def repair_incident_silver(
        context, source_result: dict[str, Any]
    ) -> dict[str, Any]:
        del source_result
        return _invoke_incident_repair(
            context, OperationName.SOURCE_RECONCILIATION
        )


    @op(
        required_resource_keys={"research_os_services"},
        out=Out(dict),
        retry_policy=RetryPolicy(max_retries=1, delay=15),
    )
    def repair_incident_data_quality(
        context, silver_result: dict[str, Any]
    ) -> dict[str, Any]:
        del silver_result
        return _invoke_incident_repair(
            context, OperationName.DATA_QUALITY_GATE
        )


    @op(
        required_resource_keys={"research_os_services"},
        out=Out(dict),
        retry_policy=RetryPolicy(max_retries=1, delay=15),
    )
    def repair_incident_gold(
        context, quality_result: dict[str, Any]
    ) -> dict[str, Any]:
        del quality_result
        return _invoke_incident_repair(
            context, OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH
        )


    @op(
        required_resource_keys={"research_os_services"},
        out=Out(dict),
        retry_policy=RetryPolicy(max_retries=1, delay=15),
    )
    def repair_incident_shadow(
        context, gold_result: dict[str, Any]
    ) -> dict[str, Any]:
        del gold_result
        return _invoke_incident_repair(context, OperationName.SHADOW_NAV_STEP)


    @op(
        required_resource_keys={"research_os_services"},
        out=Out(dict),
        retry_policy=RetryPolicy(max_retries=1, delay=15),
    )
    def repair_incident_revalidate(
        context, shadow_result: dict[str, Any]
    ) -> dict[str, Any]:
        del shadow_result
        services = context.resources.research_os_services
        revalidator = getattr(services, "revalidate_ready_data_incident", None)
        if not callable(revalidator):
            raise RuntimeError(
                "production services do not expose revalidate_ready_data_incident"
            )
        result = dict(
            revalidator(incident_id=_repair_incident_id(context))
        )
        if result.get("status") != "resolved":
            raise RuntimeError(
                "incident repair did not produce a terminal resolved authority"
            )
        context.add_output_metadata(
            {
                "status": "resolved",
                "repair_incident_id": _repair_incident_id(context),
                "revalidation_id": str(result.get("revalidation_id") or ""),
                "fleet_action": str(result.get("fleet_action") or ""),
            }
        )
        return result


    @job(
        name=DATA_INCIDENT_REPAIR_JOB_NAME,
        executor_def=multiprocess_executor,
    )
    def research_os_data_incident_repair_job() -> None:
        source = repair_incident_source()
        silver = repair_incident_silver(source)
        quality = repair_incident_data_quality(silver)
        gold = repair_incident_gold(quality)
        shadow = repair_incident_shadow(gold)
        repair_incident_revalidate(shadow)


    @op(required_resource_keys={"research_os_services"}, out=Out(dict))
    def weekly_sleeve_health(context) -> dict[str, Any]:
        return _invoke(context, CycleName.WEEKLY, OperationName.SLEEVE_HEALTH_CHECK)


    @op(required_resource_keys={"research_os_services"}, out=Out(dict))
    def weekly_drift_detection(
        context, sleeve_health: dict[str, Any]
    ) -> dict[str, Any]:
        del sleeve_health
        return _invoke(context, CycleName.WEEKLY, OperationName.DRIFT_DETECTION)


    @op(required_resource_keys={"research_os_services"}, out=Out(dict))
    def weekly_lifecycle_transition(
        context, drift: dict[str, Any]
    ) -> dict[str, Any]:
        del drift
        return _invoke(context, CycleName.WEEKLY, OperationName.LIFECYCLE_TRANSITION)


    @op(required_resource_keys={"research_os_services"}, out=Out(dict))
    def weekly_recovery_sla(
        context, lifecycle: dict[str, Any]
    ) -> dict[str, Any]:
        del lifecycle
        return _invoke(context, CycleName.WEEKLY, OperationName.RECOVERY_SLA_CHECK)


    @job(executor_def=multiprocess_executor)
    def research_os_weekly_job() -> None:
        health = weekly_sleeve_health()
        drift = weekly_drift_detection(health)
        lifecycle = weekly_lifecycle_transition(drift)
        weekly_recovery_sla(lifecycle)


    @op(required_resource_keys={"research_os_services"}, out=Out(dict))
    def monthly_confirmatory_budget_gate(
        context,
    ) -> dict[str, Any]:
        return _invoke(
            context, CycleName.MONTHLY, OperationName.CONFIRMATORY_BUDGET_GATE
        )


    @op(required_resource_keys={"research_os_services"}, out=Out(dict))
    def monthly_limited_discovery(
        context, budget: dict[str, Any]
    ) -> dict[str, Any]:
        del budget
        return _invoke(context, CycleName.MONTHLY, OperationName.LIMITED_DISCOVERY)


    @op(required_resource_keys={"research_os_services"}, out=Out(dict))
    def monthly_weight_reestimation(
        context, discovery: dict[str, Any]
    ) -> dict[str, Any]:
        del discovery
        return _invoke(context, CycleName.MONTHLY, OperationName.WEIGHT_REESTIMATION)


    @op(required_resource_keys={"research_os_services"}, out=Out(dict))
    def monthly_challenger_generation(
        context, weights: dict[str, Any]
    ) -> dict[str, Any]:
        del weights
        return _invoke(context, CycleName.MONTHLY, OperationName.CHALLENGER_GENERATION)


    @job(executor_def=multiprocess_executor)
    def research_os_monthly_job() -> None:
        budget = monthly_confirmatory_budget_gate()
        discovery = monthly_limited_discovery(budget)
        weights = monthly_weight_reestimation(discovery)
        monthly_challenger_generation(weights)


    @op(required_resource_keys={"research_os_services"}, out=Out(dict))
    def quarterly_validation_protocol_audit(
        context,
    ) -> dict[str, Any]:
        return _invoke(
            context, CycleName.QUARTERLY, OperationName.VALIDATION_PROTOCOL_AUDIT
        )


    @op(required_resource_keys={"research_os_services"}, out=Out(dict))
    def quarterly_research_budget_audit(
        context, protocol: dict[str, Any]
    ) -> dict[str, Any]:
        del protocol
        return _invoke(
            context, CycleName.QUARTERLY, OperationName.RESEARCH_BUDGET_AUDIT
        )


    @job(executor_def=multiprocess_executor)
    def research_os_quarterly_job() -> None:
        protocol = quarterly_validation_protocol_audit()
        quarterly_research_budget_audit(protocol)


    def _scheduled_run(context: Any, cycle: CycleName) -> RunRequest:
        scheduled = context.scheduled_execution_time or datetime.now(timezone.utc)
        partition_key = partition_key_for(cycle, scheduled)
        kwargs: dict[str, Any] = {
            "run_key": f"{cycle.value}:{partition_key}",
            "tags": cycle_tags(cycle, partition_key),
        }
        if cycle is CycleName.DAILY:
            kwargs["partition_key"] = partition_key
        return RunRequest(**kwargs)


    @schedule(
        job=research_os_open_observation_job,
        cron_schedule="30 9 * * 1-5",
        execution_timezone=ORCHESTRATION_TIMEZONE,
        required_resource_keys={"research_os_services"},
        default_status=DefaultScheduleStatus.STOPPED,
    )
    def research_os_open_observation_schedule(context) -> RunRequest | SkipReason:
        """Launch only a session selected by the accepted PG calendar."""

        selector = getattr(
            context.resources.research_os_services,
            "accepted_execution_open_partition",
            None,
        )
        if not callable(selector):
            raise RuntimeError(
                "production services do not expose accepted_execution_open_partition"
            )
        scheduled = context.scheduled_execution_time or datetime.now(timezone.utc)
        partition_key = selector(scheduled)
        if partition_key is None:
            return SkipReason("scheduled date is not an accepted live trading session")
        partition_key = date.fromisoformat(str(partition_key)).isoformat()
        context.instance.add_dynamic_partitions(
            A_SHARE_TRADING_DAY_PARTITIONS.name,
            [partition_key],
        )
        tags = cycle_tags(CycleName.DAILY, partition_key)
        tags["factor_lab/event"] = "execution_open_0930"
        return RunRequest(
            run_key=f"execution-open:{partition_key}",
            partition_key=partition_key,
            tags=tags,
        )


    @schedule(
        job=research_os_daily_job,
        cron_schedule=CYCLE_BLUEPRINTS[CycleName.DAILY].cron_schedule,
        execution_timezone=ORCHESTRATION_TIMEZONE,
        default_status=DefaultScheduleStatus.STOPPED,
    )
    def research_os_daily_schedule(context) -> RunRequest:
        return _scheduled_run(context, CycleName.DAILY)


    @schedule(
        job=research_os_weekly_job,
        cron_schedule=CYCLE_BLUEPRINTS[CycleName.WEEKLY].cron_schedule,
        execution_timezone=ORCHESTRATION_TIMEZONE,
        default_status=DefaultScheduleStatus.STOPPED,
    )
    def research_os_weekly_schedule(context) -> RunRequest:
        return _scheduled_run(context, CycleName.WEEKLY)


    @schedule(
        job=research_os_monthly_job,
        cron_schedule=CYCLE_BLUEPRINTS[CycleName.MONTHLY].cron_schedule,
        execution_timezone=ORCHESTRATION_TIMEZONE,
        default_status=DefaultScheduleStatus.STOPPED,
    )
    def research_os_monthly_schedule(context) -> RunRequest:
        return _scheduled_run(context, CycleName.MONTHLY)


    @schedule(
        job=research_os_quarterly_job,
        cron_schedule=CYCLE_BLUEPRINTS[CycleName.QUARTERLY].cron_schedule,
        execution_timezone=ORCHESTRATION_TIMEZONE,
        default_status=DefaultScheduleStatus.STOPPED,
    )
    def research_os_quarterly_schedule(
        context,
    ) -> RunRequest:
        return _scheduled_run(context, CycleName.QUARTERLY)


    def _sensor_requests(context: Any, sensor_name: str, cycle: CycleName) -> Any:
        poll: TriggerPoll = context.resources.research_os_services.poll(
            sensor_name, context.cursor
        )
        if poll.cursor is not None and poll.cursor != context.cursor:
            context.update_cursor(poll.cursor)
        if not poll.triggers:
            yield SkipReason(poll.message)
            return
        for trigger in poll.triggers:
            tags = cycle_tags(cycle, trigger.partition_key)
            tags.update({str(key): str(value) for key, value in trigger.metadata.items()})
            kwargs: dict[str, Any] = {"run_key": trigger.run_key, "tags": tags}
            if cycle is CycleName.DAILY:
                # The application poller may emit only sessions proven by the
                # accepted Gold calendar/partition ledger.  Registration is
                # idempotent and unlocks Dagster-native backfill/retry.
                date.fromisoformat(trigger.partition_key)
                context.instance.add_dynamic_partitions(
                    A_SHARE_TRADING_DAY_PARTITIONS.name,
                    [trigger.partition_key],
                )
                kwargs["partition_key"] = trigger.partition_key
            yield RunRequest(**kwargs)


    @sensor(
        job=research_os_daily_job,
        minimum_interval_seconds=60,
        required_resource_keys={"research_os_services"},
        default_status=DefaultSensorStatus.STOPPED,
    )
    def research_os_trading_partition_sensor(
        context,
    ) -> Any:
        """Optional event-driven alternative to the weekday schedule."""

        yield from _sensor_requests(
            context, "new_trading_partition", CycleName.DAILY
        )


    @sensor(
        job=research_os_weekly_job,
        minimum_interval_seconds=300,
        required_resource_keys={"research_os_services"},
        default_status=DefaultSensorStatus.STOPPED,
    )
    def research_os_recovery_sla_sensor(context) -> Any:
        """Wake monitoring when a 5/20/60-day recovery checkpoint is due."""

        yield from _sensor_requests(context, "recovery_sla_due", CycleName.WEEKLY)


    @sensor(
        job=research_os_data_incident_repair_job,
        minimum_interval_seconds=60,
        required_resource_keys={"research_os_services"},
        default_status=DefaultSensorStatus.STOPPED,
    )
    def research_os_data_incident_repair_sensor(context) -> Any:
        """Drain freeze controls and resume each OPEN incident to resolution.

        No Dagster ``run_key`` is used deliberately.  Durable PostgreSQL
        successor authorities make each stage idempotent, while omitting a
        permanent run key lets a later Dagster run resume after a whole-run
        crash.  Concurrent launches are suppressed by querying active runs
        carrying the exact incident tag.
        """

        services = context.resources.research_os_services
        resume_controls = getattr(
            services, "resume_pending_incident_controls", None
        )
        pending_repairs = getattr(services, "pending_data_incident_repairs", None)
        if not callable(resume_controls) or not callable(pending_repairs):
            raise RuntimeError(
                "production services do not expose durable incident repair coordination"
            )
        resume_controls(
            worker_id="dagster-incident-repair-sensor",
            limit=100,
        )
        candidates = tuple(pending_repairs())
        if not candidates:
            yield SkipReason("no OPEN data incident is ready for repair")
            return

        active_statuses = [
            DagsterRunStatus.QUEUED,
            DagsterRunStatus.NOT_STARTED,
            DagsterRunStatus.MANAGED,
            DagsterRunStatus.STARTING,
            DagsterRunStatus.STARTED,
            DagsterRunStatus.CANCELING,
        ]
        emitted = 0
        for raw in candidates:
            if not isinstance(raw, Mapping):
                raise RuntimeError(
                    "incident repair coordinator returned a malformed candidate"
                )
            incident_id = str(raw.get("incident_id") or "").strip()
            partition_key = str(raw.get("partition_key") or "").strip()
            cohort_id = str(raw.get("repair_cohort_id") or "").strip()
            try:
                canonical_partition = date.fromisoformat(
                    partition_key
                ).isoformat()
            except ValueError:
                canonical_partition = ""
            if not (
                re.fullmatch(r"incident_[0-9a-f]{64}", incident_id)
                and canonical_partition == partition_key
                and re.fullmatch(r"repaircohort_[0-9a-f]{64}", cohort_id)
            ):
                raise RuntimeError(
                    "incident repair coordinator returned non-canonical authority"
                )
            active = context.instance.get_runs(
                filters=RunsFilter(
                    tags={"factor_lab/repair_incident_id": incident_id},
                    statuses=active_statuses,
                ),
                limit=1,
            )
            if active:
                continue
            tags = cycle_tags(CycleName.DAILY, partition_key)
            tags.update(
                {
                    "factor_lab/event": "data_incident_repair",
                    "factor_lab/repair_incident_id": incident_id,
                    "factor_lab/repair_cohort_id": cohort_id,
                }
            )
            emitted += 1
            yield RunRequest(tags=tags)
        if emitted == 0:
            yield SkipReason("all OPEN data incidents already have an active repair run")


    @sensor(
        minimum_interval_seconds=300,
        required_resource_keys={"research_os_services"},
        default_status=DefaultSensorStatus.STOPPED,
    )
    def research_os_code_location_soak_sensor(context) -> SkipReason:
        """Record a no-trade gRPC/PG heartbeat sample for the 24-hour soak."""

        recorder = getattr(
            context.resources.research_os_services,
            "record_dagster_code_location_health",
            None,
        )
        if not callable(recorder):
            raise RuntimeError(
                "production services do not expose code-location health recording"
            )
        sample = recorder()
        if sample.get("status") != "recorded":
            return SkipReason(str(sample.get("reason") or "soak sample skipped"))
        return SkipReason(
            "recorded code-location health sample "
            + str(sample.get("sample_evidence_hash") or "")[:16]
        )


    @run_failure_sensor(
        monitored_jobs=[research_os_daily_job],
        minimum_interval_seconds=30,
        # Keep every production trigger stopped until doctor/readiness and the
        # explicit launch gate have passed.  Operators enable this sensor with
        # the daily schedule as one controlled activation unit.
        default_status=DefaultSensorStatus.STOPPED,
    )
    def research_os_unexpected_daily_failure_sensor(context) -> SkipReason:
        """Persist unexpected worker failures through the production incident seam."""

        dagster_run = context.dagster_run
        tags = dict(dagster_run.tags or {})
        partition_key = str(tags.get("factor_lab/partition_key") or "").strip()
        if not partition_key:
            raise RuntimeError("failed daily run has no authoritative partition tag")
        failure_event = context.failure_event
        step_key = str(getattr(failure_event, "step_key", "") or "unknown")
        message = str(getattr(failure_event, "message", "") or "Dagster run failed")
        services = services_from_environment()
        try:
            reporter = getattr(services, "report_unexpected_data_failure", None)
            if not callable(reporter):
                raise RuntimeError(
                    "production services do not expose report_unexpected_data_failure"
                )
            try:
                incident = reporter(
                    partition_key=partition_key,
                    error_code="dagster_run_failure",
                    message=message[:2000],
                    occurred_at=failure_event_occurred_at(context),
                    dagster_run_id=dagster_run.run_id,
                    failed_step_key=step_key,
                )
            except NonDataPipelineFailure:
                return SkipReason(
                    f"recorded non-data Dagster failure for {partition_key}/{step_key}"
                )
            if isinstance(incident, Mapping) and bool(incident.get("reused")):
                return SkipReason(
                    "reused durable unexpected daily failure for "
                    f"{partition_key}/{step_key}"
                )
            return SkipReason(
                f"registered unexpected daily failure for {partition_key}/{step_key}"
            )
        finally:
            close = getattr(services, "close", None)
            if not callable(close):
                close = getattr(getattr(services, "catalog", None), "close", None)
            if callable(close):
                close()


    defs = Definitions(
        assets=[
            accepted_calendar_partition_registry,
            execution_open_observation,
            *_daily_assets,
        ],
        jobs=[
            research_os_calendar_registry_job,
            research_os_open_observation_job,
            research_os_daily_job,
            research_os_historical_backfill_job,
            research_os_data_incident_repair_job,
            research_os_weekly_job,
            research_os_monthly_job,
            research_os_quarterly_job,
        ],
        schedules=[
            research_os_open_observation_schedule,
            research_os_daily_schedule,
            research_os_weekly_schedule,
            research_os_monthly_schedule,
            research_os_quarterly_schedule,
        ],
        sensors=[
            research_os_trading_partition_sensor,
            research_os_recovery_sla_sensor,
            research_os_data_incident_repair_sensor,
            research_os_code_location_soak_sensor,
            research_os_unexpected_daily_failure_sensor,
        ],
        resources={"research_os_services": research_os_services_resource},
    )


__all__ = [
    "CYCLE_BLUEPRINTS",
    "DAGSTER_AVAILABLE",
    "DATA_INCIDENT_REPAIR_JOB_NAME",
    "HISTORICAL_BACKFILL_JOB_NAME",
    "failure_event_occurred_at",
    "defs",
    "partition_key_for",
    "research_os_calendar_registry_job",
    "research_os_daily_job",
    "research_os_data_incident_repair_job",
    "research_os_historical_backfill_job",
    "research_os_monthly_job",
    "research_os_open_observation_job",
    "research_os_quarterly_job",
    "research_os_weekly_job",
]
