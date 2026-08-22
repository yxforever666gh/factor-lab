"""Safe, injectable service boundary for Research OS orchestration.

Dagster is deliberately kept at the edge of the system.  This module contains
the schedule contract and a small synchronous runner that can be exercised in
unit tests without importing Dagster.  Production services are supplied through
``FACTOR_LAB_ORCHESTRATION_FACTORY=package.module:factory``; an absent factory
fails closed instead of turning a scheduled run into false research evidence.

Only historical research, monitoring and shadow-account operations are in the
allow-list.  There is no broker, order-routing or live-trading capability here.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
import importlib
import os
from typing import Any, Callable, Mapping, Protocol, Sequence, runtime_checkable


ORCHESTRATION_FACTORY_ENV = "FACTOR_LAB_ORCHESTRATION_FACTORY"
ORCHESTRATION_TIMEZONE = "Asia/Shanghai"


class CycleName(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"


class OperationName(str, Enum):
    SOURCE_SYNC = "source_sync"
    SOURCE_RECONCILIATION = "source_reconciliation"
    DATA_QUALITY_GATE = "data_quality_gate"
    GOLD_ICEBERG_SNAPSHOT_PUBLISH = "gold_iceberg_snapshot_publish"
    SHADOW_NAV_STEP = "shadow_nav_step"
    SLEEVE_HEALTH_CHECK = "sleeve_health_check"
    DRIFT_DETECTION = "drift_detection"
    LIFECYCLE_TRANSITION = "lifecycle_transition"
    RECOVERY_SLA_CHECK = "recovery_sla_check"
    CONFIRMATORY_BUDGET_GATE = "confirmatory_budget_gate"
    LIMITED_DISCOVERY = "limited_discovery"
    WEIGHT_REESTIMATION = "weight_reestimation"
    CHALLENGER_GENERATION = "challenger_generation"
    VALIDATION_PROTOCOL_AUDIT = "validation_protocol_audit"
    RESEARCH_BUDGET_AUDIT = "research_budget_audit"


@dataclass(frozen=True)
class CycleBlueprint:
    name: CycleName
    cron_schedule: str
    operations: tuple[OperationName, ...]
    description: str


CYCLE_BLUEPRINTS: Mapping[CycleName, CycleBlueprint] = {
    CycleName.DAILY: CycleBlueprint(
        name=CycleName.DAILY,
        cron_schedule="30 18 * * 1-5",
        operations=(
            OperationName.SOURCE_SYNC,
            OperationName.SOURCE_RECONCILIATION,
            OperationName.DATA_QUALITY_GATE,
            OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
            OperationName.SHADOW_NAV_STEP,
        ),
        description="Daily PIT data, reconciliation, fail-closed quality, snapshot and shadow NAV",
    ),
    CycleName.WEEKLY: CycleBlueprint(
        name=CycleName.WEEKLY,
        cron_schedule="0 20 * * 5",
        operations=(
            OperationName.SLEEVE_HEALTH_CHECK,
            OperationName.DRIFT_DETECTION,
            OperationName.LIFECYCLE_TRANSITION,
            OperationName.RECOVERY_SLA_CHECK,
        ),
        description="Weekly sleeve health, drift, lifecycle and recovery-SLA evaluation",
    ),
    CycleName.MONTHLY: CycleBlueprint(
        name=CycleName.MONTHLY,
        cron_schedule="0 20 1 * *",
        operations=(
            OperationName.CONFIRMATORY_BUDGET_GATE,
            OperationName.LIMITED_DISCOVERY,
            OperationName.WEIGHT_REESTIMATION,
            OperationName.CHALLENGER_GENERATION,
        ),
        description="Monthly budgeted discovery, weights and Challenger generation",
    ),
    CycleName.QUARTERLY: CycleBlueprint(
        name=CycleName.QUARTERLY,
        cron_schedule="0 10 1 1,4,7,10 *",
        operations=(
            OperationName.VALIDATION_PROTOCOL_AUDIT,
            OperationName.RESEARCH_BUDGET_AUDIT,
        ),
        description="Quarterly protocol and statistical-budget audit; thresholds stay immutable",
    ),
}


@dataclass(frozen=True)
class OperationRequest:
    operation: OperationName
    cycle: CycleName
    partition_key: str
    run_id: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.partition_key.strip():
            raise ValueError("partition_key is required")
        if not self.run_id.strip():
            raise ValueError("run_id is required")
        blueprint = CYCLE_BLUEPRINTS[self.cycle]
        if self.operation not in blueprint.operations:
            raise ValueError(
                f"{self.operation.value!r} is not allowed in the {self.cycle.value} cycle"
            )


@dataclass(frozen=True)
class OperationResult:
    operation: OperationName
    status: str
    summary: str
    outputs: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in {"completed", "skipped", "blocked", "failed"}:
            raise ValueError(f"unsupported operation status: {self.status!r}")

    @property
    def successful(self) -> bool:
        return self.status in {"completed", "skipped"}

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["operation"] = self.operation.value
        return value


@dataclass(frozen=True)
class Trigger:
    partition_key: str
    run_key: str
    metadata: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.partition_key.strip() or not self.run_key.strip():
            raise ValueError("trigger partition_key and run_key are required")


@dataclass(frozen=True)
class TriggerPoll:
    triggers: tuple[Trigger, ...] = ()
    cursor: str | None = None
    message: str = "no trigger"


class OrchestrationFailure(RuntimeError):
    """Raised when a fail-closed operation cannot produce authoritative output."""


class ServiceNotConfigured(OrchestrationFailure):
    """Raised when the Dagster process has no application service factory."""


@runtime_checkable
class ResearchOSServices(Protocol):
    """Boundary implemented by the application layer, not by Dagster ops."""

    def execute(self, request: OperationRequest) -> OperationResult:
        ...

    def poll(self, sensor_name: str, cursor: str | None) -> TriggerPoll:
        ...


OperationHandler = Callable[[OperationRequest], OperationResult | Mapping[str, Any]]
TriggerPoller = Callable[[str | None], TriggerPoll]


def validate_operation_result(
    request: OperationRequest, result: OperationResult
) -> OperationResult:
    if result.operation is not request.operation:
        raise OrchestrationFailure(
            "operation handler returned a result for a different operation"
        )
    if (
        request.operation is OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH
        and result.successful
    ):
        required = {"iceberg_table", "iceberg_snapshot_id", "iceberg_tag"}
        missing = sorted(required - set(result.outputs))
        if missing:
            raise OrchestrationFailure(
                "Gold publication must commit an Iceberg catalog snapshot and immutable "
                f"tag; missing outputs: {missing}"
            )
    if not result.successful:
        raise OrchestrationFailure(
            f"{request.operation.value} {result.status}: {result.summary}"
        )
    return result


def execute_operation(
    services: ResearchOSServices, request: OperationRequest
) -> OperationResult:
    result = services.execute(request)
    if not isinstance(result, OperationResult):
        raise TypeError("ResearchOSServices.execute() must return OperationResult")
    return validate_operation_result(request, result)


class HandlerResearchOSServices:
    """In-process adapter used by local workers and deterministic tests."""

    def __init__(
        self,
        handlers: Mapping[OperationName, OperationHandler],
        *,
        pollers: Mapping[str, TriggerPoller] | None = None,
    ) -> None:
        self._handlers = dict(handlers)
        self._pollers = dict(pollers or {})
        unknown = set(self._handlers) - set(OperationName)
        if unknown:
            raise ValueError(f"unknown orchestration operations: {sorted(unknown)}")

    def execute(self, request: OperationRequest) -> OperationResult:
        try:
            handler = self._handlers[request.operation]
        except KeyError as exc:
            raise ServiceNotConfigured(
                f"no service handler registered for {request.operation.value!r}"
            ) from exc
        value = handler(request)
        if isinstance(value, OperationResult):
            result = value
        elif isinstance(value, Mapping):
            result = OperationResult(
                operation=request.operation,
                status=str(value.get("status") or "completed"),
                summary=str(value.get("summary") or request.operation.value),
                outputs=dict(value.get("outputs") or {}),
            )
        else:
            raise TypeError(
                f"handler {request.operation.value!r} returned {type(value).__name__}"
            )
        return validate_operation_result(request, result)

    def poll(self, sensor_name: str, cursor: str | None) -> TriggerPoll:
        poller = self._pollers.get(sensor_name)
        if poller is None:
            return TriggerPoll(cursor=cursor, message=f"{sensor_name} poller is not configured")
        value = poller(cursor)
        if not isinstance(value, TriggerPoll):
            raise TypeError(f"sensor poller {sensor_name!r} must return TriggerPoll")
        return value


class UnconfiguredResearchOSServices:
    """Loadable default that refuses to fabricate successful scheduled work."""

    def execute(self, request: OperationRequest) -> OperationResult:
        raise ServiceNotConfigured(
            f"set {ORCHESTRATION_FACTORY_ENV}=package.module:factory before running "
            f"{request.operation.value!r}"
        )

    def poll(self, sensor_name: str, cursor: str | None) -> TriggerPoll:
        return TriggerPoll(
            cursor=cursor,
            message=f"set {ORCHESTRATION_FACTORY_ENV} to enable {sensor_name}",
        )


def _load_factory(path: str) -> Callable[[], ResearchOSServices]:
    module_name, separator, attribute = path.partition(":")
    if not separator or not module_name.strip() or not attribute.strip():
        raise ValueError(
            f"{ORCHESTRATION_FACTORY_ENV} must use 'package.module:factory' syntax"
        )
    module = importlib.import_module(module_name)
    factory = getattr(module, attribute)
    if not callable(factory):
        raise TypeError(f"orchestration factory {path!r} is not callable")
    return factory


def services_from_environment(
    env: Mapping[str, str] | None = None,
) -> ResearchOSServices:
    values = os.environ if env is None else env
    path = str(values.get(ORCHESTRATION_FACTORY_ENV) or "").strip()
    if not path:
        return UnconfiguredResearchOSServices()
    services = _load_factory(path)()
    if not isinstance(services, ResearchOSServices):
        raise TypeError(
            f"orchestration factory {path!r} must return an object with execute() and poll()"
        )
    return services


def run_cycle(
    cycle: CycleName,
    services: ResearchOSServices,
    *,
    partition_key: str,
    run_id: str,
    metadata: Mapping[str, Any] | None = None,
) -> tuple[OperationResult, ...]:
    """Execute one blueprint synchronously, stopping at the first failed gate."""

    results: list[OperationResult] = []
    for operation in CYCLE_BLUEPRINTS[cycle].operations:
        results.append(
            execute_operation(
                services,
                OperationRequest(
                    operation=operation,
                    cycle=cycle,
                    partition_key=partition_key,
                    run_id=run_id,
                    metadata=dict(metadata or {}),
                )
            )
        )
    return tuple(results)


def cycle_tags(cycle: CycleName, partition_key: str) -> dict[str, str]:
    return {
        "factor_lab/cycle": cycle.value,
        "factor_lab/partition_key": partition_key,
        "factor_lab/execution_scope": "research_and_shadow_only",
    }


def assert_research_only_surface() -> None:
    """Guard against accidentally widening the orchestrator into execution APIs."""

    forbidden = ("broker", "live_trade", "live_order", "submit_order", "real_money")
    names: Sequence[str] = [item.value for item in OperationName]
    violations = [name for name in names if any(token in name for token in forbidden)]
    if violations:
        raise RuntimeError(f"forbidden execution capabilities in orchestrator: {violations}")


assert_research_only_surface()


__all__ = [
    "CYCLE_BLUEPRINTS",
    "ORCHESTRATION_FACTORY_ENV",
    "ORCHESTRATION_TIMEZONE",
    "CycleBlueprint",
    "CycleName",
    "HandlerResearchOSServices",
    "OperationName",
    "OperationRequest",
    "OperationResult",
    "OrchestrationFailure",
    "ResearchOSServices",
    "ServiceNotConfigured",
    "Trigger",
    "TriggerPoll",
    "UnconfiguredResearchOSServices",
    "assert_research_only_surface",
    "cycle_tags",
    "execute_operation",
    "run_cycle",
    "services_from_environment",
    "validate_operation_result",
]
