"""Production-facing Research OS daily control without legacy file inputs.

This is the narrow integration seam for Dagster/application services.  It
turns typed source outcomes into incidents, advances every configured shadow
account on accepted data, and derives monitoring exclusively from the event
ledger.  It does not read files and exposes no broker side effects.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, Mapping, Sequence

import pandas as pd

from .catalog import ResearchCatalog
from .data_incidents import (
    DataIncident,
    DataIncidentCoordinator,
    DataIncidentResult,
    DataPipelineStage,
)
from .lifecycle import SleeveLifecycleRecord
from .monitor import (
    EventChainMonitorPolicy,
    LifecycleMonitor,
    MonitorEvidenceError,
    MonitorTickResult,
)
from .shadow_catalog import ShadowStepResult
from .sleeve_lifecycle import (
    DailyShadowPlan,
    PromotedShadowBinding,
    ShadowFleetCoordinator,
    SleeveShadowLifecycleService,
)
from .sleeve_registry import SleeveRosterManifest
from .shadow_authority import ShadowEvidenceAuthority


class DailyDataStatus(str, Enum):
    ACCEPTED = "accepted"
    BLOCKED = "blocked"


@dataclass(frozen=True)
class DailyDataOutcome:
    partition_key: str
    status: DailyDataStatus
    occurred_at: datetime
    execution_snapshot_id: str | None = None
    mark_snapshot_id: str | None = None
    failure_stage: DataPipelineStage | None = None
    error_code: str | None = None
    message: str | None = None
    source_ids: tuple[str, ...] = ()
    evidence_hashes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.status, DailyDataStatus):
            object.__setattr__(self, "status", DailyDataStatus(self.status))
        if self.failure_stage is not None and not isinstance(
            self.failure_stage, DataPipelineStage
        ):
            object.__setattr__(
                self, "failure_stage", DataPipelineStage(self.failure_stage)
            )
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ValueError("daily outcome occurred_at must include a timezone")
        if not self.partition_key.strip():
            raise ValueError("partition_key is required")
        if self.status is DailyDataStatus.ACCEPTED:
            if not self.execution_snapshot_id or not self.mark_snapshot_id:
                raise ValueError("accepted outcome requires execution and mark snapshots")
            if self.execution_snapshot_id == self.mark_snapshot_id:
                raise ValueError("execution and mark snapshots must be distinct")
            if any((self.failure_stage, self.error_code, self.message)):
                raise ValueError("accepted outcome cannot carry failure fields")
        else:
            if self.failure_stage is None or not self.error_code or not self.message:
                raise ValueError("blocked outcome requires typed stage, code and message")

    def to_incident(self) -> DataIncident:
        if self.status is not DailyDataStatus.BLOCKED:
            raise ValueError("accepted outcome is not a data incident")
        assert self.failure_stage is not None and self.error_code and self.message
        return DataIncident(
            stage=self.failure_stage,
            partition_key=self.partition_key,
            error_code=self.error_code,
            message=self.message,
            occurred_at=self.occurred_at,
            source_ids=self.source_ids,
            evidence_hashes=self.evidence_hashes,
        )


@dataclass(frozen=True)
class ProductionMonitorRequest:
    record: SleeveLifecycleRecord
    shadow_account_id: str
    policy: EventChainMonitorPolicy = EventChainMonitorPolicy()


@dataclass(frozen=True)
class ProductionDailyResult:
    partition_key: str
    data_status: DailyDataStatus
    incident: DataIncidentResult | None
    projections: tuple[ShadowStepResult, ...]
    monitor_ticks: tuple[MonitorTickResult, ...]
    monitor_warmups: Mapping[str, str]


class ProductionDailyControl:
    """Single typed seam for the authoritative daily Research OS path."""

    def __init__(
        self,
        catalog: ResearchCatalog,
        *,
        shadow_authority: ShadowEvidenceAuthority | None = None,
    ) -> None:
        self.catalog = catalog
        self.shadow_authority = shadow_authority

    def run(
        self,
        *,
        outcome: DailyDataOutcome,
        lifecycle_records: Sequence[SleeveLifecycleRecord],
        shadow_accounts: Mapping[str, Sequence[str]],
        plans: Sequence[DailyShadowPlan] = (),
        market_bars: pd.DataFrame | None = None,
        benchmark_return: float | None = None,
        session_metrics: Mapping[str, Mapping[str, Any]] | None = None,
        monitor_requests: Sequence[ProductionMonitorRequest] = (),
        trading_sessions: Sequence[date] = (),
        monitor_inputs: Any = None,
    ) -> ProductionDailyResult:
        if monitor_inputs is not None:
            raise ValueError(
                "production monitoring rejects monitor_inputs; use the shadow event chain"
            )
        if outcome.status is DailyDataStatus.BLOCKED:
            incident = DataIncidentCoordinator(self.catalog).report(
                outcome.to_incident(),
                lifecycle_records=lifecycle_records,
                shadow_accounts=shadow_accounts,
            )
            return ProductionDailyResult(
                partition_key=outcome.partition_key,
                data_status=outcome.status,
                incident=incident,
                projections=(),
                monitor_ticks=(),
                monitor_warmups={},
            )
        if market_bars is None or benchmark_return is None:
            raise ValueError("accepted daily outcome requires bars and benchmark_return")
        if not plans:
            raise ValueError("accepted daily outcome requires Champion/Challenger plans")
        assert outcome.execution_snapshot_id and outcome.mark_snapshot_id
        projections = ShadowFleetCoordinator(
            self.catalog, shadow_authority=self.shadow_authority
        ).project_daily(
            plans=plans,
            trade_date=date.fromisoformat(outcome.partition_key),
            market_bars=market_bars,
            execution_snapshot_id=outcome.execution_snapshot_id,
            mark_snapshot_id=outcome.mark_snapshot_id,
            benchmark_return=float(benchmark_return),
            session_metrics=session_metrics or {},
        )
        ticks: list[MonitorTickResult] = []
        warmups: dict[str, str] = {}
        monitor = LifecycleMonitor(self.catalog)
        for request in sorted(
            monitor_requests, key=lambda item: item.record.sleeve_id
        ):
            try:
                ticks.append(
                    monitor.tick_from_event_chain(
                        request.record,
                        shadow_account_id=request.shadow_account_id,
                        policy=request.policy,
                        trading_sessions=trading_sessions,
                    )
                )
            except MonitorEvidenceError as exc:
                warmups[request.record.sleeve_id] = str(exc)
        return ProductionDailyResult(
            partition_key=outcome.partition_key,
            data_status=outcome.status,
            incident=None,
            projections=projections,
            monitor_ticks=tuple(ticks),
            monitor_warmups=dict(sorted(warmups.items())),
        )

    def promote_sleeve(
        self,
        *,
        record: SleeveLifecycleRecord,
        experiment_id: str,
        roster: SleeveRosterManifest,
        promoted_at: datetime,
        initial_capital: float = 50_000_000.0,
        recovery_case_id: str | None = None,
    ) -> PromotedShadowBinding:
        return SleeveShadowLifecycleService(self.catalog).promote(
            record=record,
            experiment_id=experiment_id,
            roster=roster,
            promoted_at=promoted_at,
            initial_capital=initial_capital,
            recovery_case_id=recovery_case_id,
        )


__all__ = [
    "DailyDataOutcome",
    "DailyDataStatus",
    "ProductionDailyControl",
    "ProductionDailyResult",
    "ProductionMonitorRequest",
]
