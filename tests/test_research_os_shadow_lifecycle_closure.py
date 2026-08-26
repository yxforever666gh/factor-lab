from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
from pathlib import Path

import pandas as pd
import pytest

from factor_lab.research_os.catalog import (
    LifecycleEvent,
    ResearchCatalog,
    ShadowEventInput,
)
from factor_lab.research_os.contracts import (
    DataSnapshotRef,
    EnvironmentRef,
    ExperimentSpec,
    LifecycleState,
    Preregistration,
)
from factor_lab.research_os.data_incidents import (
    DataIncident,
    DataIncidentCoordinator,
    DataPipelineStage,
    DataRevalidation,
)
from factor_lab.research_os.lifecycle import SleeveLifecycleRecord, SleeveState
from factor_lab.research_os.monitor import (
    EventChainHealthBuilder,
    EventChainMonitorPolicy,
    MonitorEvidenceError,
)
from factor_lab.research_os.shadow import (
    ShadowExecutionConfig,
    ShadowSnapshotBindings,
    assert_no_forward_label_access,
)
from factor_lab.research_os.shadow_catalog import ShadowDataBlocked, ShadowStepService
from factor_lab.research_os.production_daily import (
    DailyDataOutcome,
    DailyDataStatus,
    ProductionDailyControl,
)
from factor_lab.research_os.sleeve_lifecycle import (
    DailyShadowPlan,
    SleeveShadowLifecycleService,
)
from factor_lab.research_os.sleeve_registry import load_sleeve_roster


ROOT = Path(__file__).resolve().parents[1]
CALENDAR = ("2026-01-02", "2026-01-05", "2026-01-06")


def _snapshot(
    snapshot_id: str,
    *,
    as_of: datetime,
    fill: str,
    sessions: tuple[str, ...] = CALENDAR,
) -> DataSnapshotRef:
    calendar_hash = hashlib.sha256("\n".join(sessions).encode("ascii")).hexdigest()
    return DataSnapshotRef(
        snapshot_id=snapshot_id,
        tier="gold",
        uri=f"s3://test/{snapshot_id}",
        content_hash=fill * 64,
        as_of=as_of,
        manifest={
            "trading_calendar": {
                "source": "test-calendar",
                "quality_status": "accepted",
                "sessions": list(sessions),
                "content_hash": calendar_hash,
            }
        },
    )


def _register_daily_snapshots(catalog: ResearchCatalog) -> None:
    for snapshot in (
        _snapshot(
            "decision-20260102",
            as_of=datetime(2026, 1, 2, 7, tzinfo=timezone.utc),
            fill="a",
        ),
        _snapshot(
            "execution-20260105",
            as_of=datetime(2026, 1, 5, 1, 30, tzinfo=timezone.utc),
            fill="b",
        ),
        _snapshot(
            "mark-20260105",
            as_of=datetime(2026, 1, 5, 7, 1, tzinfo=timezone.utc),
            fill="c",
        ),
        _snapshot(
            "execution-20260106",
            as_of=datetime(2026, 1, 6, 1, 30, tzinfo=timezone.utc),
            fill="d",
        ),
        _snapshot(
            "mark-20260106",
            as_of=datetime(2026, 1, 6, 7, 1, tzinfo=timezone.utc),
            fill="e",
        ),
    ):
        catalog.register_snapshot(snapshot)


def _bars(
    session: str,
    *,
    execution_snapshot_id: str,
    mark_snapshot_id: str,
    split_ratio: float = 1.0,
    cash_dividend: float = 0.0,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": "000001.SZ",
                "trade_date": session,
                "execution_event_time": f"{session}T09:30:00+08:00",
                "execution_available_at": f"{session}T09:30:00+08:00",
                "mark_event_time": f"{session}T15:00:00+08:00",
                "mark_available_at": f"{session}T15:01:00+08:00",
                "execution_snapshot_id": execution_snapshot_id,
                "mark_snapshot_id": mark_snapshot_id,
                "open_adj": 10.0,
                "close_adj": 11.0,
                "adv_20": 100_000_000.0,
                "volatility_20": 0.02,
                "is_one_price_limit_up": False,
                "is_one_price_limit_down": False,
                "is_suspended": False,
                "is_delisted": False,
                "split_ratio": split_ratio,
                "cash_dividend": cash_dividend,
            }
        ]
    )


def test_daily_projection_marks_hold_days_and_separates_snapshot_roles(tmp_path) -> None:
    with ResearchCatalog(tmp_path / "daily.db") as catalog:
        catalog.initialize_schema()
        _register_daily_snapshots(catalog)
        catalog.create_shadow_account(
            account_id="champion",
            name="Champion",
            initial_capital=1_000_000,
            opened_at=datetime(2025, 12, 31, tzinfo=timezone.utc),
        )
        service = ShadowStepService(
            catalog,
            ShadowExecutionConfig(max_position_weight=0.5, lot_size=100),
        )
        first = service.project_session(
            account_id="champion",
            trade_date="2026-01-05",
            market_bars=_bars(
                "2026-01-05",
                execution_snapshot_id="execution-20260105",
                mark_snapshot_id="mark-20260105",
            ),
            snapshot_bindings=ShadowSnapshotBindings(
                decision_snapshot_id="decision-20260102",
                execution_snapshot_id="execution-20260105",
                mark_snapshot_id="mark-20260105",
            ),
            benchmark_return=0.01,
            target_weights={"000001.SZ": 0.5},
            model_version="champion-v1",
            session_metrics={"rank_ic": 0.02},
        )
        quantity_before = catalog.list_shadow_positions("champion")[0].quantity
        cash_before = catalog.get_shadow_account("champion").cash

        second = service.project_session(
            account_id="champion",
            trade_date="2026-01-06",
            market_bars=_bars(
                "2026-01-06",
                execution_snapshot_id="execution-20260106",
                mark_snapshot_id="mark-20260106",
                split_ratio=2.0,
                cash_dividend=0.1,
            ),
            snapshot_bindings=ShadowSnapshotBindings(
                decision_snapshot_id=None,
                execution_snapshot_id="execution-20260106",
                mark_snapshot_id="mark-20260106",
            ),
            benchmark_return=0.02,
            target_weights=None,
            session_metrics={"rank_ic": 0.01},
        )

        position = catalog.list_shadow_positions("champion")[0]
        account = catalog.get_shadow_account("champion")
        assert first.rebalanced is True
        assert second.rebalanced is False
        assert position.quantity == pytest.approx(quantity_before * 2.0)
        assert account.cash > cash_before
        assert account.benchmark_nav == pytest.approx(1_000_000 * 1.01 * 1.02)
        assert account.nav == pytest.approx(account.cash + position.market_value)
        projections = catalog.list_shadow_events_by_type(
            account_id="champion",
            event_type="account_projected",
            since=None,
            through=None,
            limit=10,
        )
        assert len(projections) == 2
        assert "corporate_action_split" in second.domain_event_types
        assert "corporate_action_dividend" in second.domain_event_types
        assert "target_received" not in second.domain_event_types

        with pytest.raises(ValueError, match="distinct snapshots"):
            ShadowSnapshotBindings(
                decision_snapshot_id="same",
                execution_snapshot_id="same",
                mark_snapshot_id="other",
            )
        with pytest.raises(ValueError, match="forward"):
            assert_no_forward_label_access('value = frame["forward_return_5d"]')


def test_daily_projection_cannot_skip_company_action_session(tmp_path) -> None:
    sessions = ("2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07")
    with ResearchCatalog(tmp_path / "daily-gap.db") as catalog:
        catalog.initialize_schema()
        for snapshot in (
            _snapshot(
                "gap-decision-20260102",
                as_of=datetime(2026, 1, 2, 7, tzinfo=timezone.utc),
                fill="1",
                sessions=sessions,
            ),
            _snapshot(
                "gap-execution-20260105",
                as_of=datetime(2026, 1, 5, 1, 30, tzinfo=timezone.utc),
                fill="2",
                sessions=sessions,
            ),
            _snapshot(
                "gap-mark-20260105",
                as_of=datetime(2026, 1, 5, 7, 1, tzinfo=timezone.utc),
                fill="3",
                sessions=sessions,
            ),
            _snapshot(
                "gap-execution-20260107",
                as_of=datetime(2026, 1, 7, 1, 30, tzinfo=timezone.utc),
                fill="4",
                sessions=sessions,
            ),
            _snapshot(
                "gap-mark-20260107",
                as_of=datetime(2026, 1, 7, 7, 1, tzinfo=timezone.utc),
                fill="5",
                sessions=sessions,
            ),
        ):
            catalog.register_snapshot(snapshot)
        catalog.create_shadow_account(
            account_id="gap-champion",
            name="Gap Champion",
            initial_capital=1_000_000,
            opened_at=datetime(2025, 12, 31, tzinfo=timezone.utc),
        )
        service = ShadowStepService(
            catalog,
            ShadowExecutionConfig(max_position_weight=0.5, lot_size=100),
        )
        service.project_session(
            account_id="gap-champion",
            trade_date="2026-01-05",
            market_bars=_bars(
                "2026-01-05",
                execution_snapshot_id="gap-execution-20260105",
                mark_snapshot_id="gap-mark-20260105",
            ),
            snapshot_bindings=ShadowSnapshotBindings(
                decision_snapshot_id="gap-decision-20260102",
                execution_snapshot_id="gap-execution-20260105",
                mark_snapshot_id="gap-mark-20260105",
            ),
            benchmark_return=0.0,
            target_weights={"000001.SZ": 0.5},
            model_version="gap-champion-v1",
        )
        before = catalog.list_shadow_positions("gap-champion")[0]
        event_count = len(
            catalog.list_shadow_events(account_id="gap-champion", limit=1_000)
        )

        with pytest.raises(
            ShadowDataBlocked,
            match="replay the missing session/company actions",
        ):
            service.project_session(
                account_id="gap-champion",
                trade_date="2026-01-07",
                market_bars=_bars(
                    "2026-01-07",
                    execution_snapshot_id="gap-execution-20260107",
                    mark_snapshot_id="gap-mark-20260107",
                    split_ratio=2.0,
                    cash_dividend=0.1,
                ),
                snapshot_bindings=ShadowSnapshotBindings(
                    decision_snapshot_id=None,
                    execution_snapshot_id="gap-execution-20260107",
                    mark_snapshot_id="gap-mark-20260107",
                ),
                benchmark_return=0.0,
                target_weights=None,
            )

        after = catalog.list_shadow_positions("gap-champion")[0]
        assert after.quantity == before.quantity
        assert len(
            catalog.list_shadow_events(account_id="gap-champion", limit=1_000)
        ) == event_count


def test_data_incident_freezes_and_records_cash_intent_without_fake_fill(tmp_path) -> None:
    with ResearchCatalog(tmp_path / "incident.db") as catalog:
        catalog.initialize_schema()
        _register_daily_snapshots(catalog)
        catalog.create_shadow_account(
            account_id="sleeve-account",
            name="Sleeve",
            initial_capital=1_000_000,
            opened_at=datetime(2025, 12, 31, tzinfo=timezone.utc),
        )
        before = catalog.get_shadow_account("sleeve-account")
        record = SleeveLifecycleRecord(
            sleeve_id="value_quality_v1",
            state=SleeveState.ACTIVE,
            target_weight=0.25,
            effective_weight=0.25,
        )
        catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key="incident-fixture-active",
                sleeve_id=record.sleeve_id,
                to_state=LifecycleState.ACTIVE,
                cause="fixture active state",
                occurred_at=datetime(2026, 1, 5, 8, tzinfo=timezone.utc),
            )
        )
        incident = DataIncident(
            stage=DataPipelineStage.SOURCE,
            partition_key="2026-01-06",
            error_code="source_probe_failed",
            message="mandatory source unavailable",
            occurred_at=datetime(2026, 1, 6, 8, tzinfo=timezone.utc),
            source_ids=("tushare",),
        )
        coordinator = DataIncidentCoordinator(catalog)
        result = coordinator.report(
            incident,
            lifecycle_records=(record,),
            shadow_accounts={record.sleeve_id: ("sleeve-account",)},
        )
        after = catalog.get_shadow_account("sleeve-account")
        assert result.lifecycle_records[0].state is SleeveState.FROZEN_DATA
        assert result.cash_target_intent.cash_weight == 1.0
        assert result.cash_target_intent.execution_state == "awaiting_trusted_execution"
        assert after.cash == before.cash
        assert after.nav == before.nav
        events = catalog.list_shadow_events(account_id="sleeve-account", limit=20)
        assert any(event.event_type == "cash_target_intent" for event in events)
        assert not any(event.event_type == "fill" for event in events)

        revalidation = DataRevalidation(
            incident_id=incident.incident_id,
            snapshot_id="mark-20260106",
            snapshot_content_hash="e" * 64,
            occurred_at=datetime(2026, 1, 6, 9, tzinfo=timezone.utc),
        )
        restored = coordinator.revalidate(
            revalidation,
            lifecycle_records=result.lifecycle_records,
            shadow_accounts={record.sleeve_id: ("sleeve-account",)},
        )
        assert restored[0].state is SleeveState.DORMANT
        assert restored[0].effective_weight == 0.0


def test_data_incident_freezes_a_roster_sleeve_with_no_lifecycle_root(tmp_path) -> None:
    with ResearchCatalog(tmp_path / "fresh-roster-incident.db") as catalog:
        catalog.initialize_schema()
        catalog.create_shadow_account(
            account_id="fresh-roster-account",
            name="Fresh roster account",
            initial_capital=1_000_000,
            opened_at=datetime(2026, 1, 5, tzinfo=timezone.utc),
        )
        virtual_record = SleeveLifecycleRecord(
            sleeve_id="value_quality_v1",
            state=SleeveState.PROPOSED,
        )
        incident = DataIncident(
            stage=DataPipelineStage.SOURCE,
            partition_key="2026-01-06",
            error_code="source_probe_failed",
            message="mandatory source unavailable",
            occurred_at=datetime(2026, 1, 6, 8, tzinfo=timezone.utc),
        )
        coordinator = DataIncidentCoordinator(catalog)

        first = coordinator.report(
            incident,
            lifecycle_records=(virtual_record,),
            shadow_accounts={
                virtual_record.sleeve_id: ("fresh-roster-account",)
            },
        )
        replay = coordinator.report(
            incident,
            lifecycle_records=(virtual_record,),
            shadow_accounts={
                virtual_record.sleeve_id: ("fresh-roster-account",)
            },
        )

        lifecycle = catalog.list_lifecycle_events(
            sleeve_id=virtual_record.sleeve_id, limit=10
        )
        assert len(lifecycle) == 1
        assert lifecycle[0].from_state is None
        assert lifecycle[0].to_state is LifecycleState.FROZEN_DATA
        assert first.lifecycle_records[0].state is SleeveState.FROZEN_DATA
        assert replay.lifecycle_records[0].state is SleeveState.FROZEN_DATA
        cash_intents = catalog.list_shadow_events_by_type(
            account_id="fresh-roster-account",
            event_type="cash_target_intent",
            since=None,
            through=None,
            limit=10,
        )
        assert len(cash_intents) == 1


def test_interleaved_incidents_keep_origin_time_but_apply_causal_effects(tmp_path) -> None:
    with ResearchCatalog(tmp_path / "interleaved-incidents.db") as catalog:
        catalog.initialize_schema()
        catalog.create_shadow_account(
            account_id="interleaved-account",
            name="Interleaved account",
            initial_capital=1_000_000,
            opened_at=datetime(2026, 1, 5, tzinfo=timezone.utc),
        )
        active = SleeveLifecycleRecord(
            sleeve_id="value_quality_v1",
            state=SleeveState.ACTIVE,
            target_weight=0.25,
            effective_weight=0.25,
        )
        catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key="interleaved-fixture-active",
                sleeve_id=active.sleeve_id,
                to_state=LifecycleState.ACTIVE,
                cause="fixture active state",
                occurred_at=datetime(2026, 1, 5, 8, tzinfo=timezone.utc),
            )
        )
        earlier = DataIncident(
            stage=DataPipelineStage.SOURCE,
            partition_key="2026-01-06",
            error_code="earlier_source_failure",
            message="earlier failure whose effects were delayed",
            occurred_at=datetime(2026, 1, 6, 8, tzinfo=timezone.utc),
        )
        later = DataIncident(
            stage=DataPipelineStage.SILVER,
            partition_key="2026-01-06",
            error_code="later_silver_failure",
            message="later failure applied first",
            occurred_at=datetime(2026, 1, 6, 8, 1, tzinfo=timezone.utc),
        )
        coordinator = DataIncidentCoordinator(catalog)
        later_result = coordinator.report(
            later,
            lifecycle_records=(active,),
            shadow_accounts={active.sleeve_id: ("interleaved-account",)},
        )
        earlier_result = coordinator.report(
            earlier,
            lifecycle_records=later_result.lifecycle_records,
            shadow_accounts={active.sleeve_id: ("interleaved-account",)},
        )

        events = catalog.list_lifecycle_events(
            sleeve_id=active.sleeve_id, limit=10
        )
        by_incident = {
            event.evidence["data_incident"]["incident_id"]: event
            for event in events
            if "data_incident" in event.evidence
        }
        assert by_incident[earlier.incident_id].occurred_at > by_incident[
            later.incident_id
        ].occurred_at
        persisted_origin = by_incident[earlier.incident_id].evidence[
            "data_incident"
        ]["occurred_at"]
        assert datetime.fromisoformat(
            str(persisted_origin).replace("Z", "+00:00")
        ) == earlier.occurred_at
        assert earlier_result.lifecycle_records[0].state is SleeveState.FROZEN_DATA
        assert catalog.latest_lifecycle_state(active.sleeve_id) is LifecycleState.FROZEN_DATA
        cash_intents = catalog.list_shadow_events_by_type(
            account_id="interleaved-account",
            event_type="cash_target_intent",
            since=None,
            through=None,
            limit=10,
        )
        assert {event.payload["incident_id"] for event in cash_intents} == {
            earlier.incident_id,
            later.incident_id,
        }


def test_production_daily_control_routes_failures_and_advances_entire_fleet(tmp_path) -> None:
    with ResearchCatalog(tmp_path / "production-daily.db") as catalog:
        catalog.initialize_schema()
        _register_daily_snapshots(catalog)
        for account_id in ("champion", "challenger"):
            catalog.create_shadow_account(
                account_id=account_id,
                name=account_id,
                initial_capital=1_000_000,
                opened_at=datetime(2025, 12, 31, tzinfo=timezone.utc),
            )
        record = SleeveLifecycleRecord(
            sleeve_id="value_quality_v1",
            state=SleeveState.ACTIVE,
            target_weight=0.25,
            effective_weight=0.25,
        )
        catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key="production-daily-fixture-active",
                sleeve_id=record.sleeve_id,
                to_state=LifecycleState.ACTIVE,
                cause="fixture active state",
                occurred_at=datetime(2026, 1, 4, 8, tzinfo=timezone.utc),
            )
        )
        control = ProductionDailyControl(catalog)
        accepted = control.run(
            outcome=DailyDataOutcome(
                partition_key="2026-01-05",
                status=DailyDataStatus.ACCEPTED,
                occurred_at=datetime(2026, 1, 5, 7, 2, tzinfo=timezone.utc),
                execution_snapshot_id="execution-20260105",
                mark_snapshot_id="mark-20260105",
            ),
            lifecycle_records=(record,),
            shadow_accounts={record.sleeve_id: ("challenger",)},
            plans=(
                DailyShadowPlan(
                    account_id="champion",
                    role="champion",
                    target_weights={"000001.SZ": 0.02},
                    decision_snapshot_id="decision-20260102",
                    model_version="champion-v1",
                ),
                DailyShadowPlan(
                    account_id="challenger",
                    role="challenger",
                    target_weights={"000001.SZ": 0.02},
                    decision_snapshot_id="decision-20260102",
                    model_version="challenger-v1",
                ),
            ),
            market_bars=_bars(
                "2026-01-05",
                execution_snapshot_id="execution-20260105",
                mark_snapshot_id="mark-20260105",
            ),
            benchmark_return=0.01,
            session_metrics={
                "champion": {"rank_ic": 0.01},
                "challenger": {"rank_ic": 0.02},
            },
        )
        assert len(accepted.projections) == 2
        assert {row.account_id for row in accepted.projections} == {
            "champion",
            "challenger",
        }

        blocked_outcome = DailyDataOutcome(
            partition_key="2026-01-06",
            status=DailyDataStatus.BLOCKED,
            occurred_at=datetime(2026, 1, 6, 8, tzinfo=timezone.utc),
            failure_stage=DataPipelineStage.SILVER,
            error_code="reconciliation_disputed",
            message="mandatory Silver field is disputed",
            source_ids=("tushare", "akshare"),
        )
        with pytest.raises(ValueError, match="rejects monitor_inputs"):
            control.run(
                outcome=blocked_outcome,
                lifecycle_records=(record,),
                shadow_accounts={record.sleeve_id: ("challenger",)},
                monitor_inputs={"active_ir_13w": 99},
            )
        blocked = control.run(
            outcome=blocked_outcome,
            lifecycle_records=(record,),
            shadow_accounts={record.sleeve_id: ("challenger",)},
        )
        assert blocked.incident is not None
        assert blocked.incident.lifecycle_records[0].state is SleeveState.FROZEN_DATA
        assert blocked.projections == ()


def test_event_chain_monitor_is_deterministic_and_rejects_unbound_projection(tmp_path) -> None:
    with ResearchCatalog(tmp_path / "monitor.db") as catalog:
        catalog.initialize_schema()
        mark_snapshot = _snapshot(
            "mark-monitor",
            as_of=datetime(2025, 12, 31, tzinfo=timezone.utc),
            fill="f",
        )
        catalog.register_snapshot(mark_snapshot)
        catalog.create_shadow_account(
            account_id="monitor-account",
            name="Monitor",
            initial_capital=1_000_000,
            opened_at=datetime(2025, 12, 31, tzinfo=timezone.utc),
        )
        previous_hash = catalog.get_shadow_account("monitor-account").last_event_hash
        start = date(2026, 1, 1)
        for index in range(60):
            session = start + timedelta(days=index + 1)
            occurred = datetime.combine(session, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=7)
            step_id = f"daily-{index:03d}"
            nav = 1_000_000 * (1.0015 ** (index + 1))
            benchmark = 1_000_000 * (1.0005 ** (index + 1))
            committed = catalog.append_shadow_events_atomic(
                account_id="monitor-account",
                expected_previous_hash=previous_hash,
                events=(
                    ShadowEventInput(
                        event_type="session_evidence",
                        occurred_at=occurred,
                        payload={
                            "research_os_shadow_step": {"step_id": step_id, "kind": "domain_event"},
                            "snapshot_bindings": {
                                "decision_snapshot_id": None,
                                "execution_snapshot_id": f"execution-{index}",
                                "mark_snapshot_id": "mark-monitor",
                            },
                            "timing": {"mark_available_at": occurred.isoformat()},
                            "fees": 10.0,
                            "metrics": {"rank_ic": 0.02},
                        },
                    ),
                    ShadowEventInput(
                        event_type="account_projected",
                        occurred_at=occurred,
                        payload={
                            "research_os_shadow_step": {"step_id": step_id, "kind": "account_projection"},
                            "account_status": "active",
                            "account_state": {
                                "cash": nav,
                                "nav": nav,
                                "benchmark_nav": benchmark,
                            },
                        },
                    ),
                ),
            )
            previous_hash = committed[-1].event_hash
        record = SleeveLifecycleRecord(
            sleeve_id="value_quality_v1",
            state=SleeveState.DORMANT,
            target_weight=0.25,
            dormant_since=start,
        )
        builder = EventChainHealthBuilder(catalog)
        first = builder.derive(
            account_id="monitor-account",
            record=record,
            policy=EventChainMonitorPolicy(minimum_sessions=60),
        )
        second = builder.derive(
            account_id="monitor-account",
            record=record,
            policy=EventChainMonitorPolicy(minimum_sessions=60),
        )
        assert first.evidence_hash == second.evidence_hash
        assert first.observation.active_return_20d > 0
        assert first.observation.active_return_60d > 0
        assert first.observation.new_sessions_since_dormant == 60

        catalog.create_shadow_account(
            account_id="unbound-account",
            name="Unbound",
            initial_capital=1_000_000,
            opened_at=datetime(2025, 12, 31, tzinfo=timezone.utc),
        )
        unbound = catalog.get_shadow_account("unbound-account")
        catalog.append_shadow_event(
            account_id="unbound-account",
            event_type="account_projected",
            occurred_at=datetime(2026, 1, 2, 7, tzinfo=timezone.utc),
            expected_previous_hash=unbound.last_event_hash,
            payload={
                "research_os_shadow_step": {"step_id": "missing-evidence", "kind": "account_projection"},
                "account_state": {
                    "cash": 1_000_000,
                    "nav": 1_000_000,
                    "benchmark_nav": 1_000_000,
                },
            },
        )
        with pytest.raises(MonitorEvidenceError, match="session_evidence"):
            builder.derive(
                account_id="unbound-account",
                record=record,
                policy=EventChainMonitorPolicy(minimum_sessions=60),
            )


def test_promoted_roster_sleeve_gets_shadow_account_and_59_60_activation_gate(tmp_path) -> None:
    with ResearchCatalog(tmp_path / "promotion.db") as catalog:
        catalog.initialize_schema()
        roster = load_sleeve_roster(ROOT / "configs" / "research_os_initial_sleeves.json")
        sleeve = roster.by_sleeve_id()["value_quality_v1"].sleeve
        snapshot = _snapshot(
            "gold-promotion",
            as_of=datetime(2026, 1, 2, 7, tzinfo=timezone.utc),
            fill="9",
        )
        catalog.register_snapshot(snapshot)
        experiment = catalog.register_experiment(
            ExperimentSpec(
                snapshot=snapshot,
                sleeve=sleeve,
                evaluator_version="research_os.long_only.v2",
                environment=EnvironmentRef(
                    code_hash="1" * 64,
                    dependency_lock_hash="2" * 64,
                    configuration_hash="3" * 64,
                    python_version="3.10.16",
                    platform="test",
                    evaluator_build="research_os.long_only.v2",
                ),
                preregistration=Preregistration(
                    hypothesis_id="value-quality-shadow",
                    economic_mechanism=sleeve.mechanism,
                    direction="positive",
                    falsification_criteria=sleeve.falsification_criteria,
                    stop_rules=("stop after the frozen protocol",),
                ),
            )
        )
        catalog.record_authoritative_result(
            experiment.experiment_id,
            outcome="promoted_to_shadow",
            metrics={"net_sharpe": 0.9},
            completed_at=datetime(2026, 1, 2, 8, tzinfo=timezone.utc),
        )
        record = SleeveLifecycleRecord(
            sleeve_id=sleeve.sleeve_id,
            state=SleeveState.WALK_FORWARD,
            target_weight=0.25,
        )
        promoted_at = datetime(2026, 1, 2, 9, tzinfo=timezone.utc)
        lifecycle_states = (
            LifecycleState.PREREGISTERED,
            LifecycleState.CANARY,
            LifecycleState.WALK_FORWARD,
        )
        catalog.append_lifecycle_path(
            tuple(
                LifecycleEvent(
                    idempotency_key=f"promotion-fixture:{state.value}",
                    sleeve_id=sleeve.sleeve_id,
                    from_state=(
                        None if index == 0 else lifecycle_states[index - 1]
                    ),
                    to_state=state,
                    cause="fixture deterministic research path",
                    occurred_at=datetime(2026, 1, 2, 8, 30, tzinfo=timezone.utc)
                    + timedelta(microseconds=index),
                )
                for index, state in enumerate(lifecycle_states)
            )
        )
        service = SleeveShadowLifecycleService(catalog)
        binding = service.promote(
            record=record,
            experiment_id=experiment.experiment_id,
            roster=roster,
            promoted_at=promoted_at,
            initial_capital=1_000_000,
        )
        assert binding.lifecycle.record.state is SleeveState.SHADOW
        assert catalog.get_shadow_account(binding.shadow_account_id) is not None

        previous_hash = catalog.get_shadow_account(binding.shadow_account_id).last_event_hash
        for index in range(59):
            occurred = promoted_at + timedelta(days=index + 1)
            step_id = f"shadow-{index + 1}"
            committed = catalog.append_shadow_events_atomic(
                account_id=binding.shadow_account_id,
                expected_previous_hash=previous_hash,
                events=(
                    ShadowEventInput(
                        event_type="session_evidence",
                        occurred_at=occurred,
                        payload={
                            "research_os_shadow_step": {"step_id": step_id, "kind": "domain_event"},
                            "snapshot_bindings": {
                                "decision_snapshot_id": None,
                                "execution_snapshot_id": f"exec-{index}",
                                "mark_snapshot_id": f"mark-{index}",
                            },
                            "timing": {"mark_available_at": occurred.isoformat()},
                            "fees": 0.0,
                            "metrics": {"rank_ic": 0.01},
                        },
                    ),
                    ShadowEventInput(
                        event_type="account_projected",
                        occurred_at=occurred,
                        payload={
                            "research_os_shadow_step": {"step_id": step_id, "kind": "account_projection"},
                            "account_status": "active",
                            "account_state": {
                                "cash": 1_000_000,
                                "nav": 1_000_000,
                                "benchmark_nav": 1_000_000,
                            },
                        },
                    ),
                ),
            )
            previous_hash = committed[-1].event_hash
        shadow_record = binding.lifecycle.record
        at_59 = service.authorize_activation(
            record=shadow_record,
            shadow_account_id=binding.shadow_account_id,
            observation_started_on=promoted_at.date(),
            as_of=promoted_at + timedelta(days=59, minutes=1),
        )
        assert at_59.record.state is SleeveState.SHADOW

        occurred = promoted_at + timedelta(days=60)
        committed = catalog.append_shadow_events_atomic(
            account_id=binding.shadow_account_id,
            expected_previous_hash=previous_hash,
            events=(
                ShadowEventInput(
                    event_type="session_evidence",
                    occurred_at=occurred,
                    payload={
                        "research_os_shadow_step": {"step_id": "shadow-60", "kind": "domain_event"},
                        "snapshot_bindings": {
                            "decision_snapshot_id": None,
                            "execution_snapshot_id": "exec-60",
                            "mark_snapshot_id": "mark-60",
                        },
                        "timing": {"mark_available_at": occurred.isoformat()},
                        "fees": 0.0,
                        "metrics": {"rank_ic": 0.01},
                    },
                ),
                ShadowEventInput(
                    event_type="account_projected",
                    occurred_at=occurred,
                    payload={
                        "research_os_shadow_step": {"step_id": "shadow-60", "kind": "account_projection"},
                        "account_status": "active",
                        "account_state": {
                            "cash": 1_000_000,
                            "nav": 1_000_000,
                            "benchmark_nav": 1_000_000,
                        },
                    },
                ),
            ),
        )
        assert committed
        at_60 = service.authorize_activation(
            record=shadow_record,
            shadow_account_id=binding.shadow_account_id,
            observation_started_on=promoted_at.date(),
            as_of=occurred + timedelta(minutes=1),
        )
        # The 60th new session opens the bounded probation ramp; it must never
        # jump a Challenger directly from SHADOW to ACTIVE.
        assert at_60.record.state is SleeveState.PROBATION
        assert at_60.record.effective_weight == pytest.approx(0.05)
