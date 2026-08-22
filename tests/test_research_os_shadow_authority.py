from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import hashlib
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine, delete, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from factor_lab.research_os.catalog import _SQLAlchemyCatalog, new_evidence_epoch
from factor_lab.research_os.fingerprint import content_fingerprint
from factor_lab.research_os.champion_control import AuthoritativeChampionControl
from factor_lab.research_os.lifecycle import (
    ShadowActivationEvidence,
    SleeveLifecycleRecord,
    SleeveState,
    authorize_shadow_activation,
)
from factor_lab.research_os.orm import (
    Base,
    DataSnapshotModel,
    DataIncidentModel,
    EvidenceEpochModel,
    EvidenceEpochPointerModel,
    ExperimentModel,
    ExperimentResultModel,
    ShadowAccountModel,
    ShadowEventModel,
    ShadowRoleBindingModel,
    ShadowSessionModel,
)
from factor_lab.research_os.shadow_authority import (
    IncompleteFleetEvidence,
    InsufficientForwardEvidence,
    MisalignedForwardEvidence,
    ShadowEvidenceAuthority,
    ShadowRole,
    ShadowRoleConflict,
    ShadowSessionRejected,
)


UTC = timezone.utc
FIRST_SESSION = date(2026, 8, 24)
DECISION_SNAPSHOT = "decision-gold"
EXECUTION_SNAPSHOT = "execution-gold"
MARK_SNAPSHOT = "mark-gold"


def _engine():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return engine


def _seed_authority():
    engine = _engine()
    frozen_at = datetime(2026, 8, 21, 8, tzinfo=UTC)
    with Session(engine) as session, session.begin():
        for snapshot_id, fill in (
            (DECISION_SNAPSHOT, "a"),
            (EXECUTION_SNAPSHOT, "b"),
            (MARK_SNAPSHOT, "c"),
        ):
            session.add(
                DataSnapshotModel(
                    snapshot_id=snapshot_id,
                    schema_version="research-os/data-snapshot/v1",
                    tier="gold",
                    uri=f"s3://factor-lab/gold/{snapshot_id}",
                    content_hash=fill * 64,
                    as_of=frozen_at,
                    quality_status="accepted",
                    ref_json={"snapshot_id": snapshot_id},
                    created_at=frozen_at,
                )
            )
        session.add(
            EvidenceEpochModel(
                epoch_slot="research_os",
                epoch_id="epoch_formal_001",
                schema_version="research-os/evidence-epoch/v1",
                architecture_version="test-v1",
                frozen_at=frozen_at,
                code_hash="1" * 64,
                configuration_hash="2" * 64,
                dependency_lock_hash="3" * 64,
                dirty_patch_hash="4" * 64,
                epoch_hash="5" * 64,
                first_forward_session=FIRST_SESSION.isoformat(),
                calendar_snapshot_id=DECISION_SNAPSHOT,
                calendar_snapshot_hash="a" * 64,
                calendar_content_hash="6" * 64,
                evidence_window_hash="7" * 64,
                activated_at=frozen_at + timedelta(minutes=1),
            )
        )
        session.add(
            EvidenceEpochPointerModel(
                pointer_key="research_os",
                epoch_id="epoch_formal_001",
                updated_at=frozen_at + timedelta(minutes=1),
            )
        )
        session.add(
            ExperimentModel(
                experiment_id="challenger_exp_001",
                fingerprint="8" * 64,
                snapshot_id=DECISION_SNAPSHOT,
                candidate_kind="sleeve",
                candidate_id="value_quality_v1",
                family="value_quality",
                status="completed",
                spec_json={"candidate_kind": "sleeve"},
                registered_at=frozen_at,
                updated_at=frozen_at,
            )
        )
        session.add(
            ExperimentModel(
                experiment_id="champion_exp_001",
                fingerprint="a" * 64,
                snapshot_id=DECISION_SNAPSHOT,
                candidate_kind="sleeve",
                candidate_id="static_champion_v1",
                family="static_champion",
                status="completed",
                spec_json={"candidate_kind": "sleeve"},
                registered_at=frozen_at,
                updated_at=frozen_at,
            )
        )
        session.add(
            ExperimentResultModel(
                result_id="challenger_result_001",
                experiment_id="challenger_exp_001",
                result_hash="9" * 64,
                outcome="promoted_to_shadow",
                metrics_json={"promotion_verdict": "promote"},
                artifact_uri=None,
                authoritative=True,
                completed_at=frozen_at,
            )
        )
        session.add(
            ExperimentResultModel(
                result_id="champion_result_001",
                experiment_id="champion_exp_001",
                result_hash="b" * 64,
                outcome="promoted_to_shadow",
                metrics_json={"promotion_verdict": "promote"},
                artifact_uri=None,
                authoritative=True,
                completed_at=frozen_at,
            )
        )
        for account_id in ("static_champion", "dynamic_challenger"):
            session.add(
                ShadowAccountModel(
                    account_id=account_id,
                    name=account_id,
                    currency="CNY",
                    initial_capital=100.0,
                    cash=100.0,
                    nav=100.0,
                    benchmark_nav=100.0,
                    status="active",
                    as_of=frozen_at,
                    last_event_sequence=0,
                    last_event_hash="0" * 64,
                    updated_at=frozen_at,
                )
            )
    authority = ShadowEvidenceAuthority(engine)
    champion = authority.bind_role(
        role=ShadowRole.CHAMPION,
        role_key="static",
        account_id="static_champion",
        sleeve_id="static_champion_v1",
        experiment_id="champion_exp_001",
        bound_at=frozen_at + timedelta(minutes=2),
    )
    challenger = authority.bind_role(
        role=ShadowRole.CHALLENGER,
        role_key="challenger_exp_001",
        account_id="dynamic_challenger",
        sleeve_id="value_quality_v1",
        experiment_id="challenger_exp_001",
        bound_at=frozen_at + timedelta(minutes=2),
    )
    return engine, authority, champion, challenger


def _event_hash(
    *,
    account_id: str,
    sequence_number: int,
    event_type: str,
    occurred_at: datetime,
    payload: dict,
    previous_event_hash: str,
) -> str:
    return content_fingerprint(
        {
            "account_id": account_id,
            "sequence_number": sequence_number,
            "event_type": event_type,
            "occurred_at": occurred_at,
            "payload": payload,
            "previous_event_hash": previous_event_hash,
        },
        domain="factor-lab/research-os/v1/shadow-event",
    )


def _append_projection(
    engine,
    authority: ShadowEvidenceAuthority,
    *,
    account_id: str,
    binding_id: str,
    trade_date: date,
    nav: float,
    invalid_positions_value: float | None = None,
    decision_snapshot_id: str | None = None,
    rebalanced: bool = False,
):
    occurred_at = datetime.combine(trade_date, time(7), tzinfo=UTC)
    step_id = f"daily-{account_id}-{trade_date.isoformat()}"
    bindings = {
        "decision_snapshot_id": decision_snapshot_id,
        "execution_snapshot_id": EXECUTION_SNAPSHOT,
        "mark_snapshot_id": MARK_SNAPSHOT,
    }
    payloads = (
        (
            "session_evidence",
            {
                "research_os_shadow_step": {
                    "step_id": step_id,
                    "kind": "domain_event",
                },
                "snapshot_bindings": bindings,
                "rebalanced": rebalanced,
                "fees": 0.0,
                "metrics": {},
            },
        ),
        (
            "mark_to_market",
            {
                "research_os_shadow_step": {
                    "step_id": step_id,
                    "kind": "domain_event",
                },
                "snapshot_bindings": bindings,
                "cash": nav,
                "positions_value": (
                    0.0
                    if invalid_positions_value is None
                    else invalid_positions_value
                ),
                "nav": nav,
                "benchmark_nav": nav,
                "position_count": 0,
            },
        ),
        (
            "account_projected",
            {
                "research_os_shadow_step": {
                    "step_id": step_id,
                    "kind": "account_projection",
                },
                "account_status": "active",
                "account_state": {
                    "cash": nav,
                    "nav": nav,
                    "benchmark_nav": nav,
                },
            },
        ),
    )
    projection_hash = ""
    with Session(engine) as session, session.begin():
        account = session.get(ShadowAccountModel, account_id)
        assert account is not None
        previous = account.last_event_hash
        sequence = int(account.last_event_sequence)
        for event_type, payload in payloads:
            sequence += 1
            digest = _event_hash(
                account_id=account_id,
                sequence_number=sequence,
                event_type=event_type,
                occurred_at=occurred_at,
                payload=payload,
                previous_event_hash=previous,
            )
            session.add(
                ShadowEventModel(
                    event_id=f"evt_{digest[:60]}",
                    account_id=account_id,
                    sequence_number=sequence,
                    event_type=event_type,
                    occurred_at=occurred_at,
                    payload_json=payload,
                    previous_event_hash=previous,
                    event_hash=digest,
                )
            )
            previous = digest
            if event_type == "account_projected":
                projection_hash = digest
        account.cash = nav
        account.nav = nav
        account.benchmark_nav = nav
        account.as_of = occurred_at
        account.last_event_sequence = sequence
        account.last_event_hash = previous
        account.updated_at = occurred_at
    return authority.record_projection(
        role_binding_id=binding_id,
        account_event_hash=projection_hash,
        trade_date=trade_date,
        recorded_at=occurred_at + timedelta(minutes=1),
    )


def _project_days(
    engine,
    authority,
    champion,
    challenger,
    *,
    count: int,
    challenger_daily_return: float = 0.002,
    champion_daily_return: float = 0.001,
):
    champion_nav = 100.0
    challenger_nav = 100.0
    for offset in range(count):
        session_date = FIRST_SESSION + timedelta(days=offset)
        champion_nav *= 1.0 + champion_daily_return
        challenger_nav *= 1.0 + challenger_daily_return
        _append_projection(
            engine,
            authority,
            account_id=champion.account_id,
            binding_id=champion.binding_id,
            trade_date=session_date,
            nav=champion_nav,
        )
        _append_projection(
            engine,
            authority,
            account_id=challenger.account_id,
            binding_id=challenger.binding_id,
            trade_date=session_date,
            nav=challenger_nav,
        )


def test_59_rejects_and_60_enters_probation_never_active():
    engine, authority, champion, challenger = _seed_authority()
    _project_days(engine, authority, champion, challenger, count=59)
    with pytest.raises(InsufficientForwardEvidence, match="found 59"):
        authority.aligned_forward_window(
            champion_account_id=champion.account_id,
            challenger_account_id=challenger.account_id,
        )

    champion_nav = 100.0 * (1.001**60)
    challenger_nav = 100.0 * (1.002**60)
    final_date = FIRST_SESSION + timedelta(days=59)
    _append_projection(
        engine,
        authority,
        account_id=champion.account_id,
        binding_id=champion.binding_id,
        trade_date=final_date,
        nav=champion_nav,
    )
    _append_projection(
        engine,
        authority,
        account_id=challenger.account_id,
        binding_id=challenger.binding_id,
        trade_date=final_date,
        nav=challenger_nav,
    )
    window = authority.aligned_forward_window(
        champion_account_id=champion.account_id,
        challenger_account_id=challenger.account_id,
    )
    evidence = ShadowActivationEvidence(
        shadow_account_id=challenger.account_id,
        observed_sessions=window.observed_sessions,
        chain_verified=True,
        data_quality_ok=window.data_quality_ok,
        event_chain_evidence_hash=window.evidence_hash,
        champion_account_id=champion.account_id,
        epoch_id=window.epoch_id,
        evidence_window_hash=window.evidence_window_hash,
        common_session_hash=window.evidence_hash,
        challenger_outperformed_static=window.challenger_outperformed_static,
        forward_authority_verified=True,
    )
    decision = authorize_shadow_activation(
        SleeveLifecycleRecord(
            sleeve_id="value_quality_v1",
            state=SleeveState.SHADOW,
            target_weight=0.25,
        ),
        evidence,
        as_of_date=final_date,
        require_formal_authority=True,
    )
    assert window.observed_sessions == 60
    assert decision.record.state is SleeveState.PROBATION
    assert decision.record.effective_weight == pytest.approx(0.05)


def test_new_epoch_pointer_rebinds_roles_and_cannot_reuse_old_60_sessions():
    engine, authority, champion, challenger = _seed_authority()
    _project_days(engine, authority, champion, challenger, count=60)

    backend = _SQLAlchemyCatalog.__new__(_SQLAlchemyCatalog)
    backend._engine = engine
    backend._sessions = sessionmaker(
        engine, expire_on_commit=False, autoflush=False
    )
    frozen_at = datetime(2026, 10, 23, 8, tzinfo=UTC)
    successor = new_evidence_epoch(
        architecture_version="test-v2",
        frozen_at=frozen_at,
        code_hash="9" * 64,
        configuration_hash="2" * 64,
        dependency_lock_hash="3" * 64,
        dirty_patch_hash="4" * 64,
    )
    backend.freeze_evidence_epoch(successor)
    activated = backend.activate_evidence_epoch(
        epoch_id=successor.epoch_id,
        expected_epoch_hash=successor.epoch_hash,
        first_forward_session=date(2026, 10, 26),
        calendar_snapshot_id=DECISION_SNAPSHOT,
        calendar_snapshot_hash="a" * 64,
        calendar_content_hash="6" * 64,
        activated_at=frozen_at + timedelta(minutes=1),
    )

    assert backend.get_evidence_epoch() == activated
    with Session(engine) as session:
        retired_epoch = session.scalar(
            select(EvidenceEpochModel).where(
                EvidenceEpochModel.epoch_id == "epoch_formal_001"
            )
        )
        active_bindings = list(
            session.scalars(
                select(ShadowRoleBindingModel).where(
                    ShadowRoleBindingModel.active.is_(True)
                )
            )
        )
    assert retired_epoch is not None
    assert retired_epoch.closed_at is not None
    assert retired_epoch.superseded_by_epoch_id == activated.epoch_id
    assert {item.epoch_id for item in active_bindings} == {activated.epoch_id}
    assert {item.account_id for item in active_bindings} == {
        champion.account_id,
        challenger.account_id,
    }
    with pytest.raises(InsufficientForwardEvidence, match="found 0"):
        authority.aligned_forward_window(
            champion_account_id=champion.account_id,
            challenger_account_id=challenger.account_id,
        )


def test_misaligned_dates_do_not_form_a_common_authority_window():
    engine, authority, champion, challenger = _seed_authority()
    _project_days(engine, authority, champion, challenger, count=60)
    with Session(engine) as session, session.begin():
        session.execute(
            delete(ShadowSessionModel).where(
                ShadowSessionModel.account_id == challenger.account_id,
                ShadowSessionModel.trade_date
                == (FIRST_SESSION + timedelta(days=12)).isoformat(),
            )
        )
    with pytest.raises(MisalignedForwardEvidence, match="not exactly aligned"):
        authority.aligned_forward_window(
            champion_account_id=champion.account_id,
            challenger_account_id=challenger.account_id,
        )


def test_account_equation_and_snapshot_role_checks_fail_closed():
    engine, authority, champion, _ = _seed_authority()
    with pytest.raises(ShadowSessionRejected, match="does not equal NAV"):
        _append_projection(
            engine,
            authority,
            account_id=champion.account_id,
            binding_id=champion.binding_id,
            trade_date=FIRST_SESSION,
            nav=101.0,
            invalid_positions_value=1.0,
        )
    engine, authority, champion, _ = _seed_authority()
    with pytest.raises(ShadowSessionRejected, match="must be distinct"):
        _append_projection(
            engine,
            authority,
            account_id=champion.account_id,
            binding_id=champion.binding_id,
            trade_date=FIRST_SESSION,
            nav=101.0,
            decision_snapshot_id=EXECUTION_SNAPSHOT,
            rebalanced=True,
        )


def test_cross_epoch_binding_and_tampered_event_chain_are_rejected():
    engine, authority, champion, _ = _seed_authority()
    with Session(engine) as session, session.begin():
        binding = session.get(ShadowRoleBindingModel, champion.binding_id)
        assert binding is not None
        binding.epoch_id = None
    with pytest.raises(ShadowRoleConflict, match="fingerprint is corrupt"):
        _append_projection(
            engine,
            authority,
            account_id=champion.account_id,
            binding_id=champion.binding_id,
            trade_date=FIRST_SESSION,
            nav=101.0,
        )

    engine, authority, champion, challenger = _seed_authority()
    _project_days(engine, authority, champion, challenger, count=1)
    with Session(engine) as session, session.begin():
        event = session.scalar(
            select(ShadowEventModel)
            .where(ShadowEventModel.account_id == champion.account_id)
            .order_by(ShadowEventModel.sequence_number)
            .limit(1)
        )
        assert event is not None
        payload = dict(event.payload_json)
        payload["tampered_after_commit"] = True
        event.payload_json = payload
    with pytest.raises(ShadowSessionRejected, match="event chain is corrupt"):
        authority.aligned_forward_window(
            champion_account_id=champion.account_id,
            challenger_account_id=challenger.account_id,
        )


def test_underperforming_dynamic_challenger_falls_back_to_static_champion():
    engine, authority, champion, challenger = _seed_authority()
    _project_days(
        engine,
        authority,
        champion,
        challenger,
        count=60,
        challenger_daily_return=0.0001,
        champion_daily_return=0.001,
    )
    window = authority.aligned_forward_window(
        champion_account_id=champion.account_id,
        challenger_account_id=challenger.account_id,
    )
    evidence = ShadowActivationEvidence(
        shadow_account_id=challenger.account_id,
        observed_sessions=window.observed_sessions,
        chain_verified=True,
        data_quality_ok=True,
        event_chain_evidence_hash=window.evidence_hash,
        champion_account_id=champion.account_id,
        epoch_id=window.epoch_id,
        evidence_window_hash=window.evidence_window_hash,
        common_session_hash=window.evidence_hash,
        challenger_outperformed_static=window.challenger_outperformed_static,
        forward_authority_verified=True,
    )
    decision = authorize_shadow_activation(
        SleeveLifecycleRecord(
            sleeve_id="value_quality_v1",
            state=SleeveState.SHADOW,
            target_weight=0.25,
        ),
        evidence,
        as_of_date=window.sessions[-1],
        require_formal_authority=True,
    )
    assert window.fallback == "static_champion"
    assert decision.record.state is SleeveState.SHADOW
    assert decision.recommended_action == "fallback_static_champion"


def test_authoritative_dynamic_approval_also_falls_back_when_shadow_loses():
    engine, authority, champion, challenger = _seed_authority()
    _project_days(
        engine,
        authority,
        champion,
        challenger,
        count=60,
        challenger_daily_return=0.0001,
        champion_daily_return=0.001,
    )
    epoch = SimpleNamespace(
        epoch_id="epoch_formal_001",
        epoch_hash="5" * 64,
        evidence_window_hash="7" * 64,
        first_forward_session=FIRST_SESSION,
        calendar_snapshot_id=DECISION_SNAPSHOT,
        calendar_snapshot_hash="a" * 64,
        activated_at=datetime(2026, 8, 21, 8, 1, tzinfo=UTC),
    )
    catalog = SimpleNamespace(get_evidence_epoch=lambda: epoch)
    control = AuthoritativeChampionControl(
        catalog, shadow_authority=authority, legacy_shadow_evidence=False
    )
    historical_dates = pd.DatetimeIndex(
        [
            pd.Timestamp(year=year, month=1, day=2) + pd.Timedelta(days=7 * offset)
            for year in range(2021, 2026)
            for offset in range(12)
        ]
    )

    def history(experiment_id: str, **_kwargs):
        is_challenger = experiment_id == "challenger_exp_001"
        returns = pd.Series(
            0.012 if is_challenger else 0.001,
            index=historical_dates,
            dtype=float,
        )
        return returns, {
            "experiment_id": experiment_id,
            "experiment_fingerprint": ("a" if is_challenger else "b") * 64,
            "result_id": f"result_{experiment_id}",
            "result_hash": ("c" if is_challenger else "d") * 64,
            "result_completed_at": datetime(2026, 8, 21, 7, tzinfo=UTC).isoformat(),
            "observation_count": len(returns),
            "first_session": historical_dates[0].date().isoformat(),
            "last_session": historical_dates[-1].date().isoformat(),
        }

    control._historical_result_returns = history  # type: ignore[method-assign]
    decision, gate = control.evaluate_authoritative_challenger(
        historical_challenger_experiment_id="challenger_exp_001",
        shadow_challenger_account_id=challenger.account_id,
        shadow_champion_account_id=champion.account_id,
    )
    assert decision.checks["historical_data"] is True
    assert decision.checks["shadow_excess_positive"] is False
    assert decision.fallback == "static_champion"
    assert gate["formal_shadow_window"]["fallback"] == "static_champion"


def test_formal_through_cannot_hide_unclosed_database_clock_latest_session():
    engine, authority, champion, challenger = _seed_authority()
    _project_days(engine, authority, champion, challenger, count=60)
    for offset in range(60):
        authority.close_fleet_day(FIRST_SESSION + timedelta(days=offset))
    latest_projected = FIRST_SESSION + timedelta(days=59)
    latest_complete = FIRST_SESSION + timedelta(days=60)
    calendar = tuple(
        FIRST_SESSION + timedelta(days=offset) for offset in range(61)
    )
    calendar_hash = hashlib.sha256(
        "\n".join(value.isoformat() for value in calendar).encode("ascii")
    ).hexdigest()
    with Session(engine) as session, session.begin():
        snapshot = session.get(DataSnapshotModel, DECISION_SNAPSHOT)
        epoch = session.get(EvidenceEpochModel, "research_os")
        assert snapshot is not None and epoch is not None
        snapshot.ref_json = {
            "snapshot_id": DECISION_SNAPSHOT,
            "content_hash": "a" * 64,
            "manifest": {
                "trading_calendar": {
                    "quality_status": "accepted",
                    "sessions": [value.isoformat() for value in calendar],
                    "content_hash": calendar_hash,
                }
            },
        }
        epoch.calendar_content_hash = calendar_hash
    realtime = ShadowEvidenceAuthority(
        engine,
        enforce_realtime=True,
        require_fleet_closure=True,
        database_now_for_test=lambda: datetime.combine(
            latest_complete, time(8), tzinfo=UTC
        ),
    )
    try:
        with pytest.raises(
            IncompleteFleetEvidence,
            match=f"{latest_complete.isoformat()} has no fleet_closed",
        ):
            realtime.aligned_forward_window(
                champion_account_id=champion.account_id,
                challenger_account_id=challenger.account_id,
                through=latest_projected,
            )
    finally:
        realtime.close()


def test_formal_window_cannot_infer_a_new_segment_after_an_account_gap():
    engine, authority, champion, challenger = _seed_authority()
    skipped = FIRST_SESSION + timedelta(days=30)
    champion_nav = 100.0
    challenger_nav = 100.0
    for offset in range(61):
        session_date = FIRST_SESSION + timedelta(days=offset)
        if session_date == skipped:
            continue
        champion_nav *= 1.001
        challenger_nav *= 1.002
        _append_projection(
            engine,
            authority,
            account_id=champion.account_id,
            binding_id=champion.binding_id,
            trade_date=session_date,
            nav=champion_nav,
        )
        _append_projection(
            engine,
            authority,
            account_id=challenger.account_id,
            binding_id=challenger.binding_id,
            trade_date=session_date,
            nav=challenger_nav,
        )

    latest_complete = FIRST_SESSION + timedelta(days=60)
    calendar = tuple(
        FIRST_SESSION + timedelta(days=offset) for offset in range(61)
    )
    calendar_hash = hashlib.sha256(
        "\n".join(value.isoformat() for value in calendar).encode("ascii")
    ).hexdigest()
    with Session(engine) as session, session.begin():
        snapshot = session.get(DataSnapshotModel, DECISION_SNAPSHOT)
        epoch = session.get(EvidenceEpochModel, "research_os")
        assert snapshot is not None and epoch is not None
        snapshot.ref_json = {
            "snapshot_id": DECISION_SNAPSHOT,
            "content_hash": "a" * 64,
            "manifest": {
                "trading_calendar": {
                    "quality_status": "accepted",
                    "sessions": [value.isoformat() for value in calendar],
                    "content_hash": calendar_hash,
                }
            },
        }
        epoch.calendar_content_hash = calendar_hash
    authority.close_fleet_day(latest_complete)
    realtime = ShadowEvidenceAuthority(
        engine,
        enforce_realtime=True,
        require_fleet_closure=True,
        database_now_for_test=lambda: datetime.combine(
            latest_complete, time(8), tzinfo=UTC
        ),
    )
    try:
        with pytest.raises(
            MisalignedForwardEvidence,
            match="replay the gap before later projections or start a new evidence segment",
        ):
            realtime.aligned_forward_window(
                champion_account_id=champion.account_id,
                challenger_account_id=challenger.account_id,
            )
    finally:
        realtime.close()


def test_gap_incidents_are_matched_by_exact_date_blocking_stage_and_status():
    engine, authority, _, _ = _seed_authority()
    missing = FIRST_SESSION + timedelta(days=1)
    other = FIRST_SESSION + timedelta(days=2)
    occurred = datetime.combine(missing, time(8), tzinfo=UTC)
    with Session(engine) as session, session.begin():
        session.add(
            DataIncidentModel(
                incident_id="incident_other_date",
                incident_hash="c" * 64,
                partition_run_id=None,
                partition_key=other.isoformat(),
                stage="gold",
                status="open",
                error_code="gold_failed",
                message="other date",
                source_ids_json=[],
                evidence_hashes_json=[],
                payload_json={},
                occurred_at=occurred,
                resolved_at=None,
                resolution_hash=None,
            )
        )
    with Session(engine) as session:
        assert authority._gap_incident_dispositions(session, (missing,))[missing] == (
            "unexplained"
        )

    with Session(engine) as session, session.begin():
        session.add(
            DataIncidentModel(
                incident_id="incident_exact_open",
                incident_hash="d" * 64,
                partition_run_id=None,
                partition_key=missing.isoformat(),
                stage="data_quality",
                status="open",
                error_code="dq_failed",
                message="exact blocker",
                source_ids_json=[],
                evidence_hashes_json=[],
                payload_json={},
                occurred_at=occurred,
                resolved_at=None,
                resolution_hash=None,
            )
        )
    with Session(engine) as session:
        assert authority._gap_incident_dispositions(session, (missing,))[missing] == (
            "open_blocker"
        )

    with Session(engine) as session, session.begin():
        row = session.get(DataIncidentModel, "incident_exact_open")
        assert row is not None
        row.status = "resolved"
        row.resolved_at = occurred + timedelta(hours=1)
        row.resolution_hash = "e" * 64
        row.payload_json = {
            "resolution": {
                "snapshot_id": DECISION_SNAPSHOT,
                "snapshot_content_hash": "a" * 64,
            }
        }
    with Session(engine) as session:
        assert authority._gap_incident_dispositions(session, (missing,))[missing] == (
            "resolved_requires_replay"
        )
