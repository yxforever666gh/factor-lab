from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib

import pandas as pd
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from factor_lab.research_os.catalog import ResearchCatalog
from factor_lab.research_os.contracts import DataSnapshotRef
from factor_lab.research_os.orm import (
    EvidenceEpochPointerModel,
    EvidenceEpochModel,
    ExperimentModel,
    ExperimentResultModel,
    ShadowSessionModel,
)
from factor_lab.research_os.shadow_authority import (
    IncompleteFleetEvidence,
    ShadowEvidenceAuthority,
    ShadowRole,
)
from factor_lab.research_os.shadow_catalog import ShadowStepAlreadyApplied
from factor_lab.research_os.sleeve_lifecycle import (
    DailyShadowPlan,
    ShadowFleetCoordinator,
    SleeveLifecycleBridgeError,
)


UTC = timezone.utc
DECISION_DATE = date(2026, 1, 5)
TRADE_DATE = date(2026, 1, 6)
CALENDAR = (DECISION_DATE.isoformat(), TRADE_DATE.isoformat())
DECISION_SNAPSHOT = "fleet-decision-20260105"
EXECUTION_SNAPSHOT = "fleet-execution-20260106"
MARK_SNAPSHOT = "fleet-mark-20260106"


def _snapshot(snapshot_id: str, *, fill: str, as_of: datetime) -> DataSnapshotRef:
    calendar_hash = hashlib.sha256("\n".join(CALENDAR).encode("ascii")).hexdigest()
    return DataSnapshotRef(
        snapshot_id=snapshot_id,
        tier="gold",
        uri=f"s3://factor-lab-test/gold/{snapshot_id}",
        content_hash=fill * 64,
        as_of=as_of,
        manifest={
            "trading_calendar": {
                "source": "controlled-exchange-calendar",
                "quality_status": "accepted",
                "sessions": list(CALENDAR),
                "content_hash": calendar_hash,
            }
        },
    )


def _bars() -> pd.DataFrame:
    session = TRADE_DATE.isoformat()
    return pd.DataFrame(
        [
            {
                "ticker": "000001.SZ",
                "trade_date": session,
                "execution_event_time": f"{session}T09:30:00+08:00",
                "execution_available_at": f"{session}T09:30:00+08:00",
                "mark_event_time": f"{session}T15:00:00+08:00",
                "mark_available_at": f"{session}T15:01:00+08:00",
                "execution_snapshot_id": EXECUTION_SNAPSHOT,
                "mark_snapshot_id": MARK_SNAPSHOT,
                "open_adj": 10.0,
                "close_adj": 10.1,
                "adv_20": 100_000_000.0,
                "volatility_20": 0.02,
                "is_one_price_limit_up": False,
                "is_one_price_limit_down": False,
                "is_suspended": False,
                "is_delisted": False,
                "split_ratio": 1.0,
                "cash_dividend": 0.0,
            }
        ]
    )


def _seed(tmp_path, *, challenger: bool):
    database = tmp_path / ("fleet-two.db" if challenger else "fleet-one.db")
    database_url = f"sqlite+pysqlite:///{database.as_posix()}"
    catalog = ResearchCatalog(database_url)
    catalog.initialize_schema()
    for snapshot in (
        _snapshot(
            DECISION_SNAPSHOT,
            fill="a",
            as_of=datetime(2026, 1, 5, 7, tzinfo=UTC),
        ),
        _snapshot(
            EXECUTION_SNAPSHOT,
            fill="b",
            as_of=datetime(2026, 1, 6, 1, 30, tzinfo=UTC),
        ),
        _snapshot(
            MARK_SNAPSHOT,
            fill="c",
            as_of=datetime(2026, 1, 6, 7, 1, tzinfo=UTC),
        ),
    ):
        catalog.register_snapshot(snapshot)
    opened_at = datetime(2025, 12, 31, 8, tzinfo=UTC)
    catalog.create_shadow_account(
        account_id="fleet_champion",
        name="Fleet Champion",
        initial_capital=1_000_000.0,
        opened_at=opened_at,
    )
    if challenger:
        catalog.create_shadow_account(
            account_id="fleet_challenger",
            name="Fleet Challenger",
            initial_capital=1_000_000.0,
            opened_at=opened_at,
        )
    engine = create_engine(database_url)
    frozen_at = datetime(2026, 1, 5, 7, 30, tzinfo=UTC)
    with Session(engine) as session, session.begin():
        session.add(
            EvidenceEpochModel(
                epoch_slot="research_os",
                epoch_id="fleet_epoch_001",
                schema_version="research-os/evidence-epoch/v1",
                architecture_version="fleet-test-v1",
                frozen_at=frozen_at,
                code_hash="1" * 64,
                configuration_hash="2" * 64,
                dependency_lock_hash="3" * 64,
                dirty_patch_hash="4" * 64,
                epoch_hash="5" * 64,
                first_forward_session=TRADE_DATE.isoformat(),
                calendar_snapshot_id=DECISION_SNAPSHOT,
                calendar_snapshot_hash="a" * 64,
                calendar_content_hash=hashlib.sha256(
                    "\n".join(CALENDAR).encode("ascii")
                ).hexdigest(),
                evidence_window_hash="7" * 64,
                activated_at=frozen_at + timedelta(minutes=1),
            )
        )
        session.add(
            EvidenceEpochPointerModel(
                pointer_key="research_os",
                epoch_id="fleet_epoch_001",
                updated_at=frozen_at + timedelta(minutes=1),
            )
        )
        if challenger:
            session.add(
                ExperimentModel(
                    experiment_id="fleet_challenger_exp",
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
                ExperimentResultModel(
                    result_id="fleet_challenger_result",
                    experiment_id="fleet_challenger_exp",
                    result_hash="9" * 64,
                    outcome="promoted_to_shadow",
                    metrics_json={"promotion_verdict": "promote"},
                    artifact_uri=None,
                    authoritative=True,
                    completed_at=frozen_at,
                )
            )
    authority = ShadowEvidenceAuthority(
        engine,
        enforce_realtime=False,
        require_fleet_closure=True,
    )
    bound_at = frozen_at + timedelta(minutes=2)
    authority.bind_role(
        role=ShadowRole.CHAMPION,
        role_key="static_champion",
        account_id="fleet_champion",
        bound_at=bound_at,
    )
    plans = [
        DailyShadowPlan(
            account_id="fleet_champion",
            role="champion",
            role_key="static_champion",
        )
    ]
    if challenger:
        authority.bind_role(
            role=ShadowRole.CHALLENGER,
            role_key="fleet_challenger_exp",
            account_id="fleet_challenger",
            sleeve_id="value_quality_v1",
            experiment_id="fleet_challenger_exp",
            bound_at=bound_at,
        )
        plans.append(
            DailyShadowPlan(
                account_id="fleet_challenger",
                role="challenger",
                role_key="fleet_challenger_exp",
            )
        )
    return catalog, engine, authority, tuple(plans)


def _project(
    coordinator: ShadowFleetCoordinator,
    plans: tuple[DailyShadowPlan, ...],
    *,
    benchmark_return: float = 0.001,
):
    return coordinator.project_daily(
        plans=plans,
        trade_date=TRADE_DATE,
        market_bars=_bars(),
        execution_snapshot_id=EXECUTION_SNAPSHOT,
        mark_snapshot_id=MARK_SNAPSHOT,
        benchmark_return=benchmark_return,
    )


def _daily_step_event_count(catalog: ResearchCatalog, account_id: str) -> int:
    return sum(
        1
        for event in catalog.list_shadow_events(account_id=account_id, limit=1_000)
        if str(
            event.payload.get("research_os_shadow_step", {}).get("step_id") or ""
        ).startswith("sdp_")
    )


def test_tuesday_without_challenger_still_holds_marks_and_closes_fleet(tmp_path) -> None:
    catalog, engine, authority, plans = _seed(tmp_path, challenger=False)
    try:
        results = _project(
            ShadowFleetCoordinator(catalog, shadow_authority=authority), plans
        )
        assert len(results) == 1
        assert results[0].account_id == "fleet_champion"
        assert results[0].rebalanced is False
        assert "target_received" not in results[0].domain_event_types
        assert "mark_to_market" in results[0].domain_event_types
        closure = authority.fleet_closure(TRADE_DATE)
        assert closure is not None
        assert closure.member_count == 1
        assert closure.members[0]["role"] == "champion"
        assert closure.members[0]["session_hash"]
    finally:
        authority.close()
        catalog.close()
        engine.dispose()


def test_fleet_retry_recovers_exact_committed_step_and_finishes_remaining_accounts(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog, engine, authority, plans = _seed(tmp_path, challenger=True)
    coordinator = ShadowFleetCoordinator(catalog, shadow_authority=authority)
    original_record = authority.record_projection
    calls = 0

    def crash_before_first_authority(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("injected crash after account commit")
        return original_record(**kwargs)

    try:
        monkeypatch.setattr(authority, "record_projection", crash_before_first_authority)
        with pytest.raises(RuntimeError, match="injected crash"):
            _project(coordinator, plans)
        assert authority.fleet_closure(TRADE_DATE) is None
        champion_events = _daily_step_event_count(catalog, "fleet_champion")
        assert champion_events > 0
        assert _daily_step_event_count(catalog, "fleet_challenger") == 0

        monkeypatch.setattr(authority, "record_projection", original_record)
        with pytest.raises(ShadowStepAlreadyApplied):
            _project(coordinator, plans, benchmark_return=0.002)
        assert _daily_step_event_count(catalog, "fleet_champion") == champion_events
        assert authority.fleet_closure(TRADE_DATE) is None

        recovered = _project(coordinator, plans)
        assert {result.account_id for result in recovered} == {
            "fleet_champion",
            "fleet_challenger",
        }
        assert _daily_step_event_count(catalog, "fleet_champion") == champion_events
        closure = authority.fleet_closure(TRADE_DATE)
        assert closure is not None
        assert closure.member_count == 2
        with Session(engine) as session:
            assert len(
                list(
                    session.scalars(
                        select(ShadowSessionModel).where(
                            ShadowSessionModel.trade_date
                            == TRADE_DATE.isoformat()
                        )
                    )
                )
            ) == 2

        before = {
            account_id: _daily_step_event_count(catalog, account_id)
            for account_id in ("fleet_champion", "fleet_challenger")
        }
        again = _project(coordinator, plans)
        assert len(again) == 2
        assert {
            account_id: _daily_step_event_count(catalog, account_id)
            for account_id in before
        } == before
        assert authority.fleet_closure(TRADE_DATE).closure_hash == closure.closure_hash
    finally:
        authority.close()
        catalog.close()
        engine.dispose()


def test_incomplete_active_fleet_is_rejected_before_any_projection(tmp_path) -> None:
    catalog, engine, authority, plans = _seed(tmp_path, challenger=True)
    try:
        with pytest.raises(SleeveLifecycleBridgeError, match="exactly cover"):
            _project(
                ShadowFleetCoordinator(catalog, shadow_authority=authority),
                plans[:1],
            )
        assert _daily_step_event_count(catalog, "fleet_champion") == 0
        assert _daily_step_event_count(catalog, "fleet_challenger") == 0
        assert authority.fleet_closure(TRADE_DATE) is None
    finally:
        authority.close()
        catalog.close()
        engine.dispose()


def test_unclosed_day_cannot_authorize_aligned_forward_evidence(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog, engine, authority, plans = _seed(tmp_path, challenger=True)
    coordinator = ShadowFleetCoordinator(catalog, shadow_authority=authority)
    try:
        monkeypatch.setattr(
            authority,
            "close_fleet_day",
            lambda _trade_date: (_ for _ in ()).throw(
                RuntimeError("injected crash before fleet closure")
            ),
        )
        with pytest.raises(RuntimeError, match="before fleet closure"):
            _project(coordinator, plans)
        assert authority.fleet_closure(TRADE_DATE) is None
        with pytest.raises(IncompleteFleetEvidence, match="fleet_closed"):
            authority.aligned_forward_window(
                champion_account_id="fleet_champion",
                challenger_account_id="fleet_challenger",
            )
    finally:
        authority.close()
        catalog.close()
        engine.dispose()
