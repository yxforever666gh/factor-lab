from __future__ import annotations

from dataclasses import asdict, replace
from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from factor_lab.research_os.catalog import CatalogConflict, ResearchCatalog
from factor_lab.research_os.contracts import LifecycleState
from factor_lab.research_os.engineering_canary import (
    AcceptedCanonicalCanaryInput,
    CanaryInputRejected,
    CanonicalCanarySession,
    CanonicalMarketBar,
    EVIDENCE_CLASS,
    EVIDENCE_SCOPE,
    EngineeringCanaryService,
    FormalEpochAdmissionDenied,
    OpeningExecutionCapability,
    assess_opening_execution_capability,
    require_formal_epoch_admission,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
OBSERVED_OPEN_FIELDS = (
    "ticker",
    "open_adj",
    "execution_event_time",
    "execution_available_at",
    "is_suspended",
    "is_one_price_limit_up",
    "is_one_price_limit_down",
)


def _timestamp(session: date, at: time) -> datetime:
    return datetime.combine(session, at, tzinfo=SHANGHAI).astimezone(timezone.utc)


def _bundle() -> AcceptedCanonicalCanaryInput:
    calendar = tuple(item.date() for item in pd.bdate_range("2026-06-01", periods=21))
    tickers = tuple(f"{index:06d}.SZ" for index in range(1, 51))
    sessions = []
    for day_index, session_date in enumerate(calendar[1:], start=1):
        bars = []
        for ticker_index, ticker in enumerate(tickers, start=1):
            opening = 8.0 + ticker_index * 0.05 + day_index * 0.01
            session_move = 0.0004 + (ticker_index % 7 - 3) * 0.0001
            bars.append(
                CanonicalMarketBar(
                    ticker=ticker,
                    trade_date=session_date,
                    open_adj=opening,
                    close_adj=opening * (1.0 + session_move),
                    adv_20=200_000_000.0 + ticker_index * 10_000.0,
                    volatility_20=0.02 + ticker_index * 0.00001,
                    execution_event_time=_timestamp(session_date, time(9, 30)),
                    execution_available_at=_timestamp(session_date, time(9, 30)),
                    mark_event_time=_timestamp(session_date, time(15, 0)),
                    mark_available_at=_timestamp(session_date, time(15, 0)),
                )
            )
        sessions.append(CanonicalCanarySession(session_date, tuple(bars)))
    return AcceptedCanonicalCanaryInput(
        calendar_sessions=calendar,
        sessions=tuple(sessions),
        opening_execution_capability=OpeningExecutionCapability(
            source_id="canonical-canary-fixture",
            status="accepted",
            observed_fields=OBSERVED_OPEN_FIELDS,
            event_semantics="canonical_open_09_30",
            point_in_time=True,
            real_source_probe=False,
        ),
    )


@pytest.fixture(scope="module")
def completed_canary(tmp_path_factory):
    database = tmp_path_factory.mktemp("engineering-canary") / "catalog.sqlite"
    catalog = ResearchCatalog(database)
    catalog.initialize_schema()
    bundle = _bundle()
    result = EngineeringCanaryService(catalog).run(bundle)
    try:
        yield catalog, bundle, result
    finally:
        catalog.close()


def test_canary_closes_bronze_silver_gold_shadow_and_20_daily_projections(
    completed_canary,
) -> None:
    catalog, bundle, result = completed_canary

    assert result.evidence_class == EVIDENCE_CLASS
    assert result.evidence_scope == EVIDENCE_SCOPE
    assert result.formal_epoch_eligible is False
    assert result.security_count == 50
    assert result.projected_session_count == 20
    assert result.account_projection_count == 20
    assert result.chain_verified is True
    assert result.sleeve_state == "shadow"
    assert catalog.latest_lifecycle_state(result.sleeve_id) is LifecycleState.SHADOW
    assert catalog.verify_shadow_chain(result.account_id) is True
    assert catalog.count_shadow_sessions(
        account_id=result.account_id,
        since=bundle.calendar_sessions[0],
        through=bundle.calendar_sessions[-1],
    ) == 20

    account_projection_events = catalog.list_shadow_events_by_type(
        account_id=result.account_id,
        event_type="account_projected",
        limit=100,
    )
    mark_events = catalog.list_shadow_events_by_type(
        account_id=result.account_id,
        event_type="mark_to_market",
        limit=100,
    )
    assert len(account_projection_events) == 20
    assert len(mark_events) == 20
    assert len(result.projections) == 20
    assert [item.rebalanced for item in result.projections] == [
        index % 5 == 0 for index in range(20)
    ]
    assert all(item.position_count == 50 for item in result.projections)
    assert all(item.nav > 0 and item.benchmark_nav > 0 for item in result.projections)


def test_all_canary_outputs_and_persisted_evidence_are_non_forward_labeled(
    completed_canary,
) -> None:
    catalog, _, result = completed_canary

    assert result.capability.evidence_class == EVIDENCE_CLASS
    assert result.capability.evidence_scope == EVIDENCE_SCOPE
    assert result.capability.formal_epoch_eligible is False
    assert result.evidence_markers
    for item in (*result.evidence_markers, *result.projections):
        assert item.evidence_class == EVIDENCE_CLASS
        assert item.evidence_scope == EVIDENCE_SCOPE
        assert item.formal_epoch_eligible is False

    markers = {item.snapshot_id: item for item in result.evidence_markers}
    assert {item.tier for item in markers.values()} == {"bronze", "silver", "gold"}
    assert len(markers) == 3 + 20 * 6
    for marker in markers.values():
        snapshot = catalog.get_snapshot(marker.snapshot_id)
        assert snapshot is not None
        assert snapshot.reference.trust_labels == (EVIDENCE_CLASS, EVIDENCE_SCOPE)
        assert snapshot.reference.manifest["evidence_class"] == EVIDENCE_CLASS
        assert snapshot.reference.manifest["evidence_scope"] == EVIDENCE_SCOPE
        assert snapshot.reference.manifest["formal_epoch_eligible"] is False
        if marker.tier == "bronze":
            assert marker.parent_snapshot_ids == ()
        else:
            assert len(marker.parent_snapshot_ids) == 1
            parent = markers[marker.parent_snapshot_ids[0]]
            assert (parent.tier, marker.tier) in {
                ("bronze", "silver"),
                ("silver", "gold"),
            }

    run = catalog.get_run(result.run_id)
    assert run is not None
    assert run.run_type == EVIDENCE_CLASS
    assert run.metadata["evidence_class"] == EVIDENCE_CLASS
    assert run.metadata["evidence_scope"] == EVIDENCE_SCOPE
    assert run.metadata["formal_epoch_eligible"] is False
    lifecycle = catalog.list_lifecycle_events(sleeve_id=result.sleeve_id)
    assert lifecycle
    assert all(item.evidence["evidence_class"] == EVIDENCE_CLASS for item in lifecycle)
    assert all(item.evidence["evidence_scope"] == EVIDENCE_SCOPE for item in lifecycle)
    assert all(item.evidence["formal_epoch_eligible"] is False for item in lifecycle)


def test_canary_is_deterministic_and_resumes_without_duplicate_daily_events(
    completed_canary,
    monkeypatch,
) -> None:
    catalog, bundle, first = completed_canary
    before = catalog.get_run(first.run_id)
    assert before is not None and before.status == "succeeded"

    def reject_write(*args, **kwargs):
        raise AssertionError("succeeded canary replay must be read-only")

    for method in (
        "claim_run",
        "save_run",
        "register_snapshot",
        "create_shadow_account",
        "append_lifecycle_event",
        "append_shadow_events_atomic",
    ):
        monkeypatch.setattr(catalog, method, reject_write)
    second = EngineeringCanaryService(catalog).run(bundle)

    assert second == first
    after = catalog.get_run(first.run_id)
    assert after is not None
    assert after.status == "succeeded"
    assert after.started_at == before.started_at
    assert after.completed_at == before.completed_at
    assert len(
        catalog.list_shadow_events_by_type(
            account_id=first.account_id,
            event_type="account_projected",
            limit=100,
        )
    ) == 20
    assert len(catalog.list_lifecycle_events(sleeve_id=first.sleeve_id)) == 5


def test_canary_claims_parent_before_children_and_resumes_after_fault(
    tmp_path,
    monkeypatch,
) -> None:
    catalog = ResearchCatalog(tmp_path / "canary-fault.sqlite")
    catalog.initialize_schema()
    bundle = _bundle()
    run_id = f"engcan_{bundle.canonical_hash[:32]}"
    active_parent_id = {"value": run_id}
    inject_fault = {"enabled": True}
    observed: list[tuple[str, str]] = []
    original_methods = {
        "register_snapshot": catalog.register_snapshot,
        "create_shadow_account": catalog.create_shadow_account,
        "append_lifecycle_event": catalog.append_lifecycle_event,
        "append_shadow_events_atomic": catalog.append_shadow_events_atomic,
    }

    def guarded(name, *, fail=False):
        delegate = original_methods[name]

        def wrapped(*args, **kwargs):
            parent = catalog.get_run(active_parent_id["value"])
            assert parent is not None
            observed.append((name, parent.status))
            assert parent.status == "running"
            if fail and inject_fault["enabled"]:
                raise RuntimeError("injected shadow child fault")
            return delegate(*args, **kwargs)

        return wrapped

    try:
        monkeypatch.setattr(catalog, "register_snapshot", guarded("register_snapshot"))
        monkeypatch.setattr(
            catalog,
            "create_shadow_account",
            guarded("create_shadow_account"),
        )
        monkeypatch.setattr(
            catalog,
            "append_lifecycle_event",
            guarded("append_lifecycle_event"),
        )
        monkeypatch.setattr(
            catalog,
            "append_shadow_events_atomic",
            guarded("append_shadow_events_atomic", fail=True),
        )

        with pytest.raises(RuntimeError, match="injected shadow child fault"):
            EngineeringCanaryService(catalog).run(bundle)

        failed = catalog.get_run(run_id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.completed_at is not None
        assert failed.error == "RuntimeError: injected shadow child fault"
        assert failed.metadata["failure_type"] == "RuntimeError"
        assert {name for name, _ in observed} == set(original_methods)
        assert {status for _, status in observed} == {"running"}
        first_started_at = failed.started_at

        inject_fault["enabled"] = False
        active_parent_id["value"] = f"{run_id}_attempt_2"
        result = EngineeringCanaryService(catalog).run(bundle)
        assert result.run_id == active_parent_id["value"]
        completed = catalog.get_run(result.run_id)
        assert completed is not None
        assert completed.status == "succeeded"
        assert completed.started_at > first_started_at
        assert completed.completed_at is not None
        assert completed.error is None
        still_failed = catalog.get_run(run_id)
        assert still_failed == failed
        assert result.account_projection_count == 20
        assert catalog.verify_shadow_chain(result.account_id) is True
        assert len(catalog.list_lifecycle_events(sleeve_id=result.sleeve_id)) == 5
        assert {
            item.run_id for item in catalog.list_runs(run_type=EVIDENCE_CLASS)
        } == {run_id, result.run_id}
        assert len(
            catalog.list_shadow_events_by_type(
                account_id=result.account_id,
                event_type="account_projected",
                limit=100,
            )
        ) == 20
    finally:
        catalog.close()


def test_canary_resumes_existing_running_attempt_after_process_interrupt(
    tmp_path,
    monkeypatch,
) -> None:
    catalog = ResearchCatalog(tmp_path / "canary-running-resume.sqlite")
    catalog.initialize_schema()
    bundle = _bundle()
    run_id = f"engcan_{bundle.canonical_hash[:32]}"
    interrupted = {"enabled": True}
    append_events = catalog.append_shadow_events_atomic

    def interrupt_once(*args, **kwargs):
        if interrupted["enabled"]:
            raise KeyboardInterrupt("simulated worker termination")
        return append_events(*args, **kwargs)

    monkeypatch.setattr(catalog, "append_shadow_events_atomic", interrupt_once)
    try:
        with pytest.raises(KeyboardInterrupt, match="worker termination"):
            EngineeringCanaryService(catalog).run(bundle)
        running = catalog.get_run(run_id)
        assert running is not None
        assert running.status == "running"
        assert running.completed_at is None

        interrupted["enabled"] = False
        result = EngineeringCanaryService(catalog).run(bundle)
        assert result.run_id == run_id
        completed = catalog.get_run(run_id)
        assert completed is not None and completed.status == "succeeded"
        assert len(catalog.list_runs(run_type=EVIDENCE_CLASS)) == 1
        assert len(
            catalog.list_shadow_events_by_type(
                account_id=result.account_id,
                event_type="account_projected",
                limit=100,
            )
        ) == 20
    finally:
        catalog.close()


def test_formal_epoch_admission_fails_closed_for_capability_and_evidence_class(
    completed_canary,
) -> None:
    catalog, bundle, result = completed_canary

    assert result.capability.formal_opening_ready is False
    with pytest.raises(
        FormalEpochAdmissionDenied,
        match="opening-execution capability is insufficient",
    ):
        require_formal_epoch_admission(result)

    formal_capability = OpeningExecutionCapability(
        source_id="bounded-real-source-probe",
        status="accepted",
        observed_fields=OBSERVED_OPEN_FIELDS,
        event_semantics="official_open_auction_09_30",
        point_in_time=True,
        real_source_probe=True,
        probe_hash="a" * 64,
    )
    assessment = assess_opening_execution_capability(formal_capability)
    assert assessment.formal_opening_ready is True
    technically_ready_canary = replace(result, capability=assessment)
    with pytest.raises(
        FormalEpochAdmissionDenied,
        match="engineering_canary/non_forward",
    ):
        require_formal_epoch_admission(technically_ready_canary)

    canary_gold = next(
        item for item in result.evidence_markers if item.tier == "gold"
    )
    snapshot = catalog.get_snapshot(canary_gold.snapshot_id)
    assert snapshot is not None
    with pytest.raises(CatalogConflict, match="manifest-bound trading calendar"):
        ResearchCatalog._trusted_forward_calendar(
            snapshot.reference,
            frozen_at=_timestamp(bundle.calendar_sessions[0], time(8, 0)),
            first_forward_session=bundle.calendar_sessions[1],
        )


def test_closed_input_schema_rejects_forward_fields_and_wrong_bounds() -> None:
    bundle = _bundle()
    raw = asdict(bundle.sessions[0].bars[0])
    raw["forward_return_5d"] = 0.5
    with pytest.raises(ValueError, match="forbids forward"):
        CanonicalMarketBar.from_mapping(raw)

    with pytest.raises(CanaryInputRejected, match="exactly 50 securities"):
        CanonicalCanarySession(
            bundle.sessions[0].trade_date,
            bundle.sessions[0].bars[:-1],
        )
    with pytest.raises(CanaryInputRejected, match="exactly 20 projected sessions"):
        AcceptedCanonicalCanaryInput(
            calendar_sessions=bundle.calendar_sessions,
            sessions=bundle.sessions[:-1],
            opening_execution_capability=bundle.opening_execution_capability,
        )


def test_opening_capability_must_be_sufficient_even_for_engineering_execution() -> None:
    bundle = _bundle()
    insufficient = OpeningExecutionCapability(
        source_id="mark-only-source",
        status="degraded",
        observed_fields=("ticker", "open_adj"),
        event_semantics="canonical_open_09_30",
        point_in_time=True,
        real_source_probe=False,
    )
    with pytest.raises(CanaryInputRejected, match="opening-execution capability"):
        AcceptedCanonicalCanaryInput(
            calendar_sessions=bundle.calendar_sessions,
            sessions=bundle.sessions,
            opening_execution_capability=insufficient,
        )
