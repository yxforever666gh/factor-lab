from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import hashlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from factor_lab.research_os.catalog import (
    LifecycleEvent,
    ResearchCatalog,
    ShadowEventInput,
)
from factor_lab.research_os.champion_control import (
    AuthoritativeChampionControl,
    ChampionControlError,
    ChampionProjectionUnavailable,
    ChampionStockTargetUnavailable,
)
from factor_lab.research_os.contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    EnvironmentRef,
    ExperimentSpec,
    FactorDirection,
    FactorSpec,
    HypothesisDirection,
    LifecycleState,
    Preregistration,
    SignalFieldSpec,
    SleeveSpec,
    SnapshotTier,
)
from factor_lab.research_os.data_sync import read_frame
from factor_lab.research_os.dsl import FactorGraph, FieldNode
from factor_lab.research_os.evaluator import CANONICAL_EVALUATOR_VERSION
from factor_lab.research_os.risk_optimizer import OptimizedStockPortfolio
from factor_lab.research_os.orm import Base
from factor_lab.research_os.shadow_authority import (
    ShadowEvidenceAuthority,
    ShadowRole,
)
from factor_lab.research_os.snapshots import build_immutable_snapshot_manifest
from factor_lab.research_os.sleeve_registry import (
    SleeveRosterEntry,
    build_sleeve_roster_manifest,
    persist_sleeve_roster,
)


NOW = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)


def _catalog(path) -> ResearchCatalog:
    return ResearchCatalog(
        path, allowed_evaluator_versions=(CANONICAL_EVALUATOR_VERSION,)
    )


def _snapshot(identity: str = "a") -> DataSnapshotRef:
    digest = identity * 64
    return DataSnapshotRef(
        snapshot_id=digest,
        tier=SnapshotTier.GOLD,
        uri=f"iceberg://factorlab/factor_lab.gold#ros_{digest}",
        content_hash=digest,
        as_of=NOW,
        quality_status=DataQualityStatus.ACCEPTED,
        trust_labels=("point_in_time",),
    )


def _preregistration(candidate: str) -> Preregistration:
    return Preregistration(
        hypothesis_id=f"hyp_{candidate}",
        economic_mechanism="pre-registered economic mechanism",
        direction=HypothesisDirection.POSITIVE,
        falsification_criteria=("outer OOS excess is non-positive",),
        stop_rules=("stop after two diagnostic branches",),
    )


def _environment() -> EnvironmentRef:
    return EnvironmentRef(
        code_hash="b" * 64,
        dependency_lock_hash="c" * 64,
        configuration_hash="d" * 64,
        dirty_patch_hash="e" * 64,
        python_version="3.10",
        platform="test",
        evaluator_build=CANONICAL_EVALUATOR_VERSION,
    )


def _sleeve_spec(
    sleeve_id: str,
    cluster_id: str | None,
    *,
    maximum_weight: float = 0.35,
) -> ExperimentSpec:
    return ExperimentSpec(
        snapshot=_snapshot(),
        sleeve=SleeveSpec(
            sleeve_id=sleeve_id,
            name=sleeve_id,
            mechanism="stable cross-sectional mechanism",
            factor_ids=(f"{sleeve_id}_factor",),
            signal_expression=FactorGraph(
                nodes=(FieldNode("raw", "score"),), output_id="raw"
            ).to_dict(),
            signal_field_registry=(
                SignalFieldSpec(
                    name="score",
                    available_at_column="score_available_at",
                ),
            ),
            cluster_id=cluster_id,
            maximum_weight=maximum_weight,
            falsification_criteria=("new OOS evidence reverses",),
        ),
        evaluator_version=CANONICAL_EVALUATOR_VERSION,
        environment=_environment(),
        preregistration=_preregistration(sleeve_id),
    )


def _factor_spec() -> ExperimentSpec:
    return ExperimentSpec(
        snapshot=_snapshot(),
        factor=FactorSpec(
            factor_id="factor_only",
            family="value",
            name="factor only",
            mechanism="test",
            expression=FactorGraph(
                nodes=(FieldNode("raw", "score"),), output_id="raw"
            ).to_dict(),
            signal_field_registry=(SignalFieldSpec(name="score"),),
            direction=FactorDirection.HIGHER_IS_BETTER,
            falsification_criteria=("fails",),
        ),
        evaluator_version=CANONICAL_EVALUATOR_VERSION,
        environment=_environment(),
        preregistration=_preregistration("factor_only"),
    )


def _register_candidate(
    catalog: ResearchCatalog,
    spec: ExperimentSpec,
    *,
    outcome: str = "promoted_to_shadow",
    state: LifecycleState = LifecycleState.ACTIVE,
    completed_at: datetime = NOW,
) -> None:
    experiment = catalog.register_experiment(spec)
    catalog.record_authoritative_result(
        experiment.experiment_id,
        outcome=outcome,
        metrics={"promotion_verdict": "promote"},
        completed_at=completed_at,
    )
    candidate_id = spec.candidate_id
    catalog.append_lifecycle_event(
        LifecycleEvent(
            idempotency_key=f"test:{candidate_id}:{completed_at.isoformat()}:{state.value}",
            sleeve_id=candidate_id,
            to_state=state,
            cause="test",
            occurred_at=completed_at,
            evidence={"experiment_fingerprint": spec.fingerprint()},
        )
    )


def _approved_overlay(
    control: AuthoritativeChampionControl,
    scores: dict[str, float],
    *,
    generated_at: datetime,
    session_count: int = 60,
    include_prefreeze_projection: bool = False,
) -> str:
    catalog = control.catalog
    backend_path = Path(str(catalog._backend._database))  # type: ignore[attr-defined]
    authority_root = backend_path.parent / "adaptive-authority"
    authority_root.mkdir(parents=True, exist_ok=True)
    calendar_file = authority_root / "calendar-source.csv"
    pd.DataFrame({"proof": ["trusted"]}).to_csv(calendar_file, index=False)
    sessions = pd.bdate_range("2026-08-24", periods=session_count)
    session_values = [stamp.date().isoformat() for stamp in sessions]
    manifest = build_immutable_snapshot_manifest(
        (calendar_file,),
        base_dir=authority_root,
        tier="gold",
        as_of=NOW,
        parent_snapshot_ids=("f" * 64,),
        environment_hashes={
            "config_hash": "1" * 64,
            "code_hash": "2" * 64,
            "dirty_patch_hash": "3" * 64,
            "dependency_lock_hash": "4" * 64,
        },
        quality_report={"status": "pass"},
        trust_labels=("point_in_time",),
        trading_calendar={
            "source": "test-exchange-calendar",
            "quality_status": "accepted",
            "sessions": session_values,
            "content_hash": hashlib.sha256(
                "\n".join(session_values).encode("ascii")
            ).hexdigest(),
        },
    )
    calendar_snapshot = manifest.to_snapshot_ref(
        uri=f"iceberg://factorlab/factor_lab.gold#ros_{manifest.snapshot_id}"
    )
    catalog.register_snapshot(calendar_snapshot)
    catalog.freeze_evidence_epoch(
        architecture_version="test-v1",
        code_hash="2" * 64,
        configuration_hash="1" * 64,
        dependency_lock_hash="4" * 64,
        dirty_patch_hash="3" * 64,
        frozen_at=NOW,
    )
    catalog.activate_evidence_epoch(
        calendar_snapshot_id=calendar_snapshot.snapshot_id,
        first_forward_session=sessions[0].date(),
        activated_at=NOW + timedelta(minutes=1),
    )

    history_dates = [
        pd.Timestamp(year=year, month=1, day=1) + pd.Timedelta(days=7 * offset)
        for year in range(2021, 2026)
        for offset in range(12)
    ]

    def comparison_experiment(candidate: str, base: float, variation_modulus: int):
        spec = _sleeve_spec(candidate, f"comparison-{candidate}")
        experiment = catalog.register_experiment(spec)
        periods = [
            {
                "start_date": stamp.date().isoformat(),
                "net_return": base + (
                    0.002 if index % variation_modulus == 0 else 0.0
                ),
            }
            for index, stamp in enumerate(history_dates)
        ]
        result = catalog.record_authoritative_result(
            experiment.experiment_id,
            outcome="promoted_to_shadow",
            metrics={
                "promotion_verdict": "promote",
                "fold_results": [{"fold_id": "stitched", "periods": periods}],
            },
            completed_at=NOW,
        )
        return spec, experiment.experiment_id, result

    challenger_spec, challenger_experiment_id, challenger_result = comparison_experiment(
        "cmp_challenger", 0.012, 2
    )
    champion_spec, champion_experiment_id, champion_result = comparison_experiment(
        "cmp_champion", 0.001, 3
    )
    for spec in (challenger_spec, champion_spec):
        catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key=f"comparison-role:{spec.candidate_id}",
                sleeve_id=spec.candidate_id,
                to_state=LifecycleState.DORMANT,
                cause="test_comparison_role_only",
                occurred_at=NOW,
                evidence={"experiment_fingerprint": spec.fingerprint()},
            )
        )
    roster = build_sleeve_roster_manifest(
        (
            SleeveRosterEntry(
                sleeve=challenger_spec.sleeve,
                representative_priority=0,
            ),
            SleeveRosterEntry(
                sleeve=champion_spec.sleeve,
                representative_priority=1,
            ),
        ),
        roster_name="adaptive-comparison-authority",
    )
    persist_sleeve_roster(catalog, roster, recorded_at=NOW)
    execution_snapshot = _snapshot("1")
    mark_snapshot = _snapshot("2")
    catalog.register_snapshot(execution_snapshot)
    catalog.register_snapshot(mark_snapshot)

    for account_id in ("forward-challenger", "forward-champion"):
        catalog.create_shadow_account(
            account_id=account_id,
            name=account_id,
            initial_capital=50_000_000,
            opened_at=(
                NOW - timedelta(minutes=1)
                if include_prefreeze_projection
                else NOW + timedelta(minutes=2)
            ),
        )
        if include_prefreeze_projection:
            account = catalog.get_shadow_account(account_id)
            assert account is not None
            catalog.append_shadow_event(
                account_id=account_id,
                event_type="account_projected",
                occurred_at=NOW,
                payload={
                    "account_status": "open",
                    "account_state": {
                        "cash": 50_000_000.0,
                        "nav": 50_000_000.0,
                        "benchmark_nav": 50_000_000.0,
                    },
                },
                expected_previous_hash=account.last_event_hash,
            )
    backend_path = Path(str(catalog._backend._database))  # type: ignore[attr-defined]
    shadow_authority = ShadowEvidenceAuthority(
        f"sqlite+pysqlite:///{backend_path.as_posix()}",
        enforce_realtime=False,
        require_fleet_closure=False,
    )
    Base.metadata.create_all(shadow_authority.engine)
    challenger_binding = shadow_authority.bind_role(
        role=ShadowRole.CHALLENGER,
        role_key=challenger_experiment_id,
        account_id="forward-challenger",
        sleeve_id=challenger_spec.sleeve.sleeve_id,
        experiment_id=challenger_experiment_id,
        bound_at=NOW + timedelta(minutes=3),
        metadata={
            "result_id": challenger_result.result_id,
            "result_hash": challenger_result.result_hash,
            "roster_manifest_id": roster.roster_id,
        },
    )
    champion_binding = shadow_authority.bind_role(
        role=ShadowRole.CHAMPION,
        role_key="static_champion",
        account_id="forward-champion",
        sleeve_id=champion_spec.sleeve.sleeve_id,
        experiment_id=champion_experiment_id,
        bound_at=NOW + timedelta(minutes=3),
        metadata={
            "result_id": champion_result.result_id,
            "result_hash": champion_result.result_hash,
            "roster_manifest_id": roster.roster_id,
        },
    )
    control.shadow_authority = shadow_authority
    control.legacy_shadow_evidence = False
    challenger_nav = champion_nav = 50_000_000.0
    for index, session in enumerate(sessions):
        challenger_nav *= 1.0 + (0.003 if index % 2 else 0.002)
        champion_nav *= 1.0 + (0.0008 if index % 2 else 0.0004)
        for account_id, nav, binding in (
            ("forward-challenger", challenger_nav, challenger_binding),
            ("forward-champion", champion_nav, champion_binding),
        ):
            account = catalog.get_shadow_account(account_id)
            assert account is not None
            step_id = f"test-{account_id}-{session.date().isoformat()}"
            bindings = {
                "decision_snapshot_id": None,
                "execution_snapshot_id": execution_snapshot.snapshot_id,
                "mark_snapshot_id": mark_snapshot.snapshot_id,
            }
            committed = catalog.append_shadow_events_atomic(
                account_id=account_id,
                expected_previous_hash=account.last_event_hash,
                events=(
                    ShadowEventInput(
                        event_type="session_evidence",
                        occurred_at=datetime.combine(
                            session.date(), time(20), tzinfo=timezone.utc
                        ),
                        payload={
                            "research_os_shadow_step": {
                                "step_id": step_id,
                                "kind": "domain_event",
                            },
                            "snapshot_bindings": bindings,
                            "rebalanced": False,
                            "fees": 0.0,
                            "metrics": {},
                        },
                    ),
                    ShadowEventInput(
                        event_type="mark_to_market",
                        occurred_at=datetime.combine(
                            session.date(), time(20), tzinfo=timezone.utc
                        ),
                        payload={
                            "research_os_shadow_step": {
                                "step_id": step_id,
                                "kind": "domain_event",
                            },
                            "snapshot_bindings": bindings,
                            "cash": nav,
                            "positions_value": 0.0,
                            "nav": nav,
                            "benchmark_nav": nav,
                            "position_count": 0,
                        },
                    ),
                    ShadowEventInput(
                        event_type="account_projected",
                        occurred_at=datetime.combine(
                            session.date(), time(20), tzinfo=timezone.utc
                        ),
                        payload={
                            "research_os_shadow_step": {
                                "step_id": step_id,
                                "kind": "account_projection",
                            },
                            "account_status": "open",
                            "account_state": {
                                "cash": nav,
                                "nav": nav,
                                "benchmark_nav": nav,
                            },
                        },
                    ),
                ),
            )
            shadow_authority.record_projection(
                role_binding_id=binding.binding_id,
                account_event_hash=committed[-1].event_hash,
                trade_date=session.date(),
                recorded_at=datetime.combine(
                    session.date(), time(20, 1), tzinfo=timezone.utc
                ),
            )
    decision, authority = control.evaluate_authoritative_challenger(
        historical_challenger_experiment_id=challenger_experiment_id,
        shadow_challenger_account_id="forward-challenger",
        shadow_champion_account_id="forward-champion",
    )
    return control.persist_adaptive_approval(
        decision.to_dict(),
        scores,
        authority=authority,
        generated_at=generated_at,
        source_partition="2026-08",
    ).run_id


def test_only_authoritative_promoted_sleeve_specs_enter_champion(tmp_path) -> None:
    with _catalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(_snapshot())
        _register_candidate(catalog, _sleeve_spec("accepted", "value"))
        _register_candidate(
            catalog,
            _sleeve_spec("failed", "defensive"),
            outcome="falsified_or_insufficient",
        )
        _register_candidate(catalog, _factor_spec())

        rows = AuthoritativeChampionControl(catalog).authoritative_sleeves()

    assert [row.sleeve_id for row in rows] == ["accepted"]
    assert rows[0].cluster_id == "value"
    assert rows[0].experiment_fingerprint == _sleeve_spec(
        "accepted", "value"
    ).fingerprint()


def test_promoted_sleeve_without_fingerprint_bound_cluster_fails_closed(tmp_path) -> None:
    with _catalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(_snapshot())
        _register_candidate(catalog, _sleeve_spec("unclustered", None))
        with pytest.raises(ChampionControlError, match="cluster_id"):
            AuthoritativeChampionControl(catalog).authoritative_sleeves()


def test_lifecycle_drives_real_allocation_and_projection_is_persisted(tmp_path) -> None:
    with _catalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(_snapshot())
        _register_candidate(catalog, _sleeve_spec("active", "value"))
        _register_candidate(
            catalog, _sleeve_spec("reduced", "trend"), state=LifecycleState.REDUCED
        )
        _register_candidate(
            catalog,
            _sleeve_spec("dormant", "defensive"),
            state=LifecycleState.DORMANT,
        )
        control = AuthoritativeChampionControl(catalog)
        projection = control.build_allocation(
            data_snapshot_id=_snapshot().snapshot_id,
            generated_at=NOW + timedelta(minutes=1),
        )
        effective = projection.effective_allocation
        model = projection.model_allocation["sleeve_weights"]

        assert effective["sleeve_weights"]["active"] == pytest.approx(
            model["active"]
        )
        assert effective["sleeve_weights"]["reduced"] == pytest.approx(
            model["reduced"] * 0.5
        )
        assert "dormant" not in effective["sleeve_weights"]
        assert effective["benchmark_weight"] == pytest.approx(
            1.0 - sum(effective["sleeve_weights"].values())
        )
        assert max(model.values()) <= 0.35
        control.persist_allocation(projection)
        loaded = control.latest_allocation()
        assert loaded is not None
        assert loaded.to_dict() == projection.to_dict()


def test_frozen_data_forces_cash_and_no_healthy_sleeve_uses_half_benchmark(tmp_path) -> None:
    with _catalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(_snapshot())
        _register_candidate(
            catalog,
            _sleeve_spec("dormant", "defensive"),
            state=LifecycleState.DORMANT,
        )
        control = AuthoritativeChampionControl(catalog)
        dormant = control.build_allocation(
            data_snapshot_id=_snapshot().snapshot_id,
            generated_at=NOW + timedelta(minutes=1),
        )
        assert dormant.effective_allocation["benchmark_weight"] == 0.5
        assert dormant.effective_allocation["cash_weight"] == 0.5

        catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key="freeze:dormant",
                sleeve_id="dormant",
                from_state=LifecycleState.DORMANT,
                to_state=LifecycleState.FROZEN_DATA,
                cause="data_integrity_failure",
                occurred_at=NOW + timedelta(minutes=2),
            )
        )
        frozen = control.build_allocation(
            data_snapshot_id=_snapshot().snapshot_id,
            generated_at=NOW + timedelta(minutes=3),
        )
        assert frozen.effective_allocation["sleeve_weights"] == {}
        assert frozen.effective_allocation["benchmark_weight"] == 0.0
        assert frozen.effective_allocation["cash_weight"] == 1.0


def test_state_overlay_obeys_75_25_cap_and_five_point_monthly_change(tmp_path) -> None:
    with _catalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(_snapshot())
        for sleeve, cluster in (
            ("value", "fundamental"),
            ("quality", "fundamental"),
            ("trend", "trend"),
            ("low_risk", "defensive"),
        ):
            _register_candidate(catalog, _sleeve_spec(sleeve, cluster))
        control = AuthoritativeChampionControl(catalog)
        scores = {"trend": 100.0}
        approval_time = NOW + timedelta(days=120)
        approval_run_id = _approved_overlay(
            control, scores, generated_at=approval_time
        )
        first = control.build_allocation(
            data_snapshot_id=_snapshot().snapshot_id,
            generated_at=approval_time - timedelta(minutes=1),
        )
        second = control.build_allocation(
            data_snapshot_id=_snapshot().snapshot_id,
            generated_at=approval_time,
            adaptive_scores=scores,
            previous=first,
            adaptive_approval_run_id=approval_run_id,
        )

        assert second.state_overlay["adaptive_fraction"] == 0.25
        assert max(second.model_allocation["sleeve_weights"].values()) <= 0.35
        for sleeve, weight in second.model_allocation["sleeve_weights"].items():
            prior = first.model_allocation["sleeve_weights"].get(sleeve, 0.0)
            assert abs(weight - prior) <= 0.05 + 1e-12


def test_adaptive_approval_rejects_59_shadow_sessions(tmp_path) -> None:
    with _catalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(_snapshot())
        _register_candidate(catalog, _sleeve_spec("trend", "trend"))
        control = AuthoritativeChampionControl(catalog)
        with pytest.raises(ChampionControlError, match="60 common forward sessions"):
            _approved_overlay(
                control,
                {"trend": 1.0},
                generated_at=NOW + timedelta(days=120),
                session_count=59,
            )


def test_caller_cannot_select_an_arbitrary_historical_champion_result(tmp_path) -> None:
    with _catalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        control = AuthoritativeChampionControl(catalog)
        with pytest.raises(TypeError, match="historical_champion_experiment_id"):
            control.evaluate_authoritative_challenger(
                historical_challenger_experiment_id="challenger",
                historical_champion_experiment_id="arbitrary_completed_result",  # type: ignore[call-arg]
                shadow_challenger_account_id="challenger-account",
                shadow_champion_account_id="champion-account",
            )


def test_adaptive_approval_invalidates_when_shadow_chain_tip_changes(tmp_path) -> None:
    with _catalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(_snapshot())
        _register_candidate(catalog, _sleeve_spec("trend", "trend"))
        control = AuthoritativeChampionControl(catalog)
        approval_time = NOW + timedelta(days=120)
        approval_id = _approved_overlay(
            control, {"trend": 1.0}, generated_at=approval_time
        )
        base = control.build_allocation(
            data_snapshot_id=_snapshot().snapshot_id,
            generated_at=approval_time - timedelta(minutes=1),
        )
        account = catalog.get_shadow_account("forward-challenger")
        assert account is not None
        catalog.append_shadow_event(
            account_id=account.account_id,
            event_type="risk_alert",
            occurred_at=approval_time + timedelta(minutes=1),
            payload={"code": "new_authoritative_fact"},
            expected_previous_hash=account.last_event_hash,
        )
        with pytest.raises(ChampionControlError, match="sources changed"):
            control.build_allocation(
                data_snapshot_id=_snapshot().snapshot_id,
                generated_at=approval_time + timedelta(minutes=2),
                adaptive_scores={"trend": 1.0},
                previous=base,
                adaptive_approval_run_id=approval_id,
            )


def test_unapproved_adaptive_scores_fail_and_each_cluster_has_one_representative(
    tmp_path,
) -> None:
    with _catalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(_snapshot())
        _register_candidate(
            catalog,
            _sleeve_spec("older", "value"),
            completed_at=NOW,
        )
        _register_candidate(
            catalog,
            _sleeve_spec("newer", "value"),
            completed_at=NOW + timedelta(minutes=1),
        )
        control = AuthoritativeChampionControl(catalog)
        assert [row.sleeve_id for row in control.authoritative_sleeves()] == ["newer"]
        with pytest.raises(ChampionControlError, match="approval"):
            control.build_allocation(
                data_snapshot_id=_snapshot().snapshot_id,
                generated_at=NOW + timedelta(minutes=2),
                adaptive_scores={"newer": 1.0},
            )


def _bound_gold_frame(tmp_path):
    sessions = pd.bdate_range("2026-05-18", periods=70)
    rows = []
    for ticker_index in range(500):
        ticker = f"{ticker_index:06d}.SZ"
        price = 10.0 + ticker_index / 1_000
        for day_index, session in enumerate(sessions):
            market_return = 0.0002 + 0.0008 * np.sin(day_index / 7)
            noise = 0.0002 * np.sin((day_index + 1) * (ticker_index % 17 + 1))
            price *= 1.0 + market_return + noise
            decision_time = (
                pd.Timestamp(session)
                .tz_localize("Asia/Shanghai")
                .replace(hour=18)
                .tz_convert("UTC")
            )
            rows.append(
                {
                    "date": session,
                    "ticker": ticker,
                    "score": float(ticker_index % 100),
                    "score_available_at": decision_time - pd.Timedelta(minutes=1),
                    "universe_member": True,
                    "benchmark_weight": 1.0 / 500.0,
                    "adv_20": 2_000_000_000.0,
                    "close_adj": price,
                    "industry": f"I{ticker_index % 5}",
                    "industry_available_at": decision_time - pd.Timedelta(days=30),
                    "log_market_cap": np.log(1_000_000_000.0 + ticker_index * 1_000_000),
                    "size_available_at": decision_time - pd.Timedelta(minutes=1),
                    "decision_time": decision_time,
                }
            )
    frame = pd.DataFrame(rows)
    path = tmp_path / "gold.parquet"
    frame.to_parquet(path, index=False)
    calendar_sessions = [item.date().isoformat() for item in sessions]
    calendar_sessions.append((sessions[-1] + pd.offsets.BDay()).date().isoformat())
    calendar_hash = hashlib.sha256(
        "\n".join(calendar_sessions).encode("ascii")
    ).hexdigest()
    manifest = build_immutable_snapshot_manifest(
        (path,),
        base_dir=tmp_path,
        tier="gold",
        as_of=(
            sessions[-1]
            .tz_localize("Asia/Shanghai")
            .replace(hour=20)
            .tz_convert("UTC")
            .to_pydatetime()
        ),
        parent_snapshot_ids=("f" * 64,),
        environment_hashes={
            "config_hash": "1" * 64,
            "code_hash": "2" * 64,
            "dirty_patch_hash": "3" * 64,
            "dependency_lock_hash": "4" * 64,
        },
        quality_report={"status": "pass"},
        trust_labels=("point_in_time",),
        trading_calendar={
            "source": "test-exchange-calendar",
            "quality_status": "accepted",
            "sessions": calendar_sessions,
            "content_hash": calendar_hash,
        },
    )
    snapshot = manifest.to_snapshot_ref(
        uri=f"iceberg://factorlab/factor_lab.gold#ros_{manifest.snapshot_id}"
    )
    return read_frame(path), snapshot, sessions[-1].date()


def test_live_gold_projection_physically_removes_forward_labels() -> None:
    decision = date(2026, 8, 21)
    frame = pd.DataFrame(
        [
            {
                "ticker": "000001.SZ",
                "date": decision,
                "universe_member": True,
                "benchmark_weight": 1.0,
                "adv_20": 2_000_000_000.0,
                "close_adj": 10.0,
                "industry": "bank",
                "industry_available_at": "2026-08-20T10:00:00Z",
                "log_market_cap": 20.0,
                "size_available_at": "2026-08-21T09:00:00Z",
                "score": 1.0,
                "forward_return_5d": 0.99,
                "future_alpha": 0.88,
                "next_open": 12.0,
                "return_label_5d": 1,
                "fwd_spread": 0.77,
                "lead_close": 11.0,
            }
        ]
    )

    projected = AuthoritativeChampionControl._normalise_gold_frame(frame, decision)

    assert set(projected.columns).isdisjoint(
        {
            "forward_return_5d",
            "future_alpha",
            "next_open",
            "return_label_5d",
            "fwd_spread",
            "lead_close",
        }
    )
    assert projected.loc[0, "score"] == 1.0
    assert projected.loc[0, "close_adj"] == 10.0


def test_stock_target_uses_persisted_projection_authoritative_dsl_and_gold(
    tmp_path, monkeypatch
) -> None:
    frame, gold_snapshot, decision = _bound_gold_frame(tmp_path)

    def fake_optimizer(scores, returns, metadata, benchmark, *, policy):
        selected = scores.sort_values(ascending=False).head(50).index
        weights = {str(ticker): 0.02 for ticker in selected}
        return OptimizedStockPortfolio(
            status="ok",
            weights=weights,
            cash_weight=0.0,
            promotion_eligible=True,
            audit={"position_count": 50, "source": "test optimizer"},
        )

    monkeypatch.setattr(
        "factor_lab.research_os.champion_control.optimize_stock_weights",
        fake_optimizer,
    )
    with _catalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(_snapshot())
        catalog.register_snapshot(gold_snapshot)
        _register_candidate(catalog, _sleeve_spec("trend", "trend"))
        control = AuthoritativeChampionControl(catalog)
        projection = control.build_allocation(
            data_snapshot_id=_snapshot().snapshot_id,
            generated_at=NOW + timedelta(minutes=1),
        )
        with pytest.raises(ChampionProjectionUnavailable):
            control.build_stock_target(
                projection,
                gold_snapshot_id=gold_snapshot.snapshot_id,
                gold_frame=frame,
                decision_date=decision,
                generated_at=NOW + timedelta(minutes=2),
            )
        control.persist_allocation(projection)
        target = control.build_stock_target(
            projection,
            gold_snapshot_id=gold_snapshot.snapshot_id,
            gold_frame=frame,
            decision_date=decision,
            generated_at=NOW + timedelta(minutes=2),
        )
        control.persist_stock_target(target)
        loaded = control.latest_stock_target(decision_date=decision)

        assert loaded is not None
        assert loaded.to_dict() == target.to_dict()
        assert target.component_audit["source"] == (
            "authoritative_result_plus_latest_gold_dsl"
        )
        assert target.component_audit["sleeves"]["trend"][
            "experiment_fingerprint"
        ] == _sleeve_spec("trend", "trend").fingerprint()
        assert sum(target.target_weights.values()) + target.cash_weight == pytest.approx(
            1.0
        )
        assert max(target.target_weights.values()) <= 0.02 + 1e-12


def test_stock_target_rejects_unbound_or_stale_gold_frame(tmp_path) -> None:
    frame, gold_snapshot, decision = _bound_gold_frame(tmp_path)
    with _catalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(_snapshot())
        catalog.register_snapshot(gold_snapshot)
        _register_candidate(
            catalog,
            _sleeve_spec("shadow_only", "trend"),
            state=LifecycleState.SHADOW,
        )
        control = AuthoritativeChampionControl(catalog)
        projection = control.build_allocation(
            data_snapshot_id=_snapshot().snapshot_id,
            generated_at=NOW + timedelta(minutes=1),
        )
        control.persist_allocation(projection)
        unbound = frame.copy()
        unbound.attrs.clear()
        with pytest.raises(Exception, match="source path"):
            control.build_stock_target(
                projection,
                gold_snapshot_id=gold_snapshot.snapshot_id,
                gold_frame=unbound,
                decision_date=decision,
                generated_at=NOW + timedelta(minutes=2),
            )
        with pytest.raises(
            ChampionStockTargetUnavailable, match="decision close|latest"
        ):
            control.build_stock_target(
                projection,
                gold_snapshot_id=gold_snapshot.snapshot_id,
                gold_frame=frame,
                decision_date=decision - timedelta(days=1),
                generated_at=NOW + timedelta(minutes=2),
            )
