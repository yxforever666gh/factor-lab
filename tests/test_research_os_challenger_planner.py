from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

import factor_lab.research_os.challenger_planner as module
from factor_lab.research_os.catalog import LifecycleEvent, ResearchCatalog
from factor_lab.research_os.challenger_planner import (
    AuthoritativeChallengerPlanner,
    ChallengerPlannerError,
)
from factor_lab.research_os.contracts import (
    DataSnapshotRef,
    EnvironmentRef,
    ExperimentSpec,
    FactorSpec,
    Preregistration,
)
from factor_lab.research_os.shadow_authority import (
    ShadowRole,
    ShadowRoleBinding,
)
from factor_lab.research_os.sleeve_registry import (
    load_sleeve_roster,
    persist_sleeve_roster,
)


ROOT = Path(__file__).resolve().parents[1]
DECISION = date(2026, 1, 2)


class FakeAuthority:
    def __init__(self, binding: ShadowRoleBinding) -> None:
        self.binding = binding

    def active_binding(self, *, role, role_key):
        assert ShadowRole(role) is ShadowRole.CHALLENGER
        return self.binding if role_key == self.binding.role_key else None


def _snapshot() -> DataSnapshotRef:
    return DataSnapshotRef(
        snapshot_id="a" * 64,
        tier="gold",
        uri="s3://test/gold/a",
        content_hash="a" * 64,
        as_of=datetime(2026, 1, 2, 7, tzinfo=timezone.utc),
        manifest={
            "trading_calendar": {
                "source": "accepted-test-calendar",
                "quality_status": "accepted",
                "sessions": [
                    "2026-01-02",
                    "2026-01-05",
                    "2026-01-06",
                    "2026-01-07",
                    "2026-01-08",
                    "2026-01-09",
                    "2026-01-12",
                ],
            }
        },
    )


def _environment() -> EnvironmentRef:
    return EnvironmentRef(
        code_hash="1" * 64,
        dependency_lock_hash="2" * 64,
        configuration_hash="3" * 64,
        dirty_patch_hash="4" * 64,
        python_version="3.12",
        platform="test",
        evaluator_build="research_os.long_only.v2",
    )


def _preregistration() -> Preregistration:
    return Preregistration(
        hypothesis_id="planner-authority",
        economic_mechanism="frozen value-quality mechanism",
        direction="positive",
        falsification_criteria=("outer OOS active return is non-positive",),
        stop_rules=("stop after the frozen protocol",),
    )


def _binding(
    *,
    experiment_id: str,
    sleeve_id: str,
    account_id: str,
    roster_id: str,
    result_id: str,
    result_hash: str,
):
    return ShadowRoleBinding(
        binding_id="shadow_binding_" + "5" * 64,
        binding_hash="5" * 64,
        role=ShadowRole.CHALLENGER,
        role_key=experiment_id,
        account_id=account_id,
        sleeve_id=sleeve_id,
        experiment_id=experiment_id,
        epoch_id=None,
        active=True,
        bound_at=datetime(2026, 1, 2, 8, tzinfo=timezone.utc),
        unbound_at=None,
        metadata={
            "roster_manifest_id": roster_id,
            "result_id": result_id,
            "result_hash": result_hash,
        },
    )


def _register_binding_event(
    catalog: ResearchCatalog, *, experiment_id: str, sleeve_id: str, account_id: str
) -> None:
    catalog.append_lifecycle_event(
        LifecycleEvent(
            idempotency_key=f"planner-binding:{experiment_id}",
            sleeve_id=sleeve_id,
            from_state="walk_forward",
            to_state="shadow",
            cause="challenger_shadow_account_bound",
            occurred_at=datetime(2026, 1, 2, 8, tzinfo=timezone.utc),
            evidence={
                "promotion": {"experiment_id": experiment_id},
                "shadow_account_id": account_id,
            },
        )
    )


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [pd.Timestamp(DECISION)] * 500,
            "ticker": [f"{index:06d}.SZ" for index in range(500)],
            "universe_member": [True] * 500,
            "benchmark_weight": [1.0 / 500.0] * 500,
        }
    )


def _promoted_setup(catalog: ResearchCatalog):
    roster = load_sleeve_roster(ROOT / "configs" / "research_os_initial_sleeves.json")
    persist_sleeve_roster(catalog, roster, recorded_at=datetime(2026, 1, 2, 8, tzinfo=timezone.utc))
    sleeve = roster.by_sleeve_id()["value_quality_v1"].sleeve
    snapshot = _snapshot()
    catalog.register_snapshot(snapshot)
    experiment = catalog.register_experiment(
        ExperimentSpec(
            snapshot=snapshot,
            sleeve=sleeve,
            evaluator_version="research_os.long_only.v2",
            environment=_environment(),
            preregistration=_preregistration(),
        )
    )
    result = catalog.record_authoritative_result(
        experiment.experiment_id,
        outcome="promoted_to_shadow",
        metrics={"net_sharpe": 0.9},
        completed_at=datetime(2026, 1, 2, 8, tzinfo=timezone.utc),
    )
    account_id = "challenger-planner-account"
    catalog.create_shadow_account(
        account_id=account_id,
        name="Challenger planner",
        initial_capital=50_000_000,
        opened_at=datetime(2026, 1, 2, 8, tzinfo=timezone.utc),
    )
    _register_binding_event(
        catalog,
        experiment_id=experiment.experiment_id,
        sleeve_id=sleeve.sleeve_id,
        account_id=account_id,
    )
    binding = _binding(
        experiment_id=experiment.experiment_id,
        sleeve_id=sleeve.sleeve_id,
        account_id=account_id,
        roster_id=roster.roster_id,
        result_id=result.result_id,
        result_hash=result.result_hash,
    )
    return snapshot, experiment, binding


def test_promoted_sleeve_persists_deterministic_next_session_target_and_marks_holds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with ResearchCatalog(tmp_path / "planner.db") as catalog:
        catalog.initialize_schema()
        snapshot, experiment, binding = _promoted_setup(catalog)
        planner = AuthoritativeChallengerPlanner(
            catalog, shadow_authority=FakeAuthority(binding)
        )
        frame = _frame()
        monkeypatch.setattr(module, "verify_snapshot_frame_binding", lambda *_args: None)
        monkeypatch.setattr(
            planner._weight_engine,
            "_normalise_gold_frame",
            lambda current, _decision: current,
        )
        weights = {f"{index:06d}.SZ": 0.02 for index in range(50)}
        monkeypatch.setattr(
            planner._weight_engine,
            "_sleeve_stock_weights",
            lambda *_args: (weights, {"optimizer": "deterministic-test"}),
        )

        first = planner.generate_due_targets(
            gold_snapshot_id=snapshot.snapshot_id,
            gold_frame=frame,
            decision_date=DECISION,
        )
        retry = planner.generate_due_targets(
            gold_snapshot_id=snapshot.snapshot_id,
            gold_frame=frame,
            decision_date=DECISION,
        )

        assert len(first) == 1
        assert retry[0].target_id == first[0].target_id
        assert first[0].experiment_id == experiment.experiment_id
        assert first[0].account_id == binding.account_id
        assert first[0].role_binding_id == binding.binding_id
        assert first[0].gold_snapshot_id == snapshot.snapshot_id
        assert planner.plans_for_trade_date("2026-01-05")[0].target_weights == weights
        hold = planner.plans_for_trade_date("2026-01-06")[0]
        assert hold.target_weights is None
        assert hold.decision_snapshot_id is None
        runs = catalog.list_runs(run_type="challenger_stock_target")
        assert len(runs) == 1
        assert runs[0].input_fingerprint == first[0].target_id


@pytest.mark.parametrize("outcome", ("falsified", "blocked"))
def test_non_promoted_results_cannot_enter_challenger(
    tmp_path: Path, outcome: str
) -> None:
    with ResearchCatalog(tmp_path / f"{outcome}.db") as catalog:
        catalog.initialize_schema()
        roster = load_sleeve_roster(ROOT / "configs" / "research_os_initial_sleeves.json")
        persist_sleeve_roster(catalog, roster)
        sleeve = roster.by_sleeve_id()["value_quality_v1"].sleeve
        snapshot = _snapshot()
        catalog.register_snapshot(snapshot)
        experiment = catalog.register_experiment(
            ExperimentSpec(
                snapshot=snapshot,
                sleeve=sleeve,
                evaluator_version="research_os.long_only.v2",
                environment=_environment(),
                preregistration=_preregistration(),
            )
        )
        result = catalog.record_authoritative_result(
            experiment.experiment_id,
            outcome=outcome,
            metrics={},
            completed_at=datetime(2026, 1, 2, 8, tzinfo=timezone.utc),
        )
        account_id = f"{outcome}-account"
        catalog.create_shadow_account(
            account_id=account_id,
            name=outcome,
            initial_capital=50_000_000,
            opened_at=datetime(2026, 1, 2, 8, tzinfo=timezone.utc),
        )
        _register_binding_event(
            catalog,
            experiment_id=experiment.experiment_id,
            sleeve_id=sleeve.sleeve_id,
            account_id=account_id,
        )
        planner = AuthoritativeChallengerPlanner(
            catalog,
            shadow_authority=FakeAuthority(
                _binding(
                    experiment_id=experiment.experiment_id,
                    sleeve_id=sleeve.sleeve_id,
                    account_id=account_id,
                    roster_id=roster.roster_id,
                    result_id=result.result_id,
                    result_hash=result.result_hash,
                )
            ),
        )
        with pytest.raises(ChallengerPlannerError, match="promoted authoritative Sleeve"):
            planner.active_authorities()


def test_factor_experiment_cannot_enter_challenger(tmp_path: Path) -> None:
    with ResearchCatalog(tmp_path / "factor.db") as catalog:
        catalog.initialize_schema()
        roster = load_sleeve_roster(ROOT / "configs" / "research_os_initial_sleeves.json")
        persist_sleeve_roster(catalog, roster)
        snapshot = _snapshot()
        catalog.register_snapshot(snapshot)
        experiment = catalog.register_experiment(
            ExperimentSpec(
                snapshot=snapshot,
                factor=FactorSpec(
                    factor_id="factor-only",
                    family="value_quality_v1",
                    name="Factor only",
                    mechanism="not yet assembled",
                    expression={
                        "schema_version": "research-os/factor-dsl/v1",
                        "output_id": "raw",
                        "nodes": [
                            {"id": "raw", "op": "field", "field": "book_to_price"}
                        ],
                    },
                    direction="higher_is_better",
                    falsification_criteria=("falsify",),
                ),
                evaluator_version="research_os.long_only.v2",
                environment=_environment(),
                preregistration=_preregistration(),
            )
        )
        result = catalog.record_authoritative_result(
            experiment.experiment_id,
            outcome="promoted_to_shadow",
            metrics={},
            completed_at=datetime(2026, 1, 2, 8, tzinfo=timezone.utc),
        )
        account_id = "factor-account"
        catalog.create_shadow_account(
            account_id=account_id,
            name="Factor",
            initial_capital=50_000_000,
            opened_at=datetime(2026, 1, 2, 8, tzinfo=timezone.utc),
        )
        _register_binding_event(
            catalog,
            experiment_id=experiment.experiment_id,
            sleeve_id="value_quality_v1",
            account_id=account_id,
        )
        planner = AuthoritativeChallengerPlanner(
            catalog,
            shadow_authority=FakeAuthority(
                _binding(
                    experiment_id=experiment.experiment_id,
                    sleeve_id="value_quality_v1",
                    account_id=account_id,
                    roster_id=roster.roster_id,
                    result_id=result.result_id,
                    result_hash=result.result_hash,
                )
            ),
        )
        with pytest.raises(ChallengerPlannerError, match="promoted authoritative Sleeve"):
            planner.active_authorities()
