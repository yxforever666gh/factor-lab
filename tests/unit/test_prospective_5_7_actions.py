from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

import factor_lab.data.prospective as prospective_data
import factor_lab.data.prospective_execution as execution_data
import factor_lab.implementation_closure as implementation_closure
import factor_lab.prospective_ledger as prospective_ledger
from tests.unit import test_prospective_ledger as ledger_cases


DECISION = "1" * 64
SOURCE = "2" * 64
GENERATION = "3" * 64


@pytest.fixture(autouse=True)
def _isolate_ledger_harness(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        prospective_data,
        "load_prospective_input_snapshot",
        prospective_data._load_prospective_input_snapshot_files,
    )
    monkeypatch.setattr(
        execution_data,
        "load_prospective_execution_snapshot",
        ledger_cases._load_test_execution_snapshot,
    )
    monkeypatch.setattr(
        implementation_closure,
        "verify_implementation_closure",
        lambda *_args, **_kwargs: {"schema_version": 1},
    )
    monkeypatch.setattr(
        prospective_ledger,
        "_materialize_upgrade_runtime_capsule",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        prospective_ledger,
        "_verify_upgrade_runtime_closure",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        prospective_ledger,
        "_run_active_release_operation",
        ledger_cases._run_test_release_operation_wrapped,
    )


def _cycle(*, calendar_index: int = 7, due_offset: int = 7) -> dict[str, Any]:
    return {
        "legacy_single_slot": False,
        "calendar_index": calendar_index,
        "due_offset": due_offset,
        "plan": {"source_data_snapshot_sha256": SOURCE},
    }


def _state(
    *,
    cycles: Mapping[str, Mapping[str, Any]] | None = None,
    evaluation_due: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        evaluation_due=evaluation_due,
        open_cycles=dict(cycles or {DECISION: _cycle()}),
    )


def _report() -> dict[str, Any]:
    return {
        "observed_at_utc": "2026-09-30T08:00:00Z",
        "status": "waiting",
        "reason": "same_offset_capacity",
        "ready": False,
        "next_action": "wait",
        "action": None,
        "ready_for": {
            "membership_build": False,
            "input_build": False,
            "decision_admission": False,
        },
        "issues": [{"code": "SAME_OFFSET_CAPACITY_FULL"}],
    }


def _sealed_open_cycle(tmp_path: Path) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    ledger = tmp_path / "runtime/prospective/5.0"
    ledger_cases._ready(ledger)
    case = ledger_cases._plan_case(ledger)
    decision = ledger_cases._seal_case(
        ledger,
        case,
        recorded_at="2026-08-24T12:01:00Z",
    )
    ledger_cases._decision_receipt(
        ledger,
        decision,
        run_id=571,
        signal="2026-08-24",
    )
    return ledger, case, decision


def _install_public_observer(
    tmp_path: Path,
    ledger: Path,
    case: Mapping[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    *,
    expected_ledger: Mapping[str, Any],
    calls: list[str],
) -> None:
    observed = "2026-09-30T08:00:00Z"
    report = {
        **_report(),
        "observed_at_utc": observed,
        "clock_source": "caller_supplied",
        "stable_view": True,
        "ledger": dict(expected_ledger),
        "candidate": {
            "due_offset": int(case["result"].due_offset),
            "entry_date": "2026-09-30",
        },
    }

    def observer(project_root: Path, **kwargs: Any) -> dict[str, Any]:
        assert Path(project_root) == tmp_path
        assert Path(kwargs["ledger_root"]) == ledger
        assert kwargs["observed_at_utc"] == observed
        calls.append("observer")
        return deepcopy(report)

    monkeypatch.setattr(
        "factor_lab.data.prospective_readiness.inspect_prospective_readiness",
        observer,
    )


def _post_cycle(
    monkeypatch: pytest.MonkeyPatch,
    *,
    bundle: Mapping[str, Any] | None = None,
    sources: Mapping[str, Any] | None = None,
    state: SimpleNamespace | None = None,
    generated: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any] | None:
    monkeypatch.setattr(
        prospective_ledger,
        "_execution_bundle_readiness",
        lambda *_args, **_kwargs: dict(bundle or {"status": "missing"}),
    )
    if sources is not None:
        monkeypatch.setattr(
            execution_data,
            "inspect_prospective_execution_sources",
            lambda *_args, **_kwargs: dict(sources),
        )
    return prospective_ledger._post_cycle_readiness(
        _report(),
        SimpleNamespace(),
        state or _state(),
        generated or {DECISION: {"result_sha256": GENERATION}},
        project=SimpleNamespace(),
    )


def test_public_readiness_closes_mature_oldest_before_candidate_capacity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger, case, decision = _sealed_open_cycle(tmp_path)
    observer_calls: list[str] = []
    source_calls: list[str] = []
    _install_public_observer(
        tmp_path,
        ledger,
        case,
        monkeypatch,
        expected_ledger=ledger_cases._authoritative_readiness_ledger_view(ledger),
        calls=observer_calls,
    )
    monkeypatch.setattr(
        prospective_ledger,
        "_execution_bundle_readiness",
        lambda *_args, **_kwargs: {"status": "missing"},
    )

    def sources(*_args, **_kwargs):
        source_calls.append("sources")
        return {
            "status": "suspensions_missing",
            "holding_end_date": "2026-09-09",
        }

    monkeypatch.setattr(
        execution_data,
        "inspect_prospective_execution_sources",
        sources,
    )

    report = prospective_ledger.prospective_readiness(
        ledger,
        project_root=tmp_path,
        observed_at_utc="2026-09-30T08:00:00Z",
    )

    assert report["reason"] == "outcome_suspensions_sync_ready"
    assert report["action"]["argv"] == [
        "data",
        "suspensions",
        "--from",
        execution_data.SUSPENSION_FULL_START_DATE,
        "--to",
        "2026-09-09",
        "--no-resume",
    ]
    assert report["post_cycle"]["decision_record_sha256"] == str(
        decision["record_sha256"]
    )
    # The source/action is observed twice around the final stable ledger gate.
    assert source_calls == ["sources", "sources"]
    assert len(observer_calls) == 3


def test_public_readiness_returns_evaluate_when_evaluation_is_due(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger, case, _decision = _sealed_open_cycle(tmp_path)
    layout = prospective_ledger.LedgerLayout.at(ledger)
    with prospective_ledger._existing_read_lock(layout):
        records, state, generated = prospective_ledger._load_verified_record_chain(
            layout,
            refresh_cache=False,
        )
    state.evaluation_due = "engineering_closure"

    def synthetic_chain(*_args, **_kwargs):
        return deepcopy(records), deepcopy(state), deepcopy(generated)

    monkeypatch.setattr(
        prospective_ledger,
        "_load_verified_record_chain",
        synthetic_chain,
    )
    observer_calls: list[str] = []
    _install_public_observer(
        tmp_path,
        ledger,
        case,
        monkeypatch,
        expected_ledger=ledger_cases._authoritative_readiness_ledger_view(ledger),
        calls=observer_calls,
    )
    monkeypatch.setattr(
        prospective_ledger,
        "_execution_bundle_readiness",
        lambda *_args, **_kwargs: pytest.fail(
            "evaluation must preempt open-cycle execution"
        ),
    )

    report = prospective_ledger.prospective_readiness(
        ledger,
        project_root=tmp_path,
        observed_at_utc="2026-09-30T08:00:00Z",
    )

    assert report["reason"] == "evaluation_checkpoint_ready"
    assert report["action"]["argv"] == ["prospective", "evaluate"]
    assert len(observer_calls) == 3


def test_evaluation_due_is_the_first_machine_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        prospective_ledger,
        "_execution_bundle_readiness",
        lambda *_args, **_kwargs: pytest.fail("evaluation must preempt execution"),
    )

    action = prospective_ledger._post_cycle_readiness(
        _report(),
        SimpleNamespace(),
        _state(evaluation_due="engineering_closure"),
        {DECISION: {"result_sha256": GENERATION}},
        project=SimpleNamespace(),
    )

    assert action is not None
    assert action["reason"] == "evaluation_checkpoint_ready"
    assert action["action"]["argv"] == ["prospective", "evaluate"]


def test_oldest_open_cycle_is_inspected_before_later_offsets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    later = "4" * 64
    seen: list[str] = []

    def bundle(_layout, _state, *, decision_record_sha256, generation):
        seen.append(decision_record_sha256)
        return {"status": "missing"}

    monkeypatch.setattr(prospective_ledger, "_execution_bundle_readiness", bundle)
    monkeypatch.setattr(
        execution_data,
        "inspect_prospective_execution_sources",
        lambda *_args, **_kwargs: {"status": "not_mature"},
    )
    state = _state(
        cycles={
            later: _cycle(calendar_index=9, due_offset=9),
            DECISION: _cycle(calendar_index=7, due_offset=7),
        }
    )

    assert prospective_ledger._post_cycle_readiness(
        _report(),
        SimpleNamespace(),
        state,
        {
            later: {"result_sha256": "5" * 64},
            DECISION: {"result_sha256": GENERATION},
        },
        project=SimpleNamespace(),
    ) is None
    assert seen == [DECISION]


def test_unmatured_open_cycle_does_not_preempt_decision_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert _post_cycle(
        monkeypatch,
        sources={"status": "not_mature"},
    ) is None


def test_missing_holding_data_returns_exact_daily_adj_factor_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    action = _post_cycle(
        monkeypatch,
        sources={
            "status": "market_data_missing",
            "holding_end_date": "2026-09-09",
            "missing_partition_keys": [
                "adj_factor/2026-09-09",
                "daily/2026-09-08",
            ],
        },
    )

    assert action is not None
    assert action["reason"] == "outcome_market_data_sync_ready"
    assert action["action"]["argv"] == [
        "data",
        "sync",
        "--from",
        "2026-09-08",
        "--to",
        "2026-09-09",
        "--calendar-to",
        "2026-09-09",
        "--dataset",
        "daily",
        "--dataset",
        "adj_factor",
        "--resume",
    ]


def test_missing_suspensions_returns_full_history_no_resume_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    action = _post_cycle(
        monkeypatch,
        sources={
            "status": "suspensions_missing",
            "holding_end_date": "2026-09-09",
            "recovery": "uncommitted_parquet_without_metadata",
        },
    )

    assert action is not None
    assert action["reason"] == "outcome_suspensions_sync_ready"
    assert action["action"]["argv"] == [
        "data",
        "suspensions",
        "--from",
        execution_data.SUSPENSION_FULL_START_DATE,
        "--to",
        "2026-09-09",
        "--no-resume",
    ]


def test_complete_sources_then_partial_then_complete_bundle_order_actions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    build = _post_cycle(
        monkeypatch,
        sources={"status": "complete", "holding_end_date": "2026-09-09"},
    )
    assert build is not None
    assert build["action"]["argv"] == [
        "prospective",
        "execution",
        "--decision",
        DECISION,
    ]

    partial = _post_cycle(
        monkeypatch,
        bundle={
            "status": "partial",
            "execution_snapshot_sha256": "6" * 64,
            "observation_available_at_utc": "2026-09-09T08:30:00Z",
        },
    )
    assert partial is not None
    assert partial["reason"] == "execution_resume_ready"
    assert partial["action"]["argv"] == [
        "prospective",
        "execution",
        "--decision",
        DECISION,
    ]

    complete = _post_cycle(
        monkeypatch,
        bundle={
            "status": "complete",
            "execution_snapshot_sha256": "6" * 64,
            "observation_available_at_utc": "2026-09-09T08:30:00Z",
        },
    )
    assert complete is not None
    assert complete["reason"] == "outcome_append_ready"
    assert complete["action"]["argv"] == [
        "prospective",
        "outcome",
        "--decision",
        DECISION,
        "--execution",
        "6" * 64,
    ]


def test_waiting_source_is_exit_2_state_without_an_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _post_cycle(
        monkeypatch,
        sources={
            "status": "waiting",
            "reason": "holding_end_partition_not_yet_available",
        },
    )

    assert result is not None
    assert result["status"] == "waiting"
    assert result["action"] is None
    assert result["issues"][0]["retryable"] is True


def test_invalid_or_duplicate_execution_evidence_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def invalid(*_args, **_kwargs):
        raise prospective_ledger.LedgerIntegrityError(
            "multiple execution bundles match one sealed decision"
        )

    monkeypatch.setattr(prospective_ledger, "_execution_bundle_readiness", invalid)

    result = prospective_ledger._post_cycle_readiness(
        _report(),
        SimpleNamespace(),
        _state(),
        {DECISION: {"result_sha256": GENERATION}},
        project=SimpleNamespace(),
    )

    assert result is not None
    assert result["status"] == "blocked"
    assert result["reason"] == "execution_evidence_invalid"
    assert result["action"] is None
