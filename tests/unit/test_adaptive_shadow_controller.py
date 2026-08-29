from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import factor_lab.adaptive_shadow_controller as controller
from factor_lab.adaptive_shadow import canonical_json_bytes, canonical_sha256


HEAD = "a" * 64
CANDIDATES = controller.CHALLENGER_IDS
SOURCE_SHA = "b" * 64
FORMAL_INPUT_SHA = "c" * 64
ROUTE_SHA = "d" * 64


def _head() -> dict[str, Any]:
    return {
        "sequence": 1,
        "kind": "canary_receipt",
        "record_sha256": HEAD,
        "record": {"kind": "canary_receipt", "payload": {}},
    }


def _decision_metadata(
    sequence: int = 2,
    *,
    offset: int = 0,
    signal: str = "2026-09-10",
    trade: str = "2026-09-11",
) -> dict[str, Any]:
    route = {
        "input_snapshot_sha256": FORMAL_INPUT_SHA,
        "signal_date": signal,
        "trade_date": trade,
        "due_offset": offset,
    }
    plan = {
        "route_target_plan": route,
        "route_target_plan_sha256": canonical_sha256(route),
        "source_data_snapshot_sha256": SOURCE_SHA,
        "admission_deadline_utc": f"{trade}T01:15:00Z",
    }
    return {
        "sequence": sequence,
        "kind": "decision",
        "record_sha256": f"{sequence:064x}",
        "record": {
            "kind": "decision",
            "payload": {"plan_sha256": canonical_sha256(plan), "plan": plan},
        },
    }


def _activation() -> dict[str, Any]:
    return {
        "formal_head_record_sha256": HEAD,
        "registry": {"candidates": [{"candidate_id": value} for value in CANDIDATES]},
    }


def _state(**changes: Any) -> SimpleNamespace:
    values = {
        "activation": _activation(),
        "planning_intents": {},
        "plans": {},
        "plans_by_record": {},
        "missed": {},
        "terminated_accounts": set(),
        "outcomes": {},
        "execution_plan_sha_by_record": {},
        "latest_account_states": {},
    }
    values.update(changes)
    return SimpleNamespace(**values)


def _plan_payload(decision: controller._Decision, candidate: str) -> dict[str, Any]:
    return {
        "plan_type": "adaptive_shadow_target",
        "candidate_id": candidate,
        "formal_decision_record_sha256": decision.record_sha256,
        "formal_route_target_plan_sha256": decision.plan["route_target_plan_sha256"],
        "formal_input_snapshot_sha256": decision.route["input_snapshot_sha256"],
        "source_data_snapshot_sha256": decision.plan["source_data_snapshot_sha256"],
        "shadow_target_rows_sha256": "e" * 64,
        "signal_date": decision.signal_date,
        "trade_date": decision.trade_date,
        "offset": decision.offset,
        "admission_deadline_utc": decision.plan["admission_deadline_utc"],
    }


def _store_plans(*decisions: controller._Decision) -> dict[tuple[str, str, int], tuple[str, str, dict[str, Any]]]:
    result = {}
    index = 10
    for decision in decisions:
        for candidate in CANDIDATES:
            record_sha = f"{index:064x}"
            result[(candidate, decision.signal_date, decision.offset)] = (
                record_sha,
                "f" * 64,
                _plan_payload(decision, candidate),
            )
            index += 1
    return result


def test_observed_time_is_utc_and_requires_timezone() -> None:
    assert controller._utc_text("2026-09-01T09:00:00+08:00") == "2026-09-01T01:00:00Z"
    with pytest.raises(controller.AdaptiveShadowControllerError, match="timezone-aware"):
        controller._utc_text("2026-09-01T01:00:00")


def test_unactivated_shadow_waits_without_reading_formal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(controller, "_shadow_snapshot", lambda _root: None)
    monkeypatch.setattr(
        controller,
        "_audited_formal_records",
        lambda _root: pytest.fail("formal ledger must not be opened"),
    )
    result = controller.advance_adaptive_shadow(
        tmp_path, tmp_path / "formal", tmp_path / "shadow", "2026-09-01T00:00:00Z"
    )
    assert (result["status"], result["reason"], result["action"]) == (
        "waiting",
        "shadow_not_activated",
        None,
    )


def test_earliest_post_activation_undecided_cycle_is_planned_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    later = _decision_metadata(3, signal="2026-09-11", trade="2026-09-12", offset=1)
    earlier = _decision_metadata(2)
    state = _state()
    monkeypatch.setattr(controller, "_shadow_snapshot", lambda _root: ([{"record_sha256": "9" * 64}], state))
    monkeypatch.setattr(controller, "_audited_formal_records", lambda _root: ([_head(), later, earlier], "8" * 64))
    calls: list[controller._Decision] = []

    def advance(*args: Any) -> dict[str, Any]:
        calls.append(args[3])
        return {"status": "planned", "action": "plan"}

    monkeypatch.setattr(controller, "_plan_stage", advance)
    result = controller.advance_adaptive_shadow(
        tmp_path, tmp_path / "formal", tmp_path / "shadow", "2026-09-10T10:00:00Z"
    )
    assert result["status"] == "planned"
    assert [value.sequence for value in calls] == [2]


@pytest.mark.parametrize("status,action", [("planned", "plan"), ("missed", "missed")])
def test_plan_stage_locates_sealed_plan_and_input_cas(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: str,
    action: str,
) -> None:
    decision = controller._decision(_decision_metadata())
    formal = tmp_path / "formal"
    (formal / "plans").mkdir(parents=True)
    input_path = formal / "inputs" / SOURCE_SHA
    input_path.mkdir(parents=True)
    plan_path = formal / "plans" / f"decision-{decision.plan_sha256}.json"
    plan_path.write_bytes(canonical_json_bytes(decision.plan))
    captured: dict[str, Any] = {}

    def plan(*args: Any, **kwargs: Any) -> dict[str, Any]:
        captured["args"], captured["kwargs"] = args, kwargs
        return {"status": status}

    monkeypatch.setattr(controller, "plan_shadow_runtime", plan)
    result = controller._plan_stage(
        tmp_path, formal, tmp_path / "shadow", decision, "2026-09-11T02:00:00Z"
    )
    assert result["action"] == action
    assert captured["kwargs"]["formal_plan_path"] == plan_path
    assert captured["kwargs"]["input_snapshot_path"] == input_path
    assert captured["kwargs"]["formal_decision_record_sha256"] == decision.record_sha256


@pytest.mark.parametrize("mode", ["missing", "tampered", "multiple"])
def test_sealed_plan_locator_fails_closed(tmp_path: Path, mode: str) -> None:
    decision = controller._decision(_decision_metadata())
    plan_root = tmp_path / "plans"
    plan_root.mkdir()
    raw = canonical_json_bytes(decision.plan)
    if mode != "missing":
        (plan_root / f"one-{decision.plan_sha256}.json").write_bytes(
            b"{}" if mode == "tampered" else raw
        )
    if mode == "multiple":
        (plan_root / f"two-{decision.plan_sha256}.json").write_bytes(raw)
    with pytest.raises(controller.AdaptiveShadowControllerError):
        controller._sealed_plan_path(tmp_path, decision)


def test_shadow_plan_and_missed_bindings_are_strict(tmp_path: Path) -> None:
    decision = controller._decision(_decision_metadata())
    plan = _plan_payload(decision, CANDIDATES[0])
    controller._verify_shadow_decision(plan, decision)
    plan["source_data_snapshot_sha256"] = "0" * 64
    with pytest.raises(controller.AdaptiveShadowControllerError, match="formal/source"):
        controller._verify_shadow_decision(plan, decision)
    missed = {
        "formal_decision_record_sha256": decision.record_sha256,
        "signal_date": decision.signal_date,
        "trade_date": decision.trade_date,
        "offset": decision.offset,
        "admission_deadline_utc": "2099-01-01T00:00:00Z",
    }
    with pytest.raises(controller.AdaptiveShadowControllerError, match="formal decision"):
        controller._verify_shadow_decision(missed, decision)
    rogue = _plan_payload(decision, CANDIDATES[0])
    rogue["formal_decision_record_sha256"] = "f" * 64
    rogue_state = _state(
        plans={(CANDIDATES[0], decision.signal_date, decision.offset): ("1" * 64, "2" * 64, rogue)}
    )
    with pytest.raises(controller.AdaptiveShadowControllerError, match="binds no formal"):
        controller._validate_shadow_state(
            [],
            rogue_state,
            [decision],
            {},
            project_root=tmp_path,
            shadow_root=tmp_path / "shadow",
        )
    record_sha = "3" * 64
    valid_plan = _plan_payload(decision, CANDIDATES[0])
    valid_state = _state(
        plans={(CANDIDATES[0], decision.signal_date, decision.offset): (record_sha, "4" * 64, valid_plan)},
        plans_by_record={record_sha: valid_plan},
    )
    shadow_outcome = {
        "record_sha256": "5" * 64,
        "record": {"kind": "outcome"},
        "payload": {"plan_record_sha256": record_sha, "cycle_outcome": {}},
    }
    with pytest.raises(controller.AdaptiveShadowControllerError, match="precedes"):
        controller._validate_shadow_state(
            [shadow_outcome],
            valid_state,
            [decision],
            {},
            project_root=tmp_path,
            shadow_root=tmp_path / "shadow",
        )


def test_formal_view_rejects_legacy_and_duplicate_rich_outcomes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = _decision_metadata()
    decision_sha = metadata["record_sha256"]
    legacy = {
        "sequence": 3,
        "kind": "outcome",
        "record_sha256": "3" * 64,
        "record": {"kind": "outcome", "payload": {"decision_record_sha256": decision_sha}},
    }
    with pytest.raises(controller.AdaptiveShadowControllerError, match="not replayable"):
        controller._formal_view([_head(), metadata, legacy], HEAD)
    cycle = SimpleNamespace(execution_snapshot_sha256="4" * 64, outcome_sha256="5" * 64)
    monkeypatch.setattr(controller, "CycleOutcome", SimpleNamespace(from_mapping=lambda _value: cycle))
    payload = {
        "decision_record_sha256": decision_sha,
        "execution_snapshot_sha256": cycle.execution_snapshot_sha256,
        "cycle_outcome_sha256": cycle.outcome_sha256,
        "cycle_outcome": {},
    }
    rich = lambda sequence: {
        "sequence": sequence,
        "kind": "outcome",
        "record_sha256": f"{sequence:064x}",
        "record": {"kind": "outcome", "payload": payload},
    }
    with pytest.raises(controller.AdaptiveShadowControllerError, match="duplicate"):
        controller._formal_view([_head(), metadata, rich(3), rich(4)], HEAD)


def test_outcome_candidates_advance_independently_but_require_formal_outcome() -> None:
    decision = controller._decision(_decision_metadata())
    both = _store_plans(decision)
    only_one = dict(both)
    del only_one[(CANDIDATES[1], decision.signal_date, decision.offset)]
    formal = {decision.record_sha256: ({}, SimpleNamespace())}
    work = controller._outcome_work(
        [decision], formal, _state(plans=only_one), CANDIDATES
    )
    assert work is not None and work.candidate_id == CANDIDATES[0]
    assert controller._outcome_work([decision], {}, _state(plans=both), CANDIDATES) is None


def test_outcome_order_is_formal_sequence_then_candidate_and_replay_is_idempotent() -> None:
    first = controller._decision(_decision_metadata(2, offset=1))
    second = controller._decision(
        _decision_metadata(3, offset=0, signal="2026-09-11", trade="2026-09-12")
    )
    plans = _store_plans(first, second)
    formal = {
        first.record_sha256: ({}, SimpleNamespace()),
        second.record_sha256: ({}, SimpleNamespace()),
    }
    state = _state(plans=plans)
    selected = controller._outcome_work([first, second], formal, state, CANDIDATES)
    assert (selected.candidate_id, selected.decision.offset, selected.decision.sequence) == (
        CANDIDATES[0],
        1,
        2,
    )
    state.outcomes = {stored[0]: "f" * 64 for stored in plans.values()}
    assert controller._outcome_work([first, second], formal, state, CANDIDATES) is None
    generation = SimpleNamespace(deployment_sha256="8" * 64)
    work = controller._OutcomeWork(second, CANDIDATES[0], "9" * 64, {}, {}, SimpleNamespace())
    genesis = controller._formal_previous_state(work, [first, second], formal, generation)
    assert (genesis.cycle_count, genesis.offset) == (0, second.offset)
    prior_same = controller._decision(_decision_metadata(2, offset=0))
    current_same = controller._decision(
        _decision_metadata(3, offset=0, signal="2026-09-11", trade="2026-09-12")
    )
    prior_state = SimpleNamespace(state_sha256="7" * 64)
    formal_same = {
        prior_same.record_sha256: ({}, SimpleNamespace(next_account_state=prior_state)),
        current_same.record_sha256: ({}, SimpleNamespace()),
    }
    same_work = controller._OutcomeWork(
        current_same, CANDIDATES[0], "9" * 64, {}, {}, SimpleNamespace()
    )
    assert controller._formal_previous_state(
        same_work, [prior_same, current_same], formal_same, generation
    ) is prior_state


def test_outcome_before_availability_is_read_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    decision = controller._decision(_decision_metadata())
    work = controller._OutcomeWork(
        decision,
        CANDIDATES[0],
        "9" * 64,
        {},
        {},
        SimpleNamespace(observation_available_at_utc="2026-09-30T00:00:00Z"),
    )
    monkeypatch.setattr(
        controller,
        "build_adaptive_shadow_execution_snapshot",
        lambda *args, **kwargs: pytest.fail("must not materialize before availability"),
    )
    result = controller._advance_outcome(
        tmp_path,
        tmp_path / "formal",
        tmp_path / "shadow",
        "2026-09-29T23:59:59Z",
        work,
        [decision],
        {},
        _state(),
    )
    assert result["reason"] == "shadow_outcome_not_available"


def test_public_controller_advances_only_one_ready_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    decision = controller._decision(_decision_metadata())
    state = _state(plans=_store_plans(decision))
    formal_cycle = SimpleNamespace()
    monkeypatch.setattr(controller, "_shadow_snapshot", lambda _root: ([], state))
    monkeypatch.setattr(controller, "_audited_formal_records", lambda _root: ([], "6" * 64))
    monkeypatch.setattr(
        controller,
        "_formal_view",
        lambda *_args: ([decision], {decision.record_sha256: ({}, formal_cycle)}),
    )
    calls: list[controller._OutcomeWork] = []

    def advance(*args: Any) -> dict[str, Any]:
        calls.append(args[4])
        return {"status": "advanced", "action": "outcome"}

    monkeypatch.setattr(controller, "_advance_outcome", advance)
    result = controller.advance_adaptive_shadow(
        tmp_path, tmp_path / "formal", tmp_path / "shadow", "2026-09-30T00:00:01Z"
    )
    assert result == {"status": "advanced", "action": "outcome"}
    assert [work.candidate_id for work in calls] == [CANDIDATES[0]]


def test_outcome_uses_latest_states_exact_bindings_and_appends_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    decision = controller._decision(_decision_metadata())
    execution_sha = "6" * 64
    prior_formal = SimpleNamespace(state_sha256="7" * 64)
    prior_shadow = SimpleNamespace(state_sha256="8" * 64)
    generation = SimpleNamespace(
        result_sha256="9" * 64,
        signal_date=decision.signal_date,
        trade_date=decision.trade_date,
        due_offset=decision.offset,
    )
    cycle = SimpleNamespace(
        observation_available_at_utc="2026-09-30T00:00:00Z",
        generation_result_sha256=generation.result_sha256,
        signal_date=generation.signal_date,
        holding_start_date=generation.trade_date,
        holding_end_date="2026-09-25",
        offset=generation.due_offset,
        previous_account_state_sha256=prior_formal.state_sha256,
        execution_snapshot_sha256=execution_sha,
    )
    payload = _plan_payload(decision, CANDIDATES[0])
    work = controller._OutcomeWork(decision, CANDIDATES[0], "1" * 64, payload, {}, cycle)
    state = _state(
        execution_plan_sha_by_record={work.plan_record_sha256: "2" * 64},
        latest_account_states={(work.candidate_id, decision.offset): {"sealed": True}},
    )
    execution_path = tmp_path / "formal" / "executions" / execution_sha
    execution_path.mkdir(parents=True)
    plan = SimpleNamespace(offset=decision.offset)
    monkeypatch.setattr(controller, "GenerationResult", SimpleNamespace(from_mapping=lambda _route: generation))
    monkeypatch.setattr(controller, "_execution_plan", lambda *_args: plan)
    monkeypatch.setattr(controller, "SleeveAccountState", SimpleNamespace(from_mapping=lambda _value: prior_shadow))
    monkeypatch.setattr(controller, "_formal_previous_state", lambda *_args: prior_formal)
    captured: dict[str, Any] = {}
    shadow_market_sha = "a" * 64
    shadow_wrapper_sha = "b" * 64

    def build(*args: Any, **kwargs: Any) -> SimpleNamespace:
        captured["build_args"], captured["build_kwargs"] = args, kwargs
        return SimpleNamespace(
            snapshot=SimpleNamespace(
                snapshot_sha256=shadow_wrapper_sha,
                execution_snapshot=SimpleNamespace(snapshot_sha256=shadow_market_sha),
            ),
            source_contract={"formal_execution_snapshot_sha256": execution_sha},
            source_contract_sha256="2" * 64,
            bundle_sha256="3" * 64,
        )

    outcome = SimpleNamespace(
        shadow_execution_snapshot_sha256=shadow_wrapper_sha,
        market_execution_snapshot_sha256=shadow_market_sha,
        holding_end_date=cycle.holding_end_date,
        observation_available_at_utc=cycle.observation_available_at_utc,
        to_dict=lambda: {"outcome_sha256": "4" * 64},
    )
    monkeypatch.setattr(controller, "build_adaptive_shadow_execution_snapshot", build)
    monkeypatch.setattr(controller, "evaluate_shadow_cycle", lambda *args: outcome)

    def append(root: Path, value: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        captured["append"] = (root, value, kwargs)
        return {"record_sha256": "5" * 64, "created": True}

    monkeypatch.setattr(controller._shadow_store, "append_shadow_outcome", append)
    result = controller._advance_outcome(
        tmp_path,
        tmp_path / "formal",
        tmp_path / "shadow",
        "2026-09-30T00:00:01Z",
        work,
        [decision],
        {decision.record_sha256: ({}, cycle)},
        state,
    )
    assert result["status"] == "advanced"
    assert captured["build_args"][4] is prior_shadow
    assert captured["build_kwargs"]["previous_formal_account_state"] is prior_formal
    assert captured["build_kwargs"]["plan_bindings"] == {
        "plan_record_sha256": work.plan_record_sha256,
        "source_data_snapshot_sha256": SOURCE_SHA,
        "shadow_target_rows_sha256": "e" * 64,
        "formal_route_target_plan_sha256": decision.plan["route_target_plan_sha256"],
    }
    assert captured["append"][2]["recorded_at_utc"] == "2026-09-30T00:00:01Z"
    assert captured["append"][1]["plan_record_sha256"] == work.plan_record_sha256
    assert captured["append"][1]["formal_execution_snapshot_sha256"] == execution_sha
    assert captured["append"][1]["shadow_market_source_contract_sha256"] == "2" * 64
    assert captured["append"][1]["shadow_market_bundle_sha256"] == "3" * 64
