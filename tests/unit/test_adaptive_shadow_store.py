from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
import json
import multiprocessing
from pathlib import Path
from typing import Any

import pytest

from factor_lab.adaptive_shadow import canonical_json_bytes, canonical_sha256
from factor_lab.adaptive_shadow_execution import (
    ShadowCyclePlan,
    ShadowExecutionSnapshot,
    evaluate_shadow_cycle,
    genesis_shadow_account,
)
from factor_lab.adaptive_shadow_planning import build_registry_from_protocol
from factor_lab.adaptive_shadow_store import (
    ShadowLayout,
    ShadowStoreError,
    activate_shadow_store,
    append_shadow_evaluation,
    append_shadow_missed,
    append_shadow_outcome,
    append_shadow_plan,
    append_shadow_planning,
    audit_shadow_store,
    shadow_store_status,
)
from factor_lab.prospective_execution import ExecutionSnapshot
from factor_lab.prospective_targets import calendar_prefix_sha256


COMMIT = "a" * 40
TAG_OBJECT = "b" * 40
FORMAL_HEAD = "c" * 64
PROTOCOL_SHA = "d" * 64
FORMAL_DECISION = "e" * 64
FORMAL_PLAN = "f" * 64
FORMAL_INPUT = "1" * 64
SOURCE = "2" * 64
SHADOW_ROWS = "3" * 64
FORMAL_EXECUTION = "4" * 64
FUTURE_DECISION = "6" * 64
HEALTHY_DECISION = "7" * 64
CANDIDATES = ("low_turnover_20_v1", "low_volatility_252_v1")
TARGETS = tuple(f"{index:06d}.SZ" for index in range(1, 11))


def _protocol() -> dict[str, Any]:
    path = Path(__file__).parents[2] / "protocols" / "5.9-adaptive-shadow.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _registry():
    return build_registry_from_protocol(
        _protocol(),
        release_tag="5.9",
        commit_oid=COMMIT,
        released_at_utc="2026-09-01T00:00:00Z",
        start_after="2026-09-01",
    )


def _activate(root: Path) -> dict[str, Any]:
    return activate_shadow_store(
        root,
        registry=_registry(),
        release_tag_object_oid=TAG_OBJECT,
        release_commit_oid=COMMIT,
        protocol_sha256=PROTOCOL_SHA,
        formal_head_record_sha256=FORMAL_HEAD,
        released_at_utc="2026-09-01T00:00:00Z",
        start_after="2026-09-01",
        recorded_at_utc="2026-09-01T00:00:00Z",
    )


def _base_plan(
    candidate_id: str = CANDIDATES[0],
    *,
    signal_date: str = "2026-09-10",
    trade_date: str = "2026-09-11",
    offset: int = 0,
    formal_decision_record_sha256: str = FORMAL_DECISION,
    created_at_utc: str | None = None,
) -> dict[str, Any]:
    registry = _registry()
    candidate = registry.candidate(candidate_id)
    created = created_at_utc or f"{signal_date}T12:00:00Z"
    return {
        "schema_version": 1,
        "plan_type": "adaptive_shadow_target",
        "candidate_id": candidate_id,
        "candidate_version": candidate.version,
        "signal_date": signal_date,
        "trade_date": trade_date,
        "offset": offset,
        "registry_sha256": registry.sha256,
        "candidate_sha256": candidate.sha256,
        "formal_decision_record_sha256": formal_decision_record_sha256,
        "formal_route_target_plan_sha256": FORMAL_PLAN,
        "formal_input_snapshot_sha256": FORMAL_INPUT,
        "source_data_snapshot_sha256": SOURCE,
        "shadow_target_rows_sha256": SHADOW_ROWS,
        "targets_ppm": {ticker: 100_000 for ticker in TARGETS},
        "cash_ppm": 0,
        "admission_deadline_utc": f"{trade_date}T01:15:00Z",
        "created_at_utc": created,
    }


def _append_planning(
    root: Path,
    *,
    candidate_ids: tuple[str, ...] = CANDIDATES,
    signal_date: str = "2026-09-10",
    trade_date: str = "2026-09-11",
    offset: int = 0,
    formal_decision_record_sha256: str = FORMAL_DECISION,
    created_at_utc: str | None = None,
    recorded_at_utc: str | None = None,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    created = created_at_utc or f"{signal_date}T12:00:00Z"
    bases = [
        _base_plan(
            candidate_id,
            signal_date=signal_date,
            trade_date=trade_date,
            offset=offset,
            formal_decision_record_sha256=formal_decision_record_sha256,
            created_at_utc=created,
        )
        for candidate_id in candidate_ids
    ]
    payload = {
        "schema_version": 1,
        "planning_type": "adaptive_shadow_planning_intent",
        "registry_sha256": _registry().sha256,
        "formal_decision_record_sha256": formal_decision_record_sha256,
        "signal_date": signal_date,
        "trade_date": trade_date,
        "offset": offset,
        "admission_deadline_utc": f"{trade_date}T01:15:00Z",
        "created_at_utc": created,
        "ordered_plan_payloads": bases,
    }
    stored = append_shadow_planning(
        root,
        payload,
        recorded_at_utc=recorded_at_utc or created,
    )
    binding = {
        "planning_record_sha256": stored["record_sha256"],
        "planning_payload_sha256": canonical_sha256(payload),
    }
    return stored, {
        str(base["candidate_id"]): {**base, **binding}
        for base in bases
    }


def _missed(
    candidate_id: str = CANDIDATES[0],
    *,
    signal_date: str = "2026-09-10",
    trade_date: str = "2026-09-11",
    offset: int = 0,
    formal_decision_record_sha256: str = FORMAL_DECISION,
    missed_at_utc: str = "2026-09-11T01:15:01Z",
    reason: str = "missed_deadline",
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "missed_type": "adaptive_shadow_missed",
        "candidate_id": candidate_id,
        "signal_date": signal_date,
        "trade_date": trade_date,
        "offset": offset,
        "registry_sha256": _registry().sha256,
        "formal_decision_record_sha256": formal_decision_record_sha256,
        "admission_deadline_utc": f"{trade_date}T01:15:00Z",
        "missed_at_utc": missed_at_utc,
        "reason": reason,
    }


def _execution_plan(plan: dict[str, Any]) -> ShadowCyclePlan:
    return ShadowCyclePlan(
        registry_sha256=str(plan["registry_sha256"]),
        candidate_id=str(plan["candidate_id"]),
        candidate_sha256=str(plan["candidate_sha256"]),
        offset=int(plan["offset"]),
        signal_date=str(plan["signal_date"]),
        trade_date=str(plan["trade_date"]),
        targets_ppm=dict(plan["targets_ppm"]),
        formal_input_snapshot_sha256=str(plan["formal_input_snapshot_sha256"]),
        formal_decision_record_sha256=str(plan["formal_decision_record_sha256"]),
        planned_at_utc=str(plan["created_at_utc"]),
        formal_trade_deadline_utc=str(plan["admission_deadline_utc"]),
    )


def _materialize_market_bundle(
    root: Path,
    execution_plan: ShadowCyclePlan,
) -> tuple[ShadowExecutionSnapshot, dict[str, Any]]:
    # Full source-contract replay is exercised by the real slow-path data test.
    # This low-level store fixture uses real execution/outcome objects and only
    # materializes the immutable identity fields that the store owns.
    sources = {
        "schema_version": 1,
        "kind": "adaptive_shadow_market_sources",
        "formal_execution_snapshot_sha256": FORMAL_EXECUTION,
        "formal_execution_bundle": {"snapshot_sha256": FORMAL_EXECUTION},
        "target_plan_sha256": execution_plan.plan_sha256,
        "formal_decision_record_sha256": (
            execution_plan.formal_decision_record_sha256
        ),
    }
    sources_raw = canonical_json_bytes(sources)
    source_sha = hashlib.sha256(sources_raw).hexdigest()
    sessions = (
        "2026-08-27",
        "2026-08-28",
        "2026-08-31",
        "2026-09-01",
        "2026-09-02",
        "2026-09-03",
        "2026-09-04",
        "2026-09-07",
        "2026-09-08",
        "2026-09-09",
        "2026-09-10",
        "2026-09-11",
        "2026-09-14",
        "2026-09-15",
        "2026-09-16",
        "2026-09-17",
        "2026-09-18",
        "2026-09-21",
        "2026-09-22",
        "2026-09-23",
        "2026-09-24",
        "2026-09-25",
    )
    rows = []
    for day_index, session in enumerate(sessions[11:]):
        for ticker in TARGETS:
            rows.append(
                {
                    "date": session,
                    "ticker": ticker,
                    "open_adj_hex": (100.0 + day_index).hex(),
                    "adv_20_asof_hex": (
                        (1_000_000_000.0).hex() if day_index == 0 else None
                    ),
                    "volatility_20_asof_hex": (
                        (0.0).hex() if day_index == 0 else None
                    ),
                    "execution_input_date": (
                        "2026-09-10" if day_index == 0 else None
                    ),
                    "is_one_price_limit_up": False,
                    "is_one_price_limit_down": False,
                    "is_suspended": False,
                    "is_delisted": False,
                }
            )
    market = ExecutionSnapshot(
        generation_result_sha256="8" * 64,
        execution_source_sha256=source_sha,
        official_calendar_sha256=calendar_prefix_sha256(sessions),
        signal_date="2026-09-10",
        holding_start_date="2026-09-11",
        holding_end_date="2026-09-25",
        calendar_sessions=sessions,
        benchmark_tickers=TARGETS,
        rows=rows,
        calendar_available_at_utc="2026-09-10T08:00:00Z",
        decision_inputs_available_at_utc="2026-09-10T12:00:00Z",
        trade_deadline_utc="2026-09-11T01:15:00Z",
        start_open_available_at_utc="2026-09-11T01:30:00Z",
        end_open_available_at_utc="2026-09-25T01:30:00Z",
        observation_available_at_utc="2026-09-25T02:00:00Z",
    )
    wrapper = ShadowExecutionSnapshot(
        target_plan_sha256=execution_plan.plan_sha256,
        formal_input_snapshot_sha256=execution_plan.formal_input_snapshot_sha256,
        formal_decision_record_sha256=execution_plan.formal_decision_record_sha256,
        execution_snapshot=market,
    )
    bundle_sha = canonical_sha256(
        {
            "shadow_execution_snapshot_sha256": wrapper.snapshot_sha256,
            "shadow_market_source_contract_sha256": source_sha,
        }
    )
    directory = ShadowLayout.at(root).market_windows / bundle_sha
    directory.mkdir(parents=True)
    (directory / "snapshot.json").write_bytes(canonical_json_bytes(wrapper.to_dict()))
    (directory / "sources.json").write_bytes(sources_raw)
    return wrapper, {
        "formal_execution_snapshot_sha256": FORMAL_EXECUTION,
        "shadow_market_source_contract_sha256": source_sha,
        "shadow_market_bundle_sha256": bundle_sha,
    }


def _process_append(root: str, payload: dict[str, Any]) -> None:
    append_shadow_plan(root, payload, recorded_at_utc="2026-09-10T13:00:00Z")


def test_layout_and_uninitialized_status_are_observational(tmp_path: Path) -> None:
    root = tmp_path / "shadow"
    layout = ShadowLayout.at(root)
    status = shadow_store_status(layout)
    assert status["status"] == "uninitialized"
    assert status["head_sequence"] == 0
    assert not root.exists()


def test_activation_binds_registry_release_protocol_and_formal_head(tmp_path: Path) -> None:
    activation = _activate(tmp_path / "shadow")
    audit = audit_shadow_store(tmp_path / "shadow")
    assert activation["payload"]["protocol_sha256"] == PROTOCOL_SHA
    assert activation["payload"]["release_tag_object_oid"] == TAG_OBJECT
    assert activation["payload"]["formal_head_record_sha256"] == FORMAL_HEAD
    assert audit["activated"] is True and audit["head_sequence"] == 1


def test_shadow_writes_leave_formal_ledger_tree_byte_identical(tmp_path: Path) -> None:
    formal = tmp_path / "formal"
    (formal / "records").mkdir(parents=True)
    (formal / "records" / "0001.json").write_bytes(b"sealed-formal-record")
    before = [
        (path.relative_to(formal), path.read_bytes())
        for path in formal.rglob("*")
        if path.is_file()
    ]
    root = tmp_path / "shadow"
    _activate(root)
    _planning, plans = _append_planning(root)
    append_shadow_plan(
        root,
        plans[CANDIDATES[0]],
        recorded_at_utc="2026-09-10T13:00:00Z",
    )
    after = [
        (path.relative_to(formal), path.read_bytes())
        for path in formal.rglob("*")
        if path.is_file()
    ]
    assert after == before


def test_planning_at_deadline_is_legal_and_one_second_late_is_rejected(
    tmp_path: Path,
) -> None:
    equal_root = tmp_path / "equal"
    _activate(equal_root)
    _planning, plans = _append_planning(
        equal_root,
        recorded_at_utc="2026-09-11T01:15:00Z",
    )
    append_shadow_plan(
        equal_root,
        plans[CANDIDATES[0]],
        recorded_at_utc="2026-09-11T01:15:01Z",
    )
    equal_audit = audit_shadow_store(equal_root)
    assert equal_audit["planning_intent_count"] == 1
    assert equal_audit["plan_count"] == 1

    late_root = tmp_path / "late"
    _activate(late_root)
    with pytest.raises(ShadowStoreError, match="planning intent must be recorded"):
        _append_planning(
            late_root,
            recorded_at_utc="2026-09-11T01:15:01Z",
        )
    late_audit = audit_shadow_store(late_root)
    assert late_audit["planning_intent_count"] == 0
    assert late_audit["orphan_artifact_count"] == 0


def test_planning_seals_all_active_candidates_and_exact_plan_bytes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "shadow"
    _activate(root)
    with pytest.raises(ShadowStoreError, match="every active challenger"):
        _append_planning(root, candidate_ids=(CANDIDATES[0],))

    planning, plans = _append_planning(root)
    assert [
        row["candidate_id"]
        for row in planning["payload"]["ordered_plan_payloads"]
    ] == list(CANDIDATES)
    changed = deepcopy(plans[CANDIDATES[0]])
    changed["shadow_target_rows_sha256"] = "9" * 64
    with pytest.raises(ShadowStoreError, match="bytes differ"):
        append_shadow_plan(
            root,
            changed,
            recorded_at_utc="2026-09-10T13:00:00Z",
        )
    append_shadow_plan(
        root,
        plans[CANDIDATES[0]],
        recorded_at_utc="2026-09-10T13:00:00Z",
    )
    assert audit_shadow_store(root)["plan_count"] == 1


def test_float_payload_is_rejected_before_any_artifact(tmp_path: Path) -> None:
    root = tmp_path / "shadow"
    _activate(root)
    _planning, plans = _append_planning(root)
    plan = plans[CANDIDATES[0]]
    plan["targets_ppm"] = {**plan["targets_ppm"], TARGETS[0]: 100_000.0}
    with pytest.raises(ValueError):
        append_shadow_plan(root, plan, recorded_at_utc="2026-09-10T13:00:00Z")
    audit = audit_shadow_store(root)
    assert audit["planning_intent_count"] == 1
    assert audit["plan_count"] == 0


def test_first_miss_terminates_only_that_candidate_offset_forever(
    tmp_path: Path,
) -> None:
    root = tmp_path / "shadow"
    _activate(root)
    append_shadow_missed(root, _missed(), recorded_at_utc="2026-09-11T01:15:01Z")

    with pytest.raises(ShadowStoreError, match="cannot precede"):
        append_shadow_missed(
            root,
            _missed(CANDIDATES[1]),
            recorded_at_utc="2026-09-11T01:15:00Z",
        )

    future_initial = _missed(
        signal_date="2026-09-24",
        trade_date="2026-09-25",
        formal_decision_record_sha256=FUTURE_DECISION,
        missed_at_utc="2026-09-25T01:15:01Z",
    )
    with pytest.raises(ShadowStoreError, match="requires a live account"):
        append_shadow_missed(
            root,
            future_initial,
            recorded_at_utc="2026-09-25T01:15:01Z",
        )

    append_shadow_missed(
        root,
        _missed(
            signal_date="2026-09-24",
            trade_date="2026-09-25",
            formal_decision_record_sha256=FUTURE_DECISION,
            missed_at_utc="2026-09-24T12:00:00Z",
            reason="account_terminated_after_prior_miss",
        ),
        recorded_at_utc="2026-09-24T12:00:00Z",
    )

    _planning, healthy_plans = _append_planning(
        root,
        candidate_ids=(CANDIDATES[1],),
        signal_date="2026-10-08",
        trade_date="2026-10-09",
        formal_decision_record_sha256=HEALTHY_DECISION,
    )
    append_shadow_plan(
        root,
        healthy_plans[CANDIDATES[1]],
        recorded_at_utc="2026-10-08T13:00:00Z",
    )
    audit = audit_shadow_store(root)
    assert audit["missed_count"] == 2
    assert audit["terminated_account_count"] == 1
    assert audit["planning_intent_count"] == 1
    assert audit["plan_count"] == 1


def test_plan_then_missed_is_mutually_exclusive(tmp_path: Path) -> None:
    root = tmp_path / "shadow"
    _activate(root)
    _planning, plans = _append_planning(root)
    append_shadow_plan(
        root,
        plans[CANDIDATES[0]],
        recorded_at_utc="2026-09-10T13:00:00Z",
    )
    with pytest.raises(ShadowStoreError, match="planning intent forbids"):
        append_shadow_missed(root, _missed(), recorded_at_utc="2026-09-11T01:15:01Z")


def test_complete_cycle_outcome_binds_formal_wrapper_market_and_bundle(
    tmp_path: Path,
) -> None:
    root = tmp_path / "shadow"
    _activate(root)
    _planning, plans = _append_planning(root)
    plan_payload = plans[CANDIDATES[0]]
    stored = append_shadow_plan(
        root,
        plan_payload,
        recorded_at_utc="2026-09-10T13:00:00Z",
    )
    plan_record_sha = stored["record_sha256"]
    shadow_plan = _execution_plan(plan_payload)
    wrapper, bundle = _materialize_market_bundle(root, shadow_plan)
    outcome = evaluate_shadow_cycle(
        shadow_plan,
        wrapper,
        genesis_shadow_account(shadow_plan),
    )
    assert outcome.shadow_execution_snapshot_sha256 == wrapper.snapshot_sha256
    assert (
        outcome.market_execution_snapshot_sha256
        == wrapper.execution_snapshot.snapshot_sha256
    )
    assert FORMAL_EXECUTION != outcome.market_execution_snapshot_sha256
    payload = {
        "schema_version": 1,
        "outcome_type": "adaptive_shadow_outcome",
        "plan_record_sha256": plan_record_sha,
        **bundle,
        "cycle_outcome": outcome.to_dict(),
    }
    append_shadow_outcome(root, payload, recorded_at_utc="2026-09-25T02:00:00Z")
    assert audit_shadow_store(root)["outcome_count"] == 1

    changed = deepcopy(payload)
    changed["plan_record_sha256"] = "9" * 64
    with pytest.raises(ShadowStoreError, match="no stored plan"):
        append_shadow_outcome(root, changed, recorded_at_utc="2026-09-25T02:00:00Z")


def test_evaluation_is_hashed_three_way_and_never_auto_promotes(tmp_path: Path) -> None:
    root = tmp_path / "shadow"
    _activate(root)
    unsigned = {
        "schema_version": 1,
        "candidate_ids": list(CANDIDATES),
        "conclusion": "continue",
        "automatic_promotion_allowed": False,
    }
    evaluation = {**unsigned, "evaluation_sha256": canonical_sha256(unsigned)}
    append_shadow_evaluation(root, evaluation, recorded_at_utc="2026-10-01T00:00:00Z")
    forged = {**evaluation, "automatic_promotion_allowed": True}
    forged["evaluation_sha256"] = canonical_sha256(
        {key: value for key, value in forged.items() if key != "evaluation_sha256"}
    )
    with pytest.raises(ShadowStoreError, match="auto-promote"):
        append_shadow_evaluation(root, forged, recorded_at_utc="2026-10-01T00:00:00Z")


def test_evaluation_conditional_append_closes_the_checkpoint_race(tmp_path: Path) -> None:
    root = tmp_path / "shadow"
    _activate(root)
    source_head = audit_shadow_store(root)["head_record_sha256"]
    unsigned = {
        "schema_version": 1,
        "candidate_ids": list(CANDIDATES),
        "conclusion": "continue",
        "automatic_promotion_allowed": False,
        "cutoff_date": "2026-10-01",
    }
    evaluation = {**unsigned, "evaluation_sha256": canonical_sha256(unsigned)}
    first = append_shadow_evaluation(
        root,
        evaluation,
        recorded_at_utc="2026-10-01T00:00:00Z",
        expected_previous_record_sha256=source_head,
    )
    replay = append_shadow_evaluation(
        root,
        evaluation,
        recorded_at_utc="2026-10-01T00:00:00Z",
        expected_previous_record_sha256=source_head,
    )
    assert first["created"] is True and replay["created"] is False

    newer_unsigned = {**unsigned, "cutoff_date": "2026-10-02"}
    newer = {
        **newer_unsigned,
        "evaluation_sha256": canonical_sha256(newer_unsigned),
    }
    with pytest.raises(ShadowStoreError, match="head changed"):
        append_shadow_evaluation(
            root,
            newer,
            recorded_at_utc="2026-10-02T00:00:00Z",
            expected_previous_record_sha256=source_head,
        )


def test_thread_concurrency_serializes_a_gap_free_chain(tmp_path: Path) -> None:
    root = tmp_path / "shadow"
    _activate(root)
    _planning, plans = _append_planning(root)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda candidate: append_shadow_plan(
                    root,
                    plans[candidate],
                    recorded_at_utc="2026-09-10T13:00:00Z",
                ),
                CANDIDATES,
            )
        )
    audit = audit_shadow_store(root)
    assert len({result["record_sha256"] for result in results}) == 2
    assert audit["head_sequence"] == 4 and audit["plan_count"] == 2


def test_process_concurrency_uses_the_same_gap_free_lock(tmp_path: Path) -> None:
    root = tmp_path / "shadow"
    _activate(root)
    _planning, plans = _append_planning(root)
    context = multiprocessing.get_context("spawn")
    processes = [
        context.Process(
            target=_process_append,
            args=(str(root), plans[candidate]),
        )
        for candidate in CANDIDATES
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(20)
        assert process.exitcode == 0
    assert audit_shadow_store(root)["head_sequence"] == 4


def test_artifact_first_orphan_is_recovered_idempotently(tmp_path: Path) -> None:
    root = tmp_path / "shadow"
    _activate(root)
    _planning, plans = _append_planning(root)
    plan = plans[CANDIDATES[0]]
    raw = canonical_json_bytes(plan)
    digest = hashlib.sha256(raw).hexdigest()
    (ShadowLayout.at(root).artifacts / f"plan-{digest}.json").write_bytes(raw)
    assert audit_shadow_store(root)["orphan_artifact_count"] == 1
    result = append_shadow_plan(root, plan, recorded_at_utc="2026-09-10T13:00:00Z")
    assert result["recovered_orphan"] is True
    assert audit_shadow_store(root)["orphan_artifact_count"] == 0


@pytest.mark.parametrize("target", ["record", "artifact"])
def test_audit_rejects_tampered_chain_or_artifact(tmp_path: Path, target: str) -> None:
    root = tmp_path / "shadow"
    _activate(root)
    _planning, plans = _append_planning(root)
    stored = append_shadow_plan(
        root,
        plans[CANDIDATES[0]],
        recorded_at_utc="2026-09-10T13:00:00Z",
    )
    path = Path(stored["path"])
    if target == "artifact":
        path = ShadowLayout.at(root).artifacts / stored["record"]["payload_filename"]
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ShadowStoreError):
        audit_shadow_store(root)
