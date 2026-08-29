from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import factor_lab.adaptive_shadow_runtime as runtime
from factor_lab import adaptive_shadow_store as shadow_store


FORMAL_HEAD = "f" * 64
TAG_OBJECT = "a" * 40
TAG_COMMIT = "b" * 40
SOURCE_SHA = "1" * 64
FORMAL_INPUT_SHA = "2" * 64
FORMAL_DECISION = "3" * 64
INITIAL_CREATED_AT = "2026-09-10T12:00:00Z"
ADMISSION_DEADLINE = "2026-09-11T01:15:00Z"
FIRST_RETRY_AT = "2026-09-11T02:00:00Z"
SECOND_RETRY_AT = "2026-09-11T03:00:00Z"
PLAN_INTENT_BINDING_KEYS = {
    "planning_record_sha256",
    "planning_payload_sha256",
}


class SimulatedPlanningCrash(RuntimeError):
    pass


def _protocol_bytes() -> bytes:
    return (
        Path(__file__).resolve().parents[2]
        / "protocols/5.9-adaptive-shadow.json"
    ).read_bytes()


def _formal_plan() -> dict[str, object]:
    route = {
        "route": "fixed_core_full",
        "input_snapshot_sha256": FORMAL_INPUT_SHA,
        "signal_date": "2026-09-10",
        "trade_date": "2026-09-11",
        "calendar_index": 20,
        "due_offset": 0,
    }
    return {
        "decision_session": "2026-09-11",
        "source_data_snapshot_sha256": SOURCE_SHA,
        "admission_deadline_utc": ADMISSION_DEADLINE,
        "route_target_plan": route,
        "route_target_plan_sha256": runtime.canonical_sha256(route),
    }


def _planning_input(candidate_ids: tuple[str, ...]) -> SimpleNamespace:
    rows: list[dict[str, object]] = []
    for index in range(16):
        row: dict[str, object] = {
            "date": "2026-09-10",
            "ticker": f"T{index:03d}",
            "shadow_eligible": True,
        }
        for candidate_index, candidate_id in enumerate(candidate_ids):
            row[candidate_id] = float(
                100 - index if candidate_index % 2 == 0 else index
            )
        rows.append(row)
    return SimpleNamespace(
        signal_date="2026-09-10",
        trade_date="2026-09-11",
        snapshot_sha256=SOURCE_SHA,
        shadow_target_rows_sha256="4" * 64,
        shadow_target_frame=rows,
    )


def _activate_real_store(
    shadow_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[str, ...]:
    raw_protocol = _protocol_bytes()
    protocol = json.loads(raw_protocol)
    protocol_sha256 = hashlib.sha256(raw_protocol).hexdigest()
    registry = runtime.build_registry_from_protocol(
        protocol,
        release_tag="5.9",
        commit_oid=TAG_COMMIT,
        released_at_utc="2026-09-01T00:00:00Z",
        start_after="2026-09-01",
    )
    runtime.activate_shadow_store(
        shadow_root,
        registry=registry,
        release_tag_object_oid=TAG_OBJECT,
        release_commit_oid=TAG_COMMIT,
        protocol_sha256=protocol_sha256,
        formal_head_record_sha256=FORMAL_HEAD,
        released_at_utc="2026-09-01T00:00:00Z",
        start_after="2026-09-01",
        recorded_at_utc="2026-09-01T00:00:01Z",
    )
    candidate_ids = tuple(candidate.candidate_id for candidate in registry.candidates)
    monkeypatch.setattr(
        runtime,
        "load_release_bound_protocol",
        lambda *args, **kwargs: (
            protocol,
            protocol_sha256,
            "protocols/5.9-adaptive-shadow.json",
        ),
    )
    monkeypatch.setattr(
        runtime,
        "_load_formal_decision_plan",
        lambda *args, **kwargs: (
            _formal_plan(),
            {
                "activation_head_record_sha256": FORMAL_HEAD,
                "decision_record_sha256": FORMAL_DECISION,
            },
        ),
    )
    monkeypatch.setattr(
        runtime,
        "load_prospective_input_snapshot",
        lambda _path: _planning_input(candidate_ids),
    )
    return candidate_ids


def _plan(shadow_root: Path, project_root: Path, created_at_utc: str) -> dict[str, Any]:
    return runtime.plan_shadow_runtime(
        project_root,
        shadow_root,
        project_root / "runtime/prospective/5.0",
        formal_plan_path=project_root / "formal-plan.json",
        formal_decision_record_sha256=FORMAL_DECISION,
        input_snapshot_path=project_root / SOURCE_SHA,
        created_at_utc=created_at_utc,
    )


def _validated_records(shadow_root: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    audit = shadow_store.audit_shadow_store(shadow_root)
    assert audit["integrity_valid"] is True
    records, _state = runtime._shadow_evidence_snapshot(shadow_root)
    return records, audit


def _records_of_kind(
    records: list[dict[str, Any]], kind: str
) -> list[dict[str, Any]]:
    return [row for row in records if row["record"]["kind"] == kind]


def _plan_base(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in payload.items()
        if key not in PLAN_INTENT_BINDING_KEYS
    }


@pytest.mark.parametrize(
    ("crash_point", "plans_after_crash"),
    [
        ("after_intent", 0),
        ("after_first_plan", 1),
    ],
)
def test_sealed_planning_intent_recovers_exact_remaining_plans_after_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_point: str,
    plans_after_crash: int,
) -> None:
    shadow_root = tmp_path / crash_point / "runtime/adaptive-shadow/1"
    candidate_ids = _activate_real_store(shadow_root, monkeypatch)
    real_append_planning = runtime.append_shadow_planning
    real_append_plan = runtime.append_shadow_plan

    if crash_point == "after_intent":

        def append_intent_then_crash(
            root: Any,
            payload: Mapping[str, Any],
            *,
            recorded_at_utc: Any,
        ) -> dict[str, Any]:
            real_append_planning(
                root,
                payload,
                recorded_at_utc=recorded_at_utc,
            )
            raise SimulatedPlanningCrash("crash after durable planning intent")

        monkeypatch.setattr(
            runtime,
            "append_shadow_planning",
            append_intent_then_crash,
        )
    else:
        plan_append_count = 0

        def append_first_plan_then_crash(
            root: Any,
            payload: Mapping[str, Any],
            *,
            recorded_at_utc: Any,
        ) -> dict[str, Any]:
            nonlocal plan_append_count
            result = real_append_plan(
                root,
                payload,
                recorded_at_utc=recorded_at_utc,
            )
            plan_append_count += 1
            if plan_append_count == 1:
                raise SimulatedPlanningCrash("crash after first durable plan")
            return result

        monkeypatch.setattr(
            runtime,
            "append_shadow_plan",
            append_first_plan_then_crash,
        )

    with pytest.raises(SimulatedPlanningCrash):
        _plan(shadow_root, tmp_path, INITIAL_CREATED_AT)

    crashed_records, crashed_audit = _validated_records(shadow_root)
    planning_records = _records_of_kind(crashed_records, "planning")
    crashed_plan_records = _records_of_kind(crashed_records, "plan")
    assert crashed_audit["planning_intent_count"] == 1
    assert crashed_audit["plan_count"] == plans_after_crash
    assert crashed_audit["missed_count"] == 0
    assert len(planning_records) == 1
    assert len(crashed_plan_records) == plans_after_crash

    planning_record = planning_records[0]
    sealed_plans = planning_record["payload"]["ordered_plan_payloads"]
    assert tuple(plan["candidate_id"] for plan in sealed_plans) == candidate_ids
    assert all(plan["created_at_utc"] == INITIAL_CREATED_AT for plan in sealed_plans)
    sealed_base_bytes = {
        plan["candidate_id"]: runtime.canonical_json_bytes(plan)
        for plan in sealed_plans
    }
    crashed_plan_bytes = {
        row["payload"]["candidate_id"]: runtime.canonical_json_bytes(
            row["payload"]
        )
        for row in crashed_plan_records
    }
    crashed_plan_record_shas = {
        row["payload"]["candidate_id"]: row["record_sha256"]
        for row in crashed_plan_records
    }

    monkeypatch.setattr(runtime, "append_shadow_planning", real_append_planning)
    monkeypatch.setattr(runtime, "append_shadow_plan", real_append_plan)
    recovered = _plan(shadow_root, tmp_path, FIRST_RETRY_AT)

    assert FIRST_RETRY_AT > ADMISSION_DEADLINE
    assert recovered["status"] == "planned"
    assert recovered["missed"] == []
    assert recovered["planning_intent"]["created"] is False
    recovered_by_candidate = {
        row["payload"]["candidate_id"]: row for row in recovered["plans"]
    }
    assert set(recovered_by_candidate) == set(candidate_ids)
    if crash_point == "after_intent":
        assert all(row["created"] is True for row in recovered_by_candidate.values())
    else:
        assert recovered_by_candidate[candidate_ids[0]]["created"] is False
        assert recovered_by_candidate[candidate_ids[1]]["created"] is True

    final_records, final_audit = _validated_records(shadow_root)
    final_planning_records = _records_of_kind(final_records, "planning")
    final_plan_records = _records_of_kind(final_records, "plan")
    assert final_audit["planning_intent_count"] == 1
    assert final_audit["plan_count"] == len(candidate_ids)
    assert final_audit["missed_count"] == 0
    assert len(final_planning_records) == 1
    assert len(final_plan_records) == len(candidate_ids)

    final_by_candidate: dict[str, dict[str, Any]] = {}
    for row in final_plan_records:
        candidate_id = row["payload"]["candidate_id"]
        assert candidate_id not in final_by_candidate
        final_by_candidate[candidate_id] = row
    assert set(final_by_candidate) == set(candidate_ids)
    for candidate_id in candidate_ids:
        full_payload = final_by_candidate[candidate_id]["payload"]
        base_payload = _plan_base(full_payload)
        assert runtime.canonical_json_bytes(base_payload) == sealed_base_bytes[candidate_id]
        assert base_payload["created_at_utc"] == INITIAL_CREATED_AT
        assert full_payload["planning_record_sha256"] == planning_record["record_sha256"]
        assert full_payload["planning_payload_sha256"] == planning_record["record"][
            "payload_sha256"
        ]
    for candidate_id, original_bytes in crashed_plan_bytes.items():
        assert (
            runtime.canonical_json_bytes(final_by_candidate[candidate_id]["payload"])
            == original_bytes
        )
        assert (
            final_by_candidate[candidate_id]["record_sha256"]
            == crashed_plan_record_shas[candidate_id]
        )

    head_after_recovery = final_audit["head_record_sha256"]
    replayed = _plan(shadow_root, tmp_path, SECOND_RETRY_AT)
    replay_records, replay_audit = _validated_records(shadow_root)
    assert replayed["status"] == "planned"
    assert replayed["missed"] == []
    assert replayed["planning_intent"]["created"] is False
    assert all(row["created"] is False for row in replayed["plans"])
    assert replay_audit["head_record_sha256"] == head_after_recovery
    assert replay_audit["planning_intent_count"] == 1
    assert replay_audit["plan_count"] == len(candidate_ids)
    assert replay_audit["missed_count"] == 0
    assert len(_records_of_kind(replay_records, "planning")) == 1
    assert len(_records_of_kind(replay_records, "plan")) == len(candidate_ids)
