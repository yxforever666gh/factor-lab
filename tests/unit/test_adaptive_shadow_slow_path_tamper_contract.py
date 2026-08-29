from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

import factor_lab.adaptive_shadow_checkpoint as shadow_checkpoint
import factor_lab.adaptive_shadow_controller as shadow_controller
import factor_lab.adaptive_shadow_store as shadow_store
import factor_lab.cli as cli
import factor_lab.data.adaptive_shadow_execution as shadow_execution_data
import factor_lab.data.sources as data_sources
from factor_lab.adaptive_shadow import canonical_sha256
from factor_lab.adaptive_shadow_evidence import CHALLENGER_IDS
from factor_lab.adaptive_shadow_execution import (
    ShadowCyclePlan,
    evaluate_shadow_cycle,
    genesis_shadow_account,
)
from factor_lab.adaptive_shadow_planning import build_registry_from_protocol
from factor_lab.data.adaptive_shadow_execution import (
    build_adaptive_shadow_execution_snapshot,
)
from factor_lab.data.prospective_execution import (
    build_prospective_execution_snapshot,
)
from factor_lab.prospective_execution import (
    SleeveAccountState,
    evaluate_due_sleeve_cycle,
)
from factor_lab.prospective_targets import GenerationResult, InputSnapshot

import test_adaptive_shadow_execution_data as source_fixture


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROTOCOL_PATH = PROJECT_ROOT / "protocols" / "5.9-adaptive-shadow.json"
FORMAL_HEAD_SHA = "1" * 64
FORMAL_DECISION_SHAS = ("2" * 64, "3" * 64)
FORMAL_OUTCOME_SHAS = ("7" * 64, "8" * 64)
RELEASE_OBJECT_OID = "4" * 40
RELEASE_COMMIT_OID = "5" * 40


def _formal_records(
    cycles: tuple[tuple[Any, Any, str], ...],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = [
        {
            "sequence": 1,
            "kind": "activation",
            "record_sha256": FORMAL_HEAD_SHA,
            "record": {"kind": "activation", "payload": {}},
        }
    ]
    for index, (generation, formal_cycle, source_sha) in enumerate(cycles):
        route = generation.to_dict()
        plan = {
            "route_target_plan": route,
            "route_target_plan_sha256": canonical_sha256(route),
            "source_data_snapshot_sha256": source_sha,
            "admission_deadline_utc": f"{generation.trade_date}T01:15:00Z",
        }
        decision_sha = FORMAL_DECISION_SHAS[index]
        records.extend(
            [
                {
                    "sequence": 2 + index * 2,
                    "kind": "decision",
                    "record_sha256": decision_sha,
                    "record": {
                        "kind": "decision",
                        "payload": {
                            "plan": plan,
                            "plan_sha256": canonical_sha256(plan),
                        },
                    },
                },
                {
                    "sequence": 3 + index * 2,
                    "kind": "outcome",
                    "record_sha256": FORMAL_OUTCOME_SHAS[index],
                    "record": {
                        "kind": "outcome",
                        "payload": {
                            "decision_record_sha256": decision_sha,
                            "execution_snapshot_sha256": (
                                formal_cycle.execution_snapshot_sha256
                            ),
                            "cycle_outcome_sha256": formal_cycle.outcome_sha256,
                            "cycle_outcome": formal_cycle.to_dict(),
                        },
                    },
                },
            ]
        )
    return records


def _base_plan(
    *,
    candidate: Any,
    registry_sha256: str,
    generation: Any,
    formal_decision_sha256: str,
    source_data_snapshot_sha256: str,
    targets: tuple[str, ...],
    created_at_utc: str,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "plan_type": "adaptive_shadow_target",
        "candidate_id": candidate.candidate_id,
        "candidate_version": candidate.version,
        "signal_date": generation.signal_date,
        "trade_date": generation.trade_date,
        "offset": generation.due_offset,
        "registry_sha256": registry_sha256,
        "candidate_sha256": candidate.sha256,
        "formal_decision_record_sha256": formal_decision_sha256,
        "formal_route_target_plan_sha256": canonical_sha256(
            generation.to_dict()
        ),
        "formal_input_snapshot_sha256": generation.input_snapshot_sha256,
        "source_data_snapshot_sha256": source_data_snapshot_sha256,
        "shadow_target_rows_sha256": "6" * 64,
        "targets_ppm": {ticker: 100_000 for ticker in targets},
        "cash_ppm": 0,
        "admission_deadline_utc": f"{generation.trade_date}T01:15:00Z",
        "created_at_utc": created_at_utc,
    }


def _second_cycle_source(
    root: Path,
    first_source: Any,
    first_generation: Any,
    sessions: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[str], Any, GenerationResult]:
    extended_sessions = [
        value.date().isoformat()
        for value in pd.bdate_range("2026-07-01", periods=45)
    ]
    assert extended_sessions[: len(sessions)] == sessions
    late_index = first_generation.calendar_index + 10
    late_signal = extended_sessions[late_index]
    late_trade = extended_sessions[late_index + 1]
    late_end = extended_sessions[late_index + 11]

    checkpoint_path = root / "runtime" / "data" / "raw" / "checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    raw_tickers = [*source_fixture.CURRENT, source_fixture.EXITED_PRIOR]
    for global_index in range(first_generation.calendar_index + 12, late_index + 12):
        session = extended_sessions[global_index]
        completion = f"{session}T08:00:00Z"
        raw_day_index = global_index - (first_generation.calendar_index - 20)
        source_fixture._partition(
            root,
            checkpoint,
            "daily",
            session,
            source_fixture._daily(raw_tickers, session, raw_day_index),
            completed_at=completion,
        )
        source_fixture._partition(
            root,
            checkpoint,
            "daily_basic",
            session,
            source_fixture._daily_basic(raw_tickers, session),
            completed_at=completion,
        )
        source_fixture._partition(
            root,
            checkpoint,
            "adj_factor",
            session,
            source_fixture._adj(raw_tickers, session),
            completed_at=completion,
        )

    calendar_sha = source_fixture._calendar_artifact(
        root,
        extended_sessions,
        checkpoint,
        completed_at=f"{late_signal}T06:00:00Z",
    )
    calendar_checkpoint = checkpoint["calendars"][calendar_sha]
    calendar_path = Path(calendar_checkpoint["path"])
    calendar_manifest_path = Path(calendar_checkpoint["manifest_path"])
    execution_data = source_fixture.execution_data
    # This fixture extends the shared market window past the provider-evidence
    # cutover solely to reach the next same-offset cycle.  Provider evidence is
    # orthogonal to the fallback-CAS contract under test; keep the production
    # builders/replayers real while isolating that independent admission gate.
    monkeypatch.setattr(
        execution_data,
        "provider_completion_required",
        lambda _session: False,
    )
    monkeypatch.setattr(
        shadow_execution_data,
        "provider_completion_required",
        lambda _session: False,
    )
    monkeypatch.setattr(
        data_sources,
        "provider_completion_required",
        lambda _session: False,
    )
    _calendar_cas, calendar_binding = execution_data._capture_immutable_artifact(
        root,
        calendar_path,
        expected_sha256=calendar_checkpoint["artifact_sha256"],
        sha_field="artifact_sha256",
    )
    _manifest_cas, calendar_manifest_binding = (
        execution_data._capture_immutable_artifact(
            root,
            calendar_manifest_path,
            expected_sha256=calendar_checkpoint["manifest_sha256"],
            sha_field="manifest_sha256",
            path_field="immutable_manifest_path",
            size_field="manifest_size_bytes",
            media_field="manifest_media_type",
        )
    )
    source_fixture._write_json(checkpoint_path, checkpoint)
    source_fixture._suspensions(
        root,
        start=execution_data.SUSPENSION_FULL_START_DATE,
        end=late_end,
        completed_at=f"{late_end}T08:30:00Z",
    )

    exited_member = source_fixture.TARGETS[-1]
    replacement_member = source_fixture.CURRENT[10]
    target_frame = first_source.target_frame.copy()
    target_frame["date"] = late_signal
    target_frame = target_frame.loc[target_frame["ticker"] != exited_member].copy()
    target_frame.loc[
        target_frame["ticker"] == replacement_member,
        "eligible",
    ] = True
    signal_frame = first_source.frame.copy()
    signal_frame["date"] = late_signal
    signal_frame = signal_frame.loc[signal_frame["ticker"] != exited_member].copy()
    signal_frame.loc[
        signal_frame["ticker"] == replacement_member,
        "eligible",
    ] = True
    ticker_indexes = {
        ticker: index for index, ticker in enumerate(raw_tickers)
    }
    raw_signal_index = late_index - (first_generation.calendar_index - 20)
    signal_frame["close"] = signal_frame["ticker"].map(
        lambda ticker: 101.0 + ticker_indexes[str(ticker)] * 20.0 + raw_signal_index
    )
    signal_frame["close_adj"] = signal_frame["close"] * 2.0

    late_source_sha = "9" * 64
    late_directory = root / "runtime" / "prospective" / "5.0" / "inputs" / late_source_sha
    late_directory.mkdir(parents=True)
    manifest = deepcopy(first_source.manifest)
    manifest["calendar"] = {
        "sources": [
            {
                "calendar_content_sha256": calendar_sha,
                "path": execution_data._relative(calendar_path, root),
                **calendar_binding,
                "manifest_path": execution_data._relative(
                    calendar_manifest_path,
                    root,
                ),
                **calendar_manifest_binding,
                "completed_at_utc": f"{late_signal}T06:00:00Z",
                "availability_basis": "checkpoint_completed_at_utc",
                "source_start_date": extended_sessions[0],
                "source_end_date": extended_sessions[-1],
                "row_count": int(calendar_checkpoint["row_count"]),
                "open_day_count": int(calendar_checkpoint["open_day_count"]),
            }
        ]
    }
    late_source = source_fixture._SignalSource(
        signal_date=late_signal,
        trade_date=late_trade,
        snapshot_sha256=late_source_sha,
        directory=late_directory,
        build_completed_at_utc=f"{late_signal}T08:30:00Z",
        inputs_available_at_utc=f"{late_signal}T08:00:00Z",
        frame=signal_frame,
        manifest=manifest,
        calendar_sessions=tuple(extended_sessions[: late_index + 2]),
        target_frame=target_frame,
        target_rows_sha256="a" * 64,
        input_sources_sha256="b" * 64,
        membership_artifact_sha256="c" * 64,
    )
    skipped = tuple(
        extended_sessions[first_generation.calendar_index + 1 : late_index]
    )
    late_input = InputSnapshot(
        signal_date=late_signal,
        calendar_sessions=extended_sessions[: late_index + 2],
        skipped_sessions=skipped,
        rows=target_frame,
        source_data_snapshot_sha256=late_source.snapshot_sha256,
        target_rows_sha256=late_source.target_rows_sha256,
        input_sources_sha256=late_source.input_sources_sha256,
        membership_artifact_sha256=late_source.membership_artifact_sha256,
        source_build_checkpoint_utc=late_source.build_completed_at_utc,
        max_available_at_utc=late_source.inputs_available_at_utc,
        information_cutoff_utc=late_source.build_completed_at_utc,
        signal_close_utc=f"{late_signal}T07:00:00Z",
        admission_deadline_utc=f"{late_trade}T01:15:00Z",
    )
    seed = source_fixture._generation(
        late_input.snapshot_sha256,
        extended_sessions,
        late_index,
    )
    sleeve_plans = [dict(value) for value in seed.sleeve_plans]
    sleeve_plans[seed.due_offset]["action"] = "rebalance"
    late_generation = GenerationResult(
        deployment_sha256=seed.deployment_sha256,
        input_snapshot_sha256=seed.input_snapshot_sha256,
        previous_state_sha256=first_generation.next_state.state_sha256,
        signal_date=seed.signal_date,
        trade_date=seed.trade_date,
        calendar_index=seed.calendar_index,
        due_offset=seed.due_offset,
        skipped_sessions=skipped,
        sleeve_plans=sleeve_plans,
        aggregate_targets_ppm=seed.aggregate_targets_ppm,
        aggregate_cash_ppm=seed.aggregate_cash_ppm,
        next_state=seed.next_state,
    )
    sources = {
        first_source.snapshot_sha256: first_source,
        late_source.snapshot_sha256: late_source,
    }
    monkeypatch.setattr(
        execution_data,
        "load_prospective_input_snapshot",
        lambda path: sources[Path(path).name],
    )
    return extended_sessions, late_source, late_generation


def _write_real_slow_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path, list[dict[str, Any]], Path]:
    (
        root,
        generation,
        source,
        sessions,
        formal,
        _unused_old_candidate_plan,
    ) = source_fixture.source_backed_market.__wrapped__(tmp_path, monkeypatch)
    formal_root = root / "runtime" / "prospective" / "5.0"
    shadow_root = root / "runtime" / "adaptive-shadow" / "1"

    first_previous_formal = SleeveAccountState.genesis(
        deployment_sha256=generation.deployment_sha256,
        offset=generation.due_offset,
    )
    # The shared market fixture also emits a legacy no-account formal bundle.
    # Rebuild it with the genesis account binding used by the controller's
    # independent formal replay contract.
    formal = build_prospective_execution_snapshot(
        root,
        generation,
        source_data_snapshot_sha256=source.snapshot_sha256,
        previous_account_state=first_previous_formal,
    )
    first_formal_cycle = evaluate_due_sleeve_cycle(
        generation_result=generation,
        execution_snapshot=formal.snapshot,
        previous_account_state=first_previous_formal,
    )
    extended_sessions, late_source, late_generation = _second_cycle_source(
        root,
        source,
        generation,
        sessions,
        monkeypatch,
    )
    late_formal = build_prospective_execution_snapshot(
        root,
        late_generation,
        source_data_snapshot_sha256=late_source.snapshot_sha256,
        previous_account_state=first_formal_cycle.next_account_state,
    )
    late_formal_cycle = evaluate_due_sleeve_cycle(
        generation_result=late_generation,
        execution_snapshot=late_formal.snapshot,
        previous_account_state=first_formal_cycle.next_account_state,
    )
    records = _formal_records(
        (
            (generation, first_formal_cycle, source.snapshot_sha256),
            (late_generation, late_formal_cycle, late_source.snapshot_sha256),
        )
    )

    protocol_raw = PROTOCOL_PATH.read_bytes()
    protocol = json.loads(protocol_raw)
    released_at_utc = f"{sessions[0]}T00:00:00Z"
    registry = build_registry_from_protocol(
        protocol,
        release_tag="5.9",
        commit_oid=RELEASE_COMMIT_OID,
        released_at_utc=released_at_utc,
        start_after=sessions[0],
    )
    shadow_store.activate_shadow_store(
        shadow_root,
        registry=registry,
        release_tag_object_oid=RELEASE_OBJECT_OID,
        release_commit_oid=RELEASE_COMMIT_OID,
        protocol_sha256=hashlib.sha256(protocol_raw).hexdigest(),
        formal_head_record_sha256=FORMAL_HEAD_SHA,
        released_at_utc=released_at_utc,
        start_after=sessions[0],
        recorded_at_utc=released_at_utc,
    )

    candidate_id = CHALLENGER_IDS[0]
    first_targets = source_fixture.TARGETS
    replacement_targets = (
        *source_fixture.TARGETS[:9],
        source_fixture.CURRENT[10],
    )
    cycle_inputs = (
        (
            generation,
            source,
            formal,
            first_previous_formal,
            FORMAL_DECISION_SHAS[0],
            first_targets,
        ),
        (
            late_generation,
            late_source,
            late_formal,
            first_formal_cycle.next_account_state,
            FORMAL_DECISION_SHAS[1],
            replacement_targets,
        ),
    )
    previous_shadow: SleeveAccountState | None = None
    slow_built: Any = None
    for cycle_index, (
        cycle_generation,
        cycle_source,
        cycle_formal,
        cycle_previous_formal,
        decision_sha,
        primary_targets,
    ) in enumerate(cycle_inputs):
        created_at_utc = f"{cycle_generation.signal_date}T13:00:00Z"
        target_by_candidate = {
            registered_id: tuple(primary_targets)
            for registered_id in CHALLENGER_IDS
        }
        base_plans = [
            _base_plan(
                candidate=registry.candidate(registered_id),
                registry_sha256=registry.sha256,
                generation=cycle_generation,
                formal_decision_sha256=decision_sha,
                source_data_snapshot_sha256=cycle_source.snapshot_sha256,
                targets=tuple(target_by_candidate[registered_id]),
                created_at_utc=created_at_utc,
            )
            for registered_id in CHALLENGER_IDS
        ]
        planning_payload = {
            "schema_version": 1,
            "planning_type": "adaptive_shadow_planning_intent",
            "registry_sha256": registry.sha256,
            "formal_decision_record_sha256": decision_sha,
            "signal_date": cycle_generation.signal_date,
            "trade_date": cycle_generation.trade_date,
            "offset": cycle_generation.due_offset,
            "admission_deadline_utc": f"{cycle_generation.trade_date}T01:15:00Z",
            "created_at_utc": created_at_utc,
            "ordered_plan_payloads": base_plans,
        }
        planning = shadow_store.append_shadow_planning(
            shadow_root,
            planning_payload,
            recorded_at_utc=created_at_utc,
        )
        appended_plans: dict[str, dict[str, Any]] = {}
        for base in base_plans:
            payload = {
                **base,
                "planning_record_sha256": planning["record_sha256"],
                "planning_payload_sha256": canonical_sha256(planning_payload),
            }
            appended_plans[str(base["candidate_id"])] = (
                shadow_store.append_shadow_plan(
                    shadow_root,
                    payload,
                    recorded_at_utc=created_at_utc,
                )
            )

        plan_record = appended_plans[candidate_id]
        plan_payload = dict(plan_record["payload"])
        execution_plan = ShadowCyclePlan(
            registry_sha256=registry.sha256,
            candidate_id=candidate_id,
            candidate_sha256=registry.candidate(candidate_id).sha256,
            offset=cycle_generation.due_offset,
            signal_date=cycle_generation.signal_date,
            trade_date=cycle_generation.trade_date,
            targets_ppm=plan_payload["targets_ppm"],
            formal_input_snapshot_sha256=cycle_generation.input_snapshot_sha256,
            formal_decision_record_sha256=decision_sha,
            planned_at_utc=created_at_utc,
            formal_trade_deadline_utc=plan_payload["admission_deadline_utc"],
        )
        if previous_shadow is None:
            previous_shadow = genesis_shadow_account(execution_plan)
        bindings = {
            "plan_record_sha256": plan_record["record_sha256"],
            "source_data_snapshot_sha256": cycle_source.snapshot_sha256,
            "shadow_target_rows_sha256": plan_payload["shadow_target_rows_sha256"],
            "formal_route_target_plan_sha256": plan_payload[
                "formal_route_target_plan_sha256"
            ],
        }
        built = build_adaptive_shadow_execution_snapshot(
            root,
            cycle_generation,
            cycle_formal,
            execution_plan,
            previous_shadow,
            plan_bindings=bindings,
            previous_formal_account_state=cycle_previous_formal,
        )
        if cycle_index == 0:
            assert built.source_contract["mode"] == "formal_snapshot_reuse"
            assert built.source_contract["fallback_raw_partitions"] == []
        else:
            assert built.source_contract["mode"] == "supplemented_prior_holdings"
            assert built.source_contract["fallback_raw_partitions"]
            slow_built = built
        outcome = evaluate_shadow_cycle(execution_plan, built.snapshot, previous_shadow)
        shadow_store.append_shadow_outcome(
            shadow_root,
            {
                "schema_version": 1,
                "outcome_type": "adaptive_shadow_outcome",
                "plan_record_sha256": plan_record["record_sha256"],
                "formal_execution_snapshot_sha256": (
                    cycle_formal.snapshot.snapshot_sha256
                ),
                "shadow_market_source_contract_sha256": built.source_contract_sha256,
                "shadow_market_bundle_sha256": built.bundle_sha256,
                "cycle_outcome": outcome.to_dict(),
            },
            recorded_at_utc=outcome.observation_available_at_utc,
        )
        previous_shadow = outcome.next_account_state

    assert slow_built is not None
    fallback = slow_built.source_contract["fallback_raw_partitions"][0]
    fallback_path = root / str(fallback["immutable_path"])
    assert fallback_path.is_file()
    return formal_root, shadow_root, records, fallback_path


@pytest.mark.parametrize("mutation", ["tamper", "delete"])
def test_slow_path_fallback_cas_damage_fails_closed_at_every_public_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    mutation: str,
) -> None:
    formal_root, shadow_root, records, fallback_path = _write_real_slow_outcome(
        tmp_path,
        monkeypatch,
    )
    monkeypatch.setattr(
        shadow_controller,
        "_audited_formal_records",
        lambda _root: (records, FORMAL_OUTCOME_SHAS[-1]),
    )

    baseline = shadow_controller.audit_adaptive_shadow_runtime(
        tmp_path,
        formal_root,
        shadow_root,
    )
    assert baseline["valid"] is True
    assert baseline["deep_replayed_outcome_count"] == 2
    records_before = {
        path.name: path.read_bytes()
        for path in (shadow_root / "records").iterdir()
    }
    if mutation == "tamper":
        fallback_path.write_bytes(b"tampered fallback raw CAS")
    else:
        fallback_path.unlink()
    shadow_tree_before_calls = source_fixture._tree_digest(shadow_root)
    formal_tree_before_calls = source_fixture._tree_digest(formal_root)

    audit_exit = cli.main(["--root", str(tmp_path), "adaptive-shadow", "audit"])
    audit_report = json.loads(capsys.readouterr().out)
    assert audit_exit == 1
    assert audit_report["status"] == "invalid"
    assert audit_report["integrity_valid"] is False

    sync_exit = cli.main(["--root", str(tmp_path), "adaptive-shadow", "sync"])
    sync_report = json.loads(capsys.readouterr().out)
    assert sync_exit == 3
    assert sync_report["status"] == "blocked"
    assert sync_report["reason"] == "shadow_controller_error"

    with pytest.raises(
        shadow_checkpoint.AdaptiveShadowCheckpointError,
        match="audited checkpoint read|external replay",
    ):
        shadow_checkpoint.checkpoint_adaptive_shadow_evaluation(
            tmp_path,
            formal_root,
            shadow_root,
            observed_at_utc="2026-08-30T00:00:00Z",
        )

    assert {
        path.name: path.read_bytes()
        for path in (shadow_root / "records").iterdir()
    } == records_before
    assert source_fixture._tree_digest(shadow_root) == shadow_tree_before_calls
    assert source_fixture._tree_digest(formal_root) == formal_tree_before_calls
