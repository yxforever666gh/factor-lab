from __future__ import annotations

import base64
from copy import deepcopy
from contextlib import contextmanager
from datetime import datetime, time as wall_time, timezone
from functools import lru_cache
import multiprocessing
from pathlib import Path
import sys
from types import SimpleNamespace
from typing import Any, Mapping

import pandas as pd
import pytest

import factor_lab.data.prospective as prospective_data
import factor_lab.data.prospective_execution as prospective_execution_data
import factor_lab.implementation_closure as implementation_closure
import factor_lab.prospective_ledger as prospective_ledger
from factor_lab.prospective_ledger import (
    CanonicalJSONError,
    LedgerIntegrityError,
    LedgerStateError,
    abandon_implementation_upgrade,
    activate_protocol,
    append_attestation_receipt,
    append_correction,
    append_implementation_upgrade,
    append_outcome,
    audit_ledger,
    build_decision_plan,
    build_execution_evidence,
    build_outcome_payload,
    canonical_json_bytes,
    checkpoint_evaluation,
    create_only_file,
    evaluate_ledger,
    ledger_status,
    seal_decision,
    seal_snapshot,
    sha256_bytes,
    sha256_file,
    store_decision_plan,
    strict_load_canonical,
)
from factor_lab.data.prospective import (
    CANONICAL_CALENDAR_ANCHOR,
    CANONICAL_CALENDAR_COUNT,
    CANONICAL_CALENDAR_SHA256,
    FROZEN_BRIDGE_END,
    ProspectiveDataError,
    load_prospective_input_snapshot,
)
from factor_lab.prospective_targets import (
    DeploymentSpec,
    GenerationResult,
    InputSnapshot,
    TenSleeveState,
    calendar_prefix_sha256,
    generate_fixed_core_targets,
)
from factor_lab.prospective_execution import (
    ExecutionObservation,
    ExecutionSnapshot,
    SleeveAccountState,
    evaluate_due_sleeve_cycle,
)
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[2]
PROTOCOL = ROOT / "protocols" / "5.0.json"
TAG_OID = "a" * 40
COMMIT_OID = "b" * 40
IMPLEMENTATION_TAG_OID = "7" * 40
IMPLEMENTATION_COMMIT_OID = "8" * 40
RUN_ID = "c" * 16
AUTHORITATIVE_RUN = {
    "authoritative_run_id": RUN_ID,
    "run_fingerprint": RUN_ID + "d" * 48,
    "manifest_sha256": "e" * 64,
    "manifest_self_sha256": "f" * 64,
    "adaptive_summary_sha256": "1" * 64,
    "frozen_route": "fixed_core_full",
    "integrity_valid": True,
}
# Keep the ledger unit suite hermetic.  Production derives this calendar from
# the official execution store, but that gitignored dataset is intentionally
# absent on clean CI runners.  A compact list of non-trading weekdays is enough
# to reconstruct and re-hash the exact frozen 2,340-session prefix here.
_CANONICAL_WEEKDAY_CLOSURES = frozenset(
    """
    2017-01-27 2017-01-30 2017-01-31 2017-02-01 2017-02-02 2017-04-03 2017-04-04
    2017-05-01 2017-05-29 2017-05-30 2017-10-02 2017-10-03 2017-10-04 2017-10-05
    2017-10-06 2018-01-01 2018-02-15 2018-02-16 2018-02-19 2018-02-20 2018-02-21
    2018-04-05 2018-04-06 2018-04-30 2018-05-01 2018-06-18 2018-09-24 2018-10-01
    2018-10-02 2018-10-03 2018-10-04 2018-10-05 2018-12-31 2019-01-01 2019-02-04
    2019-02-05 2019-02-06 2019-02-07 2019-02-08 2019-04-05 2019-05-01 2019-05-02
    2019-05-03 2019-06-07 2019-09-13 2019-10-01 2019-10-02 2019-10-03 2019-10-04
    2019-10-07 2020-01-01 2020-01-24 2020-01-27 2020-01-28 2020-01-29 2020-01-30
    2020-01-31 2020-04-06 2020-05-01 2020-05-04 2020-05-05 2020-06-25 2020-06-26
    2020-10-01 2020-10-02 2020-10-05 2020-10-06 2020-10-07 2020-10-08 2021-01-01
    2021-02-11 2021-02-12 2021-02-15 2021-02-16 2021-02-17 2021-04-05 2021-05-03
    2021-05-04 2021-05-05 2021-06-14 2021-09-20 2021-09-21 2021-10-01 2021-10-04
    2021-10-05 2021-10-06 2021-10-07 2022-01-03 2022-01-31 2022-02-01 2022-02-02
    2022-02-03 2022-02-04 2022-04-04 2022-04-05 2022-05-02 2022-05-03 2022-05-04
    2022-06-03 2022-09-12 2022-10-03 2022-10-04 2022-10-05 2022-10-06 2022-10-07
    2023-01-02 2023-01-23 2023-01-24 2023-01-25 2023-01-26 2023-01-27 2023-04-05
    2023-05-01 2023-05-02 2023-05-03 2023-06-22 2023-06-23 2023-09-29 2023-10-02
    2023-10-03 2023-10-04 2023-10-05 2023-10-06 2024-01-01 2024-02-09 2024-02-12
    2024-02-13 2024-02-14 2024-02-15 2024-02-16 2024-04-04 2024-04-05 2024-05-01
    2024-05-02 2024-05-03 2024-06-10 2024-09-16 2024-09-17 2024-10-01 2024-10-02
    2024-10-03 2024-10-04 2024-10-07 2025-01-01 2025-01-28 2025-01-29 2025-01-30
    2025-01-31 2025-02-03 2025-02-04 2025-04-04 2025-05-01 2025-05-02 2025-05-05
    2025-06-02 2025-10-01 2025-10-02 2025-10-03 2025-10-06 2025-10-07 2025-10-08
    2026-01-01 2026-01-02 2026-02-16 2026-02-17 2026-02-18 2026-02-19 2026-02-20
    2026-02-23 2026-04-06 2026-05-01 2026-05-04 2026-05-05 2026-06-19
    """.split()
)
STRICT_SOURCE_LOADER = load_prospective_input_snapshot


@pytest.fixture(autouse=True)
def _isolate_ledger_from_expensive_source_rebuild(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unit tests exercise ledger logic with byte-verified synthetic bundles.

    The public loader's independent source rebuild is tested separately below;
    production ledger code still imports that strict public loader.
    """

    monkeypatch.setattr(
        prospective_data,
        "load_prospective_input_snapshot",
        prospective_data._load_prospective_input_snapshot_files,
    )
    monkeypatch.setattr(
        prospective_execution_data,
        "load_prospective_execution_snapshot",
        _load_test_execution_snapshot,
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
        _run_test_release_operation_wrapped,
    )


def _run_test_release_operation_wrapped(
    layout: Any,
    state: Any,
    operation: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        return _run_test_release_operation(layout, state, operation, payload)
    except LedgerStateError:
        raise
    except Exception as exc:
        raise LedgerStateError(
            f"published prospective release operation failed: {operation}"
        ) from exc


def _run_test_release_operation(
    layout: Any,
    _state: Any,
    operation: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Cheap released-code boundary for ledger unit tests only."""

    if operation == "replay_history":
        results = []
        for item in payload["operations"]:
            results.append(
                {
                    "operation_id": item["operation_id"],
                    "operation": item["operation"],
                    "result": _run_test_release_operation(
                        layout, _state, item["operation"], item["payload"]
                    ),
                }
            )
        return {"results": results}

    if operation == "replay_target":
        source_sha = str(payload["source_data_snapshot_sha256"])
        source = prospective_data.load_prospective_input_snapshot(
            layout.inputs / source_sha
        )
        bindings = payload["deployment_bindings"]
        deployment = DeploymentSpec(
            calendar_anchor=CANONICAL_CALENDAR_ANCHOR,
            calendar_prefix_count=CANONICAL_CALENDAR_COUNT,
            calendar_prefix_last_session=FROZEN_BRIDGE_END.date().isoformat(),
            calendar_prefix_sha256=CANONICAL_CALENDAR_SHA256,
            activation_record_sha256=bindings["activation_record_sha256"],
            implementation_upgrade_record_sha256=bindings[
                "implementation_upgrade_record_sha256"
            ],
            deployment_protocol_sha256=bindings["deployment_protocol_sha256"],
        )
        previous = (
            TenSleeveState.genesis(deployment)
            if payload["previous_state"] is None
            else TenSleeveState.from_mapping(payload["previous_state"])
        )
        sessions = list(source.calendar_sessions)
        signal_index = sessions.index(source.signal_date)
        skipped = sessions[previous.last_processed_calendar_index + 1 : signal_index]
        trade_session = datetime.fromisoformat(source.trade_date).date()
        deadline = datetime.combine(
            trade_session,
            wall_time(hour=9, minute=15),
            tzinfo=ZoneInfo("Asia/Shanghai"),
        ).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        submitted = payload["admission_deadline_utc"]
        if submitted is not None and submitted != deadline:
            raise ValueError("admission deadline differs")
        snapshot = InputSnapshot(
            signal_date=source.signal_date,
            calendar_sessions=sessions,
            skipped_sessions=skipped,
            rows=source.target_frame,
            source_data_snapshot_sha256=source.snapshot_sha256,
            target_rows_sha256=source.target_rows_sha256,
            input_sources_sha256=source.input_sources_sha256,
            membership_artifact_sha256=source.membership_artifact_sha256,
            source_build_checkpoint_utc=source.inputs_available_at_utc,
            max_available_at_utc=source.inputs_available_at_utc,
            information_cutoff_utc=source.inputs_available_at_utc,
            signal_close_utc=f"{source.signal_date}T07:00:00Z",
            admission_deadline_utc=deadline,
        )
        generated = generate_fixed_core_targets(
            deployment=deployment,
            input_snapshot=snapshot,
            previous_state=previous,
        )
        return {
            "generation_result": generated.to_dict(),
            "deployment": deployment.to_dict(),
            "input_snapshot": snapshot.to_dict(),
        }
    if operation == "replay_outcome":
        generation = GenerationResult.from_mapping(payload["generation_result"])
        previous = (
            SleeveAccountState.genesis(
                deployment_sha256=generation.deployment_sha256,
                offset=generation.due_offset,
            )
            if payload["previous_account_state"] is None
            else SleeveAccountState.from_mapping(payload["previous_account_state"])
        )
        loaded = _load_test_execution_snapshot(
            layout.executions / str(payload["execution_snapshot_sha256"]),
            generation,
            previous_account_state=(
                None if payload["previous_account_state"] is None else previous
            ),
        )
        outcome = evaluate_due_sleeve_cycle(
            generation_result=generation,
            execution_snapshot=loaded.snapshot,
            previous_account_state=previous,
        )
        return {"cycle_outcome": outcome.to_dict()}
    if operation == "evaluate":
        from factor_lab.prospective_evaluation import (
            EVALUATION_CONTRACT_SHA256,
            EVALUATOR_ID,
            evaluate_prospective_outcomes,
        )

        assert payload["evaluator_id"] == EVALUATOR_ID
        assert payload["evaluation_contract_sha256"] == EVALUATION_CONTRACT_SHA256
        outcomes = list(payload["outcomes"])
        binding = {
            "evaluator_id": EVALUATOR_ID,
            "evaluation_contract_sha256": EVALUATION_CONTRACT_SHA256,
            "ledger_id": payload["ledger_id"],
            "ledger_head_record_sha256": payload["ledger_head_record_sha256"],
            "implementation_upgrade_record_sha256": payload[
                "implementation_upgrade_record_sha256"
            ],
            "outcome_count": len(outcomes),
            "outcomes_sha256": sha256_bytes(canonical_json_bytes(outcomes)),
        }
        envelope = {
            "schema_version": 1,
            "binding": binding,
            "evaluation": evaluate_prospective_outcomes(outcomes),
        }
        return {
            "evaluation_envelope": {
                **envelope,
                "evaluation_envelope_sha256": sha256_bytes(
                    canonical_json_bytes(envelope)
                ),
            }
        }
    if operation == "build_execution":
        generation = GenerationResult.from_mapping(payload["generation_result"])
        previous = (
            None
            if payload["previous_account_state"] is None
            else SleeveAccountState.from_mapping(payload["previous_account_state"])
        )
        built = prospective_execution_data.build_prospective_execution_snapshot(
            Path(payload["project_root"]),
            generation,
            source_data_snapshot_sha256=payload["source_data_snapshot_sha256"],
            previous_account_state=previous,
            available_at_utc=payload["available_at_utc"],
        )
        prior_hash = (
            previous.state_sha256
            if previous is not None
            else SleeveAccountState.genesis(
                deployment_sha256=generation.deployment_sha256,
                offset=generation.due_offset,
            ).state_sha256
        )
        return {
            "execution_snapshot_sha256": built.snapshot_sha256,
            "execution_source_sha256": built.execution_source_sha256,
            "previous_account_state_sha256": prior_hash,
            "directory": str(built.directory),
            "snapshot_path": str(built.snapshot_path),
            "sources_path": str(built.sources_path),
            "holding_start_date": built.snapshot.holding_start_date,
            "holding_end_date": built.snapshot.holding_end_date,
            "observation_available_at_utc": built.snapshot.observation_available_at_utc,
        }
    raise AssertionError(f"unexpected test release operation: {operation}")


def _activate(ledger: Path, *, recorded_at: str = "2026-08-22T00:00:00Z") -> dict:
    return activate_protocol(
        ledger,
        protocol_path=PROTOCOL,
        release_tag="5.0",
        release_tag_object_oid=TAG_OID,
        release_commit_oid=COMMIT_OID,
        authoritative_run=AUTHORITATIVE_RUN,
        recorded_at_utc=recorded_at,
    )


def _receipt(
    snapshot: Mapping[str, Any],
    *,
    purpose: str,
    decision_sha: str | None,
    run_id: int,
    created: str,
    completed: str,
    tlog: str,
    workflow_source_commit_oid: str = COMMIT_OID,
) -> dict[str, Any]:
    snapshot_sha = str(snapshot["snapshot_sha256"])
    request_id = sha256_bytes(
        canonical_json_bytes(
            {
                "release_ref": "refs/tags/5.0",
                "repository": "yxforever666gh/factor-lab",
                "snapshot_sha256": snapshot_sha,
                "workflow": "prospective-attest.yml",
            }
        )
    )
    subject_name = f"prospective-snapshot-{snapshot_sha}.json"
    invocation = (
        "https://github.com/yxforever666gh/factor-lab/"
        f"actions/runs/{run_id}/attempts/1"
    )
    statement = {
        "_type": "https://in-toto.io/Statement/v1",
        "subject": [{"name": subject_name, "digest": {"sha256": snapshot_sha}}],
        "predicateType": "https://slsa.dev/provenance/v1",
        "predicate": {
            "buildDefinition": {
                "buildType": (
                    "https://actions.github.io/buildtypes/workflow/v1"
                ),
                "externalParameters": {
                    "workflow": {
                        "ref": "refs/tags/5.0",
                        "repository": (
                            "https://github.com/yxforever666gh/factor-lab"
                        ),
                        "path": ".github/workflows/prospective-attest.yml",
                    }
                },
                "internalParameters": {},
                "resolvedDependencies": [
                    {
                        "uri": (
                            "git+https://github.com/yxforever666gh/"
                            "factor-lab@refs/tags/5.0"
                        ),
                        "digest": {"gitCommit": workflow_source_commit_oid},
                    }
                ],
            },
            "runDetails": {
                "builder": {
                    "id": (
                        "https://github.com/yxforever666gh/factor-lab/"
                        ".github/workflows/prospective-attest.yml@refs/tags/5.0"
                    )
                },
                "metadata": {"invocationId": invocation},
            },
        },
    }
    integrated_time = str(
        int(datetime.fromisoformat(tlog[:-1] + "+00:00").timestamp())
    )
    bundle = {
        "mediaType": "application/vnd.dev.sigstore.bundle.v0.3+json",
        "verificationMaterial": {
            "certificate": {"rawBytes": "dGVzdA=="},
            "tlogEntries": [
                {
                    "logIndex": str(run_id),
                    "logId": {"keyId": "dGVzdA=="},
                    "kindVersion": {"kind": "dsse", "version": "0.0.1"},
                    "integratedTime": integrated_time,
                    "inclusionPromise": {"signedEntryTimestamp": "dGVzdA=="},
                    "inclusionProof": {},
                    "canonicalizedBody": "e30=",
                }
            ],
        },
        "dsseEnvelope": {
            "payload": base64.b64encode(canonical_json_bytes(statement)).decode(),
            "payloadType": "application/vnd.in-toto+json",
            "signatures": [{"sig": "dGVzdA=="}],
        },
    }
    bundle_raw = canonical_json_bytes(bundle) + b"\n"
    bundle_sha = sha256_bytes(bundle_raw)
    ledger_root = Path(str(snapshot["path"])).parent.parent
    create_only_file(
        ledger_root / "bundles" / f"{snapshot_sha}-{bundle_sha}.jsonl",
        bundle_raw,
    )
    return {
        "purpose": purpose,
        "snapshot_sha256": snapshot_sha,
        "snapshot_head_record_sha256": snapshot["snapshot"]["head_record_sha256"],
        "decision_record_sha256": decision_sha,
        "request_id": request_id,
        "workflow_run_id": run_id,
        "workflow_run_attempt": 1,
        "workflow_run_display_title": f"prospective-{request_id}",
        "workflow_run_url": (
            f"https://github.com/yxforever666gh/factor-lab/actions/runs/{run_id}"
        ),
        "workflow_run_created_at_utc": created,
        "workflow_run_completed_at_utc": completed,
        "workflow_path": ".github/workflows/prospective-attest.yml",
        "workflow_ref": "refs/tags/5.0",
        "workflow_source_commit_oid": workflow_source_commit_oid,
        "attestation_bundle_sha256": bundle_sha,
        "certificate_identity": (
            "https://github.com/yxforever666gh/factor-lab/"
            ".github/workflows/prospective-attest.yml@refs/tags/5.0"
        ),
        "run_invocation_uri": invocation,
        "verified_timestamp_count": 1,
        "verified_timestamps": [
            {"type": "Tlog", "uri": "https://rekor.sigstore.dev", "timestamp_utc": tlog}
        ],
        "verified_tlog_type": "Tlog",
        "verified_tlog_uri": "https://rekor.sigstore.dev",
        "verified_tlog_timestamp_utc": tlog,
        "subject_name": subject_name,
        "subject_sha256": snapshot_sha,
    }


def _activation_canary(ledger: Path, activation: Mapping[str, Any]) -> dict:
    return append_attestation_receipt(
        ledger,
        _receipt(
            activation["snapshot"],
            purpose="activation_canary",
            decision_sha=None,
            run_id=121,
            created="2026-08-22T00:02:00Z",
            completed="2026-08-22T00:04:00Z",
            tlog="2026-08-22T00:03:00Z",
        ),
        recorded_at_utc="2026-08-22T00:05:00Z",
    )


def _upgrade_payload(
    activation_hash: str,
    *,
    supersedes: str | None = None,
    release_tag: str = "5.2",
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "activation_record_sha256": activation_hash,
        "supersedes_implementation_upgrade_record_sha256": supersedes,
        "protocol_id": "factor-lab/5.0/adaptive-core-overlay",
        "protocol_sha256": sha256_file(PROTOCOL),
        "frozen_route": "fixed_core_full",
        "implementation_release_tag": release_tag,
        "implementation_release_tag_object_oid": IMPLEMENTATION_TAG_OID,
        "implementation_commit_oid": IMPLEMENTATION_COMMIT_OID,
        "generator_id": "factor-lab/fixed-core-full-targets/5.2",
        "generator_entrypoint": (
            "factor_lab.prospective_targets:generate_fixed_core_targets"
        ),
        "generator_manifest_path": "protocols/5.2-target-generator.json",
        "generator_manifest_sha256": "9" * 64,
        "generator_test_vector_sha256": "a" * 64,
        "evaluator_id": "factor-lab/prospective-evaluation/5.2",
        "evaluation_contract_sha256": (
            "b3aff959751ae317f5783ec0e21fe98b03a2f047e8ec134053252feee8cb3a0c"
        ),
        "decision_plan_schema_version": 2,
    }


def _implementation_canary(ledger: Path, upgrade: Mapping[str, Any]) -> dict:
    return append_attestation_receipt(
        ledger,
        _receipt(
            upgrade["snapshot"],
            purpose="implementation_upgrade_canary",
            decision_sha=None,
            run_id=122,
            created="2026-08-23T00:02:00Z",
            completed="2026-08-23T00:04:00Z",
            tlog="2026-08-23T00:03:00Z",
        ),
        recorded_at_utc="2026-08-23T00:05:00Z",
    )


def _ready(ledger: Path) -> dict[str, dict]:
    activation = _activate(ledger)
    activation_canary = _activation_canary(ledger, activation)
    upgrade = append_implementation_upgrade(
        ledger,
        _upgrade_payload(activation["record_sha256"]),
        recorded_at_utc="2026-08-22T00:06:00Z",
    )
    implementation_canary = _implementation_canary(ledger, upgrade)
    return {
        "activation": activation,
        "activation_canary": activation_canary,
        "upgrade": upgrade,
        "implementation_canary": implementation_canary,
    }


def _signal_rows(signal: str) -> list[dict[str, Any]]:
    return [
        {
            "date": signal,
            "ticker": f"{index + 1:06d}.SZ",
            "eligible": True,
            "universe_member": True,
            "earnings_yield": 0.2 - index * 0.005,
            "pb": 1.0 + index * 0.05,
            "book_yield": 0.1 - index * 0.002,
            "volatility_20": 0.1 + index * 0.003,
        }
        for index in range(12)
    ]


@lru_cache(maxsize=1)
def _canonical_calendar() -> tuple[str, ...]:
    sessions = tuple(
        value
        for value in pd.bdate_range(
            CANONICAL_CALENDAR_ANCHOR,
            FROZEN_BRIDGE_END.date().isoformat(),
        ).strftime("%Y-%m-%d")
        if value not in _CANONICAL_WEEKDAY_CLOSURES
    )
    assert len(sessions) == CANONICAL_CALENDAR_COUNT
    assert sessions[0] == CANONICAL_CALENDAR_ANCHOR
    assert sessions[-1] == FROZEN_BRIDGE_END.date().isoformat()
    assert calendar_prefix_sha256(sessions) == CANONICAL_CALENDAR_SHA256
    return sessions


def _write_source_bundle(
    ledger: Path,
    *,
    signal: str,
    session: str,
    calendar_sessions: list[str],
    transitive_cas_root: Path | None = None,
) -> Path:
    rows = _signal_rows(signal)
    columns = list(rows[0])
    float_columns = [
        "earnings_yield",
        "pb",
        "book_yield",
        "volatility_20",
    ]
    encoded_rows = [
        {
            key: (value.hex() if key in float_columns else value)
            for key, value in row.items()
        }
        for row in rows
    ]
    rows_raw = canonical_json_bytes(encoded_rows)
    target_rows_sha = sha256_bytes(rows_raw)
    membership_sha = "4" * 64
    membership_source: dict[str, Any] = {
        "role": "membership",
        "sha256": membership_sha,
    }
    if transitive_cas_root is not None:
        def publish_cas(raw: bytes) -> tuple[str, str]:
            digest = sha256_bytes(raw)
            relative = (
                Path("runtime/prospective/5.0/source-artifacts")
                / f"sha256={digest}"
                / "artifact"
            )
            target = transitive_cas_root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(raw)
            return digest, relative.as_posix()

        leaf_raw = b"sealed-membership-origin\n"
        leaf_sha, leaf_path = publish_cas(leaf_raw)
        membership_raw = b"sealed-membership-parquet\n"
        membership_sha, membership_path = publish_cas(membership_raw)
        membership_manifest_raw = canonical_json_bytes(
            {
                "schema_version": 1,
                "membership_artifact_sha256": membership_sha,
                "source": {
                    "immutable_path": leaf_path,
                    "sha256": leaf_sha,
                    "size_bytes": len(leaf_raw),
                },
            }
        )
        membership_manifest_sha, membership_manifest_path = publish_cas(
            membership_manifest_raw
        )
        membership_source = {
            "role": "membership",
            "sha256": membership_sha,
            "immutable_path": membership_path,
            "size_bytes": len(membership_raw),
            "media_type": "application/vnd.apache.parquet",
            "immutable_manifest_path": membership_manifest_path,
            "manifest_sha256": membership_manifest_sha,
            "manifest_size_bytes": len(membership_manifest_raw),
            "manifest_media_type": "application/json",
        }
    inputs = [membership_source]
    inputs_sha = sha256_bytes(canonical_json_bytes(inputs))
    manifest = {
        "schema_version": 1,
        "protocol_release": "5.0",
        "kind": "prospective_signal_input_snapshot",
        "signal_date": signal,
        "official_trade_date": session,
        "inputs_available_at_utc": f"{signal}T10:00:00Z",
        "rows": {
            "path": "rows.json",
            "sha256": sha256_bytes(rows_raw),
            "row_count": len(encoded_rows),
            "columns": columns,
            "float_columns": float_columns,
        },
        "target_adapter": {
            "columns": columns,
            "target_rows_sha256": target_rows_sha,
            "input_sources_sha256": inputs_sha,
            "membership_artifact_sha256": membership_sha,
            "calendar_sessions": calendar_sessions,
            "calendar_next_trade_date": session,
            "calendar_sessions_sha256": calendar_prefix_sha256(calendar_sessions),
        },
        "inputs": inputs,
        "input_sources_sha256": inputs_sha,
    }
    manifest_raw = canonical_json_bytes(manifest)
    source_sha = sha256_bytes(manifest_raw)
    directory = ledger / "inputs" / source_sha
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "manifest.json").write_bytes(manifest_raw)
    (directory / "rows.json").write_bytes(rows_raw)
    (directory / "build-receipt.json").write_bytes(
        canonical_json_bytes(
            {
                "schema_version": 1,
                "snapshot_sha256": source_sha,
                "build_completed_at_utc": f"{signal}T10:30:00Z",
                "authoritative_for_snapshot_hash": False,
            }
        )
    )
    return directory


def _plan_case(
    ledger: Path,
    *,
    session: str = "2026-08-25",
    signal: str = "2026-08-24",
    previous_state: TenSleeveState | Mapping[str, Any] | None = None,
    calendar_sessions: list[str] | None = None,
    planned_at: str | None = None,
    transitive_cas_root: Path | None = None,
) -> dict[str, Any]:
    status = ledger_status(ledger)
    prefix = list(_canonical_calendar())
    deployment = DeploymentSpec(
        calendar_anchor=CANONICAL_CALENDAR_ANCHOR,
        calendar_prefix_count=len(prefix),
        calendar_prefix_last_session=FROZEN_BRIDGE_END.date().isoformat(),
        calendar_prefix_sha256=CANONICAL_CALENDAR_SHA256,
        activation_record_sha256=status["activation_record_sha256"],
        implementation_upgrade_record_sha256=status[
            "implementation_upgrade_record_sha256"
        ],
        deployment_protocol_sha256=sha256_file(PROTOCOL),
    )
    if previous_state is None:
        resolved_previous = TenSleeveState.genesis(deployment)
    elif isinstance(previous_state, TenSleeveState):
        resolved_previous = previous_state
    else:
        resolved_previous = TenSleeveState.from_mapping(previous_state)
    extension = calendar_sessions or [signal, session]
    sessions = [
        *prefix,
        *(value for value in extension if value > prefix[-1]),
    ]
    source_path = _write_source_bundle(
        ledger,
        signal=signal,
        session=session,
        calendar_sessions=sessions,
        transitive_cas_root=transitive_cas_root,
    )
    source = prospective_data.load_prospective_input_snapshot(source_path)
    signal_index = sessions.index(signal)
    skipped_sessions = sessions[
        resolved_previous.last_processed_calendar_index + 1 : signal_index
    ]
    input_snapshot = InputSnapshot(
        signal_date=signal,
        calendar_sessions=sessions,
        skipped_sessions=skipped_sessions,
        rows=source.target_frame,
        source_data_snapshot_sha256=source.snapshot_sha256,
        target_rows_sha256=source.target_rows_sha256,
        input_sources_sha256=source.input_sources_sha256,
        membership_artifact_sha256=source.membership_artifact_sha256,
        source_build_checkpoint_utc=source.inputs_available_at_utc,
        max_available_at_utc=source.inputs_available_at_utc,
        information_cutoff_utc=source.inputs_available_at_utc,
        signal_close_utc=f"{signal}T07:00:00Z",
        admission_deadline_utc=f"{session}T01:15:00Z",
    )
    result = generate_fixed_core_targets(
        deployment=deployment,
        input_snapshot=input_snapshot,
        previous_state=resolved_previous,
    )
    plan = build_decision_plan(
        ledger,
        decision_session=session,
        source_data_snapshot_sha256=source.snapshot_sha256,
        planned_at_utc=planned_at or f"{signal}T12:00:00Z",
    )
    assert plan["route_target_plan"] == result.to_dict()

    return {
        "plan": plan,
        "result": result,
        "deployment": deployment,
        "input_snapshot": input_snapshot,
        "previous_state": resolved_previous,
        "source_path": source_path,
    }


def _seal_case(
    ledger: Path,
    case: Mapping[str, Any],
    *,
    plan: Any | None = None,
    recorded_at: str,
) -> dict[str, Any]:
    return seal_decision(
        ledger,
        case["plan"] if plan is None else plan,
        recorded_at_utc=recorded_at,
    )


def _decision_receipt(
    ledger: Path,
    decision: Mapping[str, Any],
    *,
    run_id: int,
    signal: str,
) -> dict:
    return append_attestation_receipt(
        ledger,
        _receipt(
            decision["snapshot"],
            purpose="decision_anchor",
            decision_sha=str(decision["record_sha256"]),
            run_id=run_id,
            created=f"{signal}T12:02:00Z",
            completed=f"{signal}T12:04:00Z",
            tlog=f"{signal}T12:03:00Z",
        ),
        recorded_at_utc=f"{signal}T12:05:00Z",
    )


def _outcome(
    decision_sha: str,
    receipt_sha: str,
    *,
    start: str,
    end: str,
    net_return: int = 20_000_000,
) -> dict[str, Any]:
    return {
        "decision_record_sha256": decision_sha,
        "attestation_receipt_record_sha256": receipt_sha,
        "holding_start_date": start,
        "holding_end_date": end,
        "observation_available_at_utc": f"{end}T12:00:00Z",
        "source_snapshot_sha256": "5" * 64,
        "execution_status": "complete",
        "gross_return_ppb": 21_000_000,
        "net_return_ppb": net_return,
        "benchmark_return_ppb": 10_000_000,
        "turnover_ppm": 250_000,
        "fees_fen": 12_345,
        "ending_nav_fen": 5_100_000_000,
    }


def _load_test_execution_snapshot(
    path: str | Path,
    generation_result: Any,
    *,
    previous_account_state: Any = None,
) -> SimpleNamespace:
    """Cheap unit boundary; source-backed rebuild has its own focused tests."""

    del previous_account_state
    directory = Path(path).resolve()
    raw = (directory / "snapshot.json").read_bytes()
    value = strict_load_canonical(raw)
    assert isinstance(value, dict)
    snapshot = ExecutionSnapshot.from_mapping(value)
    generation_sha = (
        generation_result.result_sha256
        if hasattr(generation_result, "result_sha256")
        else generation_result["result_sha256"]
    )
    if directory.name != snapshot.snapshot_sha256:
        raise ValueError("test execution path/hash mismatch")
    if snapshot.generation_result_sha256 != generation_sha:
        raise ValueError("test execution generation mismatch")
    return SimpleNamespace(
        snapshot=snapshot,
        snapshot_sha256=snapshot.snapshot_sha256,
        directory=directory,
    )


def _execution_sessions(result: Any, *, periods: int = 40) -> list[str]:
    prefix = list(_canonical_calendar())
    extension = list(
        pd.bdate_range("2026-08-24", periods=periods).strftime("%Y-%m-%d")
    )
    sessions = [*prefix, *extension]
    end_index = int(result.calendar_index) + 11
    assert sessions[int(result.calendar_index)] == result.signal_date
    assert sessions[int(result.calendar_index) + 1] == result.trade_date
    return sessions[: end_index + 1]


def _stable_open(date_text: str, ticker: str) -> float:
    day = (pd.Timestamp(date_text) - pd.Timestamp("2026-01-01")).days
    ticker_number = int(ticker[:6])
    return 20.0 + ticker_number * 0.01 + day * 0.001


def _write_execution_snapshot(
    ledger: Path,
    result: Any,
) -> ExecutionSnapshot:
    sessions = _execution_sessions(result)
    signal_index = int(result.calendar_index)
    window = sessions[signal_index + 1 : signal_index + 12]
    benchmark = [row["ticker"] for row in _signal_rows(result.signal_date)]
    rows: list[ExecutionObservation] = []
    for observation_date in window:
        for ticker in benchmark:
            is_start = observation_date == result.trade_date
            rows.append(
                ExecutionObservation(
                    date=observation_date,
                    ticker=ticker,
                    open_adj_hex=_stable_open(observation_date, ticker).hex(),
                    adv_20_asof_hex=(1_000_000_000.0.hex() if is_start else None),
                    volatility_20_asof_hex=(0.1.hex() if is_start else None),
                    execution_input_date=(result.signal_date if is_start else None),
                )
            )
    snapshot = ExecutionSnapshot(
        generation_result_sha256=result.result_sha256,
        execution_source_sha256="d" * 64,
        official_calendar_sha256=calendar_prefix_sha256(sessions),
        signal_date=result.signal_date,
        holding_start_date=result.trade_date,
        holding_end_date=sessions[-1],
        calendar_sessions=sessions,
        benchmark_tickers=benchmark,
        rows=rows,
        calendar_available_at_utc=f"{result.signal_date}T08:00:00Z",
        decision_inputs_available_at_utc=f"{result.signal_date}T10:00:00Z",
        trade_deadline_utc=f"{result.trade_date}T01:15:00Z",
        start_open_available_at_utc=f"{result.trade_date}T02:00:00Z",
        end_open_available_at_utc=f"{sessions[-1]}T02:00:00Z",
        observation_available_at_utc=f"{sessions[-1]}T12:00:00Z",
    )
    directory = ledger / "executions" / snapshot.snapshot_sha256
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "snapshot.json").write_bytes(
        canonical_json_bytes(snapshot.to_dict())
    )
    (directory / "sources.json").write_bytes(canonical_json_bytes({}))
    return snapshot


def _rich_outcome(
    decision_sha: str,
    receipt_sha: str,
    result: Any,
    snapshot: ExecutionSnapshot,
    *,
    previous_account_state: SleeveAccountState | None = None,
) -> tuple[dict[str, Any], Any]:
    previous = previous_account_state or SleeveAccountState.genesis(
        deployment_sha256=result.deployment_sha256,
        offset=result.due_offset,
    )
    outcome = evaluate_due_sleeve_cycle(
        generation_result=result,
        execution_snapshot=snapshot,
        previous_account_state=previous,
    )
    return (
        {
            "schema_version": 2,
            "decision_record_sha256": decision_sha,
            "attestation_receipt_record_sha256": receipt_sha,
            "execution_snapshot_sha256": snapshot.snapshot_sha256,
            "cycle_outcome_sha256": outcome.outcome_sha256,
            "cycle_outcome": outcome.to_dict(),
        },
        outcome,
    )


def _legacy_snapshot(
    activation_payload: Mapping[str, Any],
    *,
    sequence: int,
    head_hash: str,
    phase: str,
    decision_count: int = 0,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "ledger_id": "factor-lab/prospective/5.0",
        "head_sequence": sequence,
        "head_record_sha256": head_hash,
        "activation_record_sha256": activation_payload["activation_record_sha256"],
        "protocol_id": activation_payload["protocol_id"],
        "protocol_sha256": activation_payload["protocol_sha256"],
        "release_tag": activation_payload["release_tag"],
        "release_commit_oid": activation_payload["release_commit_oid"],
        "authoritative_run_id": activation_payload["authoritative_run_id"],
        "run_fingerprint": activation_payload["run_fingerprint"],
        "manifest_sha256": activation_payload["manifest_sha256"],
        "manifest_self_sha256": activation_payload["manifest_self_sha256"],
        "adaptive_summary_sha256": activation_payload["adaptive_summary_sha256"],
        "frozen_route": activation_payload["frozen_route"],
        "integrity_valid": activation_payload["integrity_valid"],
        "phase": phase,
        "decision_count": decision_count,
        "confirmed_observation_count": 0,
    }


def _concurrent_activate(ledger: str, protocol: str, queue: multiprocessing.Queue) -> None:
    try:
        result = activate_protocol(
            ledger,
            protocol_path=protocol,
            release_tag="5.0",
            release_tag_object_oid=TAG_OID,
            release_commit_oid=COMMIT_OID,
            authoritative_run=AUTHORITATIVE_RUN,
            recorded_at_utc="2026-08-22T00:00:00Z",
        )
        queue.put(("ok", result["record_sha256"]))
    except Exception as exc:  # pragma: no cover
        queue.put(("error", f"{type(exc).__name__}: {exc}"))


def test_canonical_codec_normalizes_nfc_and_rejects_ambiguous_json() -> None:
    composed = canonical_json_bytes({"é": [1, True, None]})
    decomposed = canonical_json_bytes({"e\u0301": [1, True, None]})
    assert composed == decomposed == b'{"\xc3\xa9":[1,true,null]}'
    assert strict_load_canonical(composed) == {"é": [1, True, None]}
    for invalid in (
        b'{"a":1.0}',
        b'{"a":NaN}',
        b'{"a":1,"a":2}',
        b'{ "a":1}',
        b'\xef\xbb\xbf{"a":1}',
        b'{"e\\u0301":1,"\\u00e9":2}',
        b'{"a":-0}',
    ):
        with pytest.raises(CanonicalJSONError):
            strict_load_canonical(invalid)
    with pytest.raises(CanonicalJSONError):
        canonical_json_bytes({"return": 0.1})


def test_create_only_file_is_idempotent_but_never_overwrites(tmp_path: Path) -> None:
    path = tmp_path / "evidence.json"
    assert create_only_file(path, b"first") is True
    assert create_only_file(path, b"first") is False
    with pytest.raises(LedgerIntegrityError):
        create_only_file(path, b"second")
    assert path.read_bytes() == b"first"


def test_real_activation_prefix_stays_v1_bytes_and_upgrade_starts_v2(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    assert _activate(ledger)["created"] is False
    with pytest.raises(LedgerStateError, match="already activated differently"):
        activate_protocol(
            ledger,
            protocol_path=PROTOCOL,
            release_tag="5.0",
            release_tag_object_oid="f" * 40,
            release_commit_oid=COMMIT_OID,
            authoritative_run=AUTHORITATIVE_RUN,
            recorded_at_utc="2026-08-22T00:00:00Z",
        )
    activation_payload = activation["record"]["payload"]
    expected_one = _legacy_snapshot(
        {**activation_payload, "activation_record_sha256": activation["record_sha256"]},
        sequence=1,
        head_hash=activation["record_sha256"],
        phase="awaiting_new_data",
    )
    assert Path(activation["snapshot"]["path"]).read_bytes() == canonical_json_bytes(expected_one)
    canary = _activation_canary(ledger, activation)
    expected_two = _legacy_snapshot(
        {**activation_payload, "activation_record_sha256": activation["record_sha256"]},
        sequence=2,
        head_hash=canary["record_sha256"],
        phase="awaiting_new_data",
    )
    assert Path(canary["snapshot"]["path"]).read_bytes() == canonical_json_bytes(expected_two)
    assert canary["snapshot"]["snapshot"]["schema_version"] == 1
    upgrade = append_implementation_upgrade(
        ledger,
        _upgrade_payload(activation["record_sha256"]),
        recorded_at_utc="2026-08-22T00:06:00Z",
    )
    snapshot = upgrade["snapshot"]["snapshot"]
    assert snapshot["schema_version"] == 2
    assert snapshot["release_tag"] == "5.0"
    assert snapshot["release_commit_oid"] == COMMIT_OID
    assert snapshot["implementation_release_tag"] == "5.2"
    assert audit_ledger(ledger)["valid"] is True


def test_active_release_capsule_drift_invalidates_audit_and_blocks_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = tmp_path / "ledger"
    _ready(ledger)

    def drift(*_args, **_kwargs):
        raise LedgerIntegrityError("active release capsule differs")

    monkeypatch.setattr(
        prospective_ledger,
        "_verify_upgrade_runtime_closure",
        drift,
    )
    audited = audit_ledger(ledger)
    assert audited["valid"] is False
    assert audited["issues"][0]["code"] == "invalid_external_evidence"
    assert "release capsule" in audited["issues"][0]["detail"]
    with pytest.raises(LedgerIntegrityError, match="release capsule"):
        seal_snapshot(ledger)


def test_manual_plans_fail_closed_and_upgrade_requires_both_canaries(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    with pytest.raises(LedgerStateError, match="manual targets/code"):
        build_decision_plan(
            ledger,
            decision_session="2026-08-24",
            information_cutoff_utc="2026-08-23T11:00:00Z",
            input_max_available_at_utc="2026-08-23T10:59:59Z",
            input_snapshot_sha256="1" * 64,
            model_state_sha256="2" * 64,
            code_commit_oid=COMMIT_OID,
            expected_nav_fen=1,
            targets_ppm={"FAKE": 1_000_000},
            cash_weight_ppm=0,
            planned_at_utc="2026-08-23T12:00:00Z",
        )
    with pytest.raises(LedgerStateError, match="activation canary"):
        append_implementation_upgrade(
            ledger, _upgrade_payload(activation["record_sha256"])
        )
    _activation_canary(ledger, activation)
    bad = _upgrade_payload(activation["record_sha256"])
    bad["frozen_route"] = "online_overlay"
    with pytest.raises(LedgerStateError, match="frozen_route"):
        append_implementation_upgrade(ledger, bad)
    wrong_release = _upgrade_payload(
        activation["record_sha256"], release_tag="5.1"
    )
    with pytest.raises(LedgerStateError, match="implementation_release_tag"):
        append_implementation_upgrade(ledger, wrong_release)
    upgrade = append_implementation_upgrade(
        ledger,
        _upgrade_payload(activation["record_sha256"]),
        recorded_at_utc="2026-08-22T00:06:00Z",
    )
    with pytest.raises(LedgerStateError, match="awaiting_implementation_attestation"):
        _plan_case(ledger)
    forged = _receipt(
        upgrade["snapshot"],
        purpose="implementation_upgrade_canary",
        decision_sha=None,
        run_id=122,
        created="2026-08-23T00:02:00Z",
        completed="2026-08-23T00:04:00Z",
        tlog="2026-08-23T00:03:00Z",
        workflow_source_commit_oid=IMPLEMENTATION_COMMIT_OID,
    )
    with pytest.raises(LedgerStateError, match="frozen release"):
        append_attestation_receipt(ledger, forged)
    _implementation_canary(ledger, upgrade)
    assert ledger_status(ledger)["decision_generation_ready"] is True


def test_built_plan_becomes_stale_when_the_implementation_head_moves(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    ready = _ready(ledger)
    case = _plan_case(ledger)
    moved = append_implementation_upgrade(
        ledger,
        _upgrade_payload(
            ready["activation"]["record_sha256"],
            supersedes=ready["upgrade"]["record_sha256"],
            release_tag="5.3",
        ),
        recorded_at_utc="2026-08-23T00:06:00Z",
    )
    append_attestation_receipt(
        ledger,
        _receipt(
            moved["snapshot"],
            purpose="implementation_upgrade_canary",
            decision_sha=None,
            run_id=127,
            created="2026-08-23T00:07:00Z",
            completed="2026-08-23T00:09:00Z",
            tlog="2026-08-23T00:08:00Z",
        ),
        recorded_at_utc="2026-08-23T00:10:00Z",
    )
    with pytest.raises(LedgerStateError, match="stale"):
        store_decision_plan(ledger, case["plan"])


def test_two_open_cycles_anchor_then_close_out_of_order(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    _ready(ledger)
    first_case = _plan_case(ledger)
    first_stored = store_decision_plan(ledger, first_case["plan"])
    assert store_decision_plan(ledger, first_case["plan"])["created"] is False
    first = _seal_case(
        ledger,
        first_case,
        plan=first_stored["path"],
        recorded_at="2026-08-24T12:01:00Z",
    )
    first_receipt = _decision_receipt(ledger, first, run_id=123, signal="2026-08-24")
    assert ledger_status(ledger)["open_decision_count"] == 1
    second_case = _plan_case(
        ledger,
        session="2026-08-26",
        signal="2026-08-25",
        previous_state=first_case["result"].next_state,
        calendar_sessions=[
            "2026-08-21",
            "2026-08-24",
            "2026-08-25",
            "2026-08-26",
        ],
    )
    second = _seal_case(
        ledger,
        second_case,
        recorded_at="2026-08-25T12:01:00Z",
    )
    second_receipt = _decision_receipt(ledger, second, run_id=124, signal="2026-08-25")
    status = ledger_status(ledger)
    assert status["status"] == "awaiting_decision"
    assert status["open_decision_count"] == 2
    assert (
        status["latest_model_state_sha256"]
        == second_case["result"].next_state.state_sha256
    )
    first_execution = _write_execution_snapshot(ledger, first_case["result"])
    second_execution = _write_execution_snapshot(ledger, second_case["result"])
    second_payload = build_outcome_payload(
        ledger,
        decision_record_sha256=second["record_sha256"],
        execution_snapshot_sha256=second_execution.snapshot_sha256,
    )
    wrong = deepcopy(second_payload)
    wrong["attestation_receipt_record_sha256"] = first_receipt["record_sha256"]
    with pytest.raises(LedgerStateError, match="exact open"):
        append_outcome(ledger, wrong, recorded_at_utc="2026-09-10T12:01:00Z")
    second_outcome = append_outcome(
        ledger,
        second_payload,
        recorded_at_utc="2026-09-10T12:01:00Z",
    )
    first_payload = build_outcome_payload(
        ledger,
        decision_record_sha256=first["record_sha256"],
        execution_snapshot_sha256=first_execution.snapshot_sha256,
    )
    first_outcome = append_outcome(
        ledger,
        first_payload,
        recorded_at_utc="2026-09-10T12:02:00Z",
    )
    with pytest.raises(LedgerStateError, match="open decision"):
        append_outcome(
            ledger,
            second_payload,
            recorded_at_utc="2026-09-10T12:03:00Z",
        )
    assert first_outcome["sequence"] > second_outcome["sequence"]
    final = ledger_status(ledger)
    assert final["open_decision_count"] == 0
    assert final["decision_count"] == 2
    assert final["confirmed_observation_count"] == 2
    assert audit_ledger(ledger)["valid"] is True


def test_execution_evidence_recovers_all_inputs_from_verified_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = tmp_path / "ledger"
    _ready(ledger)
    case = _plan_case(ledger)
    decision = _seal_case(
        ledger,
        case,
        recorded_at="2026-08-24T12:01:00Z",
    )
    _decision_receipt(ledger, decision, run_id=130, signal="2026-08-24")
    captured: dict[str, Any] = {}
    directory = ledger / "executions" / ("9" * 64)

    def fake_build(project_root: Path, generation: Any, **kwargs: Any) -> SimpleNamespace:
        captured.update(
            {
                "project_root": Path(project_root),
                "generation": generation,
                **kwargs,
            }
        )
        return SimpleNamespace(
            snapshot_sha256="9" * 64,
            execution_source_sha256="a" * 64,
            snapshot=SimpleNamespace(
                holding_start_date="2026-08-25",
                holding_end_date="2026-09-08",
                observation_available_at_utc="2026-09-08T12:00:00Z",
            ),
            directory=directory,
            snapshot_path=directory / "snapshot.json",
            sources_path=directory / "sources.json",
        )

    monkeypatch.setattr(
        prospective_execution_data,
        "build_prospective_execution_snapshot",
        fake_build,
    )
    built = build_execution_evidence(
        ledger,
        decision_record_sha256=decision["record_sha256"],
        available_at_utc="2026-09-08T12:05:00Z",
    )

    assert captured["generation"].result_sha256 == case["result"].result_sha256
    assert (
        captured["source_data_snapshot_sha256"]
        == case["input_snapshot"].source_data_snapshot_sha256
    )
    assert captured["previous_account_state"] is None
    assert captured["available_at_utc"] == "2026-09-08T12:05:00Z"
    assert built["execution_snapshot_sha256"] == "9" * 64
    assert built["previous_account_state_sha256"] == SleeveAccountState.genesis(
        deployment_sha256=case["result"].deployment_sha256,
        offset=case["result"].due_offset,
    ).state_sha256


def test_rich_outcome_is_exactly_recomputed_and_audit_requires_sidecar(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    _ready(ledger)
    case = _plan_case(ledger)
    decision = _seal_case(
        ledger,
        case,
        recorded_at="2026-08-24T12:01:00Z",
    )
    _decision_receipt(ledger, decision, run_id=131, signal="2026-08-24")
    execution = _write_execution_snapshot(ledger, case["result"])
    rich = build_outcome_payload(
        ledger,
        decision_record_sha256=decision["record_sha256"],
        execution_snapshot_sha256=execution.snapshot_sha256,
    )
    forged = deepcopy(rich)
    forged_cycle = forged["cycle_outcome"]
    forged_cycle["net_return_ppb"] += 1
    cycle_payload = dict(forged_cycle)
    del cycle_payload["outcome_sha256"]
    forged_cycle["outcome_sha256"] = sha256_bytes(
        canonical_json_bytes(cycle_payload)
    )
    forged["cycle_outcome_sha256"] = forged_cycle["outcome_sha256"]
    with pytest.raises(LedgerStateError, match="fixed execution recomputation"):
        append_outcome(
            ledger,
            forged,
            recorded_at_utc="2026-09-09T12:01:00Z",
        )
    appended = append_outcome(
        ledger,
        rich,
        recorded_at_utc="2026-09-09T12:01:00Z",
    )
    assert appended["record"]["payload"] == rich
    assert audit_ledger(ledger)["valid"] is True

    snapshot_path = ledger / "executions" / execution.snapshot_sha256 / "snapshot.json"
    snapshot_bytes = snapshot_path.read_bytes()
    snapshot_path.unlink()
    invalid = audit_ledger(ledger)
    assert invalid["valid"] is False
    assert invalid["issues"][0]["code"] == "invalid_external_evidence"
    with pytest.raises(LedgerIntegrityError, match="external-evidence prefix"):
        seal_snapshot(ledger)
    snapshot_path.write_bytes(snapshot_bytes)
    assert audit_ledger(ledger)["valid"] is True


def test_schema2_outcome_is_always_deterministic_complete(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    _ready(ledger)
    case = _plan_case(ledger)
    decision = _seal_case(
        ledger, case, recorded_at="2026-08-24T12:01:00Z"
    )
    _decision_receipt(ledger, decision, run_id=132, signal="2026-08-24")
    execution = _write_execution_snapshot(ledger, case["result"])

    with pytest.raises(TypeError):
        build_outcome_payload(
            ledger,
            decision_record_sha256=decision["record_sha256"],
            execution_snapshot_sha256=execution.snapshot_sha256,
            execution_status="not_executed",  # type: ignore[call-arg]
        )

    complete = build_outcome_payload(
        ledger,
        decision_record_sha256=decision["record_sha256"],
        execution_snapshot_sha256=execution.snapshot_sha256,
    )
    assert complete["cycle_outcome"]["execution_status"] == "complete"
    forged = deepcopy(complete)
    forged_cycle = forged["cycle_outcome"]
    forged_cycle["execution_status"] = "not_executed"
    forged_cycle["not_executed_reason"] = "operator choice"
    cycle_payload = {
        key: value
        for key, value in forged_cycle.items()
        if key != "outcome_sha256"
    }
    forged_cycle["outcome_sha256"] = sha256_bytes(
        canonical_json_bytes(cycle_payload)
    )
    forged["cycle_outcome_sha256"] = forged_cycle["outcome_sha256"]
    with pytest.raises(LedgerStateError, match="embedded cycle outcome is invalid"):
        append_outcome(
            ledger,
            forged,
            recorded_at_utc="2026-09-09T12:01:00Z",
        )


def test_bundle_bytes_are_required_at_append_and_audit(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    receipt = _receipt(
        activation["snapshot"],
        purpose="activation_canary",
        decision_sha=None,
        run_id=123,
        created="2026-08-22T00:01:00Z",
        completed="2026-08-22T00:03:00Z",
        tlog="2026-08-22T00:02:00Z",
    )
    bundle = next((ledger / "bundles").iterdir())
    bundle_raw = bundle.read_bytes()
    bundle.unlink()
    with pytest.raises(LedgerIntegrityError, match="bundle is missing"):
        append_attestation_receipt(
            ledger,
            receipt,
            recorded_at_utc="2026-08-22T00:04:00Z",
        )

    bundle.write_bytes(bundle_raw)
    append_attestation_receipt(
        ledger,
        receipt,
        recorded_at_utc="2026-08-22T00:04:00Z",
    )
    bundle.write_bytes(b'{"bundle":false}\n')
    audited = audit_ledger(ledger)
    assert audited["valid"] is False
    assert audited["issues"][0]["code"] == "invalid_attestation_bundle"


def test_bundle_claims_are_checked_even_when_the_bound_byte_hash_matches(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    receipt = _receipt(
        activation["snapshot"],
        purpose="activation_canary",
        decision_sha=None,
        run_id=223,
        created="2026-08-22T00:01:00Z",
        completed="2026-08-22T00:03:00Z",
        tlog="2026-08-22T00:02:00Z",
    )
    original = ledger / "bundles" / (
        f"{receipt['snapshot_sha256']}-{receipt['attestation_bundle_sha256']}.jsonl"
    )
    bundle = strict_load_canonical(original.read_bytes().rstrip(b"\n"))
    statement = strict_load_canonical(
        base64.b64decode(bundle["dsseEnvelope"]["payload"], validate=True)
    )
    statement["predicate"]["runDetails"]["builder"]["id"] = (
        "https://github.com/other/repository/.github/workflows/other.yml@refs/tags/5.0"
    )
    bundle["dsseEnvelope"]["payload"] = base64.b64encode(
        canonical_json_bytes(statement)
    ).decode()
    forged_raw = canonical_json_bytes(bundle) + b"\n"
    forged_sha = sha256_bytes(forged_raw)
    forged_path = ledger / "bundles" / (
        f"{receipt['snapshot_sha256']}-{forged_sha}.jsonl"
    )
    forged_path.write_bytes(forged_raw)
    receipt["attestation_bundle_sha256"] = forged_sha
    with pytest.raises(LedgerIntegrityError, match="receipt-bound provenance"):
        append_attestation_receipt(
            ledger,
            receipt,
            recorded_at_utc="2026-08-22T00:04:00Z",
        )


def test_boolean_schema_versions_fail_closed(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    _activation_canary(ledger, activation)
    payload = _upgrade_payload(activation["record_sha256"])
    payload["schema_version"] = True
    with pytest.raises(LedgerStateError, match="must be an integer"):
        append_implementation_upgrade(ledger, payload)


def test_unattested_implementation_upgrade_can_be_explicitly_abandoned(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    _activation_canary(ledger, activation)
    upgrade = append_implementation_upgrade(
        ledger,
        _upgrade_payload(activation["record_sha256"]),
        recorded_at_utc="2026-08-22T00:06:00Z",
    )
    with pytest.raises(LedgerStateError, match="another upgrade"):
        abandon_implementation_upgrade(
            ledger,
            implementation_upgrade_record_sha256="f" * 64,
            reason="canary failed",
        )
    abandoned = abandon_implementation_upgrade(
        ledger,
        implementation_upgrade_record_sha256=upgrade["record_sha256"],
        reason="published canary failed verification",
        recorded_at_utc="2026-08-22T00:07:00Z",
    )
    assert abandoned["kind"] == "implementation_upgrade_abandonment"
    status = ledger_status(ledger)
    assert status["phase"] == "awaiting_new_data"
    assert status["implementation_upgrade_record_sha256"] is None
    assert (
        status["latest_implementation_upgrade_record_sha256"]
        == upgrade["record_sha256"]
    )

    retry = _upgrade_payload(
        activation["record_sha256"],
        supersedes=upgrade["record_sha256"],
        release_tag="5.3",
    )
    retried = append_implementation_upgrade(
        ledger, retry, recorded_at_utc="2026-08-22T00:08:00Z"
    )
    assert retried["record"]["payload"]["implementation_release_tag"] == "5.3"


def test_evaluation_is_capsule_bound_and_history_uses_one_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledger = tmp_path / "ledger"
    ready = _ready(ledger)
    case = _plan_case(ledger)
    decision = _seal_case(
        ledger, case, recorded_at="2026-08-24T12:01:00Z"
    )
    _decision_receipt(ledger, decision, run_id=133, signal="2026-08-24")
    execution = _write_execution_snapshot(ledger, case["result"])
    rich = build_outcome_payload(
        ledger,
        decision_record_sha256=decision["record_sha256"],
        execution_snapshot_sha256=execution.snapshot_sha256,
    )
    outcome = append_outcome(
        ledger, rich, recorded_at_utc="2026-09-09T12:01:00Z"
    )
    calls: list[str] = []

    def recording_operation(layout, state, operation, payload):
        calls.append(operation)
        return _run_test_release_operation_wrapped(
            layout, state, operation, payload
        )

    monkeypatch.setattr(
        prospective_ledger, "_run_active_release_operation", recording_operation
    )
    evaluation = evaluate_ledger(ledger)
    assert calls == ["replay_history", "evaluate"]
    assert evaluation["binding"]["ledger_head_record_sha256"] == outcome[
        "record_sha256"
    ]
    assert evaluation["binding"][
        "implementation_upgrade_record_sha256"
    ] == ready["upgrade"]["record_sha256"]
    assert evaluation["binding"]["outcome_count"] == 1
    assert evaluation["evaluation_envelope_sha256"] == sha256_bytes(
        canonical_json_bytes(
            {
                key: value
                for key, value in evaluation.items()
                if key != "evaluation_envelope_sha256"
            }
        )
    )


def test_verification_cache_replays_only_suffix_and_audit_refreshes_full_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledger = tmp_path / "ledger"
    _ready(ledger)
    case = _plan_case(ledger)
    decision = _seal_case(
        ledger, case, recorded_at="2026-08-24T12:01:00Z"
    )
    receipt = _decision_receipt(
        ledger, decision, run_id=333, signal="2026-08-24"
    )
    execution = _write_execution_snapshot(ledger, case["result"])
    rich, cycle = _rich_outcome(
        decision["record_sha256"],
        receipt["record_sha256"],
        case["result"],
        execution,
    )
    append_outcome(
        ledger,
        rich,
        recorded_at_utc=f"{cycle.holding_end_date}T12:01:00Z",
    )

    calls: list[tuple[str, dict[str, Any]]] = []

    def recording_operation(layout, state, operation, payload):
        calls.append((operation, dict(payload)))
        return _run_test_release_operation_wrapped(
            layout, state, operation, payload
        )

    monkeypatch.setattr(
        prospective_ledger, "_run_active_release_operation", recording_operation
    )
    audited = audit_ledger(ledger)
    assert audited["valid"] is True
    full = [payload for operation, payload in calls if operation == "replay_history"]
    assert len(full) == 1
    assert [item["operation"] for item in full[0]["operations"]] == [
        "replay_target",
        "replay_outcome",
    ]

    calls.clear()
    assert ledger_status(ledger)["valid"] is True
    assert calls == []

    checkpoint_evaluation(ledger, recorded_at_utc="2026-09-10T12:00:00Z")
    calls.clear()
    assert ledger_status(ledger)["valid"] is True
    suffix = [payload for operation, payload in calls if operation == "replay_history"]
    assert len(suffix) == 1
    assert len(suffix[0]["operations"]) == 1
    cached_operation = suffix[0]["operations"][0]
    assert cached_operation["operation"] == "evaluate"
    assert cached_operation["operation_id"] == sha256_bytes(
        canonical_json_bytes(
            {
                "operation": cached_operation["operation"],
                "payload": cached_operation["payload"],
            }
        )
    )

    calls.clear()
    assert audit_ledger(ledger)["valid"] is True
    full = [payload for operation, payload in calls if operation == "replay_history"]
    assert len(full) == 1
    assert len(full[0]["operations"]) == 3
    calls.clear()
    assert ledger_status(ledger)["valid"] is True
    assert calls == []

    cache_path = next((ledger / "verification-cache").glob("*.json"))
    cache_path.write_bytes(b"{}")
    assert ledger_status(ledger)["valid"] is True
    full = [payload for operation, payload in calls if operation == "replay_history"]
    assert len(full) == 1
    assert len(full[0]["operations"]) == 3


def test_warm_cache_fails_closed_for_top_level_and_recursive_cas_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledger = tmp_path / "ledger"
    project = tmp_path / "project"
    monkeypatch.setattr(
        prospective_ledger,
        "_implementation_project_root",
        lambda _layout: project,
    )
    _ready(ledger)
    case = _plan_case(ledger, transitive_cas_root=project)
    decision = _seal_case(
        ledger, case, recorded_at="2026-08-24T12:01:00Z"
    )
    receipt = _decision_receipt(
        ledger, decision, run_id=334, signal="2026-08-24"
    )
    execution = _write_execution_snapshot(ledger, case["result"])
    rich, cycle = _rich_outcome(
        decision["record_sha256"],
        receipt["record_sha256"],
        case["result"],
        execution,
    )
    append_outcome(
        ledger,
        rich,
        recorded_at_utc=f"{cycle.holding_end_date}T12:01:00Z",
    )
    calls: list[str] = []

    def authoritative_operation(layout, state, operation, payload):
        calls.append(operation)
        nested = payload["operations"] if operation == "replay_history" else []
        for item in nested:
            if item["operation"] != "replay_target":
                continue
            source_sha = item["payload"]["source_data_snapshot_sha256"]
            prospective_ledger._transitive_cas_bindings(
                layout,
                layout.inputs / source_sha / "manifest.json",
            )
        return _run_test_release_operation_wrapped(
            layout, state, operation, payload
        )

    monkeypatch.setattr(
        prospective_ledger,
        "_run_active_release_operation",
        authoritative_operation,
    )
    assert audit_ledger(ledger)["valid"] is True
    calls.clear()
    assert ledger_status(ledger)["valid"] is True
    assert calls == []

    rows_path = case["source_path"] / "rows.json"
    rows_raw = rows_path.read_bytes()
    rows_path.write_bytes(rows_raw + b" ")
    assert ledger_status(ledger)["valid"] is False
    assert "replay_history" in calls
    rows_path.write_bytes(rows_raw)
    calls.clear()
    assert audit_ledger(ledger)["valid"] is True

    execution_path = (
        ledger / "executions" / execution.snapshot_sha256 / "snapshot.json"
    )
    execution_raw = execution_path.read_bytes()
    execution_path.unlink()
    calls.clear()
    assert ledger_status(ledger)["valid"] is False
    assert "replay_history" in calls
    execution_path.write_bytes(execution_raw)
    assert audit_ledger(ledger)["valid"] is True

    manifest = strict_load_canonical((case["source_path"] / "manifest.json").read_bytes())
    membership_manifest_path = project / manifest["inputs"][0][
        "immutable_manifest_path"
    ]
    membership_manifest = strict_load_canonical(
        membership_manifest_path.read_bytes()
    )
    leaf_path = project / membership_manifest["source"]["immutable_path"]
    leaf_raw = leaf_path.read_bytes()
    leaf_path.unlink()
    calls.clear()
    assert ledger_status(ledger)["valid"] is False
    assert "replay_history" in calls
    leaf_path.parent.mkdir(parents=True, exist_ok=True)
    leaf_path.write_bytes(leaf_raw)


def test_warm_cache_still_verifies_the_active_capsule(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledger = tmp_path / "ledger"
    _ready(ledger)
    case = _plan_case(ledger)
    decision = _seal_case(
        ledger, case, recorded_at="2026-08-24T12:01:00Z"
    )
    _decision_receipt(ledger, decision, run_id=335, signal="2026-08-24")
    assert audit_ledger(ledger)["valid"] is True

    def reject_tampered_capsule(_layout, _state):
        raise LedgerIntegrityError("active release capsule bytes differ")

    monkeypatch.setattr(
        prospective_ledger,
        "_verify_active_runtime_closure",
        reject_tampered_capsule,
    )
    status = ledger_status(ledger)
    assert status["valid"] is False
    assert "capsule bytes differ" in status["issues"][0]["detail"]


def test_rejecting_evaluation_checkpoint_is_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledger = tmp_path / "ledger"
    _ready(ledger)
    original_operation = _run_test_release_operation

    def rejecting_operation(layout, state, operation, payload):
        result = original_operation(layout, state, operation, payload)
        if operation != "evaluate":
            return result
        envelope = result["evaluation_envelope"]
        evaluation = envelope["evaluation"]
        evaluation["reject_major_direction"] = True
        evaluation["direction_gate_passed"] = False
        evaluation["status"] = "reject_major_direction"
        envelope_payload = {
            key: value
            for key, value in envelope.items()
            if key != "evaluation_envelope_sha256"
        }
        envelope["evaluation_envelope_sha256"] = sha256_bytes(
            canonical_json_bytes(envelope_payload)
        )
        return result

    monkeypatch.setattr(
        sys.modules[__name__], "_run_test_release_operation", rejecting_operation
    )
    checkpoint = checkpoint_evaluation(
        ledger, recorded_at_utc="2026-08-24T12:00:00Z"
    )
    assert checkpoint["kind"] == "evaluation_checkpoint"
    assert checkpoint["record"]["payload"]["evaluation"][
        "reject_major_direction"
    ] is True
    status = ledger_status(ledger)
    assert status["status"] == "direction_rejected"
    assert status["direction_rejected"] is True
    assert (
        status["evaluation_checkpoint_record_sha256"]
        == checkpoint["record_sha256"]
    )
    assert evaluate_ledger(ledger) == checkpoint["record"]["payload"]
    with pytest.raises(LedgerStateError, match="non-terminal"):
        checkpoint_evaluation(ledger)


def test_evaluation_due_phase_and_terminal_inflight_outcome_are_consistent() -> None:
    state = prospective_ledger._LedgerState(
        phase="awaiting_decision",
        active_implementation_hash="1" * 64,
        active_implementation={"evaluator_id": "test"},
        active_implementation_canary_receipt_hash="2" * 64,
        active_implementation_tlog_utc="2026-08-22T00:00:00Z",
    )
    for offset in range(10):
        payload = {key: None for key in prospective_ledger.OUTCOME_KEYS}
        payload["cycle_outcome"] = {"offset": offset}
        state.outcome_versions[f"{offset + 10:064x}"] = payload
    prospective_ledger._update_evaluation_gate_after_outcome(
        state,
        ending_nav_fen=1,
    )
    assert state.evaluation_due == "engineering_closure"
    assert state.public_phase() == "awaiting_evaluation"
    assert state.decision_generation_ready() is False
    assert state.evaluation_ready() is True

    state.direction_rejected = True
    state.phase = "direction_rejected"
    state.evaluation_due = None
    prospective_ledger._update_evaluation_gate_after_outcome(
        state,
        ending_nav_fen=0,
    )
    assert state.insolvent is True
    assert state.evaluation_due is None
    assert state.public_phase() == "direction_rejected"
    assert state.decision_generation_ready() is False


def test_audit_and_status_take_the_shared_ledger_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledger = tmp_path / "ledger"
    calls: list[Path] = []
    original = prospective_ledger._exclusive_lock

    @contextmanager
    def recording_lock(layout, **kwargs):
        calls.append(layout.root)
        with original(layout, **kwargs):
            yield

    monkeypatch.setattr(prospective_ledger, "_exclusive_lock", recording_lock)
    assert audit_ledger(ledger)["valid"] is True
    assert ledger_status(ledger)["valid"] is True
    assert calls == [ledger.resolve(), ledger.resolve()]


def test_same_offset_outcomes_require_contiguous_sealed_account_state(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    _ready(ledger)
    extension = list(
        pd.bdate_range("2026-08-24", periods=24).strftime("%Y-%m-%d")
    )
    cases: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    receipts: list[dict[str, Any]] = []
    previous_state: TenSleeveState | None = None
    for index in range(11):
        case = _plan_case(
            ledger,
            signal=extension[index],
            session=extension[index + 1],
            previous_state=previous_state,
            calendar_sessions=extension[: index + 2],
        )
        decision = _seal_case(
            ledger,
            case,
            recorded_at=f"{extension[index]}T12:01:00Z",
        )
        receipt = _decision_receipt(
            ledger, decision, run_id=132 + index, signal=extension[index]
        )
        cases.append(case)
        decisions.append(decision)
        receipts.append(receipt)
        previous_state = case["result"].next_state
    first_case, second_case = cases[0], cases[10]
    first, second = decisions[0], decisions[10]
    first_receipt, second_receipt = receipts[0], receipts[10]
    assert second_case["result"].due_offset == first_case["result"].due_offset
    first_execution = _write_execution_snapshot(ledger, first_case["result"])
    second_execution = _write_execution_snapshot(ledger, second_case["result"])
    with pytest.raises(LedgerStateError, match="oldest open cycle"):
        build_execution_evidence(
            ledger,
            decision_record_sha256=second["record_sha256"],
        )
    with pytest.raises(LedgerStateError, match="oldest open cycle"):
        build_outcome_payload(
            ledger,
            decision_record_sha256=second["record_sha256"],
            execution_snapshot_sha256=second_execution.snapshot_sha256,
        )
    first_rich, first_cycle = _rich_outcome(
        first["record_sha256"],
        first_receipt["record_sha256"],
        first_case["result"],
        first_execution,
    )
    second_rich, second_cycle = _rich_outcome(
        second["record_sha256"],
        second_receipt["record_sha256"],
        second_case["result"],
        second_execution,
        previous_account_state=first_cycle.next_account_state,
    )
    with pytest.raises(LedgerStateError, match="oldest open cycle"):
        append_outcome(
            ledger,
            second_rich,
            recorded_at_utc=f"{second_cycle.holding_end_date}T12:01:00Z",
        )
    append_outcome(
        ledger,
        first_rich,
        recorded_at_utc=f"{first_cycle.holding_end_date}T12:01:00Z",
    )
    assert build_outcome_payload(
        ledger,
        decision_record_sha256=second["record_sha256"],
        execution_snapshot_sha256=second_execution.snapshot_sha256,
    ) == second_rich
    append_outcome(
        ledger,
        second_rich,
        recorded_at_utc=f"{second_cycle.holding_end_date}T12:01:00Z",
    )
    status = ledger_status(ledger)
    offset = str(first_case["result"].due_offset)
    assert (
        status["latest_account_state_sha256_by_offset"][offset]
        == second_cycle.next_account_state.state_sha256
    )
    assert audit_ledger(ledger)["valid"] is True


def test_third_same_offset_decision_waits_for_the_oldest_outcome(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    _ready(ledger)
    extension = list(
        pd.bdate_range("2026-08-24", periods=24).strftime("%Y-%m-%d")
    )
    cases: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    receipts: list[dict[str, Any]] = []
    previous_state: TenSleeveState | None = None
    for index in range(20):
        case = _plan_case(
            ledger,
            signal=extension[index],
            session=extension[index + 1],
            previous_state=previous_state,
            calendar_sessions=extension[: index + 2],
        )
        decision = _seal_case(
            ledger,
            case,
            recorded_at=f"{extension[index]}T12:01:00Z",
        )
        receipt = _decision_receipt(
            ledger,
            decision,
            run_id=500 + index,
            signal=extension[index],
        )
        cases.append(case)
        decisions.append(decision)
        receipts.append(receipt)
        previous_state = case["result"].next_state

    assert cases[0]["result"].due_offset == cases[10]["result"].due_offset
    with pytest.raises(LedgerStateError, match="two same-offset cycles"):
        _plan_case(
            ledger,
            signal=extension[20],
            session=extension[21],
            previous_state=previous_state,
            calendar_sessions=extension[:22],
        )

    execution = _write_execution_snapshot(ledger, cases[0]["result"])
    rich, _cycle = _rich_outcome(
        decisions[0]["record_sha256"],
        receipts[0]["record_sha256"],
        cases[0]["result"],
        execution,
    )
    append_outcome(
        ledger,
        rich,
        recorded_at_utc=f"{extension[20]}T11:59:00Z",
    )
    resumed = _plan_case(
        ledger,
        signal=extension[20],
        session=extension[21],
        previous_state=previous_state,
        calendar_sessions=extension[:22],
    )
    assert resumed["result"].due_offset == cases[0]["result"].due_offset


def test_pending_attestation_and_open_cycles_block_incompatible_writes(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    ready = _ready(ledger)
    plan_case = _plan_case(ledger)
    decision = _seal_case(
        ledger,
        plan_case,
        recorded_at="2026-08-24T12:01:00Z",
    )
    with pytest.raises(LedgerStateError, match="keys differ"):
        append_outcome(
            ledger,
            _outcome(
                decision["record_sha256"],
                "f" * 64,
                start="2026-08-25",
                end="2026-09-04",
            ),
            recorded_at_utc="2026-09-04T12:01:00Z",
        )
    next_upgrade = _upgrade_payload(
        ready["activation"]["record_sha256"],
        supersedes=ready["upgrade"]["record_sha256"],
        release_tag="5.3",
    )
    with pytest.raises(LedgerStateError, match="phase=awaiting_receipt"):
        append_implementation_upgrade(ledger, next_upgrade)
    receipt = _decision_receipt(ledger, decision, run_id=123, signal="2026-08-24")
    with pytest.raises(LedgerStateError, match="open cycle"):
        append_implementation_upgrade(ledger, next_upgrade)
    execution = _write_execution_snapshot(ledger, plan_case["result"])
    rich = build_outcome_payload(
        ledger,
        decision_record_sha256=decision["record_sha256"],
        execution_snapshot_sha256=execution.snapshot_sha256,
    )
    append_outcome(
        ledger,
        rich,
        recorded_at_utc="2026-09-09T12:01:00Z",
    )
    with pytest.raises(LedgerStateError, match="explicit state migration"):
        append_implementation_upgrade(ledger, next_upgrade)


def test_route_plan_structure_and_timing_tampering_is_rejected(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    _ready(ledger)
    case = _plan_case(ledger)
    bad_route = deepcopy(case["plan"])
    bad_route["route_target_plan"]["route"] = "online_overlay"
    with pytest.raises(LedgerStateError, match="route differs"):
        store_decision_plan(ledger, bad_route)
    bad_aggregate = deepcopy(case["plan"])
    bad_aggregate["route_target_plan"]["aggregate_cash_ppm"] -= 1
    with pytest.raises(LedgerStateError, match="aggregate target weights"):
        store_decision_plan(ledger, bad_aggregate)
    bad_timing = deepcopy(case["plan"])
    bad_timing["signal_close_utc"] = "2026-08-23T00:03:00Z"
    with pytest.raises(LedgerStateError, match="decision timing"):
        store_decision_plan(ledger, bad_timing)


def test_initial_seed_cannot_skip_a_post_canary_session(tmp_path: Path) -> None:
    pre_tlog_ledger = tmp_path / "pre-tlog-ledger"
    activation = _activate(pre_tlog_ledger)
    _activation_canary(pre_tlog_ledger, activation)
    upgrade = append_implementation_upgrade(
        pre_tlog_ledger,
        _upgrade_payload(activation["record_sha256"]),
        recorded_at_utc="2026-08-22T00:06:00Z",
    )
    append_attestation_receipt(
        pre_tlog_ledger,
        _receipt(
            upgrade["snapshot"],
            purpose="implementation_upgrade_canary",
            decision_sha=None,
            run_id=622,
            created="2026-08-25T07:30:00Z",
            completed="2026-08-25T08:30:00Z",
            tlog="2026-08-25T08:00:00Z",
        ),
        recorded_at_utc="2026-08-25T09:00:00Z",
    )
    allowed = _plan_case(
        pre_tlog_ledger,
        signal="2026-08-26",
        session="2026-08-27",
        calendar_sessions=[
            "2026-08-24",
            "2026-08-25",
            "2026-08-26",
            "2026-08-27",
        ],
    )
    assert allowed["plan"]["route_target_plan"]["skipped_sessions"] == [
        "2026-08-24",
        "2026-08-25",
    ]

    ledger = tmp_path / "ledger"
    _ready(ledger)
    with pytest.raises(LedgerStateError, match="follows the implementation Tlog"):
        _plan_case(
            ledger,
            signal="2026-08-25",
            session="2026-08-26",
            calendar_sessions=["2026-08-24", "2026-08-25", "2026-08-26"],
        )

    first = _plan_case(ledger)
    decision = _seal_case(
        ledger,
        first,
        recorded_at="2026-08-24T12:01:00Z",
    )
    _decision_receipt(ledger, decision, run_id=623, signal="2026-08-24")
    with pytest.raises(LedgerStateError, match="cannot be skipped"):
        _plan_case(
            ledger,
            signal="2026-08-26",
            session="2026-08-27",
            previous_state=first["result"].next_state,
            calendar_sessions=[
                "2026-08-24",
                "2026-08-25",
                "2026-08-26",
                "2026-08-27",
            ],
        )


def test_record_chain_replay_rejects_a_late_forged_decision(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    _ready(ledger)
    case = _plan_case(ledger)
    status = ledger_status(ledger)
    sequence = int(status["head_sequence"]) + 1
    record = {
        "schema_version": 1,
        "ledger_id": "factor-lab/prospective/5.0",
        "sequence": sequence,
        "kind": "decision",
        "previous_record_sha256": status["head_record_sha256"],
        "recorded_at_utc": case["plan"]["admission_deadline_utc"],
        "clock_source": "local_system_clock_untrusted",
        "payload": {
            "plan_sha256": sha256_bytes(canonical_json_bytes(case["plan"])),
            "plan": case["plan"],
        },
    }
    raw = canonical_json_bytes(record)
    digest = sha256_bytes(raw)
    create_only_file(
        ledger
        / "records"
        / f"{sequence:016d}-decision-{digest}.json",
        raw,
    )
    audited = audit_ledger(ledger)
    assert audited["valid"] is False
    assert audited["issues"][0]["code"] == "invalid_record_chain"
    assert "before admission" in audited["issues"][0]["detail"]


def test_seal_recomputes_genesis_and_rejects_bundle_or_result_forgery(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    _ready(ledger)
    case = _plan_case(ledger)
    with pytest.raises(LedgerStateError, match="before admission"):
        seal_decision(
            ledger,
            case["plan"],
            recorded_at_utc=case["plan"]["admission_deadline_utc"],
        )
    forged_plan = deepcopy(case["plan"])
    forged_result = forged_plan["route_target_plan"]
    forged_result["previous_state_sha256"] = "f" * 64
    result_payload = dict(forged_result)
    del result_payload["result_sha256"]
    forged_result["result_sha256"] = sha256_bytes(canonical_json_bytes(result_payload))
    forged_plan["route_target_plan_sha256"] = sha256_bytes(
        canonical_json_bytes(forged_result)
    )
    with pytest.raises(LedgerStateError, match="fixed generator recomputation"):
        seal_decision(
            ledger,
            forged_plan,
            recorded_at_utc="2026-08-24T12:01:00Z",
        )

    rows_path = case["source_path"] / "rows.json"
    rows_path.write_bytes(rows_path.read_bytes() + b" ")
    with pytest.raises(LedgerStateError, match="release operation"):
        _seal_case(
            ledger,
            case,
            recorded_at="2026-08-24T12:01:00Z",
        )


def test_unhashed_build_receipt_time_cannot_change_generated_targets(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    _ready(ledger)
    case = _plan_case(ledger)
    receipt_path = case["source_path"] / "build-receipt.json"
    receipt = strict_load_canonical(receipt_path.read_bytes())
    receipt["build_completed_at_utc"] = "2026-08-24T23:59:59Z"
    receipt_path.write_bytes(canonical_json_bytes(receipt))
    rebuilt = build_decision_plan(
        ledger,
        decision_session="2026-08-25",
        source_data_snapshot_sha256=case["input_snapshot"].source_data_snapshot_sha256,
        planned_at_utc="2026-08-24T12:00:00Z",
    )
    assert rebuilt == case["plan"]
    assert rebuilt["input_build_checkpoint_utc"] == "2026-08-24T10:00:00Z"
    assert rebuilt["information_cutoff_utc"] == "2026-08-24T10:00:00Z"


def test_public_source_loader_rejects_a_self_consistent_handmade_bundle(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    sessions = [*_canonical_calendar(), "2026-08-24", "2026-08-25"]
    path = _write_source_bundle(
        ledger,
        signal="2026-08-24",
        session="2026-08-25",
        calendar_sessions=sessions,
    )
    with pytest.raises(ProspectiveDataError, match="canonical project"):
        STRICT_SOURCE_LOADER(path)


def test_snapshot_audit_rejects_missing_extra_and_replaced_bytes(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    _ready(ledger)
    assert audit_ledger(ledger)["valid"] is True
    snapshots = sorted((ledger / "snapshots").glob("*.json"))
    victim = snapshots[-1]
    original = victim.read_bytes()
    victim.unlink()
    missing = audit_ledger(ledger)
    assert missing["valid"] is False
    assert any(issue["code"] == "missing_snapshot" for issue in missing["issues"])
    victim.write_bytes(original)
    extra = ledger / "snapshots" / "unexpected.json"
    extra.write_bytes(b"{}")
    assert any(
        issue["code"] == "unexpected_snapshot_file"
        for issue in audit_ledger(ledger)["issues"]
    )
    extra.unlink()
    victim.write_bytes(original + b" ")
    assert any(
        issue["code"] == "invalid_snapshot" for issue in audit_ledger(ledger)["issues"]
    )
    victim.write_bytes(original)
    record_path = sorted((ledger / "records").glob("*.json"))[-1]
    record_path.write_bytes(record_path.read_bytes() + b" ")
    assert any(
        issue["code"] == "invalid_record_chain"
        for issue in audit_ledger(ledger)["issues"]
    )


def test_legacy_plan_records_replay_but_new_seal_rejects_them(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    plan = {
        "schema_version": 1,
        "plan_type": "prospective_decision",
        "ledger_id": "factor-lab/prospective/5.0",
        "activation_record_sha256": activation["record_sha256"],
        "base_head_record_sha256": activation["record_sha256"],
        "decision_id": "5.0/2026-08-24",
        "decision_session": "2026-08-24",
        "information_cutoff_utc": "2026-08-23T11:00:00Z",
        "input_max_available_at_utc": "2026-08-23T10:59:59Z",
        "input_snapshot_sha256": "1" * 64,
        "model_state_sha256": "2" * 64,
        "code_commit_oid": COMMIT_OID,
        "expected_nav_fen": 1,
        "cash_weight_ppm": 0,
        "targets": [{"ticker": "000001.SZ", "target_weight_ppm": 1_000_000}],
        "frozen_route": "fixed_core_full",
        "admission_deadline_utc": "2026-08-24T01:15:00Z",
        "planned_at_utc": "2026-08-23T12:00:00Z",
        "clock_source": "local_system_clock_untrusted",
    }
    with pytest.raises(LedgerStateError, match="replay-only"):
        seal_decision(ledger, plan, recorded_at_utc="2026-08-23T12:01:00Z")
    record = {
        "schema_version": 1,
        "ledger_id": "factor-lab/prospective/5.0",
        "sequence": 2,
        "kind": "decision",
        "previous_record_sha256": activation["record_sha256"],
        "recorded_at_utc": "2026-08-23T12:01:00Z",
        "clock_source": "local_system_clock_untrusted",
        "payload": {
            "plan_sha256": sha256_bytes(canonical_json_bytes(plan)),
            "plan": plan,
        },
    }
    raw = canonical_json_bytes(record)
    digest = sha256_bytes(raw)
    create_only_file(ledger / "records" / f"{2:016d}-decision-{digest}.json", raw)
    activation_payload = activation["record"]["payload"]
    snapshot = _legacy_snapshot(
        {**activation_payload, "activation_record_sha256": activation["record_sha256"]},
        sequence=2,
        head_hash=digest,
        phase="awaiting_receipt",
        decision_count=1,
    )
    snapshot_raw = canonical_json_bytes(snapshot)
    snapshot_hash = sha256_bytes(snapshot_raw)
    create_only_file(
        ledger / "snapshots" / f"{2:016d}-{snapshot_hash}.json", snapshot_raw
    )
    assert audit_ledger(ledger)["valid"] is True


def test_v2_manual_corrections_fail_closed_and_snapshot_is_idempotent(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    _ready(ledger)
    case = _plan_case(ledger)
    decision = _seal_case(
        ledger,
        case,
        recorded_at="2026-08-24T12:01:00Z",
    )
    receipt = _decision_receipt(ledger, decision, run_id=123, signal="2026-08-24")
    execution = _write_execution_snapshot(ledger, case["result"])
    rich = build_outcome_payload(
        ledger,
        decision_record_sha256=decision["record_sha256"],
        execution_snapshot_sha256=execution.snapshot_sha256,
    )
    outcome = append_outcome(
        ledger,
        rich,
        recorded_at_utc="2026-09-09T12:01:00Z",
    )
    payload = {
        "supersedes_record_sha256": outcome["record_sha256"],
        "reason": "first correction",
        "replacement_outcome": _outcome(
            decision["record_sha256"],
            receipt["record_sha256"],
            start="2026-08-25",
            end="2026-09-04",
        ),
        "source_snapshot_sha256": "6" * 64,
    }
    with pytest.raises(LedgerStateError, match="replayable correction schema"):
        append_correction(ledger, payload, recorded_at_utc="2026-09-10T00:00:00Z")
    assert seal_snapshot(ledger)["created"] is False


def test_receipt_persisted_run_and_tlog_bindings_remain_strict(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    receipt = _receipt(
        activation["snapshot"],
        purpose="activation_canary",
        decision_sha=None,
        run_id=121,
        created="2026-08-22T00:02:00Z",
        completed="2026-08-22T00:04:00Z",
        tlog="2026-08-22T00:03:00Z",
    )
    receipt["workflow_run_display_title"] = "prospective-tampered"
    with pytest.raises(LedgerStateError, match="request id"):
        append_attestation_receipt(ledger, receipt)
    receipt["workflow_run_display_title"] = f"prospective-{receipt['request_id']}"
    receipt["subject_name"] = "prospective-snapshot-forged.json"
    with pytest.raises(LedgerStateError, match="subject name"):
        append_attestation_receipt(ledger, receipt)
    receipt["subject_name"] = (
        f"prospective-snapshot-{receipt['snapshot_sha256']}.json"
    )
    receipt["verified_tlog_timestamp_utc"] = "2026-08-22T00:02:59Z"
    with pytest.raises(LedgerStateError, match="earliest verified Tlog"):
        append_attestation_receipt(ledger, receipt)


def test_decision_receipt_tlog_must_be_after_inputs_and_before_admission(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    _ready(ledger)
    case = _plan_case(ledger)
    decision = _seal_case(
        ledger,
        case,
        recorded_at="2026-08-24T12:01:00Z",
    )
    early = _receipt(
        decision["snapshot"],
        purpose="decision_anchor",
        decision_sha=decision["record_sha256"],
        run_id=125,
        created="2026-08-24T12:02:00Z",
        completed="2026-08-24T12:04:00Z",
        tlog="2026-08-24T09:00:00Z",
    )
    with pytest.raises(LedgerStateError, match="predates decision inputs"):
        append_attestation_receipt(
            ledger,
            early,
            recorded_at_utc="2026-08-24T12:05:00Z",
        )
    late = _receipt(
        decision["snapshot"],
        purpose="decision_anchor",
        decision_sha=decision["record_sha256"],
        run_id=126,
        created="2026-08-24T12:02:00Z",
        completed="2026-08-25T01:16:00Z",
        tlog="2026-08-25T01:15:00Z",
    )
    with pytest.raises(LedgerStateError, match="at or after admission"):
        append_attestation_receipt(
            ledger,
            late,
            recorded_at_utc="2026-08-25T01:16:01Z",
        )


def test_concurrent_identical_activation_creates_one_record(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    processes = [
        context.Process(
            target=_concurrent_activate,
            args=(str(ledger), str(PROTOCOL), queue),
        )
        for _ in range(4)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(30)
        assert process.exitcode == 0
    results = [queue.get(timeout=5) for _ in processes]
    assert {kind for kind, _ in results} == {"ok"}, results
    assert len({value for _, value in results}) == 1
    audit = audit_ledger(ledger)
    assert audit["valid"] is True
    assert audit["record_count"] == 1
