from __future__ import annotations

import json
import multiprocessing
from pathlib import Path

import pytest

from factor_lab.prospective_ledger import (
    CanonicalJSONError,
    LedgerIntegrityError,
    LedgerStateError,
    activate_protocol,
    append_attestation_receipt,
    append_correction,
    append_outcome,
    audit_ledger,
    build_decision_plan,
    canonical_json_bytes,
    create_only_file,
    ledger_status,
    seal_decision,
    seal_snapshot,
    sha256_bytes,
    store_decision_plan,
    strict_load_canonical,
)


ROOT = Path(__file__).resolve().parents[2]
PROTOCOL = ROOT / "protocols" / "5.0.json"
TAG_OID = "a" * 40
COMMIT_OID = "b" * 40
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


def _plan(ledger: Path, *, session: str = "2026-08-24", planned_at: str = "2026-08-23T12:00:00Z") -> dict:
    return build_decision_plan(
        ledger,
        decision_session=session,
        information_cutoff_utc="2026-08-23T11:00:00Z",
        input_max_available_at_utc="2026-08-23T10:59:59Z",
        input_snapshot_sha256="1" * 64,
        model_state_sha256="2" * 64,
        code_commit_oid=COMMIT_OID,
        expected_nav_fen=5_000_000_000,
        targets_ppm={"000001.SZ": 600_000, "600000.SH": 400_000},
        planned_at_utc=planned_at,
    )


def _receipt(
    snapshot: MappingLike,
    *,
    purpose: str,
    decision_sha: str | None,
    created: str = "2026-08-23T12:02:00Z",
) -> dict:
    snapshot_sha = str(snapshot["snapshot_sha256"])
    return {
        "purpose": purpose,
        "snapshot_sha256": snapshot_sha,
        "snapshot_head_record_sha256": snapshot["snapshot"]["head_record_sha256"],
        "decision_record_sha256": decision_sha,
        "request_id": "3" * 64,
        "workflow_run_id": 123,
        "workflow_run_attempt": 1,
        "workflow_run_display_title": f"prospective-{'3' * 64}",
        "workflow_run_url": "https://github.com/yxforever666gh/factor-lab/actions/runs/123",
        "workflow_run_created_at_utc": created,
        "workflow_run_completed_at_utc": "2026-08-23T12:03:00Z",
        "workflow_path": ".github/workflows/prospective-attest.yml",
        "workflow_ref": "refs/tags/5.0",
        "workflow_source_commit_oid": COMMIT_OID,
        "attestation_bundle_sha256": "4" * 64,
        "certificate_identity": (
            "https://github.com/yxforever666gh/factor-lab/"
            ".github/workflows/prospective-attest.yml@refs/tags/5.0"
        ),
        "run_invocation_uri": (
            "https://github.com/yxforever666gh/factor-lab/"
            "actions/runs/123/attempts/1"
        ),
        "verified_timestamp_count": 1,
        "verified_timestamps": [
            {
                "type": "Tlog",
                "uri": "https://rekor.sigstore.dev/api/v1/log/entries/abc",
                "timestamp_utc": "2026-08-23T12:03:00Z",
            }
        ],
        "verified_tlog_type": "Tlog",
        "verified_tlog_uri": (
            "https://rekor.sigstore.dev/api/v1/log/entries/abc"
        ),
        "verified_tlog_timestamp_utc": "2026-08-23T12:03:00Z",
        "subject_name": f"prospective-snapshot-{snapshot_sha}.json",
        "subject_sha256": snapshot_sha,
    }


def _outcome(decision_sha: str, receipt_sha: str, *, net_return: int = 20_000_000) -> dict:
    return {
        "decision_record_sha256": decision_sha,
        "attestation_receipt_record_sha256": receipt_sha,
        "holding_start_date": "2026-08-24",
        "holding_end_date": "2026-09-04",
        "observation_available_at_utc": "2026-09-04T12:00:00Z",
        "source_snapshot_sha256": "5" * 64,
        "execution_status": "complete",
        "gross_return_ppb": 21_000_000,
        "net_return_ppb": net_return,
        "benchmark_return_ppb": 10_000_000,
        "turnover_ppm": 250_000,
        "fees_fen": 12_345,
        "ending_nav_fen": 5_100_000_000,
    }


MappingLike = dict[str, object]


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
    except Exception as exc:  # pragma: no cover - surfaced in the parent assertion
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


def test_full_activation_decision_receipt_outcome_and_correction_chain(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    assert activation["sequence"] == 1
    assert activation["created"] is True
    assert activation["snapshot"]["snapshot"]["phase"] == "awaiting_new_data"
    assert activation["record"]["payload"]["authoritative_run_id"] == RUN_ID
    assert activation["snapshot"]["snapshot"]["frozen_route"] == "fixed_core_full"
    initial_status = ledger_status(ledger)
    assert initial_status["status"] == "awaiting_new_data"
    assert initial_status["awaiting_new_data"] is True
    assert initial_status["decision_count"] == 0
    assert initial_status["confirmed_observation_count"] == 0
    assert initial_status["run_fingerprint"] == AUTHORITATIVE_RUN["run_fingerprint"]
    assert initial_status["manifest_sha256"] == AUTHORITATIVE_RUN["manifest_sha256"]
    assert initial_status["manifest_self_sha256"] == AUTHORITATIVE_RUN[
        "manifest_self_sha256"
    ]
    assert initial_status["adaptive_summary_sha256"] == AUTHORITATIVE_RUN[
        "adaptive_summary_sha256"
    ]
    assert initial_status["frozen_route"] == "fixed_core_full"
    assert initial_status["integrity_valid"] is True
    assert _activate(ledger)["created"] is False

    changed_binding = {**AUTHORITATIVE_RUN, "manifest_sha256": "2" * 64}
    with pytest.raises(LedgerStateError, match="already activated differently"):
        activate_protocol(
            ledger,
            protocol_path=PROTOCOL,
            release_tag="5.0",
            release_tag_object_oid=TAG_OID,
            release_commit_oid=COMMIT_OID,
            authoritative_run=changed_binding,
            recorded_at_utc="2026-08-22T00:00:00Z",
        )

    plan = _plan(ledger)
    assert plan["frozen_route"] == "fixed_core_full"
    changed_plan = {**plan, "frozen_route": "online_overlay"}
    with pytest.raises(LedgerStateError, match="frozen route differs"):
        store_decision_plan(ledger, changed_plan)
    stored = store_decision_plan(ledger, plan)
    assert stored["created"] is True
    assert store_decision_plan(ledger, plan)["created"] is False
    decision = seal_decision(ledger, stored["path"], recorded_at_utc="2026-08-23T12:01:00Z")
    assert decision["sequence"] == 2
    assert decision["snapshot"]["snapshot"]["phase"] == "awaiting_receipt"

    receipt = append_attestation_receipt(
        ledger,
        _receipt(
            decision["snapshot"],
            purpose="decision_anchor",
            decision_sha=decision["record_sha256"],
        ),
        recorded_at_utc="2026-08-23T12:04:00Z",
    )
    outcome = append_outcome(
        ledger,
        _outcome(decision["record_sha256"], receipt["record_sha256"]),
        recorded_at_utc="2026-09-04T12:01:00Z",
    )
    replacement = _outcome(
        decision["record_sha256"], receipt["record_sha256"], net_return=19_000_000
    )
    correction = append_correction(
        ledger,
        {
            "supersedes_record_sha256": outcome["record_sha256"],
            "reason": "vendor fee correction",
            "replacement_outcome": replacement,
            "source_snapshot_sha256": "6" * 64,
        },
        recorded_at_utc="2026-09-05T00:00:00Z",
    )
    assert correction["sequence"] == 5

    audit = audit_ledger(ledger)
    assert audit["valid"] is True
    assert [row["kind"] for row in audit["records"]] == [
        "protocol_activation",
        "decision",
        "attestation_receipt",
        "outcome",
        "correction",
    ]
    status = ledger_status(ledger)
    assert status["status"] == "awaiting_decision"
    assert status["decision_count"] == 1
    assert status["confirmed_observation_count"] == 1

    record_path = Path(audit["records"][0]["path"])
    assert record_path.name.endswith(f"-{sha256_bytes(record_path.read_bytes())}.json")
    assert "record_sha256" not in json.loads(record_path.read_text(encoding="utf-8"))
    assert seal_snapshot(ledger)["created"] is False


def test_activation_canary_is_allowed_once_and_makes_older_plans_stale(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    stale_plan = _plan(ledger)
    append_attestation_receipt(
        ledger,
        _receipt(
            activation["snapshot"],
            purpose="activation_canary",
            decision_sha=None,
        ),
        recorded_at_utc="2026-08-22T00:05:00Z",
    )
    canary_status = ledger_status(ledger)
    assert canary_status["status"] == "awaiting_new_data"
    assert canary_status["awaiting_new_data"] is True
    assert canary_status["confirmed_observation_count"] == 0
    with pytest.raises(LedgerStateError, match="stale"):
        seal_decision(ledger, stale_plan, recorded_at_utc="2026-08-23T12:01:00Z")


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("workflow_run_attempt", 2, "exact run attempt"),
        ("workflow_run_display_title", "prospective-tampered", "request id"),
        (
            "run_invocation_uri",
            "https://github.com/yxforever666gh/factor-lab/actions/runs/999/attempts/1",
            "exact run attempt",
        ),
        ("verified_tlog_type", "TimestampAuthority", "transparency-log"),
        (
            "verified_tlog_timestamp_utc",
            "2026-08-23T12:02:59Z",
            "earliest verified Tlog",
        ),
    ],
)
def test_receipt_rejects_tampered_persisted_run_and_tlog_bindings(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    receipt = _receipt(
        activation["snapshot"], purpose="activation_canary", decision_sha=None
    )
    receipt[field] = value

    with pytest.raises(LedgerStateError, match=message):
        append_attestation_receipt(
            ledger,
            receipt,
            recorded_at_utc="2026-08-23T12:04:00Z",
        )


def test_causality_deadline_weights_and_state_machine_fail_closed(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    _activate(ledger)
    with pytest.raises(LedgerStateError, match="strictly after"):
        _plan(ledger, session="2026-08-21")
    with pytest.raises(LedgerStateError, match="deadline"):
        _plan(ledger, planned_at="2026-08-24T01:15:00Z")
    with pytest.raises(LedgerStateError, match="total"):
        build_decision_plan(
            ledger,
            decision_session="2026-08-24",
            information_cutoff_utc="2026-08-23T11:00:00Z",
            input_max_available_at_utc="2026-08-23T10:00:00Z",
            input_snapshot_sha256="1" * 64,
            model_state_sha256="2" * 64,
            code_commit_oid=COMMIT_OID,
            expected_nav_fen=1,
            targets_ppm={"000001.SZ": 999_999},
            planned_at_utc="2026-08-23T12:00:00Z",
        )
    plan = _plan(ledger)
    with pytest.raises(LedgerStateError, match="sealed at or after"):
        seal_decision(ledger, plan, recorded_at_utc="2026-08-24T01:15:00Z")
    decision = seal_decision(ledger, plan, recorded_at_utc="2026-08-23T12:01:00Z")
    with pytest.raises(LedgerStateError, match="currently attested"):
        append_outcome(
            ledger,
            _outcome(decision["record_sha256"], "9" * 64),
            recorded_at_utc="2026-09-04T12:01:00Z",
        )
    late = _receipt(
        decision["snapshot"],
        purpose="decision_anchor",
        decision_sha=decision["record_sha256"],
    )
    late["verified_timestamps"][0]["timestamp_utc"] = "2026-08-24T01:15:00Z"
    late["verified_tlog_timestamp_utc"] = "2026-08-24T01:15:00Z"
    with pytest.raises(LedgerStateError, match="transparency-log timestamp is at or after"):
        append_attestation_receipt(
            ledger, late, recorded_at_utc="2026-08-24T01:16:00Z"
        )


def test_tampering_and_correction_forks_are_detected(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    record_path = Path(activation["path"])
    record_path.write_bytes(record_path.read_bytes() + b" ")
    audit = audit_ledger(ledger)
    assert audit["valid"] is False
    assert audit["issues"][0]["code"] == "invalid_record_chain"

    clean = tmp_path / "clean-ledger"
    _activate(clean)
    decision = seal_decision(clean, _plan(clean), recorded_at_utc="2026-08-23T12:01:00Z")
    receipt = append_attestation_receipt(
        clean,
        _receipt(decision["snapshot"], purpose="decision_anchor", decision_sha=decision["record_sha256"]),
        recorded_at_utc="2026-08-23T12:04:00Z",
    )
    outcome = append_outcome(
        clean,
        _outcome(decision["record_sha256"], receipt["record_sha256"]),
        recorded_at_utc="2026-09-04T12:01:00Z",
    )
    correction_payload = {
        "supersedes_record_sha256": outcome["record_sha256"],
        "reason": "first correction",
        "replacement_outcome": _outcome(decision["record_sha256"], receipt["record_sha256"]),
        "source_snapshot_sha256": "6" * 64,
    }
    append_correction(clean, correction_payload, recorded_at_utc="2026-09-05T00:00:00Z")
    with pytest.raises(LedgerStateError, match="fork"):
        append_correction(clean, correction_payload, recorded_at_utc="2026-09-05T01:00:00Z")


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
