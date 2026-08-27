from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

from factor_lab.prospective_attestation import (
    API_VERSION,
    ATTEST_ACTION_COMMIT,
    AttestationError,
    attestation_request_id,
    build_attestation_download_command,
    build_attestation_verify_command,
    build_dispatch_request,
    build_receipt_payload,
    build_workflow_run_command,
    certificate_identity,
    parse_dispatch_response,
    store_attestation_bundle,
    validate_verification_output,
    validate_workflow_run,
)
from factor_lab.prospective_ledger import canonical_json_bytes, sha256_bytes


ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = "yxforever666gh/factor-lab"
COMMIT_OID = "b" * 40
HEAD_SHA = "c" * 64


def _snapshot() -> bytes:
    return canonical_json_bytes(
        {
            "schema_version": 1,
            "ledger_id": "factor-lab/prospective/5.0",
            "head_sequence": 2,
            "head_record_sha256": HEAD_SHA,
            "activation_record_sha256": "a" * 64,
            "protocol_id": "factor-lab/5.0/adaptive-core-overlay",
            "protocol_sha256": "d" * 64,
            "release_tag": "5.0",
            "release_commit_oid": COMMIT_OID,
            "phase": "awaiting_receipt",
            "decision_count": 1,
            "confirmed_observation_count": 0,
        }
    )


def _dispatch_response() -> bytes:
    return json.dumps(
        {
            "workflow_run_id": 123,
            "run_url": "https://api.github.com/repos/yxforever666gh/factor-lab/actions/runs/123",
            "html_url": "https://github.com/yxforever666gh/factor-lab/actions/runs/123",
        }
    ).encode()


def _run_payload() -> dict:
    request_id = build_dispatch_request(_snapshot()).request_id
    return {
        "id": 123,
        "event": "workflow_dispatch",
        "status": "completed",
        "conclusion": "success",
        "head_sha": COMMIT_OID,
        "head_branch": "5.0",
        "display_title": f"prospective-{request_id}",
        "run_attempt": 1,
        "path": ".github/workflows/prospective-attest.yml@5.0",
        "html_url": "https://github.com/yxforever666gh/factor-lab/actions/runs/123",
        "created_at": "2026-08-23T12:02:00Z",
        "updated_at": "2026-08-23T12:03:00Z",
    }


def _verification_payload(
    snapshot_sha: str,
    snapshot_name: str,
    *,
    workflow_run_id: int = 123,
    workflow_run_attempt: int = 1,
    tlog_timestamp: str = "2026-08-23T12:03:00Z",
) -> list[dict]:
    identity = certificate_identity(repository=REPOSITORY, release_tag="5.0")
    return [
        {
            "attestation": {"mediaType": "application/vnd.dev.sigstore.bundle+json;version=0.3"},
            "verificationResult": {
                "statement": {
                    "predicateType": "https://slsa.dev/provenance/v1",
                    "subject": [
                        {"name": snapshot_name, "digest": {"sha256": snapshot_sha}}
                    ],
                    "predicate": {},
                },
                "signature": {
                    "certificate": {
                        "subjectAlternativeName": identity,
                        "runnerEnvironment": "github-hosted",
                        "runInvocationURI": (
                            "https://github.com/yxforever666gh/factor-lab/"
                            f"actions/runs/{workflow_run_id}/attempts/"
                            f"{workflow_run_attempt}"
                        ),
                    }
                },
                "verifiedTimestamps": [
                    {
                        "type": "Tlog",
                        "uri": "https://rekor.sigstore.dev/api/v1/log/entries/abc",
                        "timestamp": tlog_timestamp,
                    }
                ],
            },
        }
    ]


def test_dispatch_request_is_exact_and_deterministic() -> None:
    snapshot = _snapshot()
    request = build_dispatch_request(snapshot)
    repeated = build_dispatch_request(snapshot)

    assert request == repeated
    assert request.snapshot_sha256 == sha256_bytes(snapshot)
    assert request.snapshot_name == f"prospective-snapshot-{request.snapshot_sha256}.json"
    assert request.request_id == attestation_request_id(
        repository=REPOSITORY,
        release_tag="5.0",
        workflow="prospective-attest.yml",
        snapshot_sha256=request.snapshot_sha256,
    )
    assert request.command.argv == (
        "gh",
        "api",
        "repos/yxforever666gh/factor-lab/actions/workflows/prospective-attest.yml/dispatches",
        "--method",
        "POST",
        "-H",
        "Accept: application/vnd.github+json",
        "-H",
        f"X-GitHub-Api-Version: {API_VERSION}",
        "--input",
        "-",
    )
    body = json.loads(request.command.stdin)
    assert body["ref"] == "5.0"
    assert body["inputs"]["request_id"] == request.request_id
    assert body["inputs"]["snapshot_sha256"] == request.snapshot_sha256
    assert base64.b64decode(body["inputs"]["snapshot_b64"], validate=True) == snapshot


def test_dispatch_response_and_workflow_run_are_strict() -> None:
    dispatch = parse_dispatch_response(_dispatch_response())
    assert dispatch["workflow_run_id"] == 123
    command = build_workflow_run_command(123)
    assert command.argv[2] == "repos/yxforever666gh/factor-lab/actions/runs/123"
    run = validate_workflow_run(
        _run_payload(),
        workflow_run_id=123,
        request_id=build_dispatch_request(_snapshot()).request_id,
        release_tag="5.0",
        release_commit_oid=COMMIT_OID,
        admission_deadline_utc="2026-08-24T01:15:00Z",
    )
    assert run["workflow_ref"] == "refs/tags/5.0"
    assert run["workflow_source_commit_oid"] == COMMIT_OID
    assert run["workflow_run_attempt"] == 1

    with pytest.raises(AttestationError, match="duplicate"):
        parse_dispatch_response(
            '{"workflow_run_id":123,"workflow_run_id":124,'
            '"run_url":"https://api.github.com/x","html_url":"https://github.com/x"}'
        )
    with pytest.raises(AttestationError, match="positive"):
        parse_dispatch_response(
            '{"workflow_run_id":0,"run_url":"https://api.github.com/x",'
            '"html_url":"https://github.com/x"}'
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("event", "push", "workflow_dispatch"),
        ("status", "in_progress", "completed successfully"),
        ("conclusion", "failure", "completed successfully"),
        ("head_sha", "a" * 40, "head SHA"),
        ("head_branch", "main", "head branch"),
        ("display_title", "prospective-" + "0" * 64, "request id"),
        ("run_attempt", 0, "run attempt"),
        ("path", ".github/workflows/prospective-attest.yml@refs/heads/main", "path/ref"),
        ("created_at", "2026-08-24T01:15:00Z", "at or after"),
    ],
)
def test_workflow_run_rejects_wrong_identity_or_late_dispatch(
    field: str,
    value: object,
    message: str,
) -> None:
    payload = _run_payload()
    payload[field] = value
    if field == "created_at":
        # Keep the completion timestamp internally consistent so this case
        # reaches the admission-deadline gate it is intended to exercise.
        payload["updated_at"] = "2026-08-24T01:16:00Z"
    with pytest.raises(AttestationError, match=message):
        validate_workflow_run(
            payload,
            workflow_run_id=123,
            request_id=build_dispatch_request(_snapshot()).request_id,
            release_tag="5.0",
            release_commit_oid=COMMIT_OID,
            admission_deadline_utc="2026-08-24T01:15:00Z",
        )


def test_download_verify_commands_and_verification_output_are_pinned(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "snapshot.json"
    snapshot_path.write_bytes(_snapshot())
    bundle_path = tmp_path / "bundle.jsonl"
    bundle_path.write_text("{}\n", encoding="utf-8")

    download = build_attestation_download_command(
        snapshot_path,
        output_directory=tmp_path / "download",
    )
    assert download.argv[:3] == ("gh", "attestation", "download")
    assert download.cwd == tmp_path / "download"

    verify = build_attestation_verify_command(
        snapshot_path,
        bundle_path,
        release_commit_oid=COMMIT_OID,
    )
    arguments = list(verify.argv)
    assert arguments[arguments.index("--source-ref") + 1] == "refs/tags/5.0"
    assert arguments[arguments.index("--source-digest") + 1] == COMMIT_OID
    assert arguments[arguments.index("--signer-workflow") + 1] == (
        "yxforever666gh/factor-lab/.github/workflows/prospective-attest.yml"
    )
    assert "--deny-self-hosted-runners" in arguments

    request = build_dispatch_request(_snapshot())
    evidence = validate_verification_output(
        _verification_payload(request.snapshot_sha256, request.snapshot_name),
        snapshot_sha256=request.snapshot_sha256,
        snapshot_name=request.snapshot_name,
        expected_certificate_identity=certificate_identity(),
        repository=REPOSITORY,
        workflow_run_id=123,
        workflow_run_attempt=1,
        admission_deadline_utc="2026-08-24T01:15:00Z",
    )
    assert evidence["verified_timestamp_count"] == 1
    assert evidence["verified_timestamps"] == [
        {
            "type": "Tlog",
            "uri": "https://rekor.sigstore.dev/api/v1/log/entries/abc",
            "timestamp_utc": "2026-08-23T12:03:00Z",
        }
    ]
    assert evidence["subject_sha256"] == request.snapshot_sha256
    assert evidence["verified_tlog_type"] == "Tlog"
    assert evidence["verified_tlog_timestamp_utc"] == "2026-08-23T12:03:00Z"
    assert evidence["run_invocation_uri"].endswith("/runs/123/attempts/1")

    wrong = _verification_payload(request.snapshot_sha256, request.snapshot_name)
    wrong[0]["verificationResult"]["signature"]["certificate"] = {
        "subjectAlternativeName": "https://github.com/attacker/workflow"
    }
    with pytest.raises(AttestationError, match="exactly one"):
        validate_verification_output(
            wrong,
            snapshot_sha256=request.snapshot_sha256,
            snapshot_name=request.snapshot_name,
            expected_certificate_identity=certificate_identity(),
            repository=REPOSITORY,
            workflow_run_id=123,
            workflow_run_attempt=1,
        )
    with pytest.raises(AttestationError, match="no verification"):
        validate_verification_output(
            [],
            snapshot_sha256=request.snapshot_sha256,
            snapshot_name=request.snapshot_name,
            expected_certificate_identity=certificate_identity(),
            repository=REPOSITORY,
            workflow_run_id=123,
            workflow_run_attempt=1,
        )


def test_verification_selects_only_the_exact_run_attempt_from_same_subject() -> None:
    request = build_dispatch_request(_snapshot())
    payload = _verification_payload(
        request.snapshot_sha256,
        request.snapshot_name,
        workflow_run_id=999,
        workflow_run_attempt=4,
    ) + _verification_payload(
        request.snapshot_sha256,
        request.snapshot_name,
        workflow_run_id=123,
        workflow_run_attempt=2,
    )

    evidence = validate_verification_output(
        payload,
        snapshot_sha256=request.snapshot_sha256,
        snapshot_name=request.snapshot_name,
        expected_certificate_identity=certificate_identity(),
        repository=REPOSITORY,
        workflow_run_id=123,
        workflow_run_attempt=2,
    )

    assert evidence["workflow_run_id"] == 123
    assert evidence["workflow_run_attempt"] == 2
    assert evidence["run_invocation_uri"].endswith("/runs/123/attempts/2")


def test_verification_rejects_tampered_run_invocation_and_late_tlog() -> None:
    request = build_dispatch_request(_snapshot())
    wrong_run = _verification_payload(
        request.snapshot_sha256,
        request.snapshot_name,
        workflow_run_id=124,
    )
    with pytest.raises(AttestationError, match="exactly one"):
        validate_verification_output(
            wrong_run,
            snapshot_sha256=request.snapshot_sha256,
            snapshot_name=request.snapshot_name,
            expected_certificate_identity=certificate_identity(),
            repository=REPOSITORY,
            workflow_run_id=123,
            workflow_run_attempt=1,
        )

    late = _verification_payload(
        request.snapshot_sha256,
        request.snapshot_name,
        tlog_timestamp="2026-08-24T01:15:00Z",
    )
    with pytest.raises(AttestationError, match="transparency-log timestamp is at or after"):
        validate_verification_output(
            late,
            snapshot_sha256=request.snapshot_sha256,
            snapshot_name=request.snapshot_name,
            expected_certificate_identity=certificate_identity(),
            repository=REPOSITORY,
            workflow_run_id=123,
            workflow_run_attempt=1,
            admission_deadline_utc="2026-08-24T01:15:00Z",
        )


def test_bundle_storage_and_receipt_payload_are_create_only(tmp_path: Path) -> None:
    snapshot = _snapshot()
    request = build_dispatch_request(snapshot)
    dispatch = parse_dispatch_response(_dispatch_response())
    run = validate_workflow_run(
        _run_payload(),
        workflow_run_id=123,
        request_id=request.request_id,
        release_tag="5.0",
        release_commit_oid=COMMIT_OID,
        admission_deadline_utc="2026-08-24T01:15:00Z",
    )
    verification = validate_verification_output(
        _verification_payload(request.snapshot_sha256, request.snapshot_name),
        snapshot_sha256=request.snapshot_sha256,
        snapshot_name=request.snapshot_name,
        expected_certificate_identity=certificate_identity(),
        repository=REPOSITORY,
        workflow_run_id=123,
        workflow_run_attempt=1,
        admission_deadline_utc="2026-08-24T01:15:00Z",
    )
    source = tmp_path / "download" / "sha256-bundle.jsonl"
    source.parent.mkdir()
    source.write_text('{"bundle":"signed"}\n', encoding="utf-8")
    stored = store_attestation_bundle(
        source,
        tmp_path / "ledger" / "bundles",
        snapshot_sha256=request.snapshot_sha256,
    )
    assert stored["created"] is True
    assert store_attestation_bundle(
        source,
        tmp_path / "ledger" / "bundles",
        snapshot_sha256=request.snapshot_sha256,
    )["created"] is False

    receipt = build_receipt_payload(
        purpose="decision_anchor",
        snapshot=snapshot,
        request=request,
        dispatch_response=dispatch,
        workflow_run=run,
        verification=verification,
        attestation_bundle_sha256=stored["attestation_bundle_sha256"],
        decision_record_sha256=HEAD_SHA,
        admission_deadline_utc="2026-08-24T01:15:00Z",
    )
    assert receipt["decision_record_sha256"] == HEAD_SHA
    assert receipt["workflow_run_id"] == 123
    assert receipt["workflow_run_attempt"] == 1
    assert receipt["workflow_run_display_title"] == f"prospective-{request.request_id}"
    assert receipt["run_invocation_uri"].endswith("/runs/123/attempts/1")
    assert receipt["verified_tlog_timestamp_utc"] == "2026-08-23T12:03:00Z"
    assert receipt["snapshot_sha256"] == request.snapshot_sha256


def test_workflow_contract_is_dispatch_only_and_fully_pinned() -> None:
    workflow = (ROOT / ".github" / "workflows" / "prospective-attest.yml").read_text(
        encoding="utf-8"
    )
    assert "on:\n  workflow_dispatch:" in workflow
    assert "\n  push:" not in workflow
    assert "\n  pull_request:" not in workflow
    assert "id-token: write" in workflow
    assert "attestations: write" in workflow
    assert 'test "$GITHUB_REF" = "refs/tags/$RELEASE_TAG"' in workflow
    assert "snapshot digest mismatch" in workflow
    assert "snapshot release commit mismatch" in workflow
    assert f"actions/attest@{ATTEST_ACTION_COMMIT}" in workflow
    assert "actions/attest@v" not in workflow
    assert "subject-path:" in workflow
    assert "create-storage-record: false" in workflow
