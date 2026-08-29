from __future__ import annotations

import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import threading
from typing import Any

import pytest

import factor_lab.prospective_runtime as prospective_runtime
from factor_lab.prospective_attestation import (
    CommandSpec,
    build_dispatch_request,
    certificate_identity,
)
from factor_lab.prospective_ledger import (
    activate_protocol,
    audit_ledger,
    canonical_json_bytes,
    create_only_file,
    ledger_status,
    seal_snapshot,
    sha256_bytes,
)
from factor_lab.prospective_runtime import (
    CommandResult,
    ProspectiveRuntimeError,
    attest_snapshot,
    run_command,
    verify_authoritative_run,
)


ROOT = Path(__file__).resolve().parents[2]
TAG_OID = "a" * 40
COMMIT_OID = "b" * 40
RUN_ID = 9345
_REQUEST_TITLE = "__prospective_request_title__"
AUTHORITATIVE_RUN_ID = "c" * 16
AUTHORITATIVE_RUN = {
    "authoritative_run_id": AUTHORITATIVE_RUN_ID,
    "run_fingerprint": AUTHORITATIVE_RUN_ID + "d" * 48,
    "manifest_sha256": "e" * 64,
    "manifest_self_sha256": "f" * 64,
    "adaptive_summary_sha256": "1" * 64,
    "frozen_route": "fixed_core_full",
    "integrity_valid": True,
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _sigstore_bundle(
    snapshot_path: Path,
    *,
    run_id: int,
    run_attempt: int,
    tlog_timestamp: str,
) -> bytes:
    snapshot_sha = snapshot_path.stem.removeprefix("prospective-snapshot-")
    invocation = (
        "https://github.com/yxforever666gh/factor-lab/"
        f"actions/runs/{run_id}/attempts/{run_attempt}"
    )
    statement = {
        "_type": "https://in-toto.io/Statement/v1",
        "subject": [
            {"name": snapshot_path.name, "digest": {"sha256": snapshot_sha}}
        ],
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
                        "digest": {"gitCommit": COMMIT_OID},
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
        int(
            datetime.fromisoformat(
                tlog_timestamp[:-1] + "+00:00"
            ).timestamp()
        )
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
    return canonical_json_bytes(bundle) + b"\n"


def _manifest_self_sha256(manifest: dict[str, Any]) -> str:
    payload = dict(manifest)
    payload.pop("manifest_sha256", None)
    raw = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(raw).hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _authoritative_run_fixture(
    root: Path,
    *,
    commit_oid: str = COMMIT_OID,
) -> tuple[str, Path, Path]:
    protocol_path = ROOT / "protocols" / "5.0.json"
    protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
    protocol_sha256 = _sha256(protocol_path)
    run_id = "2" * 16
    fingerprint = run_id + "3" * 48
    run_dir = root / "runtime" / "runs" / run_id
    comparisons = {
        name: {
            "phase_deltas": {
                "net_annual_return": {"q20": 0.1},
                "net_sharpe": {"q20": 0.1},
                "max_drawdown": {"q20": 0.1},
            },
            "positive_annual_return_delta_ratio": 1.0,
        }
        for name in (
            "core_overlay",
            "online_vs_static",
            "online_overlay_effect",
            "combined",
        )
    }
    from factor_lab.research.adaptive_runtime import (
        _determine_route,
        _evaluate_frozen_gates,
    )

    gates = _evaluate_frozen_gates(
        protocol["frozen_gates"],
        comparisons,
        mean_overlay_fraction=0.5,
    )
    route = _determine_route(gates, integrity_passed=True)
    adaptive = {
        "enabled": True,
        "canary_smoke_only": False,
        "protocol_id": protocol["protocol_id"],
        "protocol_sha256": protocol_sha256,
        "integrity_valid": True,
        "runtime_integrity": {
            "passed": True,
            "criteria": [{"criterion": "fixture", "passed": True}],
        },
        "paired_comparisons": comparisons,
        "mean_fraction_signal_dates_exposure_below_one": 0.5,
        "gate_results": gates,
        "frozen_gate_results": gates,
        "route": route,
        "frozen_route": route["selected_account"],
    }
    adaptive_path = run_dir / "adaptive" / "adaptive-summary.json"
    _write_json(adaptive_path, adaptive)
    summary = {
        "status": "completed",
        "suite": "adaptive",
        "mode": "full",
        "canary_smoke_only": False,
        "run_id": run_id,
        "run_fingerprint": fingerprint,
        "git": {"commit": commit_oid, "dirty": False},
        "adaptive_results_interpretable": True,
        "adaptive": adaptive,
    }
    summary_path = run_dir / "summary.json"
    _write_json(summary_path, summary)
    manifest = {
        "schema_version": 2,
        "algorithm": "sha256",
        "run_id": run_id,
        "run_fingerprint": fingerprint,
        "inputs": [
            {
                "role": "adaptive_protocol",
                "path": str(protocol_path.resolve()),
                "size_bytes": protocol_path.stat().st_size,
                "sha256": protocol_sha256,
            }
        ],
        "files": [
            {
                "path": "adaptive/adaptive-summary.json",
                "size_bytes": adaptive_path.stat().st_size,
                "sha256": _sha256(adaptive_path),
            },
            {
                "path": "summary.json",
                "size_bytes": summary_path.stat().st_size,
                "sha256": _sha256(summary_path),
            },
        ],
    }
    manifest["manifest_sha256"] = _manifest_self_sha256(manifest)
    _write_json(run_dir / "manifest.json", manifest)
    return run_id, run_dir, protocol_path


def _resign_fixture(run_dir: Path) -> None:
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for row in manifest["files"]:
        path = run_dir.joinpath(*Path(row["path"]).parts)
        row["size_bytes"] = path.stat().st_size
        row["sha256"] = _sha256(path)
    manifest["manifest_sha256"] = _manifest_self_sha256(manifest)
    _write_json(manifest_path, manifest)


def _activate(ledger: Path) -> dict[str, Any]:
    return activate_protocol(
        ledger,
        protocol_path=ROOT / "protocols" / "5.0.json",
        release_tag="5.0",
        release_tag_object_oid=TAG_OID,
        release_commit_oid=COMMIT_OID,
        authoritative_run=AUTHORITATIVE_RUN,
        recorded_at_utc="2026-08-22T00:00:00Z",
    )


def _decision(ledger: Path) -> dict[str, Any]:
    """Inject one replay-only v1 record for attestation transport tests.

    Production sealing deliberately rejects this legacy manual-target schema;
    these tests need only a historical decision snapshot to exercise remote
    workflow resume and Tlog deadline validation.
    """

    activation_hash = str(ledger_status(ledger)["activation_record_sha256"])
    plan = {
        "schema_version": 1,
        "plan_type": "prospective_decision",
        "ledger_id": "factor-lab/prospective/5.0",
        "activation_record_sha256": activation_hash,
        "base_head_record_sha256": activation_hash,
        "decision_id": "5.0/2026-08-24",
        "decision_session": "2026-08-24",
        "information_cutoff_utc": "2026-08-23T11:00:00Z",
        "input_max_available_at_utc": "2026-08-23T10:00:00Z",
        "input_snapshot_sha256": "1" * 64,
        "model_state_sha256": "2" * 64,
        "code_commit_oid": COMMIT_OID,
        "expected_nav_fen": 5_000_000_000,
        "cash_weight_ppm": 0,
        "targets": [
            {"ticker": "000001.SZ", "target_weight_ppm": 600_000},
            {"ticker": "510300.SH", "target_weight_ppm": 400_000},
        ],
        "frozen_route": "fixed_core_full",
        "admission_deadline_utc": "2026-08-24T01:15:00Z",
        "planned_at_utc": "2026-08-23T12:00:00Z",
        "clock_source": "local_system_clock_untrusted",
    }
    record = {
        "schema_version": 1,
        "ledger_id": "factor-lab/prospective/5.0",
        "sequence": 2,
        "kind": "decision",
        "previous_record_sha256": activation_hash,
        "recorded_at_utc": "2026-08-23T12:01:00Z",
        "clock_source": "local_system_clock_untrusted",
        "payload": {
            "plan_sha256": sha256_bytes(canonical_json_bytes(plan)),
            "plan": plan,
        },
    }
    raw = canonical_json_bytes(record)
    digest = sha256_bytes(raw)
    path = ledger / "records" / f"{2:016d}-decision-{digest}.json"
    create_only_file(path, raw)
    return {
        "sequence": 2,
        "record_sha256": digest,
        "path": str(path),
        "record": record,
        "snapshot": seal_snapshot(ledger),
    }


def _run_payload(
    *,
    status: str = "completed",
    conclusion: str | None = "success",
    display_title: str = _REQUEST_TITLE,
    run_attempt: int = 1,
) -> dict:
    return {
        "id": RUN_ID,
        "event": "workflow_dispatch",
        "status": status,
        "conclusion": conclusion,
        "head_sha": COMMIT_OID,
        "head_branch": "5.0",
        "display_title": display_title,
        "run_attempt": run_attempt,
        "path": ".github/workflows/prospective-attest.yml",
        "workflow_id": 344235268,
        "workflow_url": (
            "https://api.github.com/repos/yxforever666gh/factor-lab/"
            "actions/workflows/344235268"
        ),
        "repository": {"full_name": "yxforever666gh/factor-lab"},
        "html_url": (
            "https://github.com/yxforever666gh/factor-lab/actions/runs/9345"
        ),
        "created_at": "2026-08-23T12:02:00Z",
        "updated_at": "2026-08-23T12:03:00Z",
    }


class FakeGitHub:
    """An argv-level fake for the complete gh dispatch/download/verify flow."""

    def __init__(
        self,
        *,
        run_payloads: list[dict[str, Any]] | None = None,
        failure: str | None = None,
        allow_dispatch: bool = True,
        request_id: str | None = None,
        remote_runs: list[dict[str, Any]] | None = None,
        tlog_timestamp: str = "2026-08-23T12:03:00Z",
        include_other_attestation: bool = False,
    ) -> None:
        self.run_payloads = list(run_payloads or [_run_payload()])
        self.failure = failure
        self.allow_dispatch = allow_dispatch
        self.request_id = request_id
        self.remote_runs = [dict(run) for run in (remote_runs or [])]
        self.tlog_timestamp = tlog_timestamp
        self.include_other_attestation = include_other_attestation
        self.workflow_run_attempt = 1
        self.calls: list[CommandSpec] = []
        self.dispatched_snapshot: bytes | None = None

    def __call__(self, command: CommandSpec) -> CommandResult:
        self.calls.append(command)
        assert isinstance(command.argv, tuple)
        assert command.argv[:1] == ("gh",)

        if (
            command.argv[1:2] == ("api",)
            and "/actions/workflows/" in command.argv[2]
            and "/runs?" in command.argv[2]
        ):
            assert command.argv[-2:] == ("--paginate", "--slurp")
            return CommandResult(
                0,
                json.dumps(
                    [
                        {
                            "total_count": len(self.remote_runs),
                            "workflow_runs": self.remote_runs,
                        }
                    ]
                ).encode(),
            )

        if command.argv[1:2] == ("api",) and command.argv[2].endswith(
            "/dispatches"
        ):
            if not self.allow_dispatch:
                raise AssertionError("resume unexpectedly dispatched another workflow")
            if self.failure == "dispatch":
                return CommandResult(1, b"", b"dispatch unavailable")
            assert command.stdin is not None
            body = json.loads(command.stdin)
            assert set(body) == {"inputs", "ref", "return_run_details"}
            assert body["return_run_details"] is True
            self.request_id = body["inputs"]["request_id"]
            self.dispatched_snapshot = base64.b64decode(
                body["inputs"]["snapshot_b64"], validate=True
            )
            self.remote_runs.append(
                _run_payload(display_title=f"prospective-{self.request_id}")
            )
            return CommandResult(
                0,
                json.dumps(
                    {
                        "workflow_run_id": RUN_ID,
                        "run_url": (
                            "https://api.github.com/repos/yxforever666gh/"
                            "factor-lab/actions/runs/9345"
                        ),
                        "html_url": (
                            "https://github.com/yxforever666gh/"
                            "factor-lab/actions/runs/9345"
                        ),
                    }
                ).encode(),
            )

        if command.argv[1:2] == ("api",) and "/actions/runs/" in command.argv[2]:
            if self.failure == "run_command":
                return CommandResult(1, b"", b"poll unavailable")
            if self.failure == "terminal_run":
                payload = _run_payload(conclusion="failure")
            elif self.run_payloads:
                payload = self.run_payloads.pop(0)
            else:
                payload = _run_payload()
            payload = dict(payload)
            if payload.get("display_title") == _REQUEST_TITLE:
                if self.request_id is None:
                    raise AssertionError("fake workflow run lacks its request id")
                payload["display_title"] = f"prospective-{self.request_id}"
            if type(payload.get("run_attempt")) is int:
                self.workflow_run_attempt = payload["run_attempt"]
            return CommandResult(0, json.dumps(payload).encode())

        if command.argv[1:3] == ("attestation", "download"):
            if self.failure == "download_command":
                return CommandResult(1, b"", b"download unavailable")
            assert command.cwd is not None
            if self.failure != "no_bundle":
                snapshot_path = Path(command.argv[3])
                (command.cwd / "sha256-bundle.jsonl").write_bytes(
                    _sigstore_bundle(
                        snapshot_path,
                        run_id=RUN_ID,
                        run_attempt=self.workflow_run_attempt,
                        tlog_timestamp=self.tlog_timestamp,
                    )
                )
            if self.failure == "multiple_bundles":
                (command.cwd / "other.jsonl").write_bytes(b'{"bundle":false}\n')
            return CommandResult(0, b"")

        if command.argv[1:3] == ("attestation", "verify"):
            if self.failure == "verify_command":
                return CommandResult(1, b"", b"verification unavailable")
            snapshot_path = Path(command.argv[3])
            snapshot_sha = snapshot_path.stem.removeprefix("prospective-snapshot-")
            identity = certificate_identity()
            if self.failure == "invalid_verification":
                identity = identity + "/wrong"

            def verification_result(run_id: int, run_attempt: int) -> dict[str, Any]:
                return {
                    "verificationResult": {
                        "statement": {
                            "predicateType": "https://slsa.dev/provenance/v1",
                            "subject": [
                                {
                                    "name": snapshot_path.name,
                                    "digest": {"sha256": snapshot_sha},
                                }
                            ],
                        },
                        "signature": {
                            "certificate": {
                                "subjectAlternativeName": identity,
                                "runnerEnvironment": "github-hosted",
                                "runInvocationURI": (
                                    "https://github.com/yxforever666gh/factor-lab/"
                                    f"actions/runs/{run_id}/attempts/{run_attempt}"
                                ),
                            }
                        },
                        "verifiedTimestamps": [
                            {
                                "type": "Tlog",
                                "uri": (
                                    "https://rekor.sigstore.dev/api/v1/"
                                    f"log/entries/{run_id}"
                                ),
                                "timestamp": self.tlog_timestamp,
                            }
                        ],
                    }
                }

            verified_run_id = (
                RUN_ID + 1
                if self.failure == "invalid_run_invocation"
                else RUN_ID
            )
            payload = [
                verification_result(verified_run_id, self.workflow_run_attempt)
            ]
            if self.include_other_attestation:
                payload.insert(0, verification_result(RUN_ID + 99, 3))
            return CommandResult(0, json.dumps(payload).encode())

        raise AssertionError(f"unexpected fake-gh argv: {command.argv!r}")


def test_default_runner_executes_an_argv_vector_without_a_shell() -> None:
    command = CommandSpec(
        argv=(
            sys.executable,
            "-c",
            "import sys; sys.stdout.buffer.write(sys.stdin.buffer.read()[::-1])",
        ),
        stdin=b"ledger",
    )
    result = run_command(command)
    assert result.returncode == 0
    assert result.stdout == b"regdel"


def test_authoritative_run_verifier_binds_complete_adaptive_evidence(
    tmp_path: Path,
) -> None:
    run_id, run_dir, protocol_path = _authoritative_run_fixture(tmp_path)

    binding = verify_authoritative_run(
        tmp_path,
        run_id,
        protocol_path=protocol_path,
        release_commit_oid=COMMIT_OID,
    )

    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert binding == {
        "authoritative_run_id": run_id,
        "run_fingerprint": run_id + "3" * 48,
        "manifest_sha256": _sha256(run_dir / "manifest.json"),
        "manifest_self_sha256": manifest["manifest_sha256"],
        "adaptive_summary_sha256": _sha256(
            run_dir / "adaptive" / "adaptive-summary.json"
        ),
        "frozen_route": "online_overlay",
        "integrity_valid": True,
    }


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("mode", "canary", "completed full adaptive non-canary"),
        ("canary_smoke_only", True, "completed full adaptive non-canary"),
        ("git.dirty", True, "clean and built from the peeled release commit"),
        ("git.commit", "9" * 40, "clean and built from the peeled release commit"),
    ],
)
def test_authoritative_run_rejects_wrong_execution_or_git_identity(
    tmp_path: Path,
    field: str,
    value: Any,
    match: str,
) -> None:
    run_id, run_dir, protocol_path = _authoritative_run_fixture(tmp_path)
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if field.startswith("git."):
        summary["git"][field.split(".", maxsplit=1)[1]] = value
    else:
        summary[field] = value
    _write_json(summary_path, summary)
    _resign_fixture(run_dir)

    with pytest.raises(ProspectiveRuntimeError, match=match):
        verify_authoritative_run(
            tmp_path,
            run_id,
            protocol_path=protocol_path,
            release_commit_oid=COMMIT_OID,
        )


def test_authoritative_run_rejects_adaptive_summary_or_frozen_route_drift(
    tmp_path: Path,
) -> None:
    run_id, run_dir, protocol_path = _authoritative_run_fixture(tmp_path)
    adaptive_path = run_dir / "adaptive" / "adaptive-summary.json"
    adaptive = json.loads(adaptive_path.read_text(encoding="utf-8"))
    adaptive["frozen_route"] = "fixed_core_full"
    _write_json(adaptive_path, adaptive)
    _resign_fixture(run_dir)
    with pytest.raises(ProspectiveRuntimeError, match="differs from summary.adaptive"):
        verify_authoritative_run(
            tmp_path,
            run_id,
            protocol_path=protocol_path,
            release_commit_oid=COMMIT_OID,
        )

    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["adaptive"] = adaptive
    _write_json(summary_path, summary)
    _resign_fixture(run_dir)
    with pytest.raises(ProspectiveRuntimeError, match="frozen gates or route differ"):
        verify_authoritative_run(
            tmp_path,
            run_id,
            protocol_path=protocol_path,
            release_commit_oid=COMMIT_OID,
        )


def test_authoritative_run_rejects_manifest_size_hash_and_unlisted_files(
    tmp_path: Path,
) -> None:
    run_id, run_dir, protocol_path = _authoritative_run_fixture(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"][0]["size_bytes"] += 1
    manifest["manifest_sha256"] = _manifest_self_sha256(manifest)
    _write_json(manifest_path, manifest)
    with pytest.raises(ProspectiveRuntimeError, match="manifest size differs"):
        verify_authoritative_run(
            tmp_path,
            run_id,
            protocol_path=protocol_path,
            release_commit_oid=COMMIT_OID,
        )

    _resign_fixture(run_dir)
    (run_dir / "unlisted.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ProspectiveRuntimeError, match="enumerate every run file"):
        verify_authoritative_run(
            tmp_path,
            run_id,
            protocol_path=protocol_path,
            release_commit_oid=COMMIT_OID,
        )


def test_activation_canary_runs_full_fake_gh_flow_and_appends_last(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    fake = FakeGitHub(
        run_payloads=[
            _run_payload(status="queued", conclusion=None),
            _run_payload(),
        ]
    )
    sleeps: list[float] = []

    result = attest_snapshot(
        ledger,
        activation["snapshot"]["path"],
        purpose="activation_canary",
        release_commit_oid=COMMIT_OID,
        recorded_at_utc="2026-08-23T12:04:00Z",
        command_runner=fake,
        sleeper=sleeps.append,
        poll_interval_seconds=0.25,
        max_poll_attempts=3,
    )

    assert result["workflow_run_id"] == RUN_ID
    assert result["workflow_run_attempt"] == 1
    assert result["resumed"] is False
    assert result["receipt"]["kind"] == "attestation_receipt"
    receipt_payload = result["receipt"]["record"]["payload"]
    assert receipt_payload["workflow_run_display_title"] == (
        f"prospective-{result['request_id']}"
    )
    assert receipt_payload["run_invocation_uri"].endswith(
        f"/runs/{RUN_ID}/attempts/1"
    )
    assert receipt_payload["verified_tlog_type"] == "Tlog"
    assert receipt_payload["verified_tlog_timestamp_utc"] == (
        "2026-08-23T12:03:00Z"
    )
    assert Path(result["bundle"]["path"]).is_file()
    assert sleeps == [0.25]
    assert [call.argv[1] for call in fake.calls] == [
        "api",
        "api",
        "api",
        "api",
        "attestation",
        "attestation",
    ]
    status = ledger_status(ledger)
    assert status["record_count"] == 2
    assert status["status"] == "awaiting_new_data"


def test_decision_anchor_can_resume_without_redispatch(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    _activate(ledger)
    decision = _decision(ledger)
    request_id = build_dispatch_request(
        Path(decision["snapshot"]["path"]).read_bytes()
    ).request_id
    fake = FakeGitHub(allow_dispatch=False, request_id=request_id)

    result = attest_snapshot(
        ledger,
        decision["snapshot"]["path"],
        purpose="decision_anchor",
        decision_record_sha256=decision["record_sha256"],
        admission_deadline_utc="2026-08-24T01:15:00Z",
        release_commit_oid=COMMIT_OID,
        workflow_run_id=RUN_ID,
        recorded_at_utc="2026-08-23T12:04:00Z",
        command_runner=fake,
        sleeper=lambda _: None,
    )

    assert result["resumed"] is True
    assert result["receipt"]["record"]["payload"]["decision_record_sha256"] == (
        decision["record_sha256"]
    )
    assert all(not call.argv[2].endswith("/dispatches") for call in fake.calls)
    assert ledger_status(ledger)["status"] == "awaiting_outcome"


def test_resume_rejects_a_run_whose_display_title_binds_another_request(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    _activate(ledger)
    decision = _decision(ledger)
    request_id = build_dispatch_request(
        Path(decision["snapshot"]["path"]).read_bytes()
    ).request_id
    fake = FakeGitHub(
        allow_dispatch=False,
        request_id=request_id,
        run_payloads=[_run_payload(display_title=f"prospective-{'0' * 64}")],
    )

    with pytest.raises(ProspectiveRuntimeError, match="workflow run failed validation"):
        attest_snapshot(
            ledger,
            decision["snapshot"]["path"],
            purpose="decision_anchor",
            decision_record_sha256=decision["record_sha256"],
            admission_deadline_utc="2026-08-24T01:15:00Z",
            release_commit_oid=COMMIT_OID,
            workflow_run_id=RUN_ID,
            command_runner=fake,
            sleeper=lambda _: None,
        )

    assert audit_ledger(ledger)["record_count"] == 2


def test_decision_anchor_rejects_tlog_at_the_admission_deadline(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    _activate(ledger)
    decision = _decision(ledger)
    request_id = build_dispatch_request(
        Path(decision["snapshot"]["path"]).read_bytes()
    ).request_id
    fake = FakeGitHub(
        allow_dispatch=False,
        request_id=request_id,
        tlog_timestamp="2026-08-24T01:15:00Z",
    )

    with pytest.raises(ProspectiveRuntimeError, match="attestation verification failed"):
        attest_snapshot(
            ledger,
            decision["snapshot"]["path"],
            purpose="decision_anchor",
            decision_record_sha256=decision["record_sha256"],
            admission_deadline_utc="2026-08-24T01:15:00Z",
            release_commit_oid=COMMIT_OID,
            workflow_run_id=RUN_ID,
            command_runner=fake,
            sleeper=lambda _: None,
        )

    assert audit_ledger(ledger)["record_count"] == 2


def test_decision_deadline_forbids_dispatch_without_local_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = tmp_path / "ledger"
    _activate(ledger)
    decision = _decision(ledger)
    request_id = build_dispatch_request(
        Path(decision["snapshot"]["path"]).read_bytes()
    ).request_id
    monkeypatch.setattr(
        prospective_runtime,
        "_runtime_now_utc",
        lambda: datetime(2026, 8, 24, 1, 15, tzinfo=timezone.utc),
    )
    fake = FakeGitHub(request_id=request_id)

    with pytest.raises(ProspectiveRuntimeError, match="before local dispatch evidence"):
        attest_snapshot(
            ledger,
            decision["snapshot"]["path"],
            purpose="decision_anchor",
            decision_record_sha256=decision["record_sha256"],
            admission_deadline_utc="2026-08-24T01:15:00Z",
            release_commit_oid=COMMIT_OID,
            command_runner=fake,
            sleeper=lambda _: None,
        )

    assert fake.calls == []
    assert not (ledger / "dispatch" / f"{request_id}.intent.json").exists()
    assert not (ledger / "dispatch" / f"{request_id}.json").exists()
    assert audit_ledger(ledger)["record_count"] == 2


def test_decision_binding_resumes_after_deadline_without_redispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = tmp_path / "ledger"
    _activate(ledger)
    decision = _decision(ledger)
    monkeypatch.setattr(
        prospective_runtime,
        "_runtime_now_utc",
        lambda: datetime(2026, 8, 23, 12, tzinfo=timezone.utc),
    )
    first = FakeGitHub(
        run_payloads=[_run_payload(status="queued", conclusion=None)]
    )
    with pytest.raises(ProspectiveRuntimeError, match="did not complete"):
        attest_snapshot(
            ledger,
            decision["snapshot"]["path"],
            purpose="decision_anchor",
            decision_record_sha256=decision["record_sha256"],
            admission_deadline_utc="2026-08-24T01:15:00Z",
            release_commit_oid=COMMIT_OID,
            command_runner=first,
            sleeper=lambda _: None,
            max_poll_attempts=1,
        )

    assert first.request_id is not None
    binding_path = ledger / "dispatch" / f"{first.request_id}.json"
    assert binding_path.is_file()
    monkeypatch.setattr(
        prospective_runtime,
        "_runtime_now_utc",
        lambda: datetime(2026, 8, 25, 2, tzinfo=timezone.utc),
    )
    retry = FakeGitHub(allow_dispatch=False, request_id=first.request_id)

    recovered = attest_snapshot(
        ledger,
        decision["snapshot"]["path"],
        purpose="decision_anchor",
        decision_record_sha256=decision["record_sha256"],
        admission_deadline_utc="2026-08-24T01:15:00Z",
        release_commit_oid=COMMIT_OID,
        recorded_at_utc="2026-08-25T02:01:00Z",
        command_runner=retry,
        sleeper=lambda _: None,
    )

    assert recovered["resumed"] is True
    assert all(not call.argv[2].endswith("/dispatches") for call in retry.calls)
    assert audit_ledger(ledger)["record_count"] == 3


def test_predeadline_intent_reconciles_its_remote_run_after_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = tmp_path / "ledger"
    _activate(ledger)
    decision = _decision(ledger)
    monkeypatch.setattr(
        prospective_runtime,
        "_runtime_now_utc",
        lambda: datetime(2026, 8, 23, 12, tzinfo=timezone.utc),
    )
    fake = FakeGitHub()
    real_store = prospective_runtime._store_dispatch_binding

    def crash_before_binding(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise ProspectiveRuntimeError("simulated crash before binding publication")

    monkeypatch.setattr(
        prospective_runtime,
        "_store_dispatch_binding",
        crash_before_binding,
    )
    with pytest.raises(ProspectiveRuntimeError, match="simulated crash"):
        attest_snapshot(
            ledger,
            decision["snapshot"]["path"],
            purpose="decision_anchor",
            decision_record_sha256=decision["record_sha256"],
            admission_deadline_utc="2026-08-24T01:15:00Z",
            release_commit_oid=COMMIT_OID,
            command_runner=fake,
            sleeper=lambda _: None,
        )

    assert fake.request_id is not None
    intent_path = ledger / "dispatch" / f"{fake.request_id}.intent.json"
    assert json.loads(intent_path.read_bytes())["created_at_utc"] == (
        "2026-08-23T12:00:00Z"
    )
    monkeypatch.setattr(
        prospective_runtime,
        "_store_dispatch_binding",
        real_store,
    )
    monkeypatch.setattr(
        prospective_runtime,
        "_runtime_now_utc",
        lambda: datetime(2026, 8, 25, 2, tzinfo=timezone.utc),
    )
    fake.allow_dispatch = False

    recovered = attest_snapshot(
        ledger,
        decision["snapshot"]["path"],
        purpose="decision_anchor",
        decision_record_sha256=decision["record_sha256"],
        admission_deadline_utc="2026-08-24T01:15:00Z",
        release_commit_oid=COMMIT_OID,
        recorded_at_utc="2026-08-25T02:01:00Z",
        command_runner=fake,
        sleeper=lambda _: None,
    )

    dispatch_calls = [
        call
        for call in fake.calls
        if call.argv[1:2] == ("api",) and call.argv[2].endswith("/dispatches")
    ]
    assert len(dispatch_calls) == 1
    assert recovered["resumed"] is True
    assert audit_ledger(ledger)["record_count"] == 3


def test_predeadline_intent_without_remote_run_never_dispatches_after_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = tmp_path / "ledger"
    _activate(ledger)
    decision = _decision(ledger)
    request_id = build_dispatch_request(
        Path(decision["snapshot"]["path"]).read_bytes()
    ).request_id
    monkeypatch.setattr(
        prospective_runtime,
        "_runtime_now_utc",
        lambda: datetime(2026, 8, 23, 12, tzinfo=timezone.utc),
    )
    before_crash = FakeGitHub(request_id=request_id)

    def crash_before_dispatch(command: CommandSpec) -> CommandResult:
        if command.argv[1:2] == ("api",) and command.argv[2].endswith(
            "/dispatches"
        ):
            raise RuntimeError("simulated crash before network dispatch")
        return before_crash(command)

    with pytest.raises(ProspectiveRuntimeError, match="dispatch failed"):
        attest_snapshot(
            ledger,
            decision["snapshot"]["path"],
            purpose="decision_anchor",
            decision_record_sha256=decision["record_sha256"],
            admission_deadline_utc="2026-08-24T01:15:00Z",
            release_commit_oid=COMMIT_OID,
            command_runner=crash_before_dispatch,
            sleeper=lambda _: None,
        )

    monkeypatch.setattr(
        prospective_runtime,
        "_runtime_now_utc",
        lambda: datetime(2026, 8, 25, 2, tzinfo=timezone.utc),
    )
    retry = FakeGitHub(allow_dispatch=False, request_id=request_id)
    with pytest.raises(ProspectiveRuntimeError, match="forbids a new workflow dispatch"):
        attest_snapshot(
            ledger,
            decision["snapshot"]["path"],
            purpose="decision_anchor",
            decision_record_sha256=decision["record_sha256"],
            admission_deadline_utc="2026-08-24T01:15:00Z",
            release_commit_oid=COMMIT_OID,
            command_runner=retry,
            sleeper=lambda _: None,
        )

    assert len(
        [call for call in retry.calls if "/runs?" in call.argv[2]]
    ) == prospective_runtime.DEFAULT_DISPATCH_RECONCILE_ATTEMPTS
    assert all(not call.argv[2].endswith("/dispatches") for call in retry.calls)
    assert audit_ledger(ledger)["record_count"] == 2


def test_postdeadline_intent_is_not_recovery_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = tmp_path / "ledger"
    _activate(ledger)
    decision = _decision(ledger)
    request = build_dispatch_request(
        Path(decision["snapshot"]["path"]).read_bytes()
    )
    prospective_runtime._store_dispatch_intent(
        prospective_runtime.LedgerLayout.at(ledger),
        request,
        created_at_utc=datetime(2026, 8, 24, 1, 15, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        prospective_runtime,
        "_runtime_now_utc",
        lambda: datetime(2026, 8, 25, 2, tzinfo=timezone.utc),
    )
    fake = FakeGitHub(allow_dispatch=False, request_id=request.request_id)

    with pytest.raises(ProspectiveRuntimeError, match="not created before its deadline"):
        attest_snapshot(
            ledger,
            decision["snapshot"]["path"],
            purpose="decision_anchor",
            decision_record_sha256=decision["record_sha256"],
            admission_deadline_utc="2026-08-24T01:15:00Z",
            release_commit_oid=COMMIT_OID,
            command_runner=fake,
            sleeper=lambda _: None,
        )

    assert fake.calls == []
    assert audit_ledger(ledger)["record_count"] == 2


def test_other_same_subject_attestations_do_not_block_the_current_run(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    fake = FakeGitHub(include_other_attestation=True)

    result = attest_snapshot(
        ledger,
        activation["snapshot"]["path"],
        purpose="activation_canary",
        release_commit_oid=COMMIT_OID,
        command_runner=fake,
        sleeper=lambda _: None,
    )

    assert result["verification"]["workflow_run_id"] == RUN_ID
    assert result["verification"]["run_invocation_uri"].endswith(
        f"/runs/{RUN_ID}/attempts/1"
    )
    assert audit_ledger(ledger)["record_count"] == 2


@pytest.mark.parametrize(
    "failure",
    [
        "dispatch",
        "run_command",
        "terminal_run",
        "download_command",
        "no_bundle",
        "multiple_bundles",
        "verify_command",
        "invalid_verification",
        "invalid_run_invocation",
    ],
)
def test_any_network_or_evidence_failure_never_appends_a_receipt(
    tmp_path: Path,
    failure: str,
) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    fake = FakeGitHub(failure=failure)

    with pytest.raises(ProspectiveRuntimeError):
        attest_snapshot(
            ledger,
            activation["snapshot"]["path"],
            purpose="activation_canary",
            release_commit_oid=COMMIT_OID,
            command_runner=fake,
            sleeper=lambda _: None,
            max_poll_attempts=2,
        )

    audit = audit_ledger(ledger)
    assert audit["valid"] is True
    assert audit["record_count"] == 1
    assert [row["kind"] for row in audit["records"]] == ["protocol_activation"]
    assert list((ledger / "bundles").iterdir()) == []


def test_poll_exhaustion_never_downloads_or_appends(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    fake = FakeGitHub(
        run_payloads=[
            _run_payload(status="queued", conclusion=None),
            _run_payload(status="in_progress", conclusion=None),
        ]
    )
    sleeps: list[float] = []

    with pytest.raises(ProspectiveRuntimeError, match="did not complete"):
        attest_snapshot(
            ledger,
            activation["snapshot"]["path"],
            purpose="activation_canary",
            release_commit_oid=COMMIT_OID,
            command_runner=fake,
            sleeper=sleeps.append,
            poll_interval_seconds=0,
            max_poll_attempts=2,
        )

    assert sleeps == [0.0]
    assert all(call.argv[1:3] != ("attestation", "download") for call in fake.calls)
    assert audit_ledger(ledger)["record_count"] == 1


def test_poll_timeout_persists_dispatch_and_automatically_resumes(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    request_id = build_dispatch_request(
        Path(activation["snapshot"]["path"]).read_bytes()
    ).request_id
    fake = FakeGitHub(
        run_payloads=[
            _run_payload(status="queued", conclusion=None),
            _run_payload(status="in_progress", conclusion=None),
        ]
    )

    with pytest.raises(ProspectiveRuntimeError, match="did not complete"):
        attest_snapshot(
            ledger,
            activation["snapshot"]["path"],
            purpose="activation_canary",
            release_commit_oid=COMMIT_OID,
            command_runner=fake,
            sleeper=lambda _: None,
            poll_interval_seconds=0,
            max_poll_attempts=2,
        )

    binding_path = ledger / "dispatch" / f"{request_id}.json"
    binding = json.loads(binding_path.read_bytes())
    assert binding["request_id"] == request_id
    assert binding["workflow_run_id"] == RUN_ID
    assert audit_ledger(ledger)["record_count"] == 1

    fake.allow_dispatch = False
    result = attest_snapshot(
        ledger,
        activation["snapshot"]["path"],
        purpose="activation_canary",
        release_commit_oid=COMMIT_OID,
        recorded_at_utc="2026-08-23T12:04:00Z",
        command_runner=fake,
        sleeper=lambda _: None,
    )

    dispatch_calls = [
        call
        for call in fake.calls
        if call.argv[1:2] == ("api",) and call.argv[2].endswith("/dispatches")
    ]
    assert len(dispatch_calls) == 1
    assert result["resumed"] is True
    assert result["workflow_run_id"] == RUN_ID
    assert audit_ledger(ledger)["record_count"] == 2


def test_pre_dispatch_intent_reconciles_after_binding_store_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    request_id = build_dispatch_request(
        Path(activation["snapshot"]["path"]).read_bytes()
    ).request_id
    fake = FakeGitHub()
    real_store = prospective_runtime._store_dispatch_binding

    def crash_after_dispatch(*_args: object, **_kwargs: object) -> dict[str, Any]:
        raise ProspectiveRuntimeError("simulated crash before binding publication")

    monkeypatch.setattr(
        prospective_runtime,
        "_store_dispatch_binding",
        crash_after_dispatch,
    )
    with pytest.raises(ProspectiveRuntimeError, match="simulated crash"):
        attest_snapshot(
            ledger,
            activation["snapshot"]["path"],
            purpose="activation_canary",
            release_commit_oid=COMMIT_OID,
            command_runner=fake,
            sleeper=lambda _: None,
        )

    intent_path = ledger / "dispatch" / f"{request_id}.intent.json"
    binding_path = ledger / "dispatch" / f"{request_id}.json"
    assert intent_path.is_file()
    assert not binding_path.exists()
    monkeypatch.setattr(
        prospective_runtime,
        "_store_dispatch_binding",
        real_store,
    )
    fake.allow_dispatch = False
    remote_runs = list(fake.remote_runs)
    fake.remote_runs.clear()
    retry_queries = 0
    reconcile_sleeps: list[float] = []

    def delayed_visibility_runner(command: CommandSpec) -> CommandResult:
        nonlocal retry_queries
        if (
            command.argv[1:2] == ("api",)
            and "/actions/workflows/" in command.argv[2]
            and "/runs?" in command.argv[2]
        ):
            retry_queries += 1
            if retry_queries == 2:
                fake.remote_runs.extend(remote_runs)
        return fake(command)

    result = attest_snapshot(
        ledger,
        activation["snapshot"]["path"],
        purpose="activation_canary",
        release_commit_oid=COMMIT_OID,
        recorded_at_utc="2026-08-23T12:04:00Z",
        command_runner=delayed_visibility_runner,
        sleeper=reconcile_sleeps.append,
    )

    dispatch_calls = [
        call
        for call in fake.calls
        if call.argv[1:2] == ("api",) and call.argv[2].endswith("/dispatches")
    ]
    query_calls = [
        call
        for call in fake.calls
        if call.argv[1:2] == ("api",) and "/runs?" in call.argv[2]
    ]
    assert len(dispatch_calls) == 1
    assert len(query_calls) == 3
    assert retry_queries == 2
    assert reconcile_sleeps == [
        prospective_runtime.DEFAULT_DISPATCH_RECONCILE_INTERVAL_SECONDS
    ]
    assert result["resumed"] is True
    assert result["workflow_run_id"] == RUN_ID
    assert binding_path.is_file()
    assert audit_ledger(ledger)["record_count"] == 2


def test_intent_owner_crash_before_network_eventually_redispatches(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    request_id = build_dispatch_request(
        Path(activation["snapshot"]["path"]).read_bytes()
    ).request_id
    before_crash = FakeGitHub()

    def crash_before_dispatch(command: CommandSpec) -> CommandResult:
        if command.argv[1:2] == ("api",) and command.argv[2].endswith(
            "/dispatches"
        ):
            raise RuntimeError("simulated process death before network dispatch")
        return before_crash(command)

    with pytest.raises(ProspectiveRuntimeError, match="dispatch failed"):
        attest_snapshot(
            ledger,
            activation["snapshot"]["path"],
            purpose="activation_canary",
            release_commit_oid=COMMIT_OID,
            command_runner=crash_before_dispatch,
            sleeper=lambda _: None,
        )

    intent_path = ledger / "dispatch" / f"{request_id}.intent.json"
    binding_path = ledger / "dispatch" / f"{request_id}.json"
    assert intent_path.is_file()
    assert not binding_path.exists()
    retry = FakeGitHub(request_id=request_id)
    reconcile_sleeps: list[float] = []

    result = attest_snapshot(
        ledger,
        activation["snapshot"]["path"],
        purpose="activation_canary",
        release_commit_oid=COMMIT_OID,
        recorded_at_utc="2026-08-23T12:04:00Z",
        command_runner=retry,
        sleeper=reconcile_sleeps.append,
    )

    query_calls = [
        call
        for call in retry.calls
        if call.argv[1:2] == ("api",) and "/runs?" in call.argv[2]
    ]
    dispatch_calls = [
        call
        for call in retry.calls
        if call.argv[1:2] == ("api",) and call.argv[2].endswith("/dispatches")
    ]
    assert len(query_calls) == (
        prospective_runtime.DEFAULT_DISPATCH_RECONCILE_ATTEMPTS
    )
    assert len(dispatch_calls) == 1
    assert reconcile_sleeps == [
        prospective_runtime.DEFAULT_DISPATCH_RECONCILE_INTERVAL_SECONDS
    ] * (prospective_runtime.DEFAULT_DISPATCH_RECONCILE_ATTEMPTS - 1)
    assert result["resumed"] is False
    assert result["workflow_run_id"] == RUN_ID
    assert binding_path.is_file()
    assert audit_ledger(ledger)["record_count"] == 2


def test_reconciliation_rejects_multiple_matching_remote_runs(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    request_id = build_dispatch_request(
        Path(activation["snapshot"]["path"]).read_bytes()
    ).request_id
    first = _run_payload(display_title=f"prospective-{request_id}")
    second = dict(first)
    second["id"] = RUN_ID + 1
    second["html_url"] = (
        f"https://github.com/yxforever666gh/factor-lab/actions/runs/{RUN_ID + 1}"
    )
    fake = FakeGitHub(
        allow_dispatch=False,
        request_id=request_id,
        remote_runs=[first, second],
    )

    with pytest.raises(ProspectiveRuntimeError, match="reconciliation failed") as caught:
        attest_snapshot(
            ledger,
            activation["snapshot"]["path"],
            purpose="activation_canary",
            release_commit_oid=COMMIT_OID,
            command_runner=fake,
            sleeper=lambda _: None,
        )

    assert "multiple workflow runs" in str(caught.value.__cause__)
    assert len(fake.calls) == 1
    assert audit_ledger(ledger)["record_count"] == 1


def test_wrong_explicit_run_id_does_not_poison_dispatch_binding(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    request_id = build_dispatch_request(
        Path(activation["snapshot"]["path"]).read_bytes()
    ).request_id
    binding_path = ledger / "dispatch" / f"{request_id}.json"
    wrong = FakeGitHub(
        allow_dispatch=False,
        request_id=request_id,
        run_payloads=[
            _run_payload(display_title=f"prospective-{'0' * 64}")
        ],
    )

    with pytest.raises(ProspectiveRuntimeError, match="before resume binding"):
        attest_snapshot(
            ledger,
            activation["snapshot"]["path"],
            purpose="activation_canary",
            release_commit_oid=COMMIT_OID,
            workflow_run_id=RUN_ID,
            command_runner=wrong,
            sleeper=lambda _: None,
        )

    assert not binding_path.exists()
    correct = FakeGitHub(
        allow_dispatch=False,
        request_id=request_id,
    )
    result = attest_snapshot(
        ledger,
        activation["snapshot"]["path"],
        purpose="activation_canary",
        release_commit_oid=COMMIT_OID,
        workflow_run_id=RUN_ID,
        recorded_at_utc="2026-08-23T12:04:00Z",
        command_runner=correct,
        sleeper=lambda _: None,
    )

    assert binding_path.is_file()
    assert result["workflow_run_id"] == RUN_ID
    assert audit_ledger(ledger)["record_count"] == 2


@pytest.mark.parametrize("slot", ["binding", "intent", "lock"])
def test_dangling_dispatch_symlink_fails_closed_before_network(
    tmp_path: Path,
    slot: str,
) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    request_id = build_dispatch_request(
        Path(activation["snapshot"]["path"]).read_bytes()
    ).request_id
    filename = {
        "binding": f"{request_id}.json",
        "intent": f"{request_id}.intent.json",
        "lock": f".{request_id}.lock",
    }[slot]
    path = ledger / "dispatch" / filename
    try:
        path.symlink_to(path.with_name("missing-dispatch-evidence.json"))
    except OSError as exc:  # pragma: no cover - depends on Windows privilege
        pytest.skip(f"symlink creation unavailable: {exc}")
    fake = FakeGitHub()

    with pytest.raises(ProspectiveRuntimeError, match="not a regular file"):
        attest_snapshot(
            ledger,
            activation["snapshot"]["path"],
            purpose="activation_canary",
            release_commit_oid=COMMIT_OID,
            command_runner=fake,
            sleeper=lambda _: None,
        )

    assert fake.calls == []
    assert audit_ledger(ledger)["record_count"] == 1


def test_retry_after_receipt_append_returns_existing_success_without_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    first = FakeGitHub()
    real_append = prospective_runtime.append_attestation_receipt
    appended: list[dict[str, Any]] = []

    def append_then_crash(*args: Any, **kwargs: Any) -> dict[str, Any]:
        receipt = real_append(*args, **kwargs)
        appended.append(receipt)
        raise RuntimeError("simulated crash after durable receipt append")

    monkeypatch.setattr(
        prospective_runtime,
        "append_attestation_receipt",
        append_then_crash,
    )
    with pytest.raises(ProspectiveRuntimeError, match="could not be appended"):
        attest_snapshot(
            ledger,
            activation["snapshot"]["path"],
            purpose="activation_canary",
            release_commit_oid=COMMIT_OID,
            recorded_at_utc="2026-08-23T12:04:00Z",
            command_runner=first,
            sleeper=lambda _: None,
        )

    assert len(appended) == 1
    assert first.request_id is not None
    assert audit_ledger(ledger)["record_count"] == 2
    monkeypatch.setattr(
        prospective_runtime,
        "append_attestation_receipt",
        real_append,
    )
    retry = FakeGitHub(allow_dispatch=False, request_id=first.request_id)

    recovered = attest_snapshot(
        ledger,
        activation["snapshot"]["path"],
        purpose="activation_canary",
        release_commit_oid=COMMIT_OID,
        command_runner=retry,
        sleeper=lambda _: None,
    )

    assert retry.calls == []
    assert recovered["resumed"] is True
    assert recovered["workflow_run_id"] == RUN_ID
    assert recovered["receipt"]["record_sha256"] == (
        appended[0]["record_sha256"]
    )
    assert audit_ledger(ledger)["record_count"] == 2


def test_per_request_lock_serializes_concurrent_identical_attestation(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    dispatch_started = threading.Event()
    release_dispatch = threading.Event()

    class BlockingFakeGitHub(FakeGitHub):
        def __call__(self, command: CommandSpec) -> CommandResult:
            if command.argv[1:2] == ("api",) and command.argv[2].endswith(
                "/dispatches"
            ):
                dispatch_started.set()
                if not release_dispatch.wait(timeout=5):
                    raise AssertionError("timed out waiting to release fake dispatch")
            return super().__call__(command)

    fake = BlockingFakeGitHub()
    results: list[dict[str, Any]] = []
    errors: list[BaseException] = []

    def run_attestation() -> None:
        try:
            results.append(
                attest_snapshot(
                    ledger,
                    activation["snapshot"]["path"],
                    purpose="activation_canary",
                    release_commit_oid=COMMIT_OID,
                    recorded_at_utc="2026-08-23T12:04:00Z",
                    command_runner=fake,
                    sleeper=lambda _: None,
                )
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    first_thread = threading.Thread(target=run_attestation)
    second_thread = threading.Thread(target=run_attestation)
    first_thread.start()
    assert dispatch_started.wait(timeout=5)
    second_thread.start()
    release_dispatch.set()
    first_thread.join(timeout=10)
    second_thread.join(timeout=10)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert errors == []
    assert len(results) == 2
    dispatch_calls = [
        call
        for call in fake.calls
        if call.argv[1:2] == ("api",) and call.argv[2].endswith("/dispatches")
    ]
    assert len(dispatch_calls) == 1
    assert results[0]["receipt"]["record_sha256"] == (
        results[1]["receipt"]["record_sha256"]
    )
    assert audit_ledger(ledger)["record_count"] == 2


def test_tampered_dispatch_binding_fails_closed_before_network(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)
    first_attempt = FakeGitHub(
        run_payloads=[_run_payload(status="queued", conclusion=None)]
    )

    with pytest.raises(ProspectiveRuntimeError, match="did not complete"):
        attest_snapshot(
            ledger,
            activation["snapshot"]["path"],
            purpose="activation_canary",
            release_commit_oid=COMMIT_OID,
            command_runner=first_attempt,
            sleeper=lambda _: None,
            max_poll_attempts=1,
        )

    assert first_attempt.request_id is not None
    binding_path = ledger / "dispatch" / f"{first_attempt.request_id}.json"
    binding = json.loads(binding_path.read_bytes())
    binding["release_tag"] = "5.1"
    binding_path.write_bytes(canonical_json_bytes(binding))
    resumed_attempt = FakeGitHub(
        allow_dispatch=False,
        request_id=first_attempt.request_id,
    )

    with pytest.raises(ProspectiveRuntimeError, match="dispatch binding identity differs"):
        attest_snapshot(
            ledger,
            activation["snapshot"]["path"],
            purpose="activation_canary",
            release_commit_oid=COMMIT_OID,
            command_runner=resumed_attempt,
            sleeper=lambda _: None,
        )

    assert resumed_attempt.calls == []
    assert audit_ledger(ledger)["record_count"] == 1


def test_decision_inputs_are_required_before_any_command_runs(tmp_path: Path) -> None:
    fake = FakeGitHub()
    with pytest.raises(ValueError, match="admission_deadline"):
        attest_snapshot(
            tmp_path / "ledger",
            b"{}",
            purpose="decision_anchor",
            decision_record_sha256="1" * 64,
            release_commit_oid=COMMIT_OID,
            command_runner=fake,
        )
    assert fake.calls == []


def test_runner_exception_is_wrapped_and_does_not_append(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger"
    activation = _activate(ledger)

    def exploding_runner(_: CommandSpec) -> subprocess.CompletedProcess[bytes]:
        raise OSError("gh is unavailable")

    with pytest.raises(ProspectiveRuntimeError, match="dispatch"):
        attest_snapshot(
            ledger,
            activation["snapshot"]["path"],
            purpose="activation_canary",
            release_commit_oid=COMMIT_OID,
            command_runner=exploding_runner,
        )
    assert audit_ledger(ledger)["record_count"] == 1
