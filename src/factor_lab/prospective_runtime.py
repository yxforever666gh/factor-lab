"""Transactional network orchestration for prospective snapshot attestation.

The ledger and attestation modules define evidence and trust contracts.  This
module is the deliberately small imperative layer between them: it executes
only explicit argv vectors, validates every external response, downloads into
a fresh directory, and appends a receipt only after the entire chain succeeds.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
from pathlib import PurePosixPath
import re
import subprocess
import tempfile
import time
from typing import Any, Callable, Mapping, Protocol, Sequence
import unicodedata

from factor_lab.prospective_attestation import (
    AttestationError,
    CommandSpec,
    DEFAULT_REPOSITORY,
    DEFAULT_WORKFLOW,
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
from factor_lab.prospective_ledger import (
    DEFAULT_LEDGER_ID,
    LedgerLayout,
    append_attestation_receipt,
    sha256_bytes,
    sha256_file,
)


DEFAULT_COMMAND_TIMEOUT_SECONDS = 120.0
DEFAULT_POLL_INTERVAL_SECONDS = 5.0
DEFAULT_MAX_POLL_ATTEMPTS = 120
_PENDING_RUN_STATES = frozenset(
    {"queued", "in_progress", "requested", "waiting", "pending"}
)
_RUN_ID_RE = re.compile(r"^[0-9a-f]{16}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_OID_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")


class ProspectiveRuntimeError(RuntimeError):
    """Raised when attestation orchestration cannot produce trusted evidence."""


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Minimal, byte-exact result contract accepted from command runners."""

    returncode: int
    stdout: bytes
    stderr: bytes = b""


class CommandRunner(Protocol):
    def __call__(self, command: CommandSpec) -> CommandResult | subprocess.CompletedProcess[Any]:
        """Execute one command specification without a shell."""


Sleeper = Callable[[float], object]


def run_command(command: CommandSpec) -> CommandResult:
    """Execute a command as an argv vector with a bounded runtime and no shell."""

    if not command.argv or any(not isinstance(argument, str) for argument in command.argv):
        raise ProspectiveRuntimeError("command argv must be a non-empty string vector")
    try:
        completed = subprocess.run(
            command.argv,
            input=command.stdin,
            cwd=command.cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            shell=False,
            timeout=DEFAULT_COMMAND_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProspectiveRuntimeError(
            f"command could not be executed: {command.argv[0]}"
        ) from exc
    return CommandResult(
        returncode=completed.returncode,
        stdout=bytes(completed.stdout or b""),
        stderr=bytes(completed.stderr or b""),
    )


def _as_bytes(value: Any, field_name: str) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise ProspectiveRuntimeError(f"command result {field_name} must be bytes or text")


def _execute(command: CommandSpec, command_runner: CommandRunner) -> bytes:
    try:
        raw_result = command_runner(command)
    except ProspectiveRuntimeError:
        raise
    except Exception as exc:
        raise ProspectiveRuntimeError(
            f"command runner failed before producing a result: {command.argv[0]}"
        ) from exc
    returncode = getattr(raw_result, "returncode", None)
    if type(returncode) is not int:
        raise ProspectiveRuntimeError("command runner returned no integer return code")
    stdout = _as_bytes(getattr(raw_result, "stdout", None), "stdout")
    stderr = _as_bytes(getattr(raw_result, "stderr", None), "stderr")
    if returncode != 0:
        diagnostic = stderr.decode("utf-8", errors="replace").strip()
        if len(diagnostic) > 500:
            diagnostic = diagnostic[:500] + "..."
        suffix = f": {diagnostic}" if diagnostic else ""
        raise ProspectiveRuntimeError(
            f"external command failed with exit code {returncode} ({command.argv[0]}){suffix}"
        )
    return stdout


def _unique_pairs(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for raw_key, value in pairs:
        key = unicodedata.normalize("NFC", raw_key)
        if key in result:
            raise ProspectiveRuntimeError(f"duplicate workflow-run response key: {key!r}")
        result[key] = value
    return result


def _reject_nonfinite_json(token: str) -> Any:
    raise ProspectiveRuntimeError(f"non-finite JSON token is forbidden: {token}")


def _load_json_object(path: Path, *, description: str) -> tuple[dict[str, Any], bytes]:
    try:
        raw = path.read_bytes()
        payload = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_pairs,
            parse_constant=_reject_nonfinite_json,
        )
    except ProspectiveRuntimeError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ProspectiveRuntimeError(f"cannot read valid {description}: {path}") from exc
    if not isinstance(payload, dict):
        raise ProspectiveRuntimeError(f"{description} must be a JSON object: {path}")
    return payload, raw


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ProspectiveRuntimeError("authoritative run contains invalid JSON values") from exc


def _manifest_payload_sha256(manifest: Mapping[str, Any]) -> str:
    payload = dict(manifest)
    payload.pop("manifest_sha256", None)
    return sha256_bytes(_canonical_json_bytes(payload))


def _require_sha256(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ProspectiveRuntimeError(f"{name} must be a lowercase SHA-256")
    return value


def _json_equal(left: Any, right: Any) -> bool:
    return _canonical_json_bytes(left) == _canonical_json_bytes(right)


def _verify_manifest_files(
    run_dir: Path,
    rows: Any,
) -> dict[str, dict[str, Any]]:
    if not isinstance(rows, list) or not rows:
        raise ProspectiveRuntimeError("authoritative manifest files must be a non-empty list")
    verified: dict[str, dict[str, Any]] = {}
    resolved_targets: set[str] = set()
    for index, raw_row in enumerate(rows):
        if not isinstance(raw_row, Mapping) or set(raw_row) != {
            "path",
            "size_bytes",
            "sha256",
        }:
            raise ProspectiveRuntimeError(
                f"authoritative manifest file[{index}] must contain path, size_bytes, and sha256"
            )
        logical = raw_row["path"]
        if not isinstance(logical, str) or not logical or "\\" in logical:
            raise ProspectiveRuntimeError(
                f"authoritative manifest file[{index}] has a non-canonical path"
            )
        relative = PurePosixPath(logical)
        if (
            relative.is_absolute()
            or relative.as_posix() != logical
            or any(part in {"", ".", ".."} for part in relative.parts)
            or logical == "manifest.json"
        ):
            raise ProspectiveRuntimeError(
                f"authoritative manifest file[{index}] escapes or names the manifest itself"
            )
        if logical in verified:
            raise ProspectiveRuntimeError(f"duplicate authoritative manifest path: {logical}")
        path = run_dir.joinpath(*relative.parts).resolve()
        if not path.is_relative_to(run_dir) or not path.is_file():
            raise ProspectiveRuntimeError(f"missing authoritative manifest file: {logical}")
        normalized_target = str(path).casefold()
        if normalized_target in resolved_targets:
            raise ProspectiveRuntimeError(
                f"authoritative manifest paths alias the same file: {logical}"
            )
        resolved_targets.add(normalized_target)
        size = raw_row["size_bytes"]
        if type(size) is not int or size < 0 or path.stat().st_size != size:
            raise ProspectiveRuntimeError(
                f"authoritative manifest size differs for {logical}"
            )
        expected_sha256 = _require_sha256(
            raw_row["sha256"], name=f"manifest file[{logical}] sha256"
        )
        if sha256_file(path) != expected_sha256:
            raise ProspectiveRuntimeError(
                f"authoritative manifest hash differs for {logical}"
            )
        verified[logical] = dict(raw_row)

    manifest_path = (run_dir / "manifest.json").resolve()
    actual_paths = {
        path.relative_to(run_dir).as_posix()
        for path in run_dir.rglob("*")
        if path.is_file() and path.resolve() != manifest_path
    }
    if set(verified) != actual_paths:
        missing = sorted(actual_paths - set(verified))
        extra = sorted(set(verified) - actual_paths)
        raise ProspectiveRuntimeError(
            "authoritative manifest does not enumerate every run file "
            f"(missing={missing}, extra={extra})"
        )
    return verified


def verify_authoritative_run(
    project_root: str | Path,
    run_id: str,
    *,
    protocol_path: str | Path,
    release_commit_oid: str,
) -> dict[str, Any]:
    """Verify the historical run that an activation will bind forever."""

    requested_run_id = str(run_id)
    if not _RUN_ID_RE.fullmatch(requested_run_id):
        raise ProspectiveRuntimeError("authoritative run id must be 16 lowercase hex characters")
    if not isinstance(release_commit_oid, str) or not _GIT_OID_RE.fullmatch(
        release_commit_oid
    ):
        raise ProspectiveRuntimeError("release commit must be a lowercase Git object id")

    root = Path(project_root).resolve()
    runs_root = (root / "runtime" / "runs").resolve()
    run_dir = (runs_root / requested_run_id).resolve()
    if run_dir.parent != runs_root or not run_dir.is_dir():
        raise ProspectiveRuntimeError(f"authoritative run directory is missing: {run_dir}")
    protocol_file = Path(protocol_path).resolve()
    protocol, protocol_raw = _load_json_object(
        protocol_file, description="adaptive protocol"
    )
    protocol_sha256 = sha256_bytes(protocol_raw)

    summary, _summary_raw = _load_json_object(
        run_dir / "summary.json", description="authoritative summary"
    )
    manifest, manifest_raw = _load_json_object(
        run_dir / "manifest.json", description="authoritative manifest"
    )
    adaptive_artifact, adaptive_raw = _load_json_object(
        run_dir / "adaptive" / "adaptive-summary.json",
        description="authoritative adaptive summary",
    )

    fingerprint = summary.get("run_fingerprint")
    if (
        summary.get("status") != "completed"
        or summary.get("suite") != "adaptive"
        or summary.get("mode") != "full"
        or summary.get("canary_smoke_only") is not False
    ):
        raise ProspectiveRuntimeError(
            "authoritative run must be a completed full adaptive non-canary run"
        )
    if (
        not isinstance(fingerprint, str)
        or not _SHA256_RE.fullmatch(fingerprint)
        or fingerprint[:16] != requested_run_id
        or summary.get("run_id") != requested_run_id
        or run_dir.name != requested_run_id
    ):
        raise ProspectiveRuntimeError("authoritative summary run id or fingerprint differs")
    if (
        manifest.get("schema_version") != 2
        or manifest.get("algorithm") != "sha256"
        or manifest.get("run_id") != requested_run_id
        or manifest.get("run_fingerprint") != fingerprint
    ):
        raise ProspectiveRuntimeError("authoritative manifest run identity differs")

    git_state = summary.get("git")
    if not isinstance(git_state, Mapping):
        raise ProspectiveRuntimeError("authoritative summary has no Git state")
    if git_state.get("commit") != release_commit_oid or git_state.get("dirty") is not False:
        raise ProspectiveRuntimeError(
            "authoritative run must be clean and built from the peeled release commit"
        )

    manifest_self_sha256 = _require_sha256(
        manifest.get("manifest_sha256"), name="manifest self sha256"
    )
    if manifest_self_sha256 != _manifest_payload_sha256(manifest):
        raise ProspectiveRuntimeError("authoritative manifest self hash differs")
    manifest_files = _verify_manifest_files(run_dir, manifest.get("files"))
    for required_path in ("summary.json", "adaptive/adaptive-summary.json"):
        if required_path not in manifest_files:
            raise ProspectiveRuntimeError(
                f"authoritative manifest omits required file: {required_path}"
            )

    inputs = manifest.get("inputs")
    if not isinstance(inputs, list):
        raise ProspectiveRuntimeError("authoritative manifest inputs must be a list")
    protocol_inputs = [
        row
        for row in inputs
        if isinstance(row, Mapping) and row.get("role") == "adaptive_protocol"
    ]
    if len(protocol_inputs) != 1:
        raise ProspectiveRuntimeError(
            "authoritative manifest must bind exactly one adaptive protocol input"
        )
    protocol_input = protocol_inputs[0]
    if set(protocol_input) != {"role", "path", "size_bytes", "sha256"}:
        raise ProspectiveRuntimeError("adaptive protocol manifest input has unexpected fields")
    try:
        recorded_protocol_path = Path(str(protocol_input["path"])).resolve()
    except (OSError, ValueError) as exc:
        raise ProspectiveRuntimeError("adaptive protocol manifest path is invalid") from exc
    if (
        recorded_protocol_path != protocol_file
        or type(protocol_input.get("size_bytes")) is not int
        or protocol_input.get("size_bytes") != len(protocol_raw)
        or protocol_input.get("sha256") != protocol_sha256
    ):
        raise ProspectiveRuntimeError("authoritative run binds another adaptive protocol")

    adaptive = summary.get("adaptive")
    if not isinstance(adaptive, Mapping) or not _json_equal(adaptive, adaptive_artifact):
        raise ProspectiveRuntimeError(
            "adaptive/adaptive-summary.json differs from summary.adaptive"
        )
    if (
        adaptive.get("enabled") is not True
        or adaptive.get("canary_smoke_only") is not False
        or adaptive.get("protocol_id") != protocol.get("protocol_id")
        or adaptive.get("protocol_sha256") != protocol_sha256
    ):
        raise ProspectiveRuntimeError("authoritative adaptive protocol binding differs")
    runtime_integrity = adaptive.get("runtime_integrity")
    integrity_criteria = (
        runtime_integrity.get("criteria")
        if isinstance(runtime_integrity, Mapping)
        else None
    )
    if (
        summary.get("adaptive_results_interpretable") is not True
        or adaptive.get("integrity_valid") is not True
        or not isinstance(runtime_integrity, Mapping)
        or runtime_integrity.get("passed") is not True
        or not isinstance(integrity_criteria, list)
        or not integrity_criteria
        or any(
            not isinstance(row, Mapping) or row.get("passed") is not True
            for row in integrity_criteria
        )
    ):
        raise ProspectiveRuntimeError("authoritative adaptive integrity is not true")

    frozen_gates = protocol.get("frozen_gates")
    routing = protocol.get("routing")
    paired_comparisons = adaptive.get("paired_comparisons")
    mean_overlay_fraction = adaptive.get(
        "mean_fraction_signal_dates_exposure_below_one"
    )
    if (
        not isinstance(frozen_gates, Mapping)
        or not isinstance(routing, Mapping)
        or not isinstance(paired_comparisons, Mapping)
        or routing.get("allow_post_run_threshold_changes") is not False
        or routing.get("allow_historical_rerun_to_change_route_after_release") is not False
    ):
        raise ProspectiveRuntimeError("frozen adaptive gates or routing are invalid")
    try:
        from factor_lab.research.adaptive_runtime import (
            _determine_route,
            _evaluate_frozen_gates,
        )

        recomputed_gates = _evaluate_frozen_gates(
            frozen_gates,
            paired_comparisons,
            mean_overlay_fraction=float(mean_overlay_fraction),
        )
        recomputed_route = _determine_route(
            recomputed_gates,
            integrity_passed=True,
        )
    except (TypeError, ValueError, KeyError) as exc:
        raise ProspectiveRuntimeError(
            "authoritative adaptive gates cannot be recomputed"
        ) from exc
    gate_passes = {
        name: bool((recomputed_gates.get(name) or {}).get("passed"))
        for name in recomputed_gates
    }
    expected_route = (
        routing.get("if_core_overlay_gate_fails")
        if not gate_passes.get("core_overlay", False)
        else routing.get("if_all_four_gates_pass")
        if gate_passes and all(gate_passes.values())
        else routing.get("otherwise")
    )
    if recomputed_route.get("selected_account") != expected_route:
        raise ProspectiveRuntimeError("frozen routing contract differs from recomputed route")
    if (
        not _json_equal(recomputed_gates, adaptive.get("gate_results"))
        or not _json_equal(recomputed_gates, adaptive.get("frozen_gate_results"))
        or not _json_equal(recomputed_route, adaptive.get("route"))
        or adaptive.get("frozen_route") != expected_route
    ):
        raise ProspectiveRuntimeError("authoritative frozen gates or route differ")
    frozen_route = adaptive.get("frozen_route")
    if not isinstance(frozen_route, str) or not frozen_route:
        raise ProspectiveRuntimeError("authoritative frozen route is empty")

    # Re-read the envelope files last so concurrent rewrites cannot mix states.
    if (
        sha256_file(run_dir / "manifest.json") != sha256_bytes(manifest_raw)
        or sha256_file(run_dir / "summary.json")
        != manifest_files["summary.json"]["sha256"]
        or sha256_file(run_dir / "adaptive" / "adaptive-summary.json")
        != manifest_files["adaptive/adaptive-summary.json"]["sha256"]
    ):
        raise ProspectiveRuntimeError("authoritative run changed during verification")

    return {
        "authoritative_run_id": requested_run_id,
        "run_fingerprint": fingerprint,
        "manifest_sha256": sha256_bytes(manifest_raw),
        "manifest_self_sha256": manifest_self_sha256,
        "adaptive_summary_sha256": sha256_bytes(adaptive_raw),
        "frozen_route": frozen_route,
        "integrity_valid": True,
    }


def _parse_run_poll(value: bytes, *, expected_run_id: int) -> Mapping[str, Any]:
    try:
        payload = json.loads(
            value.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_pairs,
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ProspectiveRuntimeError("workflow-run command returned invalid JSON") from exc
    if not isinstance(payload, Mapping):
        raise ProspectiveRuntimeError("workflow-run command did not return an object")
    if payload.get("id") != expected_run_id:
        raise ProspectiveRuntimeError("polled workflow run differs from the dispatched run")
    status = payload.get("status")
    if not isinstance(status, str):
        raise ProspectiveRuntimeError("workflow-run response has no string status")
    return payload


def _poll_workflow_run(
    *,
    workflow_run_id: int,
    request_id: str,
    repository: str,
    release_tag: str,
    release_commit_oid: str,
    admission_deadline_utc: str | None,
    command_runner: CommandRunner,
    sleeper: Sleeper,
    poll_interval_seconds: float,
    max_poll_attempts: int,
) -> dict[str, Any]:
    command = build_workflow_run_command(workflow_run_id, repository=repository)
    for attempt in range(max_poll_attempts):
        raw = _execute(command, command_runner)
        payload = _parse_run_poll(raw, expected_run_id=workflow_run_id)
        status = payload["status"]
        if status == "completed":
            try:
                return validate_workflow_run(
                    payload,
                    workflow_run_id=workflow_run_id,
                    request_id=request_id,
                    repository=repository,
                    release_tag=release_tag,
                    release_commit_oid=release_commit_oid,
                    admission_deadline_utc=admission_deadline_utc,
                )
            except AttestationError as exc:
                raise ProspectiveRuntimeError("completed workflow run failed validation") from exc
        if status not in _PENDING_RUN_STATES:
            raise ProspectiveRuntimeError(f"unsupported workflow-run status: {status!r}")
        if attempt + 1 == max_poll_attempts:
            break
        try:
            sleeper(poll_interval_seconds)
        except Exception as exc:
            raise ProspectiveRuntimeError("workflow polling sleeper failed") from exc
    raise ProspectiveRuntimeError(
        f"workflow run {workflow_run_id} did not complete after {max_poll_attempts} polls"
    )


def _snapshot_bytes(value: str | Path | bytes | bytearray | memoryview) -> bytes:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    try:
        return Path(value).read_bytes()
    except OSError as exc:
        raise ProspectiveRuntimeError(f"cannot read prospective snapshot: {value}") from exc


def _single_downloaded_bundle(directory: Path) -> Path:
    try:
        entries = list(directory.iterdir())
    except OSError as exc:
        raise ProspectiveRuntimeError("cannot inspect attestation download directory") from exc
    if (
        len(entries) != 1
        or not entries[0].is_file()
        or entries[0].suffix.lower() != ".jsonl"
    ):
        raise ProspectiveRuntimeError(
            "attestation download must produce exactly one JSONL bundle"
        )
    return entries[0]


def attest_snapshot(
    ledger_root: str | Path,
    snapshot: str | Path | bytes | bytearray | memoryview,
    *,
    purpose: str,
    release_commit_oid: str,
    decision_record_sha256: str | None = None,
    admission_deadline_utc: str | None = None,
    workflow_run_id: int | None = None,
    repository: str = DEFAULT_REPOSITORY,
    release_tag: str = "5.0",
    workflow: str = DEFAULT_WORKFLOW,
    recorded_at_utc: datetime | str | None = None,
    ledger_id: str = DEFAULT_LEDGER_ID,
    command_runner: CommandRunner = run_command,
    sleeper: Sleeper = time.sleep,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
    max_poll_attempts: int = DEFAULT_MAX_POLL_ATTEMPTS,
) -> dict[str, Any]:
    """Attest one sealed snapshot and append its verified ledger receipt.

    Supplying ``workflow_run_id`` resumes a previously dispatched request and
    deliberately skips dispatch.  No ledger receipt is appended until the run,
    downloaded bundle, and local ``gh attestation verify`` result all pass.
    """

    if purpose not in {
        "activation_canary",
        "implementation_upgrade_canary",
        "decision_anchor",
    }:
        raise ValueError(
            "purpose must be activation_canary, implementation_upgrade_canary, "
            "or decision_anchor"
        )
    if purpose in {"activation_canary", "implementation_upgrade_canary"}:
        if decision_record_sha256 is not None or admission_deadline_utc is not None:
            raise ValueError(f"{purpose} cannot carry decision admission fields")
    else:
        if decision_record_sha256 is None:
            raise ValueError("decision_anchor requires decision_record_sha256")
        if admission_deadline_utc is None:
            raise ValueError("decision_anchor requires admission_deadline_utc")
    if workflow_run_id is not None and (
        type(workflow_run_id) is not int or workflow_run_id <= 0
    ):
        raise ValueError("workflow_run_id must be a positive integer")
    if type(max_poll_attempts) is not int or max_poll_attempts <= 0:
        raise ValueError("max_poll_attempts must be a positive integer")
    if isinstance(poll_interval_seconds, bool) or not isinstance(
        poll_interval_seconds, (int, float)
    ) or poll_interval_seconds < 0:
        raise ValueError("poll_interval_seconds must be non-negative")

    snapshot_content = _snapshot_bytes(snapshot)
    try:
        request = build_dispatch_request(
            snapshot_content,
            repository=repository,
            release_tag=release_tag,
            workflow=workflow,
        )
    except Exception as exc:
        raise ProspectiveRuntimeError("snapshot cannot be dispatched for attestation") from exc

    resumed = workflow_run_id is not None
    if workflow_run_id is None:
        try:
            dispatch_response = parse_dispatch_response(
                _execute(request.command, command_runner),
                repository=repository,
            )
        except (AttestationError, ProspectiveRuntimeError) as exc:
            raise ProspectiveRuntimeError("workflow dispatch failed validation") from exc
        workflow_run_id = int(dispatch_response["workflow_run_id"])
    else:
        dispatch_response = {
            "workflow_run_id": workflow_run_id,
            "run_url": (
                f"https://api.github.com/repos/{repository}/actions/runs/{workflow_run_id}"
            ),
            "html_url": f"https://github.com/{repository}/actions/runs/{workflow_run_id}",
        }

    workflow_run = _poll_workflow_run(
        workflow_run_id=workflow_run_id,
        request_id=request.request_id,
        repository=repository,
        release_tag=release_tag,
        release_commit_oid=release_commit_oid,
        admission_deadline_utc=admission_deadline_utc,
        command_runner=command_runner,
        sleeper=sleeper,
        poll_interval_seconds=float(poll_interval_seconds),
        max_poll_attempts=max_poll_attempts,
    )

    with tempfile.TemporaryDirectory(prefix="factor-lab-attestation-") as temporary:
        temporary_root = Path(temporary)
        snapshot_path = temporary_root / request.snapshot_name
        snapshot_path.write_bytes(snapshot_content)
        download_directory = temporary_root / "download"
        download_directory.mkdir()

        download_command = build_attestation_download_command(
            snapshot_path,
            repository=repository,
            output_directory=download_directory,
        )
        _execute(download_command, command_runner)
        bundle_path = _single_downloaded_bundle(download_directory)

        verify_command = build_attestation_verify_command(
            snapshot_path,
            bundle_path,
            repository=repository,
            release_tag=release_tag,
            release_commit_oid=release_commit_oid,
        )
        try:
            verification = validate_verification_output(
                _execute(verify_command, command_runner),
                snapshot_sha256=request.snapshot_sha256,
                snapshot_name=request.snapshot_name,
                expected_certificate_identity=certificate_identity(
                    repository=repository,
                    release_tag=release_tag,
                ),
                repository=repository,
                workflow_run_id=workflow_run_id,
                workflow_run_attempt=workflow_run["workflow_run_attempt"],
                admission_deadline_utc=admission_deadline_utc,
            )
        except (AttestationError, ProspectiveRuntimeError) as exc:
            raise ProspectiveRuntimeError("attestation verification failed") from exc

        layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
        bundle = store_attestation_bundle(
            bundle_path,
            layout.bundles,
            snapshot_sha256=request.snapshot_sha256,
        )

    try:
        receipt_payload = build_receipt_payload(
            purpose=purpose,
            snapshot=snapshot_content,
            request=request,
            dispatch_response=dispatch_response,
            workflow_run=workflow_run,
            verification=verification,
            attestation_bundle_sha256=bundle["attestation_bundle_sha256"],
            decision_record_sha256=decision_record_sha256,
            admission_deadline_utc=admission_deadline_utc,
        )
        receipt = append_attestation_receipt(
            ledger_root,
            receipt_payload,
            recorded_at_utc=recorded_at_utc,
            ledger_id=ledger_id,
        )
    except Exception as exc:
        raise ProspectiveRuntimeError("verified receipt could not be appended") from exc

    return {
        "request_id": request.request_id,
        "snapshot_sha256": request.snapshot_sha256,
        "workflow_run_id": workflow_run_id,
        "workflow_run_attempt": workflow_run["workflow_run_attempt"],
        "resumed": resumed,
        "workflow_run": workflow_run,
        "verification": verification,
        "bundle": bundle,
        "receipt": receipt,
    }


__all__ = [
    "CommandResult",
    "DEFAULT_COMMAND_TIMEOUT_SECONDS",
    "DEFAULT_MAX_POLL_ATTEMPTS",
    "DEFAULT_POLL_INTERVAL_SECONDS",
    "ProspectiveRuntimeError",
    "attest_snapshot",
    "run_command",
    "verify_authoritative_run",
]
