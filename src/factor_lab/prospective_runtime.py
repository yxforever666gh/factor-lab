"""Transactional network orchestration for prospective snapshot attestation.

The ledger and attestation modules define evidence and trust contracts.  This
module is the deliberately small imperative layer between them: it executes
only explicit argv vectors, validates every external response, downloads into
a fresh directory, and appends a receipt only after the entire chain succeeds.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from pathlib import PurePosixPath
import re
import stat
import subprocess
import tempfile
import threading
import time
from typing import Any, Callable, Iterator, Mapping, Protocol, Sequence
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
    build_workflow_runs_query_command,
    certificate_identity,
    parse_dispatch_response,
    store_attestation_bundle,
    validate_verification_output,
    validate_workflow_run,
    validate_workflow_run_identity,
    WORKFLOW_PATH,
)
from factor_lab.prospective_ledger import (
    DEFAULT_LEDGER_ID,
    LedgerLayout,
    append_attestation_receipt,
    audit_ledger,
    canonical_json_bytes,
    create_only_file,
    sha256_bytes,
    sha256_file,
    strict_load_canonical,
)


DEFAULT_COMMAND_TIMEOUT_SECONDS = 120.0
DEFAULT_POLL_INTERVAL_SECONDS = 5.0
DEFAULT_MAX_POLL_ATTEMPTS = 120
DEFAULT_DISPATCH_RECONCILE_INTERVAL_SECONDS = 30.0
DEFAULT_DISPATCH_RECONCILE_ATTEMPTS = 5
_PENDING_RUN_STATES = frozenset(
    {"queued", "in_progress", "requested", "waiting", "pending"}
)
_RUN_ID_RE = re.compile(r"^[0-9a-f]{16}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_OID_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_UTC_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
)


class ProspectiveRuntimeError(RuntimeError):
    """Raised when attestation orchestration cannot produce trusted evidence."""


_DISPATCH_BINDING_KEYS = {
    "schema_version",
    "kind",
    "request_id",
    "snapshot_sha256",
    "snapshot_name",
    "repository",
    "release_tag",
    "workflow",
    "workflow_run_id",
    "run_url",
    "html_url",
}
_DISPATCH_INTENT_KEYS = {
    "schema_version",
    "kind",
    "request_id",
    "snapshot_sha256",
    "snapshot_name",
    "repository",
    "release_tag",
    "workflow",
    "created_at_utc",
}
_REQUEST_THREAD_LOCKS: dict[str, threading.Lock] = {}
_REQUEST_THREAD_LOCKS_GUARD = threading.Lock()


def _request_identity(request: Any, *, kind: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": kind,
        "request_id": request.request_id,
        "snapshot_sha256": request.snapshot_sha256,
        "snapshot_name": request.snapshot_name,
        "repository": request.repository,
        "release_tag": request.release_tag,
        "workflow": request.workflow,
    }


def _runtime_now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _runtime_utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ProspectiveRuntimeError("runtime timestamp must be timezone-aware")
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _parse_runtime_utc(value: Any, *, description: str) -> datetime:
    if not isinstance(value, str) or not _UTC_RE.fullmatch(value):
        raise ProspectiveRuntimeError(f"{description} is not canonical UTC seconds")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise ProspectiveRuntimeError(f"{description} is invalid") from exc


def _ensure_dispatch_directory(layout: LedgerLayout) -> None:
    try:
        layout.ensure_directories()
        metadata = layout.dispatch.lstat()
    except OSError as exc:
        raise ProspectiveRuntimeError(
            "attestation dispatch directory could not be prepared"
        ) from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ProspectiveRuntimeError(
            "attestation dispatch directory is not a regular directory"
        )


def _regular_file_metadata(path: Path, *, description: str) -> os.stat_result | None:
    """Return ``lstat`` metadata while treating dangling links as corruption."""

    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ProspectiveRuntimeError(f"cannot inspect {description}") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ProspectiveRuntimeError(f"{description} is not a regular file")
    return metadata


@contextmanager
def _request_lock(
    layout: LedgerLayout,
    request_id: str,
    *,
    timeout_seconds: float = 15.0,
) -> Iterator[None]:
    """Serialize one deterministic request across threads and processes."""

    if not _SHA256_RE.fullmatch(request_id):
        raise ProspectiveRuntimeError("attestation request lock id is invalid")
    _ensure_dispatch_directory(layout)
    lock_path = layout.dispatch / f".{request_id}.lock"
    lock_key = str(lock_path.resolve())
    with _REQUEST_THREAD_LOCKS_GUARD:
        thread_lock = _REQUEST_THREAD_LOCKS.setdefault(lock_key, threading.Lock())
    started = time.monotonic()
    if not thread_lock.acquire(timeout=timeout_seconds):
        raise ProspectiveRuntimeError("timed out acquiring attestation request lock")
    descriptor: int | None = None
    locked = False
    try:
        _regular_file_metadata(lock_path, description="attestation request lock")
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(lock_path, flags, 0o600)
        except OSError as exc:
            raise ProspectiveRuntimeError(
                "attestation request lock could not be opened"
            ) from exc
        descriptor_metadata = os.fstat(descriptor)
        try:
            path_metadata = lock_path.lstat()
        except OSError as exc:
            raise ProspectiveRuntimeError(
                "attestation request lock changed while opening"
            ) from exc
        if (
            stat.S_ISLNK(path_metadata.st_mode)
            or not stat.S_ISREG(path_metadata.st_mode)
            or not stat.S_ISREG(descriptor_metadata.st_mode)
            or descriptor_metadata.st_nlink != 1
            or not os.path.samestat(path_metadata, descriptor_metadata)
        ):
            raise ProspectiveRuntimeError("attestation request lock is not a regular file")
        if descriptor_metadata.st_size == 0:
            os.write(descriptor, b"\0")
            os.fsync(descriptor)
        if os.name == "nt":
            import msvcrt

            while True:
                try:
                    os.lseek(descriptor, 0, os.SEEK_SET)
                    msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                    locked = True
                    break
                except OSError:
                    if time.monotonic() - started >= timeout_seconds:
                        raise ProspectiveRuntimeError(
                            "timed out acquiring attestation request lock"
                        )
                    time.sleep(0.05)
        else:
            import fcntl

            while True:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    locked = True
                    break
                except BlockingIOError:
                    if time.monotonic() - started >= timeout_seconds:
                        raise ProspectiveRuntimeError(
                            "timed out acquiring attestation request lock"
                        )
                    time.sleep(0.05)
        yield
    finally:
        try:
            if descriptor is not None:
                try:
                    if locked:
                        if os.name == "nt":
                            import msvcrt

                            os.lseek(descriptor, 0, os.SEEK_SET)
                            msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
                        else:
                            import fcntl

                            fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    os.close(descriptor)
        finally:
            thread_lock.release()


def _dispatch_intent_path(layout: LedgerLayout, request: Any) -> Path:
    return layout.dispatch / f"{request.request_id}.intent.json"


def _load_dispatch_intent(layout: LedgerLayout, request: Any) -> dict[str, Any] | None:
    path = _dispatch_intent_path(layout, request)
    if _regular_file_metadata(
        path, description="attestation dispatch intent"
    ) is None:
        return None
    try:
        payload = strict_load_canonical(path.read_bytes())
    except Exception as exc:
        raise ProspectiveRuntimeError("attestation dispatch intent is invalid") from exc
    if not isinstance(payload, Mapping) or set(payload) != _DISPATCH_INTENT_KEYS:
        raise ProspectiveRuntimeError("attestation dispatch intent schema differs")
    expected = _request_identity(
        request, kind="prospective_attestation_dispatch_intent"
    )
    if any(payload.get(key) != value for key, value in expected.items()):
        raise ProspectiveRuntimeError("attestation dispatch intent identity differs")
    _parse_runtime_utc(
        payload.get("created_at_utc"),
        description="attestation dispatch intent creation time",
    )
    return dict(payload)


def _store_dispatch_intent(
    layout: LedgerLayout,
    request: Any,
    *,
    created_at_utc: datetime | None = None,
) -> bool:
    existing = _load_dispatch_intent(layout, request)
    if existing is not None:
        return False
    path = _dispatch_intent_path(layout, request)
    payload = _request_identity(
        request, kind="prospective_attestation_dispatch_intent"
    )
    payload["created_at_utc"] = _runtime_utc_text(
        _runtime_now_utc() if created_at_utc is None else created_at_utc
    )
    try:
        created = create_only_file(path, canonical_json_bytes(payload))
    except Exception as exc:
        raise ProspectiveRuntimeError(
            "attestation dispatch intent could not be stored"
        ) from exc
    if _load_dispatch_intent(layout, request) != payload:
        raise ProspectiveRuntimeError("stored attestation dispatch intent differs")
    return created


def _dispatch_binding(
    layout: LedgerLayout,
    request: Any,
    response: Mapping[str, Any],
) -> tuple[Path, dict[str, Any]]:
    run_id = response.get("workflow_run_id")
    if type(run_id) is not int or run_id <= 0:
        raise ProspectiveRuntimeError("dispatch binding has no positive workflow run id")
    expected_run_url = (
        f"https://api.github.com/repos/{request.repository}/actions/runs/{run_id}"
    )
    expected_html_url = (
        f"https://github.com/{request.repository}/actions/runs/{run_id}"
    )
    if (
        response.get("run_url") != expected_run_url
        or response.get("html_url") != expected_html_url
    ):
        raise ProspectiveRuntimeError("dispatch binding URLs differ from its run id")
    payload = {
        "schema_version": 1,
        "kind": "prospective_attestation_dispatch",
        "request_id": request.request_id,
        "snapshot_sha256": request.snapshot_sha256,
        "snapshot_name": request.snapshot_name,
        "repository": request.repository,
        "release_tag": request.release_tag,
        "workflow": request.workflow,
        "workflow_run_id": run_id,
        "run_url": expected_run_url,
        "html_url": expected_html_url,
    }
    return layout.dispatch / f"{request.request_id}.json", payload


def _load_dispatch_binding(layout: LedgerLayout, request: Any) -> dict[str, Any] | None:
    path = layout.dispatch / f"{request.request_id}.json"
    if _regular_file_metadata(
        path, description="attestation dispatch binding"
    ) is None:
        return None
    try:
        payload = strict_load_canonical(path.read_bytes())
    except Exception as exc:
        raise ProspectiveRuntimeError("attestation dispatch binding is invalid") from exc
    if not isinstance(payload, Mapping) or set(payload) != _DISPATCH_BINDING_KEYS:
        raise ProspectiveRuntimeError("attestation dispatch binding schema differs")
    response = {
        "workflow_run_id": payload.get("workflow_run_id"),
        "run_url": payload.get("run_url"),
        "html_url": payload.get("html_url"),
    }
    _path, expected = _dispatch_binding(layout, request, response)
    if dict(payload) != expected:
        raise ProspectiveRuntimeError("attestation dispatch binding identity differs")
    return dict(payload)


def _store_dispatch_binding(
    layout: LedgerLayout, request: Any, response: Mapping[str, Any]
) -> dict[str, Any]:
    path, payload = _dispatch_binding(layout, request, response)
    existing = _load_dispatch_binding(layout, request)
    if existing is not None:
        if existing != payload:
            raise ProspectiveRuntimeError(
                "persisted attestation dispatch binding differs"
            )
        return existing
    try:
        create_only_file(path, canonical_json_bytes(payload))
    except Exception as exc:
        raise ProspectiveRuntimeError("attestation dispatch binding could not be stored") from exc
    loaded = _load_dispatch_binding(layout, request)
    if loaded != payload:
        raise ProspectiveRuntimeError("stored attestation dispatch binding differs")
    return payload


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
            parse_constant=_reject_nonfinite_json,
        )
    except ProspectiveRuntimeError:
        raise
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


def _parse_workflow_run_query(value: bytes) -> list[dict[str, Any]]:
    try:
        payload = json.loads(
            value.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_pairs,
            parse_constant=_reject_nonfinite_json,
        )
    except ProspectiveRuntimeError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ProspectiveRuntimeError(
            "workflow-run reconciliation returned invalid JSON"
        ) from exc
    if not isinstance(payload, list) or not payload:
        raise ProspectiveRuntimeError(
            "workflow-run reconciliation did not return paginated results"
        )
    runs: list[dict[str, Any]] = []
    total_count: int | None = None
    seen_ids: set[int] = set()
    for page in payload:
        if not isinstance(page, Mapping):
            raise ProspectiveRuntimeError(
                "workflow-run reconciliation page is not an object"
            )
        page_total = page.get("total_count")
        page_runs = page.get("workflow_runs")
        if (
            type(page_total) is not int
            or page_total < 0
            or not isinstance(page_runs, list)
        ):
            raise ProspectiveRuntimeError(
                "workflow-run reconciliation page schema differs"
            )
        if total_count is None:
            total_count = page_total
        elif page_total != total_count:
            raise ProspectiveRuntimeError(
                "workflow-run reconciliation total changed across pages"
            )
        for candidate in page_runs:
            if not isinstance(candidate, Mapping):
                raise ProspectiveRuntimeError(
                    "workflow-run reconciliation candidate is not an object"
                )
            run_id = candidate.get("id")
            if type(run_id) is not int or run_id <= 0 or run_id in seen_ids:
                raise ProspectiveRuntimeError(
                    "workflow-run reconciliation candidate id is invalid or duplicated"
                )
            seen_ids.add(run_id)
            runs.append(dict(candidate))
    if total_count != len(runs):
        raise ProspectiveRuntimeError(
            "workflow-run reconciliation did not enumerate every matching run"
        )
    return runs


def _validate_run_identity(
    payload: Mapping[str, Any],
    *,
    workflow_run_id: int,
    request_id: str,
    repository: str,
    release_tag: str,
    release_commit_oid: str,
    admission_deadline_utc: str | None,
) -> dict[str, Any]:
    try:
        return validate_workflow_run_identity(
            payload,
            workflow_run_id=workflow_run_id,
            request_id=request_id,
            repository=repository,
            release_tag=release_tag,
            release_commit_oid=release_commit_oid,
            admission_deadline_utc=admission_deadline_utc,
        )
    except AttestationError as exc:
        raise ProspectiveRuntimeError("workflow run identity failed validation") from exc


def _fetch_run_identity(
    *,
    workflow_run_id: int,
    request_id: str,
    repository: str,
    release_tag: str,
    release_commit_oid: str,
    admission_deadline_utc: str | None,
    command_runner: CommandRunner,
) -> dict[str, Any]:
    command = build_workflow_run_command(workflow_run_id, repository=repository)
    payload = dict(
        _parse_run_poll(
            _execute(command, command_runner),
            expected_run_id=workflow_run_id,
        )
    )
    _validate_run_identity(
        payload,
        workflow_run_id=workflow_run_id,
        request_id=request_id,
        repository=repository,
        release_tag=release_tag,
        release_commit_oid=release_commit_oid,
        admission_deadline_utc=admission_deadline_utc,
    )
    return payload


def _reconcile_remote_dispatch(
    *,
    request: Any,
    release_commit_oid: str,
    admission_deadline_utc: str | None,
    command_runner: CommandRunner,
) -> dict[str, Any] | None:
    command = build_workflow_runs_query_command(
        repository=request.repository,
        workflow=request.workflow,
    )
    runs = _parse_workflow_run_query(_execute(command, command_runner))
    title = f"prospective-{request.request_id}"
    matches = [run for run in runs if run.get("display_title") == title]
    if len(matches) > 1:
        raise ProspectiveRuntimeError(
            "multiple workflow runs match the deterministic attestation request"
        )
    if not matches:
        return None
    candidate = matches[0]
    workflow_run_id = int(candidate["id"])
    _validate_run_identity(
        candidate,
        workflow_run_id=workflow_run_id,
        request_id=request.request_id,
        repository=request.repository,
        release_tag=request.release_tag,
        release_commit_oid=release_commit_oid,
        admission_deadline_utc=admission_deadline_utc,
    )
    return _fetch_run_identity(
        workflow_run_id=workflow_run_id,
        request_id=request.request_id,
        repository=request.repository,
        release_tag=request.release_tag,
        release_commit_oid=release_commit_oid,
        admission_deadline_utc=admission_deadline_utc,
        command_runner=command_runner,
    )


def _reconcile_remote_dispatch_with_grace(
    *,
    request: Any,
    release_commit_oid: str,
    admission_deadline_utc: str | None,
    command_runner: CommandRunner,
    sleeper: Sleeper,
    attempts: int,
) -> dict[str, Any] | None:
    """Allow an earlier dispatch time to become visible before retrying it."""

    for attempt in range(attempts):
        try:
            candidate = _reconcile_remote_dispatch(
                request=request,
                release_commit_oid=release_commit_oid,
                admission_deadline_utc=admission_deadline_utc,
                command_runner=command_runner,
            )
        except ProspectiveRuntimeError as exc:
            raise ProspectiveRuntimeError(
                "workflow dispatch reconciliation failed"
            ) from exc
        if candidate is not None:
            return candidate
        if attempt + 1 == attempts:
            break
        try:
            sleeper(DEFAULT_DISPATCH_RECONCILE_INTERVAL_SECONDS)
        except Exception as exc:
            raise ProspectiveRuntimeError(
                "workflow dispatch reconciliation sleeper failed"
            ) from exc
    return None


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
    initial_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    command = build_workflow_run_command(workflow_run_id, repository=repository)
    for attempt in range(max_poll_attempts):
        if attempt == 0 and initial_payload is not None:
            payload = initial_payload
        else:
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


def inspect_dispatch_evidence(
    ledger_root: str | Path,
    snapshot: str | Path | bytes | bytearray | memoryview,
    *,
    repository: str = DEFAULT_REPOSITORY,
    release_tag: str = "5.0",
    workflow: str = DEFAULT_WORKFLOW,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    """Read and strictly validate local dispatch evidence without creating it."""

    snapshot_content = _snapshot_bytes(snapshot)
    try:
        request = build_dispatch_request(
            snapshot_content,
            repository=repository,
            release_tag=release_tag,
            workflow=workflow,
        )
    except Exception as exc:
        raise ProspectiveRuntimeError(
            "snapshot cannot be inspected for dispatch evidence"
        ) from exc
    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    try:
        dispatch_metadata = layout.dispatch.lstat()
    except FileNotFoundError:
        dispatch_metadata = None
    except OSError as exc:
        raise ProspectiveRuntimeError(
            "attestation dispatch directory cannot be inspected"
        ) from exc
    if dispatch_metadata is not None and (
        stat.S_ISLNK(dispatch_metadata.st_mode)
        or not stat.S_ISDIR(dispatch_metadata.st_mode)
    ):
        raise ProspectiveRuntimeError(
            "attestation dispatch directory is not a regular directory"
        )
    binding = _load_dispatch_binding(layout, request)
    intent = _load_dispatch_intent(layout, request)
    return {
        "request_id": request.request_id,
        "binding": binding,
        "intent": intent,
    }


def _decision_deadline_reached(admission_deadline_utc: str | None) -> bool:
    if admission_deadline_utc is None:
        return False
    deadline = _parse_runtime_utc(
        admission_deadline_utc,
        description="decision attestation deadline",
    )
    return _runtime_now_utc() >= deadline


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


def _audited_record(path_value: Any) -> dict[str, Any]:
    path = Path(str(path_value))
    if _regular_file_metadata(path, description="prospective ledger record") is None:
        raise ProspectiveRuntimeError("audited prospective ledger record is missing")
    try:
        record = strict_load_canonical(path.read_bytes())
    except Exception as exc:
        raise ProspectiveRuntimeError("audited prospective ledger record is invalid") from exc
    if not isinstance(record, Mapping):
        raise ProspectiveRuntimeError("audited prospective ledger record is not an object")
    return dict(record)


def _receipt_snapshot(
    layout: LedgerLayout,
    *,
    sequence: int,
    record_sha256: str,
) -> dict[str, Any]:
    matches: list[tuple[Path, dict[str, Any], bytes]] = []
    for path in sorted(layout.snapshots.glob(f"{sequence:016d}-*.json")):
        if _regular_file_metadata(
            path, description="prospective receipt snapshot"
        ) is None:
            continue
        try:
            raw = path.read_bytes()
            payload = strict_load_canonical(raw)
        except Exception as exc:
            raise ProspectiveRuntimeError("prospective receipt snapshot is invalid") from exc
        if (
            isinstance(payload, Mapping)
            and payload.get("head_record_sha256") == record_sha256
        ):
            matches.append((path, dict(payload), raw))
    if len(matches) != 1:
        raise ProspectiveRuntimeError(
            "expected exactly one snapshot for the existing attestation receipt"
        )
    path, payload, raw = matches[0]
    return {
        "snapshot_sha256": sha256_bytes(raw),
        "path": str(path),
        "created": False,
        "snapshot": payload,
    }


def _existing_receipt_result(
    layout: LedgerLayout,
    request: Any,
    snapshot_content: bytes,
    *,
    purpose: str,
    release_commit_oid: str,
    decision_record_sha256: str | None,
    admission_deadline_utc: str | None,
    expected_workflow_run_id: int | None,
    ledger_id: str,
) -> dict[str, Any] | None:
    audit = audit_ledger(layout.root, ledger_id=ledger_id)
    if audit.get("valid") is not True:
        raise ProspectiveRuntimeError(
            "prospective ledger is invalid before attestation recovery"
        )
    parsed_snapshot = strict_load_canonical(snapshot_content)
    if not isinstance(parsed_snapshot, Mapping):
        raise ProspectiveRuntimeError("attestation snapshot is not an object")
    if purpose == "decision_anchor":
        decision_rows = [
            row
            for row in audit["records"]
            if row.get("record_sha256") == decision_record_sha256
        ]
        if len(decision_rows) != 1:
            raise ProspectiveRuntimeError(
                "decision receipt recovery cannot locate its decision record"
            )
        decision_record = _audited_record(decision_rows[0]["path"])
        decision_payload = decision_record.get("payload")
        plan = (
            decision_payload.get("plan")
            if isinstance(decision_payload, Mapping)
            else None
        )
        if (
            not isinstance(plan, Mapping)
            or plan.get("admission_deadline_utc") != admission_deadline_utc
        ):
            raise ProspectiveRuntimeError(
                "decision receipt recovery admission deadline differs"
            )

    matches: list[tuple[Mapping[str, Any], dict[str, Any], dict[str, Any]]] = []
    for row in audit["records"]:
        if row.get("kind") != "attestation_receipt":
            continue
        record = _audited_record(row["path"])
        payload = record.get("payload")
        if not isinstance(payload, Mapping) or payload.get("request_id") != request.request_id:
            continue
        workflow_run_id = payload.get("workflow_run_id")
        workflow_run_attempt = payload.get("workflow_run_attempt")
        expected_run_url = (
            f"https://github.com/{request.repository}/actions/runs/{workflow_run_id}"
        )
        expected_invocation = f"{expected_run_url}/attempts/{workflow_run_attempt}"
        expected = {
            "purpose": purpose,
            "snapshot_sha256": request.snapshot_sha256,
            "snapshot_head_record_sha256": parsed_snapshot.get(
                "head_record_sha256"
            ),
            "decision_record_sha256": decision_record_sha256,
            "request_id": request.request_id,
            "workflow_run_display_title": f"prospective-{request.request_id}",
            "workflow_run_url": expected_run_url,
            "workflow_path": WORKFLOW_PATH,
            "workflow_ref": f"refs/tags/{request.release_tag}",
            "workflow_source_commit_oid": release_commit_oid,
            "certificate_identity": certificate_identity(
                repository=request.repository,
                release_tag=request.release_tag,
            ),
            "run_invocation_uri": expected_invocation,
            "subject_name": request.snapshot_name,
            "subject_sha256": request.snapshot_sha256,
        }
        if any(payload.get(key) != value for key, value in expected.items()):
            raise ProspectiveRuntimeError(
                "existing attestation receipt differs from the requested identity"
            )
        if (
            expected_workflow_run_id is not None
            and workflow_run_id != expected_workflow_run_id
        ):
            raise ProspectiveRuntimeError(
                "requested workflow run differs from the existing receipt"
            )
        matches.append((row, record, dict(payload)))
    if len(matches) > 1:
        raise ProspectiveRuntimeError(
            "multiple attestation receipts match the deterministic request"
        )
    if not matches:
        return None

    row, record, payload = matches[0]
    workflow_run_id = int(payload["workflow_run_id"])
    workflow_run_attempt = int(payload["workflow_run_attempt"])
    workflow_run = {
        "workflow_run_id": workflow_run_id,
        "workflow_run_attempt": workflow_run_attempt,
        "request_id": request.request_id,
        "workflow_run_display_title": payload["workflow_run_display_title"],
        "html_url": payload["workflow_run_url"],
        "created_at_utc": payload["workflow_run_created_at_utc"],
        "completed_at_utc": payload["workflow_run_completed_at_utc"],
        "workflow_path": payload["workflow_path"],
        "workflow_ref": payload["workflow_ref"],
        "workflow_source_commit_oid": payload["workflow_source_commit_oid"],
    }
    verification = {
        "verified_timestamp_count": payload["verified_timestamp_count"],
        "verified_timestamps": payload["verified_timestamps"],
        "verified_tlog_type": payload["verified_tlog_type"],
        "verified_tlog_uri": payload["verified_tlog_uri"],
        "verified_tlog_timestamp_utc": payload["verified_tlog_timestamp_utc"],
        "certificate_identity": payload["certificate_identity"],
        "run_invocation_uri": payload["run_invocation_uri"],
        "workflow_run_id": workflow_run_id,
        "workflow_run_attempt": workflow_run_attempt,
        "subject_name": payload["subject_name"],
        "subject_sha256": payload["subject_sha256"],
    }
    bundle_path = layout.bundles / (
        f"{request.snapshot_sha256}-{payload['attestation_bundle_sha256']}.jsonl"
    )
    receipt = {
        "sequence": int(row["sequence"]),
        "kind": "attestation_receipt",
        "record_sha256": row["record_sha256"],
        "path": row["path"],
        "record": record,
        "snapshot": _receipt_snapshot(
            layout,
            sequence=int(row["sequence"]),
            record_sha256=str(row["record_sha256"]),
        ),
    }
    return {
        "request_id": request.request_id,
        "snapshot_sha256": request.snapshot_sha256,
        "workflow_run_id": workflow_run_id,
        "workflow_run_attempt": workflow_run_attempt,
        "resumed": True,
        "workflow_run": workflow_run,
        "verification": verification,
        "bundle": {
            "attestation_bundle_sha256": payload["attestation_bundle_sha256"],
            "path": str(bundle_path),
            "created": False,
        },
        "receipt": receipt,
    }


def _attest_snapshot_locked(
    layout: LedgerLayout,
    snapshot_content: bytes,
    request: Any,
    *,
    purpose: str,
    release_commit_oid: str,
    decision_record_sha256: str | None,
    admission_deadline_utc: str | None,
    workflow_run_id: int | None,
    repository: str,
    release_tag: str,
    recorded_at_utc: datetime | str | None,
    ledger_id: str,
    command_runner: CommandRunner,
    sleeper: Sleeper,
    poll_interval_seconds: float,
    max_poll_attempts: int,
) -> dict[str, Any]:
    persisted_dispatch = _load_dispatch_binding(layout, request)
    persisted_intent = _load_dispatch_intent(layout, request)
    if purpose == "decision_anchor" and persisted_intent is not None:
        intent_created = _parse_runtime_utc(
            persisted_intent.get("created_at_utc"),
            description="attestation dispatch intent creation time",
        )
        deadline = _parse_runtime_utc(
            admission_deadline_utc,
            description="decision attestation deadline",
        )
        if intent_created >= deadline:
            raise ProspectiveRuntimeError(
                "decision attestation dispatch intent was not created before its deadline"
            )
    if persisted_dispatch is not None and workflow_run_id is not None:
        if workflow_run_id != persisted_dispatch["workflow_run_id"]:
            raise ProspectiveRuntimeError(
                "requested workflow run differs from persisted dispatch binding"
            )
    expected_run_id = (
        workflow_run_id
        if workflow_run_id is not None
        else int(persisted_dispatch["workflow_run_id"])
        if persisted_dispatch is not None
        else None
    )
    recovered = _existing_receipt_result(
        layout,
        request,
        snapshot_content,
        purpose=purpose,
        release_commit_oid=release_commit_oid,
        decision_record_sha256=decision_record_sha256,
        admission_deadline_utc=admission_deadline_utc,
        expected_workflow_run_id=expected_run_id,
        ledger_id=ledger_id,
    )
    if recovered is not None:
        return recovered

    initial_payload: Mapping[str, Any] | None = None
    resumed = workflow_run_id is not None or persisted_dispatch is not None
    if persisted_dispatch is not None:
        workflow_run_id = int(persisted_dispatch["workflow_run_id"])
        dispatch_response = {
            "workflow_run_id": workflow_run_id,
            "run_url": persisted_dispatch["run_url"],
            "html_url": persisted_dispatch["html_url"],
        }
    else:
        if workflow_run_id is not None:
            try:
                initial_payload = _fetch_run_identity(
                    workflow_run_id=workflow_run_id,
                    request_id=request.request_id,
                    repository=repository,
                    release_tag=release_tag,
                    release_commit_oid=release_commit_oid,
                    admission_deadline_utc=admission_deadline_utc,
                    command_runner=command_runner,
                )
            except ProspectiveRuntimeError as exc:
                raise ProspectiveRuntimeError(
                    "workflow run failed validation before resume binding"
                ) from exc
            dispatch_response = {
                "workflow_run_id": workflow_run_id,
                "run_url": (
                    f"https://api.github.com/repos/{repository}/actions/runs/"
                    f"{workflow_run_id}"
                ),
                "html_url": (
                    f"https://github.com/{repository}/actions/runs/{workflow_run_id}"
                ),
            }
            _store_dispatch_binding(layout, request, dispatch_response)
        else:
            intent_created_at = _runtime_now_utc()
            if (
                purpose == "decision_anchor"
                and persisted_intent is None
                and intent_created_at
                >= _parse_runtime_utc(
                    admission_deadline_utc,
                    description="decision attestation deadline",
                )
            ):
                raise ProspectiveRuntimeError(
                    "decision attestation deadline passed before local dispatch evidence existed"
                )
            intent_created = _store_dispatch_intent(
                layout,
                request,
                created_at_utc=intent_created_at,
            )
            initial_payload = _reconcile_remote_dispatch_with_grace(
                request=request,
                release_commit_oid=release_commit_oid,
                admission_deadline_utc=admission_deadline_utc,
                command_runner=command_runner,
                sleeper=sleeper,
                attempts=(
                    1
                    if intent_created
                    else DEFAULT_DISPATCH_RECONCILE_ATTEMPTS
                ),
            )
            if initial_payload is not None:
                workflow_run_id = int(initial_payload["id"])
                dispatch_response = {
                    "workflow_run_id": workflow_run_id,
                    "run_url": (
                        f"https://api.github.com/repos/{repository}/actions/runs/"
                        f"{workflow_run_id}"
                    ),
                    "html_url": (
                        f"https://github.com/{repository}/actions/runs/"
                        f"{workflow_run_id}"
                    ),
                }
                _store_dispatch_binding(layout, request, dispatch_response)
                resumed = True
            else:
                if (
                    purpose == "decision_anchor"
                    and _decision_deadline_reached(admission_deadline_utc)
                ):
                    raise ProspectiveRuntimeError(
                        "decision attestation deadline forbids a new workflow dispatch"
                    )
                try:
                    dispatch_response = parse_dispatch_response(
                        _execute(request.command, command_runner),
                        repository=repository,
                    )
                except (AttestationError, ProspectiveRuntimeError) as exc:
                    raise ProspectiveRuntimeError(
                        "workflow dispatch failed validation"
                    ) from exc
                workflow_run_id = int(dispatch_response["workflow_run_id"])
                _store_dispatch_binding(layout, request, dispatch_response)
                resumed = False

    if workflow_run_id is None:
        raise ProspectiveRuntimeError("attestation workflow run id was not resolved")
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
        initial_payload=initial_payload,
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
            layout.root,
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

    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    with _request_lock(layout, request.request_id):
        return _attest_snapshot_locked(
            layout,
            snapshot_content,
            request,
            purpose=purpose,
            release_commit_oid=release_commit_oid,
            decision_record_sha256=decision_record_sha256,
            admission_deadline_utc=admission_deadline_utc,
            workflow_run_id=workflow_run_id,
            repository=repository,
            release_tag=release_tag,
            recorded_at_utc=recorded_at_utc,
            ledger_id=ledger_id,
            command_runner=command_runner,
            sleeper=sleeper,
            poll_interval_seconds=float(poll_interval_seconds),
            max_poll_attempts=max_poll_attempts,
        )


__all__ = [
    "CommandResult",
    "DEFAULT_COMMAND_TIMEOUT_SECONDS",
    "DEFAULT_DISPATCH_RECONCILE_ATTEMPTS",
    "DEFAULT_DISPATCH_RECONCILE_INTERVAL_SECONDS",
    "DEFAULT_MAX_POLL_ATTEMPTS",
    "DEFAULT_POLL_INTERVAL_SECONDS",
    "ProspectiveRuntimeError",
    "attest_snapshot",
    "inspect_dispatch_evidence",
    "run_command",
    "verify_authoritative_run",
]
