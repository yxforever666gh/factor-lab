"""Commit-bound source capsules and isolated prospective computation RPC.

The append-only prospective ledger must remain replayable while ``main`` keeps
moving.  This module installs a small source capsule directly from Git blobs,
verifies that capsule without consulting the current checkout, and executes the
released implementation in an isolated child interpreter.  Audit callers use
``verify_release_capsule`` only: missing capsules fail closed and are never
materialised as a read side effect.

Only the allowlisted operations below may cross the RPC boundary.  The
wire format is canonical JSON without JSON floating-point values; numeric
research inputs therefore remain integer-scaled or canonical binary64 strings.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time as wall_time, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import subprocess
import sys
import sysconfig
import tempfile
from typing import Any
import unicodedata
from zoneinfo import ZoneInfo


SCHEMA_VERSION = 1
RUNNER_ID = "factor-lab/prospective-release-runner/1"
CAPSULE_RECEIPT_NAME = "capsule-receipt.json"
RUNNER_MODULE_PATH = "src/factor_lab/prospective_release_runner.py"
DEFAULT_TIMEOUT_SECONDS = 120.0
REPLAY_HISTORY_TIMEOUT_PER_OPERATION_SECONDS = 5.0
MAX_REPLAY_HISTORY_TIMEOUT_SECONDS = 3600.0
MAX_RPC_BYTES = 64 * 1024 * 1024
OPERATIONS = (
    "build_membership",
    "build_input",
    "replay_target",
    "build_execution",
    "replay_outcome",
    "evaluate",
    "replay_history",
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_OID_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_SAFE_PATH_RE = re.compile(r"^[0-9A-Za-z._/-]+$")
_RELEASE_TAG_RE = re.compile(r"^[0-9]+\.[0-9]+$")


def _canonical_distribution_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value.strip().casefold())


def _running_distribution_versions() -> dict[str, str]:
    result: dict[str, str] = {}
    for distribution in importlib.metadata.distributions():
        raw_name = distribution.metadata.get("Name")
        if not isinstance(raw_name, str) or not raw_name.strip():
            raise ReleaseRunnerError(
                "running distribution omits canonical Name metadata"
            )
        name = _canonical_distribution_name(raw_name)
        if name in result:
            raise ReleaseRunnerError(
                f"duplicate running distribution metadata: {name}"
            )
        version = distribution.version
        if not isinstance(version, str) or not version:
            raise ReleaseRunnerError(
                f"running distribution version is invalid: {name}"
            )
        result[name] = version
    if not result:
        raise ReleaseRunnerError("running environment has no distributions")
    return dict(sorted(result.items()))


class ReleaseRunnerError(RuntimeError):
    """Raised when a release capsule or isolated RPC fails closed."""


@dataclass(frozen=True, slots=True)
class ReleaseCapsule:
    """One verified implementation capsule rooted at a published commit."""

    project_root: Path
    store_root: Path
    root: Path
    source_root: Path
    manifest_path: str
    manifest_sha256: str
    implementation_release_tag: str
    implementation_release_tag_object_oid: str
    implementation_commit_oid: str
    runtime_closure_payload_sha256: str


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _normalise_json(value: Any, path: str = "$") -> Any:
    if value is None or isinstance(value, bool) or type(value) is int:
        return value
    if isinstance(value, str):
        result = unicodedata.normalize("NFC", value)
        if any(0xD800 <= ord(character) <= 0xDFFF for character in result):
            raise ReleaseRunnerError(f"Unicode surrogate is forbidden at {path}")
        return result
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            if not isinstance(raw_key, str):
                raise ReleaseRunnerError(f"non-string JSON key at {path}")
            key = _normalise_json(raw_key, f"{path}.<key>")
            if key in result:
                raise ReleaseRunnerError(
                    f"duplicate key after Unicode normalisation at {path}: {key!r}"
                )
            result[key] = _normalise_json(raw_value, f"{path}.{key}")
        return result
    if isinstance(value, (list, tuple)):
        return [
            _normalise_json(item, f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, float):
        raise ReleaseRunnerError(f"JSON floating-point value is forbidden at {path}")
    raise ReleaseRunnerError(
        f"unsupported JSON value {type(value).__name__} at {path}"
    )


def canonical_json_bytes(value: Any) -> bytes:
    """Encode the exact RPC/capsule JSON form."""

    return json.dumps(
        _normalise_json(value),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _unique_pairs(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for raw_key, value in pairs:
        key = unicodedata.normalize("NFC", raw_key)
        if key in result:
            raise ReleaseRunnerError(f"duplicate JSON key: {key!r}")
        result[key] = value
    return result


def _load_json_bytes(raw: bytes, *, label: str, canonical: bool) -> dict[str, Any]:
    if len(raw) > MAX_RPC_BYTES:
        raise ReleaseRunnerError(f"{label} exceeds the {MAX_RPC_BYTES}-byte limit")
    try:
        value = json.loads(
            raw.decode("utf-8", errors="strict"), object_pairs_hook=_unique_pairs
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ReleaseRunnerError(f"{label} is not strict UTF-8 JSON") from exc
    normalised = _normalise_json(value)
    if not isinstance(normalised, dict):
        raise ReleaseRunnerError(f"{label} must be a JSON object")
    if canonical and raw != canonical_json_bytes(normalised):
        raise ReleaseRunnerError(f"{label} is not canonical JSON")
    return normalised


def _exact_mapping(value: Any, keys: set[str], *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys or len(value) != len(keys):
        raise ReleaseRunnerError(f"{label} does not have the exact schema")
    return value


def _require_sha(value: Any, *, label: str) -> str:
    result = str(value or "")
    if not _SHA256_RE.fullmatch(result):
        raise ReleaseRunnerError(f"{label} must be a lowercase SHA-256")
    return result


def _require_oid(value: Any, *, label: str) -> str:
    result = str(value or "")
    if not _OID_RE.fullmatch(result):
        raise ReleaseRunnerError(f"{label} must be a full lowercase Git object id")
    return result


def _require_schema_version(value: Any, *, label: str) -> int:
    if type(value) is not int or value != SCHEMA_VERSION:
        raise ReleaseRunnerError(f"{label} must be integer {SCHEMA_VERSION}")
    return value


def _require_release_tag(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not _RELEASE_TAG_RE.fullmatch(value):
        raise ReleaseRunnerError(f"{label} must be a canonical major.minor tag")
    return value


def _relative_path(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ReleaseRunnerError(f"{label} must be a canonical project path")
    normalised = unicodedata.normalize("NFC", value)
    path = PurePosixPath(normalised)
    if (
        path.is_absolute()
        or not _SAFE_PATH_RE.fullmatch(normalised)
        or any(part in {"", ".", ".."} for part in path.parts)
        or path.as_posix() != normalised
    ):
        raise ReleaseRunnerError(f"{label} must be a canonical project path")
    return normalised


def _git_show(project_root: Path, commit_oid: str, relative_path: str) -> bytes:
    try:
        completed = subprocess.run(
            ["git", "show", f"{commit_oid}:{relative_path}"],
            cwd=project_root,
            check=False,
            capture_output=True,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ReleaseRunnerError(
            f"cannot read published Git blob: {relative_path}"
        ) from exc
    if completed.returncode != 0 or completed.stderr:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise ReleaseRunnerError(
            f"published commit omits {relative_path}: {detail or completed.returncode}"
        )
    return bytes(completed.stdout)


def _require_git_commit(project_root: Path, commit_oid: str) -> None:
    try:
        completed = subprocess.run(
            ["git", "cat-file", "-t", commit_oid],
            cwd=project_root,
            check=False,
            capture_output=True,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ReleaseRunnerError("cannot inspect implementation commit") from exc
    if (
        completed.returncode != 0
        or completed.stderr
        or completed.stdout.strip() != b"commit"
    ):
        raise ReleaseRunnerError("implementation commit oid is not a Git commit")


def _git_object_oid(project_root: Path, revision: str, *, label: str) -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--verify", revision],
            cwd=project_root,
            check=False,
            capture_output=True,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ReleaseRunnerError(f"cannot resolve {label}") from exc
    oid = completed.stdout.decode("ascii", errors="replace").strip()
    if completed.returncode != 0 or completed.stderr or not _OID_RE.fullmatch(oid):
        raise ReleaseRunnerError(f"cannot resolve {label}")
    return oid


def _require_annotated_release_tag(
    project_root: Path,
    *,
    release_tag: str,
    release_tag_object_oid: str,
    implementation_commit_oid: str,
) -> None:
    tag = _require_release_tag(release_tag, label="implementation release tag")
    tag_object = _require_oid(
        release_tag_object_oid, label="implementation release tag object oid"
    )
    commit = _require_oid(
        implementation_commit_oid, label="implementation commit oid"
    )
    if _git_object_oid(
        project_root, f"refs/tags/{tag}", label="implementation release tag"
    ) != tag_object:
        raise ReleaseRunnerError("implementation release tag object oid differs")
    try:
        completed = subprocess.run(
            ["git", "cat-file", "-t", tag_object],
            cwd=project_root,
            check=False,
            capture_output=True,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ReleaseRunnerError("cannot inspect implementation release tag") from exc
    if (
        completed.returncode != 0
        or completed.stderr
        or completed.stdout.strip() != b"tag"
    ):
        raise ReleaseRunnerError("implementation release tag is not annotated")
    if _git_object_oid(
        project_root,
        f"refs/tags/{tag}^{{commit}}",
        label="implementation release tag commit",
    ) != commit:
        raise ReleaseRunnerError("implementation release tag peels to another commit")
    _require_git_commit(project_root, commit)


def _runtime_closure(manifest: Mapping[str, Any]) -> dict[str, Any]:
    closure = manifest.get("runtime_closure")
    required = {
        "schema_version",
        "python_version",
        "python_implementation",
        "python_runtime",
        "platform_system",
        "platform_machine",
        "platform_tag",
        "distributions",
        "files",
        "payload_sha256",
    }
    row = _exact_mapping(closure, required, label="runtime_closure")
    _require_schema_version(
        row["schema_version"], label="runtime closure schema_version"
    )
    payload = {key: row[key] for key in required - {"payload_sha256"}}
    payload_sha = _sha256(canonical_json_bytes(payload))
    if _require_sha(row["payload_sha256"], label="runtime closure payload hash") != payload_sha:
        raise ReleaseRunnerError("runtime closure payload SHA-256 differs")

    for name in (
        "python_version",
        "python_implementation",
        "python_runtime",
        "platform_system",
        "platform_machine",
        "platform_tag",
    ):
        if not isinstance(row[name], str):
            raise ReleaseRunnerError(f"runtime closure {name} binding is invalid")

    distributions = row["distributions"]
    if not isinstance(distributions, Mapping) or not distributions:
        raise ReleaseRunnerError("runtime closure distributions must be non-empty")
    declared_distributions: dict[str, str] = {}
    for name, version in distributions.items():
        if (
            not isinstance(name, str)
            or _canonical_distribution_name(name) != name
            or not isinstance(version, str)
            or not version
        ):
            raise ReleaseRunnerError("runtime closure distribution binding is invalid")
        declared_distributions[name] = version

    files = row["files"]
    if not isinstance(files, list) or not files:
        raise ReleaseRunnerError("runtime closure files must be non-empty")
    paths: list[str] = []
    for index, item in enumerate(files):
        checked = _exact_mapping(
            item, {"path", "sha256"}, label=f"runtime closure file {index}"
        )
        path = _relative_path(checked["path"], label=f"runtime closure file {index}.path")
        _require_sha(checked["sha256"], label=f"runtime closure file {index}.sha256")
        paths.append(path)
    if paths != sorted(paths) or len(paths) != len(set(paths)):
        raise ReleaseRunnerError("runtime closure files must be uniquely path-sorted")
    if RUNNER_MODULE_PATH not in paths:
        raise ReleaseRunnerError("runtime closure omits the capsule RPC module")
    result = dict(row)
    result["distributions"] = declared_distributions
    return result


def _published_contract(
    project_root: Path,
    *,
    manifest_path: str,
    manifest_sha256: str,
    implementation_release_tag: str,
    implementation_release_tag_object_oid: str,
    implementation_commit_oid: str,
) -> tuple[dict[str, Any], bytes, dict[str, Any], dict[str, bytes]]:
    relative_manifest = _relative_path(manifest_path, label="implementation manifest")
    wanted_manifest_sha = _require_sha(
        manifest_sha256, label="implementation manifest SHA-256"
    )
    commit = _require_oid(implementation_commit_oid, label="implementation commit oid")
    release_tag = _require_release_tag(
        implementation_release_tag, label="implementation release tag"
    )
    release_tag_object = _require_oid(
        implementation_release_tag_object_oid,
        label="implementation release tag object oid",
    )
    _require_annotated_release_tag(
        project_root,
        release_tag=release_tag,
        release_tag_object_oid=release_tag_object,
        implementation_commit_oid=commit,
    )
    manifest_raw = _git_show(project_root, commit, relative_manifest)
    if _sha256(manifest_raw) != wanted_manifest_sha:
        raise ReleaseRunnerError("published implementation manifest SHA-256 differs")
    manifest = _load_json_bytes(
        manifest_raw, label="published implementation manifest", canonical=False
    )
    release_runner = _exact_mapping(
        manifest.get("release_runner"),
        {
            "runner_id",
            "capsule_pattern",
            "source_origin",
            "process_isolation",
            "operations",
            "timeout_policy",
            "audit_missing_capsule_policy",
            "daily_replay_policy",
            "full_audit_policy",
        },
        label="implementation manifest release_runner",
    )
    if release_runner["runner_id"] != RUNNER_ID:
        raise ReleaseRunnerError("implementation manifest runner_id differs")
    declared_operations = release_runner["operations"]
    if declared_operations != list(OPERATIONS):
        raise ReleaseRunnerError("implementation manifest operations differ")
    if (
        release_runner["capsule_pattern"]
        != "runtime/prospective/5.0/release-runners/<implementation_commit_oid>"
        or release_runner["source_origin"]
        != "exact_git_blob_bytes_from_published_annotated_tag_commit"
        or release_runner["process_isolation"]
        != "python_-B_-s_with_python_env_reset_and_capsule_src_first"
        or release_runner["audit_missing_capsule_policy"]
        != "fail_without_materialization"
        or release_runner["daily_replay_policy"]
        != (
            "validate_structural_bundle_artifact_and_recursive_cas_bindings_"
            "then_replay_uncached_suffix"
        )
        or release_runner["full_audit_policy"]
        != "bypass_cache_replay_complete_history_and_refresh_current_head_prefix"
    ):
        raise ReleaseRunnerError("implementation manifest release runner policy differs")
    timeout_policy = _exact_mapping(
        release_runner["timeout_policy"],
        {
            "single_operation_seconds",
            "replay_history_base_seconds",
            "replay_history_per_operation_seconds",
            "replay_history_max_seconds",
        },
        label="implementation manifest timeout_policy",
    )
    if timeout_policy != {
        "single_operation_seconds": int(DEFAULT_TIMEOUT_SECONDS),
        "replay_history_base_seconds": int(DEFAULT_TIMEOUT_SECONDS),
        "replay_history_per_operation_seconds": int(
            REPLAY_HISTORY_TIMEOUT_PER_OPERATION_SECONDS
        ),
        "replay_history_max_seconds": int(MAX_REPLAY_HISTORY_TIMEOUT_SECONDS),
    }:
        raise ReleaseRunnerError("implementation manifest timeout policy differs")
    closure = _runtime_closure(manifest)
    blobs: dict[str, bytes] = {relative_manifest: manifest_raw}
    for item in closure["files"]:
        path = str(item["path"])
        raw = _git_show(project_root, commit, path)
        if _sha256(raw) != item["sha256"]:
            raise ReleaseRunnerError(f"published runtime closure hash differs: {path}")
        if path in blobs and blobs[path] != raw:
            raise ReleaseRunnerError(f"duplicate capsule artifact differs: {path}")
        blobs[path] = raw
    return manifest, manifest_raw, closure, blobs


def _receipt_payload(
    *,
    commit_oid: str,
    release_tag: str,
    release_tag_object_oid: str,
    manifest_path: str,
    manifest_sha256: str,
    closure: Mapping[str, Any],
    blobs: Mapping[str, bytes],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "prospective_release_source_capsule",
        "runner_id": RUNNER_ID,
        "implementation_release_tag": release_tag,
        "implementation_release_tag_object_oid": release_tag_object_oid,
        "implementation_commit_oid": commit_oid,
        "manifest_path": manifest_path,
        "manifest_sha256": manifest_sha256,
        "runtime_closure_payload_sha256": closure["payload_sha256"],
        "files": [
            {
                "path": path,
                "sha256": _sha256(raw),
                "size_bytes": len(raw),
            }
            for path, raw in sorted(blobs.items())
        ],
    }


def _write_file(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())


def _capsule_path(store_root: Path, commit_oid: str) -> Path:
    return store_root / commit_oid


def materialize_release_capsule(
    project_root: str | Path,
    capsule_store_root: str | Path,
    *,
    manifest_path: str | Path,
    manifest_sha256: str,
    implementation_release_tag: str,
    implementation_release_tag_object_oid: str,
    implementation_commit_oid: str,
) -> ReleaseCapsule:
    """Install one capsule exclusively from published Git blob bytes.

    This is a mutation API intended for implementation upgrade/admission.  It
    is deliberately separate from :func:`verify_release_capsule`, which never
    creates a missing directory.
    """

    project = Path(project_root).expanduser().resolve()
    store = Path(capsule_store_root).expanduser().resolve()
    relative_manifest = _relative_path(
        Path(manifest_path).as_posix(), label="implementation manifest"
    )
    wanted_manifest_sha = _require_sha(
        manifest_sha256, label="implementation manifest SHA-256"
    )
    commit = _require_oid(implementation_commit_oid, label="implementation commit oid")
    release_tag = _require_release_tag(
        implementation_release_tag, label="implementation release tag"
    )
    release_tag_object = _require_oid(
        implementation_release_tag_object_oid,
        label="implementation release tag object oid",
    )
    _manifest, _manifest_raw, closure, blobs = _published_contract(
        project,
        manifest_path=relative_manifest,
        manifest_sha256=wanted_manifest_sha,
        implementation_release_tag=release_tag,
        implementation_release_tag_object_oid=release_tag_object,
        implementation_commit_oid=commit,
    )
    target = _capsule_path(store, commit)
    if target.exists():
        return verify_release_capsule(
            project,
            store,
            manifest_path=relative_manifest,
            manifest_sha256=wanted_manifest_sha,
            implementation_release_tag=release_tag,
            implementation_release_tag_object_oid=release_tag_object,
            implementation_commit_oid=commit,
        )

    store.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".pending-capsule-", dir=store))
    try:
        for relative, raw in sorted(blobs.items()):
            _write_file(temporary / Path(relative), raw)
        receipt_payload = _receipt_payload(
            commit_oid=commit,
            release_tag=release_tag,
            release_tag_object_oid=release_tag_object,
            manifest_path=relative_manifest,
            manifest_sha256=wanted_manifest_sha,
            closure=closure,
            blobs=blobs,
        )
        receipt = {
            **receipt_payload,
            "capsule_sha256": _sha256(canonical_json_bytes(receipt_payload)),
        }
        _write_file(temporary / CAPSULE_RECEIPT_NAME, canonical_json_bytes(receipt))
        try:
            os.rename(temporary, target)
        except FileExistsError:
            pass
        except OSError:
            if not target.is_dir():
                raise
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
    return verify_release_capsule(
        project,
        store,
        manifest_path=relative_manifest,
        manifest_sha256=wanted_manifest_sha,
        implementation_release_tag=release_tag,
        implementation_release_tag_object_oid=release_tag_object,
        implementation_commit_oid=commit,
    )


def verify_release_capsule(
    project_root: str | Path,
    capsule_store_root: str | Path,
    *,
    manifest_path: str | Path,
    manifest_sha256: str,
    implementation_release_tag: str,
    implementation_release_tag_object_oid: str,
    implementation_commit_oid: str,
    require_running_environment: bool = True,
) -> ReleaseCapsule:
    """Verify an existing capsule without creating or repairing anything.

    ``require_running_environment=False`` is reserved for replaying the
    identity of a superseded, pre-decision implementation record.  It still
    verifies the annotated tag, published Git blobs, closure payload, receipt,
    and complete capsule tree, but does not require one interpreter to match
    every historical distribution set.  Any capsule that will execute remains
    subject to the default exact runtime check.
    """

    if type(require_running_environment) is not bool:
        raise ReleaseRunnerError(
            "require_running_environment must be a boolean"
        )

    project = Path(project_root).expanduser().resolve()
    store = Path(capsule_store_root).expanduser().resolve()
    relative_manifest = _relative_path(
        Path(manifest_path).as_posix(), label="implementation manifest"
    )
    wanted_manifest_sha = _require_sha(
        manifest_sha256, label="implementation manifest SHA-256"
    )
    commit = _require_oid(implementation_commit_oid, label="implementation commit oid")
    release_tag = _require_release_tag(
        implementation_release_tag, label="implementation release tag"
    )
    release_tag_object = _require_oid(
        implementation_release_tag_object_oid,
        label="implementation release tag object oid",
    )
    target = _capsule_path(store, commit)
    receipt_path = target / CAPSULE_RECEIPT_NAME
    if target.is_symlink() or not target.is_dir() or not receipt_path.is_file():
        raise ReleaseRunnerError("release capsule is not installed")

    _manifest, _manifest_raw, closure, blobs = _published_contract(
        project,
        manifest_path=relative_manifest,
        manifest_sha256=wanted_manifest_sha,
        implementation_release_tag=release_tag,
        implementation_release_tag_object_oid=release_tag_object,
        implementation_commit_oid=commit,
    )
    receipt = _load_json_bytes(
        receipt_path.read_bytes(), label="release capsule receipt", canonical=True
    )
    receipt_keys = {
        "schema_version",
        "kind",
        "runner_id",
        "implementation_release_tag",
        "implementation_release_tag_object_oid",
        "implementation_commit_oid",
        "manifest_path",
        "manifest_sha256",
        "runtime_closure_payload_sha256",
        "files",
        "capsule_sha256",
    }
    _exact_mapping(receipt, receipt_keys, label="release capsule receipt")
    payload = {key: receipt[key] for key in receipt_keys - {"capsule_sha256"}}
    if (
        type(receipt["schema_version"]) is not int
        or receipt["schema_version"] != SCHEMA_VERSION
        or receipt["kind"] != "prospective_release_source_capsule"
        or receipt["runner_id"] != RUNNER_ID
        or receipt["implementation_release_tag"] != release_tag
        or receipt["implementation_release_tag_object_oid"] != release_tag_object
        or receipt["implementation_commit_oid"] != commit
        or receipt["manifest_path"] != relative_manifest
        or receipt["manifest_sha256"] != wanted_manifest_sha
        or receipt["runtime_closure_payload_sha256"] != closure["payload_sha256"]
        or receipt["capsule_sha256"] != _sha256(canonical_json_bytes(payload))
    ):
        raise ReleaseRunnerError("release capsule receipt binding differs")
    expected_files = _receipt_payload(
        commit_oid=commit,
        release_tag=release_tag,
        release_tag_object_oid=release_tag_object,
        manifest_path=relative_manifest,
        manifest_sha256=wanted_manifest_sha,
        closure=closure,
        blobs=blobs,
    )["files"]
    if canonical_json_bytes(receipt["files"]) != canonical_json_bytes(expected_files):
        raise ReleaseRunnerError("release capsule receipt file list differs")
    expected_paths = {
        CAPSULE_RECEIPT_NAME,
        *(str(item["path"]) for item in expected_files),
    }
    actual_paths: set[str] = set()
    for path in target.rglob("*"):
        if path.is_symlink():
            raise ReleaseRunnerError("release capsule contains a symbolic link")
        if path.is_file():
            actual_paths.add(path.relative_to(target).as_posix())
        elif not path.is_dir():
            raise ReleaseRunnerError("release capsule contains an unsupported entry")
    if actual_paths != expected_paths:
        raise ReleaseRunnerError("release capsule file set differs")
    for item in expected_files:
        relative = str(item["path"])
        path = (target / Path(relative)).resolve()
        try:
            path.relative_to(target)
        except ValueError as exc:
            raise ReleaseRunnerError("release capsule artifact escapes its root") from exc
        if not path.is_file():
            raise ReleaseRunnerError(f"release capsule artifact differs: {relative}")
        raw = path.read_bytes()
        if (
            len(raw) != item["size_bytes"]
            or _sha256(raw) != item["sha256"]
            or raw != blobs[relative]
        ):
            raise ReleaseRunnerError(f"release capsule artifact differs: {relative}")
    if require_running_environment:
        if closure["python_version"] != platform.python_version():
            raise ReleaseRunnerError(
                "running Python differs from the release capsule"
            )
        platform_contract = {
            "python_implementation": platform.python_implementation(),
            "python_runtime": sys.version,
            "platform_system": platform.system(),
            "platform_machine": platform.machine(),
            "platform_tag": sysconfig.get_platform(),
        }
        for name, running in platform_contract.items():
            if closure[name] != running:
                raise ReleaseRunnerError(
                    f"running {name} differs from the release capsule"
                )
        if dict(closure["distributions"]) != _running_distribution_versions():
            raise ReleaseRunnerError(
                "running distribution set differs from the release capsule"
            )
    return ReleaseCapsule(
        project_root=project,
        store_root=store,
        root=target,
        source_root=target / "src",
        manifest_path=relative_manifest,
        manifest_sha256=wanted_manifest_sha,
        implementation_release_tag=release_tag,
        implementation_release_tag_object_oid=release_tag_object,
        implementation_commit_oid=commit,
        runtime_closure_payload_sha256=str(closure["payload_sha256"]),
    )


def _project_root(value: Any) -> Path:
    if not isinstance(value, str) or not Path(value).is_absolute():
        raise ReleaseRunnerError("project_root must be an absolute path")
    root = Path(value).resolve()
    if not root.is_dir():
        raise ReleaseRunnerError("project_root does not exist")
    return root


def _trade_deadline_utc(trade_date: Any) -> str:
    if not isinstance(trade_date, str):
        raise ReleaseRunnerError("source trade_date must be an ISO calendar date")
    try:
        session = date.fromisoformat(trade_date)
    except ValueError as exc:
        raise ReleaseRunnerError("source trade_date must be an ISO calendar date") from exc
    if session.isoformat() != trade_date:
        raise ReleaseRunnerError("source trade_date must be an ISO calendar date")
    deadline = datetime.combine(
        session,
        wall_time(hour=9, minute=15),
        tzinfo=ZoneInfo("Asia/Shanghai"),
    ).astimezone(timezone.utc)
    return deadline.strftime("%Y-%m-%dT%H:%M:%SZ")


def _replay_target(payload: Mapping[str, Any]) -> dict[str, Any]:
    row = _exact_mapping(
        payload,
        {
            "project_root",
            "source_data_snapshot_sha256",
            "deployment_bindings",
            "previous_state",
            "admission_deadline_utc",
        },
        label="replay_target payload",
    )
    bindings = _exact_mapping(
        row["deployment_bindings"],
        {
            "activation_record_sha256",
            "implementation_upgrade_record_sha256",
            "deployment_protocol_sha256",
        },
        label="deployment_bindings",
    )
    from factor_lab.data.prospective import (
        CANONICAL_CALENDAR_ANCHOR,
        CANONICAL_CALENDAR_COUNT,
        CANONICAL_CALENDAR_SHA256,
        FROZEN_BRIDGE_END,
        PROSPECTIVE_RELATIVE_ROOT,
        load_prospective_input_snapshot,
    )
    from factor_lab.prospective_targets import (
        DeploymentSpec,
        InputSnapshot,
        TenSleeveState,
        generate_fixed_core_targets,
    )

    root = _project_root(row["project_root"])
    source_sha = _require_sha(
        row["source_data_snapshot_sha256"], label="source_data_snapshot_sha256"
    )
    source = load_prospective_input_snapshot(
        root / PROSPECTIVE_RELATIVE_ROOT / "inputs" / source_sha
    )
    if source.snapshot_sha256 != source_sha:
        raise ReleaseRunnerError("loaded decision source differs from its requested hash")
    if source.manifest.get("protocol_release") != "5.0":
        raise ReleaseRunnerError("decision source protocol release differs")
    if source.target_adapter.get("calendar_next_trade_date") != source.trade_date:
        raise ReleaseRunnerError("decision source next-trade calendar binding differs")
    admission_deadline = _trade_deadline_utc(source.trade_date)
    submitted_deadline = row["admission_deadline_utc"]
    if submitted_deadline is not None and (
        not isinstance(submitted_deadline, str)
        or submitted_deadline != admission_deadline
    ):
        raise ReleaseRunnerError("submitted admission deadline differs from trade calendar")
    deployment = DeploymentSpec(
        calendar_anchor=CANONICAL_CALENDAR_ANCHOR,
        calendar_prefix_count=CANONICAL_CALENDAR_COUNT,
        calendar_prefix_last_session=FROZEN_BRIDGE_END.date().isoformat(),
        calendar_prefix_sha256=CANONICAL_CALENDAR_SHA256,
        activation_record_sha256=_require_sha(
            bindings["activation_record_sha256"], label="activation_record_sha256"
        ),
        implementation_upgrade_record_sha256=_require_sha(
            bindings["implementation_upgrade_record_sha256"],
            label="implementation_upgrade_record_sha256",
        ),
        deployment_protocol_sha256=_require_sha(
            bindings["deployment_protocol_sha256"],
            label="deployment_protocol_sha256",
        ),
    )
    if row["previous_state"] is None:
        previous = TenSleeveState.genesis(deployment)
    elif isinstance(row["previous_state"], Mapping):
        previous = TenSleeveState.from_mapping(row["previous_state"])
    else:
        raise ReleaseRunnerError("previous_state must be null or an exact state mapping")
    sessions = list(source.calendar_sessions)
    try:
        signal_index = sessions.index(source.signal_date)
    except ValueError as exc:
        raise ReleaseRunnerError("source signal date is absent from its calendar") from exc
    skipped = sessions[previous.last_processed_calendar_index + 1 : signal_index]
    signal_close = datetime.combine(
        datetime.fromisoformat(source.signal_date).date(),
        wall_time(hour=15),
        tzinfo=ZoneInfo("Asia/Shanghai"),
    ).astimezone(timezone.utc)
    input_snapshot = InputSnapshot(
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
        signal_close_utc=signal_close.strftime("%Y-%m-%dT%H:%M:%SZ"),
        admission_deadline_utc=admission_deadline,
    )
    generated = generate_fixed_core_targets(
        deployment=deployment,
        input_snapshot=input_snapshot,
        previous_state=previous,
    )
    return {
        "generation_result": generated.to_dict(),
        "deployment": deployment.to_dict(),
        "input_snapshot": input_snapshot.to_dict(),
    }


def _replay_outcome(payload: Mapping[str, Any]) -> dict[str, Any]:
    row = _exact_mapping(
        payload,
        {
            "project_root",
            "generation_result",
            "previous_account_state",
            "execution_snapshot_sha256",
        },
        label="replay_outcome payload",
    )
    from factor_lab.data.prospective import PROSPECTIVE_RELATIVE_ROOT
    from factor_lab.data.prospective_execution import (
        load_prospective_execution_snapshot,
    )
    from factor_lab.prospective_execution import (
        SleeveAccountState,
        evaluate_due_sleeve_cycle,
    )
    from factor_lab.prospective_targets import GenerationResult

    root = _project_root(row["project_root"])
    generation = GenerationResult.from_mapping(row["generation_result"])
    if row["previous_account_state"] is None:
        previous = SleeveAccountState.genesis(
            deployment_sha256=generation.deployment_sha256,
            offset=generation.due_offset,
        )
        loader_previous = None
    elif isinstance(row["previous_account_state"], Mapping):
        previous = SleeveAccountState.from_mapping(row["previous_account_state"])
        loader_previous = previous
    else:
        raise ReleaseRunnerError(
            "previous_account_state must be null or an exact state mapping"
        )
    execution_sha = _require_sha(
        row["execution_snapshot_sha256"], label="execution_snapshot_sha256"
    )
    loaded = load_prospective_execution_snapshot(
        root / PROSPECTIVE_RELATIVE_ROOT / "executions" / execution_sha,
        generation,
        previous_account_state=loader_previous,
    )
    if loaded.snapshot_sha256 != execution_sha:
        raise ReleaseRunnerError("loaded execution snapshot differs from requested hash")
    outcome = evaluate_due_sleeve_cycle(
        generation_result=generation,
        execution_snapshot=loaded.snapshot,
        previous_account_state=previous,
    )
    return {"cycle_outcome": outcome.to_dict()}


def _build_membership(payload: Mapping[str, Any]) -> dict[str, Any]:
    row = _exact_mapping(
        payload,
        {"project_root", "membership_month", "available_at_utc"},
        label="build_membership payload",
    )
    from factor_lab.data.prospective_membership import (
        build_prospective_membership_snapshot,
    )

    root = _project_root(row["project_root"])
    capsule_project = Path(__file__).resolve().parents[2]
    config_path = capsule_project / "configs/data.json"
    built = build_prospective_membership_snapshot(
        root,
        str(row["membership_month"]),
        available_at_utc=row["available_at_utc"],
        config_path=config_path,
    )
    return {
        "membership_month": built.membership_month,
        "as_of_date": built.as_of_date,
        "artifact_sha256": built.artifact_sha256,
        "directory": str(built.directory),
        "membership_path": str(built.membership_path),
        "manifest_path": str(built.manifest_path),
        "source_contract_path": str(built.source_contract_path),
        "reference_raw_path": str(built.reference_raw_path),
        "completed_at_utc": built.completed_at_utc,
    }


def _build_input(payload: Mapping[str, Any]) -> dict[str, Any]:
    row = _exact_mapping(
        payload,
        {
            "project_root",
            "signal_date",
            "available_at_utc",
            "membership_snapshot_path",
        },
        label="build_input payload",
    )
    from factor_lab.data.prospective import build_prospective_input_snapshot

    root = _project_root(row["project_root"])
    membership = row["membership_snapshot_path"]
    if membership is not None and not isinstance(membership, str):
        raise ReleaseRunnerError("membership_snapshot_path must be null or a string")
    built = build_prospective_input_snapshot(
        root,
        str(row["signal_date"]),
        available_at_utc=row["available_at_utc"],
        membership_snapshot_path=membership,
    )
    return {
        "signal_date": built.signal_date,
        "trade_date": built.trade_date,
        "source_data_snapshot_sha256": built.snapshot_sha256,
        "directory": str(built.directory),
        "manifest_path": str(built.manifest_path),
        "rows_path": str(built.rows_path),
        "build_receipt_path": str(built.build_receipt_path),
        "build_completed_at_utc": built.build_completed_at_utc,
        "inputs_available_at_utc": built.inputs_available_at_utc,
    }


def _build_execution(payload: Mapping[str, Any]) -> dict[str, Any]:
    row = _exact_mapping(
        payload,
        {
            "project_root",
            "generation_result",
            "source_data_snapshot_sha256",
            "previous_account_state",
            "available_at_utc",
        },
        label="build_execution payload",
    )
    from factor_lab.data.prospective_execution import (
        build_prospective_execution_snapshot,
    )
    from factor_lab.prospective_execution import SleeveAccountState
    from factor_lab.prospective_targets import GenerationResult

    root = _project_root(row["project_root"])
    generation = GenerationResult.from_mapping(row["generation_result"])
    previous_value = row["previous_account_state"]
    if previous_value is None:
        previous = None
    elif isinstance(previous_value, Mapping):
        previous = SleeveAccountState.from_mapping(previous_value)
    else:
        raise ReleaseRunnerError(
            "previous_account_state must be null or an exact state mapping"
        )
    built = build_prospective_execution_snapshot(
        root,
        generation,
        source_data_snapshot_sha256=_require_sha(
            row["source_data_snapshot_sha256"], label="source_data_snapshot_sha256"
        ),
        previous_account_state=previous,
        available_at_utc=row["available_at_utc"],
    )
    previous_account_state_sha256 = built.source_contract.get(
        "previous_account_state_sha256"
    )
    if previous_account_state_sha256 is not None:
        previous_account_state_sha256 = _require_sha(
            previous_account_state_sha256,
            label="built previous_account_state_sha256",
        )
    return {
        "execution_snapshot_sha256": built.snapshot_sha256,
        "execution_source_sha256": built.execution_source_sha256,
        "previous_account_state_sha256": previous_account_state_sha256,
        "directory": str(built.directory),
        "snapshot_path": str(built.snapshot_path),
        "sources_path": str(built.sources_path),
        "holding_start_date": built.snapshot.holding_start_date,
        "holding_end_date": built.snapshot.holding_end_date,
        "observation_available_at_utc": built.snapshot.observation_available_at_utc,
    }


def _evaluate(payload: Mapping[str, Any]) -> dict[str, Any]:
    row = _exact_mapping(
        payload,
        {
            "outcomes",
            "evaluator_id",
            "evaluation_contract_sha256",
            "ledger_id",
            "ledger_head_record_sha256",
            "implementation_upgrade_record_sha256",
        },
        label="evaluate payload",
    )
    outcomes = row["outcomes"]
    if (
        isinstance(outcomes, (str, bytes, bytearray))
        or not isinstance(outcomes, Sequence)
        or not all(isinstance(outcome, Mapping) for outcome in outcomes)
    ):
        raise ReleaseRunnerError("evaluate outcomes must be a sequence of objects")
    if not isinstance(row["ledger_id"], str) or not row["ledger_id"]:
        raise ReleaseRunnerError("evaluate ledger_id must be non-empty")
    ledger_head = _require_sha(
        row["ledger_head_record_sha256"], label="ledger_head_record_sha256"
    )
    implementation_upgrade = _require_sha(
        row["implementation_upgrade_record_sha256"],
        label="implementation_upgrade_record_sha256",
    )
    from factor_lab.prospective_evaluation import (
        EVALUATION_CONTRACT_SHA256,
        EVALUATOR_ID,
        evaluate_prospective_outcomes,
    )

    if row["evaluator_id"] != EVALUATOR_ID:
        raise ReleaseRunnerError("evaluate evaluator_id differs from released code")
    if row["evaluation_contract_sha256"] != EVALUATION_CONTRACT_SHA256:
        raise ReleaseRunnerError(
            "evaluate contract SHA-256 differs from released code"
        )
    normalized_outcomes = [dict(outcome) for outcome in outcomes]
    binding = {
        "evaluator_id": EVALUATOR_ID,
        "evaluation_contract_sha256": EVALUATION_CONTRACT_SHA256,
        "ledger_id": row["ledger_id"],
        "ledger_head_record_sha256": ledger_head,
        "implementation_upgrade_record_sha256": implementation_upgrade,
        "outcome_count": len(normalized_outcomes),
        "outcomes_sha256": _sha256(canonical_json_bytes(normalized_outcomes)),
    }
    envelope = {
        "schema_version": SCHEMA_VERSION,
        "binding": binding,
        "evaluation": evaluate_prospective_outcomes(normalized_outcomes),
    }
    return {
        "evaluation_envelope": {
            **envelope,
            "evaluation_envelope_sha256": _sha256(canonical_json_bytes(envelope)),
        }
    }


def _replay_history(payload: Mapping[str, Any]) -> dict[str, Any]:
    row = _exact_mapping(payload, {"operations"}, label="replay_history payload")
    operations = row["operations"]
    if (
        isinstance(operations, (str, bytes, bytearray))
        or not isinstance(operations, Sequence)
    ):
        raise ReleaseRunnerError("replay_history operations must be a sequence")
    dispatch = {
        "replay_target": _replay_target,
        "replay_outcome": _replay_outcome,
        "evaluate": _evaluate,
    }
    results: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, item in enumerate(operations):
        checked = _exact_mapping(
            item,
            {"operation_id", "operation", "payload"},
            label=f"replay_history operation {index}",
        )
        operation_id = _require_sha(
            checked["operation_id"],
            label=f"replay_history operation {index}.operation_id",
        )
        operation = checked["operation"]
        nested_payload = checked["payload"]
        if operation not in dispatch or not isinstance(nested_payload, Mapping):
            raise ReleaseRunnerError(
                f"replay_history operation {index} is not an allowlisted replay"
            )
        expected_id = _sha256(
            canonical_json_bytes(
                {
                    "operation": operation,
                    "payload": dict(nested_payload),
                }
            )
        )
        if operation_id != expected_id or operation_id in seen:
            raise ReleaseRunnerError(
                f"replay_history operation {index} id differs or is duplicated"
            )
        seen.add(operation_id)
        results.append(
            {
                "operation_id": operation_id,
                "operation": operation,
                "result": dispatch[str(operation)](nested_payload),
            }
        )
    return {"results": results}


_DISPATCH = {
    "build_membership": _build_membership,
    "build_input": _build_input,
    "replay_target": _replay_target,
    "build_execution": _build_execution,
    "replay_outcome": _replay_outcome,
    "evaluate": _evaluate,
    "replay_history": _replay_history,
}


def _request(operation: str, payload: Mapping[str, Any]) -> tuple[dict[str, Any], bytes]:
    if operation not in OPERATIONS:
        raise ReleaseRunnerError(f"unsupported release operation: {operation}")
    base = {
        "schema_version": SCHEMA_VERSION,
        "runner_id": RUNNER_ID,
        "operation": operation,
        "payload": dict(payload),
    }
    request = {**base, "request_sha256": _sha256(canonical_json_bytes(base))}
    return request, canonical_json_bytes(request)


def _validate_request(value: Mapping[str, Any]) -> tuple[str, Mapping[str, Any], str]:
    row = _exact_mapping(
        value,
        {"schema_version", "runner_id", "operation", "payload", "request_sha256"},
        label="release RPC request",
    )
    operation = str(row["operation"])
    if (
        type(row["schema_version"]) is not int
        or row["schema_version"] != SCHEMA_VERSION
        or row["runner_id"] != RUNNER_ID
        or operation not in OPERATIONS
        or not isinstance(row["payload"], Mapping)
    ):
        raise ReleaseRunnerError("release RPC request binding differs")
    base = {key: row[key] for key in row if key != "request_sha256"}
    wanted = _require_sha(row["request_sha256"], label="request_sha256")
    if wanted != _sha256(canonical_json_bytes(base)):
        raise ReleaseRunnerError("release RPC request SHA-256 differs")
    return operation, row["payload"], wanted


def _response(operation: str, request_sha256: str, result: Mapping[str, Any]) -> bytes:
    base = {
        "schema_version": SCHEMA_VERSION,
        "runner_id": RUNNER_ID,
        "operation": operation,
        "request_sha256": request_sha256,
        "result": dict(result),
    }
    return canonical_json_bytes(
        {**base, "response_sha256": _sha256(canonical_json_bytes(base))}
    )


def _validate_response(
    value: Mapping[str, Any], *, operation: str, request_sha256: str
) -> dict[str, Any]:
    row = _exact_mapping(
        value,
        {
            "schema_version",
            "runner_id",
            "operation",
            "request_sha256",
            "result",
            "response_sha256",
        },
        label="release RPC response",
    )
    if (
        type(row["schema_version"]) is not int
        or row["schema_version"] != SCHEMA_VERSION
        or row["runner_id"] != RUNNER_ID
        or row["operation"] != operation
        or row["request_sha256"] != request_sha256
        or not isinstance(row["result"], Mapping)
    ):
        raise ReleaseRunnerError("release RPC response binding differs")
    base = {key: row[key] for key in row if key != "response_sha256"}
    if _require_sha(row["response_sha256"], label="response_sha256") != _sha256(
        canonical_json_bytes(base)
    ):
        raise ReleaseRunnerError("release RPC response SHA-256 differs")
    return dict(row["result"])


def run_release_operation(
    capsule: ReleaseCapsule,
    operation: str,
    payload: Mapping[str, Any],
    *,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    """Run one allowlisted operation in the capsule's isolated interpreter."""

    if not isinstance(capsule, ReleaseCapsule):
        raise ReleaseRunnerError("capsule must be a verified ReleaseCapsule")
    verified = verify_release_capsule(
        capsule.project_root,
        capsule.store_root,
        manifest_path=capsule.manifest_path,
        manifest_sha256=capsule.manifest_sha256,
        implementation_release_tag=capsule.implementation_release_tag,
        implementation_release_tag_object_oid=(
            capsule.implementation_release_tag_object_oid
        ),
        implementation_commit_oid=capsule.implementation_commit_oid,
    )
    request, request_raw = _request(operation, payload)
    if timeout_seconds is None:
        timeout = DEFAULT_TIMEOUT_SECONDS
        if operation == "replay_history":
            operations = payload.get("operations")
            operation_count = len(operations) if isinstance(operations, list) else 0
            timeout = min(
                MAX_REPLAY_HISTORY_TIMEOUT_SECONDS,
                DEFAULT_TIMEOUT_SECONDS
                + REPLAY_HISTORY_TIMEOUT_PER_OPERATION_SECONDS * operation_count,
            )
    elif isinstance(timeout_seconds, bool) or not isinstance(
        timeout_seconds, (int, float)
    ) or timeout_seconds <= 0:
        raise ReleaseRunnerError("timeout_seconds must be a positive number")
    else:
        timeout = float(timeout_seconds)
    bootstrap = (
        "import runpy,sys;"
        "sys.path.insert(0,sys.argv[1]);"
        "sys.argv=['factor_lab.prospective_release_runner','--capsule-rpc'];"
        "runpy.run_module('factor_lab.prospective_release_runner',run_name='__main__')"
    )
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.upper().startswith("PYTHON")
    }
    environment["PYTHONHASHSEED"] = "0"
    environment["PYTHONNOUSERSITE"] = "1"
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTHONIOENCODING"] = "utf-8"
    environment["PYTHONUTF8"] = "1"
    try:
        completed = subprocess.run(
            [sys.executable, "-B", "-s", "-c", bootstrap, str(verified.source_root)],
            cwd=verified.root,
            env=environment,
            input=request_raw,
            capture_output=True,
            check=False,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ReleaseRunnerError(
            f"isolated release operation failed to start or timed out: {operation}"
        ) from exc
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise ReleaseRunnerError(
            f"isolated release operation failed ({operation}): "
            f"{detail or completed.returncode}"
        )
    if completed.stderr:
        raise ReleaseRunnerError(
            f"isolated release operation emitted stderr: {operation}"
        )
    response = _load_json_bytes(
        bytes(completed.stdout), label="release RPC response", canonical=True
    )
    return _validate_response(
        response,
        operation=operation,
        request_sha256=str(request["request_sha256"]),
    )


def _capsule_rpc_main() -> int:
    try:
        raw = sys.stdin.buffer.read(MAX_RPC_BYTES + 1)
        request = _load_json_bytes(raw, label="release RPC request", canonical=True)
        operation, payload, request_sha = _validate_request(request)
        result = _DISPATCH[operation](payload)
        sys.stdout.buffer.write(_response(operation, request_sha, result))
        sys.stdout.buffer.flush()
        return 0
    except Exception as exc:  # boundary: never leak a partial success response
        detail = " ".join(str(exc).splitlines())[:2000]
        sys.stderr.write(f"{type(exc).__name__}: {detail}\n")
        sys.stderr.flush()
        return 2


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments != ["--capsule-rpc"]:
        raise ReleaseRunnerError("this module only exposes the --capsule-rpc entrypoint")
    return _capsule_rpc_main()


if __name__ == "__main__":  # pragma: no cover - exercised through subprocess
    raise SystemExit(main())


__all__ = [
    "CAPSULE_RECEIPT_NAME",
    "DEFAULT_TIMEOUT_SECONDS",
    "OPERATIONS",
    "RUNNER_ID",
    "ReleaseCapsule",
    "ReleaseRunnerError",
    "canonical_json_bytes",
    "materialize_release_capsule",
    "run_release_operation",
    "verify_release_capsule",
]
