"""Build and verify immutable provenance for source-only container images.

The production Dagster image intentionally does not contain ``git`` or the
repository's ``.git`` directory.  This module gives that image an equally
strict provenance path: image construction inventories every source and
configuration file plus ``uv.lock``; process startup then re-inventories the
files before accepting the recorded hashes.

The file is deliberately standard-library-only so Docker can execute it before
the project and its dependencies are installed.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
import platform as platform_module
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys
from typing import Any, Mapping


SOURCE_BUNDLE_SCHEMA_VERSION = "factor-lab/source-bundle-provenance/v2"
SOURCE_BUNDLE_MANIFEST_ENV = "FACTOR_LAB_SOURCE_BUNDLE_MANIFEST"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_OCI_IMAGE_REFERENCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,511}$")


class SourceBundleProvenanceError(RuntimeError):
    """Raised when build inputs cannot be proven exactly."""


@dataclass(frozen=True)
class SourceBundleProvenance:
    manifest_path: Path
    bundle_root: Path
    source_root: Path
    configuration_root: Path
    dependency_lock: Path
    runtime_root: Path | None
    source_tree_hash: str
    configuration_tree_hash: str
    runtime_tree_hash: str | None
    dependency_lock_hash: str
    manifest_hash: str
    source_file_count: int
    configuration_file_count: int
    runtime_file_count: int
    configuration_files: tuple[str, ...]


@dataclass(frozen=True)
class SourceBundleEnvironmentCapture:
    environment: Any
    provenance: SourceBundleProvenance


@dataclass(frozen=True)
class EpochBuildProvenance:
    """Automatically measured fields persisted by the unique evidence epoch.

    ``dirty_patch_hash`` binds the actual Git worktree in checkout mode.  In a
    source-only production image, where Git metadata is intentionally absent,
    it binds the reverified immutable source-bundle manifest instead.
    """

    architecture_version: str
    code_hash: str
    configuration_hash: str
    dependency_lock_hash: str
    dirty_patch_hash: str
    provenance_kind: str
    build_identity_hash: str
    git_commit: str | None = None
    image_source_digest: str | None = None
    oci_image_id: str | None = None
    oci_repo_digests: tuple[str, ...] = ()
    oci_base_digests: tuple[str, ...] = ()

    @property
    def formal_epoch_eligible(self) -> bool:
        # This deployment is intentionally local Docker Compose rather than a
        # registry/Kubernetes release.  A locally built image often has no
        # RepoDigest, but its daemon content ID plus immutable source-bundle
        # manifest and pinned base-image digest still provide the exact
        # content-addressed identity required for an epoch.
        return (
            self.oci_image_id is not None
            and self.image_source_digest is not None
            and bool(self.oci_base_digests)
        )

    def epoch_fields(self) -> dict[str, str]:
        return {
            "architecture_version": self.architecture_version,
            "code_hash": self.code_hash,
            "configuration_hash": self.configuration_hash,
            "dependency_lock_hash": self.dependency_lock_hash,
            "dirty_patch_hash": self.dirty_patch_hash,
        }

    def public_dict(self) -> dict[str, Any]:
        return {
            **self.epoch_fields(),
            "provenance_kind": self.provenance_kind,
            "build_identity_hash": self.build_identity_hash,
            "git_commit": self.git_commit,
            "image_source_digest": self.image_source_digest,
            "oci_image_id": self.oci_image_id,
            "oci_repo_digests": list(self.oci_repo_digests),
            "oci_base_digests": list(self.oci_base_digests),
            "formal_epoch_eligible": self.formal_epoch_eligible,
        }


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _domain_hash(domain: str, value: Any) -> str:
    return hashlib.sha256(
        domain.encode("utf-8") + b"\0" + _canonical_bytes(value)
    ).hexdigest()


def _sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_relative(value: Any, *, field: str) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value or "\0" in value:
        raise SourceBundleProvenanceError(f"{field} must be a safe POSIX relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise SourceBundleProvenanceError(f"{field} must be a safe POSIX relative path")
    if ":" in path.parts[0]:
        raise SourceBundleProvenanceError(f"{field} must not contain a drive prefix")
    return path


def _resolve_relative(bundle_root: Path, value: Any, *, field: str) -> Path:
    relative = _safe_relative(value, field=field)
    path = Path(os.path.abspath(bundle_root / Path(*relative.parts)))
    try:
        path.relative_to(bundle_root)
    except ValueError as exc:
        raise SourceBundleProvenanceError(f"{field} escapes the source bundle") from exc
    current = bundle_root
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise SourceBundleProvenanceError(f"{field} traverses a symlink: {current}")
    return path


def _relative_to_bundle(bundle_root: Path, path: str | Path, *, field: str) -> str:
    supplied = Path(path)
    candidate = supplied if supplied.is_absolute() else bundle_root / supplied
    resolved = Path(os.path.abspath(candidate))
    try:
        relative = resolved.relative_to(bundle_root).as_posix()
    except ValueError as exc:
        raise SourceBundleProvenanceError(f"{field} is outside the source bundle") from exc
    _safe_relative(relative, field=field)
    _resolve_relative(bundle_root, relative, field=field)
    return relative


def _inventory_tree(root: Path, *, label: str) -> list[dict[str, Any]]:
    if not root.is_dir() or root.is_symlink():
        raise SourceBundleProvenanceError(f"{label} root is missing or is a symlink: {root}")
    entries: list[dict[str, Any]] = []

    def walk_failed(error: OSError) -> None:
        raise SourceBundleProvenanceError(
            f"cannot inventory {label} tree: {error}"
        ) from error

    for directory, dirnames, filenames in os.walk(
        root, followlinks=False, onerror=walk_failed
    ):
        directory_path = Path(directory)
        for name in sorted(dirnames):
            child = directory_path / name
            if child.is_symlink():
                raise SourceBundleProvenanceError(
                    f"{label} contains a directory symlink: {child}"
                )
        for name in sorted(filenames):
            child = directory_path / name
            metadata = child.lstat()
            if child.is_symlink() or not stat.S_ISREG(metadata.st_mode):
                raise SourceBundleProvenanceError(
                    f"{label} contains a non-regular file: {child}"
                )
            entries.append(
                {
                    "path": child.relative_to(root).as_posix(),
                    "size_bytes": metadata.st_size,
                    "sha256": _sha256_file(child),
                }
            )
    entries.sort(key=lambda entry: entry["path"])
    if not entries:
        raise SourceBundleProvenanceError(f"{label} root contains no files: {root}")
    return entries


def _tree_record(bundle_root: Path, root_value: Any, *, label: str) -> dict[str, Any]:
    root = _resolve_relative(bundle_root, root_value, field=f"{label}.root")
    files = _inventory_tree(root, label=label)
    tree_hash = _domain_hash(
        f"{SOURCE_BUNDLE_SCHEMA_VERSION}/{label}-tree",
        files,
    )
    return {"root": str(root_value), "tree_hash": tree_hash, "files": files}


def build_source_bundle_manifest(
    *,
    bundle_root: str | Path,
    source_root: str | Path = "src",
    configuration_root: str | Path = "configs",
    dependency_lock: str | Path = "uv.lock",
    runtime_root: str | Path | None = None,
) -> dict[str, Any]:
    """Inventory the exact inputs copied into a source-only image."""

    root = Path(bundle_root).resolve()
    if not root.is_dir():
        raise SourceBundleProvenanceError(f"source bundle root does not exist: {root}")
    source_relative = _relative_to_bundle(root, source_root, field="source.root")
    config_relative = _relative_to_bundle(
        root, configuration_root, field="configuration.root"
    )
    lock_relative = _relative_to_bundle(root, dependency_lock, field="dependency_lock.path")
    source = _tree_record(root, source_relative, label="source")
    configuration = _tree_record(root, config_relative, label="configuration")
    lock_path = _resolve_relative(root, lock_relative, field="dependency_lock.path")
    if not lock_path.is_file() or lock_path.is_symlink():
        raise SourceBundleProvenanceError(
            f"dependency lock is missing or is a symlink: {lock_path}"
        )
    dependency = {
        "path": lock_relative,
        "size_bytes": lock_path.stat().st_size,
        "sha256": _sha256_file(lock_path),
    }
    payload: dict[str, Any] = {
        "schema_version": SOURCE_BUNDLE_SCHEMA_VERSION,
        "hash_algorithm": "sha256",
        "source": source,
        "configuration": configuration,
        "dependency_lock": dependency,
        "runtime": (
            None
            if runtime_root is None
            else _tree_record(
                root,
                _relative_to_bundle(root, runtime_root, field="runtime.root"),
                label="runtime",
            )
        ),
    }
    payload["manifest_hash"] = _domain_hash(
        f"{SOURCE_BUNDLE_SCHEMA_VERSION}/manifest", payload
    )
    return payload


def write_source_bundle_manifest(
    output: str | Path,
    *,
    bundle_root: str | Path,
    source_root: str | Path = "src",
    configuration_root: str | Path = "configs",
    dependency_lock: str | Path = "uv.lock",
    runtime_root: str | Path | None = None,
) -> Path:
    """Write the build manifest once, allowing only byte-identical retries."""

    root = Path(bundle_root).resolve()
    output_path = Path(output)
    output_path = (
        output_path.resolve()
        if output_path.is_absolute()
        else (root / output_path).resolve()
    )
    manifest = build_source_bundle_manifest(
        bundle_root=root,
        source_root=source_root,
        configuration_root=configuration_root,
        dependency_lock=dependency_lock,
        runtime_root=runtime_root,
    )
    encoded = _canonical_bytes(manifest) + b"\n"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(output_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444)
    except FileExistsError:
        if output_path.read_bytes() != encoded:
            raise SourceBundleProvenanceError(
                f"source bundle manifest already exists with different content: {output_path}"
            )
        return output_path
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    return output_path


def _require_mapping(value: Any, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SourceBundleProvenanceError(f"{field} must be an object")
    return value


def verify_source_bundle_manifest(
    manifest_path: str | Path,
    *,
    bundle_root: str | Path | None = None,
) -> SourceBundleProvenance:
    """Re-hash every declared build input and reject additions or mutations."""

    supplied_path = Path(manifest_path)
    if supplied_path.is_symlink():
        raise SourceBundleProvenanceError(
            f"source bundle manifest is missing or is a symlink: {supplied_path}"
        )
    path = supplied_path.resolve()
    root = Path(bundle_root).resolve() if bundle_root is not None else path.parent.resolve()
    if not path.is_file():
        raise SourceBundleProvenanceError(
            f"source bundle manifest is missing or is a symlink: {path}"
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SourceBundleProvenanceError(f"source bundle manifest is unreadable: {path}") from exc
    document = _require_mapping(payload, field="manifest")
    if set(document) != {
        "schema_version",
        "hash_algorithm",
        "source",
        "configuration",
        "dependency_lock",
        "runtime",
        "manifest_hash",
    }:
        raise SourceBundleProvenanceError("source bundle manifest fields are not canonical")
    if document.get("schema_version") != SOURCE_BUNDLE_SCHEMA_VERSION:
        raise SourceBundleProvenanceError("unsupported source bundle manifest schema")
    if document.get("hash_algorithm") != "sha256":
        raise SourceBundleProvenanceError("source bundle manifest must use sha256")
    expected_manifest_hash = str(document.get("manifest_hash") or "")
    unsigned = dict(document)
    unsigned.pop("manifest_hash", None)
    actual_manifest_hash = _domain_hash(
        f"{SOURCE_BUNDLE_SCHEMA_VERSION}/manifest", unsigned
    )
    if (
        not _SHA256.fullmatch(expected_manifest_hash)
        or expected_manifest_hash != actual_manifest_hash
    ):
        raise SourceBundleProvenanceError("source bundle manifest hash mismatch")

    source_declared = _require_mapping(document.get("source"), field="source")
    config_declared = _require_mapping(document.get("configuration"), field="configuration")
    dependency_declared = _require_mapping(
        document.get("dependency_lock"), field="dependency_lock"
    )
    runtime_value = document.get("runtime")
    runtime_declared = (
        None
        if runtime_value is None
        else _require_mapping(runtime_value, field="runtime")
    )
    for label, declared in (("source", source_declared), ("configuration", config_declared)):
        if set(declared) != {"root", "tree_hash", "files"}:
            raise SourceBundleProvenanceError(f"{label} tree fields are not canonical")
        actual = _tree_record(root, declared.get("root"), label=label)
        if actual != dict(declared):
            raise SourceBundleProvenanceError(f"{label} tree does not match build provenance")
    if runtime_declared is not None:
        if set(runtime_declared) != {"root", "tree_hash", "files"}:
            raise SourceBundleProvenanceError("runtime tree fields are not canonical")
        actual_runtime = _tree_record(
            root, runtime_declared.get("root"), label="runtime"
        )
        if actual_runtime != dict(runtime_declared):
            raise SourceBundleProvenanceError(
                "runtime tree does not match build provenance"
            )
    if set(dependency_declared) != {"path", "size_bytes", "sha256"}:
        raise SourceBundleProvenanceError("dependency lock fields are not canonical")
    lock_path = _resolve_relative(
        root, dependency_declared.get("path"), field="dependency_lock.path"
    )
    if not lock_path.is_file() or lock_path.is_symlink():
        raise SourceBundleProvenanceError("dependency lock is missing or is a symlink")
    actual_dependency = {
        "path": dependency_declared.get("path"),
        "size_bytes": lock_path.stat().st_size,
        "sha256": _sha256_file(lock_path),
    }
    if actual_dependency != dict(dependency_declared):
        raise SourceBundleProvenanceError("dependency lock does not match build provenance")

    return SourceBundleProvenance(
        manifest_path=path,
        bundle_root=root,
        source_root=_resolve_relative(root, source_declared["root"], field="source.root"),
        configuration_root=_resolve_relative(
            root, config_declared["root"], field="configuration.root"
        ),
        dependency_lock=lock_path,
        runtime_root=(
            None
            if runtime_declared is None
            else _resolve_relative(root, runtime_declared["root"], field="runtime.root")
        ),
        source_tree_hash=str(source_declared["tree_hash"]),
        configuration_tree_hash=str(config_declared["tree_hash"]),
        runtime_tree_hash=(
            None if runtime_declared is None else str(runtime_declared["tree_hash"])
        ),
        dependency_lock_hash=str(dependency_declared["sha256"]),
        manifest_hash=expected_manifest_hash,
        source_file_count=len(source_declared["files"]),
        configuration_file_count=len(config_declared["files"]),
        runtime_file_count=(
            0 if runtime_declared is None else len(runtime_declared["files"])
        ),
        configuration_files=tuple(
            str(entry["path"]) for entry in config_declared["files"]
        ),
    )


def capture_source_bundle_environment(
    manifest_path: str | Path,
    *,
    bundle_root: str | Path,
    dependency_lock: str | Path,
    configuration_path: str | Path,
    evaluator_build: str,
) -> SourceBundleEnvironmentCapture:
    """Capture an EnvironmentRef plus proof after exact bundle verification."""

    provenance = verify_source_bundle_manifest(manifest_path, bundle_root=bundle_root)
    configured_lock = Path(dependency_lock).resolve()
    if configured_lock != provenance.dependency_lock:
        raise SourceBundleProvenanceError(
            "configured dependency lock is not the dependency lock recorded at build time"
        )
    config_path = Path(configuration_path).resolve()
    try:
        config_relative = config_path.relative_to(provenance.configuration_root).as_posix()
    except ValueError as exc:
        raise SourceBundleProvenanceError(
            "orchestration configuration is outside the verified configuration bundle"
        ) from exc
    if not config_path.is_file() or config_path.is_symlink():
        raise SourceBundleProvenanceError(
            "orchestration configuration is missing or is a symlink"
        )
    # Verification above guarantees there are no unrecorded configuration
    # files.  Requiring the selected file to appear makes the binding explicit.
    if config_relative not in provenance.configuration_files:
        raise SourceBundleProvenanceError(
            "orchestration configuration is not recorded in build provenance"
        )

    # Delayed import keeps the build-time command free of project dependencies.
    from .contracts import EnvironmentRef

    return SourceBundleEnvironmentCapture(
        environment=EnvironmentRef(
            code_hash=provenance.source_tree_hash,
            dependency_lock_hash=provenance.dependency_lock_hash,
            configuration_hash=provenance.configuration_tree_hash,
            dirty_patch_hash=None,
            python_version=sys.version.split()[0],
            platform=f"{platform_module.system()}-{platform_module.machine()}",
            evaluator_build=evaluator_build,
        ),
        provenance=provenance,
    )


def _load_architecture_version(configuration_path: Path) -> str:
    try:
        document = json.loads(configuration_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SourceBundleProvenanceError(
            "orchestration configuration is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(document, Mapping):
        raise SourceBundleProvenanceError("orchestration configuration must be an object")
    schema = str(document.get("schema_version") or "").strip()
    if not schema:
        raise SourceBundleProvenanceError(
            "orchestration configuration has no schema_version"
        )
    return schema


def _git_output(repository: Path, *arguments: str) -> bytes:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repository), *arguments],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise SourceBundleProvenanceError(
            f"cannot capture Git provenance for {repository}"
        ) from exc
    return completed.stdout


def _capture_git_worktree(repository: Path) -> tuple[str, str]:
    top = Path(
        _git_output(repository, "rev-parse", "--show-toplevel")
        .decode("utf-8")
        .strip()
    ).resolve()
    if top != repository.resolve():
        raise SourceBundleProvenanceError(
            "repository must be the Git worktree root for epoch capture"
        )
    commit = _git_output(repository, "rev-parse", "HEAD").decode("ascii").strip()
    if not re.fullmatch(r"[0-9a-f]{40,64}", commit):
        raise SourceBundleProvenanceError("Git HEAD is not a commit hash")
    status = _git_output(
        repository, "status", "--porcelain=v1", "-z", "--untracked-files=all"
    )
    if not status:
        return commit, hashlib.sha256(b"").hexdigest()
    diff = _git_output(
        repository,
        "diff",
        "--binary",
        "--no-ext-diff",
        "--no-textconv",
        "HEAD",
        "--",
    )
    # ``git diff HEAD`` has no payload for untracked files.  Binding only the
    # porcelain status would therefore let an operator change the contents of
    # an untracked runtime/configuration file without changing the epoch
    # provenance.  Inventory every untracked regular file explicitly.
    untracked_output = _git_output(
        repository, "ls-files", "--others", "--exclude-standard", "-z"
    )
    untracked: list[dict[str, Any]] = []
    for raw_name in untracked_output.split(b"\0"):
        if not raw_name:
            continue
        name = os.fsdecode(raw_name)
        relative = _safe_relative(name.replace(os.sep, "/"), field="untracked.path")
        candidate = Path(
            os.path.abspath(repository.joinpath(*relative.parts))
        )
        try:
            candidate.relative_to(repository.resolve())
        except ValueError as exc:
            raise SourceBundleProvenanceError(
                "untracked file escapes the Git worktree"
            ) from exc
        if candidate.is_symlink() or not candidate.is_file():
            raise SourceBundleProvenanceError(
                f"untracked path is not a regular file: {name}"
            )
        untracked.append(
            {
                "path": relative.as_posix(),
                "size_bytes": candidate.stat().st_size,
                "sha256": _sha256_file(candidate),
            }
        )
    untracked.sort(key=lambda item: item["path"])
    dirty_hash = hashlib.sha256(
        b"factor-lab/research-os/git-dirty/v2\0"
        + status
        + b"\0"
        + diff
        + b"\0"
        + _canonical_bytes(untracked)
    ).hexdigest()
    return commit, dirty_hash


def _inspect_oci_image(
    image_reference: str,
) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    """Return the content-addressed local image ID from Docker inspection.

    The operator supplies only an image *reference*. The persisted identity is
    measured from the daemon and cannot be replaced by a caller-provided hash.
    """

    reference = str(image_reference or "").strip()
    if not _OCI_IMAGE_REFERENCE.fullmatch(reference):
        raise SourceBundleProvenanceError("OCI image reference is unsafe or empty")
    try:
        completed = subprocess.run(
            ["docker", "image", "inspect", reference],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        payload = json.loads(completed.stdout)
    except (OSError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        raise SourceBundleProvenanceError(
            f"cannot inspect OCI image reference {reference!r}"
        ) from exc
    if not isinstance(payload, list) or len(payload) != 1 or not isinstance(
        payload[0], Mapping
    ):
        raise SourceBundleProvenanceError("Docker returned a non-canonical image record")
    image_id = str(payload[0].get("Id") or "")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
        raise SourceBundleProvenanceError("Docker image has no content-addressed ID")
    raw_digests = payload[0].get("RepoDigests") or []
    if not isinstance(raw_digests, list):
        raise SourceBundleProvenanceError("Docker RepoDigests is not a list")
    repo_digests = tuple(sorted({str(value) for value in raw_digests if value}))
    if any(not re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", value) for value in repo_digests):
        raise SourceBundleProvenanceError("Docker image contains an invalid repo digest")
    config = payload[0].get("Config") or {}
    if not isinstance(config, Mapping):
        raise SourceBundleProvenanceError("Docker image Config is not an object")
    labels = config.get("Labels") or {}
    if not isinstance(labels, Mapping):
        raise SourceBundleProvenanceError("Docker image Labels is not an object")
    raw_base_digest = str(
        labels.get("org.opencontainers.image.base.digest") or ""
    ).strip()
    base_digests = tuple(
        sorted({part.strip() for part in raw_base_digest.split(",") if part.strip()})
    )
    if not base_digests or any(
        not re.fullmatch(r"sha256:[0-9a-f]{64}", value)
        for value in base_digests
    ):
        raise SourceBundleProvenanceError(
            "Docker image lacks a valid inspected immutable base-image digest label"
        )
    return image_id, repo_digests, base_digests


def capture_epoch_provenance(
    *,
    configuration_path: str | Path,
    repository: str | Path,
    manifest_path: str | Path | None = None,
    image_reference: str | None = None,
) -> EpochBuildProvenance:
    """Measure epoch provenance; callers cannot supply any persisted hash.

    Production images use the build manifest generated after dependency
    installation and re-hash every included source/configuration/lock file.
    Source checkouts independently inventory the same trees and bind the exact
    Git commit plus dirty worktree.
    """

    root = Path(repository).resolve()
    config = Path(configuration_path).resolve()
    architecture = _load_architecture_version(config)
    if manifest_path:
        capture = capture_source_bundle_environment(
            manifest_path,
            bundle_root=root,
            dependency_lock=root / "uv.lock",
            configuration_path=config,
            evaluator_build="epoch-freeze-v1",
        )
        proof = capture.provenance
        image_id: str | None = None
        repo_digests: tuple[str, ...] = ()
        base_digests: tuple[str, ...] = ()
        runtime_identity = proof.manifest_hash
        if image_reference:
            image_id, repo_digests, base_digests = _inspect_oci_image(image_reference)
            runtime_identity = _domain_hash(
                "factor-lab/research-os/oci-runtime/v1",
                {
                    "source_bundle_manifest": proof.manifest_hash,
                    "oci_image_id": image_id,
                    "oci_repo_digests": repo_digests,
                    "oci_base_digests": base_digests,
                },
            )
        identity = _domain_hash(
            "factor-lab/research-os/epoch-build/v1",
            {
                "architecture_version": architecture,
                "source_tree_hash": proof.source_tree_hash,
                "configuration_tree_hash": proof.configuration_tree_hash,
                "dependency_lock_hash": proof.dependency_lock_hash,
                "image_source_digest": proof.manifest_hash,
                "oci_image_id": image_id,
                "oci_repo_digests": repo_digests,
                "oci_base_digests": base_digests,
            },
        )
        return EpochBuildProvenance(
            architecture_version=architecture,
            code_hash=proof.source_tree_hash,
            configuration_hash=proof.configuration_tree_hash,
            dependency_lock_hash=proof.dependency_lock_hash,
            # The source-only image has no Git metadata.  Its exact, reverified
            # build manifest is the immutable equivalent of a dirty-patch hash.
            dirty_patch_hash=runtime_identity,
            provenance_kind="immutable_source_bundle",
            build_identity_hash=identity,
            image_source_digest=proof.manifest_hash,
            oci_image_id=image_id,
            oci_repo_digests=repo_digests,
            oci_base_digests=base_digests,
        )

    if image_reference:
        raise SourceBundleProvenanceError(
            "OCI image binding requires an immutable source-bundle manifest"
        )

    manifest = build_source_bundle_manifest(bundle_root=root)
    configuration_root = (root / str(manifest["configuration"]["root"])).resolve()
    try:
        relative_config = config.relative_to(configuration_root).as_posix()
    except ValueError as exc:
        raise SourceBundleProvenanceError(
            "orchestration configuration is outside the repository config tree"
        ) from exc
    if relative_config not in {
        str(item["path"]) for item in manifest["configuration"]["files"]
    }:
        raise SourceBundleProvenanceError(
            "orchestration configuration is not in the measured config tree"
        )
    commit, dirty_hash = _capture_git_worktree(root)
    identity = _domain_hash(
        "factor-lab/research-os/epoch-build/v1",
        {
            "architecture_version": architecture,
            "source_tree_hash": manifest["source"]["tree_hash"],
            "configuration_tree_hash": manifest["configuration"]["tree_hash"],
            "dependency_lock_hash": manifest["dependency_lock"]["sha256"],
            "git_commit": commit,
            "dirty_patch_hash": dirty_hash,
        },
    )
    return EpochBuildProvenance(
        architecture_version=architecture,
        code_hash=str(manifest["source"]["tree_hash"]),
        configuration_hash=str(manifest["configuration"]["tree_hash"]),
        dependency_lock_hash=str(manifest["dependency_lock"]["sha256"]),
        dirty_patch_hash=dirty_hash,
        provenance_kind="git_worktree",
        build_identity_hash=identity,
        git_commit=commit,
    )


def bind_verified_oci_deployment(
    source_bundle: EpochBuildProvenance,
    *,
    oci_image_id: str,
    oci_repo_digests: tuple[str, ...] = (),
    oci_base_digests: tuple[str, ...],
) -> EpochBuildProvenance:
    """Bind verified host-Docker evidence to an immutable source bundle.

    This helper performs no Docker discovery and is therefore not a public
    attestation producer.  Readiness uses it only after independently
    validating the host-daemon attestation row and matching its re-hashed
    bundle fields to ``source_bundle``.  Keeping the derivation here ensures
    epoch freeze and runtime readiness calculate the exact same identity.
    """

    if (
        source_bundle.provenance_kind != "immutable_source_bundle"
        or source_bundle.image_source_digest is None
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", str(oci_image_id or ""))
        or not oci_base_digests
        or any(
            not re.fullmatch(r"sha256:[0-9a-f]{64}", value)
            for value in oci_base_digests
        )
        or any(
            not re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", value)
            for value in oci_repo_digests
        )
    ):
        raise SourceBundleProvenanceError(
            "verified OCI deployment evidence is incomplete or invalid"
        )
    repo_digests = tuple(sorted(set(oci_repo_digests)))
    base_digests = tuple(sorted(set(oci_base_digests)))
    runtime_identity = _domain_hash(
        "factor-lab/research-os/oci-runtime/v1",
        {
            "source_bundle_manifest": source_bundle.image_source_digest,
            "oci_image_id": oci_image_id,
            "oci_repo_digests": repo_digests,
            "oci_base_digests": base_digests,
        },
    )
    identity = _domain_hash(
        "factor-lab/research-os/epoch-build/v1",
        {
            "architecture_version": source_bundle.architecture_version,
            "source_tree_hash": source_bundle.code_hash,
            "configuration_tree_hash": source_bundle.configuration_hash,
            "dependency_lock_hash": source_bundle.dependency_lock_hash,
            "image_source_digest": source_bundle.image_source_digest,
            "oci_image_id": oci_image_id,
            "oci_repo_digests": repo_digests,
            "oci_base_digests": base_digests,
        },
    )
    return EpochBuildProvenance(
        architecture_version=source_bundle.architecture_version,
        code_hash=source_bundle.code_hash,
        configuration_hash=source_bundle.configuration_hash,
        dependency_lock_hash=source_bundle.dependency_lock_hash,
        dirty_patch_hash=runtime_identity,
        provenance_kind="immutable_source_bundle",
        build_identity_hash=identity,
        image_source_digest=source_bundle.image_source_digest,
        oci_image_id=oci_image_id,
        oci_repo_digests=repo_digests,
        oci_base_digests=base_digests,
    )


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create Factor Lab source-bundle provenance")
    parser.add_argument("--bundle-root", required=True)
    parser.add_argument("--source-root", default="src")
    parser.add_argument("--configuration-root", default="configs")
    parser.add_argument("--dependency-lock", default="uv.lock")
    parser.add_argument("--runtime-root")
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    write_source_bundle_manifest(
        args.output,
        bundle_root=args.bundle_root,
        source_root=args.source_root,
        configuration_root=args.configuration_root,
        dependency_lock=args.dependency_lock,
        runtime_root=args.runtime_root,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised by the Docker build
    raise SystemExit(_main())


__all__ = [
    "EpochBuildProvenance",
    "SOURCE_BUNDLE_MANIFEST_ENV",
    "SOURCE_BUNDLE_SCHEMA_VERSION",
    "SourceBundleProvenance",
    "SourceBundleEnvironmentCapture",
    "SourceBundleProvenanceError",
    "bind_verified_oci_deployment",
    "build_source_bundle_manifest",
    "capture_epoch_provenance",
    "capture_source_bundle_environment",
    "verify_source_bundle_manifest",
    "write_source_bundle_manifest",
]
