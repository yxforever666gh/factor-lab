"""Verify that prospective computation is the implementation published by a tag.

The prospective ledger binds an implementation manifest, not merely a symbolic
``generator_id``.  This module turns that manifest into an executable closure:
every project-local Python dependency and every numeric runtime version is
checked before a target or outcome may be generated.  Unrelated documentation
commits can therefore coexist with an old deployment, while silent edits to an
active implementation fail closed.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import re
import subprocess
import sys
import sysconfig
from typing import Any
import unicodedata


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_OID_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")


class ImplementationClosureError(RuntimeError):
    """Raised when the running code differs from the published deployment."""


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _git_lf_text_bytes(raw: bytes, *, label: str) -> bytes:
    """Mirror Git's clean conversion for closure files declared as LF text."""

    try:
        raw.decode("utf-8", errors="strict")
    except UnicodeError as exc:
        raise ImplementationClosureError(f"{label} is not UTF-8 text") from exc
    return raw.replace(b"\r\n", b"\n")


def _unique_pairs(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for raw_key, value in pairs:
        key = unicodedata.normalize("NFC", raw_key)
        if key in result:
            raise ImplementationClosureError(
                f"duplicate implementation-manifest key: {key!r}"
            )
        result[key] = value
    return result


def _running_distribution_versions() -> dict[str, str]:
    result: dict[str, str] = {}
    for distribution in importlib.metadata.distributions():
        raw_name = distribution.metadata.get("Name")
        if not isinstance(raw_name, str) or not raw_name.strip():
            raise ImplementationClosureError(
                "running distribution omits canonical Name metadata"
            )
        name = re.sub(r"[-_.]+", "-", raw_name.strip().casefold())
        if name in result:
            raise ImplementationClosureError(
                f"duplicate running distribution metadata: {name}"
            )
        version = distribution.version
        if not isinstance(version, str) or not version:
            raise ImplementationClosureError(
                f"running distribution version is invalid: {name}"
            )
        result[name] = version
    if not result:
        raise ImplementationClosureError("running environment has no distributions")
    return dict(sorted(result.items()))


def _load_manifest(path: Path) -> tuple[dict[str, Any], bytes]:
    try:
        raw = path.read_bytes()
        value = json.loads(
            raw.decode("utf-8", errors="strict"), object_pairs_hook=_unique_pairs
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ImplementationClosureError(
            f"unreadable implementation manifest: {path}"
        ) from exc
    if not isinstance(value, dict):
        raise ImplementationClosureError("implementation manifest must be an object")
    return value, raw


def _project_path(project_root: Path, value: Any, *, label: str) -> tuple[Path, str]:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ImplementationClosureError(f"{label} must be a canonical project path")
    relative = Path(value)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise ImplementationClosureError(f"{label} must be a canonical project path")
    path = (project_root / relative).resolve()
    try:
        canonical = path.relative_to(project_root).as_posix()
    except ValueError as exc:
        raise ImplementationClosureError(f"{label} escapes the project root") from exc
    if canonical != value:
        raise ImplementationClosureError(f"{label} is not canonical")
    if not path.is_file():
        raise ImplementationClosureError(f"missing {label}: {path}")
    return path, canonical


def _published_bytes(project_root: Path, commit_oid: str, relative_path: str) -> bytes:
    try:
        return subprocess.run(
            ["git", "show", f"{commit_oid}:{relative_path}"],
            cwd=project_root,
            check=True,
            capture_output=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ImplementationClosureError(
            f"published commit omits implementation artifact: {relative_path}"
        ) from exc


def verify_implementation_closure(
    project_root: str | Path,
    *,
    manifest_path: str | Path,
    manifest_sha256: str,
    implementation_commit_oid: str,
    generator_id: str,
    generator_entrypoint: str,
) -> dict[str, Any]:
    """Verify and return the manifest's canonical runtime-closure payload."""

    root = Path(project_root).expanduser().resolve()
    requested_manifest = Path(manifest_path)
    if requested_manifest.is_absolute():
        try:
            manifest_relative = requested_manifest.resolve().relative_to(root).as_posix()
        except ValueError as exc:
            raise ImplementationClosureError(
                "implementation manifest escapes the project root"
            ) from exc
    else:
        manifest_relative = requested_manifest.as_posix()
    manifest_file, manifest_relative = _project_path(
        root, manifest_relative, label="implementation manifest"
    )
    manifest, manifest_raw = _load_manifest(manifest_file)
    if not _SHA256_RE.fullmatch(str(manifest_sha256)) or _sha256(manifest_raw) != manifest_sha256:
        raise ImplementationClosureError("implementation manifest SHA-256 differs")
    if not _OID_RE.fullmatch(str(implementation_commit_oid)):
        raise ImplementationClosureError("implementation commit oid is invalid")
    if _published_bytes(root, implementation_commit_oid, manifest_relative) != manifest_raw:
        raise ImplementationClosureError(
            "implementation manifest differs from its published commit"
        )
    if manifest.get("generator_id") != generator_id:
        raise ImplementationClosureError("manifest generator_id differs from deployment")
    if manifest.get("generator_entrypoint") != generator_entrypoint:
        raise ImplementationClosureError(
            "manifest generator_entrypoint differs from deployment"
        )

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
    if not isinstance(closure, Mapping) or set(closure) != required:
        raise ImplementationClosureError("manifest runtime_closure has a non-exact schema")
    if type(closure["schema_version"]) is not int or closure["schema_version"] != 1:
        raise ImplementationClosureError("unsupported runtime closure schema")
    payload = {key: closure[key] for key in required - {"payload_sha256"}}
    expected_payload_sha = _sha256(_canonical_json_bytes(payload))
    if closure["payload_sha256"] != expected_payload_sha:
        raise ImplementationClosureError("runtime closure payload SHA-256 differs")
    if closure["python_version"] != platform.python_version():
        raise ImplementationClosureError("running Python differs from the deployment closure")
    platform_contract = {
        "python_implementation": platform.python_implementation(),
        "python_runtime": sys.version,
        "platform_system": platform.system(),
        "platform_machine": platform.machine(),
        "platform_tag": sysconfig.get_platform(),
    }
    for name, running in platform_contract.items():
        if closure[name] != running:
            raise ImplementationClosureError(
                f"running {name} differs from the deployment closure"
            )

    distributions = closure["distributions"]
    if not isinstance(distributions, Mapping) or not distributions:
        raise ImplementationClosureError("runtime closure distributions are empty")
    declared_distributions: dict[str, str] = {}
    for raw_name, raw_version in distributions.items():
        if not isinstance(raw_name, str) or not isinstance(raw_version, str):
            raise ImplementationClosureError("runtime distribution binding is invalid")
        canonical_name = re.sub(r"[-_.]+", "-", raw_name.strip().casefold())
        if canonical_name != raw_name or not raw_version:
            raise ImplementationClosureError("runtime distribution binding is invalid")
        declared_distributions[raw_name] = raw_version
    if declared_distributions != _running_distribution_versions():
        raise ImplementationClosureError(
            "running distribution set differs from the deployment closure"
        )

    files = closure["files"]
    if not isinstance(files, list) or not files:
        raise ImplementationClosureError("runtime closure files are empty")
    seen: set[str] = set()
    for index, item in enumerate(files):
        if not isinstance(item, Mapping) or set(item) != {"path", "sha256"}:
            raise ImplementationClosureError(
                f"runtime closure file {index} has a non-exact schema"
            )
        path, relative = _project_path(root, item["path"], label=f"closure file {index}")
        if relative in seen:
            raise ImplementationClosureError(f"duplicate runtime closure file: {relative}")
        seen.add(relative)
        wanted = item["sha256"]
        if not isinstance(wanted, str) or not _SHA256_RE.fullmatch(wanted):
            raise ImplementationClosureError(f"closure file hash is invalid: {relative}")
        published = _published_bytes(root, implementation_commit_oid, relative)
        if _sha256(published) != wanted:
            raise ImplementationClosureError(
                f"published implementation file differs from closure: {relative}"
            )
        local = _git_lf_text_bytes(
            path.read_bytes(), label=f"running implementation file {relative}"
        )
        if _sha256(local) != wanted:
            raise ImplementationClosureError(
                f"running implementation file differs from deployment: {relative}"
            )
        if published != local:
            raise ImplementationClosureError(
                f"implementation file differs from published commit: {relative}"
            )
    return dict(closure)


__all__ = ["ImplementationClosureError", "verify_implementation_closure"]
