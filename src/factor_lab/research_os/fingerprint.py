"""Canonical content fingerprints for replayable Research OS objects."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
import os
import platform as platform_module
import subprocess
import sys
from collections.abc import Mapping
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel


FINGERPRINT_DOMAIN = "factor-lab/research-os/v1"


def _normalize_datetime(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("naive datetimes are not allowed in canonical fingerprints")
    utc_value = value.astimezone(timezone.utc)
    return utc_value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def canonicalize(value: Any) -> Any:
    """Convert supported values into a deterministic JSON-compatible tree.

    Mapping order and set iteration never affect the result.  NaN and infinity
    are rejected because their JSON encodings are not portable across runtimes.
    """

    if isinstance(value, BaseModel):
        return canonicalize(value.model_dump(mode="python", exclude_none=False))
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return canonicalize(dataclasses.asdict(value))
    if isinstance(value, Enum):
        return canonicalize(value.value)
    if isinstance(value, datetime):
        return _normalize_datetime(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("non-finite Decimal cannot be fingerprinted")
        return format(value.normalize(), "f")
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, bytes):
        return {"$bytes_sha256": hashlib.sha256(value).hexdigest()}
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, (str, int, float, bool, Enum)):
                raise TypeError(f"unsupported mapping key type: {type(key).__name__}")
            normalized_key = str(key.value if isinstance(key, Enum) else key)
            if normalized_key in normalized:
                raise ValueError(
                    f"mapping keys collide after canonicalization: {normalized_key!r}"
                )
            normalized[normalized_key] = canonicalize(item)
        return {key: normalized[key] for key in sorted(normalized)}
    if isinstance(value, (set, frozenset)):
        items = [canonicalize(item) for item in value]
        return sorted(items, key=canonical_json)
    if isinstance(value, (list, tuple)):
        return [canonicalize(item) for item in value]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite float cannot be fingerprinted")
        return 0.0 if value == 0.0 else value
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported fingerprint value type: {type(value).__name__}")


def canonical_json(value: Any) -> str:
    return json.dumps(
        canonicalize(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def content_fingerprint(value: Any, *, domain: str = FINGERPRINT_DOMAIN) -> str:
    envelope = {"domain": domain, "payload": canonicalize(value)}
    return hashlib.sha256(canonical_json(envelope).encode("utf-8")).hexdigest()


def experiment_fingerprint(spec: Any) -> str:
    """Fingerprint every input capable of changing an experiment result."""

    from .contracts import ExperimentSpec

    if not isinstance(spec, ExperimentSpec):
        spec = ExperimentSpec.model_validate(spec)
    payload = {
        "snapshot": spec.snapshot,
        "universe": spec.universe,
        "label": spec.label,
        "features": spec.features,
        "candidate_kind": spec.candidate_kind,
        "candidate": spec.factor if spec.factor is not None else spec.sleeve,
        "portfolio": spec.portfolio,
        "validation": spec.validation,
        "evaluator_version": spec.evaluator_version,
        "environment": spec.environment,
        "evaluation_inputs": spec.evaluation_inputs,
        "preregistration": spec.preregistration,
    }
    return content_fingerprint(payload, domain=f"{FINGERPRINT_DOMAIN}/experiment")


def sha256_file(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _git_output(repository: Path, *args: str) -> bytes:
    completed = subprocess.run(
        ["git", *args],
        cwd=repository,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=True,
    )
    return completed.stdout


def capture_environment(
    repository: str | Path,
    *,
    dependency_lock: str | Path,
    configuration: Any,
    evaluator_build: str,
) -> "EnvironmentRef":
    """Capture the code, dirty patch, dependency lock, and runtime identity.

    Untracked, non-ignored files are included by relative path and content
    digest.  Git-ignored secrets and runtime artifacts are therefore excluded
    without maintaining a second, error-prone ignore policy.
    """

    from .contracts import EnvironmentRef

    repository_path = Path(repository).resolve()
    lock_path = Path(dependency_lock).resolve()
    try:
        head = _git_output(repository_path, "rev-parse", "HEAD").strip()
        patch = _git_output(repository_path, "diff", "--binary", "HEAD")
        untracked_raw = _git_output(
            repository_path, "ls-files", "--others", "--exclude-standard", "-z"
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError("repository must be a readable Git worktree") from exc
    code_hash = hashlib.sha256(head).hexdigest()
    untracked_entries: list[dict[str, str]] = []
    for raw_name in untracked_raw.split(b"\0"):
        if not raw_name:
            continue
        relative_name = os.fsdecode(raw_name)
        relative_path = Path(relative_name)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise RuntimeError(f"Git returned an unsafe untracked path: {relative_name!r}")
        path = repository_path / relative_path
        if path.is_symlink():
            digest = hashlib.sha256(
                os.fsencode(os.readlink(path))
            ).hexdigest()
        elif path.is_file():
            digest = sha256_file(path)
        else:
            continue
        untracked_entries.append(
            {"path": relative_path.as_posix(), "sha256": digest}
        )
    untracked_entries.sort(key=lambda entry: entry["path"])
    dirty_patch_hash = None
    if patch or untracked_entries:
        dirty_patch_hash = content_fingerprint(
            {
                "tracked_patch_sha256": hashlib.sha256(patch).hexdigest(),
                "untracked_files": untracked_entries,
            },
            domain=f"{FINGERPRINT_DOMAIN}/dirty-worktree",
        )
    return EnvironmentRef(
        code_hash=code_hash,
        dependency_lock_hash=sha256_file(lock_path),
        configuration_hash=content_fingerprint(
            configuration, domain=f"{FINGERPRINT_DOMAIN}/configuration"
        ),
        dirty_patch_hash=dirty_patch_hash,
        python_version=sys.version.split()[0],
        platform=f"{platform_module.system()}-{platform_module.machine()}",
        evaluator_build=evaluator_build,
    )


def file_tree_fingerprint(
    paths: list[str | Path] | tuple[str | Path, ...],
    *,
    base_dir: str | Path | None = None,
) -> str:
    """Fingerprint named files without allowing their input order to matter."""

    base = Path(base_dir).resolve() if base_dir is not None else None
    entries: list[dict[str, str]] = []
    for raw_path in paths:
        path = Path(raw_path).resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        if base is not None:
            try:
                name = path.relative_to(base).as_posix()
            except ValueError as exc:
                raise ValueError(f"{path} is outside base_dir {base}") from exc
        else:
            name = os.fspath(path)
        entries.append({"path": name, "sha256": sha256_file(path)})
    entries.sort(key=lambda entry: entry["path"])
    return content_fingerprint(entries, domain=f"{FINGERPRINT_DOMAIN}/file-tree")


__all__ = [
    "FINGERPRINT_DOMAIN",
    "canonical_json",
    "canonicalize",
    "capture_environment",
    "content_fingerprint",
    "experiment_fingerprint",
    "file_tree_fingerprint",
    "sha256_file",
]
