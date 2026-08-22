"""Content-addressed, immutable Bronze/Silver/Gold snapshot manifests."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from .contracts import DataSnapshotRef, SnapshotTier
from .data_quality import QualityReport, sha256_path


SNAPSHOT_SCHEMA_VERSION = "research-os/snapshot-manifest/v1"
REQUIRED_ENVIRONMENT_HASHES = (
    "config_hash",
    "code_hash",
    "dirty_patch_hash",
    "dependency_lock_hash",
)


class SnapshotIntegrityError(RuntimeError):
    pass


@dataclass(frozen=True)
class SnapshotFrameBinding:
    """Verified bridge from an in-memory research frame to Gold evidence."""

    snapshot_id: str
    iceberg_uri: str
    manifest_file: str
    source_sha256: str
    frame_content_hash: str
    row_count: int


@dataclass(frozen=True)
class SnapshotFile:
    path: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class ImmutableSnapshotManifest:
    schema_version: str
    snapshot_id: str
    tier: str
    as_of: str
    parent_snapshot_ids: tuple[str, ...]
    environment_hashes: Mapping[str, str]
    quality_status: str
    trust_labels: tuple[str, ...]
    files: tuple[SnapshotFile, ...]
    # Optional exchange-calendar evidence is part of the content address.  It
    # lives at the manifest top level because the shadow ledger consumes it
    # without trusting a mutable sidecar file.
    trading_calendar: Mapping[str, Any] | None = None

    @property
    def content_hash(self) -> str:
        return self.snapshot_id

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["parent_snapshot_ids"] = list(self.parent_snapshot_ids)
        payload["trust_labels"] = list(self.trust_labels)
        payload["files"] = [asdict(item) for item in self.files]
        if self.trading_calendar is None:
            payload.pop("trading_calendar", None)
        else:
            payload["trading_calendar"] = dict(self.trading_calendar)
        return payload

    def to_snapshot_ref(self, *, uri: str):
        """Convert to the public Research OS contract without creating an import cycle."""

        from .contracts import DataQualityStatus, DataSnapshotRef, SnapshotTier

        quality = {
            "pass": DataQualityStatus.ACCEPTED,
            "warning": DataQualityStatus.DISPUTED,
            "blocked": DataQualityStatus.QUARANTINED,
        }[self.quality_status]
        return DataSnapshotRef(
            snapshot_id=self.snapshot_id,
            tier=SnapshotTier(self.tier),
            uri=uri,
            content_hash=self.content_hash,
            parent_snapshot_ids=self.parent_snapshot_ids,
            as_of=self.as_of,
            quality_status=quality,
            trust_labels=self.trust_labels,
            manifest=self.to_dict(),
        )


def _require_hash(value: str, *, name: str) -> str:
    value = str(value)
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _normalize_as_of(value: str | datetime) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("as_of must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("as_of must include a timezone")
    return parsed.isoformat()


def _canonical_payload(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _content_payload(manifest: Mapping[str, Any]) -> dict[str, Any]:
    payload = {
        "schema_version": manifest["schema_version"],
        "tier": manifest["tier"],
        "as_of": manifest["as_of"],
        "parent_snapshot_ids": list(manifest["parent_snapshot_ids"]),
        "environment_hashes": dict(manifest["environment_hashes"]),
        "quality_status": manifest["quality_status"],
        "trust_labels": list(manifest["trust_labels"]),
        "files": list(manifest["files"]),
    }
    if "trading_calendar" in manifest:
        payload["trading_calendar"] = dict(manifest["trading_calendar"])
    return payload


def _validated_trading_calendar(
    value: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if value is None:
        return None
    source = str(value.get("source") or "").strip()
    quality_status = str(value.get("quality_status") or "").strip().lower()
    raw_sessions = value.get("sessions")
    if not source:
        raise SnapshotIntegrityError("trading calendar source is required")
    if quality_status != "accepted":
        raise SnapshotIntegrityError("trading calendar must be quality accepted")
    if not isinstance(raw_sessions, (list, tuple)) or not raw_sessions:
        raise SnapshotIntegrityError("trading calendar requires ordered sessions")
    try:
        sessions = tuple(pd.Timestamp(item).date().isoformat() for item in raw_sessions)
    except Exception as exc:
        raise SnapshotIntegrityError("trading calendar contains an invalid session") from exc
    if sessions != tuple(sorted(set(sessions))):
        raise SnapshotIntegrityError("trading calendar sessions must be unique and ordered")
    content_hash = hashlib.sha256("\n".join(sessions).encode("ascii")).hexdigest()
    supplied_hash = str(value.get("content_hash") or "")
    if supplied_hash and supplied_hash != content_hash:
        raise SnapshotIntegrityError("trading calendar content hash mismatch")
    return {
        "source": source,
        "quality_status": quality_status,
        "sessions": list(sessions),
        "content_hash": content_hash,
    }


def _frame_content_hash(frame: pd.DataFrame) -> str:
    """Hash values, order, index, columns and dtypes without trusting attrs."""

    header = _canonical_payload(
        {
            "columns": [str(item) for item in frame.columns],
            "dtypes": [str(item) for item in frame.dtypes],
            "index_type": type(frame.index).__name__,
            "index_name": None if frame.index.name is None else str(frame.index.name),
        }
    )
    values = pd.util.hash_pandas_object(frame, index=True, categorize=True).to_numpy(
        dtype="uint64", copy=False
    )
    digest = hashlib.sha256()
    digest.update(header)
    digest.update(values.tobytes())
    return digest.hexdigest()


def _read_bound_frame(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".jsonl", ".ndjson"}:
        return pd.read_json(path, lines=True)
    raise SnapshotIntegrityError(
        f"research frame source format is not supported for binding: {suffix}"
    )


def verify_snapshot_frame_binding(
    reference: DataSnapshotRef,
    frame: pd.DataFrame,
) -> SnapshotFrameBinding:
    """Fail closed unless ``frame`` is the exact data file published as Gold.

    A caller-supplied ``DataSnapshotRef`` is not evidence.  This verifier
    recomputes the embedded immutable manifest identity, checks the Iceberg tag,
    hashes a real source file declared by the manifest, re-reads that file, and
    compares its content hash with the actual in-memory frame.  DataFrame attrs
    only locate the source; altering either the frame or the attrs cannot make a
    different file satisfy the manifest hash.
    """

    if reference.tier is not SnapshotTier.GOLD:
        raise SnapshotIntegrityError("historical research requires a Gold snapshot")
    manifest = dict(reference.manifest or {})
    if manifest.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
        raise SnapshotIntegrityError("Gold snapshot lacks a versioned immutable manifest")
    try:
        expected_snapshot_id = hashlib.sha256(
            _canonical_payload(_content_payload(manifest))
        ).hexdigest()
    except (KeyError, TypeError, ValueError) as exc:
        raise SnapshotIntegrityError("Gold snapshot manifest structure is invalid") from exc
    if not (
        manifest.get("snapshot_id")
        == expected_snapshot_id
        == reference.snapshot_id
        == reference.content_hash
    ):
        raise SnapshotIntegrityError("snapshot ref and immutable manifest identity differ")
    if str(manifest.get("tier")) != reference.tier.value:
        raise SnapshotIntegrityError("snapshot ref and manifest tier differ")
    if str(manifest.get("quality_status")) != "pass":
        raise SnapshotIntegrityError("Gold research manifest is not quality accepted")
    try:
        manifest_as_of = datetime.fromisoformat(
            str(manifest.get("as_of") or "").replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise SnapshotIntegrityError("Gold manifest as_of is invalid") from exc
    if manifest_as_of != reference.as_of:
        raise SnapshotIntegrityError("snapshot ref and manifest as_of differ")
    if tuple(manifest.get("parent_snapshot_ids") or ()) != reference.parent_snapshot_ids:
        raise SnapshotIntegrityError("snapshot ref and manifest parents differ")
    if tuple(manifest.get("trust_labels") or ()) != reference.trust_labels:
        raise SnapshotIntegrityError("snapshot ref and manifest trust labels differ")
    if not reference.uri.startswith("iceberg://"):
        raise SnapshotIntegrityError("Gold research snapshot requires Iceberg publication evidence")
    _, separator, tag = reference.uri.partition("#")
    if not separator or not tag or not tag.endswith(reference.snapshot_id):
        raise SnapshotIntegrityError("Iceberg URI tag is not bound to the Gold snapshot id")

    source_hint = str(frame.attrs.get("research_os_source_path") or "").strip()
    if not source_hint:
        raise SnapshotIntegrityError("research frame has no verifiable source path")
    source = Path(source_hint).resolve()
    if not source.is_file():
        raise SnapshotIntegrityError(f"research frame source is missing: {source}")
    source_hash = sha256_path(source)
    hinted_hash = str(frame.attrs.get("research_os_source_sha256") or "")
    if hinted_hash and hinted_hash != source_hash:
        raise SnapshotIntegrityError("research frame source changed after it was read")
    source_size = source.stat().st_size
    entries = tuple(manifest.get("files") or ())
    try:
        matches = [
            item
            for item in entries
            if str(item.get("sha256") or "") == source_hash
            and int(item.get("size_bytes", -1)) == source_size
        ]
    except (AttributeError, TypeError, ValueError) as exc:
        raise SnapshotIntegrityError("Gold manifest file evidence is invalid") from exc
    if len(matches) != 1:
        raise SnapshotIntegrityError(
            "research frame source is not uniquely declared by the Gold manifest"
        )
    reloaded = _read_bound_frame(source)
    actual_hash = _frame_content_hash(frame)
    source_frame_hash = _frame_content_hash(reloaded)
    if actual_hash != source_frame_hash:
        raise SnapshotIntegrityError(
            "in-memory research frame differs from its manifest-bound source file"
        )
    return SnapshotFrameBinding(
        snapshot_id=reference.snapshot_id,
        iceberg_uri=reference.uri,
        manifest_file=str(matches[0].get("path") or ""),
        source_sha256=source_hash,
        frame_content_hash=actual_hash,
        row_count=len(frame),
    )


def _expand_snapshot_files(paths: Iterable[str | Path]) -> list[Path]:
    expanded: list[Path] = []
    for raw in paths:
        path = Path(raw)
        if path.is_symlink():
            raise SnapshotIntegrityError(f"snapshot inputs cannot be symlinks: {path}")
        if path.is_dir():
            for candidate in path.rglob("*"):
                if candidate.is_symlink():
                    raise SnapshotIntegrityError(
                        f"snapshot inputs cannot contain symlinks: {candidate}"
                    )
                if candidate.is_file():
                    expanded.append(candidate)
        elif path.is_file():
            expanded.append(path)
        else:
            raise FileNotFoundError(path)
    resolved = {path.resolve() for path in expanded}
    return sorted(resolved, key=lambda item: item.as_posix())


def build_immutable_snapshot_manifest(
    paths: Iterable[str | Path],
    *,
    base_dir: str | Path,
    tier: str,
    as_of: str | datetime,
    parent_snapshot_ids: Sequence[str],
    environment_hashes: Mapping[str, str],
    quality_report: QualityReport | Mapping[str, Any],
    trust_labels: Sequence[str] = (),
    trading_calendar: Mapping[str, Any] | None = None,
) -> ImmutableSnapshotManifest:
    """Build a snapshot ID from every parent, environment input, and file hash."""

    tier = str(tier).lower()
    if tier not in {"bronze", "silver", "gold"}:
        raise ValueError("tier must be bronze, silver, or gold")
    parents = tuple(sorted({_require_hash(value, name="parent_snapshot_id") for value in parent_snapshot_ids}))
    environment = dict(environment_hashes)
    missing_environment = sorted(set(REQUIRED_ENVIRONMENT_HASHES) - set(environment))
    if missing_environment:
        raise ValueError(f"environment hashes missing: {missing_environment}")
    environment = {
        key: _require_hash(value, name=key)
        for key, value in sorted(environment.items())
    }
    if isinstance(quality_report, QualityReport):
        quality_status = quality_report.status
    else:
        quality_status = str(quality_report.get("status") or "")
    if quality_status not in {"pass", "warning", "blocked"}:
        raise ValueError("quality status must be pass, warning, or blocked")
    if tier == "gold" and quality_status != "pass":
        raise SnapshotIntegrityError("Gold snapshots require a passing quality report")
    if tier in {"silver", "gold"} and not parents:
        raise SnapshotIntegrityError(f"{tier} snapshots require at least one parent snapshot")

    base = Path(base_dir).resolve()
    files: list[SnapshotFile] = []
    for path in _expand_snapshot_files(paths):
        try:
            relative = path.relative_to(base).as_posix()
        except ValueError as exc:
            raise SnapshotIntegrityError(f"snapshot input is outside base_dir: {path}") from exc
        files.append(
            SnapshotFile(path=relative, size_bytes=path.stat().st_size, sha256=sha256_path(path))
        )
    if not files:
        raise SnapshotIntegrityError("snapshot must contain at least one file")
    files.sort(key=lambda item: item.path)
    normalized_as_of = _normalize_as_of(as_of)
    normalized_calendar = _validated_trading_calendar(trading_calendar)
    payload = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "tier": tier,
        "as_of": normalized_as_of,
        "parent_snapshot_ids": list(parents),
        "environment_hashes": environment,
        "quality_status": quality_status,
        "trust_labels": sorted(set(map(str, trust_labels))),
        "files": [asdict(item) for item in files],
    }
    if normalized_calendar is not None:
        payload["trading_calendar"] = normalized_calendar
    snapshot_id = hashlib.sha256(_canonical_payload(payload)).hexdigest()
    return ImmutableSnapshotManifest(
        schema_version=SNAPSHOT_SCHEMA_VERSION,
        snapshot_id=snapshot_id,
        tier=tier,
        as_of=normalized_as_of,
        parent_snapshot_ids=parents,
        environment_hashes=environment,
        quality_status=quality_status,
        trust_labels=tuple(payload["trust_labels"]),
        files=tuple(files),
        trading_calendar=normalized_calendar,
    )


def verify_immutable_snapshot_manifest(
    manifest: ImmutableSnapshotManifest | Mapping[str, Any],
    *,
    base_dir: str | Path,
) -> dict[str, Any]:
    payload = manifest.to_dict() if isinstance(manifest, ImmutableSnapshotManifest) else dict(manifest)
    errors: list[dict[str, Any]] = []
    try:
        if payload.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
            errors.append({"code": "schema_version_invalid"})
        files = list(payload.get("files") or [])
        file_paths = [str(item.get("path") or "") for item in files]
        if not files:
            errors.append({"code": "manifest_empty"})
        if len(file_paths) != len(set(file_paths)):
            errors.append({"code": "duplicate_manifest_paths"})
        expected_id = hashlib.sha256(_canonical_payload(_content_payload(payload))).hexdigest()
        if payload.get("snapshot_id") != expected_id:
            errors.append(
                {"code": "snapshot_id_mismatch", "expected": expected_id, "actual": payload.get("snapshot_id")}
            )
        base = Path(base_dir).resolve()
        for entry in files:
            stored = str(entry.get("path") or "")
            pure = PurePosixPath(stored)
            if pure.is_absolute() or ".." in pure.parts or not stored:
                errors.append({"code": "unsafe_manifest_path", "path": stored})
                continue
            path = (base / Path(*pure.parts)).resolve()
            try:
                path.relative_to(base)
            except ValueError:
                errors.append({"code": "manifest_path_escape", "path": stored})
                continue
            if not path.is_file():
                errors.append({"code": "snapshot_file_missing", "path": stored})
                continue
            actual_size = path.stat().st_size
            raw_size = entry.get("size_bytes")
            expected_size = int(raw_size) if raw_size is not None else -1
            if expected_size != actual_size:
                errors.append(
                    {
                        "code": "snapshot_size_mismatch",
                        "path": stored,
                        "expected": expected_size,
                        "actual": actual_size,
                    }
                )
            expected_hash = str(entry.get("sha256") or "")
            actual_hash = sha256_path(path)
            if expected_hash != actual_hash:
                errors.append(
                    {
                        "code": "snapshot_hash_mismatch",
                        "path": stored,
                        "expected": expected_hash,
                        "actual": actual_hash,
                    }
                )
        if payload.get("tier") == "gold" and payload.get("quality_status") != "pass":
            errors.append({"code": "gold_quality_not_pass"})
        if "trading_calendar" in payload:
            try:
                _validated_trading_calendar(payload.get("trading_calendar"))
            except SnapshotIntegrityError as exc:
                errors.append(
                    {"code": "trading_calendar_invalid", "message": str(exc)}
                )
        environment = dict(payload.get("environment_hashes") or {})
        for name in REQUIRED_ENVIRONMENT_HASHES:
            if not re.fullmatch(r"[0-9a-f]{64}", str(environment.get(name) or "")):
                errors.append({"code": "environment_hash_invalid", "name": name})
    except (KeyError, TypeError, ValueError) as exc:
        errors.append({"code": "manifest_structure_invalid", "message": str(exc)})
    return {
        "valid": not errors,
        "snapshot_id": payload.get("snapshot_id"),
        "checked_count": len(payload.get("files") or []),
        "errors": errors,
    }


def publish_snapshot_manifest(
    snapshot_root: str | Path,
    manifest: ImmutableSnapshotManifest,
    *,
    base_dir: str | Path | None = None,
) -> Path:
    """Publish exactly once; identical retries are idempotent, changes are rejected."""

    if manifest.tier == "gold" and manifest.quality_status != "pass":
        raise SnapshotIntegrityError("Gold publication is blocked unless data quality passes")
    if manifest.tier == "gold" and base_dir is None:
        raise SnapshotIntegrityError("Gold publication requires file verification via base_dir")
    expected_id = hashlib.sha256(
        _canonical_payload(_content_payload(manifest.to_dict()))
    ).hexdigest()
    if manifest.snapshot_id != expected_id:
        raise SnapshotIntegrityError("snapshot content does not match its immutable identity")
    if base_dir is not None:
        verification = verify_immutable_snapshot_manifest(manifest, base_dir=base_dir)
        if not verification["valid"]:
            raise SnapshotIntegrityError(
                f"snapshot file verification failed: {verification['errors']}"
            )
    root = Path(snapshot_root)
    directory = root / manifest.snapshot_id
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / "manifest.json"
    encoded = json.dumps(
        manifest.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    try:
        descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444)
    except FileExistsError:
        if target.read_bytes() != encoded:
            raise SnapshotIntegrityError(
                f"immutable manifest already exists with different content: {target}"
            )
        return target
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        # A partial exclusive file is intentionally left visible: a subsequent
        # publish will reject it instead of silently replacing audit evidence.
        raise
    return target


__all__ = [
    "ImmutableSnapshotManifest",
    "REQUIRED_ENVIRONMENT_HASHES",
    "SNAPSHOT_SCHEMA_VERSION",
    "SnapshotFile",
    "SnapshotFrameBinding",
    "SnapshotIntegrityError",
    "build_immutable_snapshot_manifest",
    "publish_snapshot_manifest",
    "verify_immutable_snapshot_manifest",
    "verify_snapshot_frame_binding",
]
