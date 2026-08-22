"""Transactional boundary for building, publishing and cataloging snapshots."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .catalog import ResearchCatalog
from .evaluator import CANONICAL_EVALUATOR_VERSION
from .fingerprint import capture_environment
from .snapshots import (
    ImmutableSnapshotManifest,
    build_immutable_snapshot_manifest,
    publish_snapshot_manifest,
)


@dataclass(frozen=True)
class SnapshotPublication:
    snapshot_id: str
    tier: str
    manifest_path: str
    uri: str
    content_hash: str
    quality_status: str
    trust_labels: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def environment_hashes(environment: Any) -> dict[str, str]:
    empty = hashlib.sha256(b"").hexdigest()
    return {
        "code_hash": environment.code_hash,
        "dependency_lock_hash": environment.dependency_lock_hash,
        "config_hash": environment.configuration_hash,
        "dirty_patch_hash": environment.dirty_patch_hash or empty,
    }


def publish_cataloged_snapshot(
    catalog: ResearchCatalog,
    *,
    paths: Iterable[str | Path],
    base_dir: str | Path,
    snapshot_root: str | Path,
    tier: str,
    as_of: datetime | str,
    parent_snapshot_ids: Sequence[str],
    quality_report: Mapping[str, Any],
    trust_labels: Sequence[str],
    repository: str | Path,
    dependency_lock: str | Path,
    configuration: Mapping[str, Any],
    uri: str | None = None,
    production: bool = False,
) -> SnapshotPublication:
    """Publish once locally, then register the exact immutable reference."""

    if production and (uri is None or not uri.startswith("s3://")):
        raise ValueError("production snapshots must use an immutable s3:// MinIO URI")
    environment = capture_environment(
        repository,
        dependency_lock=dependency_lock,
        configuration=configuration,
        evaluator_build=CANONICAL_EVALUATOR_VERSION,
    )
    manifest = build_immutable_snapshot_manifest(
        paths,
        base_dir=base_dir,
        tier=tier,
        as_of=as_of,
        parent_snapshot_ids=parent_snapshot_ids,
        environment_hashes=environment_hashes(environment),
        quality_report=quality_report,
        trust_labels=trust_labels,
    )
    manifest_path = publish_snapshot_manifest(
        snapshot_root,
        manifest,
        base_dir=base_dir,
    )
    resolved_uri = uri or manifest_path.resolve().as_uri()
    reference = manifest.to_snapshot_ref(uri=resolved_uri)
    catalog.register_snapshot(reference)
    return SnapshotPublication(
        snapshot_id=manifest.snapshot_id,
        tier=manifest.tier,
        manifest_path=str(manifest_path.resolve()),
        uri=resolved_uri,
        content_hash=manifest.content_hash,
        quality_status=manifest.quality_status,
        trust_labels=manifest.trust_labels,
    )


__all__ = [
    "SnapshotPublication",
    "environment_hashes",
    "publish_cataloged_snapshot",
]
