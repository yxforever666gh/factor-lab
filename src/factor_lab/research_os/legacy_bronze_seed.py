"""Fail-closed import of the old ``expanded_long_only`` raw cache.

The expanded research artifacts predate the Research OS data contracts.  They
are useful as an immutable, hash-checked Bronze seed, but they are not accepted
market evidence and must never satisfy a Silver/Gold partition.  This module
therefore writes them under a separate ledger/source identity and separately
pre-registers the canonical Tushare partitions that still need a vendor
download.

Production callers do not supply a path.  The only supported source is the
fixed directory declared by the reviewed production configuration underneath
the mounted runtime data root.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import tempfile
from typing import Any, Mapping, Sequence

import pyarrow.parquet as pq

from .catalog import ResearchCatalog, RunRecord
from .data_quality import sha256_path
from .fingerprint import content_fingerprint
from .object_store import S3ImmutableArchive
from .production_ledger import (
    PartitionIdentity,
    PartitionStatus,
    ProductionLedger,
    ProductionLedgerError,
)
from .snapshots import (
    build_immutable_snapshot_manifest,
    publish_snapshot_manifest,
)


LEGACY_SEED_SOURCE_ID = "legacy-expanded-long-only"
LEGACY_SEED_ROOT_RELATIVE = Path("legacy") / "expanded_long_only"
LEGACY_SEED_DATASETS = ("daily", "daily_basic", "adj_factor")
LEGACY_SEED_TRUST_LABELS = (
    "gold_promotion_forbidden",
    "legacy_expanded_long_only_bronze_seed",
    "st_history_unverified",
)
FORBIDDEN_PROMOTION_TRUST_LABELS = frozenset(
    {"gold_promotion_forbidden", *LEGACY_SEED_TRUST_LABELS}
)
LEGACY_SEED_CONFIG_MODE = "hash_verified_checkpoint"
LEGACY_SEED_PROMOTION_POLICY = "bronze_only_fail_closed"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_PARTITION_KEY = re.compile(
    r"^(daily|daily_basic|adj_factor)/(\d{4}-\d{2}-\d{2})$"
)


class LegacyBronzeSeedError(RuntimeError):
    """The fixed legacy seed or its immutable evidence is invalid."""


class SnapshotPromotionBlocked(LegacyBronzeSeedError):
    """A Bronze/Silver parent carries a non-promotable legacy trust label."""


@dataclass(frozen=True)
class LegacyBronzeSeedSpec:
    root: Path
    checkpoint_path: Path
    datasets: tuple[str, ...]
    canonical_source_id: str
    source_config_by_dataset: Mapping[str, Mapping[str, Any]]


@dataclass(frozen=True)
class LegacySeedEntry:
    dataset: str
    partition_key: str
    source_path: Path
    sha256: str
    size_bytes: int
    row_count: int
    completed_at: datetime


@dataclass(frozen=True)
class LegacySeedPreparation:
    status: str
    accepted_session_count: int
    canonical_partition_count: int
    canonical_pending_count: int
    canonical_succeeded_count: int
    seed_candidate_count: int
    seed_imported_count: int
    seed_reused_count: int
    seed_failed_count: int
    seed_busy_count: int
    missing_by_dataset: Mapping[str, int]
    pending_reason_counts: Mapping[str, int]
    checkpoint_sha256: str | None
    st_history_unverified: bool = True
    st_history_row_count: int = 0
    st_history_sha256: str | None = None
    st_history_reason: str = "legacy_st_history_not_pit_verified"
    promotion_allowed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _aware(value: Any, *, fallback: datetime) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return fallback
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise LegacyBronzeSeedError("legacy checkpoint completed_at must include a timezone")
    return parsed.astimezone(timezone.utc)


def _is_link_or_reparse(path: Path) -> bool:
    """Detect links and Windows junctions without following their targets."""

    try:
        details = os.lstat(path)
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(details.st_mode):
        return True
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(getattr(details, "st_file_attributes", 0) & reparse_flag)


def _assert_no_link_components(path: Path) -> None:
    current = Path(os.path.abspath(os.fspath(path)))
    while True:
        if _is_link_or_reparse(current):
            raise LegacyBronzeSeedError(
                "legacy seed path cannot traverse a symlink/reparse point"
            )
        parent = current.parent
        if parent == current:
            return
        current = parent


def _ensure_regular_file(path: Path, *, root: Path) -> Path:
    _assert_no_link_components(root)
    _assert_no_link_components(path)
    if _is_link_or_reparse(path) or not path.is_file():
        raise LegacyBronzeSeedError(f"legacy seed input is not a regular file: {path.name}")
    resolved = path.resolve(strict=True)
    try:
        resolved.relative_to(root.resolve(strict=True))
    except ValueError as exc:
        raise LegacyBronzeSeedError("legacy seed input escapes the fixed seed root") from exc
    return resolved


def _write_bytes_once(path: Path, payload: bytes) -> Path:
    _assert_no_link_components(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if _is_link_or_reparse(path) or path.read_bytes() != payload:
            raise LegacyBronzeSeedError(f"immutable Bronze metadata differs: {path}")
        return path
    with tempfile.NamedTemporaryFile(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        try:
            os.link(temporary, path)
        except FileExistsError:
            if _is_link_or_reparse(path) or path.read_bytes() != payload:
                raise LegacyBronzeSeedError(
                    f"concurrent immutable Bronze metadata differs: {path}"
                )
    finally:
        temporary.unlink(missing_ok=True)
    return path


def _copy_once(source: Path, target: Path, *, sha256: str, size_bytes: int) -> Path:
    _assert_no_link_components(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if (
            _is_link_or_reparse(target)
            or target.stat().st_size != size_bytes
            or sha256_path(target) != sha256
        ):
            raise LegacyBronzeSeedError(f"immutable Bronze cache differs: {target}")
        return target
    with tempfile.NamedTemporaryFile(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        with source.open("rb") as source_handle:
            shutil.copyfileobj(source_handle, handle, length=1024 * 1024)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        if temporary.stat().st_size != size_bytes or sha256_path(temporary) != sha256:
            raise LegacyBronzeSeedError("legacy seed changed while it was being copied")
        try:
            os.link(temporary, target)
        except FileExistsError:
            if target.stat().st_size != size_bytes or sha256_path(target) != sha256:
                raise LegacyBronzeSeedError(
                    f"concurrent immutable Bronze cache differs: {target}"
                )
    finally:
        temporary.unlink(missing_ok=True)
    return target


def legacy_bronze_seed_spec(
    config: Mapping[str, Any], *, runtime_data_root: str | Path
) -> LegacyBronzeSeedSpec:
    """Resolve the one reviewed production seed location.

    There is deliberately no path override parameter.  A caller can either use
    the reviewed config at the mounted runtime root or not use the seed.
    """

    root_base = Path(runtime_data_root)
    bootstrap = (config.get("daily") or {}).get("bootstrap")
    seed = bootstrap.get("legacy_bronze_seed") if isinstance(bootstrap, Mapping) else None
    if not isinstance(seed, Mapping):
        raise LegacyBronzeSeedError("production bootstrap lacks legacy_bronze_seed")
    if seed.get("mode") != LEGACY_SEED_CONFIG_MODE:
        raise LegacyBronzeSeedError("legacy Bronze seed mode is not hash_verified_checkpoint")
    if seed.get("promotion_policy") != LEGACY_SEED_PROMOTION_POLICY:
        raise LegacyBronzeSeedError("legacy Bronze seed must be bronze_only_fail_closed")
    root = Path(str(seed.get("root") or ""))
    expected_root = root_base / LEGACY_SEED_ROOT_RELATIVE
    if root != expected_root or not root.is_absolute():
        raise LegacyBronzeSeedError(
            f"legacy Bronze seed root must be {expected_root.as_posix()}"
        )
    if str(seed.get("checkpoint") or "") != "download_checkpoint.json":
        raise LegacyBronzeSeedError("legacy Bronze seed checkpoint name is fixed")
    datasets = tuple(map(str, seed.get("datasets") or ()))
    if datasets != LEGACY_SEED_DATASETS:
        raise LegacyBronzeSeedError(
            "legacy Bronze seed datasets must be daily,daily_basic,adj_factor"
        )
    daily = config.get("daily")
    sources = daily.get("sources") if isinstance(daily, Mapping) else None
    if not isinstance(sources, Sequence) or isinstance(sources, (str, bytes)):
        raise LegacyBronzeSeedError("production daily sources are missing")
    selected: dict[str, Mapping[str, Any]] = {}
    canonical_source_id = ""
    for source in sources:
        if not isinstance(source, Mapping) or source.get("source") != "tushare":
            continue
        request = source.get("request")
        dataset = str(request.get("dataset") or "") if isinstance(request, Mapping) else ""
        if dataset not in LEGACY_SEED_DATASETS:
            continue
        cadence = source.get("partition_cadence")
        if not isinstance(cadence, Mapping) or cadence.get("kind") != "trading_session":
            raise LegacyBronzeSeedError(f"{dataset} is not a trading-session source")
        source_id = str(
            cadence.get("ledger_identity")
            or source.get("profile_name")
            or source.get("source")
            or ""
        )
        if not source_id:
            raise LegacyBronzeSeedError(f"{dataset} has no canonical source identity")
        if canonical_source_id and canonical_source_id != source_id:
            raise LegacyBronzeSeedError("seed datasets do not share one Tushare authority")
        canonical_source_id = source_id
        selected[dataset] = dict(source)
    if tuple(name for name in LEGACY_SEED_DATASETS if name in selected) != LEGACY_SEED_DATASETS:
        raise LegacyBronzeSeedError("production config lacks a canonical Tushare seed dataset")
    return LegacyBronzeSeedSpec(
        root=root,
        checkpoint_path=root / "download_checkpoint.json",
        datasets=datasets,
        canonical_source_id=canonical_source_id,
        source_config_by_dataset=selected,
    )


def _checkpoint_entry(
    spec: LegacyBronzeSeedSpec,
    *,
    key: str,
    raw: Mapping[str, Any],
    now: datetime,
) -> LegacySeedEntry:
    match = _PARTITION_KEY.fullmatch(str(key))
    if match is None:
        raise LegacyBronzeSeedError("legacy checkpoint partition key is invalid")
    dataset, partition_key = match.groups()
    if raw.get("status") != "complete":
        raise LegacyBronzeSeedError("legacy checkpoint partition is not complete")
    if (
        str(raw.get("dataset") or "") != dataset
        or str(raw.get("trade_date") or "") != partition_key
    ):
        raise LegacyBronzeSeedError("legacy checkpoint identity fields disagree")
    # The checkpoint came from Windows.  Parse it as data, never as a host path,
    # and require the exact old layout before constructing the fixed local path.
    declared = PurePosixPath(str(raw.get("path") or "").replace("\\", "/"))
    expected_suffix = PurePosixPath(
        f"artifacts/expanded_long_only/raw_market/{dataset}/"
        f"trade_date={partition_key}/part-000.parquet"
    )
    if declared.is_absolute() or ".." in declared.parts or declared != expected_suffix:
        raise LegacyBronzeSeedError("legacy checkpoint path is not the reviewed layout")
    digest = str(raw.get("sha256") or "").lower()
    if not _SHA256.fullmatch(digest):
        raise LegacyBronzeSeedError("legacy checkpoint SHA-256 is invalid")
    try:
        size_bytes = int(raw["size_bytes"])
        row_count = int(raw["row_count"])
    except (KeyError, TypeError, ValueError) as exc:
        raise LegacyBronzeSeedError("legacy checkpoint size/row count is invalid") from exc
    if size_bytes <= 0 or row_count <= 0:
        raise LegacyBronzeSeedError("legacy seed partitions must be non-empty")
    source = _ensure_regular_file(
        spec.root
        / "raw_market"
        / dataset
        / f"trade_date={partition_key}"
        / "part-000.parquet",
        root=spec.root,
    )
    if source.stat().st_size != size_bytes or sha256_path(source) != digest:
        raise LegacyBronzeSeedError("legacy seed file differs from checkpoint hash/size")
    parquet = pq.ParquetFile(source)
    if parquet.metadata.num_rows != row_count:
        raise LegacyBronzeSeedError("legacy seed row count differs from checkpoint")
    contract = spec.source_config_by_dataset[dataset].get("contract")
    contracted = {
        str(field.get("name") or "")
        for field in (contract.get("fields") if isinstance(contract, Mapping) else ()) or ()
        if isinstance(field, Mapping)
    }
    missing = sorted(contracted - set(parquet.schema.names))
    if missing:
        raise LegacyBronzeSeedError(
            f"legacy seed omits contracted columns: {','.join(missing)}"
        )
    return LegacySeedEntry(
        dataset=dataset,
        partition_key=partition_key,
        source_path=source,
        sha256=digest,
        size_bytes=size_bytes,
        row_count=row_count,
        completed_at=_aware(raw.get("completed_at_utc"), fallback=now),
    )


def assert_snapshot_promotion_allowed(
    catalog: ResearchCatalog,
    snapshot_ids: Sequence[str],
) -> None:
    """Reject a Silver/Gold parent closure tainted by the legacy seed."""

    pending = list(dict.fromkeys(map(str, snapshot_ids)))
    visited: set[str] = set()
    while pending:
        snapshot_id = pending.pop()
        if snapshot_id in visited:
            continue
        visited.add(snapshot_id)
        record = catalog.get_snapshot(snapshot_id)
        if record is None:
            raise SnapshotPromotionBlocked(
                f"snapshot promotion parent is absent: {snapshot_id}"
            )
        reference = record.reference
        labels = set(map(str, reference.trust_labels))
        forbidden = sorted(labels.intersection(FORBIDDEN_PROMOTION_TRUST_LABELS))
        if forbidden:
            raise SnapshotPromotionBlocked(
                f"snapshot {snapshot_id} is non-promotable: {','.join(forbidden)}"
            )
        pending.extend(map(str, reference.parent_snapshot_ids))


class LegacyExpandedBronzeSeeder:
    """Import verified seed bytes and pre-register the real vendor gaps."""

    def __init__(
        self,
        *,
        catalog: ResearchCatalog,
        ledger: ProductionLedger,
        archive: S3ImmutableArchive,
        config: Mapping[str, Any],
        runtime_data_root: str | Path,
        lake_root: str | Path,
        snapshot_root: str | Path,
        environment_hashes: Mapping[str, str],
    ) -> None:
        self.catalog = catalog
        self.ledger = ledger
        self.archive = archive
        self.spec = legacy_bronze_seed_spec(
            config, runtime_data_root=runtime_data_root
        )
        self.lake_root = Path(lake_root)
        self.snapshot_root = Path(snapshot_root)
        self.environment_hashes = dict(environment_hashes)

    def _st_history_audit(self) -> tuple[int, str | None, str]:
        path = self.spec.root / "reference" / "historical_st.parquet"
        if not path.is_file() or path.is_symlink():
            return 0, None, "legacy_st_history_missing"
        try:
            source = _ensure_regular_file(path, root=self.spec.root)
            rows = int(pq.ParquetFile(source).metadata.num_rows)
            digest = sha256_path(source)
        except Exception:
            return 0, None, "legacy_st_history_unreadable"
        if rows == 0:
            return rows, digest, "legacy_st_history_empty"
        return rows, digest, "legacy_st_history_not_pit_verified"

    def _canonical_plan(
        self, sessions: Sequence[str], *, now: datetime
    ) -> tuple[int, int, dict[str, int]]:
        pending = 0
        succeeded = 0
        reason_counts: dict[str, int] = {}
        for dataset in self.spec.datasets:
            for partition_key in sessions:
                if partition_key < "2016-07-07":
                    reason = "missing_2016_prewarm"
                elif dataset in {"daily_basic", "adj_factor"} and partition_key > "2019-04-03":
                    reason = "missing_post_2019_vendor_partition"
                else:
                    reason = "authoritative_vendor_redownload_required"
                record = self.ledger.ensure_partition(
                    PartitionIdentity(
                        self.spec.canonical_source_id, dataset, partition_key
                    ),
                    created_at=now,
                    details={
                        "planned_by": "legacy_expanded_bronze_seed_audit",
                        "reason": reason,
                        "legacy_seed_satisfies_partition": False,
                    },
                )
                if record.status is PartitionStatus.SUCCEEDED:
                    succeeded += 1
                else:
                    pending += 1
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
        return pending, succeeded, reason_counts

    def _verified_terminal(self, identity: PartitionIdentity) -> bool:
        record = self.ledger.get_partition(identity)
        if record is None or record.status is not PartitionStatus.SUCCEEDED:
            return False
        if not record.output_snapshot_id:
            raise LegacyBronzeSeedError("seed ledger terminal has no Bronze snapshot")
        snapshot = self.catalog.get_snapshot(record.output_snapshot_id)
        if snapshot is None:
            raise LegacyBronzeSeedError("seed ledger Bronze snapshot is absent")
        reference = snapshot.reference
        if (
            reference.tier.value != "bronze"
            or reference.quality_status.value != "quarantined"
            or reference.manifest.get("quality_status") != "blocked"
            or not set(LEGACY_SEED_TRUST_LABELS).issubset(reference.trust_labels)
        ):
            raise LegacyBronzeSeedError("seed ledger terminal lost its fail-closed labels")
        return True

    def _publish(self, entry: LegacySeedEntry, *, checkpoint_sha256: str) -> Mapping[str, Any]:
        relative = (
            Path("bronze")
            / LEGACY_SEED_SOURCE_ID
            / entry.dataset
            / f"trade_date={entry.partition_key}"
        )
        data_path = _copy_once(
            entry.source_path,
            self.lake_root / relative / f"sha256={entry.sha256}.parquet",
            sha256=entry.sha256,
            size_bytes=entry.size_bytes,
        )
        source_config = self.spec.source_config_by_dataset[entry.dataset]
        contract = dict(source_config["contract"])
        columns = list(pq.ParquetFile(data_path).schema.names)
        metadata = {
            "schema_version": "research-os/legacy-bronze-seed/v1",
            "source_id": LEGACY_SEED_SOURCE_ID,
            "source_priority": 1_000,
            "dataset": entry.dataset,
            "rows": entry.row_count,
            "columns": columns,
            "ingested_at": entry.completed_at.isoformat(),
            "vendor_revision": entry.sha256,
            "request": {
                "dataset": entry.dataset,
                "parameters": {"trade_date": entry.partition_key.replace("-", "")},
                "fields": list((source_config.get("request") or {}).get("fields") or ()),
            },
            "contract": contract,
            "lineage": {
                "source_artifact": "expanded_long_only",
                "checkpoint_sha256": checkpoint_sha256,
                "data_sha256": entry.sha256,
                "st_history_unverified": True,
                "direct_promotion_allowed": False,
            },
            "data_sha256": entry.sha256,
            "trust_labels": list(LEGACY_SEED_TRUST_LABELS),
        }
        metadata_path = _write_bytes_once(
            data_path.with_suffix(".metadata.json"),
            json.dumps(
                metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8"),
        )
        data_object = self.archive.archive_file(data_path, logical_path=relative.as_posix())
        metadata_object = self.archive.archive_file(
            metadata_path, logical_path=relative.as_posix()
        )
        manifest = build_immutable_snapshot_manifest(
            (data_path, metadata_path),
            base_dir=self.lake_root,
            tier="bronze",
            as_of=entry.completed_at,
            parent_snapshot_ids=(),
            environment_hashes=self.environment_hashes,
            quality_report={
                "status": "blocked",
                "issues": ["st_history_unverified", "legacy_seed_not_vendor_revalidated"],
            },
            trust_labels=LEGACY_SEED_TRUST_LABELS,
        )
        manifest_path = publish_snapshot_manifest(
            self.snapshot_root, manifest, base_dir=self.lake_root
        )
        manifest_object = self.archive.archive_file(
            manifest_path,
            logical_path=f"manifests/bronze-seed/{manifest.snapshot_id}",
        )
        self.catalog.register_snapshot(
            manifest.to_snapshot_ref(uri=manifest_object.uri)
        )
        return {
            "bronze_snapshot_id": manifest.snapshot_id,
            "data_object": data_object.to_dict(),
            "metadata_object": metadata_object.to_dict(),
            "manifest_object": manifest_object.to_dict(),
            "dataset": entry.dataset,
            "partition_key": entry.partition_key,
            "checkpoint_sha256": checkpoint_sha256,
            "trust_labels": list(LEGACY_SEED_TRUST_LABELS),
            "promotion_allowed": False,
        }

    def prepare(self, *, through: date, now: datetime) -> LegacySeedPreparation:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must include a timezone")
        start = date(2016, 6, 1)
        sessions = tuple(
            value
            for value in self.ledger.accepted_calendar_partitions(limit=5_000)
            if start <= date.fromisoformat(value) <= through
        )
        canonical_pending, canonical_succeeded, reason_counts = self._canonical_plan(
            sessions, now=now
        )
        missing_by_dataset = {dataset: len(sessions) for dataset in self.spec.datasets}
        st_rows, st_sha256, st_reason = self._st_history_audit()
        if not self.spec.root.is_dir() or not self.spec.checkpoint_path.is_file():
            return LegacySeedPreparation(
                status="seed_absent_vendor_backfill_pending",
                accepted_session_count=len(sessions),
                canonical_partition_count=len(sessions) * len(self.spec.datasets),
                canonical_pending_count=canonical_pending,
                canonical_succeeded_count=canonical_succeeded,
                seed_candidate_count=0,
                seed_imported_count=0,
                seed_reused_count=0,
                seed_failed_count=0,
                seed_busy_count=0,
                missing_by_dataset=missing_by_dataset,
                pending_reason_counts=reason_counts,
                checkpoint_sha256=None,
                st_history_row_count=st_rows,
                st_history_sha256=st_sha256,
                st_history_reason=st_reason,
            )
        checkpoint_path = _ensure_regular_file(
            self.spec.checkpoint_path, root=self.spec.root
        )
        try:
            checkpoint_bytes = checkpoint_path.read_bytes()
            checkpoint_sha256 = hashlib.sha256(checkpoint_bytes).hexdigest()
            checkpoint = json.loads(checkpoint_bytes.decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise LegacyBronzeSeedError("legacy checkpoint is not valid UTF-8 JSON") from exc
        raw_partitions = checkpoint.get("partitions") if isinstance(checkpoint, Mapping) else None
        if checkpoint.get("schema_version") != 1 or not isinstance(raw_partitions, Mapping):
            raise LegacyBronzeSeedError("legacy checkpoint schema is unsupported")
        run_input_fingerprint = content_fingerprint(
            {
                "schema_version": "research-os/legacy-bronze-seed-run/v1",
                "checkpoint_sha256": checkpoint_sha256,
                "through": through,
                "accepted_sessions": sessions,
                "datasets": self.spec.datasets,
                "canonical_source_id": self.spec.canonical_source_id,
                "trust_labels": LEGACY_SEED_TRUST_LABELS,
                "environment_hashes": self.environment_hashes,
            },
            domain="factor-lab/research-os/v1/legacy-bronze-seed-run-input",
        )
        run_metadata = {
            "schema_version": "research-os/legacy-bronze-seed-run/v1",
            "root_input_fingerprint": run_input_fingerprint,
            "checkpoint_sha256": checkpoint_sha256,
            "through": through.isoformat(),
            "accepted_session_count": len(sessions),
            "accepted_sessions_hash": content_fingerprint(
                sessions,
                domain="factor-lab/research-os/v1/legacy-bronze-seed-sessions",
            ),
            "datasets": list(self.spec.datasets),
            "canonical_source_id": self.spec.canonical_source_id,
            "trust_labels": list(LEGACY_SEED_TRUST_LABELS),
        }
        base_run_id = f"legacy_seed_{run_input_fingerprint[:48]}"
        related_runs = tuple(
            run
            for run in self.catalog.list_runs(
                limit=1_000, run_type="legacy_bronze_seed_import"
            )
            if run.run_id == base_run_id
            or run.metadata.get("root_input_fingerprint") == run_input_fingerprint
        )
        succeeded_runs = tuple(
            run for run in related_runs if run.status == "succeeded"
        )
        running_runs = tuple(run for run in related_runs if run.status == "running")
        terminal_replay = bool(succeeded_runs)
        if succeeded_runs:
            seed_run = max(succeeded_runs, key=lambda run: (run.started_at, run.run_id))
            _claimed = False
        elif running_runs:
            seed_run = max(running_runs, key=lambda run: (run.started_at, run.run_id))
            _claimed = False
        else:
            attempt_generation = len(related_runs) + 1
            attempt_run_id = (
                base_run_id
                if attempt_generation == 1
                else (
                    f"legacy_seed_{run_input_fingerprint[:40]}_"
                    f"a{attempt_generation:04d}"
                )
            )
            attempt_fingerprint = (
                run_input_fingerprint
                if attempt_generation == 1
                else content_fingerprint(
                    {
                        "root_input_fingerprint": run_input_fingerprint,
                        "attempt_generation": attempt_generation,
                    },
                    domain=(
                        "factor-lab/research-os/v1/"
                        "legacy-bronze-seed-attempt-input"
                    ),
                )
            )
            seed_run, _claimed = self.catalog.claim_run(
                RunRecord(
                    run_id=attempt_run_id,
                    run_type="legacy_bronze_seed_import",
                    status="running",
                    input_fingerprint=attempt_fingerprint,
                    started_at=now,
                    metadata={
                        **run_metadata,
                        "attempt_generation": attempt_generation,
                    },
                )
            )
        imported = reused = failed = busy = candidates = 0
        accepted = set(sessions)
        for key in sorted(raw_partitions):
            match = _PARTITION_KEY.fullmatch(str(key))
            if match is None:
                # Unknown keys cannot be used as evidence, but also cannot make
                # the vendor-redownload plan disappear.
                failed += 1
                continue
            dataset, partition_key = match.groups()
            if dataset not in self.spec.datasets or partition_key not in accepted:
                continue
            candidates += 1
            identity = PartitionIdentity(
                LEGACY_SEED_SOURCE_ID, f"bronze-seed-{dataset}", partition_key
            )
            raw = raw_partitions[key]
            if not isinstance(raw, Mapping):
                raw = {}
            input_hash = content_fingerprint(
                {
                    "checkpoint_sha256": checkpoint_sha256,
                    "partition_key": key,
                    "checkpoint_entry": dict(raw),
                    "trust_labels": LEGACY_SEED_TRUST_LABELS,
                },
                domain="factor-lab/research-os/v1/legacy-bronze-seed-input",
            )
            try:
                record = self.ledger.ensure_partition(
                    identity,
                    created_at=now,
                    input_hash=input_hash,
                    details={
                        "stage": "bronze_seed",
                        "promotion_allowed": False,
                        "trust_labels": list(LEGACY_SEED_TRUST_LABELS),
                    },
                )
                if record.status is PartitionStatus.SUCCEEDED:
                    self._verified_terminal(identity)
                    missing_by_dataset[dataset] = max(
                        0, missing_by_dataset[dataset] - 1
                    )
                    reused += 1
                    continue
                lease = self.ledger.claim(
                    identity=identity,
                    owner="legacy-seed-importer",
                    now=now,
                    lease_for=timedelta(hours=1),
                )
                if lease is None:
                    busy += 1
                    continue
                try:
                    entry = _checkpoint_entry(
                        self.spec, key=str(key), raw=raw, now=now
                    )
                    output = self._publish(
                        entry, checkpoint_sha256=checkpoint_sha256
                    )
                    self.ledger.finish(
                        lease,
                        status=PartitionStatus.SUCCEEDED,
                        completed_at=now,
                        run_id=seed_run.run_id,
                        output_snapshot_id=str(output["bronze_snapshot_id"]),
                        output_hash=content_fingerprint(
                            output,
                            domain="factor-lab/research-os/v1/legacy-bronze-seed-output",
                        ),
                        vendor_revision=entry.sha256,
                        details={
                            "stage": "bronze_seed",
                            "seed_output": output,
                            "promotion_allowed": False,
                            "trust_labels": list(LEGACY_SEED_TRUST_LABELS),
                        },
                    )
                    missing_by_dataset[dataset] = max(
                        0, missing_by_dataset[dataset] - 1
                    )
                    imported += 1
                except Exception as exc:
                    try:
                        self.ledger.finish(
                            lease,
                            status=PartitionStatus.FAILED,
                            completed_at=now,
                            run_id=seed_run.run_id,
                            error_code="legacy_seed_verification_failed",
                            error=f"{type(exc).__name__}: {exc}",
                            details={
                                "stage": "bronze_seed",
                                "promotion_allowed": False,
                                "trust_labels": list(LEGACY_SEED_TRUST_LABELS),
                            },
                        )
                    except ProductionLedgerError:
                        pass
                    failed += 1
            except Exception:
                failed += 1
        status = (
            "seed_audit_failed_vendor_backfill_pending"
            if failed
            else "seed_import_in_progress"
            if busy
            else "seed_bronze_only_vendor_backfill_pending"
        )
        terminal_metadata = {
            **seed_run.metadata,
            "preparation_status": status,
            "seed_candidate_count": candidates,
            "seed_imported_count": imported,
            "seed_reused_count": reused,
            "seed_failed_count": failed,
            "seed_busy_count": busy,
        }
        if terminal_replay:
            # A deterministic successful import is immutable.  Re-verify and
            # report its child partitions, but never rewrite its completion
            # time or first-attempt metrics during an idempotent replay.
            pass
        elif busy and not failed:
            self.catalog.save_run(
                replace(
                    seed_run,
                    status="running",
                    metadata=terminal_metadata,
                    completed_at=None,
                    error=None,
                )
            )
        else:
            self.catalog.save_run(
                replace(
                    seed_run,
                    status=("failed" if failed else "succeeded"),
                    metadata=terminal_metadata,
                    completed_at=now,
                    error=("legacy seed import failed closed" if failed else None),
                )
            )
        return LegacySeedPreparation(
            status=status,
            accepted_session_count=len(sessions),
            canonical_partition_count=len(sessions) * len(self.spec.datasets),
            canonical_pending_count=canonical_pending,
            canonical_succeeded_count=canonical_succeeded,
            seed_candidate_count=candidates,
            seed_imported_count=imported,
            seed_reused_count=reused,
            seed_failed_count=failed,
            seed_busy_count=busy,
            missing_by_dataset=missing_by_dataset,
            pending_reason_counts=reason_counts,
            checkpoint_sha256=checkpoint_sha256,
            st_history_row_count=st_rows,
            st_history_sha256=st_sha256,
            st_history_reason=st_reason,
        )


__all__ = [
    "FORBIDDEN_PROMOTION_TRUST_LABELS",
    "LEGACY_SEED_CONFIG_MODE",
    "LEGACY_SEED_DATASETS",
    "LEGACY_SEED_PROMOTION_POLICY",
    "LEGACY_SEED_ROOT_RELATIVE",
    "LEGACY_SEED_SOURCE_ID",
    "LEGACY_SEED_TRUST_LABELS",
    "LegacyBronzeSeedError",
    "LegacyBronzeSeedSpec",
    "LegacyExpandedBronzeSeeder",
    "LegacySeedEntry",
    "LegacySeedPreparation",
    "SnapshotPromotionBlocked",
    "assert_snapshot_promotion_allowed",
    "legacy_bronze_seed_spec",
]
