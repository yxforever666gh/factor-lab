"""Physical MinIO cache-restore evidence derived from a physical canary.

The operation deliberately has no object/path arguments.  It selects one
immutable Gold mark object from the newest fresh, hash-valid physical canary,
hydrates it into a service-owned ephemeral cache, deletes that cache entry,
and hydrates it again.  Only the second verified download may produce the
``minio_restore_drill`` run consumed by the production readiness auditor.

Controlled tests exercise the exact byte path with an in-memory object store,
but persist a distinct rejected run type and ``physical=false``.  Consequently
test evidence cannot be mistaken for a real MinIO recovery drill.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Callable, Mapping, Sequence
from uuid import uuid4

from .catalog import ResearchCatalog, RunRecord
from .canary_authority import physical_canary_session_errors
from .contracts import DataQualityStatus, SnapshotTier
from .fingerprint import content_fingerprint
from .object_store import S3ImmutableArchive
from .physical_canary import CANARY_OBJECT_PREFIX
from .production_config import ProductionConfigEvidence, validate_production_config
from .readiness_audit import (
    PHYSICAL_CANARY_RUN_TYPE,
    PHYSICAL_CANARY_SCHEMA_VERSION,
    RESTORE_DRILL_RUN_TYPE,
    RESTORE_DRILL_SCHEMA_VERSION,
    physical_canary_evidence_hash,
    restore_drill_evidence_hash,
)


CONTROLLED_TEST_RUN_TYPE = "minio_restore_drill_test"
READINESS_ADMISSION = "physical_minio_restore_drill"
CONTROLLED_TEST_REJECTION = "rejected_controlled_test_object_store"
_MAXIMUM_CANARY_AGE = timedelta(hours=24)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_PHYSICAL_TRUST_LABELS = {
    "physical_engineering_canary",
    "retrospective_non_forward",
    "retrospective_physical_replay",
}


class RestoreDrillError(RuntimeError):
    """A physical cache recovery could not be established."""


class RestoreDrillAdmissionError(RestoreDrillError):
    """The runtime is not a PostgreSQL + real MinIO production authority."""


class RestoreDrillEvidenceUnavailable(RestoreDrillError):
    """No fresh hash-valid physical canary object is available to restore."""


@dataclass(frozen=True)
class RestoreDrillResult:
    run_id: str
    run_type: str
    physical: bool
    readiness_admission: str
    restore_evidence_hash: str
    source_canary_run_id: str
    source_canary_evidence_hash: str
    source_canary_execution_contract_hash: str
    source_snapshot_id: str
    object_uri: str
    sha256: str
    size_bytes: int
    restored_twice: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class _SelectedObject:
    canary_run_id: str
    canary_evidence_hash: str
    canary_execution_contract_hash: str
    snapshot_id: str
    object_uri: str
    sha256: str
    size_bytes: int
    trade_date: str
    role: str


def _aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise RestoreDrillError(f"{field_name} must include a timezone")
    return value.astimezone(timezone.utc)


def _valid_hash(value: Any) -> bool:
    return bool(_SHA256.fullmatch(str(value or "")))


def _as_sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        return value
    return ()


class PhysicalMinioRestoreDrillService:
    """Run a code-selected, twice-downloaded MinIO cache recovery drill."""

    def __init__(
        self,
        *,
        catalog: ResearchCatalog,
        object_store_archive: S3ImmutableArchive,
        production_evidence: ProductionConfigEvidence | None,
        controlled_test: bool,
    ) -> None:
        self.catalog = catalog
        self.archive = S3ImmutableArchive(
            bucket=object_store_archive.bucket,
            filesystem=object_store_archive.filesystem,
            prefix=CANARY_OBJECT_PREFIX,
        )
        self.production_evidence = production_evidence
        self.controlled_test = bool(controlled_test)
        self.expected_canary_execution_contract_hash = (
            None
            if self.controlled_test
            else str(
                getattr(
                    production_evidence,
                    "engineering_canary_execution_contract_hash",
                    "",
                )
                or ""
            )
        )

    @classmethod
    def from_production_config(
        cls,
        config_path: str | Path,
        *,
        env: Mapping[str, str],
        catalog: ResearchCatalog,
        object_store_archive: S3ImmutableArchive,
        require_mounts: bool = True,
        mount_checker: Callable[[Path], bool] | None = None,
        image_reference: str | None = None,
    ) -> "PhysicalMinioRestoreDrillService":
        """Construct the production operation from reviewed configuration.

        ``config_path`` selects the immutable service configuration, never an
        object to restore.  The operation itself accepts no URI, digest, local
        cache path, or caller-authored evidence.
        """

        evidence = validate_production_config(
            config_path,
            env=env,
            require_mounts=require_mounts,
            mount_checker=mount_checker,
            image_reference=image_reference,
        )
        return cls(
            catalog=catalog,
            object_store_archive=object_store_archive,
            production_evidence=evidence,
            controlled_test=False,
        )

    @classmethod
    def for_controlled_test(
        cls,
        *,
        catalog: ResearchCatalog,
        object_store_archive: S3ImmutableArchive,
    ) -> "PhysicalMinioRestoreDrillService":
        """Exercise recovery bytes without producing readiness evidence."""

        return cls(
            catalog=catalog,
            object_store_archive=object_store_archive,
            production_evidence=None,
            controlled_test=True,
        )

    def _assert_runtime_admission(self) -> None:
        if self.controlled_test:
            return
        if self.production_evidence is None:
            raise RestoreDrillAdmissionError(
                "validated production configuration evidence is absent"
            )
        if not _valid_hash(self.expected_canary_execution_contract_hash):
            raise RestoreDrillAdmissionError(
                "validated engineering canary execution contract hash is absent"
            )
        backend = getattr(self.catalog, "_backend", None)
        engine = getattr(backend, "_engine", None)
        if getattr(getattr(engine, "dialect", None), "name", None) != "postgresql":
            raise RestoreDrillAdmissionError(
                "physical restore drill requires the PostgreSQL Research OS catalog"
            )
        if type(self.archive.filesystem).__module__.split(".", 1)[0] != "s3fs":
            raise RestoreDrillAdmissionError(
                "physical restore drill requires a real S3/MinIO filesystem"
            )

    def _database_now(self) -> datetime:
        return _aware(self.catalog.database_now(), field_name="database_now")

    def _valid_canary_contract(self, run: RunRecord, *, observed_at: datetime) -> bool:
        metadata = run.metadata
        completed_at = run.completed_at
        if completed_at is None:
            return False
        completed_at = _aware(completed_at, field_name="canary.completed_at")
        age = observed_at - completed_at
        expected_hash = physical_canary_evidence_hash(metadata)
        raw_evidence = _as_sequence(metadata.get("snapshot_evidence"))
        tier_counts = {
            tier: sum(
                isinstance(item, Mapping) and str(item.get("tier") or "") == tier
                for item in raw_evidence
            )
            for tier in ("bronze", "silver", "gold")
        }
        sessions = tuple(map(str, _as_sequence(metadata.get("calendar_sessions"))))
        source_probe_hashes = metadata.get("source_probe_hashes")
        canary_execution_contract_hash = str(
            metadata.get("canary_execution_contract_hash") or ""
        )
        evaluator_identity = metadata.get("evaluator_identity")
        current_contract_matches = bool(
            self.controlled_test
            or canary_execution_contract_hash
            == self.expected_canary_execution_contract_hash
        )
        shadow_session_hashes = tuple(
            map(str, _as_sequence(metadata.get("shadow_session_hashes")))
        )
        shadow_event_hashes = tuple(
            map(str, _as_sequence(metadata.get("shadow_account_event_hashes")))
        )
        authoritative_sessions_valid = True
        if not self.controlled_test:
            engine = getattr(getattr(self.catalog, "_backend", None), "_engine", None)
            try:
                session_dates = tuple(date.fromisoformat(item) for item in sessions)
            except ValueError:
                authoritative_sessions_valid = False
            else:
                authoritative_sessions_valid = bool(
                    engine is not None
                    and not physical_canary_session_errors(
                        engine,
                        metadata,
                        sessions=session_dates,
                    )
                )
        return bool(
            run.run_type == PHYSICAL_CANARY_RUN_TYPE
            and run.status == "succeeded"
            and timedelta(0) <= age <= _MAXIMUM_CANARY_AGE
            and metadata.get("evidence_schema") == PHYSICAL_CANARY_SCHEMA_VERSION
            and metadata.get("evidence_class") == "engineering_canary"
            and metadata.get("evidence_scope") == "retrospective_non_forward"
            and _valid_hash(canary_execution_contract_hash)
            and current_contract_matches
            and (
                self.controlled_test
                or (
                    isinstance(evaluator_identity, Mapping)
                    and evaluator_identity.get("canary_execution_contract_hash")
                    == canary_execution_contract_hash
                )
            )
            and metadata.get("formal_epoch_eligible") is False
            and metadata.get("physical_source_attested") is True
            and metadata.get("controlled_test_adapter") is False
            and metadata.get("readiness_admission")
            == "physical_engineering_prerequisite"
            and metadata.get("run_id") == run.run_id
            and metadata.get("run_type") == run.run_type
            and metadata.get("input_fingerprint") == run.input_fingerprint
            and _valid_hash(run.input_fingerprint)
            and metadata.get("canary_evidence_hash") == expected_hash
            and int(metadata.get("security_count") or 0) == 50
            and int(metadata.get("projected_session_count") or 0) == 20
            and metadata.get("sleeve_state") == "shadow"
            and len(sessions) == len(set(sessions)) == 21
            and tuple(sorted(sessions)) == sessions
            and isinstance(source_probe_hashes, Mapping)
            and bool(source_probe_hashes)
            and all(_valid_hash(item) for item in source_probe_hashes.values())
            and len(shadow_session_hashes) == 20
            and len(set(shadow_session_hashes)) == 20
            and all(_valid_hash(item) for item in shadow_session_hashes)
            and len(shadow_event_hashes) == 20
            and all(_valid_hash(item) for item in shadow_event_hashes)
            and len(raw_evidence) > 0
            and int(metadata.get("physical_object_count") or 0) == len(raw_evidence)
            and int(metadata.get("bronze_object_count") or 0)
            == tier_counts["bronze"]
            and int(metadata.get("silver_object_count") or 0)
            == tier_counts["silver"]
            and int(metadata.get("gold_object_count") or 0) == tier_counts["gold"]
            and authoritative_sessions_valid
        )

    def _validated_selected_object(
        self,
        *,
        run: RunRecord,
        evidence: Mapping[str, Any],
    ) -> _SelectedObject:
        snapshot_id = str(evidence.get("snapshot_id") or "")
        object_uri = str(evidence.get("uri") or "")
        object_sha = str(evidence.get("object_sha256") or "")
        try:
            size_bytes = int(evidence.get("size_bytes") or 0)
        except (TypeError, ValueError) as exc:
            raise RestoreDrillEvidenceUnavailable(
                "physical canary object size is invalid"
            ) from exc
        record = self.catalog.get_snapshot(snapshot_id)
        if record is None:
            raise RestoreDrillEvidenceUnavailable(
                "physical canary snapshot is absent from the catalog"
            )
        reference = record.reference
        manifest = reference.manifest
        canary_execution_contract_hash = str(
            run.metadata.get("canary_execution_contract_hash") or ""
        )
        physical_object = manifest.get("physical_object")
        if not isinstance(physical_object, Mapping):
            raise RestoreDrillEvidenceUnavailable(
                "physical canary snapshot lacks immutable object evidence"
            )
        try:
            physical_size_bytes = int(physical_object.get("size_bytes") or 0)
        except (TypeError, ValueError) as exc:
            raise RestoreDrillEvidenceUnavailable(
                "physical canary snapshot object size is invalid"
            ) from exc
        expected_content_hash = content_fingerprint(
            manifest,
            domain="factor-lab/research-os/v1/physical-canary-snapshot",
        )
        role = str(evidence.get("role") or "")
        trade_date = str(evidence.get("trade_date") or "")
        if not (
            reference.quality_status is DataQualityStatus.ACCEPTED
            and reference.tier is SnapshotTier.GOLD
            and reference.snapshot_id == snapshot_id
            and reference.uri == object_uri
            and reference.content_hash == expected_content_hash
            and str(evidence.get("content_hash") or "") == expected_content_hash
            and str(evidence.get("tier") or "") == SnapshotTier.GOLD.value
            and role == "mark"
            and bool(trade_date)
            and object_uri.startswith(f"s3://{self.archive.bucket}/")
            and f"/sha256={object_sha}/" in object_uri
            and _valid_hash(object_sha)
            and size_bytes > 0
            and str(physical_object.get("uri") or "") == object_uri
            and str(physical_object.get("sha256") or "") == object_sha
            and physical_size_bytes == size_bytes
            and manifest.get("run_id") == run.run_id
            and manifest.get("evidence_schema") == PHYSICAL_CANARY_SCHEMA_VERSION
            and manifest.get("evidence_class") == "engineering_canary"
            and manifest.get("evidence_scope") == "retrospective_non_forward"
            and manifest.get("canary_execution_contract_hash")
            == canary_execution_contract_hash
            and _valid_hash(canary_execution_contract_hash)
            and (
                self.controlled_test
                or canary_execution_contract_hash
                == self.expected_canary_execution_contract_hash
            )
            and manifest.get("formal_epoch_eligible") is False
            and manifest.get("physical_source_attested") is True
            and manifest.get("controlled_test_adapter") is False
            and manifest.get("readiness_admission")
            == "physical_engineering_prerequisite"
            and str(manifest.get("tier") or "") == SnapshotTier.GOLD.value
            and str(manifest.get("role") or "") == role
            and str(manifest.get("trade_date") or "") == trade_date
            and set(reference.trust_labels) == _PHYSICAL_TRUST_LABELS
        ):
            raise RestoreDrillEvidenceUnavailable(
                "physical canary Gold mark object contract is invalid"
            )
        return _SelectedObject(
            canary_run_id=run.run_id,
            canary_evidence_hash=str(run.metadata["canary_evidence_hash"]),
            canary_execution_contract_hash=canary_execution_contract_hash,
            snapshot_id=snapshot_id,
            object_uri=object_uri,
            sha256=object_sha,
            size_bytes=size_bytes,
            trade_date=trade_date,
            role=role,
        )

    def _select_object(self, *, observed_at: datetime) -> _SelectedObject:
        runs = self.catalog.list_runs(
            limit=1_000,
            status="succeeded",
            run_type=PHYSICAL_CANARY_RUN_TYPE,
        )
        runs.sort(
            key=lambda item: (
                item.completed_at or item.started_at,
                item.started_at,
                item.run_id,
            ),
            reverse=True,
        )
        for run in runs:
            try:
                valid_contract = self._valid_canary_contract(
                    run, observed_at=observed_at
                )
            except (KeyError, TypeError, ValueError, RestoreDrillError):
                valid_contract = False
            if not valid_contract:
                continue
            raw_evidence = _as_sequence(run.metadata.get("snapshot_evidence"))
            candidates = sorted(
                (
                    item
                    for item in raw_evidence
                    if isinstance(item, Mapping)
                    and str(item.get("tier") or "") == SnapshotTier.GOLD.value
                    and str(item.get("role") or "") == "mark"
                ),
                key=lambda item: (
                    str(item.get("trade_date") or ""),
                    str(item.get("snapshot_id") or ""),
                ),
                reverse=True,
            )
            for evidence in candidates:
                try:
                    return self._validated_selected_object(run=run, evidence=evidence)
                except (RestoreDrillEvidenceUnavailable, TypeError, ValueError):
                    continue
        raise RestoreDrillEvidenceUnavailable(
            "no fresh hash-valid physical canary Gold mark object is available"
        )

    def _cache_parent(self) -> Path | None:
        if self.controlled_test:
            return None
        evidence = self.production_evidence
        if evidence is None:  # already rejected by admission; retain fail-closed typing.
            raise RestoreDrillAdmissionError(
                "validated production configuration evidence is absent"
            )
        cache_parent = evidence.runtime_data_root / "restore-drill-cache"
        cache_parent.mkdir(parents=True, exist_ok=True)
        return cache_parent

    def run(self) -> RestoreDrillResult:
        """Restore one code-selected canary object twice and persist evidence."""

        self._assert_runtime_admission()
        started_at = self._database_now()
        selected = self._select_object(observed_at=started_at)
        challenge = content_fingerprint(
            {
                "source_canary_run_id": selected.canary_run_id,
                "source_canary_execution_contract_hash": (
                    selected.canary_execution_contract_hash
                ),
                "source_snapshot_id": selected.snapshot_id,
                "object_sha256": selected.sha256,
                "started_at": started_at,
                "nonce": uuid4().hex,
            },
            domain="factor-lab/research-os/v1/restore-drill-deletion-challenge",
        )

        with tempfile.TemporaryDirectory(
            prefix=".factor-lab-restore-drill-",
            dir=self._cache_parent(),
        ) as temporary_directory:
            destination = Path(temporary_directory) / "immutable-object.cache"
            first = self.archive.restore_file(
                selected.object_uri,
                destination,
                expected_sha256=selected.sha256,
                expected_size_bytes=selected.size_bytes,
            )
            if (
                first.reused
                or first.sha256 != selected.sha256
                or first.size_bytes != selected.size_bytes
                or not destination.is_file()
            ):
                raise RestoreDrillError("initial physical cache hydration is invalid")
            destination.unlink()
            if os.path.lexists(destination):
                raise RestoreDrillError("controlled cache deletion could not be verified")
            deleted_cache_proof = content_fingerprint(
                {
                    "deletion_challenge": challenge,
                    "source_canary_evidence_hash": selected.canary_evidence_hash,
                    "source_canary_execution_contract_hash": (
                        selected.canary_execution_contract_hash
                    ),
                    "source_snapshot_id": selected.snapshot_id,
                    "object_sha256": selected.sha256,
                    "size_bytes": selected.size_bytes,
                    "first_restore_sha256": first.sha256,
                    "first_restore_size_bytes": first.size_bytes,
                    "cache_existed_after_first_restore": True,
                    "cache_absent_before_second_restore": True,
                },
                domain="factor-lab/research-os/v1/restore-drill-deleted-cache-proof",
            )
            second = self.archive.restore_file(
                selected.object_uri,
                destination,
                expected_sha256=selected.sha256,
                expected_size_bytes=selected.size_bytes,
            )
            if (
                second.reused
                or second.sha256 != selected.sha256
                or second.size_bytes != selected.size_bytes
                or not destination.is_file()
            ):
                raise RestoreDrillError("second physical cache hydration is invalid")

        completed_at = self._database_now()
        if completed_at < started_at:
            raise RestoreDrillError("database clock moved backwards during restore drill")
        physical = not self.controlled_test
        run_type = RESTORE_DRILL_RUN_TYPE if physical else CONTROLLED_TEST_RUN_TYPE
        readiness_admission = (
            READINESS_ADMISSION if physical else CONTROLLED_TEST_REJECTION
        )
        metadata = {
            "schema_version": RESTORE_DRILL_SCHEMA_VERSION,
            "authority": "code_selected_physical_canary_twice_hydrated",
            "physical": physical,
            "controlled_test_object_store": self.controlled_test,
            "readiness_admission": readiness_admission,
            "object_uri": selected.object_uri,
            "expected_sha256": selected.sha256,
            "restored_sha256": second.sha256,
            "expected_size_bytes": selected.size_bytes,
            "restored_size_bytes": second.size_bytes,
            "deleted_cache_proof": deleted_cache_proof,
            "deletion_challenge": challenge,
            "cache_deleted_before_second_restore": True,
            "first_restore_sha256": first.sha256,
            "first_restore_size_bytes": first.size_bytes,
            "second_restore_downloaded": not second.reused,
            "local_cache_retained": False,
            "source_canary_run_id": selected.canary_run_id,
            "source_canary_evidence_hash": selected.canary_evidence_hash,
            "source_canary_execution_contract_hash": (
                selected.canary_execution_contract_hash
            ),
            "source_snapshot_id": selected.snapshot_id,
            "source_snapshot_role": selected.role,
            "source_snapshot_trade_date": selected.trade_date,
            "verified_at": completed_at.isoformat(),
        }
        evidence_hash = restore_drill_evidence_hash(metadata)
        metadata["restore_evidence_hash"] = evidence_hash
        proposed = RunRecord(
            run_id=f"restore_{evidence_hash}",
            run_type=run_type,
            status="succeeded",
            input_fingerprint=evidence_hash,
            started_at=started_at,
            completed_at=completed_at,
            metadata=metadata,
        )
        stored, won = self.catalog.claim_run(proposed)
        if not won and stored != proposed:
            raise RestoreDrillError(
                "restore drill identity collided with different persisted evidence"
            )
        return RestoreDrillResult(
            run_id=stored.run_id,
            run_type=stored.run_type,
            physical=physical,
            readiness_admission=readiness_admission,
            restore_evidence_hash=evidence_hash,
            source_canary_run_id=selected.canary_run_id,
            source_canary_evidence_hash=selected.canary_evidence_hash,
            source_canary_execution_contract_hash=(
                selected.canary_execution_contract_hash
            ),
            source_snapshot_id=selected.snapshot_id,
            object_uri=selected.object_uri,
            sha256=selected.sha256,
            size_bytes=selected.size_bytes,
            restored_twice=True,
        )


__all__ = [
    "CONTROLLED_TEST_REJECTION",
    "CONTROLLED_TEST_RUN_TYPE",
    "PhysicalMinioRestoreDrillService",
    "READINESS_ADMISSION",
    "RestoreDrillAdmissionError",
    "RestoreDrillError",
    "RestoreDrillEvidenceUnavailable",
    "RestoreDrillResult",
]
