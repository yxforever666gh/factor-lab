"""One-way inventory/import of pre-Research-OS evidence.

Legacy artifacts are preserved for audit and numerical regression only.  They
are never converted into promotion-eligible experiments and never populate the
new materialized read model as current state.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import stat
from typing import Iterable

from .catalog import LegacyEvidenceRecord, ResearchCatalog
from .data_quality import sha256_path


LEGACY_EVIDENCE_NAMES = frozenset(
    {
        "factor_lab.db",
        "task_state.json",
        "results.json",
        "three_round_manifest.json",
        "three_round_comparison.json",
        "data_snapshot_manifest.json",
        "data_audit.json",
        "reference_data_status.json",
        "historical_st_status.json",
        "historical_st.parquet",
        "experiment_ledger.json",
    }
)


@dataclass(frozen=True)
class LegacyImportReport:
    root: str
    discovered: int
    imported: int
    trust_labels: dict[str, int]
    sealed_sqlite: tuple[str, ...]

    def to_dict(self) -> dict:
        return asdict(self)


def _classification(path: Path) -> tuple[str, tuple[str, ...]]:
    normalized = path.as_posix().lower()
    if path.name == "factor_lab.db":
        return (
            "legacy_sqlite_read_only",
            (
                "pre_research_os_schema",
                "not_authoritative_for_new_experiments",
            ),
        )
    if path.name in {"historical_st.parquet", "historical_st_status.json"}:
        return (
            "st_history_unverified",
            (
                "cached_historical_st_is_empty_or_unverified",
                "promotion_forbidden",
            ),
        )
    if "expanded_long_only" in normalized:
        return (
            "legacy_execution_regression_only",
            (
                "historical_st_unverified",
                "all_history_already_observed",
                "not_in_unified_trial_ledger",
            ),
        )
    return (
        "legacy_untrusted_data",
        (
            "pre_research_os_evidence",
            "not_replayable_from_authoritative_snapshot",
        ),
    )


def discover_legacy_evidence(root: str | Path) -> tuple[Path, ...]:
    base = Path(root).resolve()
    if not base.is_dir():
        raise NotADirectoryError(base)
    result = [
        path.resolve()
        for path in base.rglob("*")
        if path.is_file() and not path.is_symlink() and path.name in LEGACY_EVIDENCE_NAMES
    ]
    return tuple(sorted(set(result), key=lambda item: item.as_posix()))


def import_legacy_evidence(
    catalog: ResearchCatalog,
    root: str | Path,
    *,
    seal_sqlite: bool = False,
    imported_at: datetime | None = None,
) -> LegacyImportReport:
    base = Path(root).resolve()
    discovered = discover_legacy_evidence(base)
    timestamp = imported_at or datetime.now(timezone.utc)
    if timestamp.tzinfo is None:
        raise ValueError("imported_at must be timezone-aware")
    counts: dict[str, int] = {}
    imported = 0
    sqlite_paths: list[Path] = []
    for path in discovered:
        trust_label, reasons = _classification(path)
        relative = path.relative_to(base).as_posix()
        catalog.import_legacy_evidence(
            LegacyEvidenceRecord(
                source_uri=f"legacy://{relative}",
                content_hash=sha256_path(path),
                trust_label=trust_label,
                reasons=reasons,
                imported_at=timestamp,
            )
        )
        imported += 1
        counts[trust_label] = counts.get(trust_label, 0) + 1
        if path.name == "factor_lab.db":
            sqlite_paths.append(path)
    sealed: list[str] = []
    if seal_sqlite:
        for path in sqlite_paths:
            mode = path.stat().st_mode
            os.chmod(path, mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))
            if os.access(path, os.W_OK):
                # Windows ACLs can report writable after chmod; opening the DB
                # in immutable mode remains mandatory in compatibility code.
                pass
            sealed.append(str(path))
    return LegacyImportReport(
        root=str(base),
        discovered=len(discovered),
        imported=imported,
        trust_labels=counts,
        sealed_sqlite=tuple(sealed),
    )


__all__ = [
    "LEGACY_EVIDENCE_NAMES",
    "LegacyImportReport",
    "discover_legacy_evidence",
    "import_legacy_evidence",
]
