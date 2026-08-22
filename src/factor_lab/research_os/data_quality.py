"""Fail-closed data quality gates used before research can be promotable."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from collections import Counter
from enum import Enum
import hashlib
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from .data_sources import DatasetContract


class QualitySeverity(str, Enum):
    BLOCKING = "blocking"
    WARNING = "warning"


@dataclass(frozen=True)
class QualityIssue:
    code: str
    message: str
    severity: QualitySeverity = QualitySeverity.BLOCKING
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["severity"] = self.severity.value
        return payload


@dataclass(frozen=True)
class QualityReport:
    issues: tuple[QualityIssue, ...]
    checks: tuple[str, ...]

    @property
    def status(self) -> str:
        if any(issue.severity is QualitySeverity.BLOCKING for issue in self.issues):
            return "blocked"
        if self.issues:
            return "warning"
        return "pass"

    @property
    def promotion_allowed(self) -> bool:
        return self.status != "blocked"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "research-os/data-quality/v1",
            "status": self.status,
            "promotion_allowed": self.promotion_allowed,
            "checks": list(self.checks),
            "issues": [issue.to_dict() for issue in self.issues],
        }


class DataQualityError(RuntimeError):
    def __init__(self, report: QualityReport):
        self.report = report
        codes = [issue.code for issue in report.issues if issue.severity is QualitySeverity.BLOCKING]
        super().__init__(f"data quality gate blocked promotion: {codes}")


def sha256_path(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def is_probable_mojibake(value: Any) -> bool:
    """Detect lossy replacement characters and common UTF-8/Latin-1 corruption."""

    if value is None or pd.isna(value):
        return False
    text = str(value)
    if not text:
        return False
    if "\ufffd" in text or any(ord(character) < 32 and character not in "\t\r\n" for character in text):
        return True
    cjk_before = sum("\u3400" <= character <= "\u9fff" for character in text)
    try:
        repaired = text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        repaired = ""
    cjk_after = sum("\u3400" <= character <= "\u9fff" for character in repaired)
    classic_markers = len(re.findall(r"[ÃÂåæçèéäöü]{1,}", text))
    return bool(repaired and cjk_after > cjk_before and classic_markers)


class DataQualityGate:
    def __init__(self) -> None:
        self._issues: list[QualityIssue] = []
        self._checks: list[str] = []

    def _record_check(self, name: str) -> None:
        if name not in self._checks:
            self._checks.append(name)

    def add_issue(
        self,
        code: str,
        message: str,
        *,
        severity: QualitySeverity = QualitySeverity.BLOCKING,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        self._issues.append(
            QualityIssue(code=code, message=message, severity=severity, details=dict(details or {}))
        )

    def check_dataframe(
        self,
        frame: pd.DataFrame,
        contract: DatasetContract,
        *,
        chinese_text_columns: Sequence[str] = (),
    ) -> "DataQualityGate":
        self._record_check(f"dataset:{contract.dataset}")
        missing = sorted(set(contract.field_map) - set(frame.columns))
        if missing:
            self.add_issue(
                "missing_required_columns",
                f"{contract.dataset} omits contracted fields",
                details={"dataset": contract.dataset, "columns": missing},
            )
            return self
        if frame.empty and not contract.allows_empty:
            self.add_issue(
                "empty_required_dataset",
                f"{contract.dataset} contains no rows",
                details={"dataset": contract.dataset},
            )
        null_keys = {
            column: int(frame[column].isna().sum())
            for column in contract.key_fields
            if frame[column].isna().any()
        }
        if null_keys:
            self.add_issue(
                "null_dataset_keys",
                f"{contract.dataset} contains null keys",
                details={"dataset": contract.dataset, "counts": null_keys},
            )
        if not frame.empty:
            duplicate_count = int(frame.duplicated(list(contract.key_fields)).sum())
            if duplicate_count:
                self.add_issue(
                    "duplicate_dataset_keys",
                    f"{contract.dataset} contains duplicate keys",
                    details={"dataset": contract.dataset, "count": duplicate_count},
                )
        self.check_text_encoding(frame, chinese_text_columns)
        return self

    def check_text_encoding(
        self,
        frame: pd.DataFrame,
        columns: Sequence[str],
    ) -> "DataQualityGate":
        if not columns:
            return self
        self._record_check("text_encoding")
        suspicious: dict[str, dict[str, Any]] = {}
        for column in columns:
            if column not in frame.columns:
                continue
            mask = frame[column].map(is_probable_mojibake)
            if mask.any():
                samples = frame.loc[mask, column].astype(str).head(5).tolist()
                suspicious[column] = {"count": int(mask.sum()), "samples": samples}
        if suspicious:
            self.add_issue(
                "probable_mojibake",
                "text fields contain replacement characters or probable encoding corruption",
                details={"columns": suspicious},
            )
        return self

    def check_historical_st(
        self,
        records: pd.DataFrame,
        *,
        available: bool,
        degraded: bool,
        reason: str | None = None,
    ) -> "DataQualityGate":
        self._record_check("historical_st")
        if not available or degraded:
            self.add_issue(
                "st_history_unverified",
                "historical ST coverage is unavailable or degraded",
                details={"available": available, "degraded": degraded, "reason": reason},
            )
            return self
        if records.empty:
            self.add_issue(
                "st_history_unverified",
                "historical ST source was marked available but contains no records",
                details={"reason": "empty_table"},
            )
            return self
        required = {"ts_code", "start_date", "end_date"}
        missing = sorted(required - set(records.columns))
        if missing:
            self.add_issue(
                "historical_st_columns_missing",
                "historical ST source omits interval fields",
                details={"columns": missing},
            )
        return self

    def check_partition_coverage(
        self,
        plan: Mapping[str, Any],
        checkpoint: Mapping[str, Any],
        *,
        required_datasets: Iterable[str] | None = None,
        verify_hashes: bool = True,
    ) -> "DataQualityGate":
        self._record_check("partition_coverage")
        required = set(required_datasets or [])
        all_rows = list(plan.get("partitions", []))
        rows = [
            row for row in all_rows
            if not required or str(row.get("dataset")) in required
        ]
        if not all_rows:
            self.add_issue("partition_plan_empty", "partition plan contains no open-market partitions")
            return self
        keys = [str(row.get("key") or "") for row in rows]
        duplicate_plan_keys = sorted(key for key, count in Counter(keys).items() if count > 1)
        if duplicate_plan_keys:
            self.add_issue(
                "duplicate_partition_plan_keys",
                "partition plan contains duplicate keys",
                details={"count": len(duplicate_plan_keys), "sample": duplicate_plan_keys[:10]},
            )
        planned_dates = {str(row.get("trade_date")) for row in all_rows}
        datasets = required or {str(row.get("dataset")) for row in rows}
        expected_pairs = {(dataset, date) for dataset in datasets for date in planned_dates}
        actual_pairs = {(str(row.get("dataset")), str(row.get("trade_date"))) for row in rows}
        missing_pairs = sorted(expected_pairs - actual_pairs)
        if missing_pairs:
            self.add_issue(
                "partition_plan_date_coverage_incomplete",
                "required datasets do not cover every planned open date",
                details={"count": len(missing_pairs), "sample": missing_pairs[:10]},
            )

        entries = checkpoint.get("partitions") or {}
        if not isinstance(entries, Mapping):
            self.add_issue("checkpoint_invalid", "checkpoint.partitions is not a mapping")
            return self
        failure_buckets: dict[str, list[str]] = {
            "missing_checkpoint": [],
            "not_complete": [],
            "path_mismatch": [],
            "file_missing": [],
            "empty_market_day": [],
            "size_mismatch": [],
            "hash_missing": [],
            "hash_mismatch": [],
        }
        for row in rows:
            key = str(row.get("key") or "")
            entry = entries.get(key)
            if not isinstance(entry, Mapping):
                failure_buckets["missing_checkpoint"].append(key)
                continue
            if entry.get("status") != "complete":
                failure_buckets["not_complete"].append(key)
            planned_path = Path(str(row.get("path"))).resolve()
            stored_path = Path(str(entry.get("path"))).resolve()
            if stored_path != planned_path:
                failure_buckets["path_mismatch"].append(key)
                continue
            if not planned_path.is_file():
                failure_buckets["file_missing"].append(key)
                continue
            if int(entry.get("row_count") or 0) <= 0:
                failure_buckets["empty_market_day"].append(key)
            actual_size = planned_path.stat().st_size
            if int(entry.get("size_bytes") or -1) != actual_size:
                failure_buckets["size_mismatch"].append(key)
            expected_hash = str(entry.get("sha256") or "")
            if not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
                failure_buckets["hash_missing"].append(key)
            elif verify_hashes and sha256_path(planned_path) != expected_hash:
                failure_buckets["hash_mismatch"].append(key)
        for code, failures in failure_buckets.items():
            if failures:
                self.add_issue(
                    code,
                    f"partition integrity check failed: {code}",
                    details={"count": len(failures), "sample": failures[:10]},
                )
        return self

    def check_manifest_verification(self, verification: Mapping[str, Any]) -> "DataQualityGate":
        self._record_check("snapshot_manifest")
        if not verification.get("valid"):
            self.add_issue(
                "snapshot_manifest_invalid",
                "snapshot content or manifest hash failed verification",
                details=dict(verification),
            )
        return self

    def report(self) -> QualityReport:
        return QualityReport(issues=tuple(self._issues), checks=tuple(self._checks))

    def raise_if_blocked(self) -> QualityReport:
        report = self.report()
        if not report.promotion_allowed:
            raise DataQualityError(report)
        return report


def combine_quality_reports(reports: Iterable[QualityReport]) -> QualityReport:
    issues: list[QualityIssue] = []
    checks: list[str] = []
    for report in reports:
        issues.extend(report.issues)
        checks.extend(name for name in report.checks if name not in checks)
    return QualityReport(issues=tuple(issues), checks=tuple(checks))


__all__ = [
    "DataQualityError",
    "DataQualityGate",
    "QualityIssue",
    "QualityReport",
    "QualitySeverity",
    "combine_quality_reports",
    "is_probable_mojibake",
    "sha256_path",
]
