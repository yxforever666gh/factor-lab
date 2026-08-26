"""Canonical local paths and lightweight Parquet audits."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd
import pyarrow.parquet as pq


DEFAULT_CONFIG_PATH = Path("configs/data.json")


def _repo_root_from_config(config_path: Path) -> Path:
    if config_path.parent.name == "configs":
        return config_path.parent.parent.resolve()
    return Path.cwd().resolve()


def _resolve(base: Path, value: str | Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (base / path).resolve()


def load_data_config(path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    config_path = Path(path).expanduser().resolve()
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    if int(payload.get("schema_version") or 0) != 1:
        raise ValueError("data config requires schema_version=1")
    if not isinstance(payload.get("paths"), Mapping):
        raise ValueError("data config requires a paths mapping")
    if not isinstance(payload.get("top500"), Mapping):
        raise ValueError("data config requires a top500 mapping")
    return payload


@dataclass(frozen=True)
class RuntimeLayout:
    repo_root: Path
    runtime_root: Path
    data_root: Path
    raw_root: Path
    top500_root: Path
    runs_root: Path
    legacy_root: Path
    features_path: Path
    execution_path: Path
    membership_path: Path
    checkpoint_path: Path
    migration_manifest_path: Path

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, Any] | None = None,
        *,
        config_path: str | Path = DEFAULT_CONFIG_PATH,
        repo_root: str | Path | None = None,
        runtime_root: str | Path | None = None,
    ) -> "RuntimeLayout":
        resolved_config_path = Path(config_path).expanduser().resolve()
        payload = dict(config or load_data_config(resolved_config_path))
        root = (
            Path(repo_root).expanduser().resolve()
            if repo_root is not None
            else Path(os.environ["FACTOR_LAB_ROOT"]).expanduser().resolve()
            if os.environ.get("FACTOR_LAB_ROOT")
            else _repo_root_from_config(resolved_config_path)
        )
        configured_runtime = runtime_root or os.environ.get("FACTOR_LAB_RUNTIME_ROOT") or payload.get(
            "runtime_root", "runtime"
        )
        runtime = _resolve(root, str(configured_runtime))
        paths = dict(payload["paths"])
        data_root = _resolve(runtime, str(paths.get("data", "data")))
        raw_root = _resolve(runtime, str(paths.get("raw", "data/raw")))
        top500_root = _resolve(runtime, str(paths.get("top500", "data/top500")))
        runs_root = _resolve(runtime, str(paths.get("runs", "runs")))
        legacy_root = _resolve(runtime, str(paths.get("legacy", "legacy")))
        top500 = dict(payload["top500"])
        sync = dict(payload.get("sync") or {})
        return cls(
            repo_root=root,
            runtime_root=runtime,
            data_root=data_root,
            raw_root=raw_root,
            top500_root=top500_root,
            runs_root=runs_root,
            legacy_root=legacy_root,
            features_path=top500_root / str(top500.get("features_file", "features.parquet")),
            execution_path=top500_root / str(top500.get("execution_file", "execution.parquet")),
            membership_path=top500_root / str(top500.get("membership_file", "membership.parquet")),
            checkpoint_path=raw_root / str(sync.get("checkpoint_file", "checkpoint.json")),
            migration_manifest_path=top500_root / "migration-manifest.json",
        )

    def ensure_directories(self) -> None:
        for path in (
            self.runtime_root,
            self.data_root,
            self.raw_root,
            self.top500_root,
            self.runs_root,
            self.legacy_root,
        ):
            path.mkdir(parents=True, exist_ok=True)


def sha256_file(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def parquet_status(
    path: str | Path,
    *,
    required_columns: Iterable[str] = (),
    hash_file: bool = False,
) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        return {
            "path": str(resolved),
            "status": "missing",
            "exists": False,
            "row_count": 0,
            "size_bytes": 0,
            "columns": [],
            "missing_columns": sorted(set(required_columns)),
        }
    try:
        parquet = pq.ParquetFile(resolved)
        columns = list(parquet.schema_arrow.names)
        missing = sorted(set(required_columns) - set(columns))
        result: dict[str, Any] = {
            "path": str(resolved),
            "status": "pass" if parquet.metadata.num_rows > 0 and not missing else "fail",
            "exists": True,
            "row_count": int(parquet.metadata.num_rows),
            "row_group_count": int(parquet.metadata.num_row_groups),
            "size_bytes": int(resolved.stat().st_size),
            "columns": columns,
            "missing_columns": missing,
        }
        if hash_file:
            result["sha256"] = sha256_file(resolved)
        return result
    except (OSError, ValueError) as exc:
        return {
            "path": str(resolved),
            "status": "unreadable",
            "exists": True,
            "row_count": 0,
            "size_bytes": int(resolved.stat().st_size),
            "columns": [],
            "missing_columns": sorted(set(required_columns)),
            "error": f"{type(exc).__name__}: {exc}",
        }


def audit_parquet(
    path: str | Path,
    *,
    required_columns: Iterable[str],
    core_columns: Iterable[str] = (),
    unique_keys: tuple[str, ...] = ("ticker", "date"),
    date_column: str | None = "date",
    ticker_column: str | None = "ticker",
    minimum_coverage: float = 0.95,
    deep: bool = False,
    hash_file: bool = False,
) -> dict[str, Any]:
    status = parquet_status(path, required_columns=required_columns, hash_file=hash_file)
    issues: list[str] = []
    if status["status"] != "pass":
        issues.append(status["status"])
    if status.get("missing_columns"):
        issues.append("missing_required_columns")
    if not deep or issues:
        return {**status, "audit_mode": "metadata", "issues": sorted(set(issues))}

    selected = set(core_columns) | set(unique_keys)
    if date_column:
        selected.add(date_column)
    if ticker_column:
        selected.add(ticker_column)
    selected &= set(status["columns"])
    frame = pd.read_parquet(Path(path), columns=sorted(selected))
    coverage = {
        column: round(float(frame[column].notna().mean()), 8) if len(frame) else 0.0
        for column in core_columns
        if column in frame
    }
    low_coverage = sorted(column for column, value in coverage.items() if value < minimum_coverage)
    duplicate_count = (
        int(frame.duplicated(list(unique_keys)).sum())
        if set(unique_keys).issubset(frame.columns)
        else None
    )
    start_date = end_date = None
    date_monotonic = None
    if date_column and date_column in frame:
        dates = pd.to_datetime(frame[date_column], errors="coerce")
        valid_dates = dates.dropna()
        start_date = valid_dates.min().date().isoformat() if len(valid_dates) else None
        end_date = valid_dates.max().date().isoformat() if len(valid_dates) else None
        date_monotonic = bool(dates.is_monotonic_increasing)
        if dates.isna().any():
            issues.append("invalid_dates")
        if not date_monotonic:
            issues.append("dates_not_monotonic")
    if duplicate_count:
        issues.append("duplicate_keys")
    if low_coverage:
        issues.append("core_coverage_below_minimum")
    ticker_count = (
        int(frame[ticker_column].astype("string").nunique())
        if ticker_column and ticker_column in frame
        else None
    )
    return {
        **status,
        "status": "pass" if not issues else "fail",
        "audit_mode": "deep",
        "issues": sorted(set(issues)),
        "duplicate_key_count": duplicate_count,
        "core_coverage": coverage,
        "minimum_core_coverage": min(coverage.values()) if coverage else None,
        "low_coverage_columns": low_coverage,
        "date_monotonic": date_monotonic,
        "start_date": start_date,
        "end_date": end_date,
        "ticker_count": ticker_count,
    }


def audit_top500_store(
    layout: RuntimeLayout,
    config: Mapping[str, Any],
    *,
    deep: bool = False,
    hash_files: bool = False,
) -> dict[str, Any]:
    quality = dict(config.get("quality") or {})
    minimum = float(quality.get("minimum_core_coverage", 0.95))
    specifications = {
        "features": (
            layout.features_path,
            tuple(quality.get("feature_required_columns") or ("ticker", "date")),
            tuple(quality.get("feature_core_columns") or ()),
            ("ticker", "date"),
            "date",
            "ticker",
        ),
        "execution": (
            layout.execution_path,
            tuple(quality.get("execution_required_columns") or ("ticker", "date")),
            tuple(quality.get("execution_core_columns") or ()),
            ("ticker", "date"),
            "date",
            "ticker",
        ),
        "membership": (
            layout.membership_path,
            tuple(quality.get("membership_required_columns") or ("ts_code", "membership_month")),
            (),
            ("ts_code", "membership_month"),
            None,
            "ts_code",
        ),
    }
    files = {
        name: audit_parquet(
            path,
            required_columns=required,
            core_columns=core,
            unique_keys=keys,
            date_column=date_column,
            ticker_column=ticker_column,
            minimum_coverage=minimum,
            deep=deep,
            hash_file=hash_files,
        )
        for name, (path, required, core, keys, date_column, ticker_column) in specifications.items()
    }
    if deep and files["features"].get("status") == "pass":
        available = set(files["features"].get("columns") or ())
        if {"amount", "amount_rmb"}.issubset(available):
            amounts = pd.read_parquet(layout.features_path, columns=["amount", "amount_rmb"])
            base = pd.to_numeric(amounts["amount"], errors="coerce")
            converted = pd.to_numeric(amounts["amount_rmb"], errors="coerce")
            valid = base.notna() & converted.notna() & base.ne(0.0)
            ratios = (converted[valid] / base[valid]).replace(
                [float("inf"), float("-inf")], float("nan")
            ).dropna()
            median_ratio = float(ratios.median()) if len(ratios) else None
            files["features"]["amount_rmb_per_amount_median"] = median_ratio
            if median_ratio is None or not 0.9 <= median_ratio <= 1.1:
                files["features"]["issues"] = sorted(
                    {*files["features"]["issues"], "amount_unit_not_rmb"}
                )
                files["features"]["status"] = "fail"
    issues = [f"{name}:{issue}" for name, result in files.items() for issue in result["issues"]]
    if deep:
        feature_end = files["features"].get("end_date")
        execution_end = files["execution"].get("end_date")
        if feature_end and execution_end and execution_end < feature_end:
            issues.append("execution:ends_before_features")
    return {
        "schema_version": 1,
        "status": "ready" if not issues else "not_ready",
        "runtime_root": str(layout.runtime_root),
        "audit_mode": "deep" if deep else "metadata",
        "files": files,
        "issues": issues,
    }


__all__ = [
    "DEFAULT_CONFIG_PATH",
    "RuntimeLayout",
    "audit_parquet",
    "audit_top500_store",
    "load_data_config",
    "parquet_status",
    "sha256_file",
]
