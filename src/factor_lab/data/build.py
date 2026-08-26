"""Adopt the frozen expanded Parquet store into the lightweight runtime."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
from pathlib import Path
import shutil
from typing import Any, Mapping

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .catalog import (
    DEFAULT_CONFIG_PATH,
    RuntimeLayout,
    audit_top500_store,
    load_data_config,
    sha256_file,
)


def _source_paths(config: Mapping[str, Any], layout: RuntimeLayout) -> dict[str, Path]:
    legacy = dict(config.get("legacy") or {})
    source_root = Path(str(legacy.get("expanded_feature_store") or "artifacts/expanded_long_only/feature_store"))
    if not source_root.is_absolute():
        source_root = layout.repo_root / source_root
    names = dict(legacy.get("files") or {})
    return {
        "features": source_root / str(names.get("features") or "expanded_top500_features.parquet"),
        "execution": source_root / str(names.get("execution") or "expanded_execution_prices.parquet"),
        "membership": source_root / str(names.get("membership") or "monthly_top500_membership.parquet"),
    }


def plan_feature_store_migration(
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    config: Mapping[str, Any] | None = None,
    layout: RuntimeLayout | None = None,
    hash_files: bool = False,
) -> dict[str, Any]:
    payload = dict(config or load_data_config(config_path))
    resolved_layout = layout or RuntimeLayout.from_config(payload, config_path=config_path)
    sources = _source_paths(payload, resolved_layout)
    targets = {
        "features": resolved_layout.features_path,
        "execution": resolved_layout.execution_path,
        "membership": resolved_layout.membership_path,
    }
    rows: list[dict[str, Any]] = []
    for name in ("features", "execution", "membership"):
        source = sources[name].resolve()
        target = targets[name].resolve()
        source_exists = source.is_file()
        target_exists = target.is_file()
        row: dict[str, Any] = {
            "name": name,
            "source": str(source),
            "target": str(target),
            "source_exists": source_exists,
            "target_exists": target_exists,
            "source_size_bytes": source.stat().st_size if source_exists else 0,
            "target_size_bytes": target.stat().st_size if target_exists else 0,
        }
        if not source_exists and not target_exists:
            row["action"] = "missing_source"
        elif not source_exists:
            row["action"] = "keep_target"
        elif not target_exists:
            row["action"] = "copy"
        elif source.resolve() == target.resolve():
            row["action"] = "already_present"
        elif source.stat().st_size != target.stat().st_size:
            row["action"] = "conflict"
        elif hash_files:
            row["source_sha256"] = sha256_file(source)
            row["target_sha256"] = sha256_file(target)
            row["action"] = (
                "already_present" if row["source_sha256"] == row["target_sha256"] else "conflict"
            )
        else:
            row["action"] = "verify"
        rows.append(row)
    blocking = [row["name"] for row in rows if row["action"] in {"missing_source", "conflict"}]
    return {
        "schema_version": 1,
        "status": "blocked" if blocking else "ready",
        "operation": "copy_only",
        "runtime_root": str(resolved_layout.runtime_root),
        "manifest_path": str(resolved_layout.migration_manifest_path),
        "blocking_files": blocking,
        "files": rows,
    }


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )
    temporary.replace(path)


def _median_feature_amount_ratio(path: Path) -> float | None:
    available = set(pq.ParquetFile(path).schema_arrow.names)
    if not {"amount", "amount_rmb"}.issubset(available):
        return None
    frame = pd.read_parquet(path, columns=["amount", "amount_rmb"])
    denominator = pd.to_numeric(frame["amount"], errors="coerce")
    numerator = pd.to_numeric(frame["amount_rmb"], errors="coerce")
    valid = denominator.notna() & numerator.notna() & denominator.ne(0.0)
    ratio = (numerator[valid] / denominator[valid]).replace([np.inf, -np.inf], np.nan).dropna()
    return float(ratio.median()) if len(ratio) else None


def _median_overlapping_adv_ratio(features_path: Path, execution_path: Path) -> float | None:
    feature_columns = set(pq.ParquetFile(features_path).schema_arrow.names)
    execution_columns = set(pq.ParquetFile(execution_path).schema_arrow.names)
    required = {"ticker", "date", "adv_20"}
    if not required.issubset(feature_columns) or not required.issubset(execution_columns):
        return None
    feature_parquet = pq.ParquetFile(features_path)
    sample = feature_parquet.read_row_group(0, columns=sorted(required)).to_pandas()
    sample = sample.head(100_000).rename(columns={"adv_20": "feature_adv"})
    if sample.empty:
        return None
    sample["date"] = pd.to_datetime(sample["date"], errors="coerce")
    lower = sample["date"].min()
    upper = sample["date"].max()
    execution = pd.read_parquet(
        execution_path,
        columns=sorted(required),
        filters=[("date", ">=", lower), ("date", "<=", upper)],
    ).rename(columns={"adv_20": "execution_adv"})
    execution["date"] = pd.to_datetime(execution["date"], errors="coerce")
    joined = sample.merge(execution, on=["ticker", "date"], how="inner")
    denominator = pd.to_numeric(joined["feature_adv"], errors="coerce")
    numerator = pd.to_numeric(joined["execution_adv"], errors="coerce")
    valid = denominator.notna() & numerator.notna() & denominator.ne(0.0)
    ratio = (numerator[valid] / denominator[valid]).replace([np.inf, -np.inf], np.nan).dropna()
    return float(ratio.median()) if len(ratio) else None


def _scaled_parquet_copy(source: Path, target: Path, scales: Mapping[str, float]) -> None:
    parquet = pq.ParquetFile(source)
    missing = sorted(set(scales) - set(parquet.schema_arrow.names))
    if missing:
        raise ValueError(f"cannot normalize {source.name}; missing columns: {missing}")
    target.unlink(missing_ok=True)
    writer = pq.ParquetWriter(target, parquet.schema_arrow, compression="zstd")
    try:
        for batch in parquet.iter_batches(batch_size=100_000):
            table = pa.Table.from_batches([batch])
            for column, scale in scales.items():
                index = table.schema.get_field_index(column)
                field = table.schema.field(index)
                values = pc.multiply(table[column], float(scale)).cast(field.type)
                table = table.set_column(index, field, values)
            writer.write_table(table)
    finally:
        writer.close()
    rewritten = pq.ParquetFile(target)
    if rewritten.metadata.num_rows != parquet.metadata.num_rows:
        target.unlink(missing_ok=True)
        raise RuntimeError(f"row count changed while normalizing {source.name}")


def normalize_legacy_amount_units(layout: RuntimeLayout) -> dict[str, Any]:
    """Repair the retired builder's RMB x1000 bug exactly once and resumably.

    AkShare turnover amount and the fallback ``circ_mv * turnover`` estimate are
    already denominated in RMB.  The retired expanded builder multiplied the
    unified amount by 1000 before calculating ADV.  The signature is therefore
    deterministic: ``median(amount_rmb / amount) ~= 1000``.
    """

    marker = layout.top500_root / "unit-normalization.json"
    feature_ratio = _median_feature_amount_ratio(layout.features_path)
    if feature_ratio is None:
        return {"status": "not_applicable", "reason": "amount columns unavailable"}
    overlap_ratio = _median_overlapping_adv_ratio(layout.features_path, layout.execution_path)
    feature_needs_scale = 900.0 <= feature_ratio <= 1100.0
    feature_is_rmb = 0.9 <= feature_ratio <= 1.1
    if not feature_needs_scale and not feature_is_rmb:
        raise RuntimeError(f"ambiguous amount unit ratio: {feature_ratio:.6g}")
    if overlap_ratio is None or not math.isfinite(overlap_ratio):
        raise RuntimeError("cannot reconcile feature and execution ADV units")
    execution_needs_scale = (feature_needs_scale and 0.9 <= overlap_ratio <= 1.1) or (
        feature_is_rmb and 900.0 <= overlap_ratio <= 1100.0
    )
    if not execution_needs_scale and not (0.9 <= overlap_ratio <= 1.1):
        raise RuntimeError(f"ambiguous execution ADV unit ratio: {overlap_ratio:.6g}")
    if not feature_needs_scale and not execution_needs_scale:
        return {
            "status": "already_normalized",
            "feature_amount_ratio": feature_ratio,
            "execution_to_feature_adv_ratio": overlap_ratio,
            "marker_path": str(marker),
        }

    before = {
        "features_sha256": sha256_file(layout.features_path),
        "execution_sha256": sha256_file(layout.execution_path),
    }
    feature_temp = layout.features_path.with_suffix(".unit-fix.partial.parquet")
    execution_temp = layout.execution_path.with_suffix(".unit-fix.partial.parquet")
    if feature_needs_scale:
        _scaled_parquet_copy(
            layout.features_path,
            feature_temp,
            {"amount_rmb": 0.001, "adv_20": 0.001},
        )
    if execution_needs_scale:
        _scaled_parquet_copy(layout.execution_path, execution_temp, {"adv_20": 0.001})
    if feature_needs_scale:
        feature_temp.replace(layout.features_path)
    if execution_needs_scale:
        execution_temp.replace(layout.execution_path)
    normalized_ratio = _median_feature_amount_ratio(layout.features_path)
    normalized_overlap = _median_overlapping_adv_ratio(layout.features_path, layout.execution_path)
    if normalized_ratio is None or not (0.9 <= normalized_ratio <= 1.1):
        raise RuntimeError("feature amount normalization did not validate")
    if normalized_overlap is None or not (0.9 <= normalized_overlap <= 1.1):
        raise RuntimeError("execution ADV normalization did not validate")
    result = {
        "schema_version": 1,
        "status": "normalized",
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "reason": "retired expanded builder multiplied RMB turnover amount by 1000",
        "scale": 0.001,
        "feature_amount_ratio_before": feature_ratio,
        "execution_to_feature_adv_ratio_before": overlap_ratio,
        "feature_amount_ratio_after": normalized_ratio,
        "execution_to_feature_adv_ratio_after": normalized_overlap,
        "before": before,
        "after": {
            "features_sha256": sha256_file(layout.features_path),
            "execution_sha256": sha256_file(layout.execution_path),
        },
    }
    _write_json_atomic(marker, result)
    return {**result, "marker_path": str(marker)}


def apply_feature_store_migration(
    plan: Mapping[str, Any],
    *,
    manifest_path: str | Path | None = None,
) -> dict[str, Any]:
    """Copy (never move) a migration plan and verify every destination hash."""

    if plan.get("operation") != "copy_only":
        raise ValueError("migration plan must use operation=copy_only")
    if plan.get("status") == "blocked":
        raise RuntimeError(f"migration is blocked: {plan.get('blocking_files')}")
    completed: list[dict[str, Any]] = []
    for raw_row in plan.get("files") or []:
        row = dict(raw_row)
        source = Path(str(row["source"]))
        target = Path(str(row["target"]))
        action = str(row.get("action") or "")
        if action == "keep_target":
            completed.append(
                {"name": row["name"], "path": str(target), "sha256": sha256_file(target), "copied": False}
            )
            continue
        if action in {"verify", "already_present"} and target.is_file():
            if source.is_file() and sha256_file(source) != sha256_file(target):
                raise RuntimeError(f"migration conflict for {row['name']}")
            completed.append(
                {"name": row["name"], "path": str(target), "sha256": sha256_file(target), "copied": False}
            )
            continue
        if action != "copy":
            raise RuntimeError(f"unsupported migration action {action!r} for {row['name']}")
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(target.suffix + ".partial")
        shutil.copy2(source, temporary)
        source_hash = sha256_file(source)
        if sha256_file(temporary) != source_hash:
            temporary.unlink(missing_ok=True)
            raise RuntimeError(f"copied file hash mismatch for {row['name']}")
        temporary.replace(target)
        completed.append(
            {"name": row["name"], "path": str(target), "sha256": source_hash, "copied": True}
        )
    result = {
        "schema_version": 1,
        "status": "complete",
        "operation": "copy_only",
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "files": completed,
    }
    destination = (
        Path(manifest_path)
        if manifest_path is not None
        else Path(str(plan.get("manifest_path") or "migration-manifest.json"))
    )
    _write_json_atomic(destination, result)
    return {**result, "manifest_path": str(destination.resolve())}


def build_data(
    mode: str,
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    layout: RuntimeLayout | None = None,
    apply_migration: bool = False,
    hash_files: bool = False,
) -> dict[str, Any]:
    """Prepare or validate canonical inputs without invoking retired research rounds."""

    if mode not in {"canary", "full"}:
        raise ValueError("mode must be 'canary' or 'full'")
    config = load_data_config(config_path)
    resolved_layout = layout or RuntimeLayout.from_config(config, config_path=config_path)
    resolved_layout.ensure_directories()
    plan = plan_feature_store_migration(
        config_path=config_path, config=config, layout=resolved_layout, hash_files=hash_files
    )
    canonical_exists = all(
        path.is_file()
        for path in (
            resolved_layout.features_path,
            resolved_layout.execution_path,
            resolved_layout.membership_path,
        )
    )
    migration = None
    if not canonical_exists and apply_migration:
        migration = apply_feature_store_migration(
            plan, manifest_path=resolved_layout.migration_manifest_path
        )
        canonical_exists = True
    if not canonical_exists:
        return {
            "schema_version": 1,
            "status": "migration_required" if plan["status"] == "ready" else "missing_data",
            "mode": mode,
            "migration_plan": plan,
        }
    normalization = normalize_legacy_amount_units(resolved_layout) if mode == "full" else None
    audit = audit_top500_store(
        resolved_layout,
        config,
        deep=mode == "full",
        hash_files=hash_files,
    )
    return {
        "schema_version": 1,
        "status": "ready" if audit["status"] == "ready" else "not_ready",
        "mode": mode,
        "migration": migration,
        "normalization": normalization,
        "audit": audit,
    }


__all__ = [
    "apply_feature_store_migration",
    "build_data",
    "normalize_legacy_amount_units",
    "plan_feature_store_migration",
]
