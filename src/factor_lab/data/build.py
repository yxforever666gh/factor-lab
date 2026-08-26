"""Adopt the frozen expanded Parquet store into the lightweight runtime."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
from typing import Any, Mapping

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
        "audit": audit,
    }


__all__ = [
    "apply_feature_store_migration",
    "build_data",
    "plan_feature_store_migration",
]
