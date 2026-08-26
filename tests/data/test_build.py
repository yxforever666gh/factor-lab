from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_lab.data import (
    RuntimeLayout,
    apply_feature_store_migration,
    build_data,
    normalize_legacy_amount_units,
    plan_feature_store_migration,
)


def _write_config(tmp_path: Path) -> tuple[Path, dict]:
    payload = {
        "schema_version": 1,
        "runtime_root": "runtime",
        "paths": {
            "data": "data",
            "raw": "data/raw",
            "top500": "data/top500",
            "runs": "runs",
            "legacy": "legacy",
        },
        "top500": {
            "features_file": "features.parquet",
            "execution_file": "execution.parquet",
            "membership_file": "membership.parquet",
        },
        "legacy": {
            "expanded_feature_store": "legacy-store",
            "files": {
                "features": "old-features.parquet",
                "execution": "old-execution.parquet",
                "membership": "old-membership.parquet",
            },
        },
        "quality": {
            "minimum_core_coverage": 0.95,
            "feature_required_columns": ["ticker", "date", "open_adj"],
            "feature_core_columns": ["ticker", "date", "open_adj"],
            "execution_required_columns": ["ticker", "date", "open_adj"],
            "execution_core_columns": ["ticker", "date", "open_adj"],
            "membership_required_columns": ["ts_code", "membership_month"],
        },
    }
    path = tmp_path / "configs" / "data.json"
    path.parent.mkdir()
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path, payload


def _write_legacy_store(tmp_path: Path) -> None:
    root = tmp_path / "legacy-store"
    root.mkdir()
    dates = pd.to_datetime(["2024-01-02", "2024-01-03"])
    pd.DataFrame(
        {"ticker": ["A", "A"], "date": dates, "open_adj": [10.0, 10.2]}
    ).to_parquet(root / "old-features.parquet", index=False)
    pd.DataFrame(
        {"ticker": ["A", "A"], "date": dates, "open_adj": [10.0, 10.2]}
    ).to_parquet(root / "old-execution.parquet", index=False)
    pd.DataFrame(
        {"ts_code": ["A"], "membership_month": ["2024-01"]}
    ).to_parquet(root / "old-membership.parquet", index=False)


def test_migration_is_planned_then_copied_without_removing_sources(tmp_path: Path) -> None:
    config_path, payload = _write_config(tmp_path)
    _write_legacy_store(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)

    plan = plan_feature_store_migration(
        config_path=config_path, config=payload, layout=layout
    )
    assert plan["status"] == "ready"
    assert {row["action"] for row in plan["files"]} == {"copy"}

    result = apply_feature_store_migration(
        plan, manifest_path=layout.migration_manifest_path
    )
    assert result["status"] == "complete"
    assert layout.features_path.is_file()
    assert (tmp_path / "legacy-store/old-features.parquet").is_file()
    assert layout.migration_manifest_path.is_file()

    status = build_data("full", config_path=config_path, layout=layout)
    assert status["status"] == "ready"
    assert status["audit"]["status"] == "ready"


def test_build_reports_migration_required_without_writing_large_files(tmp_path: Path) -> None:
    config_path, payload = _write_config(tmp_path)
    _write_legacy_store(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)

    status = build_data("canary", config_path=config_path, layout=layout)

    assert status["status"] == "migration_required"
    assert not layout.features_path.exists()


def test_legacy_hybrid_amount_and_adv_are_normalized_resumably(tmp_path: Path) -> None:
    config_path, payload = _write_config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    layout.ensure_directories()
    dates = pd.bdate_range("2024-01-02", periods=25)
    features = pd.DataFrame(
        {
            "ticker": ["A"] * len(dates),
            "date": dates,
            "open_adj": range(10, 10 + len(dates)),
            "amount": [100_000_000.0] * len(dates),
            "amount_rmb": [100_000_000_000.0] * len(dates),
            "adv_20": [100_000_000_000.0] * len(dates),
        }
    )
    execution = features[["ticker", "date", "open_adj", "adv_20"]].copy()
    features.to_parquet(layout.features_path, index=False)
    execution.to_parquet(layout.execution_path, index=False)

    result = normalize_legacy_amount_units(layout)

    assert result["status"] == "normalized"
    repaired_features = pd.read_parquet(layout.features_path)
    repaired_execution = pd.read_parquet(layout.execution_path)
    assert repaired_features["amount"].eq(100_000_000.0).all()
    assert repaired_features["amount_rmb"].eq(100_000_000.0).all()
    assert repaired_features["adv_20"].eq(100_000_000.0).all()
    assert repaired_execution["adv_20"].eq(100_000_000.0).all()
    assert repaired_features["open_adj"].equals(features["open_adj"])
    assert normalize_legacy_amount_units(layout)["status"] == "already_normalized"
