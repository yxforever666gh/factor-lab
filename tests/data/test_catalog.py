from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_lab.data import RuntimeLayout, audit_parquet, load_data_config, parquet_status


def _config(tmp_path: Path) -> tuple[Path, dict]:
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
    }
    path = tmp_path / "configs" / "data.json"
    path.parent.mkdir()
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path, payload


def test_runtime_layout_resolves_all_paths_under_canonical_runtime(tmp_path: Path) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)

    assert layout.repo_root == tmp_path.resolve()
    assert layout.runtime_root == (tmp_path / "runtime").resolve()
    assert layout.features_path == (tmp_path / "runtime/data/top500/features.parquet").resolve()
    assert layout.execution_path.name == "execution.parquet"
    assert layout.checkpoint_path == (tmp_path / "runtime/data/raw/checkpoint.json").resolve()


def test_config_requires_versioned_lightweight_shape(tmp_path: Path) -> None:
    path = tmp_path / "data.json"
    path.write_text('{"schema_version": 3}', encoding="utf-8")

    try:
        load_data_config(path)
    except ValueError as exc:
        assert "schema_version=1 or 2" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("invalid config unexpectedly loaded")


def test_parquet_status_and_deep_audit_detect_duplicate_and_order_issues(tmp_path: Path) -> None:
    path = tmp_path / "features.parquet"
    frame = pd.DataFrame(
        {
            "ticker": ["B", "A", "A"],
            "date": pd.to_datetime(["2024-01-02", "2024-01-01", "2024-01-01"]),
            "open_adj": [10.0, 11.0, None],
        }
    )
    frame.to_parquet(path, index=False)

    metadata = parquet_status(path, required_columns=("ticker", "date", "open_adj"))
    audit = audit_parquet(
        path,
        required_columns=("ticker", "date", "open_adj"),
        core_columns=("open_adj",),
        minimum_coverage=0.95,
        deep=True,
    )

    assert metadata["status"] == "pass"
    assert metadata["row_count"] == 3
    assert audit["status"] == "fail"
    assert audit["duplicate_key_count"] == 1
    assert set(audit["issues"]) == {
        "core_coverage_below_minimum",
        "dates_not_monotonic",
        "duplicate_keys",
    }

