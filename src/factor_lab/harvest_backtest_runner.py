from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.small_institutional_backtest_matrix import (
    DEFAULT_YEAR_WINDOWS,
    build_small_institutional_backtest_matrix,
    small_institutional_backtest_matrix_to_markdown,
)

DEFAULT_DATASET_PATH = "artifacts/value_route_bucket_aware/runs/value_quality_no_distress_bucket_aware/dataset.csv"


def _actions(plan: dict[str, Any], action_type: str) -> list[dict[str, Any]]:
    return [a for a in plan.get("actions") or [] if a.get("type") == action_type]


def apply_plan_filters(dataset: pd.DataFrame, actions: list[dict[str, Any]]) -> pd.DataFrame:
    frame = dataset.copy()
    for action in actions:
        if action.get("type") != "add_filter":
            continue
        field = action.get("field")
        if not field or field not in frame.columns:
            continue
        values = pd.to_numeric(frame[field], errors="coerce")
        q = float(action.get("quantile", 0.5))
        op = action.get("operator")
        if "date" in frame.columns:
            thresholds = frame.groupby("date")[field].transform(lambda s: pd.to_numeric(s, errors="coerce").quantile(q, interpolation="lower"))
        else:
            thresholds = values.quantile(q, interpolation="lower")
        if op == "<=":
            frame = frame[values <= thresholds]
        elif op == ">=":
            frame = frame[values >= thresholds]
        elif op == "<":
            frame = frame[values < thresholds]
        elif op == ">":
            frame = frame[values > thresholds]
    return frame.reset_index(drop=True)


def _first_action_value(plan: dict[str, Any], action_type: str, key: str, default: Any) -> Any:
    matches = _actions(plan, action_type)
    if matches and key in matches[-1]:
        return matches[-1][key]
    return default


def run_plan_backtest(plan: dict[str, Any], *, output_dir: str | Path, root: str | Path | None = None) -> dict[str, Any]:
    root_path = Path(root) if root is not None else Path(".")
    dataset_path = Path(plan.get("dataset_path") or DEFAULT_DATASET_PATH)
    if not dataset_path.is_absolute():
        dataset_path = root_path / dataset_path
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    if not dataset_path.exists():
        payload = {"schema_version": 1, "status": "blocked_missing_data", "dataset_path": str(dataset_path), "reason": "dataset_missing"}
        (out / "result.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return payload

    dataset = pd.read_csv(dataset_path)
    filtered = apply_plan_filters(dataset, plan.get("actions") or [])
    filtered_path = out / "filtered_dataset.csv"
    filtered.to_csv(filtered_path, index=False)

    signals = _first_action_value(plan, "set_signal_columns", "signal_columns", ["industry_relative_book_yield"])
    costs = _first_action_value(plan, "restrict_costs", "cost_bps_values", [0, 30, 60])
    holdings = _first_action_value(plan, "set_holding_counts", "holding_counts", [50, 75, 100])
    windows = _first_action_value(plan, "set_windows", "year_windows", DEFAULT_YEAR_WINDOWS)

    payload = build_small_institutional_backtest_matrix(
        dataset_path=filtered_path,
        signal_columns=signals,
        year_windows=windows,
        holding_counts=holdings,
        rebalance_frequencies=["monthly"],
        cost_bps_values=costs,
        return_column="forward_return_5d",
        dry_run=False,
    )
    payload = {"schema_version": 1, "status": "ok" if (payload.get("summary") or {}).get("ok_count") else "insufficient_data", "filtered_dataset_path": str(filtered_path), **payload}
    (out / "result.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (out / "result.md").write_text(small_institutional_backtest_matrix_to_markdown(payload), encoding="utf-8")
    return payload
