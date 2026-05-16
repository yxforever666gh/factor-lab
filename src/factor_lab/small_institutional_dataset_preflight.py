from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.small_institutional_simulation_policy import load_small_institutional_simulation_policy

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "dataset_preflight.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "dataset_preflight.md"


def _resolve_path(path: str | Path) -> Path:
    p = Path(path)
    return p if p.is_absolute() else ROOT / p


def _load_dataset(path: str | Path) -> pd.DataFrame:
    p = _resolve_path(path)
    if not p.exists():
        return pd.DataFrame()
    frame = pd.read_csv(p)
    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    return frame


def _window_status(min_date: pd.Timestamp | None, max_date: pd.Timestamp | None, window: dict[str, str]) -> dict[str, Any]:
    start = pd.to_datetime(window["start_date"])
    end = pd.to_datetime(window["end_date"])
    status = "ready" if min_date is not None and max_date is not None and min_date <= end and max_date >= start else "insufficient_date_coverage"
    if status == "ready" and not (min_date <= start and max_date >= end):
        status = "ready_partial_overlap"
    # For execution purposes a partial overlap can run, but it is still coverage risk.
    executable = status in {"ready", "ready_partial_overlap"}
    return {
        "label": window.get("label") or f"{window['start_date']}:{window['end_date']}",
        "start_date": window["start_date"],
        "end_date": window["end_date"],
        "status": "ready" if executable else status,
        "coverage_warning": status == "ready_partial_overlap",
        "executable": executable,
    }


def build_small_institutional_dataset_preflight(
    *,
    dataset_path: str | Path | None = None,
    signal_columns: list[str] | None = None,
    year_windows: list[dict[str, str]] | None = None,
    holding_counts: list[int] | None = None,
    rebalance_frequencies: list[str] | None = None,
    cost_bps_values: list[float] | None = None,
    policy_path: str | Path | None = None,
) -> dict[str, Any]:
    policy = load_small_institutional_simulation_policy(policy_path) if policy_path else load_small_institutional_simulation_policy()
    dataset_ref = dataset_path or policy["dataset_path"]
    dataset_file = _resolve_path(dataset_ref)

    windows = year_windows or policy["year_windows"]
    signals = signal_columns or policy["signal_columns"]
    counts = holding_counts or policy["holding_counts"]
    frequencies = rebalance_frequencies or policy["rebalance_frequencies"]
    costs = cost_bps_values or policy["cost_bps_values"]

    if not dataset_file.exists():
        total = len(windows) * len(signals) * len(counts) * len(frequencies) * len(costs)
        return {
            "schema_version": 1,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "preflight_status": "blocked",
            "next_action": "provide_backtest_dataset",
            "dataset": {"path": str(dataset_file), "exists": False, "row_count": 0, "ticker_count": 0, "min_date": None, "max_date": None},
            "signals": {"requested_columns": signals, "missing_columns": signals},
            "windows": [],
            "estimated_combinations": {"total": total, "ready": 0},
        }

    frame = _load_dataset(dataset_file)
    min_date_ts = frame["date"].min() if "date" in frame.columns and not frame.empty else None
    max_date_ts = frame["date"].max() if "date" in frame.columns and not frame.empty else None
    min_date = str(min_date_ts.date()) if pd.notna(min_date_ts) else None
    max_date = str(max_date_ts.date()) if pd.notna(max_date_ts) else None
    missing_signals = sorted(set(signals) - set(frame.columns))
    windows_payload = [_window_status(min_date_ts if pd.notna(min_date_ts) else None, max_date_ts if pd.notna(max_date_ts) else None, window) for window in windows]
    ready_windows = [window for window in windows_payload if window["executable"]]
    total = len(windows) * len(signals) * len(counts) * len(frequencies) * len(costs)
    ready = len(ready_windows) * max(0, len(signals) - len(missing_signals)) * len(counts) * len(frequencies) * len(costs)

    if missing_signals:
        status = "blocked"
        next_action = "repair_dataset_columns"
    elif ready == 0:
        status = "blocked"
        next_action = "extend_backtest_dataset"
    elif ready < total:
        status = "partial"
        next_action = "extend_backtest_dataset"
    else:
        status = "ready"
        next_action = "run_bounded_matrix_dry_run"

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "preflight_status": status,
        "next_action": next_action,
        "dataset": {
            "path": str(dataset_file),
            "exists": True,
            "row_count": int(len(frame)),
            "ticker_count": int(frame["ticker"].nunique()) if "ticker" in frame.columns else 0,
            "min_date": min_date,
            "max_date": max_date,
        },
        "signals": {"requested_columns": signals, "missing_columns": missing_signals},
        "windows": windows_payload,
        "estimated_combinations": {"total": total, "ready": ready},
    }


def small_institutional_dataset_preflight_to_markdown(payload: dict[str, Any]) -> str:
    dataset = payload.get("dataset") or {}
    signals = payload.get("signals") or {}
    combos = payload.get("estimated_combinations") or {}
    lines = [
        "# Small Institutional Dataset Preflight",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Preflight status: {payload.get('preflight_status')}",
        f"Next action: {payload.get('next_action')}",
        "",
        "## Dataset",
        f"- path: {dataset.get('path')}",
        f"- exists: {dataset.get('exists')}",
        f"- rows: {dataset.get('row_count')}",
        f"- tickers: {dataset.get('ticker_count')}",
        f"- date range: {dataset.get('min_date')} -> {dataset.get('max_date')}",
        "",
        "## Signals",
        f"- missing columns: {signals.get('missing_columns')}",
        "",
        "## Estimated combinations",
        f"- total: {combos.get('total')}",
        f"- ready: {combos.get('ready')}",
        "",
        "## Windows",
    ]
    for window in payload.get("windows") or []:
        lines.append(f"- {window.get('label')}: {window.get('status')} warning={window.get('coverage_warning')}")
    if not payload.get("windows"):
        lines.append("- none")
    return "\n".join(lines) + "\n"


def write_small_institutional_dataset_preflight(
    *,
    json_path: str | Path = DEFAULT_JSON_PATH,
    markdown_path: str | Path = DEFAULT_MARKDOWN_PATH,
    **kwargs: Any,
) -> dict[str, Any]:
    payload = build_small_institutional_dataset_preflight(**kwargs)
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(small_institutional_dataset_preflight_to_markdown(payload), encoding="utf-8")
    return payload
