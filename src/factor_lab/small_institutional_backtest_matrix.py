from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATASET_PATH = ROOT / "artifacts" / "value_route_bucket_aware" / "runs" / "value_quality_no_distress_bucket_aware" / "dataset.csv"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "matrix.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "matrix.md"
DEFAULT_YEAR_WINDOWS = [
    {"label": "2020-2021", "start_date": "2020-01-01", "end_date": "2021-12-31"},
    {"label": "2021-2022", "start_date": "2021-01-01", "end_date": "2022-12-31"},
    {"label": "2022-2023", "start_date": "2022-01-01", "end_date": "2023-12-31"},
]
DEFAULT_HOLDING_COUNTS = [50, 75, 100]
DEFAULT_REBALANCE_FREQUENCIES = ["monthly", "biweekly"]
DEFAULT_COST_BPS_VALUES = [0, 30, 60]


def load_dataset(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    frame = pd.read_csv(p)
    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"])
    return frame


def deterministic_combo_id(combo: dict[str, Any]) -> str:
    canonical = json.dumps(combo, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _load_existing_results(path: str | Path | None) -> dict[str, dict[str, Any]]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    existing: dict[str, dict[str, Any]] = {}
    for row in payload.get("results") or []:
        combo_id = row.get("combo_id")
        if combo_id:
            existing[str(combo_id)] = row
    return existing


def _frequency_step(rebalance_frequency: str) -> int:
    if rebalance_frequency == "monthly":
        return 1
    if rebalance_frequency == "biweekly":
        return 1
    return 1


def _max_drawdown(period_returns: list[float]) -> float:
    if not period_returns:
        return 0.0
    equity = np.cumprod([1.0 + value for value in period_returns])
    peaks = np.maximum.accumulate(equity)
    drawdowns = equity / peaks - 1.0
    return round(float(drawdowns.min()), 6)


def _annualized_sharpe(period_returns: list[float], periods_per_year: int = 52) -> float:
    if len(period_returns) < 2:
        return 0.0
    series = pd.Series(period_returns)
    std = float(series.std(ddof=1))
    if std <= 1e-12:
        return 0.0
    return round(float(series.mean() / std * np.sqrt(periods_per_year)), 6)


def run_long_only_backtest(
    dataset: pd.DataFrame,
    *,
    signal_column: str,
    start_date: str,
    end_date: str,
    holding_count: int,
    rebalance_frequency: str,
    cost_bps: float,
    return_column: str = "forward_return_5d",
) -> dict[str, Any]:
    required = {"date", "ticker", signal_column, return_column}
    missing = sorted(required - set(dataset.columns))
    if missing:
        return {
            "status": "insufficient_data",
            "reason": "missing_columns",
            "missing_columns": missing,
            "rebalance_count": 0,
        }

    frame = dataset.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame[signal_column] = pd.to_numeric(frame[signal_column], errors="coerce")
    frame[return_column] = pd.to_numeric(frame[return_column], errors="coerce")
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    frame = frame[(frame["date"] >= start) & (frame["date"] <= end)].dropna(subset=[signal_column, return_column])
    if frame.empty:
        return {
            "status": "insufficient_data",
            "reason": "empty_window",
            "rebalance_count": 0,
            "holding_count": holding_count,
            "rebalance_frequency": rebalance_frequency,
            "cost_bps": cost_bps,
            "start_date": start_date,
            "end_date": end_date,
        }

    period_returns: list[float] = []
    turnovers: list[float] = []
    previous_holdings: set[str] | None = None
    dates = sorted(frame["date"].dropna().unique())
    step = _frequency_step(rebalance_frequency)
    for date_value in dates[::step]:
        current = frame[frame["date"] == date_value].sort_values(signal_column, ascending=False)
        selected = current.head(max(1, int(holding_count))).copy()
        if selected.empty:
            continue
        holdings = set(selected["ticker"].astype(str))
        gross_return = float(selected[return_column].mean())
        if previous_holdings is None:
            turnover = 1.0
        else:
            overlap = len(holdings & previous_holdings)
            base = max(len(holdings), len(previous_holdings), 1)
            turnover = 1.0 - overlap / base
        cost = turnover * float(cost_bps) / 10000.0
        period_returns.append(gross_return - cost)
        turnovers.append(turnover)
        previous_holdings = holdings

    if not period_returns:
        return {
            "status": "insufficient_data",
            "reason": "no_rebalance_returns",
            "rebalance_count": 0,
            "holding_count": holding_count,
            "rebalance_frequency": rebalance_frequency,
            "cost_bps": cost_bps,
            "start_date": start_date,
            "end_date": end_date,
        }

    total_return = float(np.prod([1.0 + value for value in period_returns]) - 1.0)
    win_rate = float(np.mean([value > 0 for value in period_returns]))
    return {
        "status": "ok",
        "start_date": start_date,
        "end_date": end_date,
        "holding_count": int(holding_count),
        "rebalance_frequency": rebalance_frequency,
        "cost_bps": float(cost_bps),
        "rebalance_count": len(period_returns),
        "period_return_mean": round(float(np.mean(period_returns)), 6),
        "period_return_std": round(float(np.std(period_returns, ddof=1)) if len(period_returns) > 1 else 0.0, 6),
        "total_return": round(total_return, 6),
        "annualized_return_proxy": round(float(np.mean(period_returns) * 52), 6),
        "sharpe": _annualized_sharpe(period_returns),
        "max_drawdown": _max_drawdown(period_returns),
        "win_rate": round(win_rate, 6),
        "turnover_mean": round(float(np.mean(turnovers)), 6),
    }


def _build_combinations(
    *,
    signal_columns: list[str],
    year_windows: list[dict[str, str]],
    holding_counts: list[int],
    rebalance_frequencies: list[str],
    cost_bps_values: list[float],
) -> list[dict[str, Any]]:
    combos: list[dict[str, Any]] = []
    for signal in signal_columns:
        for window in year_windows:
            for count in holding_counts:
                for frequency in rebalance_frequencies:
                    for cost in cost_bps_values:
                        combo = {
                            "signal_column": signal,
                            "label": window.get("label") or f"{window['start_date']}:{window['end_date']}",
                            "start_date": window["start_date"],
                            "end_date": window["end_date"],
                            "holding_count": int(count),
                            "rebalance_frequency": frequency,
                            "cost_bps": float(cost),
                        }
                        combo["combo_id"] = deterministic_combo_id(combo)
                        combos.append(combo)
    return combos


def build_small_institutional_backtest_matrix(
    *,
    dataset_path: str | Path = DEFAULT_DATASET_PATH,
    signal_column: str = "industry_relative_book_yield",
    signal_columns: list[str] | None = None,
    year_windows: list[dict[str, str]] | None = None,
    holding_counts: list[int] | None = None,
    rebalance_frequencies: list[str] | None = None,
    cost_bps_values: list[float] | None = None,
    return_column: str = "forward_return_5d",
    max_combinations: int | None = None,
    dry_run: bool = False,
    resume_from_path: str | Path | None = None,
) -> dict[str, Any]:
    dataset = load_dataset(dataset_path)
    windows = year_windows or DEFAULT_YEAR_WINDOWS
    counts = holding_counts or DEFAULT_HOLDING_COUNTS
    frequencies = rebalance_frequencies or DEFAULT_REBALANCE_FREQUENCIES
    costs = cost_bps_values or DEFAULT_COST_BPS_VALUES
    signals = signal_columns or [signal_column]
    combos = _build_combinations(
        signal_columns=signals,
        year_windows=windows,
        holding_counts=counts,
        rebalance_frequencies=frequencies,
        cost_bps_values=costs,
    )
    existing = _load_existing_results(resume_from_path)
    cap = max_combinations if max_combinations is not None else len(combos)
    runnable = combos[: max(0, int(cap))]

    results: list[dict[str, Any]] = []
    skipped_existing_count = 0
    executed_count = 0
    if not dry_run:
        for combo in runnable:
            if combo["combo_id"] in existing:
                results.append(existing[combo["combo_id"]])
                skipped_existing_count += 1
                continue
            row = run_long_only_backtest(
                dataset,
                signal_column=combo["signal_column"],
                start_date=combo["start_date"],
                end_date=combo["end_date"],
                holding_count=combo["holding_count"],
                rebalance_frequency=combo["rebalance_frequency"],
                cost_bps=combo["cost_bps"],
                return_column=return_column,
            )
            row.update(combo)
            results.append(row)
            executed_count += 1

    ok_results = [row for row in results if row.get("status") == "ok"]
    insufficient = [row for row in results if row.get("status") != "ok"]
    best = max(ok_results, key=lambda item: (float(item.get("total_return") or 0.0), float(item.get("sharpe") or 0.0)), default=None)
    if dry_run:
        matrix_status = "dry_run"
    else:
        matrix_status = "ok" if ok_results and not insufficient and len(results) == len(combos) else "partial" if ok_results else "insufficient_data"
    return {
        "schema_version": 2,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "matrix_status": matrix_status,
        "dataset_path": str(dataset_path),
        "signal_column": signals[0] if len(signals) == 1 else None,
        "signal_columns": signals,
        "return_column": return_column,
        "parameter_grid": {
            "year_windows": windows,
            "holding_counts": counts,
            "rebalance_frequencies": frequencies,
            "cost_bps_values": costs,
            "combination_count": len(combos),
        },
        "execution": {
            "dry_run": dry_run,
            "planned_count": len(combos),
            "cap": cap,
            "capped": len(runnable) < len(combos),
            "executed_count": executed_count,
            "skipped_existing_count": skipped_existing_count,
            "result_count": len(results),
        },
        "summary": {
            "ok_count": len(ok_results),
            "insufficient_data_count": len(insufficient),
            "result_count": len(results),
        },
        "best_result": best,
        "planned_combinations": combos if dry_run else combos[: min(len(combos), 20)],
        "results": results,
    }


def small_institutional_backtest_matrix_to_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    grid = payload.get("parameter_grid") or {}
    execution = payload.get("execution") or {}
    best = payload.get("best_result") or {}
    lines = [
        "# Small Institutional Backtest Matrix",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Matrix status: {payload.get('matrix_status')}",
        f"Signal column: {payload.get('signal_column')}",
        f"Signal columns: {payload.get('signal_columns')}",
        f"Return column: {payload.get('return_column')}",
        "",
        "## Summary",
        f"- combination_count: {grid.get('combination_count')}",
        f"- ok_count: {summary.get('ok_count')}",
        f"- insufficient_data_count: {summary.get('insufficient_data_count')}",
        "",
        "## Execution",
        f"- dry_run: {execution.get('dry_run')}",
        f"- planned_count: {execution.get('planned_count')}",
        f"- executed_count: {execution.get('executed_count')}",
        f"- skipped_existing_count: {execution.get('skipped_existing_count')}",
        f"- capped: {execution.get('capped')}",
        "",
        "## Best result",
        f"- label: {best.get('label')}",
        f"- signal_column: {best.get('signal_column')}",
        f"- holding_count: {best.get('holding_count')}",
        f"- rebalance_frequency: {best.get('rebalance_frequency')}",
        f"- cost_bps: {best.get('cost_bps')}",
        f"- total_return: {best.get('total_return')}",
        f"- sharpe: {best.get('sharpe')}",
        f"- max_drawdown: {best.get('max_drawdown')}",
        "",
        "## Top rows",
    ]
    ok_rows = [row for row in payload.get("results") or [] if row.get("status") == "ok"]
    top_rows = sorted(ok_rows, key=lambda item: float(item.get("total_return") or 0.0), reverse=True)[:10]
    if top_rows:
        for row in top_rows:
            lines.append(
                f"- {row.get('label')} signal={row.get('signal_column')} holdings={row.get('holding_count')} freq={row.get('rebalance_frequency')} cost={row.get('cost_bps')} total_return={row.get('total_return')} sharpe={row.get('sharpe')} drawdown={row.get('max_drawdown')}"
            )
    else:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def write_small_institutional_backtest_matrix(
    *,
    dataset_path: str | Path = DEFAULT_DATASET_PATH,
    signal_column: str = "industry_relative_book_yield",
    signal_columns: list[str] | None = None,
    year_windows: list[dict[str, str]] | None = None,
    holding_counts: list[int] | None = None,
    rebalance_frequencies: list[str] | None = None,
    cost_bps_values: list[float] | None = None,
    return_column: str = "forward_return_5d",
    max_combinations: int | None = None,
    dry_run: bool = False,
    resume_from_path: str | Path | None = None,
    json_path: str | Path = DEFAULT_JSON_PATH,
    markdown_path: str | Path = DEFAULT_MARKDOWN_PATH,
) -> dict[str, Any]:
    resume_path = resume_from_path or (json_path if Path(json_path).exists() else None)
    payload = build_small_institutional_backtest_matrix(
        dataset_path=dataset_path,
        signal_column=signal_column,
        signal_columns=signal_columns,
        year_windows=year_windows,
        holding_counts=holding_counts,
        rebalance_frequencies=rebalance_frequencies,
        cost_bps_values=cost_bps_values,
        return_column=return_column,
        max_combinations=max_combinations,
        dry_run=dry_run,
        resume_from_path=resume_path,
    )
    json_out = Path(json_path)
    markdown_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(small_institutional_backtest_matrix_to_markdown(payload), encoding="utf-8")
    return payload
