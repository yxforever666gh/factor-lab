from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.bucket_aware_portfolio import evaluate_bucket_pair_portfolio
from factor_lab.margin_feature_builder import spearman_corr
from factor_lab.shareholder_crowding_source import DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD

DEFAULT_RUN_DIR = Path(
    "artifacts/pledge_controlled_probe_run/"
    "value_quality_high_pledge_record_count_confirmation_20260512T181603Z"
)
DEFAULT_DB_PATH = Path("artifacts/factor_lab.db")
DEFAULT_SOURCE_REPORT = Path("artifacts/pledge_source_mvp/pledge_source_mvp.json")


@dataclass(frozen=True)
class PledgeControlledValidationConfig:
    factor_col: str = "high_pledge_record_count"
    return_col: str = "forward_return_5d"
    date_col: str = "date"
    ticker_col: str = "ticker"
    quantiles: int = 5
    long_quantile: int = 3
    short_quantile: int = 0
    benchmark_spread: float = DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD
    min_observations: int = 100
    min_nonnull_rows: int = 1000
    min_nonnull_tickers: int = 20
    min_split_spread: float = 0.0
    max_abs_corr: float = 0.85


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _first_item(value: Any) -> dict[str, Any]:
    if isinstance(value, list) and value:
        return value[0]
    if isinstance(value, dict):
        return value
    return {}


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [_jsonable(v) for v in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _per_date_bucket_frame(
    dataset: pd.DataFrame,
    *,
    cfg: PledgeControlledValidationConfig,
) -> pd.DataFrame:
    work = dataset[[cfg.date_col, cfg.ticker_col, cfg.factor_col, cfg.return_col]].copy()
    work[cfg.factor_col] = pd.to_numeric(work[cfg.factor_col], errors="coerce")
    work[cfg.return_col] = pd.to_numeric(work[cfg.return_col], errors="coerce")
    work = work.dropna(subset=[cfg.date_col, cfg.ticker_col, cfg.factor_col, cfg.return_col])
    rows: list[dict[str, Any]] = []
    for date, group in work.groupby(cfg.date_col, sort=True):
        if len(group) < cfg.quantiles or group[cfg.factor_col].nunique() < cfg.quantiles:
            continue
        ranked = group.assign(
            bucket=pd.qcut(group[cfg.factor_col].rank(method="first"), cfg.quantiles, labels=False, duplicates="drop")
        )
        if cfg.long_quantile not in set(ranked["bucket"]) or cfg.short_quantile not in set(ranked["bucket"]):
            continue
        long = ranked[ranked["bucket"] == cfg.long_quantile]
        short = ranked[ranked["bucket"] == cfg.short_quantile]
        rows.append(
            {
                "date": str(date),
                "spread": float(long[cfg.return_col].mean() - short[cfg.return_col].mean()),
                "long_count": int(len(long)),
                "short_count": int(len(short)),
                "long_return_mean": float(long[cfg.return_col].mean()),
                "short_return_mean": float(short[cfg.return_col].mean()),
                "date_tickers": int(group[cfg.ticker_col].nunique()),
                "factor_nonnull_rows": int(len(group)),
            }
        )
    return pd.DataFrame(rows)


def _spread_stats(spreads: pd.Series) -> dict[str, Any]:
    values = pd.to_numeric(spreads, errors="coerce").dropna()
    if values.empty:
        return {
            "observations": 0,
            "spread_mean": None,
            "spread_std": None,
            "positive_rate": None,
            "min": None,
            "max": None,
        }
    return {
        "observations": int(len(values)),
        "spread_mean": round(float(values.mean()), 10),
        "spread_std": round(float(values.std(ddof=0)), 10) if len(values) > 1 else 0.0,
        "positive_rate": round(float((values > 0).mean()), 6),
        "min": round(float(values.min()), 10),
        "max": round(float(values.max()), 10),
    }


def _split_diagnostics(per_date: pd.DataFrame) -> dict[str, Any]:
    if per_date.empty:
        return {}
    dates = pd.to_datetime(per_date["date"].astype(str), errors="coerce")
    out: dict[str, Any] = {}
    windows = {
        "2020_2021": dates.dt.year <= 2021,
        "2022_2023": dates.dt.year >= 2022,
        "first_half_dates": pd.Series(range(len(per_date)), index=per_date.index) < len(per_date) / 2,
        "second_half_dates": pd.Series(range(len(per_date)), index=per_date.index) >= len(per_date) / 2,
    }
    for name, mask in windows.items():
        subset = per_date.loc[mask.fillna(False), "spread"]
        out[name] = _spread_stats(subset)
    return out


def _coverage_diagnostics(dataset: pd.DataFrame, *, cfg: PledgeControlledValidationConfig, per_date: pd.DataFrame) -> dict[str, Any]:
    factor = pd.to_numeric(dataset.get(cfg.factor_col), errors="coerce")
    nonnull = dataset.loc[factor.notna()].copy()
    return {
        "dataset_rows": int(len(dataset)),
        "dataset_dates": int(dataset[cfg.date_col].nunique()) if cfg.date_col in dataset.columns else 0,
        "dataset_tickers": int(dataset[cfg.ticker_col].nunique()) if cfg.ticker_col in dataset.columns else 0,
        "factor_nonnull_rows": int(factor.notna().sum()),
        "factor_nonnull_rate": round(float(factor.notna().mean()), 6) if len(factor) else 0.0,
        "factor_nonnull_dates": int(nonnull[cfg.date_col].nunique()) if len(nonnull) and cfg.date_col in nonnull.columns else 0,
        "factor_nonnull_tickers": int(nonnull[cfg.ticker_col].nunique()) if len(nonnull) and cfg.ticker_col in nonnull.columns else 0,
        "bucket_observation_dates": int(len(per_date)),
        "bucket_long_count_min": int(per_date["long_count"].min()) if not per_date.empty else 0,
        "bucket_long_count_median": float(per_date["long_count"].median()) if not per_date.empty else 0.0,
        "bucket_short_count_min": int(per_date["short_count"].min()) if not per_date.empty else 0,
        "bucket_short_count_median": float(per_date["short_count"].median()) if not per_date.empty else 0.0,
    }


def _correlation_diagnostics(dataset: pd.DataFrame, *, cfg: PledgeControlledValidationConfig) -> dict[str, Any]:
    out: dict[str, Any] = {}
    factor = pd.to_numeric(dataset.get(cfg.factor_col), errors="coerce")
    candidates = {
        "value_quality_baseline": None,
        "turnover": "turnover",
        "turnover_shock_5_20": "turnover_shock_5_20",
        "size_inv": "size_inv",
        "pledge_record_count": "pledge_record_count",
        "pledge_ratio_mean": "pledge_ratio_mean",
        "pledge_ratio_max": "pledge_ratio_max",
        "pledge_amount_sum": "pledge_amount_sum",
    }
    if {"industry_relative_book_yield", "roe"}.issubset(dataset.columns):
        baseline = pd.to_numeric(dataset["industry_relative_book_yield"], errors="coerce") + pd.to_numeric(dataset["roe"], errors="coerce")
        out["vs_value_quality_baseline"] = spearman_corr(factor, baseline)
    for label, col in candidates.items():
        if col is None or col not in dataset.columns:
            continue
        out[f"vs_{label}"] = spearman_corr(factor, pd.to_numeric(dataset[col], errors="coerce"))
    return out


def _db_task_audit(db_path: Path, task_state: dict[str, Any], run_dir: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {"db_exists": False, "rows": []}
    task_id = task_state.get("task_id")
    config_path = task_state.get("config_path")
    output_dir = task_state.get("output_dir") or str(run_dir)
    rows: list[dict[str, Any]] = []
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT task_id, task_type, status, priority, created_at_utc, started_at_utc, finished_at_utc, last_error, worker_note, payload_json
            FROM research_tasks
            WHERE task_id = ? OR payload_json LIKE ? OR payload_json LIKE ?
            ORDER BY created_at_utc DESC
            LIMIT 10
            """,
            (task_id, f"%{config_path}%", f"%{output_dir}%"),
        )
        for row in cur.fetchall():
            item = {k: row[k] for k in row.keys() if k != "payload_json"}
            payload = row["payload_json"]
            item["payload_contains_output_dir"] = bool(output_dir and payload and output_dir in payload)
            item["payload_contains_config_path"] = bool(config_path and payload and config_path in payload)
            rows.append(item)
        conn.close()
    except Exception as exc:
        return {"db_exists": True, "error": f"{type(exc).__name__}: {exc}", "rows": []}
    mismatch = [r for r in rows if r.get("status") != task_state.get("status")]
    return {
        "db_exists": True,
        "matched_rows": len(rows),
        "rows": rows,
        "status_mismatch_with_task_state": bool(mismatch),
        "task_state_status": task_state.get("status"),
        "interpretation": "historical_db_status_mismatch_do_not_delete_rows" if mismatch else "db_status_matches_or_no_matching_rows",
    }


def _source_report_comparison(source_report_path: Path, controlled_coverage: dict[str, Any]) -> dict[str, Any]:
    source = _load_json(source_report_path) or {}
    diag = source.get("diagnostic", {}) if isinstance(source, dict) else {}
    cov = diag.get("coverage", {}) if isinstance(diag, dict) else {}
    best = diag.get("best_signal", {}) if isinstance(diag, dict) else {}
    return {
        "source_report_path": str(source_report_path),
        "source_report_exists": source_report_path.exists(),
        "readonly_decision": (diag.get("decision") or {}).get("decision") if isinstance(diag, dict) else None,
        "readonly_best_signal": best,
        "readonly_coverage": {
            "rows": cov.get("rows"),
            "dates": cov.get("dates"),
            "tickers": cov.get("tickers"),
        },
        "controlled_factor_nonnull_rows": controlled_coverage.get("factor_nonnull_rows"),
        "controlled_bucket_observation_dates": controlled_coverage.get("bucket_observation_dates"),
    }


def build_pledge_controlled_validation(
    run_dir: str | Path = DEFAULT_RUN_DIR,
    *,
    db_path: str | Path = DEFAULT_DB_PATH,
    source_report_path: str | Path = DEFAULT_SOURCE_REPORT,
    config: PledgeControlledValidationConfig | None = None,
) -> dict[str, Any]:
    cfg = config or PledgeControlledValidationConfig()
    run = Path(run_dir)
    dataset_path = run / "dataset.csv"
    if not dataset_path.exists():
        return {"decision": {"decision": "stop_pledge_validation_missing_dataset", "reasons": ["missing_dataset_csv"]}, "run_dir": str(run)}
    dataset = pd.read_csv(dataset_path)
    per_date = _per_date_bucket_frame(dataset, cfg=cfg)
    coverage = _coverage_diagnostics(dataset, cfg=cfg, per_date=per_date)
    recomputed = evaluate_bucket_pair_portfolio(
        dataset,
        factor_col=cfg.factor_col,
        return_col=cfg.return_col,
        date_col=cfg.date_col,
        quantiles=cfg.quantiles,
        long_quantile=cfg.long_quantile,
        short_quantile=cfg.short_quantile,
        min_spread=0.0,
    ).to_dict()
    spread_distribution = _spread_stats(per_date["spread"] if "spread" in per_date.columns else pd.Series(dtype=float))
    splits = _split_diagnostics(per_date)
    correlations = _correlation_diagnostics(dataset, cfg=cfg)

    results = _first_item(_load_json(run / "results.json"))
    bucket_result = _first_item(_load_json(run / "bucket_aware_portfolio_results.json"))
    task_state = _load_json(run / "task_state.json") or {}
    timing = _load_json(run / "timing.json") or {}
    db_audit = _db_task_audit(Path(db_path), task_state, run)
    source_comparison = _source_report_comparison(Path(source_report_path), coverage)

    reasons: list[str] = []
    spread = recomputed.get("spread_mean")
    if spread is None or float(spread) <= cfg.benchmark_spread:
        reasons.append("bucket_spread_not_above_benchmark")
    if spread_distribution.get("observations", 0) < cfg.min_observations:
        reasons.append("bucket_observations_too_low")
    if coverage.get("factor_nonnull_rows", 0) < cfg.min_nonnull_rows:
        reasons.append("factor_nonnull_rows_too_low")
    if coverage.get("factor_nonnull_tickers", 0) < cfg.min_nonnull_tickers:
        reasons.append("factor_nonnull_tickers_too_low")
    for split_name in ("2020_2021", "2022_2023"):
        split_spread = splits.get(split_name, {}).get("spread_mean")
        if split_spread is None or float(split_spread) <= cfg.min_split_spread:
            reasons.append(f"split_{split_name}_not_positive")
    for name in ("vs_value_quality_baseline", "vs_turnover", "vs_turnover_shock_5_20"):
        corr = correlations.get(name)
        if corr is not None and abs(float(corr)) >= cfg.max_abs_corr:
            reasons.append(f"high_abs_corr_{name}")
    if task_state.get("status") != "finished":
        reasons.append("task_state_not_finished")

    if reasons:
        decision = "stop_pledge_not_robust"
    else:
        decision = "pledge_validation_pass_prepare_single_followup_plan"

    return _jsonable(
        {
            "run_dir": str(run),
            "scope": "read_only_pledge_controlled_probe_validation_hardening",
            "no_queue_write": True,
            "no_daemon_start": True,
            "no_workflow_run": True,
            "factor_name": cfg.factor_col,
            "benchmark": {"value_quality_no_distress_bucket_spread": cfg.benchmark_spread},
            "reported_standard_result": results,
            "reported_bucket_aware_result": bucket_result,
            "recomputed_bucket_aware_result": recomputed,
            "spread_distribution": spread_distribution,
            "coverage": coverage,
            "split_diagnostics": splits,
            "correlations": correlations,
            "per_date_tail": per_date.tail(5).to_dict(orient="records") if not per_date.empty else [],
            "source_report_comparison": source_comparison,
            "timing": timing,
            "task_state": task_state,
            "db_task_audit": db_audit,
            "decision": {"decision": decision, "reasons": reasons or ["bucket_spread_above_benchmark_splits_positive_not_duplicate"]},
        }
    )


def write_pledge_controlled_validation(
    output_dir: str | Path = "artifacts/pledge_controlled_validation",
    *,
    run_dir: str | Path = DEFAULT_RUN_DIR,
) -> dict[str, Any]:
    report = build_pledge_controlled_validation(run_dir)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "pledge_controlled_validation.json"
    md_path = out / "pledge_controlled_validation.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    decision = report.get("decision", {})
    recomputed = report.get("recomputed_bucket_aware_result", {})
    coverage = report.get("coverage", {})
    splits = report.get("split_diagnostics", {})
    corr = report.get("correlations", {})
    lines = [
        "# Pledge controlled probe validation hardening",
        "",
        "Scope: read-only diagnostics over the completed controlled workflow output. No workflow enqueue, no daemon start, no new API pull.",
        "",
        "## Decision",
        f"- Decision: `{decision.get('decision')}`",
        f"- Reasons: {', '.join(decision.get('reasons') or [])}",
        f"- Recomputed Q3-Q0 spread: `{recomputed.get('spread_mean')}`",
        f"- Benchmark: `{report.get('benchmark', {}).get('value_quality_no_distress_bucket_spread')}`",
        f"- Observations: `{recomputed.get('observations')}`",
        "",
        "## Coverage",
        f"- Dataset rows: {coverage.get('dataset_rows')}",
        f"- Factor non-null rows: {coverage.get('factor_nonnull_rows')} ({coverage.get('factor_nonnull_rate')})",
        f"- Factor non-null dates/tickers: {coverage.get('factor_nonnull_dates')} / {coverage.get('factor_nonnull_tickers')}",
        f"- Bucket observation dates: {coverage.get('bucket_observation_dates')}",
        f"- Bucket long/short median counts: {coverage.get('bucket_long_count_median')} / {coverage.get('bucket_short_count_median')}",
        "",
        "## Split diagnostics",
    ]
    for name, item in splits.items():
        lines.append(f"- {name}: spread={item.get('spread_mean')}, positive_rate={item.get('positive_rate')}, obs={item.get('observations')}")
    lines.extend([
        "",
        "## Correlations",
    ])
    for name, value in corr.items():
        lines.append(f"- {name}: {value}")
    db = report.get("db_task_audit", {})
    lines.extend([
        "",
        "## DB / artifact audit",
        f"- Task state status: {report.get('task_state', {}).get('status')}",
        f"- DB matched rows: {db.get('matched_rows')}",
        f"- DB status mismatch: {db.get('status_mismatch_with_task_state')}",
        f"- Interpretation: {db.get('interpretation')}",
        "",
        "## Interpretation",
        "- 若 decision 为 pass，只能写 exactly-one follow-up controlled probe 计划并先 dry-run admission；本脚本不 enqueue。",
        "- 若 decision 为 stop，则 pledge route 降级为 monitor-only/stop 并切换下一非 ownership/pledge 数据源。",
    ])
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    report["artifact_paths"] = {"json": str(json_path), "md": str(md_path)}
    return report
