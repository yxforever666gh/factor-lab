from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import pandas as pd

from factor_lab.settings import load_env_file
from factor_lab.tushare_provider import TushareDataProvider

BLOCKED_ACTIONS = [
    "controlled_backtest",
    "queue_write",
    "timer_enable",
    "broad_daemon_restore",
    "auto_promotion",
    "live_trading",
]

REQUIRED_PIT_FIELDS = ["profit_yoy", "debt_to_asset", "operating_cashflow_to_profit"]


class PitEnrichmentProvider(Protocol):
    def enrich_frame_with_pit_financial_features(
        self,
        frame: pd.DataFrame,
        *,
        cache_dir: str | Path = "artifacts/tushare_cache",
        timing: Any | None = None,
        retain_pit_cashflow_diagnostics: bool = False,
    ) -> pd.DataFrame: ...


def _coverage(frame: pd.DataFrame, fields: list[str] = REQUIRED_PIT_FIELDS) -> dict[str, float]:
    return {field: float(frame[field].notna().mean()) if field in frame.columns and not frame.empty else 0.0 for field in fields}


def _date_window(frame: pd.DataFrame) -> dict[str, str | None]:
    if frame.empty or "date" not in frame.columns:
        return {"start_date": None, "end_date": None}
    dates = pd.to_datetime(frame["date"])
    return {"start_date": dates.min().strftime("%Y-%m-%d"), "end_date": dates.max().strftime("%Y-%m-%d")}


def _newest_pit_cache(cache_dir: str | Path, before: set[Path]) -> Path | None:
    candidates = [path for path in Path(cache_dir).glob("pit_financial*.csv") if path not in before]
    if not candidates:
        candidates = list(Path(cache_dir).glob("pit_financial*.csv"))
    return max(candidates, key=lambda path: path.stat().st_mtime) if candidates else None


def run_pit_cache_extension(
    *,
    run_id: str,
    extension_plan: dict[str, Any],
    base_frame: pd.DataFrame,
    base_path: str,
    cache_dir: str | Path,
    output_dir: str | Path,
    provider: PitEnrichmentProvider | None = None,
    token_env_var: str = "TUSHARE_TOKEN",
    retain_pit_cashflow_diagnostics: bool = True,
) -> dict[str, Any]:
    load_env_file()
    data_source_configured = bool(os.environ.get(token_env_var))
    output_dir = Path(output_dir)
    cache_dir = Path(cache_dir)
    if not data_source_configured:
        return {
            "schema_version": 1,
            "run_id": run_id,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "mode": "pit_cache_extension_run",
            "execution_status": "blocked",
            "failure_reason": "tushare_token_not_configured",
            "data_source_configured": False,
            "base_path": base_path,
            "output_pit_path": None,
            "output_feature_frame_path": None,
            "coverage_after_extension": _coverage(base_frame),
            "coverage_pass": False,
            "recommended_next_step": "configure_tushare_token",
            "controlled_execution_allowed": False,
            "queue_write_allowed": False,
            "timer_enable_allowed": False,
            "blocked_actions": BLOCKED_ACTIONS,
        }
    provider = provider or TushareDataProvider()
    before = set(cache_dir.glob("pit_financial*.csv")) if cache_dir.exists() else set()
    try:
        enriched = provider.enrich_frame_with_pit_financial_features(
            base_frame,
            cache_dir=cache_dir,
            retain_pit_cashflow_diagnostics=retain_pit_cashflow_diagnostics,
        )
        execution_status = "completed"
        failure_reason = None
    except Exception as exc:  # pragma: no cover - exercised through script or integration failures
        enriched = base_frame.copy()
        execution_status = "failed"
        failure_reason = f"pit_extension_failed:{type(exc).__name__}:{exc}"
    coverage_after = _coverage(enriched)
    target = float(extension_plan.get("target_overlay_coverage") or 0.60)
    coverage_pass = all(value >= target for value in coverage_after.values()) and execution_status == "completed"
    output_dir.mkdir(parents=True, exist_ok=True)
    feature_frame_path = output_dir / "quality_profit_proxy_feature_frame_with_pit.csv"
    enriched.to_csv(feature_frame_path, index=False)
    pit_path = _newest_pit_cache(cache_dir, before)
    if execution_status == "failed":
        recommended_next_step = "inspect_data_source_or_retry_later"
    elif coverage_pass:
        recommended_next_step = "rerun_proxy_field_resolution_with_pit_overlay"
    else:
        recommended_next_step = "stop_proxy_route_or_reduce_universe"
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "pit_cache_extension_run",
        "execution_status": execution_status,
        "failure_reason": failure_reason,
        "data_source_configured": data_source_configured,
        "base_path": base_path,
        "base_rows": int(len(base_frame)),
        "base_ticker_count": int(base_frame["ticker"].nunique()) if "ticker" in base_frame.columns else 0,
        "base_date_window": _date_window(base_frame),
        "output_pit_path": str(pit_path) if pit_path else None,
        "output_feature_frame_path": str(feature_frame_path),
        "coverage_after_extension": coverage_after,
        "coverage_target": target,
        "chunk_mode": extension_plan.get("chunk_mode"),
        "coverage_pass": coverage_pass,
        "recommended_next_step": recommended_next_step,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": BLOCKED_ACTIONS,
    }


def pit_cache_extension_run_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# PIT Cache Extension Run",
        "",
        f"execution_status: {report.get('execution_status')}",
        f"failure_reason: {report.get('failure_reason')}",
        f"data_source_configured: {report.get('data_source_configured')}",
        f"coverage_pass: {report.get('coverage_pass')}",
        f"recommended_next_step: {report.get('recommended_next_step')}",
        f"controlled_execution_allowed: {report.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "## Outputs",
        f"- output_pit_path: {report.get('output_pit_path')}",
        f"- output_feature_frame_path: {report.get('output_feature_frame_path')}",
        "",
        "## Coverage after extension",
        f"- chunk_mode: {report.get('chunk_mode')}",
    ]
    for field, value in (report.get("coverage_after_extension") or {}).items():
        lines.append(f"- {field}: {value}")
    lines += ["", "## Blocked actions"]
    lines.extend(f"- {action}" for action in report.get("blocked_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_pit_cache_extension_run(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "pit_cache_extension_run.json"
    markdown_path = out / "pit_cache_extension_run.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(pit_cache_extension_run_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
