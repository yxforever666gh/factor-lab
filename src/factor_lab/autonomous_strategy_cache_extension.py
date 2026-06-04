from __future__ import annotations

import json
import os
import re
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from factor_lab.settings import load_env_file
except Exception:  # pragma: no cover - fallback for standalone import edge cases
    load_env_file = None

_CACHE_NAME_RE = re.compile(r"tushare_(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})_(?P<limit>\d+)\.csv")
REQUIRED_HISTORY_SOURCE_COLUMNS = {"date", "ticker", "pb", "pe_ttm", "forward_return_5d"}


def _parse_cache_name(path: str | Path) -> dict[str, Any]:
    match = _CACHE_NAME_RE.search(Path(path).name)
    if not match:
        return {}
    return {"start_date": match.group("start"), "end_date": match.group("end"), "universe_limit": int(match.group("limit"))}


def _read_source_frame(source_path: str | Path, root: str | Path = ".") -> pd.DataFrame:
    path = Path(source_path)
    if not path.is_absolute():
        path = Path(root) / path
    return pd.read_csv(path, usecols=lambda col: col in REQUIRED_HISTORY_SOURCE_COLUMNS)


def _find_covering_cache(*, cache_dir: str | Path, target_start_date: str, target_end_date: str, min_universe_count: int) -> str | None:
    cache_dir = Path(cache_dir)
    target_start = pd.Timestamp(target_start_date)
    target_end = pd.Timestamp(target_end_date)
    candidates: list[tuple[int, Path]] = []
    for path in cache_dir.glob("tushare_*.csv"):
        parsed = _parse_cache_name(path)
        if not parsed:
            continue
        if pd.Timestamp(parsed["start_date"]) > target_start or pd.Timestamp(parsed["end_date"]) < target_end:
            continue
        try:
            header = set(pd.read_csv(path, nrows=0).columns)
            if not REQUIRED_HISTORY_SOURCE_COLUMNS.issubset(header):
                continue
            tickers = pd.read_csv(path, usecols=["ticker"])["ticker"].nunique()
        except Exception:
            continue
        if tickers >= min_universe_count:
            candidates.append((path.stat().st_size, path))
    if not candidates:
        return None
    return str(max(candidates, key=lambda item: item[0])[1])


def build_history_cache_extension_plan(
    *,
    run_id: str,
    coverage_preflight: dict[str, Any],
    root: str | Path = ".",
    cache_dir: str | Path = "artifacts/tushare_cache",
    min_observations: int = 756,
    calendar_day_multiplier: float = 1.60,
    token_env_var: str = "TUSHARE_TOKEN",
) -> dict[str, Any]:
    source_path = coverage_preflight.get("source_path")
    if load_env_file is not None:
        load_env_file()
    if not source_path:
        return {
            "schema_version": 1,
            "run_id": run_id,
            "mode": "cache_extension_plan",
            "execution_status": "blocked_missing_source_cache",
            "external_request_required": False,
            "queue_write_allowed": False,
            "controlled_execution_allowed": False,
            "next_allowed_actions": ["rerun_coverage_preflight"],
        }

    frame = _read_source_frame(source_path, root=root)
    frame["date"] = pd.to_datetime(frame["date"])
    tickers = sorted(str(ticker) for ticker in frame["ticker"].dropna().unique())
    current_start = str(frame["date"].min().date()) if len(frame) else coverage_preflight.get("date_min")
    current_end = str(frame["date"].max().date()) if len(frame) else coverage_preflight.get("date_max")
    filename_dates = _parse_cache_name(source_path)
    base_start = pd.Timestamp(filename_dates.get("start_date") or current_start)
    target_start = (base_start - timedelta(days=int(min_observations * calendar_day_multiplier))).date().isoformat()
    target_end = str(filename_dates.get("end_date") or current_end)
    target_cache_name = f"tushare_{target_start}_{target_end}_{len(tickers)}.csv"
    target_cache_path = str(Path(cache_dir) / target_cache_name)
    covering_cache = _find_covering_cache(
        cache_dir=Path(root) / cache_dir if not Path(cache_dir).is_absolute() else cache_dir,
        target_start_date=target_start,
        target_end_date=target_end,
        min_universe_count=max(1, len(tickers)),
    )
    overall_status = coverage_preflight.get("overall_status")
    if overall_status == "pass":
        action = "no_fetch_needed"
        external_required = False
        next_actions = ["proceed_to_information_screen"]
    elif covering_cache:
        action = "reuse_covering_cache"
        external_required = False
        next_actions = ["rerun_coverage_preflight_with_covering_cache"]
    else:
        action = "fetch_required"
        external_required = True
        next_actions = ["review_fetch_plan", "run_with_allow_fetch"] if os.environ.get(token_env_var) else ["configure_tushare_token", "review_fetch_plan"]

    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "cache_extension_plan",
        "execution_status": "dry_run_plan_written",
        "coverage_overall_status": overall_status,
        "source_path": source_path,
        "source_date_min": current_start,
        "source_date_max": current_end,
        "source_ticker_count": len(tickers),
        "blocked_fields": [
            item.get("derived_field")
            for item in coverage_preflight.get("field_coverage") or []
            if item.get("status") != "pass"
        ],
        "min_observations": int(min_observations),
        "required_source_columns": sorted(REQUIRED_HISTORY_SOURCE_COLUMNS),
        "action": action,
        "external_request_required": external_required,
        "token_env_var": token_env_var,
        "token_configured": bool(os.environ.get(token_env_var)),
        "target_start_date": target_start,
        "target_end_date": target_end,
        "target_universe_count": len(tickers),
        "target_universe_codes": tickers,
        "target_cache_path": target_cache_path,
        "covering_cache_path": covering_cache,
        "queue_write_allowed": False,
        "controlled_execution_allowed": False,
        "timer_enable_allowed": False,
        "auto_promotion_allowed": False,
        "next_allowed_actions": next_actions,
        "blocked_actions": [
            "queue_write",
            "full_backtest",
            "timer_enable",
            "broad_daemon_restore",
            "auto_promotion",
        ],
    }


def cache_extension_plan_to_markdown(plan: dict[str, Any]) -> str:
    lines = [
        "# Autonomous Strategy Cache Extension Plan",
        "",
        f"run_id: {plan.get('run_id')}",
        f"execution_status: {plan.get('execution_status')}",
        f"coverage_overall_status: {plan.get('coverage_overall_status')}",
        f"action: {plan.get('action')}",
        f"external_request_required: {plan.get('external_request_required')}",
        f"token_configured: {plan.get('token_configured')}",
        f"source_path: {plan.get('source_path')}",
        f"target_cache_path: {plan.get('target_cache_path')}",
        f"target_date_range: {plan.get('target_start_date')} → {plan.get('target_end_date')}",
        f"target_universe_count: {plan.get('target_universe_count')}",
        f"queue_write_allowed: {plan.get('queue_write_allowed')}",
        f"controlled_execution_allowed: {plan.get('controlled_execution_allowed')}",
        "",
        "## Blocked fields",
    ]
    lines.extend(f"- {field}" for field in plan.get("blocked_fields") or [])
    lines.append("")
    lines.append("## Next allowed actions")
    lines.extend(f"- {action}" for action in plan.get("next_allowed_actions") or [])
    lines.append("")
    lines.append("## Blocked actions")
    lines.extend(f"- {action}" for action in plan.get("blocked_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_cache_extension_plan(plan: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "cache_extension_plan.json"
    md_path = out / "cache_extension_plan.md"
    json_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(cache_extension_plan_to_markdown(plan), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
