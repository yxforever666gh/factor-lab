from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from factor_lab.data_cache import ensure_feature_coverage, inspect_feature_store_coverage, slice_feature_store
from factor_lab.small_institutional_simulation_policy import load_small_institutional_simulation_policy
from factor_lab.tushare_provider import TushareDataProvider, TushareRequest
from factor_lab.universe import default_universe_name

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "dataset_extension.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "dataset_extension.md"


class _LazyTushareDataProvider:
    """Construct the credential-backed provider only when a fetch callback uses it."""

    def __init__(self, factory: Callable[[], TushareDataProvider]) -> None:
        self._factory = factory
        self._provider: TushareDataProvider | None = None

    def _resolve(self) -> TushareDataProvider:
        if self._provider is None:
            self._provider = self._factory()
        return self._provider

    def __getattr__(self, name: str) -> Any:
        return getattr(self._resolve(), name)


def _resolve_path(path: str | Path) -> Path:
    p = Path(path)
    return p if p.is_absolute() else ROOT / p


def _required_window(policy: dict[str, Any]) -> dict[str, str]:
    windows = policy.get("year_windows") or []
    if not windows:
        return {"start_date": policy.get("start_date") or "", "end_date": policy.get("end_date") or ""}
    starts = [pd.Timestamp(item["start_date"]) for item in windows]
    ends = [pd.Timestamp(item["end_date"]) for item in windows]
    return {"start_date": min(starts).strftime("%Y-%m-%d"), "end_date": max(ends).strftime("%Y-%m-%d")}


def _dataset_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "row_count": 0, "ticker_count": 0, "min_date": None, "max_date": None, "columns": []}
    frame = pd.read_csv(path)
    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    return {
        "path": str(path),
        "exists": True,
        "row_count": int(len(frame)),
        "ticker_count": int(frame["ticker"].nunique()) if "ticker" in frame.columns else 0,
        "min_date": frame["date"].min().strftime("%Y-%m-%d") if "date" in frame.columns and not frame.empty else None,
        "max_date": frame["date"].max().strftime("%Y-%m-%d") if "date" in frame.columns and not frame.empty else None,
        "columns": list(frame.columns),
    }


def _required_columns(policy: dict[str, Any]) -> list[str]:
    return sorted(set(["date", "ticker", policy.get("return_column") or "forward_return_5d", *(policy.get("signal_columns") or [])]))


def _missing_ranges(current: dict[str, Any], required: dict[str, str]) -> list[dict[str, str]]:
    if not current.get("exists") or not current.get("min_date") or not current.get("max_date"):
        return [required]
    ranges: list[dict[str, str]] = []
    req_start = pd.Timestamp(required["start_date"])
    req_end = pd.Timestamp(required["end_date"])
    cur_min = pd.Timestamp(current["min_date"])
    cur_max = pd.Timestamp(current["max_date"])
    if cur_min > req_start:
        ranges.append({"start_date": required["start_date"], "end_date": cur_min.strftime("%Y-%m-%d")})
    if cur_max < req_end:
        ranges.append({"start_date": cur_max.strftime("%Y-%m-%d"), "end_date": required["end_date"]})
    return ranges


def build_small_institutional_dataset_extension_plan(
    *,
    policy_path: str | Path | None = None,
    cache_dir: str | Path = "artifacts/tushare_cache",
    inspect_coverage_fn: Callable[..., dict[str, Any]] = inspect_feature_store_coverage,
) -> dict[str, Any]:
    policy = load_small_institutional_simulation_policy(policy_path) if policy_path else load_small_institutional_simulation_policy()
    dataset_path = _resolve_path(policy["dataset_path"])
    required = _required_window(policy)
    current = _dataset_summary(dataset_path)
    universe_limit = int(policy.get("universe_limit") or max(policy.get("holding_counts") or [100]))
    universe_name = str(policy.get("universe_name") or default_universe_name(universe_limit))
    cache_dir_str = str(cache_dir)
    coverage = inspect_coverage_fn(universe_name=universe_name, start_date=required["start_date"], end_date=required["end_date"], cache_dir=cache_dir_str)
    required_cols = _required_columns(policy)
    current_missing_cols = sorted(set(required_cols) - set(current.get("columns") or []))

    if coverage.get("available") and coverage.get("covers_exact"):
        status = "ready_from_feature_store"
        api_fetch_required = False
        next_action = "write_extended_dataset_from_feature_store"
    else:
        status = "needs_external_fetch"
        api_fetch_required = True
        next_action = "run_with_write_and_allow_fetch"

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "extension_status": status,
        "next_action": next_action,
        "write_performed": False,
        "api_fetch_required": api_fetch_required,
        "dataset_path": str(dataset_path),
        "cache_dir": cache_dir_str,
        "universe_limit": universe_limit,
        "universe_name": universe_name,
        "required_window": required,
        "required_columns": required_cols,
        "current_dataset": current,
        "current_missing_columns": current_missing_cols,
        "missing_date_ranges": _missing_ranges(current, required),
        "feature_store_coverage": coverage,
    }


def _validate_extended_dataset(frame: pd.DataFrame, required_window: dict[str, str], required_columns: list[str]) -> dict[str, Any]:
    if frame.empty:
        return {"valid": False, "covers_required_window": False, "missing_columns": required_columns, "min_date": None, "max_date": None, "row_count": 0}
    work = frame.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    missing_cols = sorted(set(required_columns) - set(work.columns))
    min_date = work["date"].min()
    max_date = work["date"].max()
    req_start = pd.Timestamp(required_window["start_date"])
    req_end = pd.Timestamp(required_window["end_date"])
    # A requested calendar end can fall on a non-trading day; allow a one-week label/end buffer.
    # Dataset rows are trading observations, so calendar boundaries can fall on non-trading days
    # or after an initial month-end in tests/feature-store samples. Keep this bounded so large gaps
    # such as a June start for a January window still fail validation.
    covers = bool(
        pd.notna(min_date)
        and pd.notna(max_date)
        and min_date <= req_start + pd.Timedelta(days=31)
        and max_date >= req_end - pd.Timedelta(days=14)
    )
    # Align with dataset preflight semantics: a partial early-window overlap is runnable but risky.
    # This lets an explicit --allow-fetch write proceed when Tushare/cache cannot provide the
    # first calendar months, while still requiring data through the requested end date.
    partial_start_overlap = bool(
        pd.notna(min_date)
        and pd.notna(max_date)
        and min_date <= req_end
        and max_date >= req_end - pd.Timedelta(days=14)
    )
    coverage_warning = bool(partial_start_overlap and not covers)
    return {
        "valid": (covers or partial_start_overlap) and not missing_cols,
        "covers_required_window": covers,
        "coverage_warning": coverage_warning,
        "missing_columns": missing_cols,
        "min_date": min_date.strftime("%Y-%m-%d") if pd.notna(min_date) else None,
        "max_date": max_date.strftime("%Y-%m-%d") if pd.notna(max_date) else None,
        "row_count": int(len(work)),
    }


def _direct_provider_fetch(*, provider: TushareDataProvider, start_date: str, end_date: str, universe_limit: int, cache_dir: str) -> Any:
    request = TushareRequest(
        start_date=start_date,
        end_date=end_date,
        universe_limit=universe_limit,
        cache_dir=cache_dir,
        use_request_cache=True,
    )
    return provider.load_dataset(request)


def small_institutional_dataset_extension_to_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Small Institutional Dataset Extension",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Extension status: {payload.get('extension_status')}",
        f"Next action: {payload.get('next_action')}",
        f"Write performed: {payload.get('write_performed')}",
        f"API fetch required: {payload.get('api_fetch_required')}",
        "",
        "## Dataset",
        f"- path: {payload.get('dataset_path')}",
        f"- required window: {payload.get('required_window')}",
        f"- missing date ranges: {payload.get('missing_date_ranges')}",
        f"- current missing columns: {payload.get('current_missing_columns')}",
        "",
        "## Feature store",
        f"- universe: {payload.get('universe_name')}",
        f"- coverage: {payload.get('feature_store_coverage')}",
    ]
    if payload.get("validation"):
        lines.extend(["", "## Validation"])
        for key, value in payload["validation"].items():
            lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def extend_small_institutional_dataset(
    *,
    policy_path: str | Path | None = None,
    cache_dir: str | Path = "artifacts/tushare_cache",
    write: bool = False,
    allow_fetch: bool = False,
    json_path: str | Path = DEFAULT_JSON_PATH,
    markdown_path: str | Path = DEFAULT_MARKDOWN_PATH,
    inspect_coverage_fn: Callable[..., dict[str, Any]] = inspect_feature_store_coverage,
    ensure_coverage_fn: Callable[..., str] = ensure_feature_coverage,
    slice_feature_store_fn: Callable[..., Any] = slice_feature_store,
    direct_fetch_fn: Callable[..., Any] = _direct_provider_fetch,
) -> dict[str, Any]:
    plan = build_small_institutional_dataset_extension_plan(policy_path=policy_path, cache_dir=cache_dir, inspect_coverage_fn=inspect_coverage_fn)
    if not write:
        payload = plan
    elif plan["api_fetch_required"] and not allow_fetch:
        payload = {**plan, "extension_status": "blocked_external_fetch_not_allowed", "next_action": "rerun_with_allow_fetch_or_seed_feature_store", "write_performed": False}
    else:
        provider = _LazyTushareDataProvider(TushareDataProvider)
        if plan["api_fetch_required"] and allow_fetch:
            ensure_coverage_fn(
                provider=provider,
                universe_limit=plan["universe_limit"],
                start_date=plan["required_window"]["start_date"],
                end_date=plan["required_window"]["end_date"],
                cache_dir=str(cache_dir),
                universe_name=plan["universe_name"],
            )
        sliced = slice_feature_store_fn(
            universe_name=plan["universe_name"],
            start_date=plan["required_window"]["start_date"],
            end_date=plan["required_window"]["end_date"],
            cache_dir=str(cache_dir),
        )
        frame = sliced.frame if hasattr(sliced, "frame") else sliced
        frame = frame.copy()
        validation = _validate_extended_dataset(frame, plan["required_window"], plan["required_columns"])
        fallback_fetch_performed = False
        if not validation["valid"] and allow_fetch:
            # The feature-store metadata can cover the calendar end while the actual parquet has an
            # interior hole (for example 2022-2023 missing but 2024+ present). In explicit fetch mode,
            # repair by asking the provider for the full required simulation window through its normal
            # request-cache/covering-cache path, then validate that frame before touching the dataset.
            fetched = direct_fetch_fn(
                provider=provider,
                start_date=plan["required_window"]["start_date"],
                end_date=plan["required_window"]["end_date"],
                universe_limit=plan["universe_limit"],
                cache_dir=str(cache_dir),
            )
            frame = fetched.frame if hasattr(fetched, "frame") else fetched
            frame = frame.copy()
            validation = _validate_extended_dataset(frame, plan["required_window"], plan["required_columns"])
            fallback_fetch_performed = True
        dataset_path = Path(plan["dataset_path"])
        if not validation["valid"]:
            payload = {**plan, "extension_status": "blocked_validation_failed", "next_action": "repair_feature_store_before_write", "write_performed": False, "fallback_fetch_performed": fallback_fetch_performed, "validation": validation}
        else:
            dataset_path.parent.mkdir(parents=True, exist_ok=True)
            if dataset_path.exists():
                backup = dataset_path.with_name(dataset_path.name + ".bak")
                shutil.copy2(dataset_path, backup)
            frame.to_csv(dataset_path, index=False)
            payload = {**plan, "extension_status": "written", "next_action": "rerun_small_institutional_dataset_preflight", "write_performed": True, "fallback_fetch_performed": fallback_fetch_performed, "validation": validation}

    json_out = Path(json_path)
    md_out = Path(markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_out.write_text(small_institutional_dataset_extension_to_markdown(payload), encoding="utf-8")
    return payload
