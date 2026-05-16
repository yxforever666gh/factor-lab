from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def _read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _first_list_item(path: Path) -> dict[str, Any]:
    data = _read_json(path, [])
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    if isinstance(data, dict):
        return data
    return {}


def _has_reason(row: dict[str, Any], needle: str) -> bool:
    text = " ".join(str(row.get(k) or "") for k in ("fail_reason", "rejection_reason", "blocking_reasons"))
    return needle in text


def _bucket_aware_oos_pass_gate(summary: dict[str, Any]) -> bool:
    try:
        pass_rate = float(summary.get("pass_rate") or 0)
        positive_ratio = float(summary.get("positive_spread_ratio") or 0)
        avg_spread = float(summary.get("avg_spread_mean") or 0)
    except (TypeError, ValueError):
        return False
    return pass_rate >= 0.6 and positive_ratio >= 0.7 and avg_spread > 0


def build_controlled_run_ledger(
    *,
    runs_root: str | Path = "artifacts/value_route_bucket_aware/runs",
    bucket_aware_diagnostics_root: str | Path = "artifacts/bucket_aware_split_rolling_diagnostics",
) -> list[dict[str, Any]]:
    root = Path(runs_root)
    default_runs_root = Path("artifacts/value_route_bucket_aware/runs")
    default_diagnostics_root = Path("artifacts/bucket_aware_split_rolling_diagnostics")
    diagnostics_root = Path(bucket_aware_diagnostics_root)
    if Path(runs_root) != default_runs_root and diagnostics_root == default_diagnostics_root:
        diagnostics_root = root.parent / "bucket_aware_split_rolling_diagnostics"
    rows: list[dict[str, Any]] = []
    if not root.exists():
        return []
    for run_dir in sorted([p for p in root.iterdir() if p.is_dir()]):
        task = _read_json(run_dir / "task_state.json", {})
        exp = _read_json(run_dir / "experiment_ledger.json", {})
        cfg = exp.get("config") if isinstance(exp, dict) else {}
        if not isinstance(cfg, dict):
            cfg = {}
        result = _first_list_item(run_dir / "results.json")
        bucket = _first_list_item(run_dir / "bucket_aware_portfolio_results.json")
        rolling = _first_list_item(run_dir / "rolling_summary.json")
        route_id = cfg.get("route_id")
        bucket_aware_oos = _read_json(diagnostics_root / str(route_id) / "bucket_aware_rolling_summary.json", {}) if route_id else {}
        if not isinstance(bucket_aware_oos, dict):
            bucket_aware_oos = {}
        bucket_aware_oos_pass_gate = _bucket_aware_oos_pass_gate(bucket_aware_oos)
        raw_split_failure = bool(
            _has_reason(result, "too_many_split_failures")
            or int(rolling.get("fail_count") or 0) > int(rolling.get("pass_count") or 0)
        )
        row = {
            "task_id": task.get("task_id"),
            "route_id": route_id,
            "mechanism_id": cfg.get("mechanism_id"),
            "config_path": task.get("config_path") or str(cfg.get("config_path") or ""),
            "output_dir": task.get("output_dir") or str(run_dir),
            "status": task.get("status"),
            "finished_at_utc": task.get("finished_at_utc"),
            "rank_ic_mean": result.get("rank_ic_mean"),
            "top_bottom_spread_mean": result.get("top_bottom_spread_mean"),
            "bucket_aware_spread_mean": bucket.get("spread_mean"),
            "pass_gate": bool(bucket.get("pass_gate") or result.get("pass_gate")),
            "coverage_too_low": bool(_has_reason(result, "coverage_too_low")),
            "too_many_split_failures": bool(raw_split_failure and not bucket_aware_oos_pass_gate),
            "rolling_pass_rate": rolling.get("pass_rate"),
            "bucket_aware_oos_pass_gate": bucket_aware_oos_pass_gate,
            "bucket_aware_rolling_pass_rate": bucket_aware_oos.get("pass_rate"),
            "bucket_aware_positive_spread_ratio": bucket_aware_oos.get("positive_spread_ratio"),
            "bucket_aware_avg_spread_mean": bucket_aware_oos.get("avg_spread_mean"),
            "bucket_aware_worst_spread_mean": bucket_aware_oos.get("worst_spread_mean"),
            "bucket_aware_oos_diagnostics_path": str(diagnostics_root / str(route_id) / "bucket_aware_rolling_summary.json") if bucket_aware_oos else None,
        }
        rows.append(row)
    return rows


def summarize_controlled_run_ledger(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_status = Counter(str(r.get("status") or "unknown") for r in rows)
    blockers = Counter()
    route_summary: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "run_count": 0,
            "pass_gate_count": 0,
            "coverage_too_low_count": 0,
            "too_many_split_failures_count": 0,
            "bucket_aware_oos_pass_gate_count": 0,
            "bucket_aware_rolling_pass_rate_max": None,
            "bucket_aware_positive_spread_ratio_max": None,
        }
    )
    for row in rows:
        route = str(row.get("route_id") or "unknown")
        route_summary[route]["run_count"] += 1
        if row.get("pass_gate"):
            route_summary[route]["pass_gate_count"] += 1
        if row.get("coverage_too_low"):
            blockers["coverage_too_low"] += 1
            route_summary[route]["coverage_too_low_count"] += 1
        if row.get("too_many_split_failures"):
            blockers["too_many_split_failures"] += 1
            route_summary[route]["too_many_split_failures_count"] += 1
        if row.get("bucket_aware_oos_pass_gate"):
            route_summary[route]["bucket_aware_oos_pass_gate_count"] += 1
        for row_key, summary_key in (
            ("bucket_aware_rolling_pass_rate", "bucket_aware_rolling_pass_rate_max"),
            ("bucket_aware_positive_spread_ratio", "bucket_aware_positive_spread_ratio_max"),
        ):
            value = row.get(row_key)
            if value is None:
                continue
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                continue
            current = route_summary[route].get(summary_key)
            route_summary[route][summary_key] = numeric if current is None else max(float(current), numeric)
    return {"total": len(rows), "by_status": dict(by_status), "main_blockers": dict(blockers), "route_summary": dict(route_summary)}


def _summary_md(summary: dict[str, Any]) -> str:
    lines = ["# Controlled Run Ledger Summary", "", f"Total: {summary.get('total', 0)}", f"By status: {summary.get('by_status', {})}", f"Main blockers: {summary.get('main_blockers', {})}", "", "## Routes"]
    for route, row in sorted((summary.get("route_summary") or {}).items()):
        lines.append(
            f"- {route}: runs={row.get('run_count')} pass_gate={row.get('pass_gate_count')} "
            f"coverage_low={row.get('coverage_too_low_count')} split_fail={row.get('too_many_split_failures_count')} "
            f"bucket_oos_pass={row.get('bucket_aware_oos_pass_gate_count')} bucket_oos_pass_rate_max={row.get('bucket_aware_rolling_pass_rate_max')}"
        )
    return "\n".join(lines) + "\n"


def write_controlled_run_ledger(
    *,
    runs_root: str | Path = "artifacts/value_route_bucket_aware/runs",
    output_dir: str | Path = "artifacts",
    bucket_aware_diagnostics_root: str | Path = "artifacts/bucket_aware_split_rolling_diagnostics",
) -> dict[str, str]:
    rows = build_controlled_run_ledger(runs_root=runs_root, bucket_aware_diagnostics_root=bucket_aware_diagnostics_root)
    summary = summarize_controlled_run_ledger(rows)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    jsonl = out / "controlled_run_ledger.jsonl"
    summary_json = out / "controlled_run_ledger_summary.json"
    summary_md = out / "controlled_run_ledger_summary.md"
    jsonl.write_text("".join(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n" for r in rows), encoding="utf-8")
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    summary_md.write_text(_summary_md(summary), encoding="utf-8")
    return {"jsonl_path": str(jsonl), "summary_json_path": str(summary_json), "summary_markdown_path": str(summary_md)}
