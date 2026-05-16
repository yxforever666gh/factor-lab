from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _load_first(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
    return data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else {})


def build_bucket_aware_comparison_report(*, original_run_dir: str | Path, bucket_run_dir: str | Path) -> dict[str, Any]:
    original_dir = Path(original_run_dir)
    bucket_dir = Path(bucket_run_dir)
    original = _load_first(original_dir / "results.json")
    bucket_factor = _load_first(bucket_dir / "results.json")
    bucket = _load_first(bucket_dir / "bucket_aware_portfolio_results.json")
    bucket_pass = bool(bucket.get("pass_gate")) and float(bucket.get("spread_mean") or 0.0) > 0
    decision = "expand_bucket_aware_routes" if bucket_pass else "keep_daemon_paused_investigate_bucket_aware_failure"
    return {
        "original_run_dir": str(original_dir),
        "bucket_run_dir": str(bucket_dir),
        "original": original,
        "bucket_factor_result": bucket_factor,
        "bucket_aware": bucket,
        "decision": decision,
    }


def write_bucket_aware_comparison_report(*, original_run_dir: str | Path, bucket_run_dir: str | Path, output_dir: str | Path = "artifacts/value_route_bucket_aware") -> dict[str, Any]:
    report = build_bucket_aware_comparison_report(original_run_dir=original_run_dir, bucket_run_dir=bucket_run_dir)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "bucket_aware_comparison.json"
    md_path = out / "bucket_aware_comparison.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Bucket-Aware Value Route Comparison",
        "",
        f"Decision: `{report['decision']}`",
        "",
        "| metric | original top-bottom | bucket-aware |",
        "|---|---:|---:|",
        f"| rank_ic_mean | {report['original'].get('rank_ic_mean')} | {report['bucket_factor_result'].get('rank_ic_mean')} |",
        f"| spread | {report['original'].get('top_bottom_spread_mean')} | {report['bucket_aware'].get('spread_mean')} |",
        f"| pass_gate | {report['original'].get('pass_gate')} | {report['bucket_aware'].get('pass_gate')} |",
        "",
        "Daemon should remain inactive until controlled restart dry-run shows admitted bucket-aware tasks.",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {**report, "json_path": str(json_path), "markdown_path": str(md_path)}
