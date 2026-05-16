from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.feature_schema import TUSHARE_AVAILABLE_FEATURE_COLUMNS
from factor_lab.workflow_admission import evaluate_workflow_admission

OUT_DIR = Path("artifacts/pledge_controlled_probe_plan")
CONFIG_PATH = OUT_DIR / "value_quality_high_pledge_record_count_confirmation.json"
MANIFEST_PATH = OUT_DIR / "manifest.json"
DRY_RUN_JSON = OUT_DIR / "admission_dry_run.json"
DRY_RUN_MD = OUT_DIR / "admission_dry_run.md"
PLAN_PATH = Path(".hermes/plans/2026-05-12_pledge_controlled_probe_plan.md")
PLEDGE_OVERLAY = "artifacts/pledge_source_mvp/pledge_daily_asof_features.csv"
PLEDGE_FIELDS = [
    "high_pledge_record_count",
    "pledge_record_count",
    "pledge_ratio_mean",
    "pledge_ratio_max",
    "pledge_amount_sum",
]


def build_config() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "seed": 42,
        "data_source": "tushare",
        "cache_dir": "artifacts/tushare_cache",
        "universe_limit": 80,
        "start_date": "2020-06-01",
        "end_date": "2023-12-31",
        "route_id": "value_quality_high_pledge_record_count_confirmation",
        "mechanism_id": "pledge_control_pressure",
        "source": "controlled_pledge_probe",
        "expected_new_evidence": "pledge_record_count_readonly_spread_above_value_quality_benchmark",
        "feature_overlay_csv": PLEDGE_OVERLAY,
        "feature_overlay_columns": PLEDGE_FIELDS,
        "required_data_fields": ["high_pledge_record_count", "forward_return_5d"],
        "factors": [
            {
                "name": "high_pledge_record_count",
                "expression": "high_pledge_record_count",
                "family": "pledge_control_pressure",
                "role": "controlled_probe",
                "required_data_fields": ["high_pledge_record_count"],
            }
        ],
        "portfolio_construction": {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 0},
        "benchmark": {"route_id": "value_quality_no_distress", "bucket_spread": 0.0062253011},
        "write_dataset_csv": True,
        "no_broad_daemon": True,
    }


def build_admission_dry_run(config: dict[str, Any]) -> dict[str, Any]:
    available = set(TUSHARE_AVAILABLE_FEATURE_COLUMNS) | set(PLEDGE_FIELDS)
    task = {
        "task_type": "workflow",
        "payload": {
            "config_path": str(CONFIG_PATH),
            "output_dir": "artifacts/pledge_controlled_probe_plan/dry_run_only",
            "mechanism_id": config["mechanism_id"],
            "route_id": config["route_id"],
            "required_data_fields": config["required_data_fields"],
            "factors": config["factors"],
            "portfolio_construction": config["portfolio_construction"],
            "source": config["source"],
            "feature_overlay_csv": config["feature_overlay_csv"],
            "feature_overlay_columns": config["feature_overlay_columns"],
        },
        "worker_note": "controlled_pledge_probe｜value_quality_high_pledge_record_count_confirmation",
    }
    admission = evaluate_workflow_admission(task, available_fields=available)
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": True,
        "would_enqueue_count": 1 if admission.get("decision") == "allow" else 0,
        "no_queue_write": True,
        "no_daemon_start": True,
        "task": task,
        "admission": admission,
    }


def _md(dry_run: dict[str, Any]) -> str:
    admission = dry_run.get("admission", {})
    return "\n".join([
        "# Pledge Controlled Probe Plan",
        "",
        "Read-only pledge diagnostic passed the benchmark, so this file defines the next controlled probe plan only. It does not enqueue a workflow.",
        "",
        "## Candidate",
        "- route_id: `value_quality_high_pledge_record_count_confirmation`",
        "- mechanism_id: `pledge_control_pressure`",
        "- factor: `high_pledge_record_count`",
        "- overlay: `artifacts/pledge_source_mvp/pledge_daily_asof_features.csv`",
        "- portfolio construction: bucket_pair Q3-Q0",
        "",
        "## Read-only evidence",
        "- best read-only Q3-Q0 spread: `0.0074866435`",
        "- local baseline Q3-Q0: `0.0028484279`",
        "- benchmark: `0.0062253011`",
        "- diagnostic rows/dates/tickers: `12370 / 845 / 24`",
        "",
        "## Admission dry-run",
        f"- decision: `{admission.get('decision')}`",
        f"- reasons: {', '.join(admission.get('reasons') or [])}",
        f"- would_enqueue_count: `{dry_run.get('would_enqueue_count')}`",
        "",
        "No queue write, no daemon start. A future round may enqueue only after explicit controlled admission write is requested by the run policy.",
    ]) + "\n"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    config = build_config()
    CONFIG_PATH.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "configs": [str(CONFIG_PATH)],
        "decision_input": "pledge readonly diagnostic passed benchmark",
        "no_queue_write": True,
        "no_daemon_start": True,
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    dry_run = build_admission_dry_run(config)
    DRY_RUN_JSON.write_text(json.dumps(dry_run, ensure_ascii=False, indent=2), encoding="utf-8")
    text = _md(dry_run)
    DRY_RUN_MD.write_text(text, encoding="utf-8")
    PLAN_PATH.write_text(text, encoding="utf-8")
    print(json.dumps({"config": str(CONFIG_PATH), "dry_run_json": str(DRY_RUN_JSON), "admission": dry_run.get("admission"), "would_enqueue_count": dry_run.get("would_enqueue_count")}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
