from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.workflow_admission import evaluate_workflow_admission
from factor_lab.feature_schema import TUSHARE_AVAILABLE_FEATURE_COLUMNS

OUT_DIR = Path("artifacts/margin_low_crowding_controlled_config")
CONFIG_PATH = OUT_DIR / "value_quality_low_margin_crowding_confirmation.json"
MANIFEST_PATH = OUT_DIR / "manifest.json"
DRY_RUN_JSON = OUT_DIR / "admission_dry_run.json"
DRY_RUN_MD = OUT_DIR / "admission_dry_run.md"
MARGIN_FIELDS = ["margin_balance_to_mv", "low_margin_crowding", "margin_low_crowding_confirmation"]


def build_config() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "seed": 42,
        "data_source": "tushare",
        "cache_dir": "artifacts/tushare_cache",
        "universe_limit": 80,
        "start_date": "2020-06-01",
        "end_date": "2023-12-31",
        "route_id": "value_quality_low_margin_crowding_confirmation",
        "mechanism_id": "margin_low_crowding",
        "source": "controlled_margin_probe",
        "expected_new_evidence": "margin_balance_low_crowding_confirmation_vs_value_quality_no_distress",
        "feature_overlay_csv": "artifacts/margin_feature_monthly_panel/margin_feature_monthly_panel.csv",
        "feature_overlay_columns": MARGIN_FIELDS,
        "required_data_fields": ["margin_low_crowding_confirmation", "low_margin_crowding", "margin_balance_to_mv", "forward_return_5d"],
        "factors": [
            {
                "name": "margin_low_crowding_confirmation",
                "expression": "margin_low_crowding_confirmation",
                "family": "value_quality_low_crowding",
                "role": "controlled_probe",
                "required_data_fields": ["margin_low_crowding_confirmation"],
            }
        ],
        "portfolio_construction": {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 0},
        "benchmark": {"route_id": "value_quality_no_distress", "bucket_spread": 0.0062253011},
        "write_dataset_csv": True,
        "no_broad_daemon": True,
    }


def build_admission_dry_run(config: dict[str, Any]) -> dict[str, Any]:
    available = set(TUSHARE_AVAILABLE_FEATURE_COLUMNS) | set(MARGIN_FIELDS)
    task = {
        "task_type": "workflow",
        "payload": {
            "config_path": str(CONFIG_PATH),
            "output_dir": "artifacts/margin_low_crowding_controlled_config/dry_run_only",
            "mechanism_id": config["mechanism_id"],
            "route_id": config["route_id"],
            "required_data_fields": config["required_data_fields"],
            "factors": config["factors"],
            "portfolio_construction": config["portfolio_construction"],
            "source": config["source"],
        },
        "worker_note": "controlled_margin_probe｜value_quality_low_margin_crowding_confirmation",
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


def to_markdown(payload: dict[str, Any]) -> str:
    admission = payload.get("admission", {})
    lines = [
        "# Margin Low-crowding Controlled Config Admission Dry-run",
        "",
        f"Decision: `{admission.get('decision')}`",
        f"Reasons: {', '.join(admission.get('reasons') or [])}",
        f"Would enqueue count: {payload.get('would_enqueue_count')}",
        "",
        "No queue write, no daemon start.",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    config = build_config()
    CONFIG_PATH.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest = {"generated_at": datetime.now(timezone.utc).isoformat(), "configs": [str(CONFIG_PATH)], "decision_input": "margin monthly panel passed"}
    MANIFEST_PATH.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    dry_run = build_admission_dry_run(config)
    DRY_RUN_JSON.write_text(json.dumps(dry_run, ensure_ascii=False, indent=2), encoding="utf-8")
    DRY_RUN_MD.write_text(to_markdown(dry_run), encoding="utf-8")
    print(json.dumps({"config": str(CONFIG_PATH), "manifest": str(MANIFEST_PATH), "dry_run_json": str(DRY_RUN_JSON), "admission": dry_run.get("admission"), "would_enqueue_count": dry_run.get("would_enqueue_count")}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
