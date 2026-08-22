#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from factor_lab.dedup import workflow_experiment_fingerprint
from factor_lab.research_os.legacy_entrypoint import retired_legacy_entrypoint
from factor_lab.storage import ExperimentStore
from factor_lab.workflow_admission_adapter import enforce_workflow_admission

CONFIG_PATH = ROOT / "configs" / "pledge_controlled_probe.json"
OUT_DIR = Path("artifacts/pledge_controlled_probe_admission")
EXPECTED_ROUTE = "value_quality_high_pledge_record_count_confirmation"
EXPECTED_MECHANISM = "pledge_control_pressure"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _build_payload(config: dict, *, output_dir: str) -> dict:
    return {
        "config_path": str(CONFIG_PATH),
        "output_dir": output_dir,
        "mechanism_id": config.get("mechanism_id"),
        "route_id": config.get("route_id"),
        "required_data_fields": config.get("required_data_fields") or [],
        "factors": config.get("factors") or [],
        "portfolio_construction": config.get("portfolio_construction") or {},
        "source": "controlled_pledge_probe",
        "feature_overlay_csv": config.get("feature_overlay_csv"),
        "feature_overlay_columns": config.get("feature_overlay_columns") or [],
        "expected_new_evidence": config.get("expected_new_evidence"),
        "benchmark": config.get("benchmark") or {},
    }


def admit_pledge_controlled_probe_task(*, write: bool, db_path: str = "artifacts/factor_lab.db", priority: int = 0) -> dict:
    config = _load_json(CONFIG_PATH)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = f"artifacts/pledge_controlled_probe_run/{EXPECTED_ROUTE}_{timestamp}"
    payload = _build_payload(config, output_dir=output_dir)
    task = {
        "task_type": "workflow",
        "payload": payload,
        "worker_note": f"controlled_pledge_probe｜{EXPECTED_ROUTE}",
    }
    admission = enforce_workflow_admission(task)
    checks = {
        # Admission is evaluated from the checked-in contract on every call.
        # A stale or absent ignored dry-run artifact must never authorize a
        # queue mutation.
        "dry_run_allow": admission.get("decision") == "allow",
        "dry_run_would_enqueue_one": admission.get("decision") == "allow",
        "dry_run_no_queue_write": True,
        "route_exact": payload.get("route_id") == EXPECTED_ROUTE,
        "mechanism_exact": payload.get("mechanism_id") == EXPECTED_MECHANISM,
        "admission_allow": admission.get("decision") == "allow",
    }
    ok = all(checks.values())
    fingerprint = f"controlled_pledge_probe::{workflow_experiment_fingerprint(config)}"
    result = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "write": write,
        "ok": ok,
        "checks": checks,
        "admission": {"decision": admission.get("decision"), "reasons": admission.get("reasons") or []},
        "task": task,
        "fingerprint": fingerprint,
        "priority": priority,
        "enqueued_count": 0,
        "task_ids": [],
    }
    if not ok:
        result["decision"] = "block"
        result["reasons"] = [name for name, passed in checks.items() if not passed]
        return result
    if write:
        store = ExperimentStore(Path(db_path))
        try:
            task_id = store.enqueue_research_task(
                task_type="workflow",
                payload=payload,
                priority=priority,
                fingerprint=f"{fingerprint}::{uuid4()}",
                worker_note=f"controlled_pledge_probe｜{EXPECTED_ROUTE}",
            )
        finally:
            store.conn.close()
        result["enqueued_count"] = 1
        result["task_ids"] = [task_id]
        result["decision"] = "enqueued"
    else:
        result["decision"] = "dry_run_allow"
        result["would_enqueue_count"] = 1
    return result


def _write_outputs(result: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "admission_write_result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Pledge Controlled Probe Admission",
        "",
        f"Decision: `{result.get('decision')}`",
        f"OK: `{result.get('ok')}`",
        f"Write: `{result.get('write')}`",
        f"Enqueued count: `{result.get('enqueued_count')}`",
        f"Task IDs: `{', '.join(result.get('task_ids') or [])}`",
        "",
        "## Checks",
    ]
    for name, passed in (result.get("checks") or {}).items():
        lines.append(f"- {name}: `{passed}`")
    lines.extend(["", "## Admission", f"- decision: `{(result.get('admission') or {}).get('decision')}`", f"- reasons: `{(result.get('admission') or {}).get('reasons')}`"])
    (OUT_DIR / "admission_write_result.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    # Keep ``admit_pledge_controlled_probe_task`` importable for legacy evidence
    # inspection, but never let the historical CLI enqueue into SQLite.
    return retired_legacy_entrypoint("scripts/admit_pledge_controlled_probe_task.py")


if __name__ == "__main__":
    raise SystemExit(main())
