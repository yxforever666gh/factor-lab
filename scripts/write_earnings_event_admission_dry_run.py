from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.workflow_admission_adapter import enforce_workflow_admission

CONFIG_PATH = Path("artifacts/earnings_event_controlled_probe/high_express_diluted_roe_yoy_probe.json")
OUT_JSON = Path("artifacts/earnings_event_controlled_probe/admission_dry_run_after_hardening.json")
OUT_MD = Path("artifacts/earnings_event_controlled_probe/admission_dry_run_after_hardening.md")


def build_admission_dry_run(config_path: str | Path = CONFIG_PATH) -> dict[str, Any]:
    config_path = Path(config_path)
    task = {
        "task_type": "workflow",
        "payload": {
            "config_path": str(config_path),
            "output_dir": "artifacts/earnings_event_controlled_probe/dry_run_only",
        },
        "worker_note": "controlled_earnings_event_probe|high_express_diluted_roe_yoy",
    }
    admission = enforce_workflow_admission(task)
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": True,
        "no_queue_write": True,
        "no_daemon_start": True,
        "config_path": str(config_path),
        "would_enqueue_count": 1 if admission.get("decision") == "allow" else 0,
        "task": task,
        "admission": admission,
    }


def to_markdown(payload: dict[str, Any]) -> str:
    admission = payload.get("admission") or {}
    inner = admission.get("admission") or {}
    lines = [
        "# Earnings Event Controlled Probe Admission Dry-run After Hardening",
        "",
        f"Config: `{payload.get('config_path')}`",
        f"Decision: `{admission.get('decision')}`",
        f"Reasons: {', '.join(admission.get('reasons') or []) or '(none)'}",
        f"Would enqueue count: {payload.get('would_enqueue_count')}",
        "",
        "No queue write. No daemon start.",
        "",
        "## Admission details",
        f"Route: `{inner.get('route_id')}`",
        f"Mechanism: `{inner.get('mechanism_id')}`",
        f"Coverage missing fields: {((inner.get('coverage_preflight') or {}).get('missing_fields') or [])}",
        f"Policy decision: `{((inner.get('policy_decision') or {}).get('decision'))}`",
        f"Policy reasons: {', '.join((inner.get('policy_decision') or {}).get('reasons') or []) or '(none)'}",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    payload = build_admission_dry_run(CONFIG_PATH)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_MD.write_text(to_markdown(payload), encoding="utf-8")
    print(json.dumps({"json_path": str(OUT_JSON), "markdown_path": str(OUT_MD), "admission": payload.get("admission"), "would_enqueue_count": payload.get("would_enqueue_count")}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
