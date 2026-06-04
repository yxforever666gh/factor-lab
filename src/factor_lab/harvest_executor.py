from __future__ import annotations

import json
import time
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from typing import Any

from factor_lab.defensive_quality_experiments import run_defensive_quality_experiment
from factor_lab.harvest_execution_manifest import HARVEST_ROOT, build_execution_manifest

ROOT = Path(__file__).resolve().parents[2]
SUPPORTED_CONTROLLED_TYPES = {"defensive_quality_risk_layer", "defensive_quality", "simulated_portfolio_repair"}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _dry_run_status(exp: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "experiment_id": exp.get("experiment_id"),
        "status": "dry_run",
        "execution_mode": "dry_run",
        "would_execute": False,
        "started_systemd_daemon": False,
        "timer_enabled": False,
    }


def _run_one_controlled(exp: dict[str, Any]) -> tuple[dict[str, Any], str, str]:
    exp_type = str(exp.get("experiment_type") or "")
    out = Path(exp["output_dir"])
    spec = {**(exp.get("spec") or {}), "experiment_id": exp.get("experiment_id"), "output_dir": str(out)}
    start = time.time()
    stdout_buffer, stderr_buffer = StringIO(), StringIO()
    try:
        if exp_type in SUPPORTED_CONTROLLED_TYPES:
            with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
                result = run_defensive_quality_experiment(spec)
            status = result.get("status", "unknown")
        else:
            result = {"status": "unsupported_experiment_type", "experiment_type": exp_type, "reason": "cheap_screen_record_only"}
            status = "unsupported_experiment_type"
            _write_json(out / "result.json", result)
    except Exception as exc:  # defensive: execution failures become evidence, not crashes
        result = {"status": "failed", "error": str(exc)}
        status = "failed"
        _write_json(out / "result.json", result)
    status_payload = {
        "schema_version": 1,
        "experiment_id": exp.get("experiment_id"),
        "status": status,
        "execution_mode": "controlled_local",
        "runtime_seconds": round(time.time() - start, 3),
        "started_systemd_daemon": False,
        "timer_enabled": False,
    }
    if isinstance(result, dict):
        if result.get("missing_fields"):
            status_payload["missing_fields"] = result.get("missing_fields")
        if result.get("reason"):
            status_payload["reason"] = result.get("reason")
        if result.get("error"):
            status_payload["error"] = result.get("error")
    return status_payload, stdout_buffer.getvalue(), stderr_buffer.getvalue()


def run_harvest_cycle(
    plan: dict[str, Any],
    gate_decision: dict[str, Any],
    *,
    root: str | Path = ROOT,
    dry_run: bool = False,
    allow_controlled_execution: bool = False,
    max_experiments: int | None = None,
) -> dict[str, Any]:
    """Run a single Harvest controlled-execution phase safely.

    Default is dry-run even when a gate allows controlled execution. Controlled local
    execution requires allow_controlled_execution=True. No daemon/timer/live path is used.
    """
    root = Path(root)
    cycle_id = str(plan.get("cycle_id") or "cycle_0001")
    cycle_dir = root / HARVEST_ROOT / cycle_id
    cycle_dir.mkdir(parents=True, exist_ok=True)

    if gate_decision.get("decision") in {"block", "manual_review"}:
        manifest = build_execution_manifest(plan, gate_decision, root=root, max_experiments=max_experiments)
        _write_json(cycle_dir / "execution_manifest.json", manifest)
        return {"execution_status": "blocked", "executed_count": 0, "started_systemd_daemon": False, "manifest_path": str(cycle_dir / "execution_manifest.json")}

    controlled = bool(allow_controlled_execution and not dry_run and gate_decision.get("decision") == "allow_controlled_execution")
    mode = "controlled_local" if controlled else "dry_run"
    manifest = build_execution_manifest(plan, gate_decision, root=root, execution_mode=mode, max_experiments=max_experiments)
    _write_json(cycle_dir / "execution_manifest.json", manifest)

    executed = 0
    statuses: list[str] = []
    for exp in manifest.get("experiments", []):
        out = Path(exp["output_dir"])
        out.mkdir(parents=True, exist_ok=True)
        if mode == "dry_run":
            _write_json(out / "status.json", _dry_run_status(exp))
            _write_text(out / "stdout.txt", "")
            _write_text(out / "stderr.txt", "")
            continue
        status_payload, stdout_text, stderr_text = _run_one_controlled(exp)
        _write_json(out / "status.json", status_payload)
        _write_text(out / "stdout.txt", stdout_text)
        _write_text(out / "stderr.txt", stderr_text)
        statuses.append(str(status_payload.get("status")))
        executed += 1

    execution_status = "dry_run" if mode == "dry_run" else ("completed" if all(s == "ok" for s in statuses) else "partial")
    summary = {"execution_status": execution_status, "executed_count": executed, "started_systemd_daemon": False, "manifest_path": str(cycle_dir / "execution_manifest.json")}
    _write_json(cycle_dir / "execution_status.json", summary)
    return summary
