from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
HARVEST_ROOT = Path("artifacts/harvest_agent")
V1_MAX_EXPERIMENTS = 2
BLOCKING_DECISIONS = {"block", "manual_review"}


def _cycle_id(plan: dict[str, Any]) -> str:
    return str(plan.get("cycle_id") or "cycle_0001")


def _plan_experiments(plan: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(plan.get("proposals"), list):
        return list(plan.get("proposals") or [])
    if isinstance(plan.get("experiments"), list):
        return list(plan.get("experiments") or [])
    return []


def _experiment_id(spec: dict[str, Any]) -> str:
    return str(spec.get("experiment_id") or spec.get("proposal_id") or spec.get("id") or "experiment")


def _budget_cap(plan: dict[str, Any], max_experiments: int | None = None) -> int:
    budget = plan.get("research_budget") or {}
    raw = max_experiments if max_experiments is not None else budget.get("max_experiments", V1_MAX_EXPERIMENTS)
    try:
        cap = int(raw)
    except Exception:
        cap = V1_MAX_EXPERIMENTS
    return max(0, min(V1_MAX_EXPERIMENTS, cap))


def build_execution_manifest(
    plan: dict[str, Any],
    gate_decision: dict[str, Any],
    *,
    root: str | Path = ROOT,
    execution_mode: str | None = None,
    max_experiments: int | None = None,
) -> dict[str, Any]:
    """Build a bounded Harvest execution manifest; does not execute anything."""
    root = Path(root)
    cycle_id = _cycle_id(plan)
    decision = str(gate_decision.get("decision") or "allow_dry_run")
    if decision in BLOCKING_DECISIONS or decision == "block":
        return {
            "schema_version": 1,
            "cycle_id": cycle_id,
            "manifest_status": "blocked",
            "execution_mode": "none",
            "experiments": [],
            "reasons": list(gate_decision.get("reasons") or []),
            "safety": {"started_systemd_daemon": False, "timer_enabled": False, "live_trading_enabled": False},
        }

    mode = execution_mode or ("controlled_local" if decision == "allow_controlled_execution" else "dry_run")
    if mode != "controlled_local":
        mode = "dry_run"

    allowed = gate_decision.get("allowed_experiments")
    allowed_set = set(allowed or [])
    selected: list[dict[str, Any]] = []
    for spec in _plan_experiments(plan):
        exp_id = _experiment_id(spec)
        if allowed_set and exp_id not in allowed_set:
            continue
        selected.append(spec)

    cap = _budget_cap(plan, max_experiments)
    base = root / HARVEST_ROOT / cycle_id / "runs"
    timeout_minutes = int((plan.get("research_budget") or {}).get("max_runtime_minutes", 20) or 20)
    experiments: list[dict[str, Any]] = []
    for spec in selected[:cap]:
        exp_id = _experiment_id(spec)
        out = base / exp_id
        experiments.append(
            {
                "schema_version": 1,
                "cycle_id": cycle_id,
                "experiment_id": exp_id,
                "mechanism_id": spec.get("mechanism_id") or plan.get("mechanism_id"),
                "experiment_type": spec.get("experiment_type") or plan.get("mainline") or "cheap_screen",
                "execution_mode": mode,
                "output_dir": out.as_posix(),
                "expected_output_files": [(out / "result.json").as_posix(), (out / "status.json").as_posix()],
                "timeout_seconds": int(spec.get("timeout_seconds") or spec.get("max_runtime_minutes", timeout_minutes) * 60),
                "admission": {"decision": decision},
                "spec": dict(spec),
            }
        )

    return {
        "schema_version": 1,
        "cycle_id": cycle_id,
        "manifest_status": "ready",
        "execution_mode": mode,
        "max_experiments": cap,
        "experiments": experiments,
        "safety": {"started_systemd_daemon": False, "timer_enabled": False, "live_trading_enabled": False},
    }


def write_execution_manifest(
    plan: dict[str, Any],
    gate_decision: dict[str, Any],
    *,
    root: str | Path = ROOT,
    execution_mode: str | None = None,
    max_experiments: int | None = None,
) -> Path:
    manifest = build_execution_manifest(plan, gate_decision, root=root, execution_mode=execution_mode, max_experiments=max_experiments)
    cycle_dir = Path(root) / HARVEST_ROOT / manifest["cycle_id"]
    cycle_dir.mkdir(parents=True, exist_ok=True)
    path = cycle_dir / "execution_manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path
