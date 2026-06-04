from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.harvest_strategy_evidence import collect_strategy_evidence
from factor_lab.harvest_strategy_plan import build_strategy_plan, write_strategy_plan, write_strategy_summary
from factor_lab.harvest_strategy_policy import decide_strategy

HARVEST_ROOT = Path("artifacts/harvest_agent")


def _strategy_run_id() -> str:
    return "strategy_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_md(path: Path, title: str, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"# {title}\n\n```json\n{json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)}\n```\n", encoding="utf-8")


def run_harvest_strategy_governor(
    root: str | Path = ".",
    lookback_cycles: int = 8,
    max_next_backtests: int = 120,
    write: bool = False,
    strategy_run_id: str | None = None,
) -> dict[str, Any]:
    root = Path(root)
    run_id = strategy_run_id or _strategy_run_id()
    run_dir = root / HARVEST_ROOT / "strategy_runs" / run_id
    evidence = collect_strategy_evidence(root, lookback_cycles=lookback_cycles)
    decision = decide_strategy(evidence, config={"max_next_backtests": max_next_backtests})
    plan = build_strategy_plan(
        strategy_run_id=run_id,
        evidence=evidence,
        decision=decision,
        max_next_backtests=max_next_backtests,
    )
    summary = {
        "schema_version": 1,
        "strategy_run_id": run_id,
        "strategy_status": "written" if write else "dry_run",
        "strategy_decision": decision.get("strategy_decision"),
        "plan_status": plan.get("plan_status"),
        "based_on_cycle_id": plan.get("based_on_cycle_id"),
        "based_on_controller_run_id": plan.get("based_on_controller_run_id"),
        "manual_approval_required": plan.get("manual_approval_required"),
        "reason_codes": plan.get("reason_codes") or [],
        "artifacts_dir": str(run_dir.relative_to(root) if run_dir.is_relative_to(root) else run_dir),
        "plan": plan,
    }
    if write:
        _write_json(run_dir / "strategy_config.json", {
            "schema_version": 1,
            "lookback_cycles": lookback_cycles,
            "max_next_backtests": max_next_backtests,
            "write": True,
        })
        _write_json(run_dir / "strategy_evidence.json", evidence)
        _write_md(run_dir / "strategy_evidence.md", "Harvest v5 Strategy Evidence", evidence)
        _write_json(run_dir / "strategy_decision.json", decision)
        _write_md(run_dir / "strategy_decision.md", "Harvest v5 Strategy Decision", decision)
        write_strategy_plan(run_dir, plan)
        persisted_summary = write_strategy_summary(run_dir, evidence=evidence, decision=decision, plan=plan)
        _write_json(root / HARVEST_ROOT / "latest_strategy_run.json", {
            "schema_version": 1,
            "strategy_run_id": run_id,
            "artifacts_dir": str(run_dir.relative_to(root) if run_dir.is_relative_to(root) else run_dir),
            "strategy_decision": decision.get("strategy_decision"),
            "plan_status": plan.get("plan_status"),
        })
        summary.update(persisted_summary)
        summary["strategy_status"] = "written"
        summary["plan"] = plan
    return summary


def load_latest_strategy_plan(root: str | Path = ".") -> dict[str, Any] | None:
    root = Path(root)
    pointer_path = root / HARVEST_ROOT / "latest_strategy_run.json"
    if not pointer_path.exists():
        return None
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    artifacts_dir = pointer.get("artifacts_dir")
    run_id = pointer.get("strategy_run_id")
    candidates: list[Path] = []
    if artifacts_dir:
        candidates.append(root / str(artifacts_dir) / "v5_strategy_plan.json")
        candidates.append(Path(str(artifacts_dir)) / "v5_strategy_plan.json")
    if run_id:
        candidates.append(root / HARVEST_ROOT / "strategy_runs" / str(run_id) / "v5_strategy_plan.json")
    for path in candidates:
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                return payload if isinstance(payload, dict) else None
            except Exception:
                return None
    return None
