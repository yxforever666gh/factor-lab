#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from factor_lab.controlled_restart_audit import dry_run_controlled_restart
from factor_lab.research_queue import run_orchestrator


def _write_outputs(output_dir: Path, payload: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "controlled_orchestrator_once.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Controlled Orchestrator Once",
        "",
        f"OK: {payload.get('ok')}",
        f"Exit reason: {payload.get('exit_reason')}",
        f"Requested max tasks: {payload.get('requested_max_tasks')}",
        f"Effective max tasks: {payload.get('effective_max_tasks')}",
        f"Processed: {payload.get('processed_count')}",
        "",
    ]
    (output_dir / "controlled_orchestrator_once.md").write_text("\n".join(lines), encoding="utf-8")


def run_controlled_orchestrator_once(
    *,
    max_tasks: int = 1,
    require_would_run: bool = True,
    db_path: str | Path = "artifacts/factor_lab.db",
    output_dir: str | Path = "artifacts",
) -> dict[str, Any]:
    max_tasks = max(1, int(max_tasks))
    audit = dry_run_controlled_restart(db_path=db_path)
    would_run_count = int(audit.get("would_run_count") or 0)
    payload: dict[str, Any] = {
        "ok": False,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "requested_max_tasks": max_tasks,
        "would_run_count": would_run_count,
        "dry_run": audit,
    }
    if require_would_run and would_run_count <= 0:
        payload.update({"exit_reason": "no_admitted_workflow", "processed_count": 0, "effective_max_tasks": 0})
        payload["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        _write_outputs(Path(output_dir), payload)
        return payload

    effective_max_tasks = min(max_tasks, would_run_count) if would_run_count > 0 else max_tasks
    try:
        result = run_orchestrator(max_tasks=effective_max_tasks)
    except Exception as exc:
        payload.update(
            {
                "ok": False,
                "exit_reason": "orchestrator_failed",
                "effective_max_tasks": effective_max_tasks,
                "processed_count": 0,
                "error": str(exc),
                "finished_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        )
        _write_outputs(Path(output_dir), payload)
        return payload
    processed = result.get("processed") or []
    payload.update(
        {
            "ok": True,
            "exit_reason": "completed" if processed else "no_progress",
            "effective_max_tasks": effective_max_tasks,
            "processed_count": len(processed),
            "orchestrator_result": result,
            "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        }
    )
    _write_outputs(Path(output_dir), payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-tasks", type=int, default=1)
    parser.add_argument("--db-path", default="artifacts/factor_lab.db")
    parser.add_argument("--output-dir", default="artifacts")
    parser.add_argument("--require-would-run", action="store_true")
    parser.add_argument("--allow-empty", action="store_true")
    args = parser.parse_args()
    result = run_controlled_orchestrator_once(
        max_tasks=args.max_tasks,
        require_would_run=args.require_would_run or not args.allow_empty,
        db_path=args.db_path,
        output_dir=args.output_dir,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result.get("ok") else 2)


if __name__ == "__main__":
    main()
