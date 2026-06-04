from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def append_controller_event(run_dir: str | Path, event: dict[str, Any]) -> None:
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    with (run_dir / "controller_ledger.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")


def _summary_markdown(summary: dict[str, Any]) -> str:
    branch_sequence = summary.get("branch_sequence") or []
    latest_cycle = summary.get("latest_cycle") or {}
    return "\n".join([
        "# Harvest Controller Summary",
        "",
        f"- controller_run_id: `{summary.get('controller_run_id')}`",
        f"- controller_status: `{summary.get('controller_status')}`",
        f"- cycles_run: `{summary.get('cycles_run')}`",
        f"- executed_backtest_count: `{summary.get('executed_backtest_count')}`",
        f"- stop_reason: `{summary.get('stop_reason')}`",
        f"- branch_sequence: `{branch_sequence}`",
        f"- latest_cycle_id: `{latest_cycle.get('cycle_id')}`",
        f"- latest_oos_class: `{latest_cycle.get('oos_class')}`",
        f"- latest_research_decision: `{latest_cycle.get('research_decision')}`",
        f"- started_systemd_daemon: `{summary.get('started_systemd_daemon')}`",
        f"- scheduled_timer_enabled: `{summary.get('scheduled_timer_enabled')}`",
        "",
    ])


def write_controller_summary(run_dir: str | Path, events: list[dict[str, Any]]) -> dict[str, Any]:
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    stop_reason = next((e.get("stop_reason") for e in reversed(events) if e.get("stop_reason")), None)
    cycle_events = [e for e in events if e.get("cycle_id") and e.get("event_type", "cycle") == "cycle"]
    latest_cycle = cycle_events[-1] if cycle_events else {}
    summary = {
        "schema_version": 1,
        "controller_run_id": run_dir.name,
        "controller_status": "complete",
        "cycles_run": len(cycle_events),
        "executed_backtest_count": sum(int(e.get("executed_backtest_count") or 0) for e in events),
        "stop_reason": stop_reason,
        "branch_sequence": [e.get("branch") for e in cycle_events if e.get("branch")],
        "latest_cycle": {
            "cycle_id": latest_cycle.get("cycle_id"),
            "branch": latest_cycle.get("branch"),
            "oos_class": latest_cycle.get("oos_class"),
            "research_decision": latest_cycle.get("research_decision"),
            "artifact_dir": latest_cycle.get("artifact_dir"),
        } if latest_cycle else {},
        "started_systemd_daemon": False,
        "scheduled_timer_enabled": False,
        "artifacts_dir": str(run_dir),
    }
    (run_dir / "controller_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (run_dir / "controller_summary.md").write_text(_summary_markdown(summary), encoding="utf-8")
    return summary
