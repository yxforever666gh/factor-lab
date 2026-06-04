#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def _load_latest_event(run_dir: Path) -> dict:
    ledger = run_dir / "controller_ledger.jsonl"
    if not ledger.exists():
        return {}
    events = [json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines() if line.strip()]
    cycle_events = [e for e in events if e.get("cycle_id")]
    return (cycle_events or events or [{}])[-1]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect latest Harvest v4 controller status.")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    args = parser.parse_args()

    root = Path(args.root)
    base = root / "artifacts/harvest_agent"
    latest_path = base / "latest_controller_run.json"
    if not latest_path.exists():
        print("No Harvest controller run found")
        raise SystemExit(0)
    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    run_dir = Path(latest.get("artifacts_dir") or base / "controller_runs" / latest.get("controller_run_id", ""))
    if not run_dir.is_absolute():
        run_dir = root / run_dir
    summary_path = run_dir / "controller_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else {}
    event = _load_latest_event(run_dir)

    lines = [
        "# Harvest Controller Status",
        f"controller_run_id: {summary.get('controller_run_id') or latest.get('controller_run_id')}",
        f"cycles_run: {summary.get('cycles_run')}",
        f"executed_backtest_count: {summary.get('executed_backtest_count')}",
        f"latest_cycle_id: {event.get('cycle_id')}",
        f"latest_branch: {event.get('branch')}",
        f"latest_oos_class: {event.get('oos_class')}",
        f"latest_research_decision: {event.get('research_decision')}",
        f"stop_reason: {summary.get('stop_reason')}",
        f"started_systemd_daemon: {summary.get('started_systemd_daemon')}",
        f"scheduled_timer_enabled: {summary.get('scheduled_timer_enabled')}",
        "live_trading: False",
        "automatic_promotion: False",
        f"artifacts_dir: {run_dir}",
    ]
    print("\n".join(lines))
