#!/usr/bin/env python3
"""Run the Autonomous Strategy Lab dry-run.

Artifact-only by default: reads existing Factor Lab evidence, writes a deterministic
strategy decision report, and never writes queues/enables timers/starts daemons.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.autonomous_strategy_lab import (
    build_autonomous_strategy_report,
    write_autonomous_strategy_report,
)

ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_DIR = ROOT / "artifacts" / "autonomous_strategy_lab"
DB_PATH = ROOT / "artifacts" / "factor_lab.db"
RISK_REPAIR_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "risk_reduction_repair.json"
RISK_RESULTS_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "risk_reduction_results.json"
HARVEST_V3_STATUS_PATH = ROOT / "artifacts" / "harvest_agent" / "v3_status.json"
CONTROLLED_DRY_RUN_PATH = ROOT / "artifacts" / "controlled_restart_dry_run.json"
RUNTIME_AUDIT_PATH = ROOT / "artifacts" / "runtime_takeover_audit.json"


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"_read_error": str(exc), "_path": str(path)}


def db_summary() -> dict[str, Any]:
    summary: dict[str, Any] = {"db_exists": DB_PATH.exists()}
    if not DB_PATH.exists():
        return summary
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    def rows(query: str) -> list[tuple[Any, ...]]:
        try:
            cur.execute(query)
            return cur.fetchall()
        except sqlite3.Error as exc:
            return [("_sqlite_error", str(exc))]

    summary["research_tasks_by_status"] = dict(rows(
        "SELECT status, COUNT(*) FROM research_tasks GROUP BY status ORDER BY COUNT(*) DESC"
    ))
    summary["research_tasks_by_type_status_top"] = [
        {"task_type": r[0], "status": r[1], "count": r[2]}
        for r in rows(
            "SELECT task_type,status,COUNT(*) FROM research_tasks "
            "GROUP BY task_type,status ORDER BY COUNT(*) DESC LIMIT 20"
        )
        if len(r) == 3
    ]
    summary["candidate_status"] = dict(rows(
        "SELECT status, COUNT(*) FROM factor_candidates GROUP BY status ORDER BY COUNT(*) DESC"
    ))
    summary["top_candidates"] = [
        {
            "name": r[0],
            "status": r[1],
            "evaluation_count": r[2],
            "avg_final_score": r[3],
            "best_final_score": r[4],
            "pass_rate": r[5],
            "rejection_reason": r[6],
            "next_action": r[7],
        }
        for r in rows(
            "SELECT name,status,evaluation_count,avg_final_score,best_final_score,pass_rate,"
            "rejection_reason,next_action FROM factor_candidates "
            "ORDER BY pass_rate DESC, best_final_score DESC LIMIT 12"
        )
        if len(r) == 8
    ]
    summary["rejection_reasons_top"] = dict(rows(
        "SELECT rejection_reason, COUNT(*) FROM factor_evaluations "
        "WHERE rejection_reason IS NOT NULL AND rejection_reason != '' "
        "GROUP BY rejection_reason ORDER BY COUNT(*) DESC LIMIT 12"
    ))
    count_rows = rows("SELECT COUNT(*) FROM factor_evaluations")
    summary["evaluation_count"] = count_rows[0][0] if count_rows and len(count_rows[0]) == 1 else None
    conn.close()
    return summary


def collect_evidence() -> dict[str, Any]:
    repair = read_json(RISK_REPAIR_PATH, {})
    results = read_json(RISK_RESULTS_PATH, {})
    best_result = results.get("best_result") or results.get("best") or {}
    return {
        "db": db_summary(),
        "risk": {
            "status": repair.get("repair_status"),
            "drawdown_limit": repair.get("drawdown_limit"),
            "best_available_max_drawdown": repair.get("best_available_max_drawdown"),
            "candidate_count": repair.get("candidate_count"),
            "best_sharpe": best_result.get("sharpe"),
        },
        "harvest_v3_status": read_json(HARVEST_V3_STATUS_PATH, {}),
        "controlled_dry_run": read_json(CONTROLLED_DRY_RUN_PATH, {}),
        "runtime_audit": read_json(RUNTIME_AUDIT_PATH, {}),
    }


def main() -> int:
    now = datetime.now(timezone.utc)
    run_id = "strategy_lab_" + now.strftime("%Y%m%dT%H%M%SZ")
    report = build_autonomous_strategy_report(
        run_id=run_id,
        evidence_summary=collect_evidence(),
        created_at_utc=now.isoformat(),
    )
    paths = write_autonomous_strategy_report(report, ARTIFACT_DIR)
    print(json.dumps({
        "run_id": run_id,
        "decision": report["decision"],
        "reason_codes": report["reason_codes"],
        "json_path": str(paths["latest_json"].relative_to(ROOT)),
        "markdown_path": str(paths["latest_markdown"].relative_to(ROOT)),
        "queue_write_allowed": report["safety"]["queue_write_allowed"],
        "automation_allowed": report["safety"]["automation_allowed"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
