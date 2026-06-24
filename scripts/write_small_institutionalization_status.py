#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.small_institutional_operator_pending_consistency_snapshot import (
    write_status_then_operator_pending_consistency_snapshot,
)
from factor_lab.small_institutionalization_policy import (
    DEFAULT_KNOWLEDGE_PATH,
    DEFAULT_OPERATOR_PENDING_CONSISTENCY_SNAPSHOT_PATH,
    DEFAULT_STATUS_JSON_PATH,
    DEFAULT_STATUS_MD_PATH,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OPERATOR_PENDING_CONSISTENCY_SNAPSHOT_MD_PATH = (
    ROOT / "artifacts" / "small_institutionalization" / "operator_pending_consistency_snapshot.md"
)


def main(*, status_kwargs: dict | None = None) -> dict:
    result = write_status_then_operator_pending_consistency_snapshot(
        status_json_path=DEFAULT_STATUS_JSON_PATH,
        status_markdown_path=DEFAULT_STATUS_MD_PATH,
        status_knowledge_path=DEFAULT_KNOWLEDGE_PATH,
        snapshot_json_path=DEFAULT_OPERATOR_PENDING_CONSISTENCY_SNAPSHOT_PATH,
        snapshot_markdown_path=DEFAULT_OPERATOR_PENDING_CONSISTENCY_SNAPSHOT_MD_PATH,
        status_kwargs=status_kwargs,
    )
    status = result["status"]
    snapshot = (status.get("paper_monitoring") or {}).get("operator_pending_consistency_snapshot") or {}
    repair = status.get("simulated_portfolio_construction_repair") or {}
    print(
        json.dumps(
            {
                "decision": status.get("decision"),
                "phase": status.get("phase"),
                "blockers": status.get("blockers") or [],
                "next_action": status.get("next_action"),
                "status_generated_at_utc": status.get("generated_at_utc"),
                "snapshot_freshness_status": snapshot.get("snapshot_freshness_status"),
                "operator_pending_consistency_status": snapshot.get("consistency_status"),
                "mismatches": snapshot.get("mismatches") or [],
                "benchmark_id": snapshot.get("benchmark_id"),
                "turnover_one_way_estimate": snapshot.get("turnover_one_way_estimate"),
                "estimated_round_trip_cost": snapshot.get("estimated_round_trip_cost"),
                "queue_write_allowed": snapshot.get("queue_write_allowed"),
                "broad_daemon_allowed": snapshot.get("broad_daemon_allowed"),
                "automation_allowed": snapshot.get("automation_allowed"),
                "automated_rerun_allowed": snapshot.get("automated_rerun_allowed"),
                "live_trading_enabled": snapshot.get("live_trading_enabled"),
                "repair_status": repair.get("repair_status"),
                "candidate_count": repair.get("candidate_count"),
                "best_available_max_drawdown": repair.get("best_available_max_drawdown"),
                "drawdown_gap_to_limit": repair.get("drawdown_gap_to_limit"),
            },
            ensure_ascii=False,
        )
    )
    return result


if __name__ == "__main__":
    main()
