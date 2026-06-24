#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.small_institutional_operator_pending_consistency_snapshot import (
    write_operator_pending_consistency_snapshot,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATUS_PATH = ROOT / "artifacts" / "small_institutionalization" / "status.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutionalization" / "operator_pending_consistency_snapshot.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutionalization" / "operator_pending_consistency_snapshot.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write operator-pending consistency snapshot artifact.")
    parser.add_argument("--status-path", default=str(DEFAULT_STATUS_PATH))
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    payload = write_operator_pending_consistency_snapshot(
        status_path=args.status_path,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
    )
    print(
        json.dumps(
            {
                "snapshot_status": payload.get("snapshot_status"),
                "consistency_status": payload.get("consistency_status"),
                "mismatches": payload.get("mismatches"),
                "benchmark_id": payload.get("benchmark_id"),
                "turnover_one_way_estimate": payload.get("turnover_one_way_estimate"),
                "estimated_round_trip_cost": payload.get("estimated_round_trip_cost"),
                "queue_write_allowed": (payload.get("runtime") or {}).get("queue_write_allowed"),
                "broad_daemon_allowed": (payload.get("runtime") or {}).get("broad_daemon_allowed"),
                "automation_allowed": (payload.get("runtime") or {}).get("automation_allowed"),
                "automated_rerun_allowed": (payload.get("runtime") or {}).get("automated_rerun_allowed"),
                "live_trading_enabled": (payload.get("runtime") or {}).get("live_trading_enabled"),
            },
            ensure_ascii=False,
        )
    )
