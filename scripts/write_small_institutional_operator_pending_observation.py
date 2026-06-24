#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.small_institutional_operator_pending_observation import write_operator_pending_observation

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WEEKLY_REPORT_PATH = ROOT / "artifacts" / "paper_portfolio" / "weekly_monitoring_report.json"
DEFAULT_STATUS_PATH = ROOT / "artifacts" / "small_institutionalization" / "status.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "operator_pending_observation.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "operator_pending_observation.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write small institutional operator-pending observation artifact.")
    parser.add_argument("--weekly-report-path", default=str(DEFAULT_WEEKLY_REPORT_PATH))
    parser.add_argument("--status-path", default=str(DEFAULT_STATUS_PATH))
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    payload = write_operator_pending_observation(
        weekly_report_path=args.weekly_report_path,
        status_path=args.status_path,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
    )
    print(
        json.dumps(
            {
                "observation_status": payload.get("observation_status"),
                "strategy_name": (payload.get("portfolio") or {}).get("strategy_name"),
                "benchmark_id": (payload.get("benchmark") or {}).get("benchmark_id"),
                "primary_issue": (payload.get("blocker") or {}).get("primary_issue"),
                "manual_approval_status": (payload.get("blocker") or {}).get("manual_approval_status"),
                "missing_artifacts": payload.get("missing_artifacts"),
                "queue_write_allowed": (payload.get("runtime") or {}).get("queue_write_allowed"),
                "broad_daemon_allowed": (payload.get("runtime") or {}).get("broad_daemon_allowed"),
                "automated_rerun_allowed": (payload.get("runtime") or {}).get("automated_rerun_allowed"),
                "live_trading_enabled": (payload.get("runtime") or {}).get("live_trading_enabled"),
            },
            ensure_ascii=False,
        )
    )
