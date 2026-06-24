#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.paper_portfolio_weekly_report import write_weekly_paper_report

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CURRENT_PATH = ROOT / "artifacts" / "paper_portfolio" / "current_portfolio.json"
DEFAULT_DIAGNOSTICS_PATH = ROOT / "artifacts" / "paper_portfolio" / "portfolio_diagnostics.json"
DEFAULT_STATUS_PATH = ROOT / "artifacts" / "small_institutionalization" / "status.json"
DEFAULT_OPERATOR_PENDING_OBSERVATION_PATH = (
    ROOT / "artifacts" / "small_institutional_simulation" / "operator_pending_observation.json"
)
DEFAULT_JSON_PATH = ROOT / "artifacts" / "paper_portfolio" / "weekly_monitoring_report.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "paper_portfolio" / "weekly_monitoring_report.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write the weekly paper portfolio monitoring report.")
    parser.add_argument("--current-path", default=str(DEFAULT_CURRENT_PATH))
    parser.add_argument("--diagnostics-path", default=str(DEFAULT_DIAGNOSTICS_PATH))
    parser.add_argument("--status-path", default=str(DEFAULT_STATUS_PATH))
    parser.add_argument("--operator-pending-observation-path", default=str(DEFAULT_OPERATOR_PENDING_OBSERVATION_PATH))
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    payload = write_weekly_paper_report(
        current_portfolio_path=args.current_path,
        diagnostics_path=args.diagnostics_path,
        status_path=args.status_path,
        operator_pending_observation_path=args.operator_pending_observation_path,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
    )
    print(
        json.dumps(
            {
                "cadence": payload.get("cadence"),
                "strategy_name": (payload.get("portfolio") or {}).get("strategy_name"),
                "benchmark_id": (payload.get("benchmark") or {}).get("benchmark_id"),
                "missing_artifacts": payload.get("missing_artifacts"),
                "runtime_safe": (payload.get("runtime") or {}).get("safe"),
                "would_run_count": (payload.get("runtime") or {}).get("would_run_count"),
                "operator_pending_observation_status": (payload.get("operator_pending_observation") or {}).get(
                    "observation_status"
                ),
                "queue_write_allowed": (payload.get("runtime") or {}).get("queue_write_allowed"),
                "broad_daemon_allowed": (payload.get("runtime") or {}).get("broad_daemon_allowed"),
            },
            ensure_ascii=False,
        )
    )
