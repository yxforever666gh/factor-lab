#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.paper_portfolio_diagnostics import write_paper_portfolio_diagnostics

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CURRENT_PATH = ROOT / "artifacts" / "paper_portfolio" / "current_portfolio.json"
DEFAULT_HISTORY_PATH = ROOT / "artifacts" / "paper_portfolio" / "portfolio_history.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "paper_portfolio" / "portfolio_diagnostics.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "paper_portfolio" / "portfolio_diagnostics.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write paper portfolio benchmark/cost/turnover diagnostics.")
    parser.add_argument("--current-path", default=str(DEFAULT_CURRENT_PATH))
    parser.add_argument("--history-path", default=str(DEFAULT_HISTORY_PATH))
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    parser.add_argument("--benchmark-id", required=True)
    parser.add_argument("--benchmark-name", required=True)
    parser.add_argument("--cost-bps", type=float, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    payload = write_paper_portfolio_diagnostics(
        current_path=args.current_path,
        history_path=args.history_path,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
        benchmark_id=args.benchmark_id,
        benchmark_name=args.benchmark_name,
        cost_bps=args.cost_bps,
    )
    print(
        json.dumps(
            {
                "strategy_name": payload.get("strategy_name"),
                "benchmark_id": (payload.get("benchmark") or {}).get("benchmark_id"),
                "history_status": (payload.get("turnover") or {}).get("history_status"),
                "turnover_one_way_estimate": (payload.get("turnover") or {}).get("turnover_one_way_estimate"),
                "estimated_round_trip_cost": (payload.get("cost") or {}).get("estimated_round_trip_cost"),
            },
            ensure_ascii=False,
        )
    )
