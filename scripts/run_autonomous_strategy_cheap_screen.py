#!/usr/bin/env python3
"""Run metric-bearing historical valuation cheap screen on the current ASL coverage cache."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from factor_lab.autonomous_strategy_cheap_screen_runner import (
    build_historical_valuation_cheap_screen_result,
    write_cheap_screen_result,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_COVERAGE_PREFLIGHT = ROOT / "artifacts" / "autonomous_strategy_lab" / "historical_valuation_coverage_preflight.json"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "autonomous_strategy_lab"
REQUIRED_COLUMNS = {"date", "ticker", "industry", "pb", "pe_ttm", "forward_return_5d"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--coverage-preflight", default=str(DEFAULT_COVERAGE_PREFLIGHT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--window", type=int, default=756)
    parser.add_argument("--min-periods", type=int, default=756)
    parser.add_argument("--min-rows", type=int, default=100)
    parser.add_argument("--max-drawdown-limit", type=float, default=-0.35)
    args = parser.parse_args(argv)

    coverage = json.loads(Path(args.coverage_preflight).read_text(encoding="utf-8"))
    if coverage.get("overall_status") != "pass":
        raise SystemExit(f"coverage preflight must pass before cheap screen; got {coverage.get('overall_status')}")
    source_path = Path(str(coverage["source_path"]))
    if not source_path.is_absolute():
        source_path = ROOT / source_path
    frame = pd.read_csv(source_path, usecols=lambda col: col in REQUIRED_COLUMNS)
    result = build_historical_valuation_cheap_screen_result(
        run_id=args.run_id,
        frame=frame,
        source_path=str(source_path.relative_to(ROOT)) if source_path.is_relative_to(ROOT) else str(source_path),
        window=args.window,
        min_periods=args.min_periods,
        min_rows=args.min_rows,
        max_drawdown_limit=args.max_drawdown_limit,
    )
    paths = write_cheap_screen_result(result, args.output_dir)
    print(json.dumps({
        "run_id": args.run_id,
        "overall_status": result["overall_status"],
        "recommended_next_step": result["recommended_next_step"],
        "information_screen_status": result["information_screen_status"],
        "risk_screen_status": result["risk_screen_status"],
        "cheap_expensive_spread": result["cheap_expensive_spread"],
        "rank_ic": result["rank_ic"],
        "drawdown_proxy": result["drawdown_proxy"],
        "usable_row_count": result["usable_row_count"],
        "usable_ticker_count": result["usable_ticker_count"],
        "controlled_execution_allowed": result["controlled_execution_allowed"],
        "queue_write_allowed": result["queue_write_allowed"],
        "json_path": str(paths["json"].relative_to(ROOT)) if paths["json"].is_relative_to(ROOT) else str(paths["json"]),
        "markdown_path": str(paths["markdown"].relative_to(ROOT)) if paths["markdown"].is_relative_to(ROOT) else str(paths["markdown"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
