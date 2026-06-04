#!/usr/bin/env python3
"""Run the single bounded value-trap/risk-filter probe allowed by route verdict."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from factor_lab.autonomous_strategy_risk_filter_probe import build_value_trap_risk_filter_probe, write_risk_filter_probe

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ASL_DIR = ROOT / "artifacts" / "autonomous_strategy_lab"
REQUIRED_COLUMNS = {
    "date", "ticker", "pb", "pe_ttm", "forward_return_5d",
    "volatility_20", "turnover", "roe", "debt_to_asset",
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_ASL_DIR))
    parser.add_argument("--window", type=int, default=756)
    parser.add_argument("--min-periods", type=int, default=756)
    parser.add_argument("--max-drawdown-limit", type=float, default=-0.35)
    parser.add_argument("--min-usable-rows", type=int, default=1000)
    args = parser.parse_args(argv)

    base = DEFAULT_ASL_DIR
    route_verdict = json.loads((base / "route_verdict.json").read_text(encoding="utf-8"))
    cheap_screen = json.loads((base / "cheap_screen_result.json").read_text(encoding="utf-8"))
    source_path = Path(str(cheap_screen["source_path"]))
    if not source_path.is_absolute():
        source_path = ROOT / source_path
    frame = pd.read_csv(source_path, usecols=lambda col: col in REQUIRED_COLUMNS)
    probe = build_value_trap_risk_filter_probe(
        run_id=args.run_id,
        frame=frame,
        source_path=str(source_path.relative_to(ROOT)) if source_path.is_relative_to(ROOT) else str(source_path),
        route_verdict=route_verdict,
        window=args.window,
        min_periods=args.min_periods,
        max_drawdown_limit=args.max_drawdown_limit,
        min_usable_rows=args.min_usable_rows,
    )
    paths = write_risk_filter_probe(probe, args.output_dir)
    best = probe.get("best_candidate") or {}
    print(json.dumps({
        "run_id": args.run_id,
        "overall_status": probe["overall_status"],
        "recommended_next_step": probe["recommended_next_step"],
        "best_candidate": best.get("candidate"),
        "best_candidate_mean_daily_spread": best.get("mean_daily_spread"),
        "best_candidate_max_drawdown": best.get("max_drawdown"),
        "best_candidate_risk_pass": best.get("risk_pass"),
        "candidate_count": len(probe.get("candidate_results") or []),
        "controlled_execution_allowed": probe["controlled_execution_allowed"],
        "queue_write_allowed": probe["queue_write_allowed"],
        "json_path": str(paths["json"].relative_to(ROOT)) if paths["json"].is_relative_to(ROOT) else str(paths["json"]),
        "markdown_path": str(paths["markdown"].relative_to(ROOT)) if paths["markdown"].is_relative_to(ROOT) else str(paths["markdown"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
