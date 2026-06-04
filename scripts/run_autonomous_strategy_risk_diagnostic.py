#!/usr/bin/env python3
"""Diagnose risk failure in the historical valuation cheap screen."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from factor_lab.autonomous_strategy_risk_diagnostic import build_historical_valuation_risk_diagnostic, write_risk_diagnostic

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CHEAP_SCREEN_RESULT = ROOT / "artifacts" / "autonomous_strategy_lab" / "cheap_screen_result.json"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "autonomous_strategy_lab"
REQUIRED_COLUMNS = {"date", "ticker", "industry", "pb", "pe_ttm", "forward_return_5d"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--cheap-screen-result", default=str(DEFAULT_CHEAP_SCREEN_RESULT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--window", type=int, default=756)
    parser.add_argument("--min-periods", type=int, default=756)
    parser.add_argument("--max-drawdown-limit", type=float, default=-0.35)
    args = parser.parse_args(argv)

    cheap_screen = json.loads(Path(args.cheap_screen_result).read_text(encoding="utf-8"))
    source_path = Path(str(cheap_screen["source_path"]))
    if not source_path.is_absolute():
        source_path = ROOT / source_path
    frame = pd.read_csv(source_path, usecols=lambda col: col in REQUIRED_COLUMNS)
    diagnostic = build_historical_valuation_risk_diagnostic(
        run_id=args.run_id,
        frame=frame,
        source_path=str(source_path.relative_to(ROOT)) if source_path.is_relative_to(ROOT) else str(source_path),
        cheap_screen_result=cheap_screen,
        window=args.window,
        min_periods=args.min_periods,
        max_drawdown_limit=args.max_drawdown_limit,
    )
    paths = write_risk_diagnostic(diagnostic, args.output_dir)
    best = diagnostic.get("best_repair_candidate") or {}
    print(json.dumps({
        "run_id": args.run_id,
        "overall_status": diagnostic["overall_status"],
        "recommended_next_step": diagnostic["recommended_next_step"],
        "original_drawdown": diagnostic["original_drawdown"],
        "negative_industry_count": len(diagnostic.get("negative_industries") or []),
        "best_repair_candidate": best.get("candidate"),
        "best_repair_mean_daily_spread": best.get("mean_daily_spread"),
        "best_repair_max_drawdown": best.get("max_drawdown"),
        "best_repair_risk_pass": best.get("risk_pass"),
        "controlled_execution_allowed": diagnostic["controlled_execution_allowed"],
        "queue_write_allowed": diagnostic["queue_write_allowed"],
        "json_path": str(paths["json"].relative_to(ROOT)) if paths["json"].is_relative_to(ROOT) else str(paths["json"]),
        "markdown_path": str(paths["markdown"].relative_to(ROOT)) if paths["markdown"].is_relative_to(ROOT) else str(paths["markdown"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
