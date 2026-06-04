#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from factor_lab.autonomous_strategy_pit_cache_extension_runner import run_pit_cache_extension, write_pit_cache_extension_run

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "artifacts" / "autonomous_strategy_lab"
CACHE = ROOT / "artifacts" / "tushare_cache"
DEFAULT_PLAN = ASL / "pit_cache_extension_plan.json"
DEFAULT_BASE = CACHE / "tushare_2016-09-09_2023-12-31_97.csv"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Autonomously extend PIT financial cache for ASL proxy route.")
    parser.add_argument("--run-id", default="pit_cache_extension_run")
    parser.add_argument("--extension-plan", default=str(DEFAULT_PLAN))
    parser.add_argument("--base-cache", default=str(DEFAULT_BASE))
    parser.add_argument("--cache-dir", default=str(CACHE))
    parser.add_argument("--output-dir", default=str(ASL))
    parser.add_argument("--no-diagnostics", action="store_true")
    parser.add_argument("--max-tickers", type=int, default=0, help="Bound one run to N tickers for autonomous chunking; 0 means full base cache.")
    parser.add_argument("--ticker-offset", type=int, default=0, help="Start offset into sorted ticker universe when --max-tickers is set.")
    args = parser.parse_args(argv)

    extension_plan = json.loads(Path(args.extension_plan).read_text(encoding="utf-8"))
    base_frame = pd.read_csv(args.base_cache)
    if args.max_tickers and args.max_tickers > 0:
        all_tickers = sorted(str(t) for t in base_frame["ticker"].dropna().unique())
        start = max(0, args.ticker_offset)
        tickers = all_tickers[start : start + args.max_tickers]
        base_frame = base_frame[base_frame["ticker"].astype(str).isin(tickers)].copy().reset_index(drop=True)
        extension_plan = dict(extension_plan)
        extension_plan["target_overlay_coverage"] = float(extension_plan.get("target_overlay_coverage") or 0.60)
        extension_plan["chunk_mode"] = {
            "enabled": True,
            "max_tickers": args.max_tickers,
            "ticker_offset": start,
            "next_ticker_offset": start + len(tickers),
            "total_tickers": len(all_tickers),
            "tickers": tickers,
        }
    report = run_pit_cache_extension(
        run_id=args.run_id,
        extension_plan=extension_plan,
        base_frame=base_frame,
        base_path=str(Path(args.base_cache).relative_to(ROOT)),
        cache_dir=args.cache_dir,
        output_dir=args.output_dir,
        retain_pit_cashflow_diagnostics=not args.no_diagnostics,
    )
    paths = write_pit_cache_extension_run(report, args.output_dir)
    print(
        json.dumps(
            {
                "execution_status": report["execution_status"],
                "failure_reason": report["failure_reason"],
                "data_source_configured": report["data_source_configured"],
                "coverage_after_extension": report["coverage_after_extension"],
                "coverage_pass": report["coverage_pass"],
                "recommended_next_step": report["recommended_next_step"],
                "controlled_execution_allowed": report["controlled_execution_allowed"],
                "queue_write_allowed": report["queue_write_allowed"],
                "json_path": str(paths["json"].relative_to(ROOT)),
                "markdown_path": str(paths["markdown"].relative_to(ROOT)),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if report["execution_status"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
