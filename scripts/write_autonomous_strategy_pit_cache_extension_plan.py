#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from factor_lab.autonomous_strategy_pit_cache_extension_plan import build_pit_cache_extension_plan, write_pit_cache_extension_plan

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "artifacts" / "autonomous_strategy_lab"
DEFAULT_OVERLAY = ASL / "pit_overlay_diagnostic.json"
DEFAULT_BASE = ROOT / "artifacts" / "tushare_cache" / "tushare_2016-09-09_2023-12-31_97.csv"
DEFAULT_PIT = ROOT / "artifacts" / "tushare_cache" / "pit_financial_2020-06-02_2023-12-28_77_96401d85299a_v2.csv"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write plan for extending PIT financial cache coverage.")
    parser.add_argument("--run-id", default="pit_cache_extension_plan")
    parser.add_argument("--overlay-diagnostic", default=str(DEFAULT_OVERLAY))
    parser.add_argument("--base-cache", default=str(DEFAULT_BASE))
    parser.add_argument("--pit-cache", default=str(DEFAULT_PIT))
    parser.add_argument("--output-dir", default=str(ASL))
    parser.add_argument("--target-overlay-coverage", type=float, default=0.60)
    args = parser.parse_args(argv)

    overlay = json.loads(Path(args.overlay_diagnostic).read_text(encoding="utf-8"))
    base = pd.read_csv(args.base_cache)
    pit = pd.read_csv(args.pit_cache)
    plan = build_pit_cache_extension_plan(
        run_id=args.run_id,
        overlay_diagnostic=overlay,
        base_frame=base,
        pit_frame=pit,
        base_path=str(Path(args.base_cache).relative_to(ROOT)),
        pit_path=str(Path(args.pit_cache).relative_to(ROOT)),
        target_overlay_coverage=args.target_overlay_coverage,
    )
    paths = write_pit_cache_extension_plan(plan, args.output_dir)
    print(
        json.dumps(
            {
                "decision": plan["decision"],
                "recommended_next_step": plan["recommended_next_step"],
                "human_required": plan["human_required"],
                "current_min_overlay_coverage": plan["current_min_overlay_coverage"],
                "low_fields": plan["low_fields"],
                "missing_ticker_count": plan["missing_ticker_count"],
                "missing_early_window": plan["missing_early_window"],
                "missing_late_window": plan["missing_late_window"],
                "controlled_execution_allowed": plan["controlled_execution_allowed"],
                "queue_write_allowed": plan["queue_write_allowed"],
                "json_path": str(paths["json"].relative_to(ROOT)),
                "markdown_path": str(paths["markdown"].relative_to(ROOT)),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
