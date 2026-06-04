#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from factor_lab.autonomous_strategy_pit_overlay_diagnostic import build_pit_overlay_diagnostic, write_pit_overlay_diagnostic

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "artifacts" / "autonomous_strategy_lab"
DEFAULT_BASE = ROOT / "artifacts" / "tushare_cache" / "tushare_2016-09-09_2023-12-31_97.csv"
DEFAULT_PIT = ROOT / "artifacts" / "tushare_cache" / "pit_financial_2020-06-02_2023-12-28_77_96401d85299a_v2.csv"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnose overlay coverage from PIT financial cache into ASL base feature cache.")
    parser.add_argument("--run-id", default="quality_profit_proxy_value_repair_v1")
    parser.add_argument("--base-cache", default=str(DEFAULT_BASE))
    parser.add_argument("--pit-cache", default=str(DEFAULT_PIT))
    parser.add_argument("--output-dir", default=str(ASL))
    parser.add_argument("--min-overlay-coverage", type=float, default=0.60)
    args = parser.parse_args(argv)

    base = pd.read_csv(args.base_cache)
    pit = pd.read_csv(args.pit_cache)
    report = build_pit_overlay_diagnostic(
        run_id=args.run_id,
        base_frame=base,
        pit_frame=pit,
        base_path=str(Path(args.base_cache).relative_to(ROOT)),
        pit_path=str(Path(args.pit_cache).relative_to(ROOT)),
        min_overlay_coverage=args.min_overlay_coverage,
    )
    paths = write_pit_overlay_diagnostic(report, args.output_dir)
    print(
        json.dumps(
            {
                "decision": report["decision"],
                "recommended_next_step": report["recommended_next_step"],
                "base_rows": report["base_rows"],
                "pit_rows": report["pit_rows"],
                "overlap_rows": report["overlap_rows"],
                "overlay_coverage": report["overlay_coverage"],
                "low_after_overlay": report["low_after_overlay"],
                "controlled_execution_allowed": report["controlled_execution_allowed"],
                "queue_write_allowed": report["queue_write_allowed"],
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
