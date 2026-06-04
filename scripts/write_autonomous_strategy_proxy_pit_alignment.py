#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from factor_lab.autonomous_strategy_proxy_pit_alignment import build_proxy_pit_alignment_review, write_proxy_pit_alignment_review

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "artifacts" / "autonomous_strategy_lab"
DEFAULT_PIT = ROOT / "artifacts" / "tushare_cache" / "pit_financial_asl_2017-03-15_2023-12-22_96_combined.csv"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Review PIT alignment for proxy value-repair fields.")
    parser.add_argument("--run-id", default="proxy_pit_alignment")
    parser.add_argument("--pit-cache", default=str(DEFAULT_PIT))
    parser.add_argument("--output-dir", default=str(ASL))
    args = parser.parse_args(argv)

    pit = pd.read_csv(args.pit_cache)
    report = build_proxy_pit_alignment_review(
        run_id=args.run_id,
        pit_frame=pit,
        pit_path=str(Path(args.pit_cache).relative_to(ROOT)),
    )
    paths = write_proxy_pit_alignment_review(report, args.output_dir)
    print(
        json.dumps(
            {
                "decision": report["decision"],
                "recommended_next_step": report["recommended_next_step"],
                "usable_coverage": report["usable_coverage"],
                "pit_feature_validated_coverage": report["pit_feature_validated_coverage"],
                "ann_date_alignment_coverage": report["ann_date_alignment_coverage"],
                "end_date_alignment_coverage": report["end_date_alignment_coverage"],
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
