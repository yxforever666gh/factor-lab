#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from factor_lab.autonomous_strategy_proxy_cheap_screen import build_proxy_cheap_screen, build_proxy_feature_frame, write_proxy_cheap_screen

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "artifacts" / "autonomous_strategy_lab"
BASE = ROOT / "artifacts" / "tushare_cache" / "tushare_2016-09-09_2023-12-31_97.csv"
PIT = ROOT / "artifacts" / "tushare_cache" / "pit_financial_asl_2017-03-15_2023-12-22_96_combined.csv"
PLAN = ASL / "proxy_cheap_screen_plan.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run bounded proxy cheap-screen diagnostics.")
    parser.add_argument("--run-id", default="proxy_cheap_screen")
    parser.add_argument("--base-cache", default=str(BASE))
    parser.add_argument("--pit-cache", default=str(PIT))
    parser.add_argument("--plan", default=str(PLAN))
    parser.add_argument("--output-dir", default=str(ASL))
    parser.add_argument("--window", type=int, default=756)
    parser.add_argument("--min-periods", type=int, default=756)
    args = parser.parse_args(argv)

    base = pd.read_csv(args.base_cache)
    pit = pd.read_csv(args.pit_cache)
    plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    frame = build_proxy_feature_frame(base_frame=base, pit_frame=pit)
    frame_path = Path(args.output_dir) / "proxy_cheap_screen_feature_frame.csv"
    frame_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(frame_path, index=False)
    screen = build_proxy_cheap_screen(
        run_id=args.run_id,
        frame=frame,
        plan=plan,
        source_path=str(frame_path.relative_to(ROOT)),
        window=args.window,
        min_periods=args.min_periods,
    )
    paths = write_proxy_cheap_screen(screen, args.output_dir)
    best = screen.get("best_candidate") or {}
    print(
        json.dumps(
            {
                "overall_status": screen["overall_status"],
                "recommended_next_step": screen["recommended_next_step"],
                "best_candidate": best.get("candidate"),
                "best_mean_daily_spread": best.get("mean_daily_spread"),
                "best_max_drawdown": best.get("max_drawdown"),
                "best_risk_pass": best.get("risk_pass"),
                "controlled_execution_allowed": screen["controlled_execution_allowed"],
                "queue_write_allowed": screen["queue_write_allowed"],
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
