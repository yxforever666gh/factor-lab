#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_industry_cycle_route_closure import (
    build_industry_cycle_route_closure,
    write_industry_cycle_route_closure,
)

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "artifacts" / "autonomous_strategy_lab"
DEFAULT_CHEAP_SCREEN = ASL / "industry_cycle_cheap_screen.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Close failed industry-cycle autonomous strategy route.")
    parser.add_argument("--run-id", default="industry_cycle_inflection_value_anchor_v1")
    parser.add_argument("--cheap-screen", default=str(DEFAULT_CHEAP_SCREEN))
    parser.add_argument("--output-dir", default=str(ASL))
    args = parser.parse_args(argv)

    cheap_screen_path = Path(args.cheap_screen)
    cheap_screen = json.loads(cheap_screen_path.read_text(encoding="utf-8"))
    closure = build_industry_cycle_route_closure(run_id=args.run_id, cheap_screen=cheap_screen)
    paths = write_industry_cycle_route_closure(closure, args.output_dir)
    print(
        json.dumps(
            {
                "route_status": closure["route_status"],
                "stop_reason": closure["stop_reason"],
                "recommended_next_step": closure["recommended_next_step"],
                "controlled_execution_allowed": closure["controlled_execution_allowed"],
                "queue_write_allowed": closure["queue_write_allowed"],
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
