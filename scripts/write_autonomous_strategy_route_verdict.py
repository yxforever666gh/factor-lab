#!/usr/bin/env python3
"""Write Autonomous Strategy Lab route verdict from coverage, cheap screen, and risk diagnostic artifacts."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_route_verdict import build_autonomous_strategy_route_verdict, write_route_verdict

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ASL_DIR = ROOT / "artifacts" / "autonomous_strategy_lab"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_ASL_DIR))
    parser.add_argument("--max-additional-risk-filter-probes", type=int, default=1)
    args = parser.parse_args(argv)

    base = DEFAULT_ASL_DIR
    coverage = json.loads((base / "historical_valuation_coverage_preflight.json").read_text(encoding="utf-8"))
    cheap_screen = json.loads((base / "cheap_screen_result.json").read_text(encoding="utf-8"))
    risk_diagnostic = json.loads((base / "cheap_screen_risk_diagnostic.json").read_text(encoding="utf-8"))
    verdict = build_autonomous_strategy_route_verdict(
        run_id=args.run_id,
        coverage_preflight=coverage,
        cheap_screen_result=cheap_screen,
        risk_diagnostic=risk_diagnostic,
        max_additional_risk_filter_probes=args.max_additional_risk_filter_probes,
    )
    paths = write_route_verdict(verdict, args.output_dir)
    print(json.dumps({
        "run_id": args.run_id,
        "verdict": verdict["verdict"],
        "reason_codes": verdict["reason_codes"],
        "max_next_risk_filter_probes": verdict["max_next_risk_filter_probes"],
        "controlled_execution_allowed": verdict["controlled_execution_allowed"],
        "queue_write_allowed": verdict["queue_write_allowed"],
        "json_path": str(paths["json"].relative_to(ROOT)) if paths["json"].is_relative_to(ROOT) else str(paths["json"]),
        "markdown_path": str(paths["markdown"].relative_to(ROOT)) if paths["markdown"].is_relative_to(ROOT) else str(paths["markdown"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
