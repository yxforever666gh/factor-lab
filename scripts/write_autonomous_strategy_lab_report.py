#!/usr/bin/env python3
"""Write Autonomous Strategy Lab status report artifacts."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_lab_report import build_autonomous_strategy_lab_report, write_autonomous_strategy_lab_report

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "autonomous_strategy_lab"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args(argv)

    report = build_autonomous_strategy_lab_report(args.root)
    paths = write_autonomous_strategy_lab_report(report, args.output_dir)
    print(json.dumps({
        "status": report["status"],
        "decision": report["decision"],
        "coverage_overall_status": report["coverage_overall_status"],
        "execution_status": report["execution_status"],
        "controlled_execution_started": report["controlled_execution_started"],
        "controlled_execution_allowed": report["controlled_execution_allowed"],
        "queue_write_allowed": report["queue_write_allowed"],
        "timer_enable_allowed": report["timer_enable_allowed"],
        "json_path": str(paths["json"].relative_to(ROOT)) if paths["json"].is_relative_to(ROOT) else str(paths["json"]),
        "markdown_path": str(paths["markdown"].relative_to(ROOT)) if paths["markdown"].is_relative_to(ROOT) else str(paths["markdown"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
