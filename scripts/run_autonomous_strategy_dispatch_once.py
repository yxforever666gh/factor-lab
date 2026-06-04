#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_dispatcher import dispatch_once, load_controller_state, write_dispatch_report

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "artifacts" / "autonomous_strategy_lab"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one safe autonomous strategy dispatcher step.")
    parser.add_argument("--run-id", default="autonomous_strategy_dispatch_once")
    parser.add_argument("--controller-state", default=str(ASL / "controller_state.json"))
    parser.add_argument("--output-dir", default=str(ASL))
    parser.add_argument("--max-seconds", type=int, default=300)
    args = parser.parse_args(argv)

    state = load_controller_state(args.controller_state)
    report = dispatch_once(run_id=args.run_id, controller_state=state, workdir=ROOT, max_seconds=args.max_seconds)
    paths = write_dispatch_report(report, args.output_dir)
    print(
        json.dumps(
            {
                "controller_state": report["controller_state"],
                "recommended_next_step": report["recommended_next_step"],
                "dispatch_status": report["dispatch_status"],
                "command": report["command"],
                "controlled_execution_allowed": report["controlled_execution_allowed"],
                "queue_write_allowed": report["queue_write_allowed"],
                "json_path": str(paths["json"].relative_to(ROOT)),
                "markdown_path": str(paths["markdown"].relative_to(ROOT)),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if report["dispatch_status"] in {"completed", "blocked_no_registered_safe_action"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
