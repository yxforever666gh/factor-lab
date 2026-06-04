#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_controller import (
    build_autonomous_strategy_controller_state,
    load_controller_artifacts,
    write_controller_state,
)

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "artifacts" / "autonomous_strategy_lab"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one deterministic Autonomous Strategy Lab controller tick.")
    parser.add_argument("--run-id", default="autonomous_strategy_controller_once")
    parser.add_argument("--artifact-dir", default=str(ASL))
    parser.add_argument("--output-dir", default=str(ASL))
    args = parser.parse_args(argv)

    artifacts = load_controller_artifacts(args.artifact_dir)
    state = build_autonomous_strategy_controller_state(run_id=args.run_id, artifacts=artifacts)
    paths = write_controller_state(state, args.output_dir)
    print(
        json.dumps(
            {
                "current_state": state["current_state"],
                "latest_artifact": state["latest_artifact"],
                "latest_decision": state["latest_decision"],
                "recommended_next_step": state["recommended_next_step"],
                "human_required": state["human_required"],
                "controlled_execution_allowed": state["controlled_execution_allowed"],
                "queue_write_allowed": state["queue_write_allowed"],
                "timer_enable_allowed": state["timer_enable_allowed"],
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
