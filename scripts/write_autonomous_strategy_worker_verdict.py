#!/usr/bin/env python3
"""Aggregate Autonomous Strategy Lab worker responses into a final worker verdict."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_worker_verdict import build_worker_verdict, write_worker_verdict

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKER_ROOT = ROOT / "artifacts" / "autonomous_strategy_lab" / "workers"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--worker-root", default=str(DEFAULT_WORKER_ROOT))
    args = parser.parse_args(argv)

    worker_dir = Path(args.worker_root) / args.run_id
    verdict = build_worker_verdict(worker_dir, run_id=args.run_id)
    paths = write_worker_verdict(verdict, worker_dir)
    print(json.dumps({
        "run_id": args.run_id,
        "consensus_decision": verdict["consensus_decision"],
        "valid_response_count": verdict["valid_response_count"],
        "controlled_execution_allowed": verdict["controlled_execution_allowed"],
        "queue_write_allowed": verdict["queue_write_allowed"],
        "json_path": str(paths["json"].relative_to(ROOT)) if paths["json"].is_relative_to(ROOT) else str(paths["json"]),
        "markdown_path": str(paths["markdown"].relative_to(ROOT)) if paths["markdown"].is_relative_to(ROOT) else str(paths["markdown"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
