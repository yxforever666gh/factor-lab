#!/usr/bin/env python3
"""Write Autonomous Strategy Lab data/mechanism request report."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_data_request_report import build_data_request_report, write_data_request_report

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "autonomous_strategy_lab"
DEFAULT_WORKER_ROOT = DEFAULT_OUTPUT_DIR / "workers"
DEFAULT_ROUTE_REGISTRY = ROOT / "configs" / "autonomous_strategy_routes.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--worker-root", default=str(DEFAULT_WORKER_ROOT))
    parser.add_argument("--route-registry", default=str(DEFAULT_ROUTE_REGISTRY))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args(argv)

    worker_dir = Path(args.worker_root) / args.run_id
    worker_verdict = json.loads((worker_dir / "worker_verdict.json").read_text(encoding="utf-8"))
    route_registry = json.loads(Path(args.route_registry).read_text(encoding="utf-8"))
    report = build_data_request_report(
        run_id=args.run_id,
        worker_verdict=worker_verdict,
        route_registry=route_registry,
    )
    paths = write_data_request_report(report, args.output_dir)
    print(json.dumps({
        "run_id": args.run_id,
        "decision": report["decision"],
        "field_request_count": len(report["field_requests"]),
        "blocked_route_count": len(report["blocked_route_ids"]),
        "controlled_execution_allowed": report["controlled_execution_allowed"],
        "queue_write_allowed": report["queue_write_allowed"],
        "json_path": str(paths["json"].relative_to(ROOT)) if paths["json"].is_relative_to(ROOT) else str(paths["json"]),
        "markdown_path": str(paths["markdown"].relative_to(ROOT)) if paths["markdown"].is_relative_to(ROOT) else str(paths["markdown"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
