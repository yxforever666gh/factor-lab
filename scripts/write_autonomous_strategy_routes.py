#!/usr/bin/env python3
"""Write Autonomous Strategy Lab route registry from Hermes mechanism worker output."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_routes import build_route_registry_from_worker_response, write_route_registry
from factor_lab.feature_schema import TUSHARE_BLOCKED_FEATURE_COLUMNS, TUSHARE_FEATURE_COLUMNS

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKER_ROOT = ROOT / "artifacts" / "autonomous_strategy_lab" / "workers"
DEFAULT_OUTPUT_DIR = ROOT / "configs"
DEFAULT_FIELD_DERIVATIONS = ROOT / "artifacts" / "autonomous_strategy_lab" / "field_derivation_specs.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--worker-root", default=str(DEFAULT_WORKER_ROOT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--field-derivations", default=str(DEFAULT_FIELD_DERIVATIONS))
    args = parser.parse_args(argv)

    source = Path(args.worker_root) / args.run_id / "factor_lab_mechanism_researcher_response.json"
    worker_response = json.loads(source.read_text(encoding="utf-8"))
    available_fields = set(TUSHARE_FEATURE_COLUMNS) | {"date", "ticker"}
    derivation_path = Path(args.field_derivations)
    if derivation_path.exists():
        specs = json.loads(derivation_path.read_text(encoding="utf-8"))
        for row in specs.get("derived_fields") or []:
            # Treat spec-only derivable fields as available for preview-only route
            # registry / cheap-screen planning, not as materialized provider fields.
            if row.get("implementation_status") == "spec_only_not_materialized":
                available_fields.add(str(row.get("field")))
    registry = build_route_registry_from_worker_response(
        worker_response,
        available_fields=available_fields,
        blocked_fields=set(TUSHARE_BLOCKED_FEATURE_COLUMNS),
        source_path=str(source.relative_to(ROOT)) if source.is_relative_to(ROOT) else str(source),
    )
    paths = write_route_registry(registry, args.output_dir)
    blocked_count = sum(1 for route in registry["routes"] if route["route_status"] == "blocked_missing_fields")
    print(json.dumps({
        "run_id": args.run_id,
        "route_count": len(registry["routes"]),
        "blocked_missing_fields_count": blocked_count,
        "cheap_screen_candidate_count": len(registry["routes"]) - blocked_count,
        "queue_write_allowed": registry["queue_write_allowed"],
        "controlled_execution_allowed": registry["controlled_execution_allowed"],
        "json_path": str(paths["json"].relative_to(ROOT)) if paths["json"].is_relative_to(ROOT) else str(paths["json"]),
        "markdown_path": str(paths["markdown"].relative_to(ROOT)) if paths["markdown"].is_relative_to(ROOT) else str(paths["markdown"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
