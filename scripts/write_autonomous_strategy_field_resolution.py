#!/usr/bin/env python3
"""Resolve Autonomous Strategy Lab field requests against current feature schema."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_field_resolver import build_field_resolution_report, write_field_resolution_report
from factor_lab.feature_schema import EXPRESSION_ALIASES, TUSHARE_AVAILABLE_FEATURE_COLUMNS, TUSHARE_BLOCKED_FEATURE_COLUMNS, TUSHARE_FEATURE_COLUMNS

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "artifacts" / "autonomous_strategy_lab" / "data_request_report.json"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "autonomous_strategy_lab"

# Conservative route-specific aliases discovered by worker review. These do not
# mark unavailable fields as available; they only resolve naming mismatches.
FIELD_ALIASES = {"quality_roe": "roe", **EXPRESSION_ALIASES}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args(argv)

    data_request_report = json.loads(Path(args.input).read_text(encoding="utf-8"))
    report = build_field_resolution_report(
        run_id=args.run_id,
        data_request_report=data_request_report,
        schema_fields=set(TUSHARE_FEATURE_COLUMNS) | {"date", "ticker"},
        available_fields=set(TUSHARE_AVAILABLE_FEATURE_COLUMNS) | {"date", "ticker"},
        blocked_fields=set(TUSHARE_BLOCKED_FEATURE_COLUMNS),
        aliases=FIELD_ALIASES,
    )
    paths = write_field_resolution_report(report, args.output_dir)
    counts: dict[str, int] = {}
    for row in report["field_resolutions"]:
        counts[row["resolution_status"]] = counts.get(row["resolution_status"], 0) + 1
    print(json.dumps({
        "run_id": args.run_id,
        "field_count": len(report["field_resolutions"]),
        "resolution_counts": counts,
        "ready_for_route_registry_rerun": report["ready_for_route_registry_rerun"],
        "controlled_execution_allowed": report["controlled_execution_allowed"],
        "queue_write_allowed": report["queue_write_allowed"],
        "json_path": str(paths["json"].relative_to(ROOT)) if paths["json"].is_relative_to(ROOT) else str(paths["json"]),
        "markdown_path": str(paths["markdown"].relative_to(ROOT)) if paths["markdown"].is_relative_to(ROOT) else str(paths["markdown"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
