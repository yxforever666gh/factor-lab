#!/usr/bin/env python3
"""Resolve fields for the next quality-cashflow distress mechanism route."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from factor_lab.autonomous_strategy_distress_field_resolution import (
    build_quality_cashflow_distress_field_resolution,
    write_distress_field_resolution,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROUTE_REGISTRY = ROOT / "configs" / "autonomous_strategy_routes.json"
DEFAULT_FEATURE_CACHE = ROOT / "artifacts" / "tushare_cache" / "tushare_2016-09-09_2023-12-31_97.csv"
DEFAULT_PIT_CACHE = ROOT / "artifacts" / "tushare_cache" / "pit_financial_2020-06-02_2023-12-28_77_96401d85299a_v2.csv"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "autonomous_strategy_lab"


def _columns(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return set(pd.read_csv(path, nrows=0).columns)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--route-registry", default=str(DEFAULT_ROUTE_REGISTRY))
    parser.add_argument("--feature-cache", default=str(DEFAULT_FEATURE_CACHE))
    parser.add_argument("--pit-cache", default=str(DEFAULT_PIT_CACHE))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args(argv)

    route_registry = json.loads(Path(args.route_registry).read_text(encoding="utf-8"))
    feature_fields = _columns(Path(args.feature_cache))
    pit_fields = _columns(Path(args.pit_cache))
    report = build_quality_cashflow_distress_field_resolution(
        run_id=args.run_id,
        route_registry=route_registry,
        feature_fields=feature_fields,
        pit_fields=pit_fields,
    )
    paths = write_distress_field_resolution(report, args.output_dir)
    print(json.dumps({
        "run_id": args.run_id,
        "decision": report["decision"],
        "ready_for_distress_screen": report["ready_for_distress_screen"],
        "unresolved_field_count": report["unresolved_field_count"],
        "field_statuses": {row["field"]: row["resolution_status"] for row in report["field_resolutions"]},
        "controlled_execution_allowed": report["controlled_execution_allowed"],
        "queue_write_allowed": report["queue_write_allowed"],
        "json_path": str(paths["json"].relative_to(ROOT)) if paths["json"].is_relative_to(ROOT) else str(paths["json"]),
        "markdown_path": str(paths["markdown"].relative_to(ROOT)) if paths["markdown"].is_relative_to(ROOT) else str(paths["markdown"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
