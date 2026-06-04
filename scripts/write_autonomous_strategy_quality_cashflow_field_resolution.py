#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from factor_lab.autonomous_strategy_quality_cashflow_field_resolution import (
    build_quality_cashflow_field_resolution,
    write_quality_cashflow_field_resolution,
)

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "artifacts" / "autonomous_strategy_lab"
DEFAULT_REQUEST = ASL / "quality_cashflow_value_repair_request.json"
DEFAULT_FEATURE_CACHE = ROOT / "artifacts" / "tushare_cache" / "tushare_2016-09-09_2023-12-31_97.csv"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Resolve fields for quality-cashflow value-repair mechanism.")
    parser.add_argument("--run-id", default="quality_cashflow_value_repair_v1")
    parser.add_argument("--mechanism-request", default=str(DEFAULT_REQUEST))
    parser.add_argument("--feature-cache", default=str(DEFAULT_FEATURE_CACHE))
    parser.add_argument("--output-dir", default=str(ASL))
    args = parser.parse_args(argv)

    mechanism_request = json.loads(Path(args.mechanism_request).read_text(encoding="utf-8"))
    available_fields = set(pd.read_csv(args.feature_cache, nrows=0).columns)
    report = build_quality_cashflow_field_resolution(
        run_id=args.run_id,
        mechanism_request=mechanism_request,
        available_fields=available_fields,
    )
    paths = write_quality_cashflow_field_resolution(report, args.output_dir)
    print(
        json.dumps(
            {
                "mechanism_id": report["mechanism_id"],
                "decision": report["decision"],
                "recommended_next_step": report["recommended_next_step"],
                "ready_for_cheap_screen": report["ready_for_cheap_screen"],
                "missing_fields": report["missing_fields"],
                "proxy_blocked_fields": report["proxy_blocked_fields"],
                "pit_validation_fields": report["pit_validation_fields"],
                "controlled_execution_allowed": report["controlled_execution_allowed"],
                "queue_write_allowed": report["queue_write_allowed"],
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
