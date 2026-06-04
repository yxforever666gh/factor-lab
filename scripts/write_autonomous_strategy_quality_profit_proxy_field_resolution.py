#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from factor_lab.autonomous_strategy_quality_profit_proxy_field_resolution import (
    build_quality_profit_proxy_field_resolution,
    write_quality_profit_proxy_field_resolution,
)

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "artifacts" / "autonomous_strategy_lab"
DEFAULT_REVISION = ASL / "quality_profit_proxy_value_repair_revision.json"
DEFAULT_FEATURE_CACHE = ROOT / "artifacts" / "tushare_cache" / "tushare_2016-09-09_2023-12-31_97.csv"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Resolve fields for quality-profit proxy value-repair mechanism.")
    parser.add_argument("--run-id", default="quality_profit_proxy_value_repair_v1")
    parser.add_argument("--proxy-revision", default=str(DEFAULT_REVISION))
    parser.add_argument("--feature-cache", default=str(DEFAULT_FEATURE_CACHE))
    parser.add_argument("--output-dir", default=str(ASL))
    args = parser.parse_args(argv)

    proxy_revision = json.loads(Path(args.proxy_revision).read_text(encoding="utf-8"))
    frame = pd.read_csv(args.feature_cache)
    available_fields = set(frame.columns)
    required_fields = proxy_revision.get("proxy_required_fields") or []
    coverage_by_field = {field: float(frame[field].notna().mean()) for field in required_fields if field in frame.columns}
    report = build_quality_profit_proxy_field_resolution(
        run_id=args.run_id,
        proxy_revision=proxy_revision,
        available_fields=available_fields,
        coverage_by_field=coverage_by_field,
    )
    paths = write_quality_profit_proxy_field_resolution(report, args.output_dir)
    print(
        json.dumps(
            {
                "mechanism_id": report["mechanism_id"],
                "decision": report["decision"],
                "recommended_next_step": report["recommended_next_step"],
                "ready_for_proxy_cheap_screen_plan": report["ready_for_proxy_cheap_screen_plan"],
                "missing_fields": report["missing_fields"],
                "low_coverage_fields": report["low_coverage_fields"],
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
