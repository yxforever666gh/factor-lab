#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_quality_profit_proxy_revision import (
    build_quality_profit_proxy_revision,
    write_quality_profit_proxy_revision,
)

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "artifacts" / "autonomous_strategy_lab"
DEFAULT_RESOLUTION = ASL / "quality_cashflow_field_resolution.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write proxy revision for blocked quality-cashflow value-repair route.")
    parser.add_argument("--run-id", default="quality_profit_proxy_value_repair_v1")
    parser.add_argument("--field-resolution", default=str(DEFAULT_RESOLUTION))
    parser.add_argument("--output-dir", default=str(ASL))
    args = parser.parse_args(argv)

    field_resolution = json.loads(Path(args.field_resolution).read_text(encoding="utf-8"))
    revision = build_quality_profit_proxy_revision(run_id=args.run_id, field_resolution=field_resolution)
    paths = write_quality_profit_proxy_revision(revision, args.output_dir)
    print(
        json.dumps(
            {
                "mechanism_id": revision["mechanism_id"],
                "decision": revision["decision"],
                "revision_status": revision["revision_status"],
                "recommended_next_step": revision["recommended_next_step"],
                "controlled_execution_allowed": revision["controlled_execution_allowed"],
                "queue_write_allowed": revision["queue_write_allowed"],
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
