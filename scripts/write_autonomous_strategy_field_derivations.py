#!/usr/bin/env python3
"""Write derivable field specs from field resolution report."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_field_derivations import build_field_derivation_specs, write_field_derivation_specs

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "artifacts" / "autonomous_strategy_lab" / "field_resolution_report.json"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "autonomous_strategy_lab"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args(argv)

    field_resolution_report = json.loads(Path(args.input).read_text(encoding="utf-8"))
    specs = build_field_derivation_specs(field_resolution_report)
    paths = write_field_derivation_specs(specs, args.output_dir)
    print(json.dumps({
        "run_id": specs.get("run_id"),
        "derived_field_count": len(specs["derived_fields"]),
        "materialized": specs["materialized"],
        "controlled_execution_allowed": specs["controlled_execution_allowed"],
        "queue_write_allowed": specs["queue_write_allowed"],
        "json_path": str(paths["json"].relative_to(ROOT)) if paths["json"].is_relative_to(ROOT) else str(paths["json"]),
        "markdown_path": str(paths["markdown"].relative_to(ROOT)) if paths["markdown"].is_relative_to(ROOT) else str(paths["markdown"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
