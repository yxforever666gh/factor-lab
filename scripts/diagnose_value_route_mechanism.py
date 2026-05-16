#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.portfolio_mechanism_diagnostics import write_mechanism_diagnostics


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="artifacts/value_route_mechanism_diagnostics")
    parser.add_argument("run_dirs", nargs="*")
    args = parser.parse_args()
    run_dirs = args.run_dirs or [
        "artifacts/value_route_controlled_smoke/1_industry_relative_value_v2",
        "artifacts/value_route_controlled_smoke/2_value_quality_no_distress_v2",
        "artifacts/value_route_controlled_smoke/3_value_momentum_confirmation_v2",
    ]
    result = write_mechanism_diagnostics(run_dirs=run_dirs, output_dir=args.output_dir)
    print(json.dumps({"json_path": result["json_path"], "markdown_path": result["markdown_path"], "runs": len(result["diagnostics"])}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
