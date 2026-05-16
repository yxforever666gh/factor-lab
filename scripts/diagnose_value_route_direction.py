#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from factor_lab.value_route_direction_diagnostics import write_direction_diagnostics_from_runs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs-dir", default="artifacts/value_route_direction_diagnostics/runs")
    parser.add_argument("--output-dir", default="artifacts/value_route_direction_diagnostics")
    args = parser.parse_args()
    result = write_direction_diagnostics_from_runs(runs_dir=args.runs_dir, output_dir=args.output_dir)
    print(json.dumps({"json_path": result["json_path"], "markdown_path": result["markdown_path"], "routes": len(result["diagnostics"])}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
