#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.gated_factor_feeder import write_gated_factor_configs
from factor_lab.research_gate import load_hypothesis


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare gated research configs; dry-run by default.")
    parser.add_argument("--hypothesis", default="configs/research_hypotheses/value_trap_filter_quality_confirmation.json")
    parser.add_argument("--hypothesis-id", default="")
    parser.add_argument("--output-dir", default="artifacts/gated_research/value_trap_filter_quality_confirmation")
    parser.add_argument("--preflight", default="artifacts/data_preflight/p0_pit_data_preflight_latest.json")
    parser.add_argument("--max-variants", type=int, default=3)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    hypothesis = load_hypothesis(args.hypothesis)
    if args.hypothesis_id and args.hypothesis_id != hypothesis.get("hypothesis_id"):
        result = {"decision": "blocked", "reasons": ["hypothesis_id_mismatch"], "requested": args.hypothesis_id, "actual": hypothesis.get("hypothesis_id")}
    else:
        result = write_gated_factor_configs(
            hypothesis,
            output_dir=args.output_dir,
            write=args.write and not args.dry_run,
            preflight_path=args.preflight,
            max_variants=args.max_variants,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("decision") in {"dry_run_ready", "blocked"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
