from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.pit_cashflow_denominator_audit import write_cashflow_denominator_audit

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Write PIT cashflow denominator candidate audit")
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()
    kwargs = {k: v for k, v in {"dataset_path": args.dataset, "output_dir": args.output_dir}.items() if v}
    audit = write_cashflow_denominator_audit(**kwargs)
    print(json.dumps({
        "decision": audit.get("decision"),
        "baseline_nonzero_coverage": audit.get("baseline_nonzero_coverage"),
        "best_candidate": audit.get("best_candidate"),
        "best_nonzero_coverage": audit.get("best_nonzero_coverage"),
        "hard_stops": audit.get("hard_stops"),
        "artifact": "artifacts/pit_cashflow_denominator_audit/denominator_audit.json",
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
