from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.pit_value_trap_attribution import DEFAULT_OUTPUT_DIR, DEFAULT_RUN_DIR, write_attribution_reports


def main() -> int:
    parser = argparse.ArgumentParser(description="Write PIT value-trap field attribution diagnostics")
    parser.add_argument("--run-dir", default=str(DEFAULT_RUN_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--field", action="append", dest="fields", help="PIT field to diagnose; repeatable")
    args = parser.parse_args()
    payload = write_attribution_reports(run_dir=Path(args.run_dir), output_dir=Path(args.output_dir), fields=args.fields)
    print(json.dumps({
        "output_dir": payload["output_dir"],
        "decision": payload["decision"].get("decision"),
        "hard_stops": payload["decision"].get("hard_stops"),
        "reasons": payload["decision"].get("reasons"),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
