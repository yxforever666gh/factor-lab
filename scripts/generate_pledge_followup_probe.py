#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from factor_lab.pledge_followup_probe import write_pledge_followup_probe


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent-config-path", default="artifacts/pledge_controlled_probe_plan/value_quality_high_pledge_record_count_confirmation.json")
    parser.add_argument("--validation-path", default="artifacts/pledge_controlled_validation/pledge_controlled_validation.json")
    parser.add_argument("--output-dir", default="artifacts/pledge_followup_probe")
    args = parser.parse_args()

    result = write_pledge_followup_probe(
        parent_config_path=Path(args.parent_config_path),
        validation_path=Path(args.validation_path),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result.get("ok") else 2)


if __name__ == "__main__":
    main()
