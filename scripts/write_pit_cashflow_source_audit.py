from __future__ import annotations

import argparse
import json

from factor_lab.pit_cashflow_source_audit import write_cashflow_source_audit


def main() -> int:
    parser = argparse.ArgumentParser(description="Write PIT cashflow raw-source provenance audit artifacts")
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--cache-dir", default=None)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()
    kwargs = {k: v for k, v in {"dataset_path": args.dataset, "cache_dir": args.cache_dir, "output_dir": args.output_dir}.items() if v}
    payload = write_cashflow_source_audit(**kwargs)
    print(json.dumps({
        "cashflow_coverage": payload.get("cashflow_coverage"),
        "final_diagnosis": payload.get("final_diagnosis"),
        "raw_source_proof_level": payload.get("raw_source_proof_level"),
        "artifact": "artifacts/pit_cashflow_source_audit/source_audit.json",
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
