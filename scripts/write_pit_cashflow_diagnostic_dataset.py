from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from factor_lab.pit_cashflow_source_audit import build_cashflow_source_audit, audit_to_markdown
from factor_lab.tushare_provider import TushareDataProvider

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATASET = ROOT / "artifacts" / "value_route_bucket_aware" / "daemon_runs" / "value_trap_filter_quality_confirmation" / "dataset.csv"
DEFAULT_CACHE_DIR = ROOT / "artifacts" / "tushare_cache"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "pit_cashflow_source_audit"


def write_cashflow_diagnostic_dataset(
    dataset_path: str | Path = DEFAULT_DATASET,
    cache_dir: str | Path = DEFAULT_CACHE_DIR,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> dict:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    frame = pd.read_csv(dataset_path)
    provider = TushareDataProvider()
    enriched = provider.enrich_frame_with_pit_financial_features(
        frame,
        cache_dir=cache_dir,
        retain_pit_cashflow_diagnostics=True,
    )
    diagnostic_dataset = output / "diagnostic_dataset.csv"
    enriched.to_csv(diagnostic_dataset, index=False)
    audit = build_cashflow_source_audit(enriched, cache_dir=cache_dir)
    audit["diagnostic_dataset"] = str(diagnostic_dataset)
    (output / "source_audit_after_retention.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "source_audit_after_retention.md").write_text(audit_to_markdown(audit), encoding="utf-8")
    return audit


def main() -> int:
    parser = argparse.ArgumentParser(description="Regenerate diagnostic PIT cashflow dataset with raw numerator/denominator columns")
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--cache-dir", default=None)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()
    kwargs = {k: v for k, v in {"dataset_path": args.dataset, "cache_dir": args.cache_dir, "output_dir": args.output_dir}.items() if v}
    payload = write_cashflow_diagnostic_dataset(**kwargs)
    print(json.dumps({
        "cashflow_coverage": payload.get("cashflow_coverage"),
        "final_diagnosis": payload.get("final_diagnosis"),
        "raw_source_proof_level": payload.get("raw_source_proof_level"),
        "diagnostic_dataset": payload.get("diagnostic_dataset"),
        "artifact": "artifacts/pit_cashflow_source_audit/source_audit_after_retention.json",
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
