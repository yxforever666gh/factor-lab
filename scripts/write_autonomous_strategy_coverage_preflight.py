#!/usr/bin/env python3
"""Write historical valuation coverage preflight from cached Tushare data."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from factor_lab.autonomous_strategy_coverage_preflight import (
    build_historical_valuation_coverage_preflight,
    write_coverage_preflight,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROUTE_REGISTRY = ROOT / "configs" / "autonomous_strategy_routes.json"
DEFAULT_DERIVATION_SPECS = ROOT / "artifacts" / "autonomous_strategy_lab" / "field_derivation_specs.json"
DEFAULT_CACHE_DIR = ROOT / "artifacts" / "tushare_cache"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "autonomous_strategy_lab"
REQUIRED_SOURCE_COLUMNS = {"date", "ticker", "pb", "pe_ttm", "forward_return_5d"}


def choose_cache_file(cache_dir: Path, required_columns: set[str] = REQUIRED_SOURCE_COLUMNS) -> Path:
    candidates = sorted(cache_dir.glob("tushare_*.csv"))
    usable: list[tuple[int, Path]] = []
    for path in candidates:
        try:
            header = set(pd.read_csv(path, nrows=0).columns)
        except Exception:
            continue
        if required_columns.issubset(header):
            usable.append((path.stat().st_size, path))
    if not usable:
        raise FileNotFoundError(f"no Tushare cache under {cache_dir} contains columns: {sorted(required_columns)}")
    return max(usable, key=lambda item: item[0])[1]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--route-registry", default=str(DEFAULT_ROUTE_REGISTRY))
    parser.add_argument("--derivation-specs", default=str(DEFAULT_DERIVATION_SPECS))
    parser.add_argument("--cache-path", default=None, help="Optional explicit Tushare cache CSV. Defaults to the largest compatible cache.")
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--min-observations", type=int, default=756)
    parser.add_argument("--min-eligible-ticker-ratio", type=float, default=0.60)
    args = parser.parse_args(argv)

    cache_path = Path(args.cache_path) if args.cache_path else choose_cache_file(Path(args.cache_dir))
    route_registry = json.loads(Path(args.route_registry).read_text(encoding="utf-8"))
    derivation_specs = json.loads(Path(args.derivation_specs).read_text(encoding="utf-8"))
    frame = pd.read_csv(cache_path, usecols=lambda col: col in REQUIRED_SOURCE_COLUMNS)
    report = build_historical_valuation_coverage_preflight(
        run_id=args.run_id,
        route_registry=route_registry,
        derivation_specs=derivation_specs,
        frame=frame,
        source_path=str(cache_path.relative_to(ROOT)) if cache_path.is_relative_to(ROOT) else str(cache_path),
        min_observations=args.min_observations,
        min_eligible_ticker_ratio=args.min_eligible_ticker_ratio,
    )
    paths = write_coverage_preflight(report, args.output_dir)
    print(json.dumps({
        "run_id": args.run_id,
        "mode": report["mode"],
        "overall_status": report["overall_status"],
        "source_path": report["source_path"],
        "date_min": report["date_min"],
        "date_max": report["date_max"],
        "ticker_count": report["ticker_count"],
        "controlled_execution_allowed": report["controlled_execution_allowed"],
        "queue_write_allowed": report["queue_write_allowed"],
        "json_path": str(paths["json"].relative_to(ROOT)) if paths["json"].is_relative_to(ROOT) else str(paths["json"]),
        "markdown_path": str(paths["markdown"].relative_to(ROOT)) if paths["markdown"].is_relative_to(ROOT) else str(paths["markdown"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
