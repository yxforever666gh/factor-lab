#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from factor_lab.expanded_market_data import HistoricalSTSnapshot
from factor_lab.expanded_research_runner import (
    RetryingTushareClient,
    SessionTushareClient,
    build_expanded_feature_store,
    build_membership_from_raw_daily,
    cache_reference_data,
    download_raw_partitions,
    download_hybrid_supplements,
    load_expanded_config,
    resolve_expanded_plan,
    run_expanded_rounds,
    run_offline_canary,
)
from factor_lab.tushare_provider import TushareDataProvider
from factor_lab.research_os.legacy_entrypoint import retired_legacy_entrypoint


DEFAULT_CONFIG = ROOT / "configs" / "tushare_long_only_expanded.json"


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _client() -> SessionTushareClient:
    return SessionTushareClient(TushareDataProvider().pro)


def main() -> int:
    os.chdir(ROOT)
    parser = argparse.ArgumentParser(description="Run the frozen expanded-sample long-only research protocol.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument(
        "--phase",
        choices=["canary", "plan", "live-canary-download", "live-canary-build", "download", "supplements", "build", "rounds", "all"],
        default="canary",
    )
    parser.add_argument("--max-partitions", type=int, default=None)
    parser.add_argument("--canary-open-days", type=int, default=20)
    parser.add_argument("--datasets", default="", help="Comma-separated raw datasets to download (daily,daily_basic,adj_factor).")
    parser.add_argument("--max-supplement-tickers", type=int, default=None)
    args = parser.parse_args()

    if args.phase in {"rounds", "all"}:
        # Download/build/canary modes remain useful as regression and migration
        # tools.  Candidate-producing rounds are retired in favour of the
        # canonical Research OS evaluator and lifetime trial ledger.
        return retired_legacy_entrypoint(
            f"scripts/run_expanded_long_only_research.py --phase {args.phase}"
        )

    config = load_expanded_config(args.config)
    output_root = ROOT / str(config["output_dir"])
    if args.phase == "canary":
        result = run_offline_canary(config)
        print(json.dumps({"phase": "canary", "passes": result["passes"]}, ensure_ascii=False))
        return 0

    if args.phase == "rounds":
        feature_path = ROOT / str(config["feature_store_dir"]) / "expanded_top500_features.parquet"
        execution_path = ROOT / str(config["feature_store_dir"]) / "expanded_execution_prices.parquet"
        if not feature_path.exists():
            raise SystemExit(f"missing feature store: {feature_path}")
        if not execution_path.exists():
            raise SystemExit(f"missing execution price store: {execution_path}")
        comparison = run_expanded_rounds(
            pd.read_parquet(feature_path),
            config,
            execution_frame=pd.read_parquet(execution_path),
        )
        print(json.dumps({
            "paper_candidate_count": comparison["paper_candidate_count"],
            "shorting_used": comparison["shorting_used"],
            "best_corrected_strategy": (comparison.get("best_corrected_strategy") or {}).get("name"),
            "best_validation_variant": (comparison.get("best_validation_variant") or {}).get("name"),
            "report": str(Path(config["output_dir"]) / "three_round_report.md"),
        }, ensure_ascii=False, indent=2))
        return 0

    client = _client()
    analysis_limit = args.canary_open_days if args.phase in {"live-canary-download", "live-canary-build"} else None
    calendar, plan = resolve_expanded_plan(client, config, analysis_open_day_limit=analysis_limit)
    plan_path = output_root / ("live_canary_download_plan.json" if analysis_limit else "expanded_download_plan.json")
    _write_json(plan_path, plan)
    print(json.dumps({
        "phase": "plan",
        "analysis_start": plan["analysis_start"],
        "analysis_end": plan["analysis_end"],
        "fetch_start": plan["fetch_start"],
        "fetch_end": plan["fetch_end"],
        "partition_count": plan["partition_count"],
        "pending_partition_count": plan["pending_partition_count"],
    }, ensure_ascii=False), flush=True)
    if args.phase == "plan":
        return 0

    if args.phase in {"live-canary-download", "download", "all"}:
        requested_datasets = {item.strip() for item in args.datasets.split(",") if item.strip()}
        if args.phase == "all" and config.get("market_detail_source") == "hybrid_tushare_basic_akshare_hfq":
            requested_datasets = {"daily"}
        download_plan = dict(plan)
        if requested_datasets:
            download_plan["partitions"] = [row for row in plan["partitions"] if row["dataset"] in requested_datasets]
            download_plan["pending_partition_count"] = sum(row.get("status") != "complete" for row in download_plan["partitions"])
        download = download_raw_partitions(
            client,
            download_plan,
            checkpoint_path=ROOT / str(config["checkpoint_path"]),
            requests_per_minute=float(config.get("request_rate_per_minute") or 120),
            max_partitions=args.max_partitions,
            progress_every=250,
            workers=int(config.get("download_workers") or 1),
            checkpoint_flush_every=int(config.get("checkpoint_flush_every") or 25),
        )
        _write_json(output_root / "latest_download_result.json", download)
        print(json.dumps(download, ensure_ascii=False), flush=True)
        if args.phase in {"live-canary-download", "download"}:
            return 0
        if args.max_partitions is not None and download["completed_this_run"] < plan["pending_partition_count"]:
            return 0

    required_datasets = {"daily"} if config.get("market_detail_source") == "hybrid_tushare_basic_akshare_hfq" else {"daily", "daily_basic", "adj_factor"}
    required_pending = sum(row.get("status") != "complete" and row.get("dataset") in required_datasets for row in plan["partitions"])
    if args.phase in {"live-canary-build", "build", "supplements"} and required_pending:
        raise SystemExit(f"required raw download incomplete: {required_pending} partitions remain")

    metadata, historical_st, reference = cache_reference_data(client, config, plan)
    _write_json(output_root / "reference_data_status.json", reference)

    if args.phase in {"supplements", "all"}:
        membership = build_membership_from_raw_daily(config, calendar, plan, metadata, historical_st)
        supplement_result = download_hybrid_supplements(
            client,
            config,
            plan,
            membership,
            max_tickers=args.max_supplement_tickers,
        )
        print(json.dumps({"phase": "supplements", **supplement_result}, ensure_ascii=False), flush=True)
        if args.phase == "supplements":
            return 0

    if args.phase in {"live-canary-build", "build", "all"}:
        build_config = dict(config)
        if args.phase == "live-canary-build":
            build_config["output_dir"] = str(Path(config["output_dir"]) / "live_canary")
            build_config["feature_store_dir"] = str(Path(config["feature_store_dir"]) / "live_canary")
            build_config["market_detail_source"] = "tushare_daily_partitions"
        frame, audit, manifest = build_expanded_feature_store(
            build_config,
            calendar,
            plan,
            metadata,
            historical_st,
        )
        print(json.dumps({
            "phase": "build",
            "rows": len(frame),
            "securities": int(frame["ticker"].nunique()),
            "audit_passes": audit["acceptance"]["passes"],
            "snapshot": manifest["manifest_sha256"],
        }, ensure_ascii=False), flush=True)
        if args.phase == "live-canary-build":
            return 0
    else:
        feature_path = ROOT / str(config["feature_store_dir"]) / "expanded_top500_features.parquet"
        frame = pd.read_parquet(feature_path)

    if args.phase == "all":
        execution_path = ROOT / str(config["feature_store_dir"]) / "expanded_execution_prices.parquet"
        comparison = run_expanded_rounds(frame, config, execution_frame=pd.read_parquet(execution_path))
        print(json.dumps({
            "paper_candidate_count": comparison["paper_candidate_count"],
            "shorting_used": comparison["shorting_used"],
            "report": str(Path(config["output_dir"]) / "three_round_report.md"),
        }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
