#!/usr/bin/env python3
"""Plan or execute a bounded historical valuation cache extension for Autonomous Strategy Lab."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from factor_lab.autonomous_strategy_cache_extension import build_history_cache_extension_plan, write_cache_extension_plan

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_COVERAGE_PREFLIGHT = ROOT / "artifacts" / "autonomous_strategy_lab" / "historical_valuation_coverage_preflight.json"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "autonomous_strategy_lab"
DEFAULT_CACHE_DIR = ROOT / "artifacts" / "tushare_cache"


def _execute_fetch(plan: dict) -> dict:
    if not plan.get("external_request_required"):
        return {**plan, "execution_status": "no_external_fetch_required"}
    if not os.environ.get(str(plan.get("token_env_var") or "TUSHARE_TOKEN")):
        return {
            **plan,
            "execution_status": "blocked_missing_tushare_token",
            "next_allowed_actions": ["configure_tushare_token", "rerun_with_allow_fetch"],
        }

    from factor_lab.tushare_provider import TushareDataProvider, TushareRequest

    provider = TushareDataProvider()
    request = TushareRequest(
        start_date=str(plan["target_start_date"]),
        end_date=str(plan["target_end_date"]),
        universe_limit=int(plan["target_universe_count"]),
        universe_codes=list(plan.get("target_universe_codes") or []),
        cache_dir=str(DEFAULT_CACHE_DIR),
        use_request_cache=True,
    )
    dataset = provider.load_dataset(request)
    return {
        **plan,
        "execution_status": "fetch_completed",
        "fetched_row_count": int(len(dataset.frame)),
        "fetched_ticker_count": int(dataset.frame["ticker"].nunique()) if "ticker" in dataset.frame.columns else 0,
        "next_allowed_actions": ["rerun_coverage_preflight"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--coverage-preflight", default=str(DEFAULT_COVERAGE_PREFLIGHT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR))
    parser.add_argument("--allow-fetch", action="store_true")
    args = parser.parse_args(argv)

    coverage_preflight = json.loads(Path(args.coverage_preflight).read_text(encoding="utf-8"))
    cache_dir = Path(args.cache_dir)
    plan = build_history_cache_extension_plan(
        run_id=args.run_id,
        coverage_preflight=coverage_preflight,
        root=ROOT,
        cache_dir=str(cache_dir.relative_to(ROOT)) if cache_dir.is_relative_to(ROOT) else str(cache_dir),
    )
    if args.allow_fetch:
        plan = _execute_fetch(plan)
    paths = write_cache_extension_plan(plan, args.output_dir)
    print(json.dumps({
        "run_id": args.run_id,
        "execution_status": plan["execution_status"],
        "action": plan.get("action"),
        "external_request_required": plan.get("external_request_required"),
        "token_configured": plan.get("token_configured"),
        "source_path": plan.get("source_path"),
        "target_cache_path": plan.get("target_cache_path"),
        "target_start_date": plan.get("target_start_date"),
        "target_end_date": plan.get("target_end_date"),
        "target_universe_count": plan.get("target_universe_count"),
        "queue_write_allowed": plan.get("queue_write_allowed"),
        "controlled_execution_allowed": plan.get("controlled_execution_allowed"),
        "json_path": str(paths["json"].relative_to(ROOT)) if paths["json"].is_relative_to(ROOT) else str(paths["json"]),
        "markdown_path": str(paths["markdown"].relative_to(ROOT)) if paths["markdown"].is_relative_to(ROOT) else str(paths["markdown"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
