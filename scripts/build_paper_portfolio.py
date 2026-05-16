#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.approved_universe import resolve_paper_portfolio_inputs
from factor_lab.paper_portfolio import (
    build_paper_portfolio,
    append_portfolio_history,
    build_portfolio_change_log,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Factor Lab paper portfolio")
    parser.add_argument("--strategy-name", default="paper_candidates_only")
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--factor-name", default=None)
    parser.add_argument("--expression", default=None)
    parser.add_argument("--target-position-count", type=int, default=None)
    parser.add_argument("--single-name-weight-cap", type=float, default=None)
    args = parser.parse_args()

    inputs = resolve_paper_portfolio_inputs(
        db_path="artifacts/factor_lab.db",
        approved_universe_path="artifacts/approved_candidate_universe.json",
        fallback_candidate_pool_path="artifacts/tushare_workflow/candidate_pool.json",
        fallback_dataset_path="artifacts/tushare_workflow/dataset.csv",
    )
    dataset_path = Path(args.dataset) if args.dataset else Path(inputs["dataset_path"]) if inputs.get("dataset_path") else Path("artifacts/tushare_workflow/dataset.csv")
    if args.factor_name and args.expression:
        factor_definitions = [
            {
                "name": args.factor_name,
                "expression": args.expression,
                "allocated_weight": 1.0,
                "portfolio_bucket": "core_alpha",
                "approval_tier": "manual_cli",
                "lifecycle_state": "paper_baseline",
            }
        ]
        source_metadata = {
            "source": "cli_factor_definition",
            "dataset_path": dataset_path,
            "factor_name": args.factor_name,
        }
    else:
        factor_definitions = inputs.get("factor_definitions") or []
        source_metadata = {"source": inputs.get("source"), **(inputs.get("metadata") or {})}

    current = build_paper_portfolio(
        dataset_path=dataset_path,
        factor_definitions=factor_definitions,
        output_dir="artifacts/paper_portfolio",
        strategy_name=args.strategy_name,
        target_position_count=args.target_position_count,
        single_name_weight_cap=args.single_name_weight_cap,
        source_metadata=source_metadata,
    )
    append_portfolio_history(
        current_path="artifacts/paper_portfolio/current_portfolio.json",
        history_path="artifacts/paper_portfolio/portfolio_history.json",
    )
    build_portfolio_change_log(
        current_path="artifacts/paper_portfolio/current_portfolio.json",
        history_path="artifacts/paper_portfolio/portfolio_history.json",
        output_path="artifacts/paper_portfolio/portfolio_change_log.md",
    )
    print(
        f"paper portfolio built from source={source_metadata.get('source')} dataset={dataset_path} "
        f"positions={current.get('position_count')} strategy={current.get('strategy_name')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
