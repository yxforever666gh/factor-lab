#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.small_institutional_backtest_matrix import write_small_institutional_backtest_matrix
from factor_lab.small_institutional_simulation_policy import load_small_institutional_simulation_policy


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Write bounded/resumable small institutional backtest matrix artifacts.")
    parser.add_argument("--dry-run", action="store_true", help="Preview planned combinations without executing backtests.")
    parser.add_argument("--max-combinations", type=int, default=None, help="Maximum combinations to execute/write in this run.")
    parser.add_argument("--policy", default=None, help="Optional simulation policy path.")
    args = parser.parse_args()

    policy = load_small_institutional_simulation_policy(args.policy) if args.policy else load_small_institutional_simulation_policy()
    max_combinations = args.max_combinations
    if max_combinations is None and not args.dry_run:
        max_combinations = int(policy.get("max_combinations_per_run") or 500)

    payload = write_small_institutional_backtest_matrix(
        dataset_path=policy["dataset_path"],
        signal_columns=policy["signal_columns"],
        year_windows=policy["year_windows"],
        holding_counts=policy["holding_counts"],
        rebalance_frequencies=policy["rebalance_frequencies"],
        cost_bps_values=policy["cost_bps_values"],
        return_column=policy.get("return_column") or "forward_return_5d",
        max_combinations=max_combinations,
        dry_run=args.dry_run,
    )
    print(
        json.dumps(
            {
                "matrix_status": payload.get("matrix_status"),
                "execution": payload.get("execution") or {},
                "summary": payload.get("summary") or {},
                "best_result": payload.get("best_result") or {},
            },
            ensure_ascii=False,
            indent=2,
        )
    )
