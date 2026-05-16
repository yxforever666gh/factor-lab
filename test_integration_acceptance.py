#!/usr/bin/env python3
from __future__ import annotations

import json
import tempfile
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from factor_lab.factor_pipeline_integration import build_integrated_factor_reports


def build_synthetic_dataset(path: Path) -> None:
    rng = np.random.default_rng(42)
    dates = pd.date_range("2024-01-01", periods=120, freq="D")
    rows = []
    tickers = [f"stk_{i:03d}" for i in range(60)]
    base_close = {t: 20 + i * 0.3 for i, t in enumerate(tickers)}

    for d_idx, date in enumerate(dates):
        for i, ticker in enumerate(tickers):
            close = base_close[ticker] * (1 + 0.0005 * d_idx) + rng.normal(0, 0.8)
            close_20d = max(1.0, close * (1 - rng.normal(0.02, 0.03)))
            close_60d = max(1.0, close * (1 - rng.normal(0.05, 0.05)))
            volume = abs(rng.normal(2e6, 5e5))
            volume_20d = abs(rng.normal(2e6, 2e5))
            amount = volume * close
            amount_20d = amount * (0.9 + 0.2 * rng.random())
            total_mv = abs(rng.normal(8e9, 2e9))
            pb = abs(rng.normal(2.0, 0.8)) + 0.2
            pe = abs(rng.normal(18, 6)) + 1
            net_profit_yoy = rng.normal(0.15, 0.2)
            revenue_yoy = rng.normal(0.12, 0.15)
            momentum = close / close_20d - 1
            forward_return_5d = 0.002 * momentum + 0.001 * net_profit_yoy + rng.normal(0, 0.015)
            rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "close": close,
                    "close_20d": close_20d,
                    "close_60d": close_60d,
                    "volume": volume,
                    "volume_20d": volume_20d,
                    "amount": amount,
                    "amount_20d": amount_20d,
                    "total_mv": total_mv,
                    "pb": pb,
                    "pe": pe,
                    "net_profit_yoy": net_profit_yoy,
                    "revenue_yoy": revenue_yoy,
                    "forward_return_5d": forward_return_5d,
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        dataset_path = tmp / "dataset.csv"
        build_synthetic_dataset(dataset_path)

        approved_universe = {
            "rows": [
                {"factor_name": "mom20", "expression": "close / close_20d - 1", "allocated_weight": 0.4},
                {"factor_name": "value_pb", "expression": "1 / pb", "allocated_weight": 0.3},
                {"factor_name": "growth_np", "expression": "net_profit_yoy", "allocated_weight": 0.3},
            ]
        }
        recent_artifacts = [{"run": {"run_id": "synthetic-run"}, "dataset_path": dataset_path}]

        payload = build_integrated_factor_reports(
            approved_universe=approved_universe,
            recent_artifacts=recent_artifacts,
            artifacts_dir=tmp,
            validation_simulations=100,
        )

        print("available:", payload["available"])
        print("approved_factor_count:", payload["approved_factor_count"])
        print("optimization keys:", sorted(payload["optimization"].keys())[:5])
        print("capacity reports:", len(payload["capacity"]))
        print("attribution reports:", len(payload["attribution"]))
        print("monitoring reports:", len(payload["monitoring"]))
        print("validation reports:", len(payload["validation"]))

        assert payload["available"] is True
        assert payload["approved_factor_count"] == 3
        assert len(payload["capacity"]) == 3
        assert len(payload["attribution"]) == 3
        assert len(payload["validation"]) == 3
        assert (tmp / "integrated_factor_reports.json").exists()
        assert (tmp / "capacity_reports.json").exists()
        assert (tmp / "attribution_reports.json").exists()
        assert (tmp / "monitoring_reports.json").exists()
        assert (tmp / "validation_reports.json").exists()

        print("integration acceptance: PASS")


if __name__ == "__main__":
    main()
