from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_lab.expanded_research_runner import (
    _derive_market_features,
    build_round_comparison,
    download_raw_partitions,
    load_expanded_config,
    run_offline_canary,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_expanded_config_is_frozen_to_long_only_50m_weekly():
    config = load_expanded_config(REPO_ROOT / "configs" / "tushare_long_only_expanded.json")

    assert config["universe"]["target_size"] == 500
    assert config["portfolio"]["mode"] == "long_only"
    assert config["portfolio"]["capital"] == 50_000_000
    assert config["portfolio"]["rebalance_every_days"] == 5
    assert config["portfolio"]["open_column"] == "open_adj"


def test_offline_canary_runs_without_shorting(tmp_path: Path):
    config = load_expanded_config(REPO_ROOT / "configs" / "tushare_long_only_expanded.json")
    config["output_dir"] = str(tmp_path / "output")

    result = run_offline_canary(config)

    assert result["passes"] is True
    assert result["checks"]["no_shorting"] is True
    assert result["evaluation"]["rebalance_count"] >= 10
    assert Path(config["output_dir"], "canary", "offline_canary.json").exists()


def test_derived_features_keep_negative_valuation_inputs_and_build_liquidity():
    dates = pd.bdate_range("2022-01-03", periods=25)
    frame = pd.DataFrame({
        "ts_code": ["000001.SZ"] * len(dates),
        "trade_date": dates,
        "open": [10.0] * len(dates),
        "high": [10.0] * len(dates),
        "low": [10.0] * len(dates),
        "close": [10.0] * len(dates),
        "close_adj": [10.0 + index / 10 for index in range(len(dates))],
        "amount": [100_000.0] * len(dates),
        "pct_chg": [10.0] * len(dates),
        "pe_ttm": [-10.0] * len(dates),
        "pb": [2.0] * len(dates),
        "total_mv": [1_000_000.0] * len(dates),
    })

    result = _derive_market_features(frame)

    assert result["earnings_yield"].dropna().iloc[0] == -0.1
    assert result["book_yield"].dropna().iloc[0] == 0.5
    assert result["adv_20"].notna().sum() >= 16
    assert result["is_one_price_limit_up"].all()


class _RawClient:
    def query(self, endpoint: str, **kwargs):
        return pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": [kwargs["trade_date"]], "value": [1.0]})


def test_download_raw_partitions_writes_verified_checkpoint(tmp_path: Path):
    partition_path = tmp_path / "raw" / "daily" / "trade_date=2024-01-02" / "part-000.parquet"
    partition = {
        "key": "daily/2024-01-02",
        "dataset": "daily",
        "trade_date": "2024-01-02",
        "path": str(partition_path),
        "status": "pending",
        "request": {"trade_date": "20240102", "fields": "ts_code,trade_date,value"},
    }
    checkpoint_path = tmp_path / "checkpoint.json"

    result = download_raw_partitions(
        _RawClient(),
        {"partitions": [partition]},
        checkpoint_path=checkpoint_path,
        requests_per_minute=1_000_000,
    )

    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert result["completed_this_run"] == 1
    assert partition_path.exists()
    assert checkpoint["partitions"]["daily/2024-01-02"]["status"] == "complete"
    assert len(checkpoint["partitions"]["daily/2024-01-02"]["sha256"]) == 64


def test_comparison_never_claims_shorting_or_live_profitability():
    comparison = build_round_comparison(
        [{"name": "legacy"}],
        [{"name": "corrected", "portfolio": {"status": "ok", "net_excess_annual_return": 0.01}}],
        [{"name": "variant", "status": "rejected", "windows": {"validation": {"net_excess_annual_return": -0.01}}}],
    )

    assert comparison["shorting_used"] is False
    assert comparison["capital"] == 50_000_000
    assert comparison["paper_candidate_count"] == 0
    assert comparison["stopped_without_threshold_relaxation"] is True
