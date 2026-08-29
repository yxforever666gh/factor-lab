from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_lab.data import RuntimeLayout, sync_data, turnover_amount_to_rmb
from factor_lab.data.sources import DATASET_FIELDS


class FixtureClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def query(self, endpoint: str, **kwargs) -> pd.DataFrame:
        self.calls.append((endpoint, kwargs))
        if endpoint == "trade_cal":
            return pd.DataFrame(
                {
                    "exchange": ["SSE", "SSE", "SSE"],
                    "cal_date": ["20240102", "20240103", "20240104"],
                    "is_open": [1, 0, 1],
                    "pretrade_date": ["20231229", "20240102", "20240102"],
                }
            )
        fields = [field for field in DATASET_FIELDS[endpoint].split(",") if field]
        row = {field: 1.0 for field in fields}
        row["ts_code"] = "000001.SZ"
        row["trade_date"] = kwargs["trade_date"]
        return pd.DataFrame([row], columns=fields)


def test_turnover_amount_units_are_source_aware() -> None:
    tushare_thousand_rmb = pd.Series([274_337.1858])
    akshare_rmb = pd.Series([274_337_186.0])

    tushare = turnover_amount_to_rmb(tushare_thousand_rmb, source="tushare_daily")
    akshare = turnover_amount_to_rmb(akshare_rmb, source="akshare")
    estimate = turnover_amount_to_rmb(akshare_rmb, source="turnover_estimate_rmb")

    assert abs(tushare.iloc[0] - 274_337_185.8) < 0.001
    assert abs(tushare.iloc[0] - akshare.iloc[0]) < 1.0
    assert estimate.iloc[0] == akshare.iloc[0]


def _config(tmp_path: Path) -> tuple[Path, dict]:
    payload = {
        "schema_version": 1,
        "runtime_root": "runtime",
        "paths": {
            "data": "data",
            "raw": "data/raw",
            "top500": "data/top500",
            "runs": "runs",
            "legacy": "legacy",
        },
        "top500": {
            "features_file": "features.parquet",
            "execution_file": "execution.parquet",
            "membership_file": "membership.parquet",
        },
        "sync": {
            "datasets": ["daily", "daily_basic", "adj_factor"],
            "checkpoint_file": "checkpoint.json",
            "request_rate_per_minute": 0,
            "verify_hashes_on_resume": True,
        },
    }
    path = tmp_path / "configs" / "data.json"
    path.parent.mkdir()
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path, payload


def test_sync_writes_daily_partitions_and_resumes_verified_checkpoint(tmp_path: Path) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    client = FixtureClient()

    first = sync_data(
        "2024-01-02",
        "2024-01-04",
        config_path=config_path,
        layout=layout,
        client=client,
    )
    first_data_calls = [name for name, _ in client.calls if name != "trade_cal"]
    assert first["status"] == "complete"
    assert first["completed_this_run"] == 6
    assert len(first_data_calls) == 6
    assert layout.checkpoint_path.is_file()

    second_client = FixtureClient()
    second = sync_data(
        "2024-01-02",
        "2024-01-04",
        config_path=config_path,
        layout=layout,
        client=second_client,
        resume=True,
    )
    assert second["completed_before"] == 6
    assert second["completed_this_run"] == 0
    assert [name for name, _ in second_client.calls if name != "trade_cal"] == []


def test_sync_treats_empty_open_market_partition_as_retryable(tmp_path: Path) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)

    class EmptyClient(FixtureClient):
        def query(self, endpoint: str, **kwargs) -> pd.DataFrame:
            if endpoint == "trade_cal":
                return super().query(endpoint, **kwargs)
            return pd.DataFrame(columns=DATASET_FIELDS[endpoint].split(","))

    result = sync_data(
        "2024-01-02",
        "2024-01-02",
        config_path=config_path,
        layout=layout,
        client=EmptyClient(),
        datasets=("daily",),
    )

    assert result["status"] == "waiting"
    assert result["reason"] == "provider_empty"
    assert result["dataset"] == "daily"
    assert result["trade_date"] == "2024-01-02"
    assert result["completed_this_run"] == 0
    assert result["remaining_partition_count"] == 1
    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    assert "daily/2024-01-02" not in checkpoint["partitions"]


def test_sync_treats_empty_trade_calendar_as_retryable(tmp_path: Path) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)

    class EmptyCalendarClient(FixtureClient):
        def query(self, endpoint: str, **kwargs) -> pd.DataFrame:
            if endpoint == "trade_cal":
                return pd.DataFrame(
                    columns=["exchange", "cal_date", "is_open", "pretrade_date"]
                )
            return super().query(endpoint, **kwargs)

    result = sync_data(
        "2024-01-02",
        "2024-01-02",
        config_path=config_path,
        layout=layout,
        client=EmptyCalendarClient(),
        datasets=("daily",),
    )

    assert result["status"] == "waiting"
    assert result["reason"] == "provider_empty"
    assert result["dataset"] == "trade_cal"
    assert result["completed_this_run"] == 0
