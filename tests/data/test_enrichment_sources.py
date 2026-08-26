from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_lab.data import RuntimeLayout, sync_enrichment
from factor_lab.data.sources import ENRICHMENT_DATASET_FIELDS


def _config(tmp_path: Path) -> tuple[Path, dict, RuntimeLayout]:
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
        "sync": {"request_rate_per_minute": 0, "verify_hashes_on_resume": True},
        "fundamentals": {
            "start_period": "20230930",
            "checkpoint_file": "fundamentals-checkpoint.json",
            "request_rate_per_minute": 0,
        },
        "reference_snapshots": {
            "checkpoint_file": "reference-snapshots-checkpoint.json",
            "request_rate_per_minute": 0,
        },
    }
    config_path = tmp_path / "configs" / "data.json"
    config_path.parent.mkdir()
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    layout.ensure_directories()
    membership = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "membership_month": ["2024-01", "2024-02"],
            "as_of_date": pd.to_datetime(["2023-12-29", "2024-01-31"]),
            "effective_start_date": pd.to_datetime(["2024-01-02", "2024-02-01"]),
            "effective_end_date": pd.to_datetime(["2024-01-31", "2024-02-29"]),
        }
    )
    membership.to_parquet(layout.membership_path, index=False)
    return config_path, payload, layout


class EnrichmentClient:
    def __init__(self, *, empty_st: bool = False) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.empty_st = empty_st

    def query(self, endpoint: str, **kwargs) -> pd.DataFrame:
        self.calls.append((endpoint, kwargs))
        fields = ENRICHMENT_DATASET_FIELDS[endpoint].split(",")
        if endpoint == "fina_indicator_vip":
            period = kwargs["period"]
            row = {field: 1.0 for field in fields}
            row.update(
                {
                    "ts_code": "000001.SZ",
                    "ann_date": (pd.Timestamp(period) + pd.Timedelta(days=30)).strftime(
                        "%Y%m%d"
                    ),
                    "end_date": period,
                    "update_flag": "0",
                }
            )
            return pd.DataFrame([row], columns=fields)
        trade_date = kwargs["trade_date"]
        if endpoint == "bak_basic":
            return pd.DataFrame(
                [
                    {
                        "trade_date": trade_date,
                        "ts_code": "000001.SZ",
                        "name": "平安银行",
                        "industry": "银行",
                        "list_date": "19910403",
                    }
                ],
                columns=fields,
            )
        if self.empty_st:
            return pd.DataFrame(columns=fields)
        return pd.DataFrame(
            [
                {
                    "ts_code": "000002.SZ",
                    "name": "*ST示例",
                    "trade_date": trade_date,
                    "type": "ST",
                    "type_name": "风险警示板",
                }
            ],
            columns=fields,
        )


def test_sync_enrichment_resumes_quarters_and_membership_month_ends(tmp_path: Path) -> None:
    config_path, _, layout = _config(tmp_path)
    client = EnrichmentClient()

    first = sync_enrichment(
        "2024-01-01",
        "2024-02-29",
        config_path=config_path,
        layout=layout,
        client=client,
    )

    assert first["status"] == "complete"
    assert first["quarter_partition_count"] == 2
    assert first["membership_month_end_count"] == 2
    # stock_st is opt-in because it needs a higher Tushare permission tier.
    assert first["datasets"] == ["fina_indicator_vip", "bak_basic"]
    assert first["completed_this_run"] == 4
    assert {endpoint for endpoint, _ in client.calls} == {
        "fina_indicator_vip",
        "bak_basic",
    }
    assert (layout.raw_root / "fundamentals-checkpoint.json").is_file()
    assert (layout.raw_root / "reference-snapshots-checkpoint.json").is_file()

    second_client = EnrichmentClient()
    second = sync_enrichment(
        "2024-01-01",
        "2024-02-29",
        config_path=config_path,
        layout=layout,
        client=second_client,
    )
    assert second["completed_before"] == 4
    assert second["completed_this_run"] == 0
    assert second_client.calls == []


def test_stock_st_is_explicit_and_empty_response_fails_closed(tmp_path: Path) -> None:
    config_path, _, layout = _config(tmp_path)
    client = EnrichmentClient(empty_st=True)

    try:
        sync_enrichment(
            "2024-01-01",
            "2024-01-31",
            config_path=config_path,
            layout=layout,
            client=client,
            datasets=("stock_st",),
        )
    except ValueError as exc:
        assert "returned no rows" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("empty explicit stock_st partition unexpectedly accepted")


def test_tiny_early_announcement_vendor_defect_is_quarantined(tmp_path: Path) -> None:
    config_path, _, layout = _config(tmp_path)

    class OneBadFinancialRow(EnrichmentClient):
        def query(self, endpoint: str, **kwargs) -> pd.DataFrame:
            frame = super().query(endpoint, **kwargs)
            if endpoint != "fina_indicator_vip":
                return frame
            good = pd.concat([frame] * 1_000, ignore_index=True)
            good["ts_code"] = [f"{index:06d}.SZ" for index in range(len(good))]
            bad = frame.iloc[[0]].copy()
            bad["ts_code"] = "999999.SZ"
            bad["ann_date"] = (
                pd.Timestamp(kwargs["period"]) - pd.Timedelta(days=1)
            ).strftime("%Y%m%d")
            return pd.concat([good, bad], ignore_index=True)

    result = sync_enrichment(
        "2024-01-01",
        "2024-01-31",
        config_path=config_path,
        layout=layout,
        client=OneBadFinancialRow(),
        datasets=("fina_indicator_vip",),
    )

    assert result["status"] == "complete"
    quarantine = next(
        layout.raw_root.glob(
            "fina_indicator_vip/period=*/part-000.quarantine.parquet"
        )
    )
    assert len(pd.read_parquet(quarantine)) == 1


def test_missing_bak_basic_day_uses_only_a_prior_snapshot(tmp_path: Path) -> None:
    config_path, _, layout = _config(tmp_path)

    class MissingExactDay(EnrichmentClient):
        def query(self, endpoint: str, **kwargs) -> pd.DataFrame:
            if endpoint == "bak_basic" and kwargs["trade_date"] == "20240131":
                self.calls.append((endpoint, kwargs))
                return pd.DataFrame(columns=ENRICHMENT_DATASET_FIELDS[endpoint].split(","))
            return super().query(endpoint, **kwargs)

    client = MissingExactDay()
    result = sync_enrichment(
        "2024-02-01",
        "2024-02-29",
        config_path=config_path,
        layout=layout,
        client=client,
        datasets=("bak_basic",),
    )

    assert result["status"] == "complete"
    path = layout.raw_root / "bak_basic/trade_date=2024-01-31/part-000.parquet"
    saved = pd.read_parquet(path)
    assert saved["trade_date"].astype(str).eq("20240131").all()
    assert saved["source_trade_date"].astype(str).eq("20240130").all()
    queried = [kwargs["trade_date"] for endpoint, kwargs in client.calls if endpoint == "bak_basic"]
    assert queried == ["20240131", "20240130"]
