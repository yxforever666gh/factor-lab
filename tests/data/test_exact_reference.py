from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import pytest

from factor_lab.data import RuntimeLayout, sync_exact_reference
from factor_lab.data.catalog import sha256_file
from factor_lab.data.sources import (
    DATASET_FIELDS,
    ENRICHMENT_DATASET_FIELDS,
    EXACT_REFERENCE_CONTRACT_ID,
)


TRADE_DATE = "2024-01-02"
COMPACT_TRADE_DATE = "20240102"
TICKERS = ("000001.SZ", "600000.SH")


class ReferenceClient:
    def __init__(self, responses: list[pd.DataFrame]) -> None:
        self.responses = [frame.copy() for frame in responses]
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def query(self, endpoint: str, **kwargs: Any) -> pd.DataFrame:
        self.calls.append((endpoint, dict(kwargs)))
        if not self.responses:
            raise AssertionError("unexpected provider request")
        return self.responses.pop(0).copy()


class NoRequestClient:
    def query(self, endpoint: str, **kwargs: Any) -> pd.DataFrame:
        raise AssertionError(f"resume unexpectedly requested {endpoint}: {kwargs}")


def _config(tmp_path: Path) -> tuple[Path, RuntimeLayout]:
    payload = {
        "schema_version": 2,
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
            "checkpoint_file": "checkpoint.json",
            "request_rate_per_minute": 0,
            "verify_hashes_on_resume": False,
        },
        "enrichment": {
            "checkpoint_file": "enrichment-checkpoint.json",
            "request_rate_per_minute": 0,
        },
    }
    config_path = tmp_path / "configs/data.json"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    layout.ensure_directories()
    return config_path, layout


def _daily_frame(tickers: tuple[str, ...] = TICKERS) -> pd.DataFrame:
    fields = DATASET_FIELDS["daily"].split(",")
    rows: list[dict[str, Any]] = []
    for index, ticker in enumerate(tickers, start=1):
        row = {field: float(index) for field in fields}
        row["ts_code"] = ticker
        row["trade_date"] = COMPACT_TRADE_DATE
        rows.append(row)
    return pd.DataFrame(rows, columns=fields)


def _write_daily_partition(
    layout: RuntimeLayout, tickers: tuple[str, ...] = TICKERS
) -> dict[str, Any]:
    path = (
        layout.raw_root
        / "daily"
        / f"trade_date={TRADE_DATE}"
        / "part-000.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = _daily_frame(tickers)
    frame.to_parquet(path, index=False)
    entry = {
        "status": "complete",
        "dataset": "daily",
        "trade_date": TRADE_DATE,
        "path": str(path),
        "row_count": len(frame),
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "completed_at_utc": "2024-01-02T08:00:00Z",
    }
    layout.checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "partitions": {f"daily/{TRADE_DATE}": entry},
            }
        ),
        encoding="utf-8",
    )
    return entry


def _reference_frame(
    tickers: tuple[str, ...] = TICKERS,
    *,
    trade_date: str = COMPACT_TRADE_DATE,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trade_date": [trade_date] * len(tickers),
            "ts_code": list(tickers),
            "name": [f"name-{ticker}" for ticker in tickers],
            "industry": ["test"] * len(tickers),
            "list_date": ["20100101"] * len(tickers),
        },
        columns=ENRICHMENT_DATASET_FIELDS["bak_basic"].split(","),
    )


def _reference_paths(layout: RuntimeLayout) -> tuple[Path, Path]:
    checkpoint = layout.raw_root / "enrichment-checkpoint.json"
    partition = (
        layout.raw_root
        / "bak_basic"
        / f"trade_date={TRADE_DATE}"
        / "part-000.parquet"
    )
    return checkpoint, partition


def test_exact_reference_requires_two_equal_samples_and_publishes_checkpoint(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    daily_entry = _write_daily_partition(layout)
    first = _reference_frame(tuple(reversed(TICKERS)))
    second = _reference_frame(TICKERS)
    client = ReferenceClient([first, second])

    result = sync_exact_reference(
        TRADE_DATE,
        config_path=config_path,
        layout=layout,
        client=client,
    )

    assert result == {
        "schema_version": 1,
        "status": "complete",
        "source": "tushare",
        "dataset": "bak_basic",
        "trade_date": TRADE_DATE,
        "exact_source_required": True,
        "stability_sample_count": 2,
        "completed_before": 0,
        "completed_this_run": 1,
        "checkpoint_path": str(layout.raw_root / "enrichment-checkpoint.json"),
        "partition_path": str(
            layout.raw_root
            / "bak_basic"
            / f"trade_date={TRADE_DATE}"
            / "part-000.parquet"
        ),
        "daily_ticker_count": 2,
        "reference_ticker_count": 2,
        "covered_ticker_count": 2,
    }
    assert client.calls == [
        (
            "bak_basic",
            {
                "trade_date": COMPACT_TRADE_DATE,
                "fields": ENRICHMENT_DATASET_FIELDS["bak_basic"],
            },
        ),
        (
            "bak_basic",
            {
                "trade_date": COMPACT_TRADE_DATE,
                "fields": ENRICHMENT_DATASET_FIELDS["bak_basic"],
            },
        ),
    ]

    checkpoint_path, partition_path = _reference_paths(layout)
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    entry = checkpoint["partitions"][f"bak_basic/trade_date={TRADE_DATE}"]
    assert entry["capture_contract_id"] == EXACT_REFERENCE_CONTRACT_ID
    assert entry["capture_mode"] == "exact_only"
    assert entry["fallback_used"] is False
    assert entry["request_trade_date"] == TRADE_DATE
    assert entry["source_trade_date"] == TRADE_DATE
    assert entry["stability_sample_count"] == 2
    assert entry["daily_partition_sha256"] == daily_entry["sha256"]
    assert entry["sha256"] == sha256_file(partition_path)
    saved = pd.read_parquet(partition_path)
    assert saved["ts_code"].tolist() == sorted(TICKERS)
    assert saved["trade_date"].astype(str).eq(COMPACT_TRADE_DATE).all()
    assert saved["source_trade_date"].astype(str).eq(COMPACT_TRADE_DATE).all()


def test_exact_reference_resume_performs_no_request_or_partition_checkpoint_write(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    _write_daily_partition(layout)
    sync_exact_reference(
        TRADE_DATE,
        config_path=config_path,
        layout=layout,
        client=ReferenceClient([_reference_frame(), _reference_frame()]),
    )
    checkpoint_path, partition_path = _reference_paths(layout)
    before = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in (checkpoint_path, partition_path)
    }

    result = sync_exact_reference(
        TRADE_DATE,
        config_path=config_path,
        layout=layout,
        client=NoRequestClient(),
    )

    assert result["status"] == "complete"
    assert result["completed_before"] == 1
    assert result["completed_this_run"] == 0
    assert {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in (checkpoint_path, partition_path)
    } == before


def test_exact_reference_empty_response_waits_without_publishing(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    _write_daily_partition(layout)
    client = ReferenceClient(
        [pd.DataFrame(columns=ENRICHMENT_DATASET_FIELDS["bak_basic"].split(","))]
    )

    result = sync_exact_reference(
        TRADE_DATE,
        config_path=config_path,
        layout=layout,
        client=client,
    )

    assert result["status"] == "waiting"
    assert result["reason"] == "provider_empty"
    assert len(client.calls) == 1
    checkpoint_path, partition_path = _reference_paths(layout)
    assert not checkpoint_path.exists()
    assert not partition_path.exists()


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda frame: frame.assign(trade_date="20240101"),
            "mismatched trade_date",
        ),
        (
            lambda frame: frame.assign(ts_code=[TICKERS[0], TICKERS[0]]),
            "duplicate securities",
        ),
        (
            lambda frame: frame.assign(ts_code=[TICKERS[0], ""]),
            "blank securities",
        ),
    ],
    ids=("prior-date", "duplicate", "blank"),
)
def test_exact_reference_rejects_non_exact_or_malformed_provider_rows(
    tmp_path: Path,
    mutate: Callable[[pd.DataFrame], pd.DataFrame],
    message: str,
) -> None:
    config_path, layout = _config(tmp_path)
    _write_daily_partition(layout)
    client = ReferenceClient([mutate(_reference_frame())])

    with pytest.raises(ValueError, match=message):
        sync_exact_reference(
            TRADE_DATE,
            config_path=config_path,
            layout=layout,
            client=client,
        )

    assert len(client.calls) == 1
    checkpoint_path, partition_path = _reference_paths(layout)
    assert not checkpoint_path.exists()
    assert not partition_path.exists()


def test_exact_reference_incomplete_daily_universe_waits_without_publishing(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    _write_daily_partition(layout)
    client = ReferenceClient([_reference_frame((TICKERS[0],))])

    result = sync_exact_reference(
        TRADE_DATE,
        config_path=config_path,
        layout=layout,
        client=client,
    )

    assert result["status"] == "waiting"
    assert result["reason"] == "provider_universe_incomplete"
    assert result["missing_tickers"] == [TICKERS[1]]
    assert len(client.calls) == 1
    checkpoint_path, partition_path = _reference_paths(layout)
    assert not checkpoint_path.exists()
    assert not partition_path.exists()


def test_exact_reference_resume_rejects_a_prior_day_fallback_checkpoint(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    daily_entry = _write_daily_partition(layout)
    checkpoint_path, partition_path = _reference_paths(layout)
    partition_path.parent.mkdir(parents=True, exist_ok=True)
    fallback = _reference_frame()
    fallback["source_trade_date"] = "20240101"
    fallback.to_parquet(partition_path, index=False)
    entry = {
        "status": "complete",
        "dataset": "bak_basic",
        "trade_date": TRADE_DATE,
        "request_trade_date": TRADE_DATE,
        "source_trade_date": "2024-01-01",
        "capture_contract_id": EXACT_REFERENCE_CONTRACT_ID,
        "capture_mode": "exact_only",
        "fallback_used": False,
        "fields": ENRICHMENT_DATASET_FIELDS["bak_basic"],
        "path": str(partition_path),
        "row_count": len(fallback),
        "size_bytes": partition_path.stat().st_size,
        "sha256": sha256_file(partition_path),
        "completed_at_utc": "2024-01-02T08:00:00Z",
        "exact_source_required": True,
        "stability_sample_count": 2,
        "daily_partition_sha256": daily_entry["sha256"],
        "daily_ticker_count": len(TICKERS),
        "covered_ticker_count": len(TICKERS),
        "reference_ticker_count": len(TICKERS),
    }
    checkpoint_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "partitions": {f"bak_basic/trade_date={TRADE_DATE}": entry},
            }
        ),
        encoding="utf-8",
    )
    before = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in (checkpoint_path, partition_path)
    }

    with pytest.raises(
        ValueError, match="exact reference checkpoint identity or bytes are invalid"
    ):
        sync_exact_reference(
            TRADE_DATE,
            config_path=config_path,
            layout=layout,
            client=NoRequestClient(),
        )

    assert {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in (checkpoint_path, partition_path)
    } == before
