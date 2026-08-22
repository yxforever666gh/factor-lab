from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
import pytest

from factor_lab.research_os.data_sources import SourceContractError
from factor_lab.research_os.execution_open_sources import (
    TUSHARE_RT_MIN,
    TUSHARE_RT_MIN_DAILY,
    TushareRealtimeOpenAdapter,
)


SESSION = date(2026, 8, 24)
RECEIVED = datetime(2026, 8, 24, 1, 31, tzinfo=timezone.utc)
TICKERS = ("000001.SZ", "000002.SZ", "600000.SH")


def _rows(*, tickers=TICKERS, event="2026-08-24 09:30:00") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": ticker,
                "time": event,
                "open": 10.0 + index,
                "close": 10.0 + index,
                "high": 10.1 + index,
                "low": 9.9 + index,
                "vol": 1000.0 + index,
                "amount": 10_000.0 + index,
            }
            for index, ticker in enumerate(tickers)
        ]
    )


class _Client:
    def __init__(self, frame: pd.DataFrame | Exception) -> None:
        self.frame = frame
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def query(self, endpoint: str, **parameters: Any):
        self.calls.append((endpoint, dict(parameters)))
        if isinstance(self.frame, Exception):
            raise self.frame
        return self.frame.copy(deep=True)


def _adapter(client: _Client, *, endpoint: str = TUSHARE_RT_MIN, at=RECEIVED):
    return TushareRealtimeOpenAdapter(
        client,
        endpoint=endpoint,
        receive_clock=lambda: at,
        max_universe_size=len(TICKERS),
    )


def test_rt_min_live_fetch_is_the_permission_probe_and_uses_ingestion_time() -> None:
    client = _Client(_rows())
    result = _adapter(client).fetch_open_batch(TICKERS, SESSION)

    assert client.calls == [
        ("rt_min", {"ts_code": ",".join(TICKERS), "freq": "1MIN"})
    ]
    assert result.received_at == RECEIVED
    assert result.batch.ingested_at == RECEIVED
    assert result.batch.lineage["permission_probe"] == "session_bound_successful_fetch"
    assert set(result.batch.frame["stock_code"]) == set(TICKERS)
    assert set(pd.to_datetime(result.batch.frame["trade_time"], utc=True)) == {
        pd.Timestamp("2026-08-24T01:30:00Z")
    }
    assert result.doc_id == 374
    # Injected clients/clocks can exercise semantics but never self-attest.
    assert _adapter(client).production_attested is False


def test_https_sdk_client_is_attested_only_after_direct_transport_binding() -> None:
    import requests
    import tushare as ts

    client = ts.pro_api("private-token-must-not-leak")
    client._DataApi__http_url = "https://api.waditu.com/dataapi"
    adapter = TushareRealtimeOpenAdapter(client)
    session = getattr(client, "_factor_lab_direct_http_session")

    assert adapter.production_attested is True
    assert type(session) is requests.Session
    assert session.trust_env is False
    assert session.proxies == {}


def test_permission_failure_is_fail_closed_and_vendor_message_is_not_repeated() -> None:
    adapter = _adapter(_Client(RuntimeError("token=do-not-persist; no permission")))

    with pytest.raises(SourceContractError, match="unavailable or unauthorized") as caught:
        adapter.fetch_open_batch(TICKERS, SESSION)

    assert "do-not-persist" not in str(caught.value)


@pytest.mark.parametrize(
    ("at", "event", "message"),
    [
        (
            datetime(2026, 8, 24, 1, 36, tzinfo=timezone.utc),
            "2026-08-24 09:30:00",
            "live opening window",
        ),
        (
            RECEIVED,
            "2026-08-23 09:30:00",
            "current-session 09:30",
        ),
        (
            RECEIVED,
            "09:30:00",
            "explicit calendar date",
        ),
    ],
)
def test_late_cross_day_and_date_less_provider_times_are_rejected(
    at: datetime, event: str, message: str
) -> None:
    adapter = _adapter(_Client(_rows(event=event)), at=at)

    with pytest.raises(SourceContractError, match=message):
        adapter.fetch_open_batch(TICKERS, SESSION)


def test_missing_and_unrequested_stock_sets_cannot_be_silently_filled() -> None:
    missing = _adapter(_Client(_rows(tickers=TICKERS[:-1]))).fetch_open_batch(
        TICKERS, SESSION
    )
    assert set(missing.batch.frame["stock_code"]) == set(TICKERS[:-1])
    assert missing.batch.lineage["coverage_status"] == "provisional_missing"
    assert missing.batch.lineage["missing_ticker_count"] == 1
    assert len(missing.batch.lineage["missing_ticker_hashes"]) == 1
    assert missing.batch.lineage["request_batches"][0]["missing_ticker_count"] == 1

    unexpected = _rows(tickers=(*TICKERS, "300001.SZ"))
    with pytest.raises(SourceContractError, match="unrequested tickers"):
        _adapter(_Client(unexpected)).fetch_open_batch(TICKERS, SESSION)


def test_extra_fields_are_rejected_even_when_they_look_benign() -> None:
    frame = _rows().assign(server_hint="09:30")

    with pytest.raises(SourceContractError, match="schema must match exactly"):
        _adapter(_Client(frame)).fetch_open_batch(TICKERS, SESSION)


def test_rt_min_daily_filters_current_day_history_but_requires_exact_contract() -> None:
    frame = pd.concat(
        [
            _rows(tickers=(TICKERS[0],), event="2026-08-24 09:30:00"),
            _rows(tickers=(TICKERS[0],), event="2026-08-24 09:31:00"),
        ],
        ignore_index=True,
    ).assign(freq="1MIN")
    frame = frame[
        ["ts_code", "freq", "time", "open", "close", "high", "low", "vol", "amount"]
    ]
    result = _adapter(
        _Client(frame), endpoint=TUSHARE_RT_MIN_DAILY
    ).fetch_open_batch((TICKERS[0],), SESSION)

    assert len(result.batch.frame) == 1
    assert result.doc_id == 457


class _ChunkClient:
    def __init__(
        self,
        *,
        fail_on_call: int | None = None,
        duplicate_first_ticker_on_second_call: bool = False,
    ) -> None:
        self.fail_on_call = fail_on_call
        self.duplicate_first_ticker_on_second_call = (
            duplicate_first_ticker_on_second_call
        )
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.first_ticker: str | None = None

    def query(self, endpoint: str, **parameters: Any) -> pd.DataFrame:
        self.calls.append((endpoint, dict(parameters)))
        if self.fail_on_call == len(self.calls):
            raise PermissionError("secret vendor failure")
        requested = str(parameters["ts_code"]).split(",")
        if self.first_ticker is None:
            self.first_ticker = requested[0]
        if (
            len(self.calls) == 2
            and self.duplicate_first_ticker_on_second_call
            and self.first_ticker is not None
        ):
            requested.append(self.first_ticker)
        return _rows(tickers=tuple(requested))


def _large_universe(size: int = 500) -> tuple[str, ...]:
    return tuple(f"{index:06d}.SZ" for index in range(1, size + 1))


def test_rt_min_chunks_500_symbols_into_two_hashed_requests() -> None:
    tickers = _large_universe()
    client = _ChunkClient()
    receipts = iter(
        (
            datetime(2026, 8, 24, 1, 31, tzinfo=timezone.utc),
            datetime(2026, 8, 24, 1, 32, tzinfo=timezone.utc),
        )
    )
    result = TushareRealtimeOpenAdapter(
        client,
        receive_clock=lambda: next(receipts),
        max_universe_size=500,
    ).fetch_open_batch(tickers, SESSION)

    assert [len(call[1]["ts_code"].split(",")) for call in client.calls] == [300, 200]
    assert result.received_at == datetime(
        2026, 8, 24, 1, 32, tzinfo=timezone.utc
    )
    assert len(result.batch.frame) == 500
    assert result.batch.lineage["request_batch_count"] == 2
    hashes = result.batch.lineage["request_hashes"]
    assert len(hashes) == 2
    assert len(set(hashes)) == 2
    assert all(len(item) == 64 for item in hashes)
    assert [
        item["ticker_count"] for item in result.batch.lineage["request_batches"]
    ] == [300, 200]


def test_rt_min_second_chunk_failure_and_late_receipt_fail_closed() -> None:
    tickers = _large_universe()
    failing = TushareRealtimeOpenAdapter(
        _ChunkClient(fail_on_call=2),
        receive_clock=lambda: RECEIVED,
        max_universe_size=500,
    )
    with pytest.raises(SourceContractError, match="unavailable or unauthorized"):
        failing.fetch_open_batch(tickers, SESSION)

    receipts = iter(
        (
            RECEIVED,
            datetime(2026, 8, 24, 1, 36, tzinfo=timezone.utc),
        )
    )
    late = TushareRealtimeOpenAdapter(
        _ChunkClient(),
        receive_clock=lambda: next(receipts),
        max_universe_size=500,
    )
    with pytest.raises(SourceContractError, match="live opening window"):
        late.fetch_open_batch(tickers, SESSION)


def test_rt_min_cross_chunk_duplicate_and_capacity_override_fail_closed() -> None:
    tickers = _large_universe()
    duplicated = TushareRealtimeOpenAdapter(
        _ChunkClient(duplicate_first_ticker_on_second_call=True),
        receive_clock=lambda: RECEIVED,
        max_universe_size=500,
    )
    with pytest.raises(SourceContractError, match="cross-batch duplicate"):
        duplicated.fetch_open_batch(tickers, SESSION)

    with pytest.raises(ValueError, match="between 1 and 300"):
        TushareRealtimeOpenAdapter(
            _ChunkClient(),
            max_symbols_per_request=301,
        )
