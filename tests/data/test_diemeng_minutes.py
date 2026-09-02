from __future__ import annotations

import pandas as pd
import pytest

from factor_lab.data.diemeng_minutes import (
    DIEMENG_MINUTE_COLUMNS,
    DIEMENG_RAW_COLUMNS,
    DiemengMinuteDataError,
    DiemengMinuteHTTPClient,
    DiemengMinuteTransportError,
    audit_diemeng_execution_slice,
    audit_diemeng_full_day,
    audit_diemeng_one_minute_day,
    capture_diemeng_minutes,
    first_five_minute_audit_bar,
    freeze_diemeng_unit_contract,
    normalize_diemeng_minutes,
)


def _row(time: str, **updates):
    value = {
        "stock_code": "000001.SZ",
        "trade_time": time,
        "open": 10.0,
        "high": 10.2,
        "low": 9.9,
        "close": 10.1,
        "vol": 100.0,
        "amount": 100_500.0,
    }
    value.update(updates)
    return value


def _raw(*rows) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=DIEMENG_RAW_COLUMNS)


def test_normalizer_freezes_hands_to_shares_and_rmb() -> None:
    result = normalize_diemeng_minutes(
        _raw(
            _row(
                "2021-01-04 09:30:00",
                open=10.0,
                high=10.0,
                low=10.0,
                close=10.0,
                vol=0.0,
                amount=0.0,
            ),
            _row("2021-01-04 09:31:00", vol=123.45, amount=124_000.0),
        ),
        ticker="000001.SZ",
        level="1min",
        start_time="2021-01-04 09:30:00",
        end_time="2021-01-04 09:31:00",
    )
    assert tuple(result.columns) == DIEMENG_MINUTE_COLUMNS
    assert result.provider_volume.tolist() == [0.0, 123.45]
    assert result.volume_multiplier_to_shares.tolist() == [100.0, 100.0]
    assert result.volume_shares.tolist() == [0.0, 12_345.0]
    assert result.amount_rmb.tolist() == [0.0, 124_000.0]


def test_normalizer_identifies_provider_share_volume_per_slice() -> None:
    result = normalize_diemeng_minutes(
        _raw(
            _row(
                "2020-07-01 09:31:00",
                stock_code="300630.SZ",
                open=74.08,
                high=74.12,
                low=73.34,
                close=73.51,
                vol=56_000.0,
                amount=4_139_990.0,
            )
        ),
        ticker="300630.SZ",
        level="1min",
        start_time="2020-07-01 09:31:00",
        end_time="2020-07-01 09:31:00",
    )
    assert result.volume_multiplier_to_shares.tolist() == [1.0]
    assert result.volume_shares.tolist() == [56_000.0]


def test_normalizer_excludes_exact_0930_anchor_from_unit_inference() -> None:
    result = normalize_diemeng_minutes(
        _raw(
            _row(
                "2019-07-01 09:30:00",
                stock_code="002203.SZ",
                open=10.88,
                high=10.88,
                low=10.88,
                close=10.88,
                vol=100.0,
                amount=108_908.57,
            ),
            _row(
                "2019-07-01 09:31:00",
                stock_code="002203.SZ",
                open=10.88,
                high=10.89,
                low=10.87,
                close=10.88,
                vol=100.0,
                amount=108_800.0,
            ),
        ),
        ticker="002203.SZ",
        level="1min",
        start_time="2019-07-01 09:30:00",
        end_time="2019-07-01 09:31:00",
    )
    assert result.volume_multiplier_to_shares.tolist() == [100.0, 100.0]
    assert result.amount_multiplier_to_rmb.tolist() == [1.0, 1.0]
    anchor_vwap = result.iloc[0].amount_rmb / result.iloc[0].volume_shares
    assert anchor_vwap > result.iloc[0].high + 0.01


def test_normalizer_does_not_relax_continuous_bar_unit_validation() -> None:
    with pytest.raises(DiemengMinuteDataError, match="one unique"):
        normalize_diemeng_minutes(
            _raw(
                _row(
                    "2019-07-01 09:30:00",
                    stock_code="002203.SZ",
                    open=10.88,
                    high=10.88,
                    low=10.88,
                    close=10.88,
                    vol=100.0,
                    amount=108_800.0,
                ),
                _row(
                    "2019-07-01 09:31:00",
                    stock_code="002203.SZ",
                    open=10.88,
                    high=10.88,
                    low=10.88,
                    close=10.88,
                    vol=100.0,
                    amount=108_908.57,
                ),
            ),
            ticker="002203.SZ",
            level="1min",
            start_time="2019-07-01 09:30:00",
            end_time="2019-07-01 09:31:00",
        )


def test_normalizer_excludes_formal_decision_overlap_from_unit_validation() -> None:
    rows = []
    for minute in range(30, 48):
        amount = 108_694.869 if minute == 42 else 108_800.0
        rows.append(
            _row(
                f"2019-07-01 09:{minute:02d}:00",
                stock_code="600064.SH",
                open=10.88,
                high=10.89,
                low=10.88,
                close=10.88,
                vol=100.0,
                amount=amount,
            )
        )
    result = normalize_diemeng_minutes(
        _raw(*rows),
        ticker="600064.SH",
        level="1min",
        start_time="2019-07-01 09:30:00",
        end_time="2019-07-01 09:47:00",
    )
    assert result.volume_multiplier_to_shares.unique().tolist() == [100.0]
    assert result.amount_multiplier_to_rmb.unique().tolist() == [1.0]
    overlap = result.loc[result.trade_time.dt.minute.eq(42)].iloc[0]
    assert overlap.amount_rmb / overlap.volume_shares < overlap.low - 0.01


def test_normalizer_uses_consumed_window_aggregate_not_individual_vwap() -> None:
    rows = []
    for minute in range(30, 48):
        amount = 109_066.0 if minute == 32 else 108_800.0
        rows.append(
            _row(
                f"2019-10-08 09:{minute:02d}:00",
                stock_code="002714.SZ",
                open=10.88,
                high=10.89,
                low=10.87,
                close=10.88,
                vol=100.0,
                amount=amount,
            )
        )
    result = normalize_diemeng_minutes(
        _raw(*rows),
        ticker="002714.SZ",
        level="1min",
        start_time="2019-10-08 09:30:00",
        end_time="2019-10-08 09:47:00",
    )
    outlier = result.loc[result.trade_time.dt.minute.eq(32)].iloc[0]
    assert outlier.amount_rmb / outlier.volume_shares > outlier.high + 0.01
    window_a = result.loc[result.trade_time.dt.minute.between(31, 35)]
    aggregate_vwap = window_a.amount_rmb.sum() / window_a.volume_shares.sum()
    assert window_a.low.min() <= aggregate_vwap <= window_a.high.max()


def test_normalizer_keeps_formal_execution_window_aggregate_strict() -> None:
    rows = []
    for minute in range(30, 48):
        amount = 108_600.0 if 43 <= minute <= 47 else 108_800.0
        rows.append(
            _row(
                f"2019-07-01 09:{minute:02d}:00",
                stock_code="600064.SH",
                open=10.88,
                high=10.89,
                low=10.88,
                close=10.88,
                vol=100.0,
                amount=amount,
            )
        )
    with pytest.raises(DiemengMinuteDataError, match="one unique"):
        normalize_diemeng_minutes(
            _raw(*rows),
            ticker="600064.SH",
            level="1min",
            start_time="2019-07-01 09:30:00",
            end_time="2019-07-01 09:47:00",
        )


def test_normalizer_accepts_only_complete_present_execution_windows() -> None:
    rows = [
        _row(f"2021-01-04 09:{minute:02d}:00")
        for minute in range(43, 48)
    ]
    result = normalize_diemeng_minutes(
        _raw(*rows),
        ticker="000001.SZ",
        level="1min",
        start_time="2021-01-04 09:30:00",
        end_time="2021-01-04 09:47:00",
    )
    assert result.trade_time.dt.minute.tolist() == [43, 44, 45, 46, 47]
    assert result.volume_multiplier_to_shares.unique().tolist() == [100.0]

    with pytest.raises(DiemengMinuteDataError, match="window is partial"):
        normalize_diemeng_minutes(
            _raw(*rows[:-1]),
            ticker="000001.SZ",
            level="1min",
            start_time="2021-01-04 09:30:00",
            end_time="2021-01-04 09:47:00",
        )


def test_formal_context_without_execution_liquidity_has_null_units() -> None:
    result = normalize_diemeng_minutes(
        _raw(
            _row(
                "2021-01-04 09:30:00",
                open=10.0,
                high=10.0,
                low=10.0,
                close=10.0,
                vol=100.0,
                amount=100_000.0,
            )
        ),
        ticker="000001.SZ",
        level="1min",
        start_time="2021-01-04 09:30:00",
        end_time="2021-01-04 09:47:00",
    )
    assert result.volume_multiplier_to_shares.isna().all()
    assert result.amount_multiplier_to_rmb.isna().all()
    assert result.volume_shares.isna().all()
    assert result.amount_rmb.isna().all()


@pytest.mark.parametrize(
    ("frame", "message"),
    [
        (_raw(_row("2021-01-04 09:31:01")), "off-grid"),
        (_raw(_row("2021-01-04 09:31:00", stock_code="600000.SH")), "another ticker"),
        (_raw(_row("2021-01-04 09:31:00", high=9.8)), "OHLC"),
        (_raw(_row("2021-01-04 09:31:00", vol=0.0, amount=1.0)), "zero volume"),
        (
            _raw(
                _row("2021-01-04 09:31:00"),
                _row("2021-01-04 09:31:00"),
            ),
            "duplicate",
        ),
    ],
)
def test_normalizer_fails_closed_on_malformed_rows(frame, message) -> None:
    with pytest.raises(DiemengMinuteDataError, match=message):
        normalize_diemeng_minutes(
            frame,
            ticker="000001.SZ",
            level="1min",
            start_time="2021-01-04 09:30:00",
            end_time="2021-01-04 09:40:00",
        )


class _PagedClient:
    def __init__(self, rows, *, total=None):
        self.rows = rows
        self.total = len(rows) if total is None else total
        self.calls = []

    def query_history(self, **payload):
        self.calls.append(payload)
        start = payload["page"] * payload["page_size"]
        end = start + payload["page_size"]
        return {
            "code": 200,
            "data": {"total": self.total, "list": self.rows[start:end]},
        }


def test_capture_exhausts_pages_and_binds_payload() -> None:
    rows = [
        _row(f"2021-01-04 09:{minute:02d}:00")
        for minute in range(31, 36)
    ]
    client = _PagedClient(rows)
    result = capture_diemeng_minutes(
        client,
        ticker="000001.SZ",
        level="1min",
        start_time="2021-01-04 09:31:00",
        end_time="2021-01-04 09:35:00",
        page_size=2,
    )
    assert [call["page"] for call in client.calls] == [0, 1, 2, 3]
    assert len(result.frame) == 5
    assert result.receipt["page_count"] == 3
    assert result.receipt["request_count"] == 4
    assert result.receipt["sentinel_empty_page_verified"] is True
    assert result.receipt["provider_volume_unit"] == "hands"
    assert result.receipt["volume_multiplier_to_shares"] == 100.0
    assert result.receipt["canonical_volume_unit"] == "shares"
    assert len(result.receipt["payload_sha256"]) == 64


def test_capture_rejects_short_pagination() -> None:
    client = _PagedClient([_row("2021-01-04 09:31:00")], total=2)
    with pytest.raises(DiemengMinuteDataError, match="before declared total"):
        capture_diemeng_minutes(
            client,
            ticker="000001.SZ",
            level="1min",
            start_time="2021-01-04 09:31:00",
            end_time="2021-01-04 09:35:00",
            page_size=1,
        )


def test_capture_nonempty_zero_liquidity_slice_uses_null_unit_receipt() -> None:
    row = _row(
        "2021-01-04 09:31:00",
        open=10.0,
        high=10.0,
        low=10.0,
        close=10.0,
        vol=0.0,
        amount=0.0,
    )
    result = capture_diemeng_minutes(
        _PagedClient([row]),
        ticker="000001.SZ",
        level="1min",
        start_time="2021-01-04 09:31:00",
        end_time="2021-01-04 09:31:00",
    )
    assert result.receipt["volume_multiplier_to_shares"] is None
    assert result.receipt["amount_multiplier_to_rmb"] is None
    assert len(result.receipt["payload_sha256"]) == 64


def test_first_five_minute_bar_produces_bounded_vwap() -> None:
    frame = normalize_diemeng_minutes(
        _raw(
            _row(
                "2021-01-04 09:35:00",
                open=10.0,
                high=10.2,
                low=9.8,
                close=10.1,
                vol=1_000.0,
                amount=1_005_000.0,
            )
        ),
        ticker="000001.SZ",
        level="5min",
        start_time="2021-01-04 09:35:00",
        end_time="2021-01-04 09:35:00",
    )
    execution = first_five_minute_audit_bar(
        frame, ticker="000001.SZ", session="2021-01-04", daily_open=10.0
    )
    assert execution["vwap"] == pytest.approx(10.05)
    assert execution["volume_shares"] == 100_000.0
    with pytest.raises(DiemengMinuteDataError, match="differs"):
        first_five_minute_audit_bar(
            frame,
            ticker="000001.SZ",
            session="2021-01-04",
            daily_open=10.02,
        )


def test_full_day_unit_contract_requires_cross_exchange_and_four_years() -> None:
    bars = pd.date_range("2021-01-04 09:30:00", "2021-01-04 09:47:00", freq="1min")
    raw = _raw(
        *[
            _row(value.strftime("%Y-%m-%d %H:%M:%S"), amount=100_500.0)
            for value in bars
        ]
    )
    frame = normalize_diemeng_minutes(
        raw,
        ticker="000001.SZ",
        level="1min",
        start_time="2021-01-04 09:30:00",
        end_time="2021-01-04 09:47:00",
    )
    audit = audit_diemeng_execution_slice(
        frame,
        ticker="000001.SZ",
        session="2021-01-04",
        daily_open=10.0,
    )
    audits = []
    for year in (2017, 2019, 2021, 2022):
        for suffix in ("SH", "SZ"):
            value = dict(audit)
            value.update(
                {
                    "ticker": f"600000.{suffix}" if suffix == "SH" else f"000001.{suffix}",
                    "session": f"{year}-01-04",
                    "exchange": suffix,
                    "year": year,
                    "volume_multiplier_to_shares": (
                        1.0 if suffix == "SH" else 100.0
                    ),
                    "first_trade_time": f"{year}-01-04 09:30:00",
                    "last_trade_time": f"{year}-01-04 09:47:00",
                }
            )
            audits.append(value)
    contract = freeze_diemeng_unit_contract(audits)
    assert contract["sample_count"] == 8
    assert contract["allowed_volume_multipliers_to_shares"] == [1.0, 100.0]
    assert contract["allowed_amount_multipliers_to_rmb"] == [1.0]
    assert (
        contract["auction_anchor_excluded_from_unit_inference_and_validation"]
        is True
    )
    assert contract[
        "decision_overlap_clocks_excluded_from_unit_inference_and_validation"
    ] == ["09:36:00", "09:42:00"]
    assert len(contract["unit_inference_and_validation_clocks"]) == 15
    assert set(contract["unit_inference_and_validation_windows"]) == {
        "A",
        "B",
        "C",
    }
    assert contract["unit_validation_granularity"] == (
        "consumed_five_minute_window_aggregate"
    )
    assert contract["aggregate_vwap_ohlc_tolerance_rmb"] == 0.01
    assert len(contract["payload_sha256"]) == 64


class _Response:
    def __init__(self, status_code, value):
        self.status_code = status_code
        self._value = value

    def json(self):
        return self._value


def test_http_client_retries_transport_but_not_permission() -> None:
    calls = []

    def post(*args, **kwargs):
        calls.append((args, kwargs))
        if len(calls) == 1:
            return _Response(503, {})
        return _Response(200, {"code": 200, "data": {"total": 0, "list": []}})

    clock = iter([0.0, 0.0, 2.0, 2.0])
    sleeps = []
    client = DiemengMinuteHTTPClient(
        "secret",
        request_rate_per_minute=60.0,
        post_fn=post,
        monotonic_fn=lambda: next(clock),
        sleep_fn=sleeps.append,
    )
    payload = {
        "stock_code": "000001.SZ",
        "level": "5min",
        "start_time": "2021-01-04 00:00:00",
        "end_time": "2021-01-04 23:59:59",
        "page": 0,
        "page_size": 10,
    }
    assert client.query_history(**payload)["code"] == 200
    assert len(calls) == 2
    assert "secret" not in repr(calls[0][0])
    assert calls[0][1]["allow_redirects"] is False
    assert sleeps

    denied = DiemengMinuteHTTPClient(
        "secret",
        post_fn=lambda *args, **kwargs: _Response(403, {}),
        monotonic_fn=lambda: 0.0,
        sleep_fn=lambda value: None,
    )
    with pytest.raises(PermissionError) as denied_error:
        denied.query_history(**payload)
    assert "secret" not in str(denied_error.value)
