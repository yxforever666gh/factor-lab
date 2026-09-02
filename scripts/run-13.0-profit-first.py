#!/usr/bin/env python
"""Capture stable corporate actions for the 13.0 real-share closure."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
from math import isfinite
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid
from typing import Any, Callable, Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
while str(SRC) in sys.path:
    sys.path.remove(str(SRC))
sys.path.insert(0, str(SRC))

from factor_lab.data.corporate_actions import (  # noqa: E402
    CNINFO_RESPONSE_COLUMNS,
    DIVIDEND_FIELDS,
    TUSHARE_REFERENCE_DIAGNOSTIC_COLUMNS,
    canonical_cninfo_actions,
    canonical_implemented_actions,
    normalize_cninfo_dividend_response,
    normalize_dividend_response,
    project_akshare_cninfo_response,
    resolve_corporate_actions,
)
from factor_lab.data.etf_live import RateLimitedRetryingClient  # noqa: E402
from factor_lab.data.pit_stock import PITStockRawStore  # noqa: E402
from factor_lab.data.sources import TushareClient  # noqa: E402
from factor_lab.release_integrity import (  # noqa: E402
    canonical_payload_sha256,
    file_sha256,
)


RELEASE = "13.0"
PROTOCOL_PATH = Path("protocols/13.0-profit-first-real-share-closure.json")
PROTOCOL_ID = "factor-lab/13.0/profit-first-real-share-closure-v1"
PROTOCOL_PAYLOAD_SHA256 = "7989478d24cc4597a7066eab5a737e698d3647d5a70704f85854a744ec3fa9d8"
PROTOCOL_FILE_SHA256 = "9973cbdeb0e2d421e227afe7d22ffa9074e0f847041a8025b294cdb5f642fef2"
PANEL_PATH = ROOT / "runtime" / "data" / "pit-stock-12.0" / "development" / "quarterly-snapshots.parquet"
TARGETS_PATH = ROOT / "runtime" / "data" / "pit-stock-12.0" / "development" / "targets.parquet"
PANEL_FILE_SHA256 = "d51cad0de60484292ca24e4909d2c7617e0d83d3b8df58b29358f438eb5a48ca"
TARGETS_FILE_SHA256 = "ce3cd6c37dd1b04e77c170055e1bb7351cd99b6e2a201e7745ac1cf29a9ea06c"
ALL_SCOPE_COUNT = 2252
ALL_SCOPE_PAYLOAD_SHA256 = "457c0602df6038736b97bc9535348de20a9d3d32799023a1ec24bca42710befb"
START_DATE = pd.Timestamp("2017-10-09")
END_DATE = pd.Timestamp("2023-01-03")
CNINFO_REQUEST_RATE_PER_MINUTE = 240.0
TUSHARE_REQUEST_RATE_PER_MINUTE = 300.0
TUSHARE_EX_DATE_ROW_LIMIT = 5000
REFERENCE_THEORETICAL_RELATIVE_ERROR_AT_MOST = 0.01
REFERENCE_FACTOR_RATIO_ERROR_AT_MOST = 0.0011
REFERENCE_FACTOR_JUMP_EPSILON = 1e-12
DEFAULT_OUTPUT = ROOT / "runtime" / "data" / "pit-stock-13.0" / "corporate-actions"
ACTION_ARTIFACT_ROLES = (
    "scope",
    "cninfo_first",
    "cninfo_second",
    "tushare_first",
    "tushare_second",
    "cninfo_actions",
    "tushare_actions",
    "tushare_reference_diagnostics",
    "resolved_actions",
)
REANCHOR_SOURCE_IDENTITIES = (
    {
        "label": "candidate-actions-v2",
        "manifest_payload_sha256": "fae90bcdfe847ac140f8de52a839631ea9add4dfef69dbe1cf1da161ae132593",
        "manifest_file_sha256": "c6e3c076ac24f218f615ce0e6ef3b57649a856ed615c5f79ed9fcec0e56258fa",
        "protocol_payload_sha256": "633de5a27f59c4362ed085ee734ee07d25bab7541686b455092b45be031cdbe0",
        "protocol_file_sha256": "d9857577a5043cbfea4e381191ad5751fc1be86b7a1786db8f6419edba6085bf",
    },
    {
        "label": "candidate-actions-v3",
        "manifest_payload_sha256": "eabf3b156071a7670571a192810c67b3321632864c2829d984654314113d6eed",
        "manifest_file_sha256": "4e25c8d09a2438fef84f0a1d39dfa35a8abc8d4becff56d5c29081dc9a4adc48",
        "protocol_payload_sha256": "5e6f5bdac43b3ee261d8e80081315e09287f3b19c4caacd3577a79e4ff7f9225",
        "protocol_file_sha256": "b392753c6185e781ed4a52b74fb76a6d9ecfe60c15b76911e698806668bfbac1",
    },
)


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=ROOT, check=True, stdout=subprocess.PIPE
    ).stdout.decode("utf-8", errors="strict").strip()


def _canonical_sha256(value: Any) -> str:
    unsigned = dict(value) if isinstance(value, dict) else value
    if isinstance(unsigned, dict):
        unsigned.pop("payload_sha256", None)
    encoded = json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(
            json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False)
            + "\n"
        )
        handle.flush()
        os.fsync(handle.fileno())


def _fsync_file(path: Path) -> None:
    with path.open("r+b") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        if os.name == "nt":
            return
        raise
    try:
        try:
            os.fsync(descriptor)
        except OSError:
            if os.name != "nt":
                raise
    finally:
        os.close(descriptor)


def _read_protocol() -> dict[str, Any]:
    path = ROOT / PROTOCOL_PATH
    value = json.loads(path.read_text(encoding="utf-8"))
    if (
        value.get("protocol_id") != PROTOCOL_ID
        or value.get("release") != RELEASE
        or value.get("payload_sha256") != PROTOCOL_PAYLOAD_SHA256
        or canonical_payload_sha256(value) != PROTOCOL_PAYLOAD_SHA256
        or file_sha256(path) != PROTOCOL_FILE_SHA256
    ):
        raise ValueError("13.0 protocol identity/hash differs")
    return value


def _scope(scope: str) -> tuple[str, ...]:
    if file_sha256(PANEL_PATH) != PANEL_FILE_SHA256:
        raise ValueError("12.0 development panel bytes differ")
    if file_sha256(TARGETS_PATH) != TARGETS_FILE_SHA256:
        raise ValueError("12.0 development target bytes differ")
    panel = pd.read_parquet(PANEL_PATH)
    targets = pd.read_parquet(TARGETS_PATH)
    candidate = set(targets["ticker"].astype(str))
    benchmark: set[str] = set()
    panel["signal_date"] = pd.to_datetime(panel["signal_date"]).dt.normalize()
    for _, frame in panel.loc[panel["universe_member"]].groupby(
        "signal_date", sort=True
    ):
        selected = frame.assign(_ticker=frame["ticker"].astype(str)).sort_values(
            ["adv20", "_ticker"],
            ascending=[False, True],
            kind="mergesort",
        ).head(500)
        if len(selected) != 500:
            raise ValueError("ADV500 target scope is incomplete")
        benchmark.update(selected["ticker"].astype(str))
    values = tuple(sorted(candidate if scope == "candidate" else candidate | benchmark))
    if scope == "all" and (
        len(values) != ALL_SCOPE_COUNT
        or _canonical_sha256(list(values)) != ALL_SCOPE_PAYLOAD_SHA256
    ):
        raise ValueError("13.0 all-ticker scope differs")
    return values


def _temporary_provider_error(error: Exception) -> bool:
    text = str(error).casefold()
    return isinstance(error, (TimeoutError, ConnectionError)) or any(
        marker in text
        for marker in (
            "timeout",
            "timed out",
            "connection",
            "reset",
            "temporarily",
            "too many",
            "频率",
            "频繁",
        )
    )


def _capture_cninfo_sample(
    tickers: Iterable[str],
    provider: Callable[..., pd.DataFrame],
    *,
    sleep_fn: Callable[[float], None] = time.sleep,
    monotonic_fn: Callable[[], float] = time.monotonic,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    values = tuple(tickers)
    frames: list[pd.DataFrame] = []
    empty_tickers: list[str] = []
    nonempty_tickers: list[str] = []
    previous_attempt: float | None = None
    minimum_interval = 60.0 / CNINFO_REQUEST_RATE_PER_MINUTE
    for index, ticker in enumerate(values, start=1):
        response: pd.DataFrame | None = None
        for attempt in range(3):
            if previous_attempt is not None:
                remaining = minimum_interval - (
                    float(monotonic_fn()) - previous_attempt
                )
                if remaining > 0:
                    sleep_fn(remaining)
            previous_attempt = float(monotonic_fn())
            try:
                response = provider(symbol=ticker.split(".", 1)[0])
            except Exception as exc:
                if attempt == 2 or not _temporary_provider_error(exc):
                    raise
                sleep_fn((1.0, 2.0)[attempt])
                continue
            break
        assert response is not None
        projected = project_akshare_cninfo_response(response, ticker=ticker)
        if projected.empty:
            empty_tickers.append(str(ticker))
        else:
            nonempty_tickers.append(str(ticker))
            frames.append(projected)
        if index % 250 == 0 or index == len(values):
            print(f"CNInfo sample fetched {index}/{len(values)}", flush=True)
    result = (
        normalize_cninfo_dividend_response(
            pd.concat(frames, ignore_index=True), expected_tickers=values
        )
        if frames
        else pd.DataFrame(columns=CNINFO_RESPONSE_COLUMNS)
    )
    result = result.loc[
        result["ex_date"].notna()
        & result["ex_date"].between(START_DATE, END_DATE)
    ].reset_index(drop=True)
    return result, {
        "request_count": len(values),
        "requested_ticker_payload_sha256": _canonical_sha256(list(values)),
        "empty_ticker_count": len(empty_tickers),
        "empty_tickers": empty_tickers,
        "empty_ticker_payload_sha256": _canonical_sha256(empty_tickers),
        "nonempty_ticker_count": len(nonempty_tickers),
        "nonempty_tickers": nonempty_tickers,
        "nonempty_ticker_payload_sha256": _canonical_sha256(nonempty_tickers),
        "row_count": len(result),
    }


def _capture_tushare_sample(
    tickers: Iterable[str],
    sessions: Iterable[pd.Timestamp],
    client: Any,
) -> tuple[pd.DataFrame, dict[str, int]]:
    scope = set(map(str, tickers))
    frames: list[pd.DataFrame] = []
    provider_rows = 0
    empty_dates = 0
    values = tuple(pd.Timestamp(value).normalize() for value in sessions)
    fields = ",".join(DIVIDEND_FIELDS)
    for index, date in enumerate(values, start=1):
        response = client.query(
            "dividend", ex_date=date.strftime("%Y%m%d"), fields=fields
        )
        if response.empty:
            empty_dates += 1
            continue
        if len(response) >= TUSHARE_EX_DATE_ROW_LIMIT:
            raise ValueError("Tushare ex-date response reached the frozen row limit")
        provider_rows += len(response)
        normalized = normalize_dividend_response(response)
        if not normalized["ex_date"].eq(date.strftime("%Y%m%d")).all():
            raise ValueError("Tushare ex-date query returned another date")
        selected = normalized.loc[normalized["ts_code"].isin(scope)]
        if not selected.empty:
            frames.append(selected)
        if index % 250 == 0 or index == len(values):
            print(f"Tushare sample fetched {index}/{len(values)}", flush=True)
    result = (
        normalize_dividend_response(
            pd.concat(frames, ignore_index=True), expected_tickers=scope
        )
        if frames
        else pd.DataFrame(columns=DIVIDEND_FIELDS)
    )
    return result, {
        "request_count": len(values),
        "empty_ex_date_count": empty_dates,
        "provider_row_count": provider_rows,
        "scope_row_count": len(result),
    }


def _frame_exact(left: pd.DataFrame, right: pd.DataFrame, *, role: str) -> None:
    try:
        pd.testing.assert_frame_equal(
            left,
            right,
            check_exact=True,
            check_dtype=True,
            check_index_type=True,
            check_column_type=True,
            check_names=True,
            check_like=False,
        )
    except AssertionError as exc:
        raise ValueError(f"stable {role} samples differ") from exc


def _default_tushare_client() -> Any:
    token_path = ROOT / "runtime" / "secrets" / "settings" / "tushare_token"
    token = token_path.read_text(encoding="utf-8").strip()
    return RateLimitedRetryingClient(
        TushareClient(token=token), TUSHARE_REQUEST_RATE_PER_MINUTE
    )


def _default_cninfo_provider(*, symbol: str) -> pd.DataFrame:
    """Read the CNInfo endpoint without AkShare's empty-frame KeyError."""

    import py_mini_racer
    import requests
    from akshare.stock.stock_dividend_cninfo import _get_file_content_ths

    raw_columns = (
        "F006D",
        "F044V",
        "F010N",
        "F011N",
        "F012N",
        "F018D",
        "F020D",
        "F023D",
        "F025D",
        "F007V",
        "F001V",
    )
    engine = py_mini_racer.MiniRacer()
    engine.eval(_get_file_content_ths("cninfo.js"))
    code = engine.call("getResCode1")
    response = requests.post(
        "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1139",
        params={"scode": symbol},
        headers={
            "Accept": "*/*",
            "Accept-Enckey": code,
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Content-Length": "0",
            "Host": "webapi.cninfo.com.cn",
            "Origin": "http://webapi.cninfo.com.cn",
            "Pragma": "no-cache",
            "Proxy-Connection": "keep-alive",
            "Referer": "http://webapi.cninfo.com.cn/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/93.0.4577.63 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
        },
        timeout=20.0,
    )
    response.raise_for_status()
    payload = response.json()
    records = payload.get("records")
    if not isinstance(records, list):
        raise ValueError("CNInfo response lacks a records list")
    return pd.DataFrame(records).reindex(columns=raw_columns)


def _implementation_identity(*, formal: bool) -> dict[str, Any]:
    value = {
        "git_head": _git("rev-parse", "HEAD"),
        "branch": _git("branch", "--show-current"),
        "git_dirty": bool(_git("status", "--porcelain")),
        "commit_bound": bool(formal),
    }
    if formal and (value["branch"] != "main" or value["git_dirty"]):
        raise RuntimeError("formal 13.0 capture requires clean main")
    return value


def _tushare_reference_diagnostics(
    cninfo_actions: pd.DataFrame,
    tushare_actions: pd.DataFrame,
    *,
    store: PITStockRawStore,
) -> pd.DataFrame:
    """Classify every Tushare action under the frozen raw-reference fallback."""

    primary_pairs = set(
        cninfo_actions[["ticker", "ex_date"]]
        .astype(str)
        .itertuples(index=False, name=None)
    )
    sessions = tuple(pd.Timestamp(value).normalize() for value in store.market_sessions)
    session_index = {value: index for index, value in enumerate(sessions)}
    market_cache: dict[pd.Timestamp, pd.DataFrame] = {}

    def market(value: pd.Timestamp) -> pd.DataFrame:
        if value not in market_cache:
            frame = store.read_market(value)
            if frame["ts_code"].duplicated().any():
                raise ValueError("raw market contains duplicate tickers")
            market_cache[value] = frame.set_index("ts_code")
        return market_cache[value]

    rows: list[dict[str, Any]] = []
    grouped = tushare_actions.groupby(
        ["ticker", "ex_date"], sort=True, dropna=False
    )
    for (ticker_value, ex_value), group in grouped:
        ticker = str(ticker_value)
        ex_text = str(ex_value)
        pair = (ticker, ex_text)
        common: dict[str, Any] = {
            "previous_session": None,
            "previous_close": None,
            "provider_pre_close": None,
            "previous_adj_factor": None,
            "ex_adj_factor": None,
            "factor_ratio": None,
            "price_ratio": None,
            "theoretical_reference": None,
            "theoretical_reference_relative_error": None,
            "factor_reference_absolute_ratio_error": None,
            "factor_jump": None,
            "fallback_eligible": False,
            "status": "covered_by_cninfo",
        }
        if pair not in primary_pairs:
            if len(group) != 1:
                common["status"] = "ambiguous_multiple_tushare_actions"
            else:
                ex_date = pd.Timestamp(ex_text).normalize()
                index = session_index.get(ex_date)
                record_date = pd.Timestamp(group.iloc[0]["record_date"]).normalize()
                if (
                    index is None
                    or index == 0
                    or record_date not in session_index
                    or record_date >= ex_date
                ):
                    common["status"] = "invalid_event_session"
                else:
                    previous_session = sessions[index - 1]
                    common["previous_session"] = previous_session.date().isoformat()
                    previous = market(previous_session)
                    current = market(ex_date)
                    if ticker not in previous.index or ticker not in current.index:
                        common["status"] = "missing_adjacent_raw_bars"
                    else:
                        previous_close = float(previous.at[ticker, "close"])
                        provider_pre_close = float(current.at[ticker, "pre_close"])
                        previous_factor = float(previous.at[ticker, "adj_factor"])
                        ex_factor = float(current.at[ticker, "adj_factor"])
                        values = (
                            previous_close,
                            provider_pre_close,
                            previous_factor,
                            ex_factor,
                        )
                        if not all(
                            isfinite(value) and value > 0.0 for value in values
                        ):
                            common["status"] = "invalid_raw_reference"
                        else:
                            factor_ratio = ex_factor / previous_factor
                            price_ratio = previous_close / provider_pre_close
                            action = group.iloc[0]
                            cash = float(
                                action["cash_dividend_before_tax_per_share"]
                            )
                            stock = float(action["stock_dividend_per_share"])
                            theoretical = (previous_close - cash) / (1.0 + stock)
                            theory_error = abs(
                                theoretical / provider_pre_close - 1.0
                            )
                            factor_error = abs(factor_ratio - price_ratio)
                            factor_jump = (
                                abs(factor_ratio - 1.0)
                                > REFERENCE_FACTOR_JUMP_EPSILON
                            )
                            common.update(
                                {
                                    "previous_close": previous_close,
                                    "provider_pre_close": provider_pre_close,
                                    "previous_adj_factor": previous_factor,
                                    "ex_adj_factor": ex_factor,
                                    "factor_ratio": factor_ratio,
                                    "price_ratio": price_ratio,
                                    "theoretical_reference": theoretical,
                                    "theoretical_reference_relative_error": theory_error,
                                    "factor_reference_absolute_ratio_error": factor_error,
                                    "factor_jump": factor_jump,
                                }
                            )
                            if not factor_jump:
                                common["status"] = "no_factor_jump"
                            elif (
                                theory_error
                                > REFERENCE_THEORETICAL_RELATIVE_ERROR_AT_MOST
                                or factor_error
                                > REFERENCE_FACTOR_RATIO_ERROR_AT_MOST
                            ):
                                common["status"] = "raw_reference_mismatch"
                            else:
                                common["fallback_eligible"] = True
                                common["status"] = (
                                    "eligible_raw_reference_fallback"
                                )
        for action in group.sort_values("action_id", kind="mergesort").itertuples(
            index=False
        ):
            rows.append(
                {
                    "action_id": str(action.action_id),
                    "ticker": ticker,
                    "record_date": str(action.record_date),
                    "ex_date": ex_text,
                    **common,
                }
            )
    result = pd.DataFrame(rows, columns=TUSHARE_REFERENCE_DIAGNOSTIC_COLUMNS)
    if result["action_id"].duplicated().any():
        raise ValueError("Tushare reference diagnostic action IDs are duplicate")
    return result.sort_values(
        ["ex_date", "ticker", "record_date", "action_id"], kind="mergesort"
    ).reset_index(drop=True)


def capture_actions(
    output: Path,
    *,
    scope_name: str,
    formal: bool,
    cninfo_provider: Callable[..., pd.DataFrame] | None = None,
    tushare_client: Any | None = None,
) -> dict[str, Any]:
    protocol = _read_protocol()
    output = output.resolve()
    if output.exists():
        raise FileExistsError(f"create-only corporate-action output exists: {output}")
    if formal and scope_name != "all":
        raise RuntimeError("formal 13.0 capture requires the all-ticker scope")
    tickers = _scope(scope_name)
    scope_payload = _canonical_sha256(list(tickers))
    store = PITStockRawStore(ROOT, maximum_read_date=END_DATE)
    sessions = tuple(
        value
        for value in store.market_sessions
        if START_DATE <= value <= END_DATE
    )
    if not sessions or sessions[0] != START_DATE or sessions[-1] != END_DATE:
        raise ValueError("corporate-action official session boundary differs")
    if cninfo_provider is None:
        cninfo_provider = _default_cninfo_provider
    if tushare_client is None:
        tushare_client = _default_tushare_client()
    captured_at: list[str] = []
    cninfo_samples = []
    cninfo_receipts = []
    tushare_samples = []
    tushare_receipts = []
    for sample in range(2):
        cninfo, cninfo_receipt = _capture_cninfo_sample(
            tickers, cninfo_provider
        )
        captured_at.append(datetime.now(timezone.utc).isoformat())
        tushare, tushare_receipt = _capture_tushare_sample(
            tickers, sessions, tushare_client
        )
        captured_at.append(datetime.now(timezone.utc).isoformat())
        cninfo_samples.append(cninfo)
        cninfo_receipts.append(cninfo_receipt)
        tushare_samples.append(tushare)
        tushare_receipts.append(tushare_receipt)
        print(f"completed stable sample {sample + 1}/2", flush=True)
    _frame_exact(cninfo_samples[0], cninfo_samples[1], role="CNInfo")
    _frame_exact(tushare_samples[0], tushare_samples[1], role="Tushare")
    cninfo_actions = canonical_cninfo_actions(
        cninfo_samples[0],
        start_date=START_DATE,
        end_date=END_DATE,
        expected_tickers=tickers,
    )
    tushare_actions = canonical_implemented_actions(
        tushare_samples[0],
        start_date=START_DATE,
        end_date=END_DATE,
        expected_tickers=tickers,
    )
    reference_diagnostics = _tushare_reference_diagnostics(
        cninfo_actions, tushare_actions, store=store
    )
    resolved = resolve_corporate_actions(
        cninfo_actions, tushare_actions, reference_diagnostics
    )

    transaction = output.parent / f".{output.name}.tmp-{uuid.uuid4().hex}"
    transaction.mkdir(parents=True, exist_ok=False)
    try:
        files = {
            "scope": transaction / "ticker-scope.json",
            "cninfo_first": transaction / "cninfo-first.parquet",
            "cninfo_second": transaction / "cninfo-second.parquet",
            "tushare_first": transaction / "tushare-first.parquet",
            "tushare_second": transaction / "tushare-second.parquet",
            "cninfo_actions": transaction / "cninfo-actions.parquet",
            "tushare_actions": transaction / "tushare-actions.parquet",
            "tushare_reference_diagnostics": transaction
            / "tushare-reference-diagnostics.parquet",
            "resolved_actions": transaction / "resolved-actions.parquet",
        }
        scope_value = {
            "schema_version": 1,
            "scope": scope_name,
            "ticker_count": len(tickers),
            "tickers": list(tickers),
            "payload_sha256": scope_payload,
        }
        _write_json(files["scope"], scope_value)
        cninfo_samples[0].to_parquet(files["cninfo_first"], index=False)
        cninfo_samples[1].to_parquet(files["cninfo_second"], index=False)
        tushare_samples[0].to_parquet(files["tushare_first"], index=False)
        tushare_samples[1].to_parquet(files["tushare_second"], index=False)
        cninfo_actions.to_parquet(files["cninfo_actions"], index=False)
        tushare_actions.to_parquet(files["tushare_actions"], index=False)
        reference_diagnostics.to_parquet(
            files["tushare_reference_diagnostics"], index=False
        )
        resolved.to_parquet(files["resolved_actions"], index=False)
        for path in files.values():
            _fsync_file(path)
        artifacts = {
            name: {
                "path": path.name,
                "file_sha256": file_sha256(path),
                "row_count": (
                    1
                    if name == "scope"
                    else len(
                        {
                            "cninfo_first": cninfo_samples[0],
                            "cninfo_second": cninfo_samples[1],
                            "tushare_first": tushare_samples[0],
                            "tushare_second": tushare_samples[1],
                            "cninfo_actions": cninfo_actions,
                            "tushare_actions": tushare_actions,
                            "tushare_reference_diagnostics": reference_diagnostics,
                            "resolved_actions": resolved,
                        }[name]
                    )
                ),
            }
            for name, path in files.items()
        }
        manifest: dict[str, Any] = {
            "schema_version": 1,
            "kind": "factor_lab_13_0_stable_corporate_action_capture",
            "release": RELEASE,
            "status": "stable_corporate_actions_captured_real_share_return_unopened",
            "protocol": {
                "path": PROTOCOL_PATH.as_posix(),
                "protocol_id": PROTOCOL_ID,
                "payload_sha256": protocol["payload_sha256"],
                "file_sha256": PROTOCOL_FILE_SHA256,
            },
            "scope": {
                "name": scope_name,
                "ticker_count": len(tickers),
                "payload_sha256": scope_payload,
            },
            "phase": {
                "start_date": str(START_DATE.date()),
                "end_date": str(END_DATE.date()),
                "official_session_count": len(sessions),
            },
            "captures": {
                "captured_at_utc": captured_at,
                "cninfo": cninfo_receipts,
                "tushare": tushare_receipts,
                "exact_samples": True,
            },
            "raw_reference_fallback": {
                "diagnostic_row_count": len(reference_diagnostics),
                "eligible_action_count": int(
                    reference_diagnostics["fallback_eligible"].sum()
                ),
                "status_counts": {
                    str(key): int(value)
                    for key, value in reference_diagnostics["status"]
                    .value_counts(sort=False)
                    .sort_index()
                    .items()
                },
                "theoretical_reference_relative_error_at_most": REFERENCE_THEORETICAL_RELATIVE_ERROR_AT_MOST,
                "factor_reference_absolute_ratio_error_at_most": REFERENCE_FACTOR_RATIO_ERROR_AT_MOST,
                "factor_jump_epsilon": REFERENCE_FACTOR_JUMP_EPSILON,
            },
            "provider_versions": {
                "akshare": importlib.metadata.version("akshare"),
                "tushare": importlib.metadata.version("tushare"),
            },
            "artifacts": artifacts,
            "implementation": _implementation_identity(formal=formal),
            "claim_contract": {
                "return_opened": False,
                "selection_opened": False,
                "profit_claim_allowed": False,
            },
        }
        manifest["payload_sha256"] = _canonical_sha256(manifest)
        _write_json(transaction / "manifest.json", manifest)
        _fsync_directory(transaction)
        os.replace(transaction, output)
        _fsync_directory(output.parent)
    except BaseException:
        if transaction.exists():
            for path in transaction.iterdir():
                if path.is_file():
                    path.unlink()
            transaction.rmdir()
        raise
    return verify_capture(output)


def _load_reanchor_source(
    root: Path, expected_identity: dict[str, str]
) -> dict[str, Any]:
    root = root.resolve()
    manifest_path = root / "manifest.json"
    if (
        not manifest_path.is_file()
        or file_sha256(manifest_path)
        != expected_identity["manifest_file_sha256"]
    ):
        raise ValueError("13.0 reanchor source manifest file identity differs")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if (
        manifest.get("payload_sha256")
        != expected_identity["manifest_payload_sha256"]
        or _canonical_sha256(manifest)
        != expected_identity["manifest_payload_sha256"]
        or manifest.get("schema_version") != 1
        or manifest.get("kind")
        != "factor_lab_13_0_stable_corporate_action_capture"
        or manifest.get("release") != RELEASE
        or manifest.get("status")
        != "stable_corporate_actions_captured_real_share_return_unopened"
        or manifest.get("protocol", {}).get("protocol_id") != PROTOCOL_ID
        or manifest.get("protocol", {}).get("payload_sha256")
        != expected_identity["protocol_payload_sha256"]
        or manifest.get("protocol", {}).get("file_sha256")
        != expected_identity["protocol_file_sha256"]
        or manifest.get("scope", {}).get("name") != "candidate"
        or manifest.get("scope", {}).get("ticker_count") != 501
        or manifest.get("scope", {}).get("payload_sha256")
        != _canonical_sha256(list(_scope("candidate")))
        or manifest.get("captures", {}).get("exact_samples") is not True
        or manifest.get("claim_contract")
        != {
            "return_opened": False,
            "selection_opened": False,
            "profit_claim_allowed": False,
        }
        or set(manifest.get("artifacts", {}))
        != set(ACTION_ARTIFACT_ROLES)
    ):
        raise ValueError("13.0 reanchor source manifest contract differs")
    for role, entry in manifest["artifacts"].items():
        relative = Path(str(entry.get("path", "")))
        path = root / relative
        if (
            relative.name != str(relative)
            or not path.is_file()
            or file_sha256(path) != entry.get("file_sha256")
        ):
            raise ValueError(
                f"13.0 reanchor source artifact identity differs: {role}"
            )
    return manifest


def reanchor_candidate_actions(
    output: Path, *, source_roots: Iterable[Path]
) -> dict[str, Any]:
    """Rebind two byte-identical, already stable captures to this protocol.

    This path does not claim a new external capture.  It is intentionally pinned
    to the two known pre-return candidate captures made before the minute contract
    reached v5, and it re-runs the current semantic verifier after copying them.
    """

    output = output.resolve()
    roots = tuple(Path(value).resolve() for value in source_roots)
    if len(roots) != len(REANCHOR_SOURCE_IDENTITIES):
        raise ValueError("13.0 reanchor requires both pinned source captures")
    if output.exists():
        raise FileExistsError(
            f"create-only corporate-action output exists: {output}"
        )
    sources = tuple(
        _load_reanchor_source(root, identity)
        for root, identity in zip(
            roots, REANCHOR_SOURCE_IDENTITIES, strict=True
        )
    )
    left = json.loads(json.dumps(sources[0]))
    right = json.loads(json.dumps(sources[1]))
    for value in (left, right):
        value.pop("payload_sha256")
        value.pop("protocol")
        value["captures"].pop("captured_at_utc")
    if left != right:
        raise ValueError("13.0 reanchor source receipts or contracts differ")
    shared_hashes = {
        role: sources[0]["artifacts"][role]["file_sha256"]
        for role in ACTION_ARTIFACT_ROLES
    }
    if any(
        sources[1]["artifacts"][role]["file_sha256"] != digest
        for role, digest in shared_hashes.items()
    ):
        raise ValueError("13.0 reanchor source artifact bytes differ")

    protocol = _read_protocol()
    transaction = output.parent / f".{output.name}.tmp-{uuid.uuid4().hex}"
    transaction.mkdir(parents=True, exist_ok=False)
    try:
        canonical_source = roots[1]
        for role in ACTION_ARTIFACT_ROLES:
            relative = sources[1]["artifacts"][role]["path"]
            destination = transaction / relative
            shutil.copy2(canonical_source / relative, destination)
            _fsync_file(destination)
        manifest = json.loads(json.dumps(sources[1]))
        manifest["protocol"] = {
            "path": PROTOCOL_PATH.as_posix(),
            "protocol_id": PROTOCOL_ID,
            "payload_sha256": protocol["payload_sha256"],
            "file_sha256": PROTOCOL_FILE_SHA256,
        }
        manifest["implementation"] = _implementation_identity(formal=False)
        manifest["reanchored_from"] = {
            "reason_code": "cninfo_http_403_after_minute_capture",
            "canonical_source": REANCHOR_SOURCE_IDENTITIES[1]["label"],
            "content_equality": (
                "all_non_manifest_artifact_file_sha256_equal"
            ),
            "source_manifest_identities": list(
                REANCHOR_SOURCE_IDENTITIES
            ),
            "shared_artifact_file_sha256": shared_hashes,
        }
        manifest["payload_sha256"] = _canonical_sha256(manifest)
        _write_json(transaction / "manifest.json", manifest)
        _fsync_directory(transaction)
        os.replace(transaction, output)
        _fsync_directory(output.parent)
    except BaseException:
        if transaction.exists():
            for path in transaction.iterdir():
                if path.is_file():
                    path.unlink()
            transaction.rmdir()
        raise
    return verify_capture(output)


def verify_capture(output: Path) -> dict[str, Any]:
    output = output.resolve()
    protocol = _read_protocol()
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("payload_sha256") != _canonical_sha256(manifest):
        raise ValueError("13.0 corporate-action manifest payload differs")
    expected_artifacts = set(ACTION_ARTIFACT_ROLES)
    if set(manifest.get("artifacts", {})) != expected_artifacts:
        raise ValueError("13.0 corporate-action artifact set differs")
    if manifest.get("protocol") != {
        "path": PROTOCOL_PATH.as_posix(),
        "protocol_id": PROTOCOL_ID,
        "payload_sha256": protocol["payload_sha256"],
        "file_sha256": PROTOCOL_FILE_SHA256,
    }:
        raise ValueError("13.0 corporate-action protocol binding differs")
    reanchored = manifest.get("reanchored_from")
    if reanchored is not None:
        shared_hashes = {
            role: manifest["artifacts"][role]["file_sha256"]
            for role in ACTION_ARTIFACT_ROLES
        }
        if reanchored != {
            "reason_code": "cninfo_http_403_after_minute_capture",
            "canonical_source": REANCHOR_SOURCE_IDENTITIES[1]["label"],
            "content_equality": (
                "all_non_manifest_artifact_file_sha256_equal"
            ),
            "source_manifest_identities": list(
                REANCHOR_SOURCE_IDENTITIES
            ),
            "shared_artifact_file_sha256": shared_hashes,
        }:
            raise ValueError("13.0 corporate-action reanchor receipt differs")
    scope_value = json.loads((output / "ticker-scope.json").read_text(encoding="utf-8"))
    scope_name = str(manifest.get("scope", {}).get("name"))
    tickers = _scope(scope_name)
    scope_payload = _canonical_sha256(list(tickers))
    if scope_value != {
        "schema_version": 1,
        "scope": scope_name,
        "ticker_count": len(tickers),
        "tickers": list(tickers),
        "payload_sha256": scope_payload,
    } or manifest.get("scope") != {
        "name": scope_name,
        "ticker_count": len(tickers),
        "payload_sha256": scope_payload,
    }:
        raise ValueError("13.0 corporate-action ticker scope differs")
    cninfo_receipts = manifest.get("captures", {}).get("cninfo", [])
    if len(cninfo_receipts) != 2:
        raise ValueError("13.0 CNInfo stable receipts differ")
    for receipt in cninfo_receipts:
        empty = [str(value) for value in receipt.get("empty_tickers", [])]
        nonempty = [str(value) for value in receipt.get("nonempty_tickers", [])]
        nonempty_count = int(receipt.get("nonempty_ticker_count", -1))
        if (
            int(receipt.get("request_count", -1)) != len(tickers)
            or receipt.get("requested_ticker_payload_sha256") != scope_payload
            or len(empty) != int(receipt.get("empty_ticker_count", -1))
            or _canonical_sha256(empty)
            != receipt.get("empty_ticker_payload_sha256")
            or len(empty) + nonempty_count != len(tickers)
            or len(nonempty) != nonempty_count
            or _canonical_sha256(nonempty)
            != receipt.get("nonempty_ticker_payload_sha256")
            or set(empty).intersection(nonempty)
            or set(empty).union(nonempty) != set(tickers)
            or empty
            != [ticker for ticker in tickers if ticker in set(empty)]
            or nonempty
            != [ticker for ticker in tickers if ticker in set(nonempty)]
        ):
            raise ValueError("13.0 CNInfo requested/empty ticker receipt differs")
    if cninfo_receipts[0]["empty_tickers"] != cninfo_receipts[1]["empty_tickers"]:
        raise ValueError("13.0 CNInfo empty ticker sets differ between samples")
    for entry in manifest["artifacts"].values():
        path = output / entry["path"]
        if not path.is_file() or file_sha256(path) != entry["file_sha256"]:
            raise ValueError("13.0 corporate-action artifact hash differs")
    cninfo_first = pd.read_parquet(output / "cninfo-first.parquet")
    cninfo_second = pd.read_parquet(output / "cninfo-second.parquet")
    tushare_first = pd.read_parquet(output / "tushare-first.parquet")
    tushare_second = pd.read_parquet(output / "tushare-second.parquet")
    _frame_exact(cninfo_first, cninfo_second, role="persisted CNInfo")
    _frame_exact(tushare_first, tushare_second, role="persisted Tushare")
    cninfo_actions = canonical_cninfo_actions(
        cninfo_first,
        start_date=START_DATE,
        end_date=END_DATE,
        expected_tickers=tickers,
    )
    tushare_actions = canonical_implemented_actions(
        tushare_first,
        start_date=START_DATE,
        end_date=END_DATE,
        expected_tickers=tickers,
    )
    store = PITStockRawStore(ROOT, maximum_read_date=END_DATE)
    reference_diagnostics = _tushare_reference_diagnostics(
        cninfo_actions, tushare_actions, store=store
    )
    resolved = resolve_corporate_actions(
        cninfo_actions, tushare_actions, reference_diagnostics
    )
    expected_fallback = {
        "diagnostic_row_count": len(reference_diagnostics),
        "eligible_action_count": int(
            reference_diagnostics["fallback_eligible"].sum()
        ),
        "status_counts": {
            str(key): int(value)
            for key, value in reference_diagnostics["status"]
            .value_counts(sort=False)
            .sort_index()
            .items()
        },
        "theoretical_reference_relative_error_at_most": REFERENCE_THEORETICAL_RELATIVE_ERROR_AT_MOST,
        "factor_reference_absolute_ratio_error_at_most": REFERENCE_FACTOR_RATIO_ERROR_AT_MOST,
        "factor_jump_epsilon": REFERENCE_FACTOR_JUMP_EPSILON,
    }
    if manifest.get("raw_reference_fallback") != expected_fallback:
        raise ValueError("13.0 raw-reference fallback receipt differs")
    _frame_exact(
        cninfo_actions,
        pd.read_parquet(output / "cninfo-actions.parquet"),
        role="persisted CNInfo actions",
    )
    _frame_exact(
        tushare_actions,
        pd.read_parquet(output / "tushare-actions.parquet"),
        role="persisted Tushare actions",
    )
    _frame_exact(
        reference_diagnostics,
        pd.read_parquet(output / "tushare-reference-diagnostics.parquet"),
        role="persisted Tushare reference diagnostics",
    )
    _frame_exact(
        resolved,
        pd.read_parquet(output / "resolved-actions.parquet"),
        role="persisted resolved actions",
    )
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", choices=("capture-actions",), default="capture-actions"
    )
    parser.add_argument("--scope", choices=("candidate", "all"), default="all")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--formal", action="store_true")
    parser.add_argument(
        "--reuse-capture",
        action="append",
        type=Path,
        default=[],
        help=(
            "reanchor the two pinned pre-return candidate captures instead "
            "of calling providers"
        ),
    )
    args = parser.parse_args(argv)
    if args.reuse_capture:
        if args.scope != "candidate" or args.formal:
            parser.error(
                "--reuse-capture is restricted to the non-formal candidate scope"
            )
        result = reanchor_candidate_actions(
            args.output, source_roots=args.reuse_capture
        )
    else:
        result = capture_actions(
            args.output,
            scope_name=args.scope,
            formal=args.formal,
        )
    print(
        json.dumps(
            {
                "output": str(args.output.resolve()),
                "status": result["status"],
                "payload_sha256": result["payload_sha256"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
