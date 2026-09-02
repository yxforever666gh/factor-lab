#!/usr/bin/env python
"""Resumable create-only Diemeng/Tushare candidate execution capture."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import sys
import uuid
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
while str(SRC) in sys.path:
    sys.path.remove(str(SRC))
sys.path.insert(0, str(SRC))

from factor_lab.data.diemeng_minutes import (  # noqa: E402
    DIEMENG_MINUTE_COLUMNS,
    DIEMENG_UNIT_CONTRACT_ID,
    DiemengMinuteHTTPClient,
    capture_diemeng_minutes,
)
from factor_lab.data.etf_live import RateLimitedRetryingClient  # noqa: E402
from factor_lab.data.diemeng_minute_store import candidate_snapshot_payload  # noqa: E402
from factor_lab.data.pit_stock import PITStockRawStore  # noqa: E402
from factor_lab.data.sources import TushareClient  # noqa: E402
from factor_lab.release_integrity import (  # noqa: E402
    canonical_payload_sha256,
    file_sha256,
)
from factor_lab.research.pit_stock_minute_scope import (  # noqa: E402
    FORMAL_STAGE1_PAIR_COUNT,
    FORMAL_STAGE1_PAYLOAD_SHA256,
    build_formal_development_scopes,
)
from factor_lab.research.pit_stock import canonical_sha256  # noqa: E402


RELEASE = "13.0"
PROTOCOL_PATH = ROOT / "protocols/13.0-profit-first-real-share-closure.json"
PROTOCOL_PAYLOAD_SHA256 = "7989478d24cc4597a7066eab5a737e698d3647d5a70704f85854a744ec3fa9d8"
PROTOCOL_FILE_SHA256 = "9973cbdeb0e2d421e227afe7d22ffa9074e0f847041a8025b294cdb5f642fef2"
PANEL_PATH = ROOT / "runtime/data/pit-stock-12.0/development/quarterly-snapshots.parquet"
TARGETS_PATH = ROOT / "runtime/data/pit-stock-12.0/development/targets.parquet"
PANEL_FILE_SHA256 = "d51cad0de60484292ca24e4909d2c7617e0d83d3b8df58b29358f438eb5a48ca"
TARGETS_FILE_SHA256 = "ce3cd6c37dd1b04e77c170055e1bb7351cd99b6e2a201e7745ac1cf29a9ea06c"
TARGETS_PAYLOAD_SHA256 = "1022288372cd07f97b0e963b670c3a7e9ddfc94414b4d21cb7cbea9c643e76be"
PANEL_PAYLOAD_SHA256 = "3392346175e43618039020d5f5989483bd520d4366949371e5f940aa938d7a65"
PAIR_SCOPE_COUNT = FORMAL_STAGE1_PAIR_COUNT
PAIR_SCOPE_PAYLOAD_SHA256 = FORMAL_STAGE1_PAYLOAD_SHA256
CALIBRATION_ROOT = Path(r"H:\Download\FactorLabPytest\factor-lab-13.0-diemeng-unit-regimes-v5")
CALIBRATION_MANIFEST_PAYLOAD_SHA256 = "04c43dacebdfc35b40a457023f40696188254bf9858ea3b026aadb820253fd43"
CALIBRATION_MANIFEST_FILE_SHA256 = "c014eece5670f925bf323442e8605ebe2518834b50fb750141f6b6b5cf3c3056"
CALIBRATION_UNIT_PAYLOAD_SHA256 = "b7d676b4ed2230b910a896749c705c0e46d5d53031430c44484823bc0be9fc22"
DEFAULT_OUTPUT = Path(r"H:\Download\FactorLabPytest\factor-lab-13.0-candidate-minutes")
LIMIT_COLUMNS = ("trade_date", "ticker", "pre_close", "up_limit", "down_limit")
REANCHOR_SOURCE_IDENTITY = {
    "manifest_payload_sha256": "39c47f45a854fc3a0283879bef428f1f00e252b49af1701fd2d74dafcf6bdc6f",
    "manifest_file_sha256": "a37bf3e14092148b3fc64e0ced1be68564b93681e1ad8284aa127bd88a10ba45",
    "capture_plan_file_sha256": "ce52603038b7398ac3558093a0e97dc273a3d81739abb946d45e667cd63591e4",
    "protocol_payload_sha256": "89cc2962ec4bbb0b86ba7b578f8803639828da371d8e3f64aee13cf6520f1f9e",
    "protocol_file_sha256": "58a2660d4f2490bcc2db32665c66683a74d4463469a602dd7476042cb56bfa11",
    "plan_payload_sha256": "3767f8ea6be5d194ee43f6ca54764a5662a284c9d5f9859d1f7bd86b14fd5045",
}


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    with path.open("r+b") as handle:
        os.fsync(handle.fileno())


def _fsync(path: Path) -> None:
    with path.open("r+b") as handle:
        os.fsync(handle.fileno())


def _stable_receipt(value: dict[str, Any]) -> dict[str, Any]:
    result = dict(value)
    result.pop("captured_at_utc", None)
    return result


def _verify_protocol_and_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    protocol = json.loads(PROTOCOL_PATH.read_text(encoding="utf-8"))
    if (
        protocol.get("payload_sha256") != PROTOCOL_PAYLOAD_SHA256
        or canonical_payload_sha256(protocol) != PROTOCOL_PAYLOAD_SHA256
        or file_sha256(PROTOCOL_PATH) != PROTOCOL_FILE_SHA256
    ):
        raise ValueError("13.0 minute protocol binding differs")
    if (
        file_sha256(PANEL_PATH) != PANEL_FILE_SHA256
        or file_sha256(TARGETS_PATH) != TARGETS_FILE_SHA256
    ):
        raise ValueError("12.0 exact panel/targets file differs")
    panel = pd.read_parquet(PANEL_PATH)
    targets = pd.read_parquet(TARGETS_PATH)
    identity = targets[["strategy_id", "signal_date", "ticker", "target_weight"]].copy()
    identity["signal_date"] = pd.to_datetime(identity["signal_date"]).dt.strftime(
        "%Y-%m-%d"
    )
    identity = identity.sort_values(["signal_date", "ticker"], kind="mergesort")
    if canonical_sha256(identity.to_dict("records")) != TARGETS_PAYLOAD_SHA256:
        raise ValueError("12.0 exact target payload differs")
    return panel, targets


def _verify_calibration() -> None:
    path = CALIBRATION_ROOT / "manifest.json"
    value = json.loads(path.read_text(encoding="utf-8"))
    if (
        value.get("payload_sha256") != CALIBRATION_MANIFEST_PAYLOAD_SHA256
        or canonical_payload_sha256(value) != CALIBRATION_MANIFEST_PAYLOAD_SHA256
        or file_sha256(path) != CALIBRATION_MANIFEST_FILE_SHA256
        or value.get("unit_contract", {}).get("payload_sha256")
        != CALIBRATION_UNIT_PAYLOAD_SHA256
    ):
        raise ValueError("Diemeng per-slice calibration binding differs")


def _derive_plan() -> dict[str, Any]:
    panel, targets = _verify_protocol_and_inputs()
    panel = panel.copy()
    targets = targets.copy()
    panel["signal_date"] = pd.to_datetime(panel["signal_date"]).dt.normalize()
    targets["signal_date"] = pd.to_datetime(targets["signal_date"]).dt.normalize()
    signals = tuple(sorted(pd.Timestamp(value) for value in panel["signal_date"].unique()))
    if len(signals) != 22:
        raise ValueError("candidate minute signal count differs")
    formal_scope = build_formal_development_scopes(ROOT).stage1
    pairs = [dict(value) for value in formal_scope.records]
    if (
        formal_scope.pair_count != PAIR_SCOPE_COUNT
        or formal_scope.payload_sha256 != PAIR_SCOPE_PAYLOAD_SHA256
        or canonical_sha256(pairs) != PAIR_SCOPE_PAYLOAD_SHA256
    ):
        raise ValueError("candidate minute pair scope differs")
    rows_by_signal: dict[str, list[dict[str, Any]]] = {}
    for pair in pairs:
        rows_by_signal.setdefault(str(pair["signal_date"]), []).append(pair)
    raw_store = PITStockRawStore(
        ROOT,
        maximum_read_date="2023-01-03",
        calendar_through_date="2026-08-21",
    )
    previous: set[str] = set()
    executions: list[dict[str, Any]] = []
    for signal in signals:
        current = set(
            targets.loc[targets["signal_date"].eq(signal), "ticker"].astype(str)
        )
        scope_rows = rows_by_signal.get(signal.date().isoformat(), [])
        execution_date = raw_store.sessions[
            raw_store.sessions.index(signal) + 1
        ].date().isoformat()
        execution_dates = {str(value["execution_date"]) for value in scope_rows}
        if execution_dates and execution_dates != {execution_date}:
            raise ValueError("candidate minute execution scope differs")
        scope = [str(value["ticker"]) for value in scope_rows]
        executions.append(
            {
                "signal_date": signal.date().isoformat(),
                "execution_date": execution_date,
                "previous_target_count": len(previous),
                "current_target_count": len(current),
                "ticker_count": len(scope),
                "tickers": scope,
                "snapshot_payload_sha256": candidate_snapshot_payload(
                    panel.loc[panel["signal_date"].eq(signal)]
                ),
                "mark_only": signal == signals[-1],
            }
        )
        previous = current
    plan = {
        "schema_version": 1,
        "kind": "factor_lab_13_0_candidate_minute_plan",
        "release": RELEASE,
        "signal_count": len(signals),
        "pair_count": len(pairs),
        "unique_ticker_count": len({value["ticker"] for value in pairs}),
        "nonempty_execution_count": sum(bool(value["tickers"]) for value in executions),
        "executions": executions,
        "pairs": pairs,
        "pair_payload_sha256": canonical_sha256(pairs),
        "target_payload_sha256": TARGETS_PAYLOAD_SHA256,
        "panel_file_sha256": PANEL_FILE_SHA256,
        "panel_payload_sha256": PANEL_PAYLOAD_SHA256,
        "protocol_payload_sha256": PROTOCOL_PAYLOAD_SHA256,
        "unit_contract_id": DIEMENG_UNIT_CONTRACT_ID,
        "calibration_manifest_payload_sha256": (
            CALIBRATION_MANIFEST_PAYLOAD_SHA256
        ),
        "calibration_manifest_file_sha256": CALIBRATION_MANIFEST_FILE_SHA256,
        "calibration_unit_payload_sha256": CALIBRATION_UNIT_PAYLOAD_SHA256,
    }
    plan["payload_sha256"] = canonical_payload_sha256(plan)
    return plan


def _pair_dir(staging: Path, pair: dict[str, str]) -> Path:
    return (
        staging
        / "minutes"
        / f"execution_date={pair['execution_date']}"
        / f"ticker={pair['ticker']}"
    )


def _verify_pair(path: Path, pair: dict[str, str]) -> dict[str, Any]:
    receipt_path = path / "receipt.json"
    data_path = path / "data.parquet"
    value = json.loads(receipt_path.read_text(encoding="utf-8"))
    if (
        value.get("payload_sha256") != canonical_payload_sha256(value)
        or value.get("identity") != pair
        or not data_path.is_file()
        or data_path.stat().st_size != int(value["artifact"]["size_bytes"])
        or file_sha256(data_path) != value["artifact"]["file_sha256"]
    ):
        raise ValueError("candidate minute pair stage differs")
    frame = pd.read_parquet(data_path)
    if tuple(map(str, frame.columns)) != DIEMENG_MINUTE_COLUMNS:
        raise ValueError("candidate minute pair schema differs")
    if len(frame) != int(value["artifact"]["row_count"]):
        raise ValueError("candidate minute pair row count differs")
    expected_start = f"{pair['execution_date']} 09:30:00"
    expected_end = f"{pair['execution_date']} 09:47:00"
    payload = frame.copy()
    payload["trade_time"] = pd.to_datetime(payload["trade_time"]).dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    payload["observable_at"] = pd.to_datetime(
        payload["observable_at"]
    ).dt.strftime("%Y-%m-%d %H:%M:%S")
    payload = payload.astype(object).where(pd.notna(payload), None)
    payload_sha256 = canonical_sha256(payload.to_dict("records"))
    captures = (value.get("first_capture"), value.get("second_capture"))
    if not all(isinstance(capture, dict) for capture in captures):
        raise ValueError("candidate minute pair lacks stable captures")
    for capture in captures:
        assert isinstance(capture, dict)
        if (
            capture.get("provider") != "diemeng"
            or capture.get("ticker") != pair["ticker"]
            or capture.get("level") != "1min"
            or capture.get("start_time") != expected_start
            or capture.get("end_time") != expected_end
            or capture.get("page_size") != 512
            or capture.get("page_origin") != 0
            or capture.get("sentinel_empty_page_verified") is not True
            or capture.get("unit_contract_id") != DIEMENG_UNIT_CONTRACT_ID
            or capture.get("row_count") != len(frame)
            or capture.get("provider_total") != len(frame)
            or capture.get("payload_sha256") != payload_sha256
            or not isinstance(capture.get("pages"), list)
            or not capture["pages"]
            or capture["pages"][-1].get("sentinel") is not True
            or capture["pages"][-1].get("row_count") != 0
            or capture.get("request_count") != len(capture["pages"])
        ):
            raise ValueError("candidate minute pair capture contract differs")
    if _stable_receipt(captures[0]) != _stable_receipt(captures[1]):
        raise ValueError("candidate minute pair stable receipts differ")
    return value


def _capture_pair(
    staging: Path,
    pair: dict[str, str],
    *,
    client: Any,
    resume: bool,
) -> bool:
    destination = _pair_dir(staging, pair)
    if destination.exists():
        if not resume:
            raise FileExistsError("candidate minute pair already staged")
        _verify_pair(destination, pair)
        return False
    first = capture_diemeng_minutes(
        client,
        ticker=pair["ticker"],
        level="1min",
        start_time=f"{pair['execution_date']} 09:30:00",
        end_time=f"{pair['execution_date']} 09:47:00",
        page_size=512,
        require_nonempty=False,
    )
    second = capture_diemeng_minutes(
        client,
        ticker=pair["ticker"],
        level="1min",
        start_time=f"{pair['execution_date']} 09:30:00",
        end_time=f"{pair['execution_date']} 09:47:00",
        page_size=512,
        require_nonempty=False,
    )
    pd.testing.assert_frame_equal(
        first.frame, second.frame, check_exact=True, check_dtype=True, check_like=False
    )
    if _stable_receipt(first.receipt) != _stable_receipt(second.receipt):
        raise ValueError("candidate Diemeng stable pair differs")
    temporary = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    temporary.mkdir(parents=True, exist_ok=False)
    try:
        data_path = temporary / "data.parquet"
        first.frame.to_parquet(data_path, index=False)
        _fsync(data_path)
        value = {
            "schema_version": 1,
            "kind": "factor_lab_13_0_candidate_minute_pair",
            "identity": pair,
            "stable_exact": True,
            "first_capture": first.receipt,
            "second_capture": second.receipt,
            "artifact": {
                "path": "data.parquet",
                "file_sha256": file_sha256(data_path),
                "size_bytes": data_path.stat().st_size,
                "row_count": len(first.frame),
            },
            "secret_in_artifact": False,
        }
        value["payload_sha256"] = canonical_payload_sha256(value)
        _write_json(temporary / "receipt.json", value)
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.replace(temporary, destination)
    except BaseException:
        if temporary.exists():
            shutil.rmtree(temporary)
        raise
    _verify_pair(destination, pair)
    return True


def _normalize_limits(frame: pd.DataFrame, *, date: str, tickers: list[str]) -> pd.DataFrame:
    required = {"trade_date", "ts_code", "pre_close", "up_limit", "down_limit"}
    if not required.issubset(frame.columns):
        raise ValueError("stk_limit columns differ")
    work = frame.loc[:, ["trade_date", "ts_code", "pre_close", "up_limit", "down_limit"]].copy()
    work = work.loc[work["ts_code"].astype(str).isin(tickers)]
    work = work.rename(columns={"ts_code": "ticker"})
    work["trade_date"] = pd.to_datetime(work["trade_date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    for column in ("pre_close", "up_limit", "down_limit"):
        work[column] = pd.to_numeric(work[column], errors="raise").astype(float)
    work = work.sort_values("ticker", kind="mergesort").reset_index(drop=True)
    if (
        work["ticker"].duplicated().any()
        or not set(work["ticker"].astype(str)).issubset(set(tickers))
        or (not work.empty and not work["trade_date"].eq(date).all())
    ):
        raise ValueError("stk_limit scope/date differs")
    return work.loc[:, LIMIT_COLUMNS]


def _limit_dir(staging: Path, date: str) -> Path:
    return staging / "limits" / f"execution_date={date}"


def _verify_limits(path: Path, *, date: str, tickers: list[str]) -> dict[str, Any]:
    value = json.loads((path / "receipt.json").read_text(encoding="utf-8"))
    data_path = path / "limits.parquet"
    if (
        value.get("payload_sha256") != canonical_payload_sha256(value)
        or value.get("execution_date") != date
        or value.get("tickers") != tickers
        or value.get("missing_tickers")
        != sorted(set(tickers) - set(pd.read_parquet(data_path)["ticker"].astype(str)))
        or file_sha256(data_path) != value["artifact"]["file_sha256"]
        or data_path.stat().st_size != int(value["artifact"]["size_bytes"])
    ):
        raise ValueError("candidate limit stage differs")
    _normalize_limits(pd.read_parquet(data_path).rename(columns={"ticker": "ts_code"}).assign(trade_date=date.replace("-", "")), date=date, tickers=tickers)
    return value


def _capture_limits(
    staging: Path,
    *,
    date: str,
    tickers: list[str],
    client: Any,
    resume: bool,
) -> bool:
    destination = _limit_dir(staging, date)
    if destination.exists():
        if not resume:
            raise FileExistsError("candidate limit stage already exists")
        _verify_limits(destination, date=date, tickers=tickers)
        return False
    fields = "trade_date,ts_code,pre_close,up_limit,down_limit"
    first = _normalize_limits(
        client.query("stk_limit", trade_date=date.replace("-", ""), fields=fields),
        date=date,
        tickers=tickers,
    )
    second = _normalize_limits(
        client.query("stk_limit", trade_date=date.replace("-", ""), fields=fields),
        date=date,
        tickers=tickers,
    )
    pd.testing.assert_frame_equal(first, second, check_exact=True, check_dtype=True)
    temporary = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    temporary.mkdir(parents=True, exist_ok=False)
    try:
        data_path = temporary / "limits.parquet"
        first.to_parquet(data_path, index=False)
        _fsync(data_path)
        value = {
            "schema_version": 1,
            "kind": "factor_lab_13_0_candidate_price_limits",
            "execution_date": date,
            "tickers": tickers,
            "missing_tickers": sorted(
                set(tickers) - set(first["ticker"].astype(str))
            ),
            "stable_exact": True,
            "artifact": {
                "path": "limits.parquet",
                "file_sha256": file_sha256(data_path),
                "size_bytes": data_path.stat().st_size,
                "row_count": len(first),
            },
        }
        value["payload_sha256"] = canonical_payload_sha256(value)
        _write_json(temporary / "receipt.json", value)
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.replace(temporary, destination)
    except BaseException:
        if temporary.exists():
            shutil.rmtree(temporary)
        raise
    _verify_limits(destination, date=date, tickers=tickers)
    return True


def capture(
    output: Path,
    *,
    resume: bool,
    max_new_pairs: int | None,
    diemeng_client: Any | None = None,
    limit_client: Any | None = None,
) -> dict[str, Any]:
    output = output.resolve()
    _verify_calibration()
    if output.exists():
        raise FileExistsError(f"create-only final minute output exists: {output}")
    staging = output.with_name(output.name + ".staging")
    plan = _derive_plan()
    plan_path = staging / "capture-plan.json"
    if staging.exists():
        if not resume:
            raise FileExistsError("candidate minute staging exists; pass --resume")
        existing = json.loads(plan_path.read_text(encoding="utf-8"))
        if existing != plan:
            raise ValueError("candidate minute staged plan differs")
    else:
        staging.mkdir(parents=True, exist_ok=False)
        _write_json(plan_path, plan)
    if diemeng_client is None:
        key = (ROOT / "runtime/secrets/settings/diemeng_api_key").read_text(encoding="utf-8").strip()
        diemeng_client = DiemengMinuteHTTPClient(key, request_rate_per_minute=60.0)
    if limit_client is None:
        token = (ROOT / "runtime/secrets/settings/tushare_token").read_text(encoding="utf-8").strip()
        limit_client = RateLimitedRetryingClient(TushareClient(token=token), 300.0)
    new_pairs = 0
    for index, pair in enumerate(plan["pairs"], start=1):
        try:
            created = _capture_pair(
                staging, pair, client=diemeng_client, resume=True
            )
        except Exception as exc:
            raise RuntimeError(
                "candidate minute capture failed at "
                f"{pair['ticker']} {pair['execution_date']}: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        new_pairs += int(created)
        if index % 25 == 0 or index == len(plan["pairs"]):
            print(f"candidate minute pairs {index}/{len(plan['pairs'])}", flush=True)
        if max_new_pairs is not None and new_pairs >= max_new_pairs:
            return {
                "status": "partial_staging",
                "staging": str(staging),
                "new_pair_count": new_pairs,
                "completed_pair_count": index,
                "pair_count": len(plan["pairs"]),
            }
    for execution in plan["executions"]:
        _capture_limits(
            staging,
            date=execution["execution_date"],
            tickers=execution["tickers"],
            client=limit_client,
            resume=True,
        )
    artifacts = []
    for pair in plan["pairs"]:
        receipt = _verify_pair(_pair_dir(staging, pair), pair)
        artifacts.append(
            {
                "role": "minute_pair",
                "identity": pair,
                "receipt_payload_sha256": receipt["payload_sha256"],
                "receipt_file_sha256": file_sha256(
                    _pair_dir(staging, pair) / "receipt.json"
                ),
                "data_file_sha256": receipt["artifact"]["file_sha256"],
            }
        )
    for execution in plan["executions"]:
        receipt = _verify_limits(
            _limit_dir(staging, execution["execution_date"]),
            date=execution["execution_date"],
            tickers=execution["tickers"],
        )
        artifacts.append(
            {
                "role": "price_limits",
                "execution_date": execution["execution_date"],
                "receipt_payload_sha256": receipt["payload_sha256"],
                "receipt_file_sha256": file_sha256(
                    _limit_dir(staging, execution["execution_date"])
                    / "receipt.json"
                ),
                "data_file_sha256": receipt["artifact"]["file_sha256"],
            }
        )
    manifest = {
        "schema_version": 1,
        "kind": "factor_lab_13_0_candidate_minute_capture",
        "release": RELEASE,
        "status": "candidate_minutes_captured_return_unopened",
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "protocol": {
            "payload_sha256": PROTOCOL_PAYLOAD_SHA256,
            "file_sha256": PROTOCOL_FILE_SHA256,
        },
        "plan_payload_sha256": plan["payload_sha256"],
        "pair_scope_payload_sha256": PAIR_SCOPE_PAYLOAD_SHA256,
        "target_payload_sha256": TARGETS_PAYLOAD_SHA256,
        "panel_file_sha256": PANEL_FILE_SHA256,
        "panel_payload_sha256": PANEL_PAYLOAD_SHA256,
        "unit_contract_id": DIEMENG_UNIT_CONTRACT_ID,
        "calibration_manifest_payload_sha256": CALIBRATION_MANIFEST_PAYLOAD_SHA256,
        "calibration_manifest_file_sha256": CALIBRATION_MANIFEST_FILE_SHA256,
        "calibration_unit_payload_sha256": CALIBRATION_UNIT_PAYLOAD_SHA256,
        "execution_level": "1min",
        "execution_range": "09:30:00 opening-price evidence plus 09:31:00 through 09:47:00 continuous execution evidence",
        "pair_count": len(plan["pairs"]),
        "execution_count": len(plan["executions"]),
        "artifacts": artifacts,
        "secret_in_artifact": False,
        "claim_contract": {
            "strategy_return_opened": False,
            "selection_opened": False,
        },
    }
    manifest["payload_sha256"] = canonical_payload_sha256(manifest)
    _write_json(staging / "manifest.json", manifest)
    os.replace(staging, output)
    return verify_capture(output)


def reanchor_capture(source: Path, output: Path) -> dict[str, Any]:
    """Rebind the exact v5 raw capture after a pre-return anchor refreeze."""

    source = source.resolve()
    output = output.resolve()
    source_manifest_path = source / "manifest.json"
    source_plan_path = source / "capture-plan.json"
    if output.exists():
        raise FileExistsError(f"create-only candidate minute output exists: {output}")
    if (
        not source_manifest_path.is_file()
        or file_sha256(source_manifest_path)
        != REANCHOR_SOURCE_IDENTITY["manifest_file_sha256"]
        or not source_plan_path.is_file()
        or file_sha256(source_plan_path)
        != REANCHOR_SOURCE_IDENTITY["capture_plan_file_sha256"]
    ):
        raise ValueError("candidate minute reanchor source file identity differs")
    source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
    source_plan = json.loads(source_plan_path.read_text(encoding="utf-8"))
    if (
        source_manifest.get("payload_sha256")
        != REANCHOR_SOURCE_IDENTITY["manifest_payload_sha256"]
        or canonical_payload_sha256(source_manifest)
        != REANCHOR_SOURCE_IDENTITY["manifest_payload_sha256"]
        or source_manifest.get("protocol")
        != {
            "payload_sha256": REANCHOR_SOURCE_IDENTITY[
                "protocol_payload_sha256"
            ],
            "file_sha256": REANCHOR_SOURCE_IDENTITY["protocol_file_sha256"],
        }
        or source_manifest.get("plan_payload_sha256")
        != REANCHOR_SOURCE_IDENTITY["plan_payload_sha256"]
        or source_plan.get("payload_sha256")
        != REANCHOR_SOURCE_IDENTITY["plan_payload_sha256"]
        or canonical_payload_sha256(source_plan)
        != REANCHOR_SOURCE_IDENTITY["plan_payload_sha256"]
        or source_manifest.get("pair_count") != PAIR_SCOPE_COUNT
        or source_manifest.get("execution_count") != 22
        or source_manifest.get("claim_contract")
        != {"strategy_return_opened": False, "selection_opened": False}
    ):
        raise ValueError("candidate minute reanchor source contract differs")

    current_plan = _derive_plan()
    expected_source_plan = json.loads(json.dumps(current_plan))
    expected_source_plan["protocol_payload_sha256"] = REANCHOR_SOURCE_IDENTITY[
        "protocol_payload_sha256"
    ]
    expected_source_plan["payload_sha256"] = canonical_payload_sha256(
        expected_source_plan
    )
    if source_plan != expected_source_plan:
        raise ValueError("candidate minute reanchor source plan differs")

    transaction = output.parent / f".{output.name}.tmp-{uuid.uuid4().hex}"
    try:
        shutil.copytree(source, transaction)
        _write_json(transaction / "capture-plan.json", current_plan)
        manifest = json.loads(json.dumps(source_manifest))
        manifest["protocol"] = {
            "payload_sha256": PROTOCOL_PAYLOAD_SHA256,
            "file_sha256": PROTOCOL_FILE_SHA256,
        }
        manifest["plan_payload_sha256"] = current_plan["payload_sha256"]
        manifest["execution_range"] = (
            "09:30:00 opening-price evidence plus 09:31:00 through "
            "09:47:00 continuous execution evidence"
        )
        manifest["reanchored_from"] = {
            "reason_code": (
                "pre_return_zero_liquidity_preclose_placeholder_refreeze_v6"
            ),
            "source_identity": REANCHOR_SOURCE_IDENTITY,
            "raw_pair_and_limit_artifacts_unchanged": True,
            "candidate_pair_audit": {
                "pair_count": 4729,
                "missing_0930_row_count": 3,
                "positive_liquidity_0930_row_count": 4671,
                "positive_liquidity_daily_open_mismatch_count": 0,
                "zero_liquidity_flat_0930_row_count": 55,
                "proven_preclose_placeholder_mismatch_count": 6,
                "strategy_return_opened": False,
            },
        }
        manifest["payload_sha256"] = canonical_payload_sha256(manifest)
        _write_json(transaction / "manifest.json", manifest)
        verify_capture(transaction)
        os.replace(transaction, output)
    except BaseException:
        if transaction.exists():
            shutil.rmtree(transaction)
        raise
    return verify_capture(output)


def verify_capture(output: Path) -> dict[str, Any]:
    output = output.resolve()
    _verify_calibration()
    plan = _derive_plan()
    stored_plan = json.loads((output / "capture-plan.json").read_text(encoding="utf-8"))
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    if stored_plan != plan or manifest.get("payload_sha256") != canonical_payload_sha256(manifest):
        raise ValueError("candidate minute final manifest/plan differs")
    if (
        manifest.get("schema_version") != 1
        or manifest.get("kind")
        != "factor_lab_13_0_candidate_minute_capture"
        or manifest.get("release") != RELEASE
        or manifest.get("status")
        != "candidate_minutes_captured_return_unopened"
        or manifest.get("protocol")
        != {
            "payload_sha256": PROTOCOL_PAYLOAD_SHA256,
            "file_sha256": PROTOCOL_FILE_SHA256,
        }
        or manifest.get("plan_payload_sha256") != plan["payload_sha256"]
        or manifest.get("pair_scope_payload_sha256")
        != PAIR_SCOPE_PAYLOAD_SHA256
        or manifest.get("target_payload_sha256") != TARGETS_PAYLOAD_SHA256
        or manifest.get("panel_file_sha256") != PANEL_FILE_SHA256
        or manifest.get("panel_payload_sha256") != PANEL_PAYLOAD_SHA256
        or manifest.get("unit_contract_id") != DIEMENG_UNIT_CONTRACT_ID
        or manifest.get("calibration_manifest_payload_sha256")
        != CALIBRATION_MANIFEST_PAYLOAD_SHA256
        or manifest.get("calibration_manifest_file_sha256")
        != CALIBRATION_MANIFEST_FILE_SHA256
        or manifest.get("calibration_unit_payload_sha256")
        != CALIBRATION_UNIT_PAYLOAD_SHA256
        or manifest.get("execution_level") != "1min"
        or manifest.get("execution_range")
        != (
            "09:30:00 opening-price evidence plus 09:31:00 through "
            "09:47:00 continuous execution evidence"
        )
        or manifest.get("pair_count") != len(plan["pairs"])
        or manifest.get("execution_count") != len(plan["executions"])
        or manifest.get("secret_in_artifact") is not False
        or manifest.get("claim_contract")
        != {"strategy_return_opened": False, "selection_opened": False}
        or not isinstance(manifest.get("artifacts"), list)
        or len(manifest["artifacts"])
        != len(plan["pairs"]) + len(plan["executions"])
    ):
        raise ValueError("candidate minute final manifest contract differs")
    reanchored = manifest.get("reanchored_from")
    if reanchored is not None and reanchored != {
        "reason_code": "pre_return_zero_liquidity_preclose_placeholder_refreeze_v6",
        "source_identity": REANCHOR_SOURCE_IDENTITY,
        "raw_pair_and_limit_artifacts_unchanged": True,
        "candidate_pair_audit": {
            "pair_count": 4729,
            "missing_0930_row_count": 3,
            "positive_liquidity_0930_row_count": 4671,
            "positive_liquidity_daily_open_mismatch_count": 0,
            "zero_liquidity_flat_0930_row_count": 55,
            "proven_preclose_placeholder_mismatch_count": 6,
            "strategy_return_opened": False,
        },
    }:
        raise ValueError("candidate minute reanchor receipt differs")
    pair_artifact_rows = [
        value
        for value in manifest["artifacts"]
        if value.get("role") == "minute_pair"
    ]
    pair_artifacts = {
        (
            str(value.get("identity", {}).get("execution_date")),
            str(value.get("identity", {}).get("ticker")),
        ): value
        for value in pair_artifact_rows
    }
    limit_artifact_rows = [
        value
        for value in manifest["artifacts"]
        if value.get("role") == "price_limits"
    ]
    limit_artifacts = {
        str(value.get("execution_date")): value
        for value in limit_artifact_rows
    }
    if (
        any(
            value.get("role") not in {"minute_pair", "price_limits"}
            for value in manifest["artifacts"]
        )
        or len(pair_artifacts) != len(pair_artifact_rows)
        or len(limit_artifacts) != len(limit_artifact_rows)
        or set(pair_artifacts)
        != {
            (str(pair["execution_date"]), str(pair["ticker"]))
            for pair in plan["pairs"]
        }
        or set(limit_artifacts)
        != {
            str(execution["execution_date"])
            for execution in plan["executions"]
        }
    ):
        raise ValueError("candidate minute final artifact scope differs")
    for pair in plan["pairs"]:
        root = _pair_dir(output, pair)
        receipt = _verify_pair(root, pair)
        artifact = pair_artifacts[
            (str(pair["execution_date"]), str(pair["ticker"]))
        ]
        if artifact != {
            "role": "minute_pair",
            "identity": pair,
            "receipt_payload_sha256": receipt["payload_sha256"],
            "receipt_file_sha256": file_sha256(root / "receipt.json"),
            "data_file_sha256": receipt["artifact"]["file_sha256"],
        }:
            raise ValueError("candidate minute manifest pair anchor differs")
    for execution in plan["executions"]:
        root = _limit_dir(output, execution["execution_date"])
        receipt = _verify_limits(
            root,
            date=execution["execution_date"],
            tickers=execution["tickers"],
        )
        if limit_artifacts[str(execution["execution_date"])] != {
            "role": "price_limits",
            "execution_date": execution["execution_date"],
            "receipt_payload_sha256": receipt["payload_sha256"],
            "receipt_file_sha256": file_sha256(root / "receipt.json"),
            "data_file_sha256": receipt["artifact"]["file_sha256"],
        }:
            raise ValueError("candidate minute manifest limit anchor differs")
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--max-new-pairs", type=int)
    parser.add_argument("--reanchor-from", type=Path)
    args = parser.parse_args(argv)
    if args.verify and args.reanchor_from is not None:
        parser.error("--verify and --reanchor-from are mutually exclusive")
    if args.reanchor_from is not None and (
        args.resume or args.max_new_pairs is not None
    ):
        parser.error(
            "--reanchor-from cannot be combined with capture/resume options"
        )
    if args.verify:
        result = verify_capture(args.output)
    elif args.reanchor_from is not None:
        result = reanchor_capture(args.reanchor_from, args.output)
    else:
        result = capture(
            args.output,
            resume=args.resume,
            max_new_pairs=args.max_new_pairs,
        )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
