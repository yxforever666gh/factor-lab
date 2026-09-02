#!/usr/bin/env python
"""Create-only Diemeng minute unit/pagination/bar-label calibration."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import sys
import uuid

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
while str(SRC) in sys.path:
    sys.path.remove(str(SRC))
sys.path.insert(0, str(SRC))

from factor_lab.data.diemeng_minutes import (  # noqa: E402
    DIEMENG_UNIT_CONTRACT_ID,
    DiemengMinuteHTTPClient,
    audit_diemeng_execution_slice,
    capture_diemeng_minutes,
    freeze_diemeng_unit_contract,
)
from factor_lab.release_integrity import (  # noqa: E402
    canonical_payload_sha256,
    file_sha256,
)


SAMPLES = (
    ("000001.SZ", "2017-10-09"),
    ("600000.SH", "2017-10-09"),
    ("000001.SZ", "2019-01-02"),
    ("002203.SZ", "2019-07-01"),
    ("600064.SH", "2019-07-01"),
    ("002714.SZ", "2019-10-08"),
    ("300630.SZ", "2020-07-01"),
    ("300750.SZ", "2021-01-04"),
    ("601318.SH", "2021-01-04"),
    ("000001.SZ", "2022-12-30"),
    ("600000.SH", "2022-12-30"),
)
DEFAULT_OUTPUT = Path(
    r"H:\Download\FactorLabPytest\factor-lab-13.0-diemeng-unit-regimes-v5"
)


def _read_checkpoint() -> dict:
    value = json.loads(
        (ROOT / "runtime/data/raw/checkpoint.json").read_text(encoding="utf-8")
    )
    if value.get("schema_version") != 1 or not isinstance(
        value.get("partitions"), dict
    ):
        raise ValueError("raw checkpoint contract differs")
    return value


def _daily_row(checkpoint: dict, *, ticker: str, date: str) -> tuple[pd.Series, dict]:
    entry = checkpoint["partitions"].get(f"daily/{date}")
    if not isinstance(entry, dict) or entry.get("status") != "complete":
        raise ValueError("calibration daily partition is absent")
    path = Path(str(entry["path"])).resolve()
    if (
        not path.is_file()
        or path.stat().st_size != int(entry["size_bytes"])
        or file_sha256(path) != str(entry["sha256"])
    ):
        raise ValueError("calibration daily partition hash/size differs")
    frame = pd.read_parquet(path)
    rows = frame.loc[frame["ts_code"].astype(str).eq(ticker)]
    if len(rows) != 1:
        raise ValueError("calibration daily ticker row differs")
    receipt = {
        "path": path.relative_to(ROOT).as_posix(),
        "file_sha256": str(entry["sha256"]),
        "size_bytes": int(entry["size_bytes"]),
        "row_count": int(entry["row_count"]),
    }
    return rows.iloc[0], receipt


def _stable_receipt(value: dict) -> dict:
    result = dict(value)
    result.pop("captured_at_utc", None)
    return result


def _fsync(path: Path) -> None:
    with path.open("r+b") as handle:
        os.fsync(handle.fileno())


def capture_contract(
    output: Path,
    *,
    request_rate_per_minute: float = 60.0,
    page_size: int = 512,
) -> dict:
    output = output.resolve()
    if output.exists():
        raise FileExistsError(f"create-only Diemeng output exists: {output}")
    key_path = ROOT / "runtime/secrets/settings/diemeng_api_key"
    api_key = key_path.read_text(encoding="utf-8").strip()
    client = DiemengMinuteHTTPClient(
        api_key, request_rate_per_minute=request_rate_per_minute
    )
    checkpoint = _read_checkpoint()
    transaction = output.parent / f".{output.name}.tmp-{uuid.uuid4().hex}"
    transaction.mkdir(parents=True, exist_ok=False)
    sample_rows = []
    audits = []
    try:
        for ticker, date in SAMPLES:
            daily, daily_receipt = _daily_row(
                checkpoint, ticker=ticker, date=date
            )
            captures = []
            sample_audits = []
            files = []
            for sample_name in ("first", "second"):
                build = capture_diemeng_minutes(
                    client,
                    ticker=ticker,
                    level="1min",
                    start_time=f"{date} 09:30:00",
                    end_time=f"{date} 09:47:00",
                    page_size=page_size,
                )
                audit = audit_diemeng_execution_slice(
                    build.frame,
                    ticker=ticker,
                    session=date,
                    daily_open=float(daily["open"]),
                )
                filename = f"{date}-{ticker.replace('.', '-')}-{sample_name}.parquet"
                path = transaction / filename
                build.frame.to_parquet(path, index=False)
                _fsync(path)
                captures.append(build)
                sample_audits.append(audit)
                files.append(
                    {
                        "sample": sample_name,
                        "path": filename,
                        "file_sha256": file_sha256(path),
                        "size_bytes": path.stat().st_size,
                        "capture_receipt": build.receipt,
                    }
                )
            pd.testing.assert_frame_equal(
                captures[0].frame,
                captures[1].frame,
                check_exact=True,
                check_dtype=True,
                check_like=False,
            )
            if (
                _stable_receipt(captures[0].receipt)
                != _stable_receipt(captures[1].receipt)
                or sample_audits[0] != sample_audits[1]
            ):
                raise ValueError("Diemeng stable sample differs")
            audits.append(sample_audits[0])
            sample_rows.append(
                {
                    "ticker": ticker,
                    "session": date,
                    "daily_source": daily_receipt,
                    "stable_exact": True,
                    "audit": sample_audits[0],
                    "files": files,
                }
            )
        contract = freeze_diemeng_unit_contract(audits)
        manifest = {
            "schema_version": 1,
            "kind": "factor_lab_13_0_diemeng_minute_contract",
            "status": "per_slice_unit_and_1min_bar_label_calibrated_return_unopened",
            "release": "13.0",
            "endpoint": "https://data.diemeng.chat/api/stock/history",
            "secret_in_artifact": False,
            "sample_count": len(sample_rows),
            "samples": sample_rows,
            "unit_contract": contract,
            "claim_contract": {
                "strategy_return_opened": False,
                "exact_open_auction_fill_proven": False,
                "minute_window_execution_allowed_after_protocol_refreeze": True,
            },
        }
        manifest["payload_sha256"] = canonical_payload_sha256(manifest)
        manifest_path = transaction / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False)
            + "\n",
            encoding="utf-8",
        )
        _fsync(manifest_path)
        os.replace(transaction, output)
    except BaseException:
        if transaction.exists():
            shutil.rmtree(transaction)
        raise
    return verify_contract(output)


def verify_contract(output: Path) -> dict:
    output = output.resolve()
    manifest_path = output / "manifest.json"
    value = json.loads(manifest_path.read_text(encoding="utf-8"))
    if value.get("payload_sha256") != canonical_payload_sha256(value):
        raise ValueError("Diemeng contract manifest payload differs")
    if (
        value.get("unit_contract", {}).get("contract_id")
        != DIEMENG_UNIT_CONTRACT_ID
        or value.get("sample_count") != len(SAMPLES)
    ):
        raise ValueError("Diemeng unit contract identity differs")
    for sample in value["samples"]:
        frames = []
        for entry in sample["files"]:
            path = output / entry["path"]
            if (
                not path.is_file()
                or path.stat().st_size != int(entry["size_bytes"])
                or file_sha256(path) != entry["file_sha256"]
            ):
                raise ValueError("Diemeng calibration artifact differs")
            frames.append(pd.read_parquet(path))
        pd.testing.assert_frame_equal(
            frames[0], frames[1], check_exact=True, check_dtype=True
        )
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--request-rate-per-minute", type=float, default=60.0)
    parser.add_argument("--page-size", type=int, default=512)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args(argv)
    result = (
        verify_contract(args.output)
        if args.verify
        else capture_contract(
            args.output,
            request_rate_per_minute=args.request_rate_per_minute,
            page_size=args.page_size,
        )
    )
    print(
        json.dumps(
            {
                "output": str(args.output.resolve()),
                "payload_sha256": result["payload_sha256"],
                "unit_contract_payload_sha256": result["unit_contract"][
                    "payload_sha256"
                ],
                "sample_count": result["sample_count"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
