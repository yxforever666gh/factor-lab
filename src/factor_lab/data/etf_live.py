"""Stable, append-safe publication for prospective multi-asset ETF stages.

The underlying :func:`capture_multi_asset_stage` deliberately performs one
provider snapshot.  This module adds only the local transaction around it:
two independent snapshots must agree exactly and their historical prefix must
equal an already verified baseline before either snapshot can be published.

Provider publication-time policy belongs to the caller.  In particular, this
module neither creates a network client nor sleeps until an end-of-day gate.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import tempfile
from typing import Any, Callable, Mapping

import pandas as pd

from .etf_assets import (
    CALENDAR_COLUMNS,
    ETF_TICKERS,
    HISTORY_COLUMNS,
    MultiAssetStage,
    capture_multi_asset_stage,
    load_multi_asset_stage,
)
from ..release_integrity import canonical_payload_sha256


def _date(value: str, *, field: str) -> pd.Timestamp:
    text = str(value).strip()
    if re.fullmatch(r"\d{8}", text):
        parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        parsed = pd.to_datetime(text, format="%Y-%m-%d", errors="coerce")
    else:
        raise ValueError(f"{field} must be YYYY-MM-DD or YYYYMMDD")
    if pd.isna(parsed):
        raise ValueError(f"{field} is not a valid date")
    return pd.Timestamp(parsed).normalize()


def _stage_name(value: str) -> str:
    stage = str(value).strip()
    if (
        not stage
        or stage in {".", ".."}
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", stage)
    ):
        raise ValueError("stage_name must be a safe non-empty identifier")
    return stage


def _manifest_date(
    manifest: Mapping[str, Any], key: str, *, role: str
) -> pd.Timestamp:
    value = manifest.get(key)
    if not isinstance(value, str):
        raise ValueError(f"{role} manifest lacks {key}")
    return _date(value, field=f"{role} {key}")


def _assert_stage_identity(
    stage: MultiAssetStage,
    *,
    expected_stage: str,
    expected_start: pd.Timestamp,
    expected_end: pd.Timestamp,
    role: str,
) -> None:
    manifest = stage.manifest
    if (
        manifest.get("stage") != expected_stage
        or _manifest_date(manifest, "price_start_date", role=role)
        != expected_start
        or _manifest_date(manifest, "price_end_date", role=role) != expected_end
    ):
        raise ValueError(f"{role} stage identity differs from the request")
    if set(stage.assets) != set(ETF_TICKERS):
        raise ValueError(f"{role} stage does not contain the fixed six ETFs")


def _frame_exact(left: pd.DataFrame, right: pd.DataFrame, *, role: str) -> None:
    try:
        pd.testing.assert_frame_equal(
            left,
            right,
            check_exact=True,
            check_dtype=True,
            check_index_type=True,
            check_column_type=True,
            check_frame_type=True,
            check_names=True,
            check_like=False,
        )
    except AssertionError as exc:
        raise ValueError(f"{role} differs") from exc


def _prefix(
    frame: pd.DataFrame,
    *,
    end: pd.Timestamp,
    columns: tuple[str, ...],
    role: str,
) -> pd.DataFrame:
    if tuple(map(str, frame.columns)) != columns:
        raise ValueError(f"{role} columns differ from the stage contract")
    dates = pd.to_datetime(frame["trade_date"], errors="coerce")
    if dates.isna().any():
        raise ValueError(f"{role} contains an invalid trade_date")
    return frame.loc[dates.dt.normalize().le(end), list(columns)].reset_index(drop=True)


def _assert_samples_exact(first: MultiAssetStage, second: MultiAssetStage) -> None:
    if dict(first.manifest) != dict(second.manifest):
        raise ValueError("stable multi-asset capture manifests differ")
    _frame_exact(first.calendar, second.calendar, role="stable capture calendar")
    if set(first.assets) != set(ETF_TICKERS) or set(second.assets) != set(ETF_TICKERS):
        raise ValueError("stable capture does not contain the fixed six ETFs")
    for ticker in ETF_TICKERS:
        _frame_exact(
            first.assets[ticker],
            second.assets[ticker],
            role=f"stable capture asset {ticker}",
        )


def _assert_baseline_prefix(
    candidate: MultiAssetStage, baseline: MultiAssetStage
) -> None:
    baseline_end = _manifest_date(
        baseline.manifest, "price_end_date", role="baseline"
    )
    if set(baseline.assets) != set(ETF_TICKERS):
        raise ValueError("baseline stage does not contain the fixed six ETFs")
    _frame_exact(
        _prefix(
            candidate.calendar,
            end=baseline_end,
            columns=CALENDAR_COLUMNS,
            role="candidate calendar",
        ),
        _prefix(
            baseline.calendar,
            end=baseline_end,
            columns=CALENDAR_COLUMNS,
            role="baseline calendar",
        ),
        role="candidate calendar baseline prefix",
    )
    for ticker in ETF_TICKERS:
        _frame_exact(
            _prefix(
                candidate.assets[ticker],
                end=baseline_end,
                columns=HISTORY_COLUMNS,
                role=f"candidate asset {ticker}",
            ),
            _prefix(
                baseline.assets[ticker],
                end=baseline_end,
                columns=HISTORY_COLUMNS,
                role=f"baseline asset {ticker}",
            ),
            role=f"candidate asset {ticker} baseline prefix",
        )


def _assert_publication_receipt(
    stage: MultiAssetStage, expected: Mapping[str, Any]
) -> None:
    actual = stage.manifest.get("stable_capture_receipt")
    if not isinstance(actual, Mapping):
        raise ValueError("published stage lacks the exact stable-capture receipt")
    manifest_without_receipt = dict(stage.manifest)
    manifest_without_receipt.pop("stable_capture_receipt", None)
    capture_payload = canonical_payload_sha256(manifest_without_receipt)
    expected_receipt = dict(expected)
    expected_receipt.setdefault(
        "canonical_capture_payload_sha256", capture_payload
    )
    if (
        dict(actual) != expected_receipt
        or actual.get("canonical_capture_payload_sha256") != capture_payload
    ):
        raise ValueError("published stage stable-capture receipt differs")


def _attach_publication_receipt(
    stage: MultiAssetStage, receipt: Mapping[str, Any]
) -> MultiAssetStage:
    manifest = dict(stage.manifest)
    manifest["stable_capture_receipt"] = dict(receipt)
    manifest["payload_sha256"] = canonical_payload_sha256(manifest)
    manifest_path = stage.path / "manifest.json"
    manifest_path.write_bytes(
        json.dumps(
            manifest,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
    )
    return load_multi_asset_stage(stage.path.parent, str(manifest["stage"]))


def stable_capture_multi_asset_stage(
    client: Any,
    destination_root: str | Path,
    start: str,
    as_of: str,
    stage_name: str,
    baseline_stage: MultiAssetStage,
    validator: Callable[[MultiAssetStage], None] | None = None,
    publication_receipt: Mapping[str, Any] | None = None,
    publication_receipt_factory: (
        Callable[[MultiAssetStage], Mapping[str, Any]] | None
    ) = None,
) -> MultiAssetStage:
    """Capture, compare and atomically publish one stable prospective stage.

    If the destination already exists, it is deeply loaded and checked against
    the requested identity and baseline without touching ``client``.  A fresh
    destination is published only after two captures in independent temporary
    roots have identical manifests, calendars and six asset frames.
    """

    name = _stage_name(stage_name)
    start_date = _date(start, field="start")
    as_of_date = _date(as_of, field="as_of")
    baseline_start = _manifest_date(
        baseline_stage.manifest, "price_start_date", role="baseline"
    )
    baseline_end = _manifest_date(
        baseline_stage.manifest, "price_end_date", role="baseline"
    )
    if publication_receipt is not None and publication_receipt_factory is not None:
        raise ValueError("publication receipt and receipt factory are mutually exclusive")
    if start_date != baseline_start:
        raise ValueError("start must equal the baseline price_start_date")
    if as_of_date < baseline_end:
        raise ValueError("as_of must not precede the baseline price_end_date")

    root = Path(destination_root).expanduser().resolve()
    destination = root / f"stage={name}"
    if destination.exists() or destination.is_symlink():
        if publication_receipt_factory is not None:
            raise ValueError("an existing stage requires its exact frozen publication receipt")
        existing = load_multi_asset_stage(root, name)
        _assert_stage_identity(
            existing,
            expected_stage=name,
            expected_start=start_date,
            expected_end=as_of_date,
            role="existing published",
        )
        _assert_baseline_prefix(existing, baseline_stage)
        if publication_receipt is not None:
            _assert_publication_receipt(existing, publication_receipt)
        if validator is not None:
            validator(existing)
        return existing

    root.mkdir(parents=True, exist_ok=True)
    transaction = Path(
        tempfile.mkdtemp(prefix=f".stable-capture-{name}-", dir=root)
    )
    published = False
    try:
        first = capture_multi_asset_stage(
            client, transaction / "sample-a", start, as_of, name
        )
        second = capture_multi_asset_stage(
            client, transaction / "sample-b", start, as_of, name
        )
        for role, stage in (("first capture", first), ("second capture", second)):
            _assert_stage_identity(
                stage,
                expected_stage=name,
                expected_start=start_date,
                expected_end=as_of_date,
                role=role,
            )
        _assert_samples_exact(first, second)
        _assert_baseline_prefix(first, baseline_stage)
        _assert_baseline_prefix(second, baseline_stage)
        if validator is not None:
            validator(first)
            validator(second)
        frozen_receipt = (
            dict(publication_receipt_factory(first))
            if publication_receipt_factory is not None
            else dict(publication_receipt)
            if publication_receipt is not None
            else None
        )
        if frozen_receipt is not None:
            capture_payload = canonical_payload_sha256(first.manifest)
            if frozen_receipt.setdefault(
                "canonical_capture_payload_sha256", capture_payload
            ) != capture_payload:
                raise ValueError("publication receipt capture payload differs")
            first = _attach_publication_receipt(first, frozen_receipt)

        os.rename(first.path, destination)
        published = True
        result = load_multi_asset_stage(root, name)
        _assert_stage_identity(
            result,
            expected_stage=name,
            expected_start=start_date,
            expected_end=as_of_date,
            role="published",
        )
        _assert_baseline_prefix(result, baseline_stage)
        if frozen_receipt is not None:
            _assert_publication_receipt(result, frozen_receipt)
        if validator is not None:
            validator(result)
        return result
    except BaseException:
        if published and (destination.exists() or destination.is_symlink()):
            if destination.is_symlink() or destination.is_file():
                destination.unlink()
            else:
                shutil.rmtree(destination)
        raise
    finally:
        if transaction.exists():
            shutil.rmtree(transaction)


__all__ = ["stable_capture_multi_asset_stage"]
