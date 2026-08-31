#!/usr/bin/env python
"""Run one minimal prospective 11.2 quarterly signal or outcome cycle."""

from __future__ import annotations

import argparse
from datetime import datetime, time, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Callable, Mapping, Sequence
import uuid
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
while str(SRC) in sys.path:
    sys.path.remove(str(SRC))
sys.path.insert(0, str(SRC))

from factor_lab.data.etf_assets import MultiAssetStage, load_multi_asset_stage  # noqa: E402
from factor_lab.data.catalog import RuntimeLayout, load_data_config  # noqa: E402
from factor_lab.data.etf_live import (  # noqa: E402
    RateLimitedRetryingClient,
    stable_capture_multi_asset_stage,
)
from factor_lab.data.sources import _configured_tushare_client  # noqa: E402
from factor_lab.release_integrity import canonical_payload_sha256, file_sha256  # noqa: E402
from factor_lab.research.multi_asset import (  # noqa: E402
    ALL_CODES,
    QUARTERLY_DUAL_CONFIRM_BLEND_ID,
    SimulationConfig,
    build_monthly_targets,
    simulate_targets,
)


RELEASE = "11.2"
ROUTE = "quarterly_dual_confirm_top3_borda_blend_75_25"
PROTOCOL_ID = "factor-lab/11.2/quarterly-prospective-cycle-v1"
PROTOCOL_PATH = Path("protocols/11.2-quarterly-prospective-cycle.json")
PROTOCOL_PAYLOAD = "b9da758aad617d8752f9dbc628f8421fe4c04fe26f9f2a677fee1a8797b50e08"
PROTOCOL_FILE_SHA256 = "d363ae60326b17d3b28c04201f1ab411df544b2e16f0fe93e7fba30010c728a6"
DEFAULT_RUNTIME_ROOT = ROOT / "runtime" / "prospective" / "11.2"
GENESIS_SOURCE_ROOT = ROOT / "runtime" / "data" / "multi-asset-9.0" / "sources"
GENESIS_MANIFEST_PAYLOAD = "050ad4ddcb86dc4fbc71befad54c400b48a44f72ab6fecc33936b6da0c8f9aff"
GENESIS_MANIFEST_FILE_SHA256 = "cdbf8ba498142adff04216b476522f47ee18df6f0fa02f3395d0e141191adbfa"
SHANGHAI = ZoneInfo("Asia/Shanghai")
SIGNAL_READY_TIME = time(17, 10)
DECISION_DEADLINE = time(9, 15)
SOURCE_RECEIPT_CONTRACT = "factor-lab/11.2/stable-source-v1"
REQUEST_RATE_PER_MINUTE = 300.0
MAX_PROVIDER_ATTEMPTS = 3
PLAN_FIELDS = (
    "code",
    "side",
    "target_weight",
    "signal_price",
    "signal_adv20_rmb",
    "capacity_rmb",
    "pre_signal_shares",
    "desired_shares",
    "requested_delta_shares",
    "requested_shares",
    "capacity_capped_shares",
    "planned_delta_shares",
    "planned_shares",
    "requested_signal_notional",
    "planned_signal_notional",
    "capacity_limited_shares",
    "capacity_limited_signal_notional",
    "capacity_limited",
)
UNIT_SCALE_PLAN_FIELDS = {
    "pre_signal_shares",
    "desired_shares",
    "requested_delta_shares",
    "requested_shares",
    "capacity_capped_shares",
    "planned_delta_shares",
    "planned_shares",
    "capacity_limited_shares",
}
DECISION_FIELDS = {
    "schema_version", "kind", "release", "mode", "cycle_id", "strategy_id",
    "signal_date", "execution_date", "recorded_at_utc", "protocol_payload_sha256",
    "source", "predecessor_outcome_payload_sha256", "targets", "signal_close_nav",
    "signal_close_cash", "signal_close_holdings", "sealed_order_plan", "payload_sha256",
}
OUTCOME_FIELDS = {
    "schema_version", "kind", "release", "mode", "cycle_id", "strategy_id",
    "signal_date", "execution_date", "outcome_date", "recorded_at_utc",
    "protocol_payload_sha256", "decision_payload_sha256", "source",
    "sealed_plan_exact", "start_nav", "end_nav", "net_return", "daily_nav",
    "trades", "terminal_holdings", "maximum_reconciliation_error", "payload_sha256",
}


def _date(value: str, *, field: str) -> pd.Timestamp:
    text = str(value).strip()
    parsed = pd.to_datetime(text, format="%Y-%m-%d", errors="coerce")
    if pd.isna(parsed) or not text or len(text) != 10:
        raise ValueError(f"{field} must be an exact YYYY-MM-DD date")
    return pd.Timestamp(parsed).normalize()


def _aware_datetime(value: Any, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO-8601 datetime") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware")
    return parsed


def _read_protocol() -> dict[str, Any]:
    path = ROOT / PROTOCOL_PATH
    value = json.loads(path.read_text(encoding="utf-8"))
    if (
        value.get("payload_sha256") != PROTOCOL_PAYLOAD
        or canonical_payload_sha256(value) != PROTOCOL_PAYLOAD
        or file_sha256(path) != PROTOCOL_FILE_SHA256
        or value.get("protocol_id") != PROTOCOL_ID
        or value.get("release") != RELEASE
        or value.get("route") != ROUTE
        or value.get("frozen_strategy", {}).get("strategy_id") != QUARTERLY_DUAL_CONFIRM_BLEND_ID
    ):
        raise ValueError("11.2 prospective protocol differs")
    return value


def _require_release_tag() -> dict[str, str]:
    tag = "refs/tags/11.2"
    tag_type = subprocess.run(
        ["git", "cat-file", "-t", tag], cwd=ROOT, check=False,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", tag, "HEAD"],
        cwd=ROOT, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=False,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    tagged = subprocess.run(
        ["git", "rev-parse", f"{tag}^{{}}"], cwd=ROOT, check=False,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    tag_object = subprocess.run(
        ["git", "rev-parse", tag], cwd=ROOT, check=False,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    dirty = subprocess.run(
        ["git", "status", "--porcelain"], cwd=ROOT, check=False,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    remote = subprocess.run(
        ["git", "ls-remote", "--exit-code", "origin", tag, f"{tag}^{{}}"],
        cwd=ROOT, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    remote_refs = {
        ref: object_id
        for object_id, ref in (line.split() for line in remote.stdout.splitlines())
    }
    if (
        tag_type.returncode != 0
        or tag_type.stdout.strip() != "tag"
        or ancestor.returncode != 0
        or head.returncode != 0
        or tagged.returncode != 0
        or tag_object.returncode != 0
        or dirty.returncode != 0
        or bool(dirty.stdout.strip())
        or remote.returncode != 0
        or remote_refs.get(tag) != tag_object.stdout.strip()
        or remote_refs.get(f"{tag}^{{}}") != tagged.stdout.strip()
        or head.stdout.strip() != tagged.stdout.strip()
    ):
        raise RuntimeError(
            "prospective 11.2 cycle requires checkout of the published annotated 11.2 tag"
        )
    return {
        "annotated_tag_object": tag_object.stdout.strip(),
        "peeled_commit": tagged.stdout.strip(),
    }


def _formal_stage_entries(root: Path, *, before: pd.Timestamp) -> list[tuple[pd.Timestamp, str]]:
    entries: list[tuple[pd.Timestamp, str]] = []
    if not root.exists():
        return entries
    for path in root.glob("stage=asof-*"):
        match = re.fullmatch(r"stage=asof-(\d{8})", path.name)
        if match is None:
            raise ValueError(f"formal source stage name differs: {path}")
        value = pd.to_datetime(match.group(1), format="%Y%m%d", errors="raise").normalize()
        if value < before:
            entries.append((value, path.name.split("=", 1)[1]))
    return sorted(entries)


def _source_receipt_base(
    root: Path,
    stage_name: str,
    as_of: pd.Timestamp,
    baseline: MultiAssetStage,
    release_identity: Mapping[str, str],
) -> dict[str, Any]:
    baseline_manifest = baseline.path / "manifest.json"
    return {
        "schema_version": 1,
        "contract_id": SOURCE_RECEIPT_CONTRACT,
        "release": RELEASE,
        "release_tag": RELEASE,
        "release_annotated_tag_object": release_identity["annotated_tag_object"],
        "release_peeled_commit": release_identity["peeled_commit"],
        "protocol_payload_sha256": PROTOCOL_PAYLOAD,
        "formal_source_root": str(root.resolve()),
        "stage": stage_name,
        "price_end_date": as_of.date().isoformat(),
        "full_capture_count": 2,
        "independent_complete_provider_pulls": True,
        "canonical_payloads_match_exactly": True,
        "baseline_manifest_path": str(baseline_manifest.resolve()),
        "baseline_manifest_file_sha256": file_sha256(baseline_manifest),
        "baseline_manifest_payload_sha256": baseline.manifest["payload_sha256"],
    }


def _validate_source_receipt(
    stage: MultiAssetStage,
    root: Path,
    stage_name: str,
    as_of: pd.Timestamp,
    release_identity: Mapping[str, str] | None,
) -> None:
    previous = _formal_stage_entries(root, before=as_of)
    if previous:
        previous_date, previous_name = previous[-1]
        baseline = load_multi_asset_stage(root, previous_name)
        _validate_source_receipt(
            baseline, root, previous_name, previous_date, release_identity
        )
    else:
        baseline = load_multi_asset_stage(GENESIS_SOURCE_ROOT, "audit")
        baseline_manifest = baseline.path / "manifest.json"
        if (
            baseline.manifest.get("payload_sha256") != GENESIS_MANIFEST_PAYLOAD
            or file_sha256(baseline_manifest) != GENESIS_MANIFEST_FILE_SHA256
        ):
            raise ValueError("11.2 genesis source differs from the frozen 9.0 baseline")
    baseline_end = _date(
        str(baseline.manifest["price_end_date"]), field="baseline price_end_date"
    )
    _assert_source_prefix(stage, baseline, baseline_end)
    actual = stage.manifest.get("stable_capture_receipt")
    if not isinstance(actual, Mapping):
        raise ValueError("formal source lacks its stable-capture receipt")
    receipt_release_identity = {
        "annotated_tag_object": str(actual.get("release_annotated_tag_object") or ""),
        "peeled_commit": str(actual.get("release_peeled_commit") or ""),
    }
    if (
        any(re.fullmatch(r"[0-9a-f]{40}", value) is None for value in receipt_release_identity.values())
        or release_identity is not None
        and receipt_release_identity != dict(release_identity)
    ):
        raise ValueError("formal source release tag identity differs")
    expected = _source_receipt_base(
        root, stage_name, as_of, baseline, receipt_release_identity
    )
    recorded_text = actual.get("validated_at_utc")
    try:
        recorded = datetime.fromisoformat(str(recorded_text))
    except ValueError as exc:
        raise ValueError("formal source receipt has an invalid validation time") from exc
    if recorded.tzinfo is None or recorded.utcoffset() != timedelta(0):
        raise ValueError("formal source receipt validation time must be UTC")
    manifest_without_receipt = dict(stage.manifest)
    manifest_without_receipt.pop("stable_capture_receipt", None)
    capture_payload = canonical_payload_sha256(manifest_without_receipt)
    expected_with_time = {
        **expected,
        "validated_at_utc": recorded_text,
        "canonical_capture_payload_sha256": capture_payload,
    }
    if dict(actual) != expected_with_time:
        raise ValueError("formal source stable-capture receipt differs")
    execution_date = _require_quarter_end(_sessions(stage), as_of)
    _signal_window(as_of, execution_date, recorded.astimezone(SHANGHAI))


def _load_source(
    root: Path,
    stage_name: str,
    as_of: pd.Timestamp,
    release_identity: Mapping[str, str] | None = None,
) -> MultiAssetStage:
    expected_name = f"asof-{as_of:%Y%m%d}"
    if stage_name != expected_name:
        raise ValueError("source stage name must exactly encode the requested as_of date")
    stage = load_multi_asset_stage(root, stage_name)
    if (
        stage.manifest.get("stage") != stage_name
        or stage.manifest.get("price_end_date") != as_of.date().isoformat()
    ):
        raise ValueError("source price_end_date must equal the requested as_of date")
    if set(stage.assets) != set(ALL_CODES):
        raise ValueError("source must contain the exact fixed six-ETF universe")
    for code, frame in stage.assets.items():
        dates = pd.to_datetime(frame["trade_date"], errors="raise").dt.normalize()
        if dates.max() != as_of or bool(dates.gt(as_of).any()):
            raise ValueError(f"source asset cutoff differs for {code}")
    _validate_source_receipt(stage, root, stage_name, as_of, release_identity)
    return stage


def _sessions(stage: MultiAssetStage) -> tuple[pd.Timestamp, ...]:
    return tuple(pd.to_datetime(stage.calendar["trade_date"], errors="raise").dt.normalize())


def _next_session(sessions: Sequence[pd.Timestamp], value: pd.Timestamp) -> pd.Timestamp:
    try:
        index = list(sessions).index(value)
        return sessions[index + 1]
    except (ValueError, IndexError) as exc:
        raise ValueError("official calendar lacks the immediate next session") from exc


def _quarter(value: pd.Timestamp) -> tuple[int, int]:
    return value.year, (value.month - 1) // 3 + 1


def _require_quarter_end(
    sessions: Sequence[pd.Timestamp], value: pd.Timestamp
) -> pd.Timestamp:
    following = _next_session(sessions, value)
    if _quarter(value) == _quarter(following):
        raise ValueError("as_of is not the last official session of its quarter")
    return following


def _next_quarter_end(
    sessions: Sequence[pd.Timestamp], signal_date: pd.Timestamp
) -> pd.Timestamp:
    for value in sessions:
        if value <= signal_date:
            continue
        try:
            if _quarter(value) != _quarter(_next_session(sessions, value)):
                return value
        except ValueError:
            break
    raise ValueError("calendar does not identify the next quarter-end outcome date")


def _local_now(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if value.tzinfo is None:
        raise ValueError("clock must return a timezone-aware datetime")
    return value.astimezone(SHANGHAI)


def _signal_window(
    signal_date: pd.Timestamp, execution_date: pd.Timestamp, now: datetime
) -> None:
    opens = datetime.combine(signal_date.date(), SIGNAL_READY_TIME, SHANGHAI)
    deadline = datetime.combine(execution_date.date(), DECISION_DEADLINE, SHANGHAI)
    if now < opens:
        raise RuntimeError("signal source is not eligible before the 17:10 close gate")
    if now >= deadline:
        raise RuntimeError("prospective decision window was missed and cannot be backfilled")


def _outcome_window(outcome_date: pd.Timestamp, now: datetime) -> None:
    ready = datetime.combine(outcome_date.date(), SIGNAL_READY_TIME, SHANGHAI)
    if now < ready:
        raise RuntimeError("outcome is not complete before the quarter-end 17:10 gate")


def _json_value(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return None if not math.isfinite(float(value)) else float(value)
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    return value


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {str(key): _json_value(value) for key, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


def _target_frame(records: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame([dict(value) for value in records])
    for column in (
        "signal_date",
        "execution_date",
        "momentum_start_date",
        "short_momentum_start_date",
        "momentum_end_date",
    ):
        if column in frame:
            frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.normalize()
    return frame


def _payload(value: dict[str, Any]) -> dict[str, Any]:
    value["payload_sha256"] = canonical_payload_sha256(value)
    return value


def _read_artifact(path: Path, *, kind: str) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    fields = DECISION_FIELDS if kind == "factor_lab_11_2_quarterly_decision" else OUTCOME_FIELDS
    source = value.get("source")
    if (
        value.get("kind") != kind
        or set(value) != fields
        or value.get("payload_sha256") != canonical_payload_sha256(value)
        or value.get("schema_version") != 1
        or value.get("release") != RELEASE
        or value.get("mode") != "prospective_quarterly_cycle"
        or value.get("strategy_id") != QUARTERLY_DUAL_CONFIRM_BLEND_ID
        or value.get("protocol_payload_sha256") != PROTOCOL_PAYLOAD
        or not isinstance(source, Mapping)
        or set(source)
        != {
            "root", "stage", "path", "manifest_file_sha256",
            "manifest_payload_sha256", "price_end_date",
        }
        or re.fullmatch(r"[0-9a-f]{64}", str(source.get("manifest_file_sha256") or "")) is None
        or re.fullmatch(r"[0-9a-f]{64}", str(source.get("manifest_payload_sha256") or "")) is None
    ):
        raise ValueError(f"prospective artifact differs: {path}")
    signal = _date(value["signal_date"], field="artifact signal_date")
    if value.get("cycle_id") != _cycle_id(signal):
        raise ValueError(f"prospective artifact cycle identity differs: {path}")
    execution = _date(value["execution_date"], field="execution_date")
    recorded = _aware_datetime(value["recorded_at_utc"], field="recorded_at_utc")
    if recorded.utcoffset() != timedelta(0):
        raise ValueError(f"prospective artifact recorded_at_utc is not UTC: {path}")
    if kind == "factor_lab_11_2_quarterly_decision":
        targets = value.get("targets")
        holdings = value.get("signal_close_holdings")
        if (
            execution <= signal
            or not (
                datetime.combine(signal.date(), SIGNAL_READY_TIME, SHANGHAI)
                <= recorded.astimezone(SHANGHAI)
                < datetime.combine(execution.date(), DECISION_DEADLINE, SHANGHAI)
            )
            or source.get("price_end_date") != signal.date().isoformat()
            or not isinstance(targets, list)
            or len(targets) != len(ALL_CODES)
            or {row.get("code") for row in targets} != set(ALL_CODES)
            or any(
                _date(row.get("signal_date"), field="target signal_date") != signal
                or _date(row.get("execution_date"), field="target execution_date")
                != execution
                for row in targets
            )
            or not isinstance(holdings, list)
            or len(holdings) != len(ALL_CODES)
            or {row.get("code") for row in holdings} != set(ALL_CODES)
            or any(
                _date(row.get("trade_date"), field="holding trade_date") != signal
                for row in holdings
            )
            or not isinstance(value.get("sealed_order_plan"), list)
            or any(set(row) != set(PLAN_FIELDS) for row in value["sealed_order_plan"])
            or not math.isfinite(float(value.get("signal_close_nav", math.nan)))
            or float(value["signal_close_nav"]) <= 0.0
            or not math.isfinite(float(value.get("signal_close_cash", math.nan)))
        ):
            raise ValueError(f"prospective decision contract differs: {path}")
    else:
        outcome = _date(value["outcome_date"], field="outcome_date")
        daily_nav = value.get("daily_nav")
        terminal = value.get("terminal_holdings")
        start_nav = float(value.get("start_nav", math.nan))
        end_nav = float(value.get("end_nav", math.nan))
        net_return = float(value.get("net_return", math.nan))
        reconciliation = float(value.get("maximum_reconciliation_error", math.nan))
        if (
            outcome <= signal
            or execution <= signal
            or recorded.astimezone(SHANGHAI)
            < datetime.combine(outcome.date(), SIGNAL_READY_TIME, SHANGHAI)
            or source.get("price_end_date") != outcome.date().isoformat()
            or value.get("sealed_plan_exact") is not True
            or re.fullmatch(r"[0-9a-f]{64}", str(value.get("decision_payload_sha256") or "")) is None
            or not isinstance(daily_nav, list)
            or not daily_nav
            or _date(daily_nav[0].get("trade_date"), field="first NAV date") != signal
            or _date(daily_nav[-1].get("trade_date"), field="last NAV date") != outcome
            or not isinstance(value.get("trades"), list)
            or not isinstance(terminal, list)
            or len(terminal) != len(ALL_CODES)
            or {row.get("code") for row in terminal} != set(ALL_CODES)
            or any(
                _date(row.get("trade_date"), field="terminal holding date") != outcome
                for row in terminal
            )
            or not all(math.isfinite(number) for number in (start_nav, end_nav, net_return))
            or start_nav <= 0.0
            or end_nav <= 0.0
            or float(daily_nav[0].get("nav", math.nan)) != start_nav
            or float(daily_nav[-1].get("nav", math.nan)) != end_nav
            or net_return != end_nav / start_nav - 1.0
            or not math.isfinite(reconciliation)
            or reconciliation < 0.0
            or reconciliation > 1e-8
        ):
            raise ValueError(f"prospective outcome contract differs: {path}")
    return value


def _create_only(
    path: Path,
    value: Mapping[str, Any],
    *,
    before_link: Callable[[], None] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8")
    temporary = path.parent / f".{path.name}.partial-{uuid.uuid4().hex}"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        if before_link is not None:
            before_link()
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _cycle_id(value: pd.Timestamp) -> str:
    year, quarter = _quarter(value)
    return f"{year}Q{quarter}"


def _decision_paths(runtime_root: Path) -> list[Path]:
    return sorted(runtime_root.glob("cycle=*/decision.json"))


def _load_decisions(runtime_root: Path) -> list[dict[str, Any]]:
    return [_read_artifact(path, kind="factor_lab_11_2_quarterly_decision") for path in _decision_paths(runtime_root)]


def _combined_targets(decisions: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    if not decisions:
        raise ValueError("at least one decision is required")
    return pd.concat([_target_frame(value["targets"]) for value in decisions], ignore_index=True)


def _source_identity(stage: MultiAssetStage, root: Path, stage_name: str) -> dict[str, Any]:
    manifest = stage.path / "manifest.json"
    return {
        "root": str(root.resolve()),
        "stage": stage_name,
        "path": str(stage.path),
        "manifest_file_sha256": file_sha256(manifest),
        "manifest_payload_sha256": stage.manifest["payload_sha256"],
        "price_end_date": stage.manifest["price_end_date"],
    }


def _default_client() -> Any:
    config_path = ROOT / "configs" / "data.json"
    config = load_data_config(config_path)
    layout = RuntimeLayout.from_config(
        config, config_path=config_path, repo_root=ROOT
    )
    sync = dict(config.get("sync") or {})
    rate = float(sync.get("request_rate_per_minute") or 0.0)
    if rate != REQUEST_RATE_PER_MINUTE:
        raise ValueError("11.2 requires sync.request_rate_per_minute=300")
    client = RateLimitedRetryingClient(
        _configured_tushare_client(sync, layout),
        request_rate_per_minute=rate,
    )
    if client.MAX_ATTEMPTS != MAX_PROVIDER_ATTEMPTS:
        raise ValueError("11.2 requires exactly three provider attempts")
    return client


def capture_source(
    as_of: pd.Timestamp,
    *,
    runtime_root: Path,
    clock: Callable[[], datetime],
    client_factory: Callable[[], Any],
) -> MultiAssetStage:
    _read_protocol()
    now = _local_now(clock)
    if now < datetime.combine(as_of.date(), SIGNAL_READY_TIME, SHANGHAI):
        raise RuntimeError("prospective source capture is forbidden before the 17:10 gate")
    source_root = runtime_root / "sources"
    previous = _formal_stage_entries(source_root, before=as_of)
    if previous:
        baseline_date, baseline_name = previous[-1]
        baseline = load_multi_asset_stage(source_root, baseline_name)
    else:
        baseline = load_multi_asset_stage(GENESIS_SOURCE_ROOT, "audit")
        manifest_path = baseline.path / "manifest.json"
        if (
            baseline.manifest.get("payload_sha256") != GENESIS_MANIFEST_PAYLOAD
            or file_sha256(manifest_path) != GENESIS_MANIFEST_FILE_SHA256
        ):
            raise ValueError("11.2 genesis source differs from the frozen 9.0 baseline")
    start = str(baseline.manifest["price_start_date"])
    stage_name = f"asof-{as_of:%Y%m%d}"
    destination = source_root / f"stage={stage_name}"
    release_identity = _require_release_tag()
    if previous:
        _validate_source_receipt(
            baseline,
            source_root,
            baseline_name,
            baseline_date,
            release_identity,
        )
    if _local_now(clock) < datetime.combine(as_of.date(), SIGNAL_READY_TIME, SHANGHAI):
        raise RuntimeError("prospective source capture is forbidden before the 17:10 gate")
    receipt = _source_receipt_base(
        source_root, stage_name, as_of, baseline, release_identity
    )

    def checked_time(stage: MultiAssetStage) -> datetime:
        execution_date = _require_quarter_end(_sessions(stage), as_of)
        checked = _local_now(clock)
        _signal_window(as_of, execution_date, checked)
        return checked

    def validate(stage: MultiAssetStage) -> None:
        checked_time(stage)

    def freeze_receipt(stage: MultiAssetStage) -> dict[str, Any]:
        checked = checked_time(stage)
        return {
            **receipt,
            "validated_at_utc": checked.astimezone(timezone.utc).isoformat(),
        }

    if destination.exists() or destination.is_symlink():
        existing = _load_source(
            source_root, stage_name, as_of, release_identity
        )
        _assert_source_prefix(existing, baseline, _date(
            str(baseline.manifest["price_end_date"]), field="baseline price_end_date"
        ))
        validate(existing)
        return existing

    client = client_factory()

    return stable_capture_multi_asset_stage(
        client,
        source_root,
        start,
        as_of.date().isoformat(),
        stage_name,
        baseline,
        validator=validate,
        publication_receipt_factory=freeze_receipt,
    )


def _assert_source_prefix(current: MultiAssetStage, prior: MultiAssetStage, cutoff: pd.Timestamp) -> None:
    def prefix(frame: pd.DataFrame) -> pd.DataFrame:
        dates = pd.to_datetime(frame["trade_date"], errors="raise").dt.normalize()
        return frame.loc[dates.le(cutoff)].reset_index(drop=True)

    try:
        pd.testing.assert_frame_equal(prefix(current.calendar), prefix(prior.calendar), check_exact=True)
        if set(current.assets) != set(prior.assets):
            raise AssertionError("asset set differs")
        for code in ALL_CODES:
            pd.testing.assert_frame_equal(
                prefix(current.assets[code]), prefix(prior.assets[code]), check_exact=True
            )
    except AssertionError as exc:
        raise ValueError("prospective source rewrote the decision-time prefix") from exc


def _plan(records: pd.DataFrame) -> list[dict[str, Any]]:
    if records.empty:
        return []
    return _records(records.loc[:, list(PLAN_FIELDS)].sort_values("code", kind="mergesort"))


def _execution_plan_matches_seal(
    records: pd.DataFrame, sealed: Sequence[Mapping[str, Any]]
) -> bool:
    if records.empty:
        return not sealed
    actual = _plan(records)
    expected_by_code = {str(row["code"]): dict(row) for row in sealed}
    if len(expected_by_code) != len(sealed) or {row["code"] for row in actual} != set(expected_by_code):
        return False
    adjusted: list[dict[str, Any]] = []
    for row in records.sort_values("code", kind="mergesort").itertuples(index=False):
        expected = expected_by_code[str(row.code)]
        multiplier = float(getattr(row, "execution_unit_multiplier", 1.0))
        if not math.isfinite(multiplier) or multiplier <= 0.0:
            return False
        transformed = dict(expected)
        for field in UNIT_SCALE_PLAN_FIELDS:
            scaled = int(expected[field]) * multiplier
            if not float(scaled).is_integer():
                return False
            transformed[field] = int(scaled)
        adjusted.append(transformed)
    return actual == adjusted


def create_signal(
    source_root: Path,
    stage_name: str,
    signal_date: pd.Timestamp,
    *,
    runtime_root: Path,
    clock: Callable[[], datetime],
) -> dict[str, Any]:
    _read_protocol()
    release_identity = _require_release_tag()
    if source_root.resolve() != (runtime_root / "sources").resolve():
        raise ValueError("formal signal source must use the frozen 11.2 sources root")
    cycle = _cycle_id(signal_date)
    decision_path = runtime_root / f"cycle={cycle}" / "decision.json"
    if decision_path.exists():
        raise FileExistsError(f"prospective decision is create-only: {decision_path}")
    stage = _load_source(source_root, stage_name, signal_date, release_identity)
    stage_identity = _source_identity(stage, source_root, stage_name)
    sessions = _sessions(stage)
    execution_date = _require_quarter_end(sessions, signal_date)
    now = _local_now(clock)
    _signal_window(signal_date, execution_date, now)
    previous = _load_decisions(runtime_root)
    predecessor_payload: str | None = None
    predecessor: dict[str, Any] | None = None
    if previous:
        prior = previous[-1]
        prior_signal = _date(prior["signal_date"], field="prior signal_date")
        if _next_quarter_end(sessions, prior_signal) != signal_date:
            raise RuntimeError("prospective decisions must advance one exact quarter")
        outcome_path = runtime_root / f"cycle={_cycle_id(prior_signal)}" / "outcome.json"
        if not outcome_path.is_file():
            raise RuntimeError("prior quarter outcome must be confirmed before the next signal")
        predecessor = _read_artifact(
            outcome_path, kind="factor_lab_11_2_quarterly_outcome"
        )
        if (
            _date(predecessor["signal_date"], field="predecessor signal date")
            != prior_signal
            or _date(predecessor["outcome_date"], field="predecessor outcome date")
            != signal_date
            or predecessor["execution_date"] != prior["execution_date"]
            or predecessor["decision_payload_sha256"] != prior["payload_sha256"]
            or predecessor["source"] != stage_identity
        ):
            raise ValueError("prior outcome does not bind the exact continuous cycle")
        predecessor_payload = predecessor["payload_sha256"]

    targets = build_monthly_targets(stage.assets, sessions, QUARTERLY_DUAL_CONFIRM_BLEND_ID)
    selected = targets.loc[
        pd.to_datetime(targets["signal_date"]).dt.normalize().eq(signal_date)
    ].reset_index(drop=True)
    if len(selected) != len(ALL_CODES) or set(selected["code"]) != set(ALL_CODES):
        raise RuntimeError("signal must select exactly the current quarter's six target rows")
    candidate = {
        "schema_version": 1,
        "kind": "factor_lab_11_2_quarterly_decision",
        "release": RELEASE,
        "mode": "prospective_quarterly_cycle",
        "cycle_id": cycle,
        "strategy_id": QUARTERLY_DUAL_CONFIRM_BLEND_ID,
        "signal_date": signal_date.date().isoformat(),
        "execution_date": execution_date.date().isoformat(),
        "recorded_at_utc": now.astimezone(timezone.utc).isoformat(),
        "protocol_payload_sha256": PROTOCOL_PAYLOAD,
        "source": stage_identity,
        "predecessor_outcome_payload_sha256": predecessor_payload,
        "targets": _records(selected),
    }
    trial = [*previous, candidate]
    simulation = simulate_targets(
        stage.assets, _combined_targets(trial), sessions, SimulationConfig()
    )
    orders = simulation["orders"].loc[
        pd.to_datetime(simulation["orders"]["signal_date"]).dt.normalize().eq(signal_date)
    ].reset_index(drop=True)
    if not orders.empty and set(orders["status"]) != {"pending"}:
        raise RuntimeError("signal-close orders were not sealed as pending")
    nav = simulation["daily_nav"].loc[
        pd.to_datetime(simulation["daily_nav"]["trade_date"]).dt.normalize().eq(signal_date)
    ]
    holdings = simulation["holdings"].loc[
        pd.to_datetime(simulation["holdings"]["trade_date"]).dt.normalize().eq(signal_date)
    ]
    if len(nav) != 1 or len(holdings) != len(ALL_CODES):
        raise RuntimeError("signal-close account state is incomplete")
    if predecessor is not None and (
        _records(nav)[0] != predecessor["daily_nav"][-1]
        or float(nav.iloc[0]["nav"]) != float(predecessor["end_nav"])
        or _records(holdings) != predecessor["terminal_holdings"]
    ):
        raise ValueError("next signal does not exactly continue the prior terminal account")
    candidate.update(
        {
            "signal_close_nav": float(nav.iloc[0]["nav"]),
            "signal_close_cash": float(nav.iloc[0]["cash"]),
            "signal_close_holdings": _records(holdings),
            "sealed_order_plan": _plan(orders),
        }
    )
    sealed_at = _local_now(clock)
    _signal_window(signal_date, execution_date, sealed_at)
    candidate["recorded_at_utc"] = sealed_at.astimezone(timezone.utc).isoformat()
    value = _payload(candidate)
    _create_only(
        decision_path,
        value,
        before_link=lambda: _signal_window(
            signal_date, execution_date, _local_now(clock)
        ),
    )
    return value


def create_outcome(
    source_root: Path,
    stage_name: str,
    signal_date: pd.Timestamp,
    outcome_date: pd.Timestamp,
    *,
    runtime_root: Path,
    clock: Callable[[], datetime],
) -> dict[str, Any]:
    _read_protocol()
    release_identity = _require_release_tag()
    if source_root.resolve() != (runtime_root / "sources").resolve():
        raise ValueError("formal outcome source must use the frozen 11.2 sources root")
    cycle = _cycle_id(signal_date)
    decision_path = runtime_root / f"cycle={cycle}" / "decision.json"
    decision = _read_artifact(decision_path, kind="factor_lab_11_2_quarterly_decision")
    if _date(decision["signal_date"], field="decision signal date") != signal_date:
        raise ValueError("outcome signal_date does not match its sealed decision")
    outcome_path = runtime_root / f"cycle={cycle}" / "outcome.json"
    if outcome_path.exists():
        raise FileExistsError(f"prospective outcome is create-only: {outcome_path}")
    stage = _load_source(source_root, stage_name, outcome_date, release_identity)
    sessions = _sessions(stage)
    execution_date = _next_session(sessions, signal_date)
    if _date(decision["execution_date"], field="decision execution date") != execution_date:
        raise ValueError("sealed decision does not use the immediate next official session")
    if _next_quarter_end(sessions, signal_date) != outcome_date:
        raise ValueError("outcome must use the immediate next official quarter-end")
    _outcome_window(outcome_date, _local_now(clock))
    prior_source = decision["source"]
    if Path(prior_source["root"]).resolve() != source_root.resolve():
        raise ValueError("sealed decision source root differs from the formal source root")
    prior = _load_source(
        Path(prior_source["root"]),
        str(prior_source["stage"]),
        signal_date,
        release_identity,
    )
    if _source_identity(
        prior, Path(prior_source["root"]), str(prior_source["stage"])
    ) != prior_source:
        raise ValueError("sealed decision source identity no longer matches its manifest")
    _assert_source_prefix(stage, prior, signal_date)
    decisions = [
        value
        for value in _load_decisions(runtime_root)
        if _date(value["signal_date"], field="signal_date") <= signal_date
    ]
    simulation = simulate_targets(
        stage.assets, _combined_targets(decisions), sessions, SimulationConfig()
    )
    orders = simulation["orders"].loc[
        pd.to_datetime(simulation["orders"]["signal_date"]).dt.normalize().eq(signal_date)
    ].reset_index(drop=True)
    if not _execution_plan_matches_seal(orders, decision["sealed_order_plan"]):
        raise ValueError("next-open replay changed the sealed signal order plan")
    if not orders.empty and bool(
        orders["status"].isin({"pending", "blocked_missing_open"}).any()
    ):
        raise RuntimeError("exact next-open execution evidence is incomplete")
    nav = simulation["daily_nav"].copy()
    dates = pd.to_datetime(nav["trade_date"]).dt.normalize()
    period_nav = nav.loc[dates.between(signal_date, outcome_date)].reset_index(drop=True)
    if period_nav.empty or pd.Timestamp(period_nav.iloc[-1]["trade_date"]).normalize() != outcome_date:
        raise RuntimeError("outcome NAV does not reach the exact quarter-end close")
    trades = simulation["trades"].loc[
        pd.to_datetime(simulation["trades"]["signal_date"]).dt.normalize().eq(signal_date)
    ].reset_index(drop=True)
    holdings = simulation["holdings"].loc[
        pd.to_datetime(simulation["holdings"]["trade_date"]).dt.normalize().eq(outcome_date)
    ].reset_index(drop=True)
    signal_nav = nav.loc[dates.eq(signal_date)].reset_index(drop=True)
    signal_holdings = simulation["holdings"].loc[
        pd.to_datetime(simulation["holdings"]["trade_date"]).dt.normalize().eq(signal_date)
    ].reset_index(drop=True)
    if (
        len(signal_nav) != 1
        or len(signal_holdings) != len(ALL_CODES)
        or float(signal_nav.iloc[0]["nav"]) != float(decision["signal_close_nav"])
        or float(signal_nav.iloc[0]["cash"]) != float(decision["signal_close_cash"])
        or _records(signal_holdings) != decision["signal_close_holdings"]
    ):
        raise ValueError("outcome replay does not reproduce the sealed signal-close account")
    start_nav = float(period_nav.iloc[0]["nav"])
    end_nav = float(period_nav.iloc[-1]["nav"])
    confirmed_at = _local_now(clock)
    _outcome_window(outcome_date, confirmed_at)
    value = _payload(
        {
            "schema_version": 1,
            "kind": "factor_lab_11_2_quarterly_outcome",
            "release": RELEASE,
            "mode": "prospective_quarterly_cycle",
            "cycle_id": cycle,
            "strategy_id": QUARTERLY_DUAL_CONFIRM_BLEND_ID,
            "signal_date": signal_date.date().isoformat(),
            "execution_date": decision["execution_date"],
            "outcome_date": outcome_date.date().isoformat(),
            "recorded_at_utc": confirmed_at.astimezone(timezone.utc).isoformat(),
            "protocol_payload_sha256": PROTOCOL_PAYLOAD,
            "decision_payload_sha256": decision["payload_sha256"],
            "source": _source_identity(stage, source_root, stage_name),
            "sealed_plan_exact": True,
            "start_nav": start_nav,
            "end_nav": end_nav,
            "net_return": end_nav / start_nav - 1.0,
            "daily_nav": _records(period_nav),
            "trades": _records(trades),
            "terminal_holdings": _records(holdings),
            "maximum_reconciliation_error": float(period_nav["accounting_error"].abs().max()),
        }
    )
    _create_only(outcome_path, value)
    return value


def main(
    argv: list[str] | None = None,
    *,
    clock: Callable[[], datetime] | None = None,
    runtime_root: Path | None = None,
    client_factory: Callable[[], Any] | None = None,
) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    capture = sub.add_parser("capture")
    capture.add_argument("--as-of", required=True)
    for mode in ("signal", "outcome"):
        command = sub.add_parser(mode)
        command.add_argument("--source-root", type=Path, required=True)
        command.add_argument("--stage", required=True)
        command.add_argument("--as-of", required=True)
        if mode == "outcome":
            command.add_argument("--signal-date", required=True)
    args = parser.parse_args(argv)
    now = clock or (lambda: datetime.now(timezone.utc))
    target_root = (runtime_root or DEFAULT_RUNTIME_ROOT).resolve()
    as_of = _date(args.as_of, field="as_of")
    if args.mode == "capture":
        stage = capture_source(
            as_of,
            runtime_root=target_root,
            clock=now,
            client_factory=client_factory or _default_client,
        )
        print(f"stage={stage.manifest['stage']}")
        print(f"payload_sha256={stage.manifest['payload_sha256']}")
        return 0
    if args.mode == "signal":
        value = create_signal(
            args.source_root.resolve(), args.stage, as_of,
            runtime_root=target_root, clock=now,
        )
    else:
        value = create_outcome(
            args.source_root.resolve(), args.stage,
            _date(args.signal_date, field="signal_date"), as_of,
            runtime_root=target_root, clock=now,
        )
    print(f"kind={value['kind']}")
    print(f"payload_sha256={value['payload_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
