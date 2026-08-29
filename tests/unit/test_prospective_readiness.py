from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

import factor_lab.data.prospective_readiness as readiness
from factor_lab.data.prospective_readiness import (
    inspect_prospective_readiness,
    prospective_readiness_exit_code,
)


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: Any, *, canonical: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if canonical:
        path.write_bytes(_canonical_bytes(value))
    else:
        path.write_text(json.dumps(value, indent=2), encoding="utf-8")


def _frozen_sessions() -> list[str]:
    candidates = pd.bdate_range(
        readiness.CANONICAL_CALENDAR_ANCHOR, readiness.FROZEN_BRIDGE_END
    ).strftime("%Y-%m-%d").tolist()
    # The observer also verifies the frozen prefix hash.  A compact synthetic
    # prefix need not duplicate China's full holiday table; each test binds
    # this exact fixture hash through monkeypatch below.
    return [candidates[0], *candidates[-(readiness.CANONICAL_CALENDAR_COUNT - 1) :]]


def _calendar_records(start: str, end: str) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    dates = pd.date_range(start, end, freq="D")
    frame = pd.DataFrame(
        {
            "exchange": "SSE",
            "cal_date": dates,
            "is_open": dates.weekday < 5,
            "pretrade_date": pd.NaT,
        }
    )
    records = [
        {
            "cal_date": row.cal_date.date().isoformat(),
            "exchange": "SSE",
            "is_open": bool(row.is_open),
            "pretrade_date": None,
        }
        for row in frame.itertuples(index=False)
    ]
    return frame, records


def _partition_frame(dataset: str, trade_date: str) -> pd.DataFrame:
    tickers = ["000001.SZ", "000002.SZ"]
    compact = trade_date.replace("-", "")
    common: dict[str, Any] = {"ts_code": tickers, "trade_date": compact}
    if dataset == "daily":
        return pd.DataFrame(
            {
                **common,
                "open": [10.0, 20.0],
                "high": [11.0, 21.0],
                "low": [9.0, 19.0],
                "close": [10.5, 20.5],
                "pre_close": [10.0, 20.0],
                "pct_chg": [5.0, 2.5],
                "amount": [1000.0, 2000.0],
            }
        )
    if dataset == "daily_basic":
        return pd.DataFrame({**common, "pe_ttm": [10.0, 20.0], "pb": [1.0, 2.0]})
    if dataset == "adj_factor":
        return pd.DataFrame({**common, "adj_factor": [1.0, 1.0]})
    raise AssertionError(dataset)


def _write_partition(
    root: Path,
    checkpoint: dict[str, Any],
    dataset: str,
    trade_date: str,
    *,
    completed_at_utc: str,
) -> None:
    path = (
        root
        / "runtime/data/raw"
        / dataset
        / f"trade_date={trade_date}"
        / "part-000.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = _partition_frame(dataset, trade_date)
    frame.to_parquet(path, index=False)
    checkpoint["partitions"][f"{dataset}/{trade_date}"] = {
        "status": "complete",
        "dataset": dataset,
        "trade_date": trade_date,
        "path": str(path.resolve()),
        "row_count": len(frame),
        "size_bytes": path.stat().st_size,
        "sha256": _sha256(path),
        "completed_at_utc": completed_at_utc,
    }


def _write_reference(root: Path, trade_date: str) -> None:
    path = (
        root
        / "runtime/data/raw/bak_basic"
        / f"trade_date={trade_date}"
        / "part-000.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        {
            "trade_date": trade_date.replace("-", ""),
            "ts_code": ["000001.SZ", "000002.SZ"],
            "name": ["一号", "二号"],
            "industry": ["测试", "测试"],
            "list_date": ["20100101", "20100101"],
        }
    )
    frame.to_parquet(path, index=False)
    checkpoint = {
        "schema_version": 1,
        "partitions": {
            f"bak_basic/trade_date={trade_date}": {
                "status": "complete",
                "dataset": "bak_basic",
                "trade_date": trade_date,
                "source_trade_date": trade_date,
                "path": str(path.resolve()),
                "row_count": len(frame),
                "size_bytes": path.stat().st_size,
                "sha256": _sha256(path),
                "completed_at_utc": "2026-08-31T07:40:00Z",
            }
        },
    }
    _write_json(root / "runtime/data/raw/enrichment-checkpoint.json", checkpoint)


def _write_ledger(root: Path, *, ledger_id: str = readiness.LEDGER_ID) -> None:
    ledger = root / "runtime/prospective/5.0"
    record = {
        "clock_source": "test_clock",
        "kind": "protocol_activation",
        "ledger_id": ledger_id,
        "payload": {},
        "previous_record_sha256": None,
        "recorded_at_utc": "2026-08-28T14:00:00Z",
        "schema_version": 1,
        "sequence": 1,
    }
    raw = _canonical_bytes(record)
    digest = hashlib.sha256(raw).hexdigest()
    record_path = ledger / "records" / f"{1:016d}-protocol_activation-{digest}.json"
    record_path.parent.mkdir(parents=True, exist_ok=True)
    record_path.write_bytes(raw)
    snapshot = {
        "schema_version": 2,
        "ledger_id": ledger_id,
        "head_sequence": 1,
        "head_record_sha256": digest,
        "integrity_valid": True,
        "phase": "awaiting_new_data",
        "decision_generation_ready": True,
        "decision_count": 0,
        "open_decision_count": 0,
        "implementation_trusted_tlog_timestamp_utc": "2026-08-28T14:44:28Z",
    }
    snapshot_raw = _canonical_bytes(snapshot)
    snapshot_sha = hashlib.sha256(snapshot_raw).hexdigest()
    snapshot_path = ledger / "snapshots" / f"{1:016d}-{snapshot_sha}.json"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_bytes(snapshot_raw)


def _write_case(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    calendar_start: str = "2026-08-22",
    calendar_end: str = "2026-09-30",
    missing_dataset: str | None = None,
    current_golden: bool = False,
    ledger_id: str = readiness.LEDGER_ID,
) -> None:
    _write_ledger(root, ledger_id=ledger_id)
    frozen = _frozen_sessions()
    execution_path = root / "runtime/data/top500/execution.parquet"
    execution_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"date": frozen}).to_parquet(execution_path, index=False)
    feature_path = root / "runtime/data/top500/features.parquet"
    pd.DataFrame({"date": ["2026-08-13"]}).to_parquet(feature_path, index=False)
    monkeypatch.setattr(
        readiness,
        "CANONICAL_CALENDAR_SHA256",
        readiness._calendar_prefix_sha256(frozen),
    )

    calendar, records = _calendar_records(calendar_start, calendar_end)
    content_sha = hashlib.sha256(_canonical_bytes(records)).hexdigest()
    calendar_dir = (
        root
        / "runtime/data/raw/trade_cal"
        / f"calendar_sha256={content_sha}"
    )
    calendar_dir.mkdir(parents=True, exist_ok=True)
    calendar_path = calendar_dir / "part-000.parquet"
    calendar.to_parquet(calendar_path, index=False)
    manifest_path = calendar_dir / "manifest.json"
    manifest = {
        "schema_version": 1,
        "status": "complete",
        "exchange": "SSE",
        "start_date": calendar_start,
        "end_date": calendar_end,
        "row_count": len(calendar),
        "open_day_count": int(calendar["is_open"].sum()),
        "path": str(calendar_path.resolve()),
        "artifact_sha256": _sha256(calendar_path),
        "calendar_content_sha256": content_sha,
        "completed_at_utc": "2026-08-28T15:29:27Z",
        "records_sha256": content_sha,
    }
    _write_json(manifest_path, manifest)
    raw_checkpoint: dict[str, Any] = {
        "schema_version": 1,
        "partitions": {},
        "calendars": {
            content_sha: {
                key: value for key, value in manifest.items() if key != "schema_version"
            }
        },
    }
    raw_checkpoint["calendars"][content_sha].update(
        manifest_path=str(manifest_path.resolve()),
        manifest_sha256=_sha256(manifest_path),
    )

    official_opens = [row["cal_date"] for row in records if row["is_open"]]
    post_bridge = [value for value in official_opens if value > readiness.FROZEN_BRIDGE_END]
    complete = [*frozen, *post_bridge]
    signal = "2026-08-31"
    liquidity = [value for value in complete if value < "2026-09-01"][-60:]
    signal_daily = [value for value in complete if "2026-08-13" <= value <= signal]
    post_bridge_signal = [value for value in post_bridge if value <= signal]
    pairs = {
        *(('daily', value) for value in liquidity),
        *(('daily', value) for value in signal_daily),
        *(('daily_basic', value) for value in post_bridge_signal),
        *(('adj_factor', value) for value in post_bridge_signal),
    }
    for dataset, trade_date in sorted(pairs):
        if current_golden and trade_date == signal:
            continue
        if missing_dataset == dataset and trade_date == signal:
            continue
        _write_partition(
            root,
            raw_checkpoint,
            dataset,
            trade_date,
            completed_at_utc=(
                "2026-08-28T14:00:00Z"
                if trade_date < signal
                else "2026-08-31T07:30:00Z"
            ),
        )
    _write_json(root / "runtime/data/raw/checkpoint.json", raw_checkpoint)
    if not current_golden:
        _write_reference(root, signal)


def _install_mock_membership(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    reject_replay: bool = False,
) -> str:
    artifact_raw = b"authoritative-membership-fixture\n"
    artifact_sha = hashlib.sha256(artifact_raw).hexdigest()
    directory = (
        root
        / "runtime/prospective/5.0/membership/2026-09"
        / artifact_sha
    )
    artifact_path = directory / "membership.parquet"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_bytes(artifact_raw)
    manifest_path = directory / "manifest.json"
    manifest = {
        "schema_version": 1,
        "rule_id": "prospective_top500_membership_5_2",
        "membership_month": "2026-09",
        "as_of_date": "2026-08-31",
        "effective_start_date": "2026-09-01",
        "effective_end_date": "2026-09-30",
        "completed_at_utc": "2026-08-31T07:45:00Z",
        "artifact_sha256": artifact_sha,
    }
    _write_json(manifest_path, manifest, canonical=True)

    def load(path, *, project_root, available_at_utc):
        assert Path(path).resolve() == artifact_path.resolve()
        assert Path(project_root).resolve() == root.resolve()
        assert available_at_utc == "2026-08-31T08:00:00Z"
        if reject_replay:
            raise ValueError("forged membership")
        return SimpleNamespace(
            membership_month="2026-09",
            as_of_date="2026-08-31",
            artifact_sha256=artifact_sha,
            membership_path=artifact_path.resolve(),
            manifest_path=manifest_path.resolve(),
            manifest=manifest,
            frame=pd.DataFrame({"ts_code": [f"T{index:06d}" for index in range(500)]}),
        )

    monkeypatch.setattr(
        "factor_lab.data.prospective_membership.load_prospective_membership_snapshot",
        load,
    )
    return artifact_sha


def _install_mock_input(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    membership_sha: str,
    loaded_membership_sha: str | None = None,
    calendar_sources: list[dict[str, Any]] | None = None,
) -> str:
    manifest = {
        "schema_version": 1,
        "kind": "prospective_signal_input_snapshot",
        "signal_date": "2026-08-31",
        "official_trade_date": "2026-09-01",
    }
    if calendar_sources is not None:
        manifest["calendar"] = {"sources": calendar_sources}
    snapshot_sha = hashlib.sha256(_canonical_bytes(manifest)).hexdigest()
    directory = root / "runtime/prospective/5.0/inputs" / snapshot_sha
    _write_json(directory / "manifest.json", manifest, canonical=True)

    def load(path):
        assert Path(path).resolve() == directory.resolve()
        return SimpleNamespace(
            signal_date="2026-08-31",
            trade_date="2026-09-01",
            snapshot_sha256=snapshot_sha,
            inputs_available_at_utc="2026-08-31T07:30:00Z",
            build_completed_at_utc="2026-08-31T07:45:00Z",
            membership_artifact_sha256=(
                membership_sha
                if loaded_membership_sha is None
                else loaded_membership_sha
            ),
            manifest=manifest,
        )

    monkeypatch.setattr(
        "factor_lab.data.prospective.load_prospective_input_snapshot",
        load,
    )
    return snapshot_sha


def _seal_calendar_source(root: Path) -> dict[str, Any]:
    checkpoint = json.loads(
        (root / "runtime/data/raw/checkpoint.json").read_text(encoding="utf-8")
    )
    content_sha, entry = next(iter(checkpoint["calendars"].items()))
    artifact_path = Path(entry["path"])
    manifest_path = Path(entry["manifest_path"])
    artifact_sha = str(entry["artifact_sha256"])
    manifest_sha = str(entry["manifest_sha256"])
    artifact_relative = (
        Path("runtime/prospective/5.0/source-artifacts")
        / f"sha256={artifact_sha}"
        / "artifact"
    )
    manifest_relative = (
        Path("runtime/prospective/5.0/source-artifacts")
        / f"sha256={manifest_sha}"
        / "artifact"
    )
    artifact_cas = root / artifact_relative
    manifest_cas = root / manifest_relative
    artifact_cas.parent.mkdir(parents=True, exist_ok=True)
    manifest_cas.parent.mkdir(parents=True, exist_ok=True)
    artifact_cas.write_bytes(artifact_path.read_bytes())
    manifest_cas.write_bytes(manifest_path.read_bytes())
    return {
        "role": "official_trade_calendar",
        "calendar_content_sha256": content_sha,
        "path": str(artifact_path.resolve()),
        "artifact_sha256": artifact_sha,
        "immutable_path": artifact_relative.as_posix(),
        "size_bytes": artifact_cas.stat().st_size,
        "media_type": "application/vnd.apache.parquet",
        "manifest_path": str(manifest_path.resolve()),
        "manifest_sha256": manifest_sha,
        "immutable_manifest_path": manifest_relative.as_posix(),
        "manifest_size_bytes": manifest_cas.stat().st_size,
        "manifest_media_type": "application/json",
        "completed_at_utc": entry["completed_at_utc"],
        "source_start_date": entry["start_date"],
        "source_end_date": entry["end_date"],
        "row_count": entry["row_count"],
        "open_day_count": entry["open_day_count"],
        "exchange": "SSE",
    }


def _codes(report: dict[str, Any]) -> set[str]:
    return {str(item["code"]) for item in report["issues"]}


def _tree_snapshot(root: Path) -> list[tuple[str, str, int, str | None]]:
    rows: list[tuple[str, str, int, str | None]] = []
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        relative = path.relative_to(root).as_posix()
        if path.is_dir():
            rows.append((relative, "dir", 0, None))
        else:
            rows.append((relative, "file", path.stat().st_size, _sha256(path)))
    return rows


def test_current_golden_waits_for_close_and_august_31_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(tmp_path, monkeypatch, current_golden=True)

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-08-29T00:00:00Z"
    )

    assert report["status"] == "waiting"
    assert report["reason"] == "before_signal_close"
    assert report["next_action"] == "wait"
    assert report["candidate"] == {
        "signal_date": "2026-08-31",
        "signal_close_utc": "2026-08-31T07:00:00Z",
        "entry_date": "2026-09-01",
        "i_plus_11_date": "2026-09-15",
        "admission_deadline_utc": "2026-09-01T01:15:00Z",
        "calendar_index": 2345,
        "due_offset": 5,
        "membership_month": "2026-09",
        "membership_as_of_date": "2026-08-31",
        "membership_effective_start_date": "2026-09-01",
        "membership_effective_end_date": "2026-09-30",
        "membership_calendar_end_date": "2026-09-30",
        "initial_skipped_sessions": [
            "2026-08-24",
            "2026-08-25",
            "2026-08-26",
            "2026-08-27",
            "2026-08-28",
        ],
    }
    assert report["calendar"]["status"] == "complete"
    assert report["coverage"]["liquidity_daily"]["complete_count"] == 59
    assert report["coverage"]["signal_daily"]["required_count"] == 0
    assert report["coverage"]["signal_daily_basic"]["required_count"] == 0
    assert report["coverage"]["signal_adj_factor"]["required_count"] == 0
    assert report["reference"]["status"] == "missing"
    assert _codes(report) == {
        "BEFORE_SIGNAL_CLOSE",
        "DAILY_PARTITION_MISSING",
        "REFERENCE_MISSING",
    }
    assert prospective_readiness_exit_code(report) == 2


def test_observer_honours_a_non_default_ledger_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledger_id = "factor-lab/prospective/test-ledger"
    _write_case(
        tmp_path,
        monkeypatch,
        current_golden=True,
        ledger_id=ledger_id,
    )

    report = inspect_prospective_readiness(
        tmp_path,
        observed_at_utc="2026-08-29T00:00:00Z",
        ledger_id=ledger_id,
    )

    assert report["status"] == "waiting"
    assert report["ledger"]["ledger_id"] == ledger_id


def test_complete_evidence_opens_membership_build_after_signal_close(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(tmp_path, monkeypatch)

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-08-31T08:00:00Z"
    )

    assert report["status"] == "ready"
    assert report["reason"] == "membership_build_ready"
    assert report["next_action"] == "build_membership"
    assert report["ready_for"] == {
        "membership_build": True,
        "input_build": False,
        "decision_admission": False,
    }
    assert report["issues"] == []
    assert prospective_readiness_exit_code(report) == 0


def test_membership_complete_without_input_only_enables_input_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(tmp_path, monkeypatch)
    artifact_sha = _install_mock_membership(tmp_path, monkeypatch)

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-08-31T08:00:00Z"
    )

    assert report["status"] == "ready"
    assert report["reason"] == "input_build_ready"
    assert report["next_action"] == "build_input"
    assert report["membership"]["artifact_sha256"] == artifact_sha
    assert report["input_snapshot"]["status"] == "not_built"
    assert report["ready_for"] == {
        "membership_build": False,
        "input_build": True,
        "decision_admission": False,
    }
    assert report["reference"]["status"] == "satisfied_by_authoritative_replay"
    assert report["coverage"]["liquidity_daily"]["required_count"] == 0


def test_verified_input_waits_for_authoritative_target_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(tmp_path, monkeypatch)
    membership_sha = _install_mock_membership(tmp_path, monkeypatch)
    snapshot_sha = _install_mock_input(
        tmp_path, monkeypatch, membership_sha=membership_sha
    )

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-08-31T08:00:00Z"
    )

    assert report["status"] == "waiting"
    assert report["reason"] == "authoritative_target_replay_required"
    assert report["input_snapshot"]["snapshot_sha256"] == snapshot_sha
    assert report["ready_for"]["decision_admission"] is False
    assert report["target_replay"]["status"] == "not_run"


def test_sealed_input_replay_does_not_depend_on_mutable_raw_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(tmp_path, monkeypatch)
    calendar_source = _seal_calendar_source(tmp_path)
    membership_sha = _install_mock_membership(tmp_path, monkeypatch)
    snapshot_sha = _install_mock_input(
        tmp_path,
        monkeypatch,
        membership_sha=membership_sha,
        calendar_sources=[calendar_source],
    )
    (tmp_path / "runtime/data/raw/checkpoint.json").unlink()
    before = _tree_snapshot(tmp_path)

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-08-31T08:00:00Z"
    )

    assert report["status"] == "waiting"
    assert report["reason"] == "authoritative_target_replay_required"
    assert report["input_snapshot"]["snapshot_sha256"] == snapshot_sha
    assert "RAW_CHECKPOINT_MISSING" not in _codes(report)
    assert report["stable_view"] is True
    assert _tree_snapshot(tmp_path) == before


def test_old_sealed_calendar_does_not_hide_a_longer_live_calendar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(
        tmp_path,
        monkeypatch,
        calendar_end="2026-09-12",
    )
    old_calendar_source = _seal_calendar_source(tmp_path)
    _write_case(tmp_path, monkeypatch, calendar_end="2026-09-30")
    membership_sha = _install_mock_membership(tmp_path, monkeypatch)
    _install_mock_input(
        tmp_path,
        monkeypatch,
        membership_sha=membership_sha,
        calendar_sources=[old_calendar_source],
    )

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-08-31T08:00:00Z"
    )

    assert report["reason"] == "authoritative_target_replay_required"
    assert report["candidate"]["signal_date"] == "2026-08-31"
    assert report["candidate"]["i_plus_11_date"] == "2026-09-15"
    assert report["calendar"]["end_date"] == "2026-09-30"


def test_later_cycle_build_windows_switch_to_the_live_calendar_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(
        tmp_path,
        monkeypatch,
        calendar_end="2026-11-30",
    )
    checkpoint = json.loads(
        (tmp_path / "runtime/data/raw/checkpoint.json").read_text(encoding="utf-8")
    )
    live_calendars, invalid, future = readiness._verified_calendars(
        tmp_path,
        checkpoint,
        readiness._utc("2026-09-30T08:00:00Z", label="test observation"),
    )
    assert len(live_calendars) == 1
    assert not invalid and not future
    live = live_calendars[0]
    sealed = json.loads(json.dumps(live))
    sealed["content_sha256"] = "e" * 64
    sealed["artifact_sha256"] = "d" * 64
    sealed["completed_at_utc"] = "2026-08-28T15:00:00Z"
    sealed["open_dates"].remove("2026-09-10")
    signal_index = readiness.CANONICAL_CALENDAR_COUNT + [
        value for value in live["open_dates"] if value > readiness.FROZEN_BRIDGE_END
    ].index("2026-09-30")
    monkeypatch.setattr(
        readiness,
        "_sealed_artifact_calendars",
        lambda _root, _observed: [sealed],
    )

    def ledger_view(_root, _ledger_root, ledger_id):
        return (
            {
                "root": "runtime/prospective/5.0",
                "ledger_id": ledger_id,
                "head_sequence": 5,
                "head_record_sha256": "a" * 64,
                "snapshot_sha256": "b" * 64,
                "phase": "awaiting_decision",
                "decision_generation_ready": True,
                "decision_count": 1,
                "open_decision_count": 0,
                "implementation_trusted_tlog_timestamp_utc": (
                    "2026-08-28T14:44:28Z"
                ),
                "prospective_epoch_tlog_timestamp_utc": (
                    "2026-08-28T14:44:28Z"
                ),
                "last_decision_signal_date": "2026-09-29",
                "last_decision_calendar_index": signal_index - 1,
                "observer_validation_scope": (
                    "canonical_record_chain_and_snapshot_binding"
                ),
            },
            [],
        )

    monkeypatch.setattr(readiness, "_ledger_view", ledger_view)

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-09-30T08:00:00Z"
    )

    assert report["candidate"]["signal_date"] == "2026-09-30"
    assert report["calendar"]["content_sha256"] == live["content_sha256"]
    assert "2026-09-10" in report["coverage"]["liquidity_daily"][
        "required_dates"
    ]


def test_build_gate_rejects_a_mixed_invalid_live_calendar_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(tmp_path, monkeypatch)
    checkpoint_path = tmp_path / "runtime/data/raw/checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    checkpoint["calendars"]["f" * 64] = {
        "status": "complete",
        "calendar_content_sha256": "f" * 64,
    }
    _write_json(checkpoint_path, checkpoint)

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-08-31T08:00:00Z"
    )

    assert report["status"] == "blocked"
    assert report["ready_for"] == {
        "membership_build": False,
        "input_build": False,
        "decision_admission": False,
    }
    assert "CALENDAR_INVALID" in _codes(report)


def test_sealed_admission_ignores_unrelated_invalid_live_calendar_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(tmp_path, monkeypatch)
    calendar_source = _seal_calendar_source(tmp_path)
    membership_sha = _install_mock_membership(tmp_path, monkeypatch)
    _install_mock_input(
        tmp_path,
        monkeypatch,
        membership_sha=membership_sha,
        calendar_sources=[calendar_source],
    )
    checkpoint_path = tmp_path / "runtime/data/raw/checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    checkpoint["calendars"]["f" * 64] = {
        "status": "complete",
        "calendar_content_sha256": "f" * 64,
    }
    _write_json(checkpoint_path, checkpoint)

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-08-31T08:00:00Z"
    )

    assert report["status"] == "waiting"
    assert report["reason"] == "authoritative_target_replay_required"
    assert "CALENDAR_INVALID" not in _codes(report)


def test_wrong_membership_input_cannot_unlock_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(tmp_path, monkeypatch)
    membership_sha = _install_mock_membership(tmp_path, monkeypatch)
    _install_mock_input(
        tmp_path,
        monkeypatch,
        membership_sha=membership_sha,
        loaded_membership_sha="f" * 64,
    )

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-08-31T08:00:00Z"
    )

    assert report["status"] == "blocked"
    assert report["input_snapshot"]["status"] == "invalid"
    assert "INPUT_SNAPSHOT_INVALID" in _codes(report)
    assert report["ready_for"]["decision_admission"] is False


def test_authoritative_membership_replay_rejects_a_forged_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(tmp_path, monkeypatch)
    _install_mock_membership(tmp_path, monkeypatch, reject_replay=True)

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-08-31T08:00:00Z"
    )

    assert report["status"] == "blocked"
    assert report["membership"]["status"] == "invalid"
    assert "MEMBERSHIP_INVALID" in _codes(report)


def test_incomplete_month_calendar_waits_even_when_i11_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(tmp_path, monkeypatch, calendar_end="2026-09-15")

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-08-31T08:00:00Z"
    )

    assert report["status"] == "waiting"
    assert report["calendar"]["covers_i_plus_11"] is True
    assert report["calendar"]["full_membership_month"] is False
    assert "CALENDAR_MONTH_INCOMPLETE" in _codes(report)


def test_post_bridge_calendar_prefix_gap_waits_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(tmp_path, monkeypatch, calendar_start="2026-08-23")

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-08-31T08:00:00Z"
    )

    assert report["status"] == "waiting"
    assert report["calendar"]["prefix_contiguous"] is False
    assert "CALENDAR_PREFIX_GAP" in _codes(report)


@pytest.mark.parametrize(
    ("dataset", "code"),
    [
        ("daily", "DAILY_PARTITION_MISSING"),
        ("daily_basic", "DAILY_BASIC_PARTITION_MISSING"),
        ("adj_factor", "ADJ_FACTOR_PARTITION_MISSING"),
    ],
)
def test_each_required_partition_type_is_independently_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dataset: str,
    code: str,
) -> None:
    _write_case(tmp_path, monkeypatch, missing_dataset=dataset)
    _install_mock_membership(tmp_path, monkeypatch)

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-08-31T08:00:00Z"
    )

    assert report["status"] == "waiting"
    assert report["ready"] is False
    assert code in _codes(report)
    assert "2026-08-31" in next(
        item["details"]["missing_dates"]
        for item in report["issues"]
        if item["code"] == code
    )


def test_deadline_is_terminal_and_cannot_be_reopened_by_complete_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(tmp_path, monkeypatch)

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-09-01T01:15:00Z"
    )

    assert report["status"] == "terminal"
    assert report["reason"] == "admission_deadline_missed"
    assert report["next_action"] == "none"
    assert report["timing"]["admission_deadline_missed"] is True
    assert "ADMISSION_DEADLINE_MISSED" in _codes(report)
    assert prospective_readiness_exit_code(report) == 4


def test_missed_deadline_dominates_invalid_frozen_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(tmp_path, monkeypatch)
    execution = tmp_path / "runtime/data/top500/execution.parquet"
    pd.DataFrame({"date": ["2026-08-21"]}).to_parquet(execution, index=False)

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-09-01T01:15:00Z"
    )

    assert report["status"] == "terminal"
    assert report["reason"] == "admission_deadline_missed"
    assert "ADMISSION_DEADLINE_MISSED" in _codes(report)
    assert "FROZEN_CALENDAR_INVALID" in _codes(report)


def test_second_signal_reuses_same_month_membership_coordinates() -> None:
    open_dates = pd.bdate_range("2026-08-24", "2026-10-31").strftime(
        "%Y-%m-%d"
    ).tolist()
    candidate, _ = readiness._candidate_from_calendar(
        {"open_dates": open_dates},
        {
            "last_decision_signal_date": "2026-08-31",
            "last_decision_calendar_index": 2345,
        },
    )

    assert candidate["signal_date"] == "2026-09-01"
    assert candidate["entry_date"] == "2026-09-02"
    assert candidate["calendar_index"] == 2346
    assert candidate["membership_month"] == "2026-09"
    assert candidate["membership_as_of_date"] == "2026-08-31"
    assert candidate["membership_effective_start_date"] == "2026-09-01"
    assert candidate["membership_effective_end_date"] == "2026-09-30"


def test_membership_effective_end_is_last_open_not_natural_month_end() -> None:
    open_dates = pd.bdate_range("2026-08-24", "2026-11-30").strftime(
        "%Y-%m-%d"
    ).tolist()
    candidate, _ = readiness._candidate_from_calendar(
        {"open_dates": open_dates},
        {
            "last_decision_signal_date": "2026-09-29",
            "last_decision_calendar_index": 2365,
        },
    )

    assert candidate["signal_date"] == "2026-09-30"
    assert candidate["entry_date"] == "2026-10-01"
    assert candidate["membership_month"] == "2026-10"
    assert candidate["membership_as_of_date"] == "2026-09-30"
    assert candidate["membership_effective_start_date"] == "2026-10-01"
    assert candidate["membership_effective_end_date"] == "2026-10-30"
    assert candidate["membership_calendar_end_date"] == "2026-10-31"


def test_inspection_is_byte_for_byte_zero_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(tmp_path, monkeypatch, current_golden=True)
    before = _tree_snapshot(tmp_path)

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-08-29T00:00:00Z"
    )

    after = _tree_snapshot(tmp_path)
    assert report["stable_view"] is True
    assert after == before
    assert not (tmp_path / "runtime/prospective/5.0/verification-cache").exists()
    assert not (tmp_path / "runtime/prospective/5.0/source-artifacts").exists()


def test_full_membership_and_input_replay_is_byte_for_byte_zero_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(tmp_path, monkeypatch)
    membership_sha = _install_mock_membership(tmp_path, monkeypatch)
    _install_mock_input(tmp_path, monkeypatch, membership_sha=membership_sha)
    before = _tree_snapshot(tmp_path)

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-08-31T08:00:00Z"
    )

    assert report["reason"] == "authoritative_target_replay_required"
    assert report["stable_view"] is True
    assert _tree_snapshot(tmp_path) == before


def test_report_has_stable_top_level_and_issue_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_case(tmp_path, monkeypatch, current_golden=True)

    report = inspect_prospective_readiness(
        tmp_path, observed_at_utc="2026-08-29T00:00:00Z"
    )

    assert set(report) == {
        "schema_version",
        "kind",
        "contract_id",
        "observed_at_utc",
        "clock_source",
        "stable_view",
        "status",
        "reason",
        "ready",
        "next_action",
        "ready_for",
        "ledger",
        "candidate",
        "calendar",
        "coverage",
        "reference",
        "membership",
        "input_snapshot",
        "target_replay",
        "timing",
        "issues",
    }
    assert set(report["ledger"]) == {
        "root",
        "ledger_id",
        "head_sequence",
        "head_record_sha256",
        "snapshot_sha256",
        "phase",
        "decision_generation_ready",
        "decision_count",
        "open_decision_count",
        "implementation_trusted_tlog_timestamp_utc",
        "prospective_epoch_tlog_timestamp_utc",
        "last_decision_signal_date",
        "last_decision_calendar_index",
        "observer_validation_scope",
    }
    assert all(
        set(issue)
        == {"code", "severity", "component", "retryable", "message", "details"}
        for issue in report["issues"]
    )
    assert prospective_readiness_exit_code({"status": "unknown"}) == 1


def test_first_candidate_uses_the_immutable_epoch_tlog() -> None:
    calendar = {
        "open_dates": [
            "2026-08-24",
            "2026-08-25",
            "2026-08-26",
            "2026-08-27",
            "2026-08-28",
            "2026-08-31",
            "2026-09-01",
            "2026-09-02",
            "2026-09-03",
            "2026-09-04",
            "2026-09-07",
            "2026-09-08",
            "2026-09-09",
            "2026-09-10",
            "2026-09-11",
            "2026-09-14",
            "2026-09-15",
            "2026-09-16",
            "2026-09-17",
            "2026-09-18",
            "2026-09-21",
            "2026-09-22",
            "2026-09-23",
            "2026-09-24",
            "2026-09-25",
            "2026-09-28",
            "2026-09-29",
            "2026-09-30",
        ]
    }
    candidate, _post_bridge = readiness._candidate_from_calendar(
        calendar,
        {
            "last_decision_signal_date": None,
            "last_decision_calendar_index": None,
            "implementation_trusted_tlog_timestamp_utc": (
                "2026-08-31T07:01:00Z"
            ),
            "prospective_epoch_tlog_timestamp_utc": (
                "2026-08-28T14:44:28Z"
            ),
        },
    )

    assert candidate["signal_date"] == "2026-08-31"
    assert candidate["initial_skipped_sessions"] == [
        "2026-08-24",
        "2026-08-25",
        "2026-08-26",
        "2026-08-27",
        "2026-08-28",
    ]
