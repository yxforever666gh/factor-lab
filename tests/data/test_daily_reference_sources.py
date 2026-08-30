from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import threading
from typing import Any, Callable

import pandas as pd
import pytest

import factor_lab.data.sources as sources
from factor_lab.cli import build_parser
from factor_lab.data import (
    RuntimeLayout,
    create_daily_stock_st_cutoff_checkpoint,
    sync_daily_stock_st,
)
from factor_lab.data.catalog import sha256_file
from factor_lab.data.sources import ENRICHMENT_DATASET_FIELDS


OPEN_DATES = ("2024-01-02", "2024-01-04")


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
        },
    }
    config_path = tmp_path / "configs" / "data.json"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    layout.ensure_directories()
    return config_path, layout


def _calendar() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "exchange": ["SSE", "SSE", "SSE"],
            "cal_date": ["20240102", "20240103", "20240104"],
            "is_open": [1, 0, 1],
            "pretrade_date": ["20231229", "20240102", "20240102"],
        }
    )


def _stock_st_frame(
    trade_date: str,
    *,
    row_count: int = 2,
) -> pd.DataFrame:
    fields = ENRICHMENT_DATASET_FIELDS["stock_st"].split(",")
    rows = [
        {
            "ts_code": f"{index:06d}.SZ",
            "name": f"*ST示例{index}",
            "trade_date": trade_date,
            "type": "S",
            "type_name": "ST",
        }
        for index in range(1, row_count + 1)
    ]
    return pd.DataFrame(rows, columns=fields)


class DailyStockStClient:
    def __init__(
        self,
        response: Callable[[str], pd.DataFrame] | None = None,
        *,
        barrier: threading.Barrier | None = None,
    ) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.response = response or _stock_st_frame
        self.barrier = barrier

    def query(self, endpoint: str, **kwargs: Any) -> pd.DataFrame:
        self.calls.append((endpoint, kwargs))
        if endpoint == "trade_cal":
            assert kwargs["exchange"] == "SSE"
            return _calendar()
        assert endpoint == "stock_st"
        if self.barrier is not None:
            self.barrier.wait(timeout=10)
        return self.response(str(kwargs["trade_date"]))


def test_daily_stock_st_is_official_session_only_partial_and_resumable(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    first_client = DailyStockStClient()

    first = sync_daily_stock_st(
        "2024-01-02",
        "2024-01-04",
        config_path=config_path,
        layout=layout,
        client=first_client,
        max_partitions=1,
    )

    checkpoint_path = layout.raw_root / "stock-st-checkpoint.json"
    assert first["status"] == "partial"
    assert first["open_day_count"] == 2
    assert first["partition_count"] == 2
    assert first["completed_this_run"] == 1
    assert first["remaining_partition_count"] == 1
    assert Path(first["checkpoint_path"]) == checkpoint_path
    assert not layout.checkpoint_path.exists()
    assert [
        call[1]["trade_date"]
        for call in first_client.calls
        if call[0] == "stock_st"
    ] == ["20240102"]

    second_client = DailyStockStClient()
    second = sync_daily_stock_st(
        "2024-01-02",
        "2024-01-04",
        config_path=config_path,
        layout=layout,
        client=second_client,
    )

    assert second["status"] == "complete"
    assert second["completed_before"] == 1
    assert second["completed_this_run"] == 1
    assert second["completed_concurrently"] == 0
    assert second["remaining_partition_count"] == 0
    assert [
        call[1]["trade_date"]
        for call in second_client.calls
        if call[0] == "stock_st"
    ] == ["20240104"]

    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert checkpoint["partition_root"] == str(
        (layout.raw_root / "stock_st").resolve()
    )
    assert checkpoint["partition_root_payload_sha256"] == (
        sources._daily_stock_st_partition_root_payload_sha256(
            layout.raw_root / "stock_st",
            checkpoint["partitions"],
        )
    )
    assert second["partition_root"] == checkpoint["partition_root"]
    assert (
        second["partition_root_payload_sha256"]
        == checkpoint["partition_root_payload_sha256"]
    )
    assert set(checkpoint["partitions"]) == {
        "stock_st/trade_date=2024-01-02",
        "stock_st/trade_date=2024-01-04",
    }
    assert len(checkpoint["calendars"]) == 1
    assert Path(second["calendar_path"]).is_relative_to(
        layout.raw_root / "stock_st" / "trade_cal"
    )
    for trade_date in OPEN_DATES:
        entry = checkpoint["partitions"][f"stock_st/trade_date={trade_date}"]
        artifact = Path(entry["path"])
        assert artifact == (
            layout.raw_root
            / "stock_st"
            / f"trade_date={trade_date}"
            / "part-000.parquet"
        )
        assert entry["sha256"] == sha256_file(artifact)
        assert entry["row_count"] == 2
        assert entry["endpoint_row_limit"] == 1_000
        saved = pd.read_parquet(artifact)
        assert saved["trade_date"].astype(str).eq(trade_date.replace("-", "")).all()
        assert not saved.duplicated(["ts_code", "trade_date"]).any()

    resume_client = DailyStockStClient()
    resumed = sync_daily_stock_st(
        "2024-01-02",
        "2024-01-04",
        config_path=config_path,
        layout=layout,
        client=resume_client,
    )
    assert resumed["completed_before"] == 2
    assert resumed["completed_this_run"] == 0
    assert [call for call in resume_client.calls if call[0] == "stock_st"] == []


def test_daily_stock_st_can_use_an_isolated_audit_checkpoint(tmp_path: Path) -> None:
    config_path, layout = _config(tmp_path)
    audit_directory = tmp_path / "wide-universe" / "audit-source"
    audit_checkpoint = audit_directory / "stock-st-checkpoint.json"
    audit_partition_root = audit_directory / "stock_st"

    result = sync_daily_stock_st(
        "2024-01-02",
        "2024-01-04",
        config_path=config_path,
        layout=layout,
        client=DailyStockStClient(),
        checkpoint_path=audit_checkpoint,
        partition_root=audit_partition_root,
    )

    assert result["status"] == "complete"
    assert Path(result["checkpoint_path"]) == audit_checkpoint
    assert Path(result["partition_root"]) == audit_partition_root
    assert audit_checkpoint.is_file()
    assert not (layout.raw_root / "stock-st-checkpoint.json").exists()
    assert not (layout.raw_root / "stock_st").exists()
    audit_payload = json.loads(audit_checkpoint.read_text(encoding="utf-8"))
    assert audit_payload["partition_root"] == str(audit_partition_root)
    for entry in audit_payload["partitions"].values():
        assert Path(entry["path"]).is_relative_to(audit_partition_root)

    sync_daily_stock_st(
        "2024-01-02",
        "2024-01-04",
        config_path=config_path,
        layout=layout,
        client=DailyStockStClient(),
    )
    shared_payload = json.loads(
        (layout.raw_root / "stock-st-checkpoint.json").read_text(
            encoding="utf-8"
        )
    )
    for trade_date in OPEN_DATES:
        key = f"stock_st/trade_date={trade_date}"
        audit_path = Path(audit_payload["partitions"][key]["path"])
        shared_path = Path(shared_payload["partitions"][key]["path"])
        assert not audit_path.samefile(shared_path)
        assert sha256_file(audit_path) == sha256_file(shared_path)


def test_daily_stock_st_cutoff_view_is_exact_create_only_allowlist(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    source = layout.raw_root / "stock-st-checkpoint.json"
    destination = tmp_path / "wide-universe" / "train" / "stock-st-checkpoint.json"
    sync_daily_stock_st(
        "2024-01-02",
        "2024-01-04",
        config_path=config_path,
        layout=layout,
        client=DailyStockStClient(),
    )

    created = create_daily_stock_st_cutoff_checkpoint(
        "2024-01-02",
        "2024-01-03",
        source_checkpoint_path=source,
        destination_checkpoint_path=destination,
        stage="train",
        config_path=config_path,
        layout=layout,
    )
    view = json.loads(destination.read_text(encoding="utf-8"))

    assert created["created"] is True
    assert created["partitions_created"] == 1
    assert created["partition_count"] == 1
    assert view["kind"] == "factor_lab_daily_stock_st_cutoff_view"
    assert view["stage"] == "train"
    assert view["cutoff_date"] == "2024-01-03"
    assert set(view) == {
        "schema_version",
        "kind",
        "contract_id",
        "status",
        "source",
        "dataset",
        "stage",
        "start_date",
        "cutoff_date",
        "partition_count",
        "official_open_session_count",
        "official_calendar_records_sha256",
        "source_checkpoint_path",
        "source_checkpoint_sha256",
        "partition_root",
        "partition_root_payload_sha256",
        "partitions_payload_sha256",
        "partitions",
        "payload_sha256",
    }
    assert set(view["partitions"]) == {"stock_st/trade_date=2024-01-02"}
    assert not any("2024-01-04" in key for key in view["partitions"])
    partition_root = (
        destination.parent / "stock_st" / "stage=train"
    ).resolve()
    assert Path(view["partition_root"]) == partition_root
    assert created["partition_root"] == str(partition_root)
    assert view["partition_root_payload_sha256"] == (
        sources._daily_stock_st_partition_root_payload_sha256(
            partition_root,
            view["partitions"],
        )
    )
    source_payload = json.loads(source.read_text(encoding="utf-8"))
    key = "stock_st/trade_date=2024-01-02"
    source_partition = Path(source_payload["partitions"][key]["path"])
    copied_partition = Path(view["partitions"][key]["path"])
    assert copied_partition.is_relative_to(partition_root)
    assert not copied_partition.samefile(source_partition)
    assert copied_partition.stat().st_nlink == 1
    assert sha256_file(copied_partition) == sha256_file(source_partition)
    pd.testing.assert_frame_equal(
        pd.read_parquet(copied_partition),
        pd.read_parquet(source_partition),
    )

    resumed = create_daily_stock_st_cutoff_checkpoint(
        "2024-01-02",
        "2024-01-03",
        source_checkpoint_path=source,
        destination_checkpoint_path=destination,
        stage="train",
        config_path=config_path,
        layout=layout,
    )
    assert resumed["created"] is False
    assert resumed["partitions_created"] == 0


def test_daily_stock_st_cutoff_views_use_distinct_stage_roots(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    source = layout.raw_root / "stock-st-checkpoint.json"
    sync_daily_stock_st(
        "2024-01-02",
        "2024-01-04",
        config_path=config_path,
        layout=layout,
        client=DailyStockStClient(),
    )

    view_paths: dict[str, Path] = {}
    for stage in ("train", "validation", "audit"):
        destination = tmp_path / "views" / f"{stage}.json"
        kwargs = (
            {"winner_freeze_payload_sha256": "a" * 64}
            if stage == "audit"
            else {}
        )
        create_daily_stock_st_cutoff_checkpoint(
            "2024-01-02",
            "2024-01-03",
            source_checkpoint_path=source,
            destination_checkpoint_path=destination,
            stage=stage,
            config_path=config_path,
            layout=layout,
            **kwargs,
        )
        view = json.loads(destination.read_text(encoding="utf-8"))
        key = "stock_st/trade_date=2024-01-02"
        view_paths[stage] = Path(view["partitions"][key]["path"])
        assert view_paths[stage].is_relative_to(
            destination.parent / "stock_st" / f"stage={stage}"
        )

    assert not view_paths["train"].samefile(view_paths["validation"])
    assert not view_paths["train"].samefile(view_paths["audit"])
    assert not view_paths["validation"].samefile(view_paths["audit"])
    assert len({sha256_file(path) for path in view_paths.values()}) == 1


def test_daily_stock_st_cutoff_view_refuses_existing_different_partition(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    source = layout.raw_root / "stock-st-checkpoint.json"
    sync_daily_stock_st(
        "2024-01-02",
        "2024-01-04",
        config_path=config_path,
        layout=layout,
        client=DailyStockStClient(),
    )
    destination = tmp_path / "wide-universe" / "train" / "view.json"
    target = (
        destination.parent
        / "stock_st"
        / "stage=train"
        / "trade_date=2024-01-02"
        / "part-000.parquet"
    )
    target.parent.mkdir(parents=True)
    target.write_bytes(b"pre-existing evidence must not be overwritten")
    original = target.read_bytes()

    with pytest.raises(
        ValueError,
        match="refusing to overwrite a different stock_st cutoff-view partition",
    ):
        create_daily_stock_st_cutoff_checkpoint(
            "2024-01-02",
            "2024-01-03",
            source_checkpoint_path=source,
            destination_checkpoint_path=destination,
            stage="train",
            config_path=config_path,
            layout=layout,
        )

    assert target.read_bytes() == original
    assert not destination.exists()


def test_daily_stock_st_checkpoint_root_binding_refuses_path_reuse(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    private_checkpoint = tmp_path / "private" / "stock-st-checkpoint.json"
    first_root = tmp_path / "private" / "stock_st"
    second_root = tmp_path / "other" / "stock_st"
    sync_daily_stock_st(
        "2024-01-02",
        "2024-01-02",
        config_path=config_path,
        layout=layout,
        client=DailyStockStClient(),
        checkpoint_path=private_checkpoint,
        partition_root=first_root,
    )

    second_client = DailyStockStClient()
    with pytest.raises(ValueError, match="already bound to a different"):
        sync_daily_stock_st(
            "2024-01-02",
            "2024-01-02",
            config_path=config_path,
            layout=layout,
            client=second_client,
            checkpoint_path=private_checkpoint,
            partition_root=second_root,
        )
    assert second_client.calls == []

    with pytest.raises(ValueError, match="must be independent"):
        sync_daily_stock_st(
            "2024-01-02",
            "2024-01-02",
            config_path=config_path,
            layout=layout,
            client=DailyStockStClient(),
            checkpoint_path=tmp_path / "collision.json",
            partition_root=layout.raw_root / "stock_st",
        )


def test_daily_stock_st_cutoff_view_refuses_source_root_reuse(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    destination = tmp_path / "views" / "train.json"
    reused_root = destination.parent / "stock_st" / "stage=train"
    source = tmp_path / "source" / "stock-st-checkpoint.json"
    sync_daily_stock_st(
        "2024-01-02",
        "2024-01-04",
        config_path=config_path,
        layout=layout,
        client=DailyStockStClient(),
        checkpoint_path=source,
        partition_root=reused_root,
    )

    with pytest.raises(ValueError, match="partition roots must be physically distinct"):
        create_daily_stock_st_cutoff_checkpoint(
            "2024-01-02",
            "2024-01-03",
            source_checkpoint_path=source,
            destination_checkpoint_path=destination,
            stage="train",
            config_path=config_path,
            layout=layout,
        )
    assert not destination.exists()


def test_daily_stock_st_cutoff_view_refuses_collision_and_audit_without_freeze(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    source = layout.raw_root / "stock-st-checkpoint.json"
    sync_daily_stock_st(
        "2024-01-02",
        "2024-01-04",
        config_path=config_path,
        layout=layout,
        client=DailyStockStClient(),
    )
    destination = tmp_path / "collision.json"
    destination.write_text('{"partitions": {}}', encoding="utf-8")

    with pytest.raises(ValueError, match="refusing to overwrite"):
        create_daily_stock_st_cutoff_checkpoint(
            "2024-01-02",
            "2024-01-04",
            source_checkpoint_path=source,
            destination_checkpoint_path=destination,
            stage="validation",
            config_path=config_path,
            layout=layout,
        )
    assert not (tmp_path / "stock_st" / "stage=validation").exists()
    with pytest.raises(ValueError, match="winner-freeze"):
        create_daily_stock_st_cutoff_checkpoint(
            "2024-01-02",
            "2024-01-04",
            source_checkpoint_path=source,
            destination_checkpoint_path=tmp_path / "audit.json",
            stage="audit",
            config_path=config_path,
            layout=layout,
        )


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (
            lambda date: pd.DataFrame(
                columns=ENRICHMENT_DATASET_FIELDS["stock_st"].split(",")
            ),
            "returned no rows",
        ),
        (
            lambda date: _stock_st_frame(date, row_count=1_000),
            "ambiguous 1000-row endpoint limit",
        ),
        (
            lambda date: _stock_st_frame("20240104", row_count=1),
            "mismatched trade_date",
        ),
        (
            lambda date: pd.concat(
                [_stock_st_frame(date, row_count=1)] * 2, ignore_index=True
            ),
            "duplicate securities",
        ),
        (
            lambda date: pd.concat(
                [
                    _stock_st_frame(date, row_count=1),
                    _stock_st_frame(date, row_count=1).assign(
                        ts_code=" 000001.SZ "
                    ),
                ],
                ignore_index=True,
            ),
            "duplicate normalized tickers",
        ),
        (
            lambda date: _stock_st_frame(date, row_count=1).drop(
                columns="type_name"
            ),
            "missing columns",
        ),
    ],
)
def test_daily_stock_st_malformed_or_ambiguous_response_fails_closed(
    tmp_path: Path,
    response: Callable[[str], pd.DataFrame],
    message: str,
) -> None:
    config_path, layout = _config(tmp_path)

    with pytest.raises(ValueError, match=message):
        sync_daily_stock_st(
            "2024-01-02",
            "2024-01-02",
            config_path=config_path,
            layout=layout,
            client=DailyStockStClient(response),
        )

    checkpoint = json.loads(
        (layout.raw_root / "stock-st-checkpoint.json").read_text(
            encoding="utf-8"
        )
    )
    assert checkpoint["partitions"] == {}
    assert not list(layout.raw_root.glob("stock_st/trade_date=*/part-000.parquet"))


def test_daily_stock_st_concurrent_publish_uses_checkpoint_cas(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    barrier = threading.Barrier(2)

    def run() -> dict[str, Any]:
        return sync_daily_stock_st(
            "2024-01-02",
            "2024-01-02",
            config_path=config_path,
            layout=layout,
            client=DailyStockStClient(barrier=barrier),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _value: run(), range(2)))

    assert [result["status"] for result in results] == ["complete", "complete"]
    assert sum(result["completed_this_run"] for result in results) == 1
    assert sum(result["completed_concurrently"] for result in results) == 1
    checkpoint = json.loads(
        (layout.raw_root / "stock-st-checkpoint.json").read_text(
            encoding="utf-8"
        )
    )
    assert set(checkpoint["partitions"]) == {
        "stock_st/trade_date=2024-01-02"
    }


def _windows_error(code: int) -> PermissionError:
    error = PermissionError(f"simulated WinError {code}")
    error.winerror = code
    return error


@pytest.mark.parametrize("kind", ["json", "parquet"])
@pytest.mark.parametrize("winerror", [5, 32, 33])
def test_atomic_replace_retries_transient_windows_sharing_violation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    winerror: int,
) -> None:
    destination = tmp_path / f"artifact.{kind}"
    original_replace = Path.replace
    calls = 0
    delays: list[float] = []

    def transient_replace(source: Path, target: Path) -> Path:
        nonlocal calls
        calls += 1
        if calls <= 2:
            raise _windows_error(winerror)
        return original_replace(source, target)

    monkeypatch.setattr(Path, "replace", transient_replace)
    monkeypatch.setattr(sources.time, "sleep", delays.append)
    if kind == "json":
        sources._write_json_atomic(destination, {"value": 1})
        assert json.loads(destination.read_text(encoding="utf-8")) == {
            "value": 1
        }
    else:
        sources._write_parquet_atomic(
            destination, pd.DataFrame({"value": [1]})
        )
        assert pd.read_parquet(destination)["value"].tolist() == [1]

    assert calls == 3
    assert delays == [0.01, 0.02]


@pytest.mark.parametrize("kind", ["json", "parquet"])
def test_atomic_replace_persistent_windows_lock_remains_fatal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    destination = tmp_path / f"artifact.{kind}"
    destination.write_bytes(b"original")
    calls = 0

    def persistently_locked(_source: Path, _target: Path) -> Path:
        nonlocal calls
        calls += 1
        raise _windows_error(33)

    monkeypatch.setattr(Path, "replace", persistently_locked)
    monkeypatch.setattr(sources.time, "sleep", lambda _seconds: None)
    with pytest.raises(PermissionError, match="WinError 33"):
        if kind == "json":
            sources._write_json_atomic(destination, {"value": 1})
        else:
            sources._write_parquet_atomic(
                destination, pd.DataFrame({"value": [1]})
            )

    assert calls == sources._WINDOWS_ATOMIC_REPLACE_MAX_ATTEMPTS
    assert destination.read_bytes() == b"original"
    assert not list(tmp_path.glob(f".{destination.name}.*.tmp"))


def test_cli_exposes_daily_stock_st_resume_and_partition_budget() -> None:
    arguments = build_parser().parse_args(
        [
            "data",
            "stock-st",
            "--from",
            "2017-01-03",
            "--to",
            "2024-12-31",
            "--no-resume",
            "--max-partitions",
            "17",
            "--checkpoint",
            "runtime/data/raw/stock-st-audit-checkpoint.json",
        ]
    )

    assert arguments.data_command == "stock-st"
    assert arguments.start_date == "2017-01-03"
    assert arguments.end_date == "2024-12-31"
    assert arguments.resume is False
    assert arguments.max_partitions == 17
    assert arguments.checkpoint == Path(
        "runtime/data/raw/stock-st-audit-checkpoint.json"
    )
