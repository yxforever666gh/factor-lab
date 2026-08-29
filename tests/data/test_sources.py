from __future__ import annotations

import inspect
import json
from pathlib import Path

import pandas as pd
import pytest

from factor_lab.data import RuntimeLayout, sync_data, turnover_amount_to_rmb
import factor_lab.data.sources as sources
from factor_lab.data.sources import DATASET_FIELDS


class FixtureClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def query(self, endpoint: str, **kwargs) -> pd.DataFrame:
        self.calls.append((endpoint, kwargs))
        if endpoint == "trade_cal":
            return pd.DataFrame(
                {
                    "exchange": ["SSE", "SSE", "SSE"],
                    "cal_date": ["20240102", "20240103", "20240104"],
                    "is_open": [1, 0, 1],
                    "pretrade_date": ["20231229", "20240102", "20240102"],
                }
            )
        fields = [field for field in DATASET_FIELDS[endpoint].split(",") if field]
        row = {field: 1.0 for field in fields}
        row["ts_code"] = "000001.SZ"
        row["trade_date"] = kwargs["trade_date"]
        return pd.DataFrame([row], columns=fields)


def test_turnover_amount_units_are_source_aware() -> None:
    tushare_thousand_rmb = pd.Series([274_337.1858])
    akshare_rmb = pd.Series([274_337_186.0])

    tushare = turnover_amount_to_rmb(tushare_thousand_rmb, source="tushare_daily")
    akshare = turnover_amount_to_rmb(akshare_rmb, source="akshare")
    estimate = turnover_amount_to_rmb(akshare_rmb, source="turnover_estimate_rmb")

    assert abs(tushare.iloc[0] - 274_337_185.8) < 0.001
    assert abs(tushare.iloc[0] - akshare.iloc[0]) < 1.0
    assert estimate.iloc[0] == akshare.iloc[0]


def _config(tmp_path: Path) -> tuple[Path, dict]:
    payload = {
        "schema_version": 1,
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
            "datasets": ["daily", "daily_basic", "adj_factor"],
            "checkpoint_file": "checkpoint.json",
            "request_rate_per_minute": 0,
            "verify_hashes_on_resume": True,
        },
    }
    path = tmp_path / "configs" / "data.json"
    path.parent.mkdir()
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path, payload


def test_sync_writes_daily_partitions_and_resumes_verified_checkpoint(tmp_path: Path) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    client = FixtureClient()

    first = sync_data(
        "2024-01-02",
        "2024-01-04",
        config_path=config_path,
        layout=layout,
        client=client,
    )
    first_data_calls = [name for name, _ in client.calls if name != "trade_cal"]
    assert first["status"] == "complete"
    assert first["completed_this_run"] == 6
    assert len(first_data_calls) == 6
    assert layout.checkpoint_path.is_file()

    second_client = FixtureClient()
    second = sync_data(
        "2024-01-02",
        "2024-01-04",
        config_path=config_path,
        layout=layout,
        client=second_client,
        resume=True,
    )
    assert second["completed_before"] == 6
    assert second["completed_this_run"] == 0
    assert [name for name, _ in second_client.calls if name != "trade_cal"] == []


def test_sync_treats_empty_open_market_partition_as_retryable(tmp_path: Path) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)

    class EmptyClient(FixtureClient):
        def query(self, endpoint: str, **kwargs) -> pd.DataFrame:
            if endpoint == "trade_cal":
                return super().query(endpoint, **kwargs)
            return pd.DataFrame(columns=DATASET_FIELDS[endpoint].split(","))

    result = sync_data(
        "2024-01-02",
        "2024-01-02",
        config_path=config_path,
        layout=layout,
        client=EmptyClient(),
        datasets=("daily",),
    )

    assert result["status"] == "waiting"
    assert result["reason"] == "provider_empty"
    assert result["dataset"] == "daily"
    assert result["trade_date"] == "2024-01-02"
    assert result["completed_this_run"] == 0
    assert result["remaining_partition_count"] == 1
    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    assert "daily/2024-01-02" not in checkpoint["partitions"]


def test_sync_treats_empty_trade_calendar_as_retryable(tmp_path: Path) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)

    class EmptyCalendarClient(FixtureClient):
        def query(self, endpoint: str, **kwargs) -> pd.DataFrame:
            if endpoint == "trade_cal":
                return pd.DataFrame(
                    columns=["exchange", "cal_date", "is_open", "pretrade_date"]
                )
            return super().query(endpoint, **kwargs)

    result = sync_data(
        "2024-01-02",
        "2024-01-02",
        config_path=config_path,
        layout=layout,
        client=EmptyCalendarClient(),
        datasets=("daily",),
    )

    assert result["status"] == "waiting"
    assert result["reason"] == "provider_empty"
    assert result["dataset"] == "trade_cal"
    assert result["completed_this_run"] == 0


GUARDED_DATE = "2026-08-24"
GUARDED_COMPACT = "20260824"
GUARDED_TICKERS = ("000001.SZ", "600000.SH")


def _guarded_calendar() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "exchange": ["SSE"],
            "cal_date": [GUARDED_COMPACT],
            "is_open": [1],
            "pretrade_date": ["20260821"],
        }
    )


def _guarded_frame(dataset: str, tickers=GUARDED_TICKERS) -> pd.DataFrame:
    fields = DATASET_FIELDS[dataset].split(",")
    rows = []
    for index, ticker in enumerate(tickers, start=1):
        row = {field: float(index) for field in fields}
        row["ts_code"] = ticker
        row["trade_date"] = GUARDED_COMPACT
        if dataset == "adj_factor":
            row["adj_factor"] = float(index)
        rows.append(row)
    return pd.DataFrame(rows, columns=fields)


class GuardedClient:
    def __init__(self, samples: list[dict[str, pd.DataFrame]]) -> None:
        self.samples = samples
        self.calls: list[str] = []
        self.dataset_calls = 0

    def query(self, endpoint: str, **_kwargs) -> pd.DataFrame:
        self.calls.append(endpoint)
        if endpoint == "trade_cal":
            return _guarded_calendar()
        sample_index = self.dataset_calls // len(sources.PROVIDER_COMPLETION_DATASETS)
        self.dataset_calls += 1
        return self.samples[sample_index][endpoint].copy()


def _stable_samples(*, tickers=GUARDED_TICKERS) -> list[dict[str, pd.DataFrame]]:
    first = {
        dataset: _guarded_frame(dataset, tickers)
        for dataset in sources.PROVIDER_COMPLETION_DATASETS
    }
    second = {
        dataset: frame.iloc[::-1].reset_index(drop=True)
        for dataset, frame in first.items()
    }
    return [first, second]


def test_post_bridge_sync_requires_two_canonical_stable_full_bundle_samples(
    tmp_path: Path,
) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    client = GuardedClient(_stable_samples())

    result = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=client,
    )

    assert result["status"] == "complete"
    assert client.calls == [
        "trade_cal",
        "daily",
        "daily_basic",
        "adj_factor",
        "daily",
        "daily_basic",
        "adj_factor",
    ]
    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    proofs = {
        checkpoint["partitions"][f"{dataset}/{GUARDED_DATE}"][
            "provider_completion"
        ]["evidence_sha256"]
        for dataset in sources.PROVIDER_COMPLETION_DATASETS
    }
    assert len(proofs) == 1
    for dataset in sources.PROVIDER_COMPLETION_DATASETS:
        entry = checkpoint["partitions"][f"{dataset}/{GUARDED_DATE}"]
        evidence = entry["provider_completion"]
        assert evidence["contract_id"] == sources.PROVIDER_COMPLETION_CONTRACT_ID
        assert evidence["sample_count"] == 2
        assert evidence["not_before_local_time"] == "17:10:00"
        assert evidence["relations"] == {
            "daily_equals_daily_basic": True,
            "daily_subset_of_adj_factor": True,
        }


def test_post_bridge_partial_then_full_is_unstable_and_retryable(
    tmp_path: Path,
) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    partial = {
        dataset: _guarded_frame(dataset, GUARDED_TICKERS[:1])
        for dataset in sources.PROVIDER_COMPLETION_DATASETS
    }
    full = {
        dataset: _guarded_frame(dataset)
        for dataset in sources.PROVIDER_COMPLETION_DATASETS
    }

    waiting = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=GuardedClient([partial, full]),
    )
    assert waiting["status"] == "waiting"
    assert waiting["reason"] == "provider_response_unstable"
    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    assert checkpoint["partitions"] == {}

    complete = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=GuardedClient(_stable_samples()),
    )
    assert complete["status"] == "complete"


def test_post_bridge_stable_response_before_protocol_gate_waits_without_query(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    client = GuardedClient(_stable_samples())
    monkeypatch.setattr(
        sources,
        "_now_utc",
        lambda: pd.Timestamp("2026-08-24T09:09:59Z").to_pydatetime(),
    )

    result = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=client,
    )

    assert result["status"] == "waiting"
    assert result["reason"] == "before_provider_completion_gate"
    assert result["provider_completion_not_before_utc"] == (
        "2026-08-24T09:10:00+00:00"
    )
    assert client.calls == ["trade_cal"]


def test_post_bridge_cross_endpoint_universe_mismatch_is_retryable(
    tmp_path: Path,
) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    mismatched = {
        "daily": _guarded_frame("daily"),
        "daily_basic": _guarded_frame("daily_basic", GUARDED_TICKERS[:1]),
        "adj_factor": _guarded_frame("adj_factor"),
    }
    client = GuardedClient([mismatched, mismatched])

    result = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=client,
    )

    assert result["status"] == "waiting"
    assert result["reason"] == "provider_universe_incomplete"
    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    assert checkpoint["partitions"] == {}


def test_post_bridge_endpoint_row_limit_is_ambiguous_and_retryable(
    tmp_path: Path,
) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    tickers = tuple(f"{index:06d}.SZ" for index in range(6_000))

    result = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=GuardedClient(_stable_samples(tickers=tickers)),
    )

    assert result["status"] == "waiting"
    assert result["reason"] == "provider_response_at_row_limit"
    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    assert checkpoint["partitions"] == {}


def test_resume_reconciles_legacy_post_bridge_partial_checkpoint(
    tmp_path: Path,
) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    layout.ensure_directories()
    legacy_ticker = (GUARDED_TICKERS[0],)
    partitions = {}
    for dataset in sources.PROVIDER_COMPLETION_DATASETS:
        path = sources._partition_path(layout.raw_root, dataset, GUARDED_DATE)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame = _guarded_frame(dataset, legacy_ticker)
        frame.to_parquet(path, index=False)
        partitions[f"{dataset}/{GUARDED_DATE}"] = {
            "status": "complete",
            "dataset": dataset,
            "trade_date": GUARDED_DATE,
            "path": str(path),
            "row_count": len(frame),
            "size_bytes": path.stat().st_size,
            "sha256": sources.sha256_file(path),
            "completed_at_utc": "2026-08-24T08:00:00Z",
        }
    layout.checkpoint_path.write_text(
        json.dumps({"schema_version": 1, "partitions": partitions}),
        encoding="utf-8",
    )

    result = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=GuardedClient(_stable_samples()),
        resume=True,
    )

    assert result["status"] == "complete"
    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    for dataset in sources.PROVIDER_COMPLETION_DATASETS:
        entry = checkpoint["partitions"][f"{dataset}/{GUARDED_DATE}"]
        assert entry["row_count"] == 2
        assert entry["reconciliation"]["previous_status"] == "complete"
        assert entry["provider_completion"]["sample_count"] == 2


@pytest.mark.parametrize("malformed", [None, "forged", []])
def test_resume_blocks_present_malformed_completion_evidence_without_mutation(
    tmp_path: Path,
    malformed: object,
) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    first = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=GuardedClient(_stable_samples()),
    )
    assert first["status"] == "complete"
    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    checkpoint["partitions"][f"daily/{GUARDED_DATE}"][
        "provider_completion"
    ] = malformed
    layout.checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")
    before_partitions = checkpoint["partitions"]
    before_bytes = {
        dataset: sources._partition_path(
            layout.raw_root, dataset, GUARDED_DATE
        ).read_bytes()
        for dataset in sources.PROVIDER_COMPLETION_DATASETS
    }

    result = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=GuardedClient(_stable_samples()),
        resume=True,
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "provider_revision_conflict"
    assert json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))[
        "partitions"
    ] == before_partitions
    assert {
        dataset: sources._partition_path(
            layout.raw_root, dataset, GUARDED_DATE
        ).read_bytes()
        for dataset in sources.PROVIDER_COMPLETION_DATASETS
    } == before_bytes


@pytest.mark.parametrize(
    ("field", "mutate"),
    [
        ("dataset", lambda value: "daily_basic"),
        ("trade_date", lambda value: "2026-08-25"),
        ("path", lambda value: str(Path(value).with_name("wrong.parquet"))),
        ("row_count", lambda value: int(value) + 1),
    ],
)
def test_resume_blocks_guarded_checkpoint_identity_tamper_without_mutation(
    tmp_path: Path,
    field: str,
    mutate,
) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    first = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=GuardedClient(_stable_samples()),
    )
    assert first["status"] == "complete"
    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    daily_entry = checkpoint["partitions"][f"daily/{GUARDED_DATE}"]
    daily_entry[field] = mutate(daily_entry[field])
    layout.checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")
    before_partitions = checkpoint["partitions"]
    before_bytes = {
        dataset: sources._partition_path(
            layout.raw_root, dataset, GUARDED_DATE
        ).read_bytes()
        for dataset in sources.PROVIDER_COMPLETION_DATASETS
    }

    result = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=GuardedClient(_stable_samples()),
        resume=True,
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "provider_revision_conflict"
    assert json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))[
        "partitions"
    ] == before_partitions
    assert {
        dataset: sources._partition_path(
            layout.raw_root, dataset, GUARDED_DATE
        ).read_bytes()
        for dataset in sources.PROVIDER_COMPLETION_DATASETS
    } == before_bytes


def test_resume_never_reuses_a_guarded_canonical_partition_symlink(
    tmp_path: Path,
) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    first = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=GuardedClient(_stable_samples()),
    )
    assert first["status"] == "complete"
    path = sources._partition_path(layout.raw_root, "daily", GUARDED_DATE)
    target = path.with_name("same-bytes-target.parquet")
    target.write_bytes(path.read_bytes())
    path.unlink()
    try:
        path.symlink_to(target)
    except OSError as exc:  # pragma: no cover - Windows without symlink support
        pytest.skip(f"symlinks unavailable: {exc}")

    result = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=GuardedClient(_stable_samples()),
        resume=True,
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "provider_revision_conflict"
    assert path.is_symlink()
    assert target.is_file()
    from factor_lab.data import prospective_execution, prospective_readiness

    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    assert prospective_readiness._partition_result(
        layout.repo_root,
        checkpoint,
        "daily",
        GUARDED_DATE,
        pd.Timestamp("2030-01-01T00:00:00Z").to_pydatetime(),
    )["status"] == "invalid"
    with pytest.raises(
        prospective_execution.ProspectiveExecutionDataError,
        match="symlink",
    ):
        prospective_execution._inspect_checkpoint_partition(
            layout.repo_root,
            checkpoint,
            dataset="daily",
            trade_date=GUARDED_DATE,
        )


def test_clock_rollback_during_stability_sampling_waits_before_partition_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    timestamps = iter(
        pd.Timestamp(value).to_pydatetime()
        for value in (
            "2026-08-24T09:10:00Z",
            "2026-08-24T09:10:01Z",
            "2026-08-24T09:10:02Z",
            "2026-08-24T09:10:01Z",
            "2026-08-24T09:10:03Z",
        )
    )
    monkeypatch.setattr(sources, "_now_utc", lambda: next(timestamps))

    result = sync_data(
        GUARDED_DATE,
        GUARDED_DATE,
        config_path=config_path,
        layout=layout,
        client=GuardedClient(_stable_samples()),
    )

    assert result["status"] == "waiting"
    assert result["reason"] == "provider_sample_observation_invalid"
    checkpoint = json.loads(layout.checkpoint_path.read_text(encoding="utf-8"))
    assert checkpoint["partitions"] == {}
    assert all(
        not sources._partition_path(
            layout.raw_root, dataset, GUARDED_DATE
        ).exists()
        for dataset in sources.PROVIDER_COMPLETION_DATASETS
    )


def test_production_sync_has_no_observed_time_injection_and_bridge_is_legacy() -> None:
    assert "observed_at_utc" not in inspect.signature(sync_data).parameters
    with pytest.raises(TypeError, match="observations"):
        sources._build_provider_completion_evidence(  # type: ignore[call-arg]
            GUARDED_DATE, _stable_samples()
        )
    bridge_frame = _guarded_frame("daily")
    assert sources.provider_completion_required("2026-08-21") is False
    assert (
        sources.validate_provider_completion_evidence(
            {"completed_at_utc": "2026-08-21T09:30:00Z"},
            bridge_frame.assign(trade_date="20260821"),
            dataset="daily",
            trade_date="2026-08-21",
            required_datasets=sources.PROVIDER_COMPLETION_DATASETS,
        )
        is None
    )
