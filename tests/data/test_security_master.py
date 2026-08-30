from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd
import pytest

from factor_lab.data.catalog import RuntimeLayout
import factor_lab.data.security_master as security_master
from factor_lab.data.security_master import (
    SECURITY_MASTER_FIELDS,
    audit_security_master,
    load_security_master,
    security_master_checkpoint_path,
    sync_security_master,
)


OBSERVED = datetime(2026, 8, 30, 4, 0, tzinfo=timezone.utc)


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
            "checkpoint_file": "daily-market-checkpoint.json",
            "request_rate_per_minute": 0,
        },
    }
    path = tmp_path / "configs" / "data.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path, payload


def _row(
    status: str,
    *,
    ticker: str | None = None,
    name: str | None = None,
    list_date: str = "20100101",
    delist_date: str | None = None,
) -> dict[str, object]:
    default_ticker = {"L": "600000.SH", "D": "000001.SZ", "P": "920001.BJ"}[status]
    return {
        "ts_code": ticker or default_ticker,
        "symbol": (ticker or default_ticker).split(".")[0],
        "name": name or f"sample-{status}",
        "area": "上海" if status == "L" else "深圳",
        "industry": "银行",
        "fullname": f"sample company {status}",
        "enname": f"sample {status}",
        "cnspell": f"sample{status.lower()}",
        "market": "主板" if status != "P" else "北交所",
        "exchange": (ticker or default_ticker).split(".")[1],
        "curr_type": "CNY",
        "list_status": status,
        "list_date": list_date,
        "delist_date": delist_date,
        "is_hs": "N",
    }


def _responses(*, empty_p: bool = False) -> dict[str, pd.DataFrame]:
    return {
        "L": pd.DataFrame([_row("L")], columns=SECURITY_MASTER_FIELDS),
        "D": pd.DataFrame(
            [_row("D", list_date="19910403", delist_date="20240131")],
            columns=SECURITY_MASTER_FIELDS,
        ),
        "P": (
            pd.DataFrame()
            if empty_p
            else pd.DataFrame(
                [_row("P", list_date="20260915")], columns=SECURITY_MASTER_FIELDS
            )
        ),
    }


class FakeClient:
    def __init__(self, responses: dict[str, pd.DataFrame]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, object]]] = []

    def query(self, endpoint: str, **kwargs: object) -> pd.DataFrame:
        self.calls.append((endpoint, kwargs))
        assert endpoint == "stock_basic"
        return self.responses[str(kwargs["list_status"])].copy()


@pytest.fixture(autouse=True)
def _fixed_observation_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(security_master, "_now_utc", lambda: OBSERVED)


def test_sync_queries_all_statuses_and_publishes_content_addressed_snapshot(
    tmp_path: Path,
) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    client = FakeClient(_responses())

    result = sync_security_master(layout, config_path=config_path, client=client)

    assert result["status"] == "complete"
    assert result["status_counts"] == {"L": 1, "D": 1, "P": 1}
    assert [kwargs["list_status"] for _, kwargs in client.calls] == ["L", "D", "P"]
    assert all(kwargs["exchange"] == "" for _, kwargs in client.calls)
    assert all(
        kwargs["fields"] == ",".join(SECURITY_MASTER_FIELDS)
        for _, kwargs in client.calls
    )

    snapshot = Path(result["snapshot_path"])
    assert snapshot.parent == layout.raw_root / "reference" / "stock_basic"
    assert snapshot.name == f"snapshot_sha256={result['snapshot_sha256']}"
    assert {path.name for path in snapshot.iterdir()} == {
        "part-000.parquet",
        "manifest.json",
    }
    manifest = json.loads((snapshot / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["selection_contract"] == {
        "all_statuses_queried": ["L", "D", "P"],
        "includes_historical_delisted": True,
        "current_listed_only": False,
        "exchange_filter_applied": False,
        "currency_filter_applied": False,
    }
    assert manifest["queries"]["P"]["empty_allowed"] is True
    assert manifest["queries"]["L"]["empty_allowed"] is False
    assert security_master_checkpoint_path(layout) != layout.checkpoint_path
    assert audit_security_master(layout)["status"] == "pass"

    loaded = load_security_master(layout)
    assert loaded["ts_code"].is_unique
    assert set(loaded["list_status"]) == {"L", "D", "P"}
    assert loaded.loc[loaded["list_status"].eq("D"), "delist_date"].notna().all()


def test_paused_listing_status_is_explicitly_allowed_to_be_empty(tmp_path: Path) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)

    result = sync_security_master(
        layout,
        config_path=config_path,
        client=FakeClient(_responses(empty_p=True)),
    )

    assert result["status_counts"] == {"L": 1, "D": 1, "P": 0}
    manifest = json.loads(
        (Path(result["snapshot_path"]) / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["queries"]["P"] == {
        "list_status": "P",
        "row_count": 0,
        "empty_allowed": True,
    }
    assert audit_security_master(layout)["status"] == "pass"


@pytest.mark.parametrize("status", ["L", "D"])
def test_listed_and_delisted_provider_responses_must_be_nonempty(
    tmp_path: Path, status: str
) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    responses = _responses()
    responses[status] = pd.DataFrame()

    with pytest.raises(ValueError, match=f"list_status={status} returned no rows"):
        sync_security_master(
            layout, config_path=config_path, client=FakeClient(responses)
        )
    assert not security_master_checkpoint_path(layout).exists()


def test_query_and_returned_list_status_must_match(tmp_path: Path) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    responses = _responses()
    responses["D"].loc[:, "list_status"] = "L"

    with pytest.raises(ValueError, match="query/response list_status mismatch"):
        sync_security_master(
            layout, config_path=config_path, client=FakeClient(responses)
        )


def test_ts_code_is_globally_unique_across_status_queries(tmp_path: Path) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    responses = _responses()
    responses["D"].loc[:, "ts_code"] = "600000.SH"

    with pytest.raises(ValueError, match="duplicate ts_code"):
        sync_security_master(
            layout, config_path=config_path, client=FakeClient(responses)
        )


def test_delisted_vendor_historical_t_code_is_preserved(tmp_path: Path) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    responses = _responses()
    responses["D"] = pd.DataFrame(
        [
            _row(
                "D",
                ticker="T600018.SH",
                list_date="20000719",
                delist_date="20061020",
            )
        ],
        columns=SECURITY_MASTER_FIELDS,
    )

    result = sync_security_master(
        layout, config_path=config_path, client=FakeClient(responses)
    )

    loaded = load_security_master(layout)
    assert loaded.loc[loaded["list_status"].eq("D"), "ts_code"].tolist() == [
        "T600018.SH"
    ]
    manifest = json.loads(
        (Path(result["snapshot_path"]) / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["snapshot_sha256"] == security_master.security_master_content_sha256(
        loaded
    )
    assert audit_security_master(layout)["status"] == "pass"


@pytest.mark.parametrize("status", ["L", "P"])
def test_vendor_historical_t_code_is_not_accepted_for_listed_or_paused_status(
    tmp_path: Path, status: str
) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    responses = _responses()
    responses[status].loc[:, "ts_code"] = "T600018.SH"

    with pytest.raises(ValueError, match="invalid or empty ts_code"):
        sync_security_master(
            layout, config_path=config_path, client=FakeClient(responses)
        )


@pytest.mark.parametrize(
    ("status", "changes", "message"),
    [
        ("L", {"list_date": "not-a-date"}, "invalid or empty list_date"),
        ("D", {"delist_date": None}, "require a valid delist_date"),
        (
            "D",
            {"list_date": "20240102", "delist_date": "20240101"},
            "on or after list_date",
        ),
        ("L", {"delist_date": "20200101"}, "cannot have a past delist_date"),
    ],
)
def test_listing_and_delisting_date_semantics_are_enforced(
    tmp_path: Path,
    status: str,
    changes: dict[str, object],
    message: str,
) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    responses = _responses()
    for column, value in changes.items():
        responses[status].loc[:, column] = value

    with pytest.raises(ValueError, match=message):
        sync_security_master(
            layout, config_path=config_path, client=FakeClient(responses)
        )


def test_verified_checkpoint_resumes_without_provider_calls(tmp_path: Path) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    first = sync_security_master(
        layout, config_path=config_path, client=FakeClient(_responses())
    )

    class NoCallClient:
        def query(self, _endpoint: str, **_kwargs: object) -> pd.DataFrame:
            raise AssertionError("a verified resume must not call the provider")

    second = sync_security_master(
        layout, config_path=config_path, client=NoCallClient(), resume=True
    )

    assert second["snapshot_sha256"] == first["snapshot_sha256"]
    assert second["completed_before"] == 1
    assert second["completed_this_run"] == 0
    assert second["resumed"] is True


def test_tampered_parquet_is_detected_and_resume_refetches_and_repairs(
    tmp_path: Path,
) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    first = sync_security_master(
        layout, config_path=config_path, client=FakeClient(_responses())
    )
    part = Path(first["snapshot_path"]) / "part-000.parquet"
    part.write_bytes(part.read_bytes() + b"tampered")

    assert audit_security_master(layout)["status"] == "fail"
    with pytest.raises(ValueError, match="snapshot audit failed"):
        load_security_master(layout)

    repair_client = FakeClient(_responses())
    repaired = sync_security_master(
        layout, config_path=config_path, client=repair_client, resume=True
    )

    assert len(repair_client.calls) == 3
    assert repaired["snapshot_sha256"] == first["snapshot_sha256"]
    assert repaired["completed_this_run"] == 1
    assert audit_security_master(layout)["status"] == "pass"


def test_tampered_manifest_is_detected_and_repaired(tmp_path: Path) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    first = sync_security_master(
        layout, config_path=config_path, client=FakeClient(_responses())
    )
    manifest_path = Path(first["snapshot_path"]) / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["status_counts"]["D"] = 0
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    audited = audit_security_master(layout)
    assert audited["status"] == "fail"
    assert "manifest_sha256_mismatch" in audited["issues"]

    sync_security_master(
        layout,
        config_path=config_path,
        client=FakeClient(_responses()),
        resume=True,
    )
    assert audit_security_master(layout)["status"] == "pass"


def test_forced_refresh_preserves_old_snapshot_and_delisted_history(
    tmp_path: Path,
) -> None:
    config_path, payload = _config(tmp_path)
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    first = sync_security_master(
        layout, config_path=config_path, client=FakeClient(_responses())
    )
    changed = _responses()
    changed["L"].loc[:, "name"] = "renamed-listed-company"

    second = sync_security_master(
        layout,
        config_path=config_path,
        client=FakeClient(changed),
        resume=False,
    )

    assert second["snapshot_sha256"] != first["snapshot_sha256"]
    assert Path(first["snapshot_path"]).is_dir()
    assert Path(second["snapshot_path"]).is_dir()
    checkpoint = json.loads(
        security_master_checkpoint_path(layout).read_text(encoding="utf-8")
    )
    assert set(checkpoint["snapshots"]) == {
        first["snapshot_sha256"],
        second["snapshot_sha256"],
    }
    old = load_security_master(layout, snapshot_sha256=first["snapshot_sha256"])
    current = load_security_master(layout)
    assert old.loc[old["list_status"].eq("D"), "ts_code"].tolist() == ["000001.SZ"]
    assert current.loc[current["list_status"].eq("D"), "ts_code"].tolist() == [
        "000001.SZ"
    ]
    assert current.loc[current["list_status"].eq("L"), "name"].item() == (
        "renamed-listed-company"
    )
