from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from factor_lab.data import RuntimeLayout, audit_suspensions_snapshot, sync_suspensions
from factor_lab.data.catalog import sha256_file


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
            "token_env": "TUSHARE_TOKEN",
            "request_rate_per_minute": 0,
        },
    }
    path = tmp_path / "configs" / "data.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path, RuntimeLayout.from_config(payload, config_path=path)


class SuspensionClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        first_year = pd.DataFrame(
            {
                "ts_code": [f"{index:06d}.SZ" for index in range(5_001)],
                "trade_date": ["20231231"] * 5_001,
                "suspend_type": ["S" if index % 2 == 0 else "R" for index in range(5_001)],
                "suspend_timing": ["09:30"] * 5_001,
            }
        )
        second_year = pd.DataFrame(
            [
                {
                    "ts_code": "600001.SH",
                    "trade_date": "20240102",
                    "suspend_type": "s",
                    "suspend_timing": "09:30",
                },
                {
                    "ts_code": "600002.SH",
                    "trade_date": "20240101",
                    "suspend_type": "R",
                    "suspend_timing": None,
                },
                {
                    "ts_code": "600001.SH",
                    "trade_date": "20240102",
                    "suspend_type": "S",
                    "suspend_timing": "09:30",
                },
                {
                    "ts_code": "600001.SH",
                    "trade_date": "20240102",
                    "suspend_type": "R",
                    "suspend_timing": None,
                },
            ]
        )
        self.frames = {"20231230": first_year, "20240101": second_year}

    def query(self, endpoint: str, **kwargs: object) -> pd.DataFrame:
        assert endpoint == "suspend_d"
        self.calls.append(dict(kwargs))
        frame = self.frames[str(kwargs["start_date"])]
        offset = int(kwargs["offset"])
        limit = int(kwargs["limit"])
        return frame.iloc[offset : offset + limit].copy()


def test_sync_suspensions_pages_by_year_and_resumes_verified_output(tmp_path: Path) -> None:
    config_path, layout = _config(tmp_path)
    client = SuspensionClient()

    first = sync_suspensions(
        "2023-12-30",
        "2024-01-02",
        config_path=config_path,
        layout=layout,
        client=client,
        resume=True,
    )

    assert first["status"] == "complete"
    assert first["resumed"] is False
    assert first["request_count"] == 3
    assert [(call["start_date"], call["end_date"], call["limit"], call["offset"]) for call in client.calls] == [
        ("20231230", "20231231", 5_000, 0),
        ("20231230", "20231231", 5_000, 5_000),
        ("20240101", "20240102", 5_000, 0),
    ]
    target = layout.top500_root / "suspensions.parquet"
    metadata_path = layout.top500_root / "suspensions.meta.json"
    saved = pd.read_parquet(target)
    assert list(saved.columns) == ["ticker", "date", "suspend_type", "suspend_timing"]
    assert len(saved) == first["rows"] == 5_004
    assert not saved.duplicated(list(saved.columns)).any()
    assert saved.equals(
        saved.sort_values(
            ["date", "ticker", "suspend_type", "suspend_timing"], na_position="last"
        ).reset_index(drop=True)
    )
    assert first["date"] == {"min": "2023-12-31", "max": "2024-01-02"}
    assert first["S"] + first["R"] == first["rows"]
    assert first["hash"] == sha256_file(target)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["query"] == {
        "start_date": "2023-12-30",
        "end_date": "2024-01-02",
        "limit": 5_000,
        "window": "calendar_year",
    }
    assert metadata["file"]["sha256"] == first["hash"]
    assert not list(layout.top500_root.glob("*.tmp"))

    audited = audit_suspensions_snapshot(
        target,
        requested_start="2023-12-30",
        requested_end="2024-01-02",
    )
    assert audited["status"] == "complete"
    assert audited["hash"] == first["hash"]
    assert audited["S"] + audited["R"] == audited["rows"]
    same_day = saved.loc[
        saved["ticker"].eq("600001.SH")
        & saved["date"].eq(pd.Timestamp("2024-01-02"))
    ]
    assert set(same_day["suspend_type"]) == {"S", "R"}
    with pytest.raises(ValueError, match="provided together"):
        audit_suspensions_snapshot(target, requested_start="2023-12-30")

    class NoCallClient:
        def query(self, endpoint: str, **kwargs: object) -> pd.DataFrame:  # pragma: no cover
            raise AssertionError("verified resume must not call Tushare")

    resumed = sync_suspensions(
        "2024-01-01",
        "2024-01-02",
        config_path=config_path,
        layout=layout,
        client=NoCallClient(),
        resume=True,
    )
    assert resumed["resumed"] is True
    assert resumed["request_count"] == 0
    assert resumed["hash"] == first["hash"]


def test_sync_suspensions_resume_fails_closed_on_hash_or_range_mismatch(
    tmp_path: Path,
) -> None:
    config_path, layout = _config(tmp_path)
    first = sync_suspensions(
        "2023-12-30",
        "2024-01-02",
        config_path=config_path,
        layout=layout,
        client=SuspensionClient(),
    )
    target = Path(first["path"])
    changed = pd.read_parquet(target)
    changed.iloc[:-1].to_parquet(target, index=False)

    with pytest.raises(ValueError, match="size|hash"):
        sync_suspensions(
            "2023-12-30",
            "2024-01-02",
            config_path=config_path,
            layout=layout,
            client=SuspensionClient(),
            resume=True,
        )

    target.unlink()
    (layout.top500_root / "suspensions.meta.json").unlink()
    sync_suspensions(
        "2023-12-30",
        "2024-01-02",
        config_path=config_path,
        layout=layout,
        client=SuspensionClient(),
    )
    with pytest.raises(ValueError, match="does not cover"):
        sync_suspensions(
            "2023-01-01",
            "2024-01-02",
            config_path=config_path,
            layout=layout,
            client=SuspensionClient(),
            resume=True,
        )


def test_sync_suspensions_rejects_malformed_nonempty_page(tmp_path: Path) -> None:
    config_path, layout = _config(tmp_path)

    class BadClient:
        def query(self, endpoint: str, **kwargs: object) -> pd.DataFrame:
            return pd.DataFrame(
                [{"ts_code": "000001.SZ", "trade_date": "20240102", "suspend_type": "S"}]
            )

    with pytest.raises(ValueError, match="missing columns"):
        sync_suspensions(
            "2024-01-01",
            "2024-01-02",
            config_path=config_path,
            layout=layout,
            client=BadClient(),
            resume=False,
        )
