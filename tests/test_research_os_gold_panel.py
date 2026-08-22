from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from factor_lab.research_os.catalog import ResearchCatalog
from factor_lab.research_os.bitemporal import (
    CanonicalizationSpec,
    canonicalize_batch,
)
from factor_lab.research_os.contracts import DataSnapshotRef, LabelSpec, UniverseSpec
from factor_lab.research_os.data_sources import (
    DatasetContract,
    FetchRequest,
    FieldContract,
    SourceBatch,
    SourceContractError,
    validate_source_frame,
)
from factor_lab.research_os.gold_panel import (
    DEFAULT_SILVER_SNAPSHOT_DISCOVERY_CAP,
    GoldPanelError,
    ResearchGoldPanelService,
    SilverSnapshotInput,
    build_research_gold_panel,
    discover_cataloged_silver_inputs,
    load_gold_research_panel,
    read_verified_silver_inputs,
    _normalise_silver,
)
from factor_lab.research_os.iceberg_service import PyIcebergGoldPublisher
from factor_lab.research_os.legacy_bronze_seed import LEGACY_SEED_TRUST_LABELS
from factor_lab.research_os.reconciliation import reconcile_observations
from factor_lab.research_os.snapshots import (
    build_immutable_snapshot_manifest,
    verify_immutable_snapshot_manifest,
)


_EMPTY_HASH = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def _register_cataloged_silver_snapshots(
    catalog: ResearchCatalog,
    *,
    count: int,
) -> set[str]:
    expected: set[str] = set()
    freeze = datetime(2024, 1, 1, tzinfo=timezone.utc)
    for index in range(count):
        snapshot_id = f"silver-discovery-{index:05d}"
        expected.add(snapshot_id)
        catalog.register_snapshot(
            DataSnapshotRef(
                snapshot_id=snapshot_id,
                tier="silver",
                uri=f"s3://factor-lab/silver/{snapshot_id}",
                content_hash=f"{index + 1:064x}",
                as_of=freeze + timedelta(days=index // 20),
                manifest={
                    "files": [
                        {
                            "path": f"silver/{snapshot_id}.parquet",
                            "sha256": f"{index + 1:064x}",
                        }
                    ]
                },
            )
        )
    return expected


def _at(day: pd.Timestamp, hour: int) -> datetime:
    return datetime.combine(day.date(), time(hour), tzinfo=timezone.utc)


def _stock_limit_contract() -> DatasetContract:
    return DatasetContract(
        dataset="stock_limit",
        key_fields=("ts_code", "trade_date"),
        event_time_field="trade_date",
        release_timing="after exchange close",
        fields=(
            FieldContract("ts_code", "string", nullable=False),
            FieldContract("trade_date", "date", nullable=False),
            FieldContract("available_at", "datetime", nullable=False),
            FieldContract("pre_close", "float64", nullable=True, unit="CNY"),
            FieldContract("up_limit", "float64", nullable=False, unit="CNY"),
            FieldContract("down_limit", "float64", nullable=False, unit="CNY"),
        ),
    )


def _stock_limit_source_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "300001.SZ"],
            "trade_date": ["2023-12-01", "2023-12-01"],
            "available_at": [
                "2023-12-01T16:00:00Z",
                "2023-12-01T16:00:00Z",
            ],
            "pre_close": [pd.NA, pd.NA],
            "up_limit": [11.0, 22.0],
            "down_limit": [9.0, 18.0],
        }
    )


def _silver_fixture(*, include_st: bool = True) -> tuple[pd.DataFrame, pd.DatetimeIndex, pd.Timestamp, pd.Timestamp]:
    calendar = pd.bdate_range("2023-09-01", periods=85)
    start = calendar[65]
    end = calendar[70]
    tickers = ["600001.SH", "600002.SH", "000001.SZ", "300001.SZ"]
    amounts = {code: float(index + 1) * 1_000 for index, code in enumerate(tickers)}
    rows: list[dict] = []
    for day_index, day in enumerate(calendar):
        rows.append(
            {
                "dataset": "trade_calendar",
                "event_time": day,
                "available_at": _at(day, 8),
                "ingested_at": _at(day, 21),
                "exchange": "SSE",
                "is_open": 1,
            }
        )
        for ticker_index, ticker in enumerate(tickers):
            raw_open = 10.0 + ticker_index + day_index * 0.01
            raw_close = raw_open + 0.02
            common = {
                "event_time": day,
                "available_at": _at(day, 8),
                "ingested_at": _at(day, 21),
                "ts_code": ticker,
            }
            rows.append(
                {
                    **common,
                    "dataset": "daily",
                    "open": raw_open,
                    "high": raw_close + 0.02,
                    "low": raw_open - 0.02,
                    "close": raw_close,
                    "pre_close": raw_open - 0.01,
                    "vol": 10_000.0,
                    "amount": amounts[ticker],
                }
            )
            rows.append(
                {
                    **common,
                    "dataset": "daily_basic",
                    "total_mv": 100_000.0 + ticker_index * 10_000,
                    "circ_mv": 80_000.0 + ticker_index * 10_000,
                }
            )
            rows.append({**common, "dataset": "adj_factor", "adj_factor": 2.0})
            rows.append(
                {
                    **common,
                    "dataset": "trade_status",
                    "trade_status": "trading",
                    "is_suspended": False,
                    "up_limit": raw_close * 1.1,
                    "down_limit": raw_close * 0.9,
                }
            )

    reference_day = calendar[0]
    for ticker in tickers:
        rows.append(
            {
                "dataset": "stock_basic",
                "event_time": reference_day,
                "available_at": _at(reference_day, 8),
                "ingested_at": _at(reference_day, 21),
                "ts_code": ticker,
                "list_status": "L",
                "queried_list_status": "L",
                "list_date": "2000-01-01",
                "delist_date": None,
            }
        )
        rows.append(
            {
                "dataset": "industry_classification",
                "event_time": reference_day,
                "available_at": _at(reference_day, 8),
                "ingested_at": _at(reference_day, 21),
                "ts_code": ticker,
                "industry": "industry-a" if ticker.endswith("SH") else "industry-b",
                "effective_from": "2000-01-01",
            }
        )
    # Scope evidence proves stock_basic queried all current/paused/delisted universes.
    for ticker, status in (("900001.SH", "P"), ("200001.SZ", "D")):
        rows.append(
            {
                "dataset": "stock_basic",
                "event_time": reference_day,
                "available_at": _at(reference_day, 8),
                "ingested_at": _at(reference_day, 21),
                "ts_code": ticker,
                "list_status": status,
                "queried_list_status": status,
                "list_date": "2000-01-01",
                "delist_date": "2020-01-01" if status == "D" else None,
            }
        )
    if include_st:
        rows.append(
            {
                "dataset": "historical_st",
                "event_time": reference_day,
                "available_at": _at(reference_day, 8),
                "ingested_at": _at(reference_day, 21),
                "ts_code": "600001.SH",
                "start_date": calendar[60].date().isoformat(),
                "end_date": calendar[75].date().isoformat(),
            }
        )
    rows.append(
        {
            "dataset": "company_action",
            "event_time": calendar[20],
            "available_at": _at(calendar[20], 8),
            "ingested_at": _at(calendar[20], 21),
            "ts_code": "600002.SH",
            "cash_dividend": 0.1,
        }
    )
    return pd.DataFrame(rows), calendar, start, end


def _build(frame: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, calendar: pd.DatetimeIndex, *, freeze: datetime | None = None):
    return build_research_gold_panel(
        frame,
        analysis_start=start,
        analysis_end=end,
        as_of=freeze or _at(calendar[-1], 23),
        universe=UniverseSpec(
            target_size=2,
            liquidity_lookback_sessions=60,
            minimum_liquidity_observations=40,
            minimum_listing_days=180,
        ),
        label=LabelSpec(),
        parent_snapshot_ids=("a" * 64,),
    )


def test_gold_panel_builds_pit_universe_features_label_and_equal_weight_benchmark() -> None:
    frame, calendar, start, end = _silver_fixture()
    result = _build(frame, start, end, calendar)
    panel = result.panel

    assert result.audit["status"] == "pass"
    assert result.parent_snapshot_ids == ("a" * 64,)
    assert not panel.duplicated(["ts_code", "trade_date"]).any()
    assert {"open_adj", "close_adj", "adv_20", "volatility_20"} <= set(panel)
    assert {"is_suspended", "can_buy", "can_sell", "has_company_action"} <= set(panel)
    assert {"industry", "log_market_cap", "eligible", "universe_member"} <= set(panel)
    assert {"label_available_at", "label_is_research_only", "forward_return_5d_open"} <= set(panel)
    assert panel["label_is_research_only"].all()
    assert panel["label_available_at"].dropna().gt(panel.loc[panel["label_available_at"].notna(), "decision_cutoff"]).all()
    assert panel["liquidity_window_end"].dropna().lt(panel.loc[panel["liquidity_window_end"].notna(), "trade_date"]).all()
    assert panel.loc[panel["universe_member"]].groupby("trade_date").size().eq(2).all()
    assert panel.groupby("trade_date")["benchmark_weight"].sum().eq(1.0).all()
    assert panel["benchmark_forward_return_5d_open"].notna().all()
    # Highest-liquidity names are selected except the name that was ST at PIT cutoff.
    selected = set(panel.loc[panel["universe_member"], "ts_code"])
    assert selected == {"300001.SZ", "000001.SZ"}
    assert panel["open_adj"].iloc[0] == pytest.approx(panel["open"].iloc[0] * 2.0)
    assert panel.loc[panel["universe_member"], "industry"].notna().all()


def test_production_dataset_shapes_normalize_to_the_authoritative_gold_contract() -> None:
    frame, calendar, start, end = _silver_fixture()
    stock = frame["dataset"].eq("stock_basic")
    status = frame.loc[stock, "queried_list_status"].astype(str).str.lower()
    frame.loc[stock, "dataset"] = "stock_basic_" + status
    frame = frame.drop(columns="queried_list_status")

    trade_status = frame["dataset"].eq("trade_status")
    frame.loc[trade_status, "dataset"] = "stock_limit"
    frame.loc[trade_status, "is_suspended"] = pd.NA
    frame.loc[trade_status, "trade_status"] = pd.NA

    interval = frame["dataset"].eq("historical_st")
    template = frame.loc[interval].iloc[0].to_dict()
    daily_st: list[dict] = []
    for day in calendar[60:76]:
        row = dict(template)
        row.pop("start_date", None)
        row.pop("end_date", None)
        row["event_time"] = day
        row["available_at"] = _at(day, 8)
        row["ingested_at"] = _at(day, 21)
        row["trade_date"] = day.strftime("%Y%m%d")
        row["type"] = "ST"
        daily_st.append(row)
    frame = pd.concat(
        [frame.loc[~interval], pd.DataFrame(daily_st)], ignore_index=True, sort=False
    )
    result = _build(frame, start, end, calendar)

    assert result.audit["status"] == "pass"
    assert set(result.panel.loc[result.panel["universe_member"], "ts_code"]) == {
        "300001.SZ",
        "000001.SZ",
    }
    assert result.panel["can_buy"].all()


def test_nullable_stock_limit_pre_close_survives_source_and_silver_contracts_and_gold_uses_limits() -> None:
    contract = _stock_limit_contract()
    source_frame = _stock_limit_source_frame()
    validate_source_frame(source_frame, contract)

    canonical = canonicalize_batch(
        SourceBatch(
            source_id="tushare",
            source_priority=10,
            dataset="stock_limit",
            frame=source_frame,
            ingested_at=datetime(2023, 12, 2, tzinfo=timezone.utc),
            vendor_revision="stock-limit-r1",
            contract=contract,
            request=FetchRequest("stock_limit"),
        ),
        CanonicalizationSpec(
            entity_columns=("ts_code",),
            event_time_column="trade_date",
            available_at_column="available_at",
            value_columns=("pre_close", "up_limit", "down_limit"),
        ),
    )
    silver = reconcile_observations(canonical)

    assert silver.promotion_allowed is True
    assert silver.audit["nullable_missing_row_count"] == 2
    assert silver.audit["quarantined_row_count"] == 0
    assert set(silver.accepted["field"]) == {"up_limit", "down_limit"}
    assert canonical.loc[canonical["field"].eq("pre_close"), "nullable"].all()

    frame, calendar, start, end = _silver_fixture()
    status = frame["dataset"].eq("trade_status")
    frame.loc[status, "dataset"] = "stock_limit"
    frame.loc[status, "pre_close"] = pd.NA
    for ticker, boundary in (
        ("000001.SZ", "up_limit"),
        ("300001.SZ", "down_limit"),
    ):
        status_row = status & frame["ts_code"].eq(ticker) & pd.to_datetime(
            frame["event_time"]
        ).eq(start)
        limit_price = float(frame.loc[status_row, boundary].iloc[0])
        daily_row = (
            frame["dataset"].eq("daily")
            & frame["ts_code"].eq(ticker)
            & pd.to_datetime(frame["event_time"]).eq(start)
        )
        for price_field in ("open", "high", "low", "close"):
            frame.loc[daily_row, price_field] = limit_price
        # Remove the daily fallback too: the result must be driven by the
        # authoritative non-null price-limit columns, not by pre_close.
        frame.loc[daily_row, "pre_close"] = pd.NA

    result = _build(frame, start, end, calendar)
    day = result.panel["trade_date"].eq(start)
    limit_up = result.panel.loc[
        day & result.panel["ts_code"].eq("000001.SZ")
    ].iloc[0]
    limit_down = result.panel.loc[
        day & result.panel["ts_code"].eq("300001.SZ")
    ].iloc[0]

    assert bool(limit_up["is_one_price_limit_up"]) is True
    assert bool(limit_up["can_buy"]) is False
    assert bool(limit_up["can_sell"]) is True
    assert bool(limit_down["is_one_price_limit_down"]) is True
    assert bool(limit_down["can_buy"]) is True
    assert bool(limit_down["can_sell"]) is False


def test_stock_limit_contract_fails_closed_when_pre_close_column_is_absent() -> None:
    missing_pre_close = _stock_limit_source_frame().drop(columns="pre_close")

    with pytest.raises(
        SourceContractError,
        match=r"stock_limit missing contracted fields: \['pre_close'\]",
    ):
        validate_source_frame(missing_pre_close, _stock_limit_contract())


def test_current_security_status_is_not_backdated_and_trade_status_conflicts_block() -> None:
    frame, calendar, start, end = _silver_fixture()
    paused = frame["dataset"].eq("stock_basic") & frame[
        "queried_list_status"
    ].eq("P")
    frame.loc[paused, "dataset"] = "stock_basic_p"
    normalized = _normalise_silver(frame, as_of=_at(calendar[-1], 23))
    paused_row = normalized.loc[
        normalized["dataset"].eq("stock_basic")
        & normalized["queried_list_status"].eq("P")
    ].iloc[0]
    assert paused_row["available_at"] == paused_row["ingested_at"]

    duplicate = frame.loc[
        frame["dataset"].eq("trade_status")
        & frame["ts_code"].eq("000001.SZ")
        & pd.to_datetime(frame["event_time"]).eq(start)
    ].copy()
    duplicate.loc[:, "dataset"] = "stock_limit"
    duplicate.loc[:, "up_limit"] = duplicate["up_limit"].astype(float) + 1.0
    broken = pd.concat([frame, duplicate], ignore_index=True, sort=False)
    with pytest.raises(
        GoldPanelError,
        match="same-time Silver revisions conflict|conflicting up_limit",
    ):
        _build(broken, start, end, calendar)


def test_company_action_is_applied_on_ex_date_not_announcement_date() -> None:
    frame, calendar, start, end = _silver_fixture()
    announcement = calendar[62]
    frame = pd.concat(
        [
            frame,
            pd.DataFrame(
                [
                    {
                        "dataset": "company_action",
                        "event_time": announcement,
                        "available_at": _at(announcement, 8),
                        "ingested_at": _at(announcement, 21),
                        "ts_code": "000001.SZ",
                        "ex_date": start.strftime("%Y%m%d"),
                        "cash_dividend": 0.2,
                    }
                ]
            ),
        ],
        ignore_index=True,
        sort=False,
    )

    result = _build(frame, start, end, calendar)
    ticker = result.panel["ts_code"].eq("000001.SZ")

    assert result.panel.loc[ticker & result.panel["trade_date"].eq(start), "has_company_action"].item()
    assert not result.panel.loc[
        ticker & result.panel["trade_date"].eq(calendar[66]), "has_company_action"
    ].item()


def test_gold_panel_label_is_unavailable_until_future_exit_observation_is_ingested() -> None:
    frame, calendar, start, end = _silver_fixture()
    result = _build(frame, start, end, calendar, freeze=_at(end, 23))
    assert result.panel["forward_return_5d_open"].isna().all()
    assert result.panel["label_available_at"].isna().all()


def test_gold_panel_fails_closed_on_empty_st_missing_dataset_and_future_liquidity() -> None:
    frame, calendar, start, end = _silver_fixture(include_st=False)
    with pytest.raises(GoldPanelError, match="historical ST"):
        _build(frame, start, end, calendar)

    frame, calendar, start, end = _silver_fixture()
    without_adj = frame.loc[~frame["dataset"].eq("adj_factor")]
    with pytest.raises(GoldPanelError, match="required Silver datasets"):
        _build(without_adj, start, end, calendar)

    # Removing one open-day partition is a hard completeness failure.
    missing_date = calendar[10]
    broken = frame.loc[~(frame["dataset"].eq("daily_basic") & pd.to_datetime(frame["event_time"]).eq(missing_date))]
    with pytest.raises(GoldPanelError, match="daily_basic omits"):
        _build(broken, start, end, calendar)


def test_silver_revision_is_selected_by_system_time_without_mutating_old_gold() -> None:
    frame, calendar, start, end = _silver_fixture()
    revision_mask = (
        frame["dataset"].eq("daily")
        & frame["ts_code"].eq("300001.SZ")
        & pd.to_datetime(frame["event_time"]).eq(start)
    )
    revised = frame.loc[revision_mask].copy()
    revised.loc[:, "close"] = revised["close"].astype(float) + 1.0
    revised.loc[:, "ingested_at"] = _at(calendar[-1], 22)
    with_revision = pd.concat([frame, revised], ignore_index=True)
    result = _build(with_revision, start, end, calendar)
    row = result.panel.loc[
        result.panel["ts_code"].eq("300001.SZ")
        & result.panel["trade_date"].eq(start)
    ].iloc[0]
    assert row["close_adj"] == pytest.approx(float(revised.iloc[0]["close"]) * 2.0)
    assert result.audit["silver_revision_rows_superseded"] == 1

    ambiguous = revised.copy()
    ambiguous.loc[:, "close"] = ambiguous["close"].astype(float) + 1.0
    with_ambiguous = pd.concat([with_revision, ambiguous], ignore_index=True)
    with pytest.raises(GoldPanelError, match="same-time Silver revisions conflict"):
        _build(with_ambiguous, start, end, calendar)


def test_verified_silver_inputs_require_complete_cataloged_bronze_parent_closure(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    lake.mkdir()
    bronze_path = lake / "bronze.parquet"
    pd.DataFrame({"raw": [1]}).to_parquet(bronze_path, index=False)
    hashes = {
        "config_hash": _EMPTY_HASH,
        "code_hash": _EMPTY_HASH,
        "dirty_patch_hash": _EMPTY_HASH,
        "dependency_lock_hash": _EMPTY_HASH,
    }
    bronze = build_immutable_snapshot_manifest(
        (bronze_path,),
        base_dir=lake,
        tier="bronze",
        as_of="2024-01-01T00:00:00+00:00",
        parent_snapshot_ids=(),
        environment_hashes=hashes,
        quality_report={"status": "pass"},
    )
    silver_path = lake / "accepted_silver.parquet"
    pd.DataFrame({"dataset": ["fixture"]}).to_parquet(silver_path, index=False)
    silver = build_immutable_snapshot_manifest(
        (silver_path,),
        base_dir=lake,
        tier="silver",
        as_of="2024-01-01T01:00:00+00:00",
        parent_snapshot_ids=(bronze.snapshot_id,),
        environment_hashes=hashes,
        quality_report={"status": "pass"},
    )
    catalog = ResearchCatalog(tmp_path / "catalog.sqlite")
    try:
        catalog.initialize_schema()
        catalog.register_snapshot(bronze.to_snapshot_ref(uri=bronze_path.resolve().as_uri()))
        catalog.register_snapshot(silver.to_snapshot_ref(uri=silver_path.resolve().as_uri()))
        frame, parents = read_verified_silver_inputs(
            (SilverSnapshotInput(silver.to_snapshot_ref(uri=silver_path.resolve().as_uri()), silver_path),),
            catalog=catalog,
            lake_root=lake,
        )
        assert frame["dataset"].tolist() == ["fixture"]
        assert parents == (silver.snapshot_id,)

        missing_catalog = ResearchCatalog(tmp_path / "missing.sqlite")
        try:
            missing_catalog.initialize_schema()
            missing_catalog.register_snapshot(silver.to_snapshot_ref(uri=silver_path.resolve().as_uri()))
            with pytest.raises(GoldPanelError, match="missing Bronze ancestor"):
                read_verified_silver_inputs(
                    (SilverSnapshotInput(silver.to_snapshot_ref(uri=silver_path.resolve().as_uri()), silver_path),),
                    catalog=missing_catalog,
                    lake_root=lake,
                )
        finally:
            missing_catalog.close()
    finally:
        catalog.close()


def test_verified_silver_inputs_reject_non_promotable_bronze_ancestor(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    lake.mkdir()
    hashes = {
        "config_hash": _EMPTY_HASH,
        "code_hash": _EMPTY_HASH,
        "dirty_patch_hash": _EMPTY_HASH,
        "dependency_lock_hash": _EMPTY_HASH,
    }
    bronze_path = lake / "legacy-bronze.parquet"
    pd.DataFrame({"raw": [1]}).to_parquet(bronze_path, index=False)
    bronze = build_immutable_snapshot_manifest(
        (bronze_path,),
        base_dir=lake,
        tier="bronze",
        as_of="2024-01-01T00:00:00+00:00",
        parent_snapshot_ids=(),
        environment_hashes=hashes,
        quality_report={"status": "pass"},
        trust_labels=LEGACY_SEED_TRUST_LABELS,
    )
    silver_path = lake / "accepted-silver.parquet"
    pd.DataFrame({"dataset": ["fixture"]}).to_parquet(silver_path, index=False)
    silver = build_immutable_snapshot_manifest(
        (silver_path,),
        base_dir=lake,
        tier="silver",
        as_of="2024-01-02T00:00:00+00:00",
        parent_snapshot_ids=(bronze.snapshot_id,),
        environment_hashes=hashes,
        quality_report={"status": "pass"},
    )
    with ResearchCatalog(tmp_path / "catalog.sqlite") as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(
            bronze.to_snapshot_ref(uri=bronze_path.resolve().as_uri())
        )
        silver_ref = silver.to_snapshot_ref(uri=silver_path.resolve().as_uri())
        catalog.register_snapshot(silver_ref)

        with pytest.raises(
            GoldPanelError, match="Bronze ancestor carries non-promotable"
        ):
            read_verified_silver_inputs(
                (SilverSnapshotInput(silver_ref, silver_path),),
                catalog=catalog,
                lake_root=lake,
            )


class _Arrow:
    schema = object()


class _ResearchTable:
    def __init__(self) -> None:
        self.metadata = SimpleNamespace(snapshots=[], refs={})
        self.append_count = 0
        self.overwrite_count = 0

    def append(self, _arrow, *, snapshot_properties: dict) -> None:
        self.append_count += 1
        self.metadata.snapshots.append(
            SimpleNamespace(snapshot_id=len(self.metadata.snapshots) + 1, summary=snapshot_properties)
        )

    def overwrite(self, _arrow, *, snapshot_properties: dict) -> None:
        self.overwrite_count += 1
        self.metadata.snapshots.append(
            SimpleNamespace(snapshot_id=len(self.metadata.snapshots) + 1, summary=snapshot_properties)
        )

    def refresh(self) -> None:
        return None

    def manage_snapshots(self):
        table = self

        class Manager:
            def create_tag(self, snapshot_id: int, tag: str):
                self.snapshot_id = snapshot_id
                self.tag = tag
                return self

            def commit(self):
                table.metadata.refs[self.tag] = SimpleNamespace(snapshot_id=self.snapshot_id)

        return Manager()


class _ResearchCatalog:
    def __init__(self, table: _ResearchTable) -> None:
        self.table = table

    def create_namespace_if_not_exists(self, _namespace: str) -> None:
        return None

    def table_exists(self, _identifier: str) -> bool:
        return bool(self.table.metadata.snapshots)

    def load_table(self, _identifier: str) -> _ResearchTable:
        return self.table

    def create_table_if_not_exists(self, _identifier: str, *, schema) -> _ResearchTable:
        assert schema is _Arrow.schema
        return self.table


def test_research_iceberg_same_partition_revision_overwrites_and_preserves_tags() -> None:
    table = _ResearchTable()
    publisher = PyIcebergGoldPublisher(
        catalog_loader=lambda _name: _ResearchCatalog(table),
        arrow_builder=lambda _frame: _Arrow(),
    )
    frame = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "decision_cutoff": ["2024-01-02T08:00:00+00:00"],
            "label_available_at": ["2024-01-08T08:00:00+00:00"],
            "label_is_research_only": [True],
            "forward_return_5d_open": [0.01],
        }
    )
    first = publisher.publish_research_panel(
        frame,
        table_identifier="factor_lab.gold_research_panel",
        tag="ros_first",
        snapshot_key="a" * 64,
        partition_key="2024-01-08",
    )
    retry = publisher.publish_research_panel(
        frame,
        table_identifier="factor_lab.gold_research_panel",
        tag="ros_first",
        snapshot_key="a" * 64,
        partition_key="2024-01-08",
    )
    revised = publisher.publish_research_panel(
        frame.assign(forward_return_5d_open=0.02),
        table_identifier="factor_lab.gold_research_panel",
        tag="ros_revision",
        snapshot_key="b" * 64,
        partition_key="2024-01-08",
    )

    assert first.snapshot_id == retry.snapshot_id == 1
    assert retry.reused is True
    assert revised.snapshot_id == 2 and revised.reused is False
    assert table.append_count == 1
    assert table.overwrite_count == 1
    assert table.metadata.refs["ros_first"].snapshot_id == 1
    assert table.metadata.refs["ros_revision"].snapshot_id == 2


def test_catalog_backed_gold_service_writes_manifest_bindable_history(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    lake.mkdir()
    frame, calendar, start, end = _silver_fixture()
    bronze_path = lake / "raw.parquet"
    pd.DataFrame({"raw": [1]}).to_parquet(bronze_path, index=False)
    hashes = {
        "config_hash": _EMPTY_HASH,
        "code_hash": _EMPTY_HASH,
        "dirty_patch_hash": _EMPTY_HASH,
        "dependency_lock_hash": _EMPTY_HASH,
    }
    bronze = build_immutable_snapshot_manifest(
        (bronze_path,),
        base_dir=lake,
        tier="bronze",
        as_of="2024-01-01T00:00:00+00:00",
        parent_snapshot_ids=(),
        environment_hashes=hashes,
        quality_report={"status": "pass"},
    )
    silver_path = lake / "accepted_silver.parquet"
    frame.to_parquet(silver_path, index=False)
    silver = build_immutable_snapshot_manifest(
        (silver_path,),
        base_dir=lake,
        tier="silver",
        as_of="2024-01-02T00:00:00+00:00",
        parent_snapshot_ids=(bronze.snapshot_id,),
        environment_hashes=hashes,
        quality_report={"status": "pass"},
    )
    catalog = ResearchCatalog(tmp_path / "catalog.sqlite")
    try:
        catalog.initialize_schema()
        catalog.register_snapshot(bronze.to_snapshot_ref(uri=bronze_path.resolve().as_uri()))
        silver_ref = silver.to_snapshot_ref(uri=silver_path.resolve().as_uri())
        catalog.register_snapshot(silver_ref)
        artifacts = ResearchGoldPanelService(catalog, lake_root=lake).build(
            inputs=(SilverSnapshotInput(silver_ref, silver_path),),
            output_dir=lake / "gold-build",
            analysis_start=start.date().isoformat(),
            analysis_end=end.date().isoformat(),
            as_of=_at(calendar[-1], 23),
            universe=UniverseSpec(
                target_size=2,
                liquidity_lookback_sessions=60,
                minimum_liquidity_observations=40,
            ),
        )
        gold = build_immutable_snapshot_manifest(
            artifacts.manifest_paths,
            base_dir=lake,
            tier="gold",
            as_of=_at(calendar[-1], 23),
            parent_snapshot_ids=artifacts.parent_snapshot_ids,
            environment_hashes=hashes,
            quality_report=artifacts.audit,
            trust_labels=("research_ready_panel",),
            trading_calendar=artifacts.audit["trading_calendar"],
        )
        assert gold.parent_snapshot_ids == (silver.snapshot_id,)
        assert verify_immutable_snapshot_manifest(gold, base_dir=lake)["valid"] is True
        assert pd.read_parquet(artifacts.panel_path)["label_is_research_only"].all()
        gold_ref = gold.to_snapshot_ref(
            uri=f"iceberg://factorlab/factor_lab.gold_research_panel#ros_{gold.snapshot_id}"
        )
        loaded = load_gold_research_panel(gold_ref, lake_root=lake)
        assert loaded.attrs["research_os_source_path"] == str(artifacts.panel_path.resolve())
        assert loaded.attrs["research_os_source_sha256"]
        assert gold_ref.manifest["trading_calendar"]["content_hash"]
    finally:
        catalog.close()


def test_default_silver_discovery_reads_every_snapshot_beyond_catalog_page_limit(
    tmp_path: Path,
) -> None:
    assert DEFAULT_SILVER_SNAPSHOT_DISCOVERY_CAP >= 50_000
    lake = tmp_path / "lake"
    lake.mkdir()
    with ResearchCatalog(tmp_path / "catalog.sqlite") as catalog:
        catalog.initialize_schema()
        expected = _register_cataloged_silver_snapshots(catalog, count=1_005)

        discovered = discover_cataloged_silver_inputs(catalog, lake_root=lake)

    assert len(discovered) == 1_005
    assert {item.reference.snapshot_id for item in discovered} == expected


def test_silver_discovery_fails_closed_when_total_safety_cap_has_more_rows(
    tmp_path: Path,
) -> None:
    lake = tmp_path / "lake"
    lake.mkdir()
    with ResearchCatalog(tmp_path / "catalog.sqlite") as catalog:
        catalog.initialize_schema()
        _register_cataloged_silver_snapshots(catalog, count=1_001)

        with pytest.raises(GoldPanelError, match="safety cap.*additional"):
            discover_cataloged_silver_inputs(
                catalog,
                lake_root=lake,
                limit=1_000,
            )

        exact = discover_cataloged_silver_inputs(
            catalog,
            lake_root=lake,
            limit=1_001,
        )

    assert len(exact) == 1_001
