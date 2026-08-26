from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from factor_lab.data import RuntimeLayout, enrich_top500_store, prepare_financial_pit
from factor_lab.data.catalog import sha256_file
from factor_lab.data.sources import ENRICHMENT_DATASET_FIELDS, enrichment_partition_path


def _layout(tmp_path: Path) -> tuple[Path, RuntimeLayout]:
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
    }
    config_path = tmp_path / "configs" / "data.json"
    config_path.parent.mkdir()
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    layout = RuntimeLayout.from_config(payload, config_path=config_path)
    layout.ensure_directories()
    return config_path, layout


def _financial_row(**overrides) -> dict:
    row = {field: 1.0 for field in ENRICHMENT_DATASET_FIELDS["fina_indicator_vip"].split(",")}
    row.update(
        {
            "ts_code": "000001.SZ",
            "ann_date": "20240105",
            "end_date": "20230930",
            "update_flag": "0",
            "roic": 12.0,
            "q_ocf_to_sales": 1.5,
            "debt_to_assets": 40.0,
        }
    )
    row.update(overrides)
    return row


def _write_store(tmp_path: Path) -> tuple[Path, RuntimeLayout]:
    config_path, layout = _layout(tmp_path)
    membership = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ", "000001.SZ", "000002.SZ"],
            "membership_month": ["2024-01", "2024-01", "2024-02", "2024-02"],
            "as_of_date": pd.to_datetime(
                ["2023-12-29", "2023-12-29", "2024-01-31", "2024-01-31"]
            ),
            "effective_start_date": pd.to_datetime(
                ["2024-01-05", "2024-01-05", "2024-02-01", "2024-02-01"]
            ),
            "effective_end_date": pd.to_datetime(
                ["2024-01-31", "2024-01-31", "2024-02-29", "2024-02-29"]
            ),
            "historical_st_known": [False] * 4,
            "is_st_at_asof": [False] * 4,
            "st_filter_status": ["unverified"] * 4,
        }
    )
    membership.to_parquet(layout.membership_path, index=False)

    keys = pd.DataFrame(
        {
            "ticker": ["000001.SZ", "000002.SZ"] * 3,
            "date": pd.to_datetime(
                ["2024-01-05", "2024-01-05", "2024-01-08", "2024-01-08", "2024-02-01", "2024-02-01"]
            ),
            "membership_month": ["2024-01"] * 4 + ["2024-02"] * 2,
            "eligible": [True] * 6,
        }
    )
    features = keys.assign(value=range(6))
    features.to_parquet(layout.features_path, index=False, row_group_size=2)
    execution = keys.drop(columns="membership_month").assign(
        universe_member=True,
        open_adj=10.0,
    )
    execution.to_parquet(layout.execution_path, index=False, row_group_size=2)

    for as_of, names, industries in (
        ("2023-12-29", ["甲公司", "*ST乙"], ["银行", "软件"]),
        ("2024-01-31", ["ST甲", "乙公司"], ["金融", "制造"]),
    ):
        path = enrichment_partition_path(layout.raw_root, "bak_basic", as_of)
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "trade_date": [as_of.replace("-", "")] * 2,
                "ts_code": ["000001.SZ", "000002.SZ"],
                "name": names,
                "industry": industries,
                "list_date": ["19910403", "19910129"],
            }
        ).to_parquet(path, index=False)

    financial_path = enrichment_partition_path(
        layout.raw_root, "fina_indicator_vip", "2023-09-30"
    )
    financial_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            _financial_row(),
            _financial_row(
                ts_code="000002.SZ",
                roic=8.0,
                q_ocf_to_sales=0.8,
                debt_to_assets=55.0,
            ),
        ]
    ).to_parquet(financial_path, index=False)
    return config_path, layout


def test_prepare_financial_pit_uses_strict_next_trading_day() -> None:
    rows = pd.DataFrame([_financial_row()])
    timeline = prepare_financial_pit(
        rows,
        pd.to_datetime(["2024-01-05", "2024-01-08", "2024-01-09"]),
    )

    assert timeline.loc[0, "financial_ann_date"] == pd.Timestamp("2024-01-05")
    assert timeline.loc[0, "financial_available_date"] == pd.Timestamp("2024-01-08")
    assert timeline.loc[0, "fundamental_roic"] == 12.0
    assert timeline.loc[0, "fundamental_q_ocf_to_sales"] == 1.5


def test_prepare_financial_pit_rejects_ambiguous_same_time_revision() -> None:
    rows = pd.DataFrame([_financial_row(), _financial_row(roic=99.0)])

    with pytest.raises(ValueError, match="ambiguous financial revisions"):
        prepare_financial_pit(rows, pd.to_datetime(["2024-01-05", "2024-01-08"]))


def test_enrich_updates_all_canonical_files_with_monthly_name_st_fallback(
    tmp_path: Path,
) -> None:
    config_path, layout = _write_store(tmp_path)

    result = enrich_top500_store(
        config_path=config_path,
        layout=layout,
        batch_size=2,
    )

    features = pd.read_parquet(layout.features_path)
    execution = pd.read_parquet(layout.execution_path)
    membership = pd.read_parquet(layout.membership_path)
    assert result["features"]["st_excluded_row_count"] == 3
    assert result["execution"]["st_excluded_row_count"] == 3
    assert result["membership"]["st_member_count"] == 2
    assert len(features) == 6
    assert len(execution) == 6
    assert len(membership) == 4

    by_key = features.set_index(["date", "ticker"])
    assert not bool(by_key.loc[(pd.Timestamp("2024-01-05"), "000001.SZ"), "financial_pit_valid"])
    visible = by_key.loc[(pd.Timestamp("2024-01-08"), "000001.SZ")]
    assert bool(visible["financial_pit_valid"])
    assert visible["financial_available_date"] == pd.Timestamp("2024-01-08")
    assert visible["fundamental_roic"] == 12.0
    assert visible["fundamental_q_ocf_to_sales"] == 1.5
    assert visible["fundamental_debt_to_assets"] == 40.0
    assert visible["fundamental_age_days"] == 100.0
    assert visible["industry_pit"] == "银行"
    assert by_key.loc[(pd.Timestamp("2024-01-05"), "000002.SZ"), "eligible"] == False
    assert by_key.loc[(pd.Timestamp("2024-02-01"), "000001.SZ"), "eligible"] == False
    assert features["st_filter_status"].eq(
        "monthly_name_verified_daily_events_unavailable"
    ).all()

    execution_by_key = execution.set_index(["date", "ticker"])
    assert execution_by_key.loc[(pd.Timestamp("2024-01-05"), "000002.SZ"), "eligible"] == False
    assert execution_by_key.loc[(pd.Timestamp("2024-02-01"), "000001.SZ"), "eligible"] == False
    assert execution["eligible_pre_pit"].all()

    membership_by_key = membership.set_index(["membership_month", "ts_code"])
    assert bool(membership_by_key.loc[("2024-01", "000002.SZ"), "is_st_at_asof"])
    assert bool(membership_by_key.loc[("2024-02", "000001.SZ"), "is_st_at_asof"])
    assert membership["historical_st_known"].all()
    assert membership["st_filter_status"].eq(
        "monthly_name_verified_daily_events_unavailable"
    ).all()
    assert (layout.top500_root / "enrichment-manifest.json").is_file()


def test_verified_interval_alias_resolves_historical_vendor_code(
    tmp_path: Path,
) -> None:
    config_path, layout = _write_store(tmp_path)
    canonical = "999999.SZ"
    historical = "000002.SZ"
    for path, column in (
        (layout.membership_path, "ts_code"),
        (layout.features_path, "ticker"),
        (layout.execution_path, "ticker"),
    ):
        frame = pd.read_parquet(path)
        frame.loc[frame[column] == historical, column] = canonical
        frame.to_parquet(path, index=False)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["enrichment"] = {
        "security_code_aliases": [
            {
                "canonical_ts_code": canonical,
                "vendor_ts_code": historical,
                "effective_from": "1900-01-01",
                "effective_to": "2025-01-01",
                "source": "https://example.invalid/verified-announcement.pdf",
            }
        ]
    }
    config_path.write_text(json.dumps(config), encoding="utf-8")

    result = enrich_top500_store(
        config_path=config_path,
        layout=layout,
        batch_size=2,
    )

    membership = pd.read_parquet(layout.membership_path)
    aliased = membership[membership["ts_code"] == canonical]
    assert aliased["reference_verified_pit"].all()
    assert aliased["security_alias_applied_pit"].all()
    assert aliased["vendor_ts_code_pit"].eq(historical).all()
    assert aliased["security_alias_source"].notna().all()
    assert result["membership"]["reference_missing_count"] == 0
    assert result["membership"]["security_alias_applied_count"] == 2


def test_enrichment_transaction_rolls_back_all_targets_on_install_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, layout = _write_store(tmp_path)
    before = {
        path.name: sha256_file(path)
        for path in (layout.features_path, layout.execution_path, layout.membership_path)
    }
    original_replace = Path.replace
    raised = False

    def fail_once(path: Path, target: Path) -> Path:
        nonlocal raised
        if path.name == "execution.enrich.partial.parquet" and not raised:
            raised = True
            raise OSError("injected install failure")
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", fail_once)
    with pytest.raises(OSError, match="injected install failure"):
        enrich_top500_store(
            config_path=config_path,
            layout=layout,
            batch_size=2,
        )

    after = {
        path.name: sha256_file(path)
        for path in (layout.features_path, layout.execution_path, layout.membership_path)
    }
    assert after == before
    assert not (layout.top500_root / "enrichment-transaction.json").exists()
    assert not list(layout.top500_root.glob("*.enrich.partial.parquet"))
