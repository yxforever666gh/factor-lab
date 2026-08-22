from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "research_os_orchestration.example.json"


def test_formal_gold_inputs_have_explicit_source_adapter_contracts() -> None:
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    daily = config["daily"]
    sources = daily["sources"]
    required = set(daily["gold"]["research_panel"]["required_datasets"])
    configured = {str(row["contract"]["dataset"]) for row in sources}

    assert configured >= required
    assert {str(row["source"]) for row in sources} == {
        "tushare",
        "akshare",
        "local_file",
    }
    for source in sources:
        assert source["profile_name"]
        assert source["request"]["dataset"] == source["contract"]["dataset"]
        assert source["canonicalization"]["value_columns"]
        assert source["contract"]["release_timing"]
        assert source["contract"]["allows_empty"] is False


def test_akshare_mapping_is_explicit_and_preserves_unit_compatible_crosscheck() -> None:
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    source = next(
        row for row in config["daily"]["sources"] if row["source"] == "akshare"
    )

    assert source["probe_endpoint"] == "stock_zh_a_hist"
    assert source["probe_parameters"]["symbol"] == "000001"
    assert source["response_field_mapping"] == {
        "日期": "trade_date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
    }
    # Volume/amount are deliberately not compared because the two providers
    # expose different units at this boundary.
    assert source["canonicalization"]["value_columns"] == [
        "open",
        "high",
        "low",
        "close",
    ]


def test_pit_reference_files_are_required_and_not_claimed_available_by_config() -> None:
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    local_by_dataset = {
        row["contract"]["dataset"]: row
        for row in config["daily"]["sources"]
        if row["source"] == "local_file"
    }
    for dataset in (
        "trade_status",
        "stock_basic",
        "historical_st",
        "industry_classification",
        "company_action",
    ):
        source = local_by_dataset[dataset]
        assert source["profile_name"] == "pit-reference-local"
        assert source["root"] == "data/reference/pit"
        assert source["contract"]["allows_empty"] is False
    historical_st = config["daily"]["data_quality"]["historical_st"]
    assert historical_st == {"path": "data/reference/pit/historical_st.parquet"}

