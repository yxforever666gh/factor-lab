from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "build-7.0-asset-selection.py"
SPEC = importlib.util.spec_from_file_location("build_7_0_asset_selection", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
ASSET_SELECTION = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ASSET_SELECTION)


def _basic(
    ts_code: str,
    name: str,
    benchmark: str,
    *,
    status: str = "L",
    list_date: str = "20130102",
    delist_date: str | None = None,
    fund_type: str = "股票型",
    invest_type: str = "被动指数型",
) -> dict[str, Any]:
    return {
        "ts_code": ts_code,
        "name": name,
        "fund_type": fund_type,
        "list_date": list_date,
        "delist_date": delist_date,
        "benchmark": benchmark,
        "status": status,
        "invest_type": invest_type,
        "type": fund_type,
        "market": "E",
    }


def _daily(ts_code: str, amount_thousand_rmb: float) -> pd.DataFrame:
    dates = pd.bdate_range(end="2015-02-27", periods=300)
    close = np.full(len(dates), 100.0)
    pre_close = np.full(len(dates), 100.0)
    # The implemented cash event used by 511880 is on this row.  Other funds
    # retain the ordinary prior-close relation.
    if ts_code == "511880.SH":
        pre_close[220] = 99.0
    return pd.DataFrame(
        {
            "ts_code": ts_code,
            "trade_date": dates.strftime("%Y%m%d"),
            "pre_close": pre_close,
            "close": close,
            "vol": np.full(len(dates), amount_thousand_rmb * 10.0),
            "amount": np.full(len(dates), amount_thousand_rmb),
        }
    )


class FakeProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.basic_by_status = {
            "L": pd.DataFrame(
                [
                    _basic("510300.SH", "华泰柏瑞沪深300ETF", "沪深300指数×100%"),
                    _basic("159920.SZ", "华夏恒生ETF(QDII)", "香港恒生指数×100%"),
                    _basic("513100.SH", "国泰纳斯达克100ETF(QDII)", "纳斯达克100指数×100%"),
                    _basic("513500.SH", "博时标普500ETF(QDII)", "标普500指数×100%", list_date="20140115"),
                    _basic("518880.SH", "华安黄金ETF", "国内黄金现货价格收益率×100%", fund_type="其他"),
                    _basic("518881.SH", "测试黄金ETF", "国内黄金现货价格收益率×100%", fund_type="其他"),
                    _basic("511010.SH", "国泰上证5年期国债ETF", "上证5年期国债指数×100%", fund_type="债券型"),
                    _basic("511880.SH", "银华货币ETF-A", "活期存款利率×100%", fund_type="货币型", invest_type="货币型"),
                    _basic("511990.SH", "华宝现金添益货币ETF-A", "七天通知存款利率×100%", fund_type="货币型", invest_type="货币型"),
                ],
                columns=list(ASSET_SELECTION.BASIC_FIELDS),
            ),
            "D": pd.DataFrame(
                [
                    # It is delisted today, but it was alive at the cutoff and
                    # must remain in the historical candidate/ranking set.
                    _basic(
                        "510301.SH",
                        "历史退市沪深300ETF",
                        "沪深300指数×100%",
                        status="D",
                        delist_date="20160115",
                    ),
                    _basic(
                        "510302.SH",
                        "截止日前已退市沪深300ETF",
                        "沪深300指数×100%",
                        status="D",
                        delist_date="20140115",
                    ),
                ],
                columns=list(ASSET_SELECTION.BASIC_FIELDS),
            ),
            "I": pd.DataFrame(
                [
                    _basic(
                        "510399.SH",
                        "未来上市沪深300ETF",
                        "沪深300指数×100%",
                        status="I",
                        list_date="20150302",
                    )
                ],
                columns=list(ASSET_SELECTION.BASIC_FIELDS),
            ),
        }
        self.amounts = {
            "510300.SH": 1_000_000.0,
            "510301.SH": 100_000.0,
            "159920.SZ": 900_000.0,
            "513100.SH": 800_000.0,
            "513500.SH": 700_000.0,
            # Exact ADV tie proves the ascending-code secondary ordering.
            "518880.SH": 600_000.0,
            "518881.SH": 600_000.0,
            "511010.SH": 500_000.0,
            "511880.SH": 400_000.0,
            # This would win on ADV, but its cash return is not reconstructible
            # from the frozen fund_div + raw pre_close contract.
            "511990.SH": 2_000_000.0,
        }

    def query(self, endpoint: str, **kwargs: Any) -> pd.DataFrame:
        self.calls.append((endpoint, dict(kwargs)))
        if endpoint == "fund_basic":
            return self.basic_by_status[str(kwargs["status"])].copy()
        if endpoint == "fund_daily":
            code = str(kwargs["ts_code"])
            frame = _daily(code, self.amounts[code])
            fields = str(kwargs["fields"]).split(",")
            return frame.loc[:, fields].copy()
        if endpoint == "fund_div":
            columns = list(ASSET_SELECTION.DIVIDEND_FIELDS)
            dates = pd.bdate_range(end="2015-02-27", periods=300)
            ex_date = dates[220]
            if str(kwargs["ex_date"]) != ex_date.strftime("%Y%m%d"):
                return pd.DataFrame(columns=columns)
            return pd.DataFrame(
                [
                    {
                        "ts_code": "511880.SH",
                        "ann_date": dates[215].strftime("%Y%m%d"),
                        "imp_anndate": dates[215].strftime("%Y%m%d"),
                        "div_proc": "实施",
                        "record_date": dates[219].strftime("%Y%m%d"),
                        "ex_date": ex_date.strftime("%Y%m%d"),
                        "pay_date": dates[222].strftime("%Y%m%d"),
                        "div_cash": 1.0,
                    },
                    # An exact supplier duplicate is one economic event.
                    {
                        "ts_code": "511880.SH",
                        "ann_date": dates[215].strftime("%Y%m%d"),
                        "imp_anndate": dates[215].strftime("%Y%m%d"),
                        "div_proc": "实施",
                        "record_date": dates[219].strftime("%Y%m%d"),
                        "ex_date": ex_date.strftime("%Y%m%d"),
                        "pay_date": dates[222].strftime("%Y%m%d"),
                        "div_cash": 1.0,
                    },
                    # Exact-date queries may legitimately return other funds;
                    # selection must filter the requested historical candidate
                    # after the bounded response arrives.
                    {
                        "ts_code": "599999.SH",
                        "ann_date": dates[215].strftime("%Y%m%d"),
                        "imp_anndate": dates[215].strftime("%Y%m%d"),
                        "div_proc": "实施",
                        "record_date": dates[219].strftime("%Y%m%d"),
                        "ex_date": ex_date.strftime("%Y%m%d"),
                        "pay_date": dates[222].strftime("%Y%m%d"),
                        "div_cash": 0.5,
                    },
                ],
                columns=columns,
            )
        raise AssertionError(f"unexpected endpoint: {endpoint}")


def _candidate(artifact: dict[str, Any], asset_class: str, code: str) -> dict[str, Any]:
    matches = [
        row
        for row in artifact["classes"][asset_class]["candidates"]
        if row["ts_code"] == code
    ]
    assert len(matches) == 1
    return matches[0]


def test_cutoff_universe_selection_cash_admission_tie_and_sources(tmp_path: Path) -> None:
    provider = FakeProvider()
    output = tmp_path / "protocols" / "7.0-asset-selection.json"
    artifact = ASSET_SELECTION.build_asset_selection(provider, output)

    assert artifact["selected_codes"] == [
        "510300.SH",
        "159920.SZ",
        "513100.SH",
        "518880.SH",
        "511010.SH",
        "511880.SH",
    ]
    assert artifact["payload_sha256"] == ASSET_SELECTION.payload_sha256(artifact)
    assert ASSET_SELECTION.load_asset_selection(output) == artifact
    assert set(artifact["fund_basic_sources"]) == {"L", "D", "I"}
    assert all(
        len(source["content_sha256"]) == 64
        for source in artifact["fund_basic_sources"].values()
    )

    later_delisted = _candidate(artifact, "mainland_broad", "510301.SH")
    assert later_delisted["provider_current_status"] == "D"
    assert later_delisted["eligible"] is True
    assert later_delisted["exclusion_reasons"] == []

    already_delisted = _candidate(artifact, "mainland_broad", "510302.SH")
    future_listing = _candidate(artifact, "mainland_broad", "510399.SH")
    assert already_delisted["exclusion_reasons"] == ["delisted_on_or_before_cutoff"]
    assert future_listing["exclusion_reasons"] == ["listed_after_cutoff"]

    excluded_cash = _candidate(artifact, "cash_proxy", "511990.SH")
    selected_cash = _candidate(artifact, "cash_proxy", "511880.SH")
    assert excluded_cash["adv20_rmb"] > selected_cash["adv20_rmb"]
    assert excluded_cash["exclusion_reasons"] == [
        "cash_distribution_not_reconstructible_before_cutoff"
    ]
    assert excluded_cash["cash_distribution_admission"]["unbounded_query_count"] == 0
    assert excluded_cash["cash_distribution_admission"]["queried_ex_dates"] == []
    assert excluded_cash["cash_distribution_admission"]["exact_ex_date_queries"] == []
    assert selected_cash["cash_distribution_admission"]["passed"] is True
    assert selected_cash["cash_distribution_admission"]["accepted_event_count"] == 1
    assert selected_cash["cash_distribution_admission"]["unbounded_query_count"] == 0
    assert len(selected_cash["cash_distribution_admission"]["queried_ex_dates"]) == 1
    assert selected_cash["cash_distribution_admission"]["exact_ex_date_queries"][0][
        "provider_row_count"
    ] == 3
    assert selected_cash["cash_distribution_admission"]["exact_ex_date_queries"][0][
        "matching_ticker_row_count"
    ] == 2
    assert artifact["fund_div_query_audit"]["unbounded_query_count"] == 0

    gold = artifact["classes"]["gold"]
    assert _candidate(artifact, "gold", "518880.SH")["adv20_rmb"] == _candidate(
        artifact, "gold", "518881.SH"
    )["adv20_rmb"]
    assert gold["selected_ts_code"] == "518880.SH"

    basic_calls = [kwargs for endpoint, kwargs in provider.calls if endpoint == "fund_basic"]
    assert [call["status"] for call in basic_calls] == ["L", "D", "I"]
    daily_calls = [kwargs for endpoint, kwargs in provider.calls if endpoint == "fund_daily"]
    assert daily_calls
    assert all(call["end_date"] == "20150227" for call in daily_calls)
    assert not any(call["ts_code"] in {"510302.SH", "510399.SH"} for call in daily_calls)
    dividend_calls = [
        kwargs for endpoint, kwargs in provider.calls if endpoint == "fund_div"
    ]
    assert len(dividend_calls) == 1
    assert all("ex_date" in call and "ts_code" not in call for call in dividend_calls)
    assert all(call["ex_date"] <= "20150227" for call in dividend_calls)


def test_create_only_fails_before_new_provider_requests(tmp_path: Path) -> None:
    provider = FakeProvider()
    output = tmp_path / "7.0-asset-selection.json"
    ASSET_SELECTION.build_asset_selection(provider, output)
    calls = len(provider.calls)
    with pytest.raises(FileExistsError, match="create-only"):
        ASSET_SELECTION.build_asset_selection(provider, output)
    assert len(provider.calls) == calls


def test_tamper_and_rehashed_identity_tamper_are_rejected(tmp_path: Path) -> None:
    output = tmp_path / "7.0-asset-selection.json"
    ASSET_SELECTION.build_asset_selection(FakeProvider(), output)

    value = json.loads(output.read_text(encoding="utf-8"))
    value["classes"]["gold"]["candidates"][0]["adv20_rmb"] += 1.0
    output.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ValueError, match="payload hash mismatch"):
        ASSET_SELECTION.load_asset_selection(output)

    value["payload_sha256"] = ASSET_SELECTION.payload_sha256(value)
    value["selected_codes"][-1] = "511990.SH"
    value["payload_sha256"] = ASSET_SELECTION.payload_sha256(value)
    output.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ValueError, match="identity mismatch"):
        ASSET_SELECTION.load_asset_selection(output)
