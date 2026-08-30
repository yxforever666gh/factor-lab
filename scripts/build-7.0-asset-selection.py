#!/usr/bin/env python
"""Freeze the causal, pre-return ETF representative selection for 7.0.

The artifact is intentionally built only from fund metadata and observations
available no later than 2015-02-27.  Current provider status is queried for all
L/D/I states, but it is never used as a survival filter: a fund delisted after
the cutoff remains a valid historical candidate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
from typing import Any, Mapping, Protocol
import unicodedata

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from factor_lab.data.sources import TushareClient  # noqa: E402


CUTOFF = pd.Timestamp("2015-02-27")
CUTOFF_COMPACT = "20150227"
FUND_BASIC_STATUSES = ("L", "D", "I")
MINIMUM_DAILY_ROWS = 252
ADV20_SESSIONS = 20
RECENT_TRADE_CALENDAR_DAYS = 31
PRE_CLOSE_ABS_TOLERANCE_RMB = 0.0011
DEFAULT_OUTPUT = PROJECT_ROOT / "protocols" / "7.0-asset-selection.json"

BASIC_FIELDS = (
    "ts_code",
    "name",
    "fund_type",
    "list_date",
    "delist_date",
    "benchmark",
    "status",
    "invest_type",
    "type",
    "market",
)
DAILY_ADV_FIELDS = ("ts_code", "trade_date", "vol", "amount")
DAILY_CASH_FIELDS = (
    "ts_code",
    "trade_date",
    "pre_close",
    "close",
    "vol",
    "amount",
)
DIVIDEND_FIELDS = (
    "ts_code",
    "ann_date",
    "imp_anndate",
    "div_proc",
    "record_date",
    "ex_date",
    "pay_date",
    "div_cash",
)

CLASS_ORDER = (
    "mainland_broad",
    "hong_kong_broad",
    "united_states_broad",
    "gold",
    "five_year_government_bond",
    "cash_proxy",
)
EXPECTED_SELECTION = {
    "mainland_broad": "510300.SH",
    "hong_kong_broad": "159920.SZ",
    "united_states_broad": "513100.SH",
    "gold": "518880.SH",
    "five_year_government_bond": "511010.SH",
    "cash_proxy": "511880.SH",
}
RULE_DESCRIPTIONS = {
    "mainland_broad": "ETF whose name identifies the plain CSI 300 (沪深300), excluding linked, sector, style and leveraged variants",
    "hong_kong_broad": "ETF tracking the plain Hang Seng Index, excluding China-enterprise, technology and other sub-index variants",
    "united_states_broad": "ETF tracking Nasdaq 100 or S&P 500",
    "gold": "gold ETF",
    "five_year_government_bond": "ETF tracking five-year Chinese government bonds",
    "cash_proxy": "exchange-traded money-market ETF with a reconstructible implemented pre-cutoff cash distribution",
}


class Provider(Protocol):
    def query(self, endpoint: str, **kwargs: Any) -> pd.DataFrame: ...


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def payload_sha256(value: Mapping[str, Any]) -> str:
    payload = dict(value)
    payload.pop("payload_sha256", None)
    return canonical_sha256(payload)


def _call(client: Any, endpoint: str, **kwargs: Any) -> pd.DataFrame:
    direct = getattr(client, endpoint, None)
    if callable(direct):
        result = direct(**kwargs)
    else:
        query = getattr(client, "query", None)
        if not callable(query):
            raise TypeError(f"provider has no {endpoint!r} endpoint or query method")
        result = query(endpoint, **kwargs)
    if result is None:
        return pd.DataFrame()
    if not isinstance(result, pd.DataFrame):
        raise TypeError(f"{endpoint} did not return a pandas DataFrame")
    return result.copy()


def _require_columns(frame: pd.DataFrame, required: tuple[str, ...], *, role: str) -> None:
    missing = sorted(set(required) - set(map(str, frame.columns)))
    if missing:
        raise ValueError(f"{role} missing required columns: {missing}")


def _string(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _normalized_text(value: Any) -> str:
    text = _string(value) or ""
    text = unicodedata.normalize("NFKC", text).upper()
    return re.sub(r"[\s\-_/()（）·]+", "", text)


def _date(value: Any) -> pd.Timestamp | None:
    text = _string(value)
    if text is None:
        return None
    if not re.fullmatch(r"\d{8}", text):
        return None
    parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else pd.Timestamp(parsed).normalize()


def _date_text(value: pd.Timestamp | None) -> str | None:
    return value.date().isoformat() if value is not None else None


def _json_number(value: Any) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("source contains a non-finite numeric value")
    return number


def _content_hash(columns: tuple[str, ...], records: list[dict[str, Any]]) -> str:
    rows = [[record.get(column) for column in columns] for record in records]
    return canonical_sha256({"columns": list(columns), "rows": rows})


def _normalize_basic_source(frame: pd.DataFrame, query_status: str) -> list[dict[str, Any]]:
    _require_columns(frame, BASIC_FIELDS, role=f"fund_basic status={query_status}")
    records: list[dict[str, Any]] = []
    for raw in frame.loc[:, BASIC_FIELDS].to_dict("records"):
        record = {field: _string(raw[field]) for field in BASIC_FIELDS}
        if not record["ts_code"]:
            raise ValueError("fund_basic contains a missing ts_code")
        record["queried_status"] = query_status
        records.append(record)
    return sorted(records, key=lambda row: (str(row["ts_code"]), query_status))


def _merge_basic(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record["ts_code"]), []).append(record)
    merged: list[dict[str, Any]] = []
    for code in sorted(grouped):
        rows = grouped[code]
        comparable = [
            {field: row.get(field) for field in BASIC_FIELDS} for row in rows
        ]
        if any(row != comparable[0] for row in comparable[1:]):
            raise ValueError(f"conflicting fund_basic metadata across L/D/I for {code}")
        item = dict(comparable[0])
        item["queried_statuses"] = sorted({str(row["queried_status"]) for row in rows})
        merged.append(item)
    return merged


def _class_matches(row: Mapping[str, Any], asset_class: str) -> bool:
    name = _normalized_text(row.get("name"))
    benchmark = _normalized_text(row.get("benchmark"))
    fund_type = _normalized_text(row.get("fund_type"))
    invest_type = _normalized_text(row.get("invest_type"))
    combined = name + benchmark
    is_etf = "ETF" in name and "联接" not in name
    if not is_etf:
        return False
    if asset_class == "mainland_broad":
        excluded = ("医药", "非银行", "地产", "等权", "成长", "价值", "高贝塔", "低波", "增强")
        return "沪深300" in combined and not any(token in combined for token in excluded)
    if asset_class == "hong_kong_broad":
        excluded = (
            "中国企业",
            "国企",
            "科技",
            "医疗",
            "互联网",
            "生物",
            "高股息",
            "红利",
        )
        plain = "恒生指数" in combined or "恒生ETF" in name
        return plain and not any(token in combined for token in excluded)
    if asset_class == "united_states_broad":
        return "纳斯达克100" in combined or "标普500" in combined
    if asset_class == "gold":
        return "黄金" in combined
    if asset_class == "five_year_government_bond":
        return "5年期国债" in combined or "5年国债" in combined
    if asset_class == "cash_proxy":
        return "货币" in name or "货币" in fund_type or "货币" in invest_type
    raise ValueError(f"unknown asset class: {asset_class}")


def _metadata_exclusions(row: Mapping[str, Any]) -> tuple[list[str], pd.Timestamp | None]:
    reasons: list[str] = []
    listed = _date(row.get("list_date"))
    delisted = _date(row.get("delist_date"))
    if listed is None:
        reasons.append("missing_or_invalid_list_date")
    elif listed > CUTOFF:
        reasons.append("listed_after_cutoff")
    if row.get("delist_date") is not None and delisted is None:
        reasons.append("invalid_delist_date")
    elif delisted is not None and delisted <= CUTOFF:
        reasons.append("delisted_on_or_before_cutoff")
    return reasons, listed


def _normalize_daily(
    frame: pd.DataFrame, code: str, fields: tuple[str, ...]
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    _require_columns(frame, fields, role=f"fund_daily {code}")
    work = frame.loc[:, fields].copy()
    if work.empty:
        return work, []
    codes = work["ts_code"].astype("string").str.strip()
    if codes.isna().any() or set(codes.astype(str)) != {code}:
        raise ValueError(f"fund_daily returned a wrong ts_code for {code}")
    text_dates = work["trade_date"].astype("string")
    if text_dates.isna().any() or not text_dates.str.fullmatch(r"\d{8}").all():
        raise ValueError(f"fund_daily contains an invalid trade_date for {code}")
    work["trade_date"] = pd.to_datetime(text_dates, format="%Y%m%d", errors="coerce")
    if work["trade_date"].isna().any() or work["trade_date"].duplicated().any():
        raise ValueError(f"fund_daily contains duplicate or invalid dates for {code}")
    if bool(work["trade_date"].gt(CUTOFF).any()):
        raise ValueError(f"fund_daily returned a row after the selection cutoff for {code}")
    for column in set(fields) - {"ts_code", "trade_date"}:
        work[column] = pd.to_numeric(work[column], errors="coerce")
        if work[column].isna().any() or not np.isfinite(work[column]).all():
            raise ValueError(f"fund_daily contains invalid {column} for {code}")
    if bool(work["vol"].lt(0).any() or work["amount"].lt(0).any()):
        raise ValueError(f"fund_daily contains negative volume or amount for {code}")
    if "pre_close" in work and bool(
        work[["pre_close", "close"]].le(0).any(axis=None)
    ):
        raise ValueError(f"fund_daily contains non-positive prices for {code}")
    work = work.sort_values("trade_date", kind="mergesort").reset_index(drop=True)
    records: list[dict[str, Any]] = []
    for row in work.to_dict("records"):
        record: dict[str, Any] = {
            "ts_code": code,
            "trade_date": pd.Timestamp(row["trade_date"]).strftime("%Y%m%d"),
        }
        for column in fields[2:]:
            record[column] = _json_number(row[column])
        records.append(record)
    return work, records


def _normalize_dividends(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty and not len(frame.columns):
        return []
    _require_columns(frame, DIVIDEND_FIELDS, role="fund_div exact-ex-date response")
    records: list[dict[str, Any]] = []
    for raw in frame.loc[:, DIVIDEND_FIELDS].to_dict("records"):
        observed_code = _string(raw["ts_code"])
        if observed_code is None:
            raise ValueError("fund_div returned a missing ts_code")
        record: dict[str, Any] = {"ts_code": observed_code}
        for field in DIVIDEND_FIELDS[1:-1]:
            record[field] = _string(raw[field])
        record["div_cash"] = (
            None if raw["div_cash"] is None or pd.isna(raw["div_cash"]) else _json_number(raw["div_cash"])
        )
        records.append(record)
    return sorted(
        records,
        key=lambda row: (
            str(row.get("ex_date") or ""),
            float(row.get("div_cash") or 0.0),
            str(row.get("ann_date") or ""),
        ),
    )


def _cash_distribution_admission(
    client: Any,
    code: str,
    daily: pd.DataFrame,
) -> dict[str, Any]:
    suspicious_actions: list[dict[str, Any]] = []
    for index in range(1, len(daily)):
        prior_close = float(daily.iloc[index - 1]["close"])
        observed_pre_close = float(daily.iloc[index]["pre_close"])
        if not math.isclose(
            observed_pre_close,
            prior_close,
            rel_tol=0.0,
            abs_tol=PRE_CLOSE_ABS_TOLERANCE_RMB,
        ):
            ex_date = pd.Timestamp(daily.iloc[index]["trade_date"]).normalize()
            if ex_date > CUTOFF:
                raise ValueError(f"cash action detector crossed cutoff for {code}")
            suspicious_actions.append(
                {
                    "ex_date": ex_date.date().isoformat(),
                    "prior_raw_close_rmb": prior_close,
                    "observed_pre_close_rmb": observed_pre_close,
                    "implied_action_rmb_per_unit": prior_close - observed_pre_close,
                }
            )

    cutoff_records: list[dict[str, Any]] = []
    exact_queries: list[dict[str, Any]] = []
    queried_ex_dates: list[str] = []
    for action in suspicious_actions:
        ex_date = pd.Timestamp(action["ex_date"])
        compact = ex_date.strftime("%Y%m%d")
        query = {
            "ex_date": compact,
            "fields": ",".join(DIVIDEND_FIELDS),
        }
        response = _normalize_dividends(_call(client, "fund_div", **query))
        wrong_dates = sorted(
            {
                str(record.get("ex_date"))
                for record in response
                if str(record.get("ex_date") or "") != compact
            }
        )
        if wrong_dates:
            raise ValueError(
                f"fund_div exact ex_date query {compact} returned other dates: {wrong_dates}"
            )
        matching = [
            record for record in response if str(record.get("ts_code")) == code
        ]
        cutoff_records.extend(matching)
        queried_ex_dates.append(ex_date.date().isoformat())
        exact_queries.append(
            {
                "endpoint": "fund_div",
                **query,
                "provider_row_count": len(response),
                "provider_content_sha256": _content_hash(DIVIDEND_FIELDS, response),
                "matching_ticker_row_count": len(matching),
                "matching_ticker_content_sha256": _content_hash(
                    DIVIDEND_FIELDS, matching
                ),
            }
        )

    daily_by_date = daily.set_index("trade_date", drop=False)
    dates = list(pd.DatetimeIndex(daily_by_date.index))
    position = {date: index for index, date in enumerate(dates)}
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    seen_economic_events: set[tuple[str, float]] = set()
    amounts_by_ex_date: dict[str, set[float]] = {}
    for record in cutoff_records:
        ex_date_text = str(record.get("ex_date") or "")
        cash = record.get("div_cash")
        if cash is not None and math.isfinite(float(cash)):
            amounts_by_ex_date.setdefault(ex_date_text, set()).add(float(cash))
    conflicting_ex_dates = {
        ex_date for ex_date, amounts in amounts_by_ex_date.items() if len(amounts) > 1
    }
    for record in cutoff_records:
        reasons: list[str] = []
        dates_by_field = {
            field: _date(record.get(field))
            for field in ("ann_date", "imp_anndate", "record_date", "ex_date", "pay_date")
        }
        if any(value is None for value in dates_by_field.values()):
            reasons.append("incomplete_distribution_dates")
        elif not (
            dates_by_field["ann_date"] <= dates_by_field["imp_anndate"]
            <= dates_by_field["record_date"]
            <= dates_by_field["ex_date"]
            <= dates_by_field["pay_date"]
            <= CUTOFF
        ):
            reasons.append("noncausal_distribution_date_order")
        if "实施" not in _normalized_text(record.get("div_proc")):
            reasons.append("distribution_not_implemented")
        cash = record.get("div_cash")
        if cash is None or not math.isfinite(float(cash)) or float(cash) <= 0.0:
            reasons.append("invalid_distribution_cash")
        if str(record.get("ex_date") or "") in conflicting_ex_dates:
            reasons.append("conflicting_distribution_cash_on_ex_date")
        ex_date = dates_by_field["ex_date"]
        if ex_date is None or ex_date not in position or position.get(ex_date, 0) == 0:
            reasons.append("distribution_ex_date_not_reconstructible")
        if not reasons:
            index = position[ex_date]
            prior_close = float(daily.iloc[index - 1]["close"])
            observed_pre_close = float(daily_by_date.at[ex_date, "pre_close"])
            expected_pre_close = prior_close - float(cash)
            error = observed_pre_close - expected_pre_close
            if expected_pre_close <= 0.0 or not math.isclose(
                observed_pre_close,
                expected_pre_close,
                rel_tol=0.0,
                abs_tol=PRE_CLOSE_ABS_TOLERANCE_RMB,
            ):
                reasons.append("distribution_pre_close_reconciliation_failed")
            else:
                key = (ex_date.date().isoformat(), float(cash))
                if key not in seen_economic_events:
                    accepted.append(
                        {
                            "ann_date": _date_text(dates_by_field["ann_date"]),
                            "imp_anndate": _date_text(dates_by_field["imp_anndate"]),
                            "record_date": _date_text(dates_by_field["record_date"]),
                            "ex_date": _date_text(ex_date),
                            "pay_date": _date_text(dates_by_field["pay_date"]),
                            "div_cash_per_fund_unit_rmb": float(cash),
                            "prior_close_rmb": prior_close,
                            "observed_pre_close_rmb": observed_pre_close,
                            "reconciliation_error_rmb": error,
                        }
                    )
                    seen_economic_events.add(key)
        if reasons:
            rejected.append(
                {
                    "ann_date": record.get("ann_date"),
                    "ex_date": record.get("ex_date"),
                    "div_cash": record.get("div_cash"),
                    "reasons": sorted(set(reasons)),
                }
            )
    return {
        "unbounded_query_count": 0,
        "queried_ex_dates": queried_ex_dates,
        "exact_ex_date_queries": exact_queries,
        "suspicious_action_count": len(suspicious_actions),
        "suspicious_actions": suspicious_actions,
        "cutoff_source_row_count": len(cutoff_records),
        "cutoff_source_content_sha256": _content_hash(DIVIDEND_FIELDS, cutoff_records),
        "accepted_event_count": len(accepted),
        "accepted_events": accepted,
        "rejected_events": rejected,
        "passed": bool(accepted),
        "admission_rule": "at least one implemented pre-cutoff cash distribution with complete causal dates and raw pre_close reconciliation",
    }


def _evaluate_candidate(
    client: Any,
    row: Mapping[str, Any],
    asset_class: str,
) -> dict[str, Any]:
    code = str(row["ts_code"])
    reasons, listed = _metadata_exclusions(row)
    result: dict[str, Any] = {
        "ts_code": code,
        "name": row.get("name"),
        "benchmark": row.get("benchmark"),
        "fund_type": row.get("fund_type"),
        "invest_type": row.get("invest_type"),
        "provider_current_status": row.get("status"),
        "queried_statuses": row.get("queried_statuses"),
        "list_date": _date_text(listed),
        "delist_date": _date_text(_date(row.get("delist_date"))),
        "daily_source": None,
        "cash_distribution_admission": None,
        "eligible": False,
        "exclusion_reasons": list(reasons),
        "adv20_rmb": None,
    }
    if reasons:
        return result

    daily_fields = DAILY_CASH_FIELDS if asset_class == "cash_proxy" else DAILY_ADV_FIELDS
    query = {
        "ts_code": code,
        "start_date": listed.strftime("%Y%m%d"),
        "end_date": CUTOFF_COMPACT,
        "fields": ",".join(daily_fields),
    }
    daily, daily_records = _normalize_daily(
        _call(client, "fund_daily", **query), code, daily_fields
    )
    source: dict[str, Any] = {
        "query": {"endpoint": "fund_daily", **query},
        "row_count": len(daily),
        "content_sha256": _content_hash(daily_fields, daily_records),
        "first_trade_date": (
            _date_text(pd.Timestamp(daily["trade_date"].min())) if not daily.empty else None
        ),
        "last_trade_date": (
            _date_text(pd.Timestamp(daily["trade_date"].max())) if not daily.empty else None
        ),
        "last_positive_trade_date": None,
    }
    result["daily_source"] = source
    if len(daily) < MINIMUM_DAILY_ROWS:
        reasons.append("fewer_than_252_pre_cutoff_daily_rows")
    positive = daily.loc[(daily["vol"] > 0.0) & (daily["amount"] > 0.0)]
    last_positive = (
        pd.Timestamp(positive["trade_date"].max()) if not positive.empty else None
    )
    source["last_positive_trade_date"] = _date_text(last_positive)
    recent_floor = CUTOFF - pd.Timedelta(days=RECENT_TRADE_CALENDAR_DAYS)
    if last_positive is None or last_positive < recent_floor:
        reasons.append("no_positive_trade_within_31_calendar_days_of_cutoff")
    if len(daily) < ADV20_SESSIONS:
        reasons.append("fewer_than_20_rows_for_adv20")
    else:
        adv20 = math.fsum(float(value) * 1_000.0 for value in daily.tail(20)["amount"]) / 20.0
        if not math.isfinite(adv20) or adv20 <= 0.0:
            reasons.append("nonpositive_or_invalid_adv20")
        else:
            result["adv20_rmb"] = adv20

    if asset_class == "cash_proxy":
        admission = _cash_distribution_admission(client, code, daily)
        result["cash_distribution_admission"] = admission
        if not admission["passed"]:
            reasons.append("cash_distribution_not_reconstructible_before_cutoff")

    result["exclusion_reasons"] = sorted(set(reasons))
    result["eligible"] = not result["exclusion_reasons"]
    return result


def compute_asset_selection(client: Any) -> dict[str, Any]:
    basic_sources: dict[str, Any] = {}
    all_basic: list[dict[str, Any]] = []
    for status in FUND_BASIC_STATUSES:
        query = {
            "market": "E",
            "status": status,
            "fields": ",".join(BASIC_FIELDS),
        }
        records = _normalize_basic_source(
            _call(client, "fund_basic", **query), status
        )
        all_basic.extend(records)
        source_columns = (*BASIC_FIELDS, "queried_status")
        basic_sources[status] = {
            "query": {"endpoint": "fund_basic", **query},
            "row_count": len(records),
            "content_sha256": _content_hash(source_columns, records),
        }
    master = _merge_basic(all_basic)

    classes: dict[str, Any] = {}
    selected_codes: list[str] = []
    for asset_class in CLASS_ORDER:
        matched = [row for row in master if _class_matches(row, asset_class)]
        candidates = [
            _evaluate_candidate(client, row, asset_class)
            for row in sorted(matched, key=lambda item: str(item["ts_code"]))
        ]
        eligible = [candidate for candidate in candidates if candidate["eligible"]]
        eligible.sort(key=lambda candidate: (-float(candidate["adv20_rmb"]), str(candidate["ts_code"])))
        selected = str(eligible[0]["ts_code"]) if eligible else None
        classes[asset_class] = {
            "rule": RULE_DESCRIPTIONS[asset_class],
            "candidate_count": len(candidates),
            "eligible_count": len(eligible),
            "selected_ts_code": selected,
            "ranking": "descending cutoff ADV20 RMB, then ts_code ascending",
            "candidates": candidates,
        }
        if selected is None:
            raise RuntimeError(f"no eligible pre-cutoff candidate for {asset_class}")
        selected_codes.append(selected)

    expected_codes = [EXPECTED_SELECTION[asset_class] for asset_class in CLASS_ORDER]
    if selected_codes != expected_codes:
        raise RuntimeError(
            "causal asset selection differs from the frozen 7.0 registry; "
            f"expected={expected_codes}, actual={selected_codes}"
        )

    master_columns = (*BASIC_FIELDS, "queried_statuses")
    artifact: dict[str, Any] = {
        "schema_version": 1,
        "kind": "factor_lab_7_0_asset_selection",
        "contract_id": "factor-lab/7.0/pre-return-etf-selection/1",
        "cutoff_date": CUTOFF.date().isoformat(),
        "selection_information_boundary": "fund_basic metadata plus fund_daily/fund_div observations no later than cutoff; no return evaluation",
        "status_queries": list(FUND_BASIC_STATUSES),
        "current_status_not_used_as_survival_filter": True,
        "minimum_pre_cutoff_daily_rows": MINIMUM_DAILY_ROWS,
        "recent_positive_trade_calendar_days": RECENT_TRADE_CALENDAR_DAYS,
        "adv20": {
            "sessions": ADV20_SESSIONS,
            "amount_conversion": "Tushare fund_daily amount times 1000 equals RMB",
            "volume_conversion": "Tushare fund_daily vol times 100 equals fund units",
        },
        "cash_distribution_contract": {
            "implemented_event_required": True,
            "query_boundary": "derive suspicious pre_close action dates from cutoff daily first, then query fund_div by exact ex_date only",
            "unbounded_fund_div_queries_forbidden": True,
            "complete_fields": [
                "ann_date",
                "imp_anndate",
                "record_date",
                "ex_date",
                "pay_date",
                "div_cash",
            ],
            "pre_close_equation": "pre_close_on_ex_date = prior_raw_close - div_cash_per_fund_unit",
            "pre_close_absolute_tolerance_rmb": PRE_CLOSE_ABS_TOLERANCE_RMB,
            "automatic_unit_accretion_or_present-day_NAV_backfill_forbidden": True,
        },
        "fund_basic_sources": basic_sources,
        "merged_master": {
            "row_count": len(master),
            "content_sha256": _content_hash(master_columns, master),
        },
        "class_order": list(CLASS_ORDER),
        "classes": classes,
        "selected_codes": selected_codes,
        "expected_codes": expected_codes,
        "post_cutoff_fund_daily_requested": False,
        "fund_div_query_audit": {
            "unbounded_query_count": 0,
            "queried_ex_dates_by_cash_candidate": {
                str(candidate["ts_code"]): list(
                    (candidate.get("cash_distribution_admission") or {}).get(
                        "queried_ex_dates", []
                    )
                )
                for candidate in classes["cash_proxy"]["candidates"]
                if candidate.get("cash_distribution_admission") is not None
            },
        },
    }
    artifact["payload_sha256"] = payload_sha256(artifact)
    return artifact


def load_asset_selection(path: str | Path) -> dict[str, Any]:
    source = Path(path)
    value = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("asset selection artifact must be a JSON object")
    if value.get("payload_sha256") != payload_sha256(value):
        raise ValueError("asset selection payload hash mismatch")
    if (
        value.get("kind") != "factor_lab_7_0_asset_selection"
        or value.get("cutoff_date") != CUTOFF.date().isoformat()
        or value.get("selected_codes")
        != [EXPECTED_SELECTION[asset_class] for asset_class in CLASS_ORDER]
    ):
        raise ValueError("asset selection identity mismatch")
    return value


def build_asset_selection(client: Any, output: str | Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    destination = Path(output).resolve()
    if destination.exists() or destination.is_symlink():
        raise FileExistsError("7.0 asset selection artifact is create-only")
    artifact = compute_asset_selection(client)
    destination.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        artifact,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
        allow_nan=False,
    ).encode("utf-8") + b"\n"
    try:
        with destination.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise FileExistsError("7.0 asset selection artifact is create-only") from exc
    load_asset_selection(destination)
    return artifact


def _configured_client(config_path: Path) -> TushareClient:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    sync = config.get("sync")
    if not isinstance(sync, Mapping):
        raise ValueError("data config lacks sync settings")
    token: str | None = None
    token_file = str(sync.get("token_file") or "").strip()
    if token_file:
        path = Path(token_file).expanduser()
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        if path.is_file():
            token = path.read_text(encoding="utf-8").strip() or None
    return TushareClient(
        token=token,
        token_env=str(sync.get("token_env") or "TUSHARE_TOKEN"),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(PROJECT_ROOT / "configs" / "data.json"))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args(argv)
    artifact = build_asset_selection(
        _configured_client(Path(args.config).resolve()),
        Path(args.output),
    )
    print(f"selected_codes={','.join(artifact['selected_codes'])}")
    print(f"payload_sha256={artifact['payload_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
