"""Point-in-time normalization for Tushare stock dividend actions.

The provider endpoint is documented at
https://tushare.pro/document/2?doc_id=103.  This module performs no I/O: it
normalizes captured responses and collapses announcement duplicates while
preserving economically distinct distributions that share an ex-date.
"""

from __future__ import annotations

from hashlib import sha256
import json
from typing import Any, Iterable

import numpy as np
import pandas as pd


DIVIDEND_IMPLEMENTED = "实施"
DIVIDEND_FIELDS = (
    "ts_code",
    "end_date",
    "ann_date",
    "div_proc",
    "stk_div",
    "stk_bo_rate",
    "stk_co_rate",
    "cash_div",
    "cash_div_tax",
    "record_date",
    "ex_date",
    "pay_date",
    "div_listdate",
    "imp_ann_date",
    "base_date",
    "base_share",
)
DIVIDEND_DATE_FIELDS = (
    "end_date",
    "ann_date",
    "record_date",
    "ex_date",
    "pay_date",
    "div_listdate",
    "imp_ann_date",
    "base_date",
)
DIVIDEND_NUMERIC_FIELDS = (
    "stk_div",
    "stk_bo_rate",
    "stk_co_rate",
    "cash_div",
    "cash_div_tax",
    "base_share",
)
CANONICAL_ACTION_COLUMNS = (
    "action_id",
    "ticker",
    "end_date",
    "record_date",
    "ex_date",
    "pay_date",
    "share_list_date",
    "available_date",
    "stock_dividend_per_share",
    "stock_bonus_per_share",
    "stock_capitalization_per_share",
    "cash_dividend_after_tax_per_share",
    "cash_dividend_before_tax_per_share",
    "source_row_count",
)
CNINFO_RESPONSE_COLUMNS = (
    "ticker",
    "implementation_ann_date",
    "dividend_type",
    "bonus_per_10",
    "capitalization_per_10",
    "cash_before_tax_per_10",
    "record_date",
    "ex_date",
    "pay_date",
    "share_arrival_date",
    "plan_text",
    "report_period",
)
CNINFO_ACTION_COLUMNS = (
    "action_id",
    "ticker",
    "report_period",
    "dividend_type",
    "available_date",
    "record_date",
    "ex_date",
    "pay_date",
    "share_arrival_date",
    "bonus_per_share",
    "capitalization_per_share",
    "stock_dividend_per_share",
    "cash_dividend_before_tax_per_share",
    "source_row_count",
)
RESOLVED_CNINFO_ACTION_COLUMNS = (
    *CNINFO_ACTION_COLUMNS,
    "share_arrival_source",
)
RESOLVED_ACTION_COLUMNS = (
    *RESOLVED_CNINFO_ACTION_COLUMNS,
    "economic_source",
)
TUSHARE_REFERENCE_DIAGNOSTIC_COLUMNS = (
    "action_id",
    "ticker",
    "record_date",
    "ex_date",
    "previous_session",
    "previous_close",
    "provider_pre_close",
    "previous_adj_factor",
    "ex_adj_factor",
    "factor_ratio",
    "price_ratio",
    "theoretical_reference",
    "theoretical_reference_relative_error",
    "factor_reference_absolute_ratio_error",
    "factor_jump",
    "fallback_eligible",
    "status",
)


class CorporateActionDataError(ValueError):
    """Raised when a captured corporate-action response is ambiguous."""


def _canonical_sha256(value: Any) -> str:
    text = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return sha256(text.encode("utf-8")).hexdigest()


def _date_values(values: pd.Series, *, field: str) -> pd.Series:
    text = values.astype("string").str.strip().replace("", pd.NA)
    invalid_shape = text.notna() & ~text.str.fullmatch(r"\d{8}", na=False)
    if invalid_shape.any():
        raise CorporateActionDataError(f"{field} contains a non-YYYYMMDD value")
    parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce").dt.normalize()
    if (text.notna() & parsed.isna()).any():
        raise CorporateActionDataError(f"{field} contains an invalid date")
    return parsed


def normalize_dividend_response(
    frame: pd.DataFrame,
    *,
    expected_tickers: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Return a schema-stable, canonically sorted provider response."""

    if not isinstance(frame, pd.DataFrame):
        raise TypeError("dividend response must be a pandas DataFrame")
    if tuple(map(str, frame.columns)) != DIVIDEND_FIELDS:
        raise CorporateActionDataError("dividend response columns differ")
    work = frame.copy()
    work["ts_code"] = work["ts_code"].astype("string").str.strip()
    work["div_proc"] = work["div_proc"].astype("string").str.strip()
    if work["ts_code"].isna().any() or work["ts_code"].eq("").any():
        raise CorporateActionDataError("dividend response contains an unknown ticker")
    allowed = (
        {str(value).strip() for value in expected_tickers}
        if expected_tickers is not None
        else None
    )
    if allowed is not None:
        unexpected = sorted(set(work["ts_code"].astype(str)) - allowed)
        if unexpected:
            raise CorporateActionDataError(
                f"dividend response contains unexpected tickers: {unexpected[:10]}"
            )
    for column in DIVIDEND_DATE_FIELDS:
        text = work[column].astype("string").str.strip().replace("", pd.NA)
        _date_values(text, field=column)
        work[column] = text
    for column in DIVIDEND_NUMERIC_FIELDS:
        numeric = pd.to_numeric(work[column], errors="coerce").astype(float)
        source_missing = work[column].isna() | work[column].astype("string").str.strip().eq("")
        invalid = ~source_missing & ~np.isfinite(numeric)
        if invalid.any():
            raise CorporateActionDataError(
                f"dividend response contains invalid {column}"
            )
        if numeric.dropna().lt(0.0).any():
            raise CorporateActionDataError(
                f"dividend response contains negative {column}"
            )
        work[column] = numeric
    return work.sort_values(
        list(DIVIDEND_FIELDS),
        kind="mergesort",
        na_position="last",
    ).reset_index(drop=True)


def canonical_implemented_actions(
    frame: pd.DataFrame,
    *,
    start_date: Any,
    end_date: Any,
    expected_tickers: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Collapse announcement duplicates without merging distinct payouts."""

    work = normalize_dividend_response(
        frame, expected_tickers=expected_tickers
    )
    start = pd.Timestamp(start_date).normalize()
    end = pd.Timestamp(end_date).normalize()
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    parsed = {
        column: _date_values(work[column], field=column)
        for column in DIVIDEND_DATE_FIELDS
    }
    selected = work.loc[
        work["div_proc"].eq(DIVIDEND_IMPLEMENTED)
        & parsed["ex_date"].between(start, end)
    ].copy()
    if selected.empty:
        return pd.DataFrame(columns=CANONICAL_ACTION_COLUMNS)
    dates = {
        column: parsed[column].loc[selected.index]
        for column in DIVIDEND_DATE_FIELDS
    }
    required = ("end_date", "record_date", "ex_date", "imp_ann_date")
    if any(dates[column].isna().any() for column in required):
        raise CorporateActionDataError(
            "implemented action lacks end/record/ex/implementation date"
        )
    if (dates["imp_ann_date"] > dates["ex_date"]).any():
        raise CorporateActionDataError(
            "implemented action was not available by its ex-date"
        )
    if (dates["record_date"] >= dates["ex_date"]).any():
        raise CorporateActionDataError(
            "implemented action record date must precede ex-date"
        )
    selected["stk_div"] = selected["stk_div"].fillna(0.0)
    selected["stk_bo_rate"] = selected["stk_bo_rate"].fillna(0.0)
    selected["stk_co_rate"] = selected["stk_co_rate"].fillna(0.0)
    one_cash_field_missing = selected["cash_div"].isna() ^ selected[
        "cash_div_tax"
    ].isna()
    if one_cash_field_missing.any():
        raise CorporateActionDataError(
            "implemented action has ambiguous after/before-tax cash fields"
        )
    selected["cash_div"] = selected["cash_div"].fillna(0.0)
    selected["cash_div_tax"] = selected["cash_div_tax"].fillna(0.0)
    if (selected["cash_div"] > selected["cash_div_tax"] + 1e-12).any():
        raise CorporateActionDataError(
            "after-tax cash dividend exceeds before-tax cash dividend"
        )
    if not np.isclose(
        selected["stk_div"],
        selected["stk_bo_rate"] + selected["stk_co_rate"],
        rtol=0.0,
        atol=1e-12,
    ).all():
        raise CorporateActionDataError(
            "stock dividend differs from bonus plus capitalization rates"
        )
    stock = selected["stk_div"].gt(0.0)
    cash = selected["cash_div_tax"].gt(0.0)
    if (~stock & ~cash).any():
        raise CorporateActionDataError("implemented action has no economic payout")
    if (cash & dates["pay_date"].isna()).any() or (
        cash & (dates["pay_date"] < dates["ex_date"])
    ).any():
        raise CorporateActionDataError(
            "cash dividend lacks a valid pay date"
        )
    if (stock & dates["div_listdate"].isna()).any() or (
        stock & (dates["div_listdate"] < dates["ex_date"])
    ).any():
        raise CorporateActionDataError(
            "stock dividend lacks a valid share-list date"
        )
    for column in DIVIDEND_DATE_FIELDS:
        selected[f"_{column}"] = dates[column]

    identity_columns = [
        "ts_code",
        "end_date",
        "record_date",
        "ex_date",
        "pay_date",
        "div_listdate",
    ]
    rows: list[dict[str, Any]] = []
    for _, group in selected.groupby(
        identity_columns, sort=True, dropna=False
    ):
        economic_columns = (
            "stk_div",
            "stk_bo_rate",
            "stk_co_rate",
            "cash_div",
            "cash_div_tax",
        )
        for column in economic_columns:
            if group[column].nunique(dropna=False) != 1:
                raise CorporateActionDataError(
                    "one action identity contains conflicting economics"
                )
        first = group.iloc[0]
        available = group["_imp_ann_date"].max()
        semantics = {
            "ticker": str(first["ts_code"]),
            "end_date": str(first["end_date"]),
            "record_date": group["_record_date"].iloc[0].date().isoformat(),
            "ex_date": group["_ex_date"].iloc[0].date().isoformat(),
            "pay_date": (
                None
                if pd.isna(group["_pay_date"].iloc[0])
                else group["_pay_date"].iloc[0].date().isoformat()
            ),
            "share_list_date": (
                None
                if pd.isna(group["_div_listdate"].iloc[0])
                else group["_div_listdate"].iloc[0].date().isoformat()
            ),
            "available_date": available.date().isoformat(),
            "stock_dividend_per_share": float(first["stk_div"]),
            "stock_bonus_per_share": float(first["stk_bo_rate"]),
            "stock_capitalization_per_share": float(first["stk_co_rate"]),
            "cash_dividend_after_tax_per_share": float(first["cash_div"]),
            "cash_dividend_before_tax_per_share": float(
                first["cash_div_tax"]
            ),
        }
        rows.append(
            {
                "action_id": _canonical_sha256(semantics),
                **semantics,
                "source_row_count": int(len(group)),
            }
        )
    result = pd.DataFrame(rows, columns=CANONICAL_ACTION_COLUMNS)
    if result["action_id"].duplicated().any():
        raise CorporateActionDataError("canonical action IDs are not unique")
    return result.sort_values(
        ["ex_date", "ticker", "action_id"], kind="mergesort"
    ).reset_index(drop=True)


def normalize_cninfo_dividend_response(
    frame: pd.DataFrame,
    *,
    expected_tickers: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Normalize the ASCII projection of ``akshare.stock_dividend_cninfo``."""

    if not isinstance(frame, pd.DataFrame):
        raise TypeError("CNInfo dividend response must be a pandas DataFrame")
    if tuple(map(str, frame.columns)) != CNINFO_RESPONSE_COLUMNS:
        raise CorporateActionDataError("CNInfo dividend response columns differ")
    work = frame.copy()
    for column in ("ticker", "dividend_type", "plan_text", "report_period"):
        work[column] = work[column].astype("string").str.strip()
    if work["ticker"].isna().any() or work["ticker"].eq("").any():
        raise CorporateActionDataError("CNInfo response contains an unknown ticker")
    if expected_tickers is not None:
        allowed = {str(value).strip() for value in expected_tickers}
        unexpected = sorted(set(work["ticker"].astype(str)) - allowed)
        if unexpected:
            raise CorporateActionDataError(
                f"CNInfo response contains unexpected tickers: {unexpected[:10]}"
            )
    date_columns = (
        "implementation_ann_date",
        "record_date",
        "ex_date",
        "pay_date",
        "share_arrival_date",
    )
    for column in date_columns:
        parsed = pd.to_datetime(work[column], errors="coerce").dt.normalize()
        source_missing = work[column].isna() | work[column].astype("string").str.strip().eq("")
        if (~source_missing & parsed.isna()).any():
            raise CorporateActionDataError(
                f"CNInfo response contains an invalid {column}"
            )
        work[column] = parsed
    for column in (
        "bonus_per_10",
        "capitalization_per_10",
        "cash_before_tax_per_10",
    ):
        numeric = pd.to_numeric(work[column], errors="coerce").astype(float)
        source_missing = work[column].isna() | work[column].astype("string").str.strip().eq("")
        if (~source_missing & ~np.isfinite(numeric)).any() or numeric.dropna().lt(0).any():
            raise CorporateActionDataError(
                f"CNInfo response contains an invalid {column}"
            )
        work[column] = numeric
    return work.sort_values(
        list(CNINFO_RESPONSE_COLUMNS),
        kind="mergesort",
        na_position="last",
    ).reset_index(drop=True)


def project_akshare_cninfo_response(
    frame: pd.DataFrame, *, ticker: str
) -> pd.DataFrame:
    """Project AkShare's localized 11-column frame to the frozen ASCII schema."""

    code = str(ticker).strip()
    if not code:
        raise CorporateActionDataError("CNInfo projection ticker is empty")
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("AkShare CNInfo response must be a pandas DataFrame")
    if frame.empty:
        return pd.DataFrame(columns=CNINFO_RESPONSE_COLUMNS)
    if frame.shape[1] != len(CNINFO_RESPONSE_COLUMNS) - 1:
        raise CorporateActionDataError("AkShare CNInfo response column count differs")
    result = frame.copy()
    result.columns = CNINFO_RESPONSE_COLUMNS[1:]
    result.insert(0, "ticker", code)
    return normalize_cninfo_dividend_response(
        result, expected_tickers={code}
    )


def canonical_cninfo_actions(
    frame: pd.DataFrame,
    *,
    start_date: Any,
    end_date: Any,
    expected_tickers: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Create deterministic per-share economic actions from CNInfo records."""

    work = normalize_cninfo_dividend_response(
        frame, expected_tickers=expected_tickers
    )
    start = pd.Timestamp(start_date).normalize()
    end = pd.Timestamp(end_date).normalize()
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    selected = work.loc[work["ex_date"].between(start, end)].copy()
    if selected.empty:
        return pd.DataFrame(columns=CNINFO_ACTION_COLUMNS)
    required = (
        "implementation_ann_date",
        "record_date",
        "ex_date",
        "dividend_type",
    )
    if any(selected[column].isna().any() for column in required):
        raise CorporateActionDataError(
            "CNInfo action lacks implementation/identity/effective dates"
        )
    selected["report_period"] = selected["report_period"].fillna(
        "UNKNOWN_REPORT"
    )
    if (selected["implementation_ann_date"] > selected["ex_date"]).any():
        raise CorporateActionDataError(
            "CNInfo action was not available by ex-date"
        )
    if (selected["record_date"] >= selected["ex_date"]).any():
        raise CorporateActionDataError(
            "CNInfo record date must precede ex-date"
        )
    for column in (
        "bonus_per_10",
        "capitalization_per_10",
        "cash_before_tax_per_10",
    ):
        selected[column] = selected[column].fillna(0.0)
    stock_per_10 = (
        selected["bonus_per_10"] + selected["capitalization_per_10"]
    )
    cash_per_10 = selected["cash_before_tax_per_10"]
    if (stock_per_10.eq(0.0) & cash_per_10.eq(0.0)).any():
        raise CorporateActionDataError("CNInfo action has no economic payout")
    if (
        cash_per_10.gt(0.0)
        & (
            selected["pay_date"].isna()
            | (selected["pay_date"] < selected["ex_date"])
        )
    ).any():
        raise CorporateActionDataError("CNInfo cash action lacks a valid pay date")
    if (
        stock_per_10.gt(0.0)
        & selected["share_arrival_date"].notna()
        & (selected["share_arrival_date"] < selected["ex_date"])
    ).any():
        raise CorporateActionDataError(
            "CNInfo stock action has a pre-ex share-arrival date"
        )
    identity = [
        "ticker",
        "report_period",
        "dividend_type",
        "record_date",
        "ex_date",
        "pay_date",
        "share_arrival_date",
    ]
    rows: list[dict[str, Any]] = []
    for _, group in selected.groupby(identity, sort=True, dropna=False):
        economics = (
            "bonus_per_10",
            "capitalization_per_10",
            "cash_before_tax_per_10",
        )
        if any(group[column].nunique(dropna=False) != 1 for column in economics):
            raise CorporateActionDataError(
                "one CNInfo action identity contains conflicting economics"
            )
        first = group.iloc[0]
        semantics = {
            "ticker": str(first["ticker"]),
            "report_period": str(first["report_period"]),
            "dividend_type": str(first["dividend_type"]),
            "available_date": group["implementation_ann_date"].max().date().isoformat(),
            "record_date": first["record_date"].date().isoformat(),
            "ex_date": first["ex_date"].date().isoformat(),
            "pay_date": (
                None
                if pd.isna(first["pay_date"])
                else first["pay_date"].date().isoformat()
            ),
            "share_arrival_date": (
                None
                if pd.isna(first["share_arrival_date"])
                else first["share_arrival_date"].date().isoformat()
            ),
            "bonus_per_share": float(first["bonus_per_10"]) / 10.0,
            "capitalization_per_share": float(
                first["capitalization_per_10"]
            )
            / 10.0,
            "stock_dividend_per_share": float(
                first["bonus_per_10"] + first["capitalization_per_10"]
            )
            / 10.0,
            "cash_dividend_before_tax_per_share": float(
                first["cash_before_tax_per_10"]
            )
            / 10.0,
        }
        rows.append(
            {
                "action_id": _canonical_sha256(semantics),
                **semantics,
                "source_row_count": int(len(group)),
            }
        )
    result = pd.DataFrame(rows, columns=CNINFO_ACTION_COLUMNS)
    if result["action_id"].duplicated().any():
        raise CorporateActionDataError("CNInfo canonical action IDs are not unique")
    return result.sort_values(
        ["ex_date", "ticker", "report_period", "dividend_type", "action_id"],
        kind="mergesort",
    ).reset_index(drop=True)


def resolve_cninfo_share_arrivals(
    cninfo_actions: pd.DataFrame,
    tushare_actions: pd.DataFrame,
) -> pd.DataFrame:
    """Fill only missing CNInfo stock-arrival dates from Tushare timing."""

    if tuple(map(str, cninfo_actions.columns)) != CNINFO_ACTION_COLUMNS:
        raise CorporateActionDataError("CNInfo canonical action columns differ")
    if tuple(map(str, tushare_actions.columns)) != CANONICAL_ACTION_COLUMNS:
        raise CorporateActionDataError("Tushare canonical action columns differ")
    result = cninfo_actions.copy()
    result["share_arrival_source"] = np.where(
        result["share_arrival_date"].notna(), "cninfo", "not_applicable"
    )
    stock_rows = result["stock_dividend_per_share"].gt(0.0)
    stock_groups = result.loc[stock_rows].groupby(
        ["ticker", "record_date", "ex_date"], sort=True, dropna=False
    )
    for (ticker, record_date, ex_date), group in stock_groups:
        matches = tushare_actions.loc[
            tushare_actions["ticker"].eq(ticker)
            & tushare_actions["record_date"].eq(record_date)
            & tushare_actions["ex_date"].eq(ex_date)
        ]
        if len(matches) != 1:
            raise CorporateActionDataError(
                "CNInfo stock group lacks one unambiguous Tushare action"
            )
        secondary = matches.iloc[0]
        primary_stock = float(group["stock_dividend_per_share"].sum())
        if not np.isclose(
            float(secondary["stock_dividend_per_share"]),
            primary_stock,
            rtol=0.0,
            atol=1e-12,
        ):
            raise CorporateActionDataError(
                "CNInfo and Tushare stock-group economics disagree"
            )
        primary_pay_dates = sorted(
            {str(value) for value in group["pay_date"].dropna().tolist()}
        )
        secondary_pay_dates = (
            [] if pd.isna(secondary["pay_date"]) else [str(secondary["pay_date"])]
        )
        if primary_pay_dates != secondary_pay_dates:
            raise CorporateActionDataError(
                "CNInfo and Tushare stock-group pay dates disagree"
            )
        secondary_arrival = secondary["share_list_date"]
        if pd.isna(secondary_arrival):
            raise CorporateActionDataError(
                "CNInfo stock group lacks one Tushare arrival date"
            )
        current_arrivals = sorted(
            {
                str(value)
                for value in group["share_arrival_date"].dropna().tolist()
            }
        )
        if current_arrivals and current_arrivals != [str(secondary_arrival)]:
            raise CorporateActionDataError(
                "CNInfo and Tushare share-arrival dates disagree"
            )
        for index in group.index[group["share_arrival_date"].isna()]:
            result.at[index, "share_arrival_date"] = str(secondary_arrival)
            result.at[index, "share_arrival_source"] = "tushare"
    unresolved = stock_rows & result["share_arrival_date"].isna()
    if unresolved.any():
        raise CorporateActionDataError("stock action share-arrival remains unknown")
    return result.loc[:, RESOLVED_CNINFO_ACTION_COLUMNS].sort_values(
        ["ex_date", "ticker", "report_period", "dividend_type", "action_id"],
        kind="mergesort",
    ).reset_index(drop=True)


def resolve_corporate_actions(
    cninfo_actions: pd.DataFrame,
    tushare_actions: pd.DataFrame,
    tushare_reference_diagnostics: pd.DataFrame,
) -> pd.DataFrame:
    """Resolve primary CNInfo actions plus strictly price-validated fallbacks.

    Tushare economics are admitted only when CNInfo has no action for the same
    ticker/ex-date and a separate raw-price diagnostic marks the exact canonical
    Tushare action as eligible.  The diagnostic is deliberately an input so this
    pure normalizer cannot silently inspect or adapt to a return path.
    """

    primary = resolve_cninfo_share_arrivals(cninfo_actions, tushare_actions)
    if tuple(map(str, tushare_reference_diagnostics.columns)) != (
        TUSHARE_REFERENCE_DIAGNOSTIC_COLUMNS
    ):
        raise CorporateActionDataError(
            "Tushare reference diagnostic columns differ"
        )
    diagnostics = tushare_reference_diagnostics.copy()
    diagnostics["action_id"] = diagnostics["action_id"].astype(str)
    if diagnostics["action_id"].duplicated().any():
        raise CorporateActionDataError(
            "Tushare reference diagnostics contain duplicate action IDs"
        )
    if set(diagnostics["action_id"]) != set(
        tushare_actions["action_id"].astype(str)
    ):
        raise CorporateActionDataError(
            "Tushare reference diagnostics do not exhaust canonical actions"
        )
    eligible = set(
        diagnostics.loc[
            diagnostics["fallback_eligible"].eq(True), "action_id"
        ].astype(str)
    )
    fallback = tushare_actions.loc[
        tushare_actions["action_id"].astype(str).isin(eligible)
    ].copy()
    primary_pairs = set(
        primary[["ticker", "ex_date"]].astype(str).itertuples(
            index=False, name=None
        )
    )
    conflicting = fallback.loc[
        fallback[["ticker", "ex_date"]]
        .astype(str)
        .apply(tuple, axis=1)
        .isin(primary_pairs)
    ]
    if not conflicting.empty:
        raise CorporateActionDataError(
            "Tushare fallback overlaps a CNInfo ticker/ex-date"
        )
    rows = primary.copy()
    rows["economic_source"] = "cninfo"
    if not fallback.empty:
        mapped = pd.DataFrame(
            {
                "action_id": fallback["action_id"].astype(str),
                "ticker": fallback["ticker"].astype(str),
                "report_period": fallback["end_date"].astype(str),
                "dividend_type": "TUSHARE_RAW_REFERENCE_FALLBACK",
                "available_date": fallback["available_date"],
                "record_date": fallback["record_date"],
                "ex_date": fallback["ex_date"],
                "pay_date": fallback["pay_date"],
                "share_arrival_date": fallback["share_list_date"],
                "bonus_per_share": fallback["stock_bonus_per_share"],
                "capitalization_per_share": fallback[
                    "stock_capitalization_per_share"
                ],
                "stock_dividend_per_share": fallback[
                    "stock_dividend_per_share"
                ],
                "cash_dividend_before_tax_per_share": fallback[
                    "cash_dividend_before_tax_per_share"
                ],
                "source_row_count": fallback["source_row_count"].astype(int),
                "share_arrival_source": np.where(
                    fallback["stock_dividend_per_share"].gt(0.0),
                    "tushare",
                    "not_applicable",
                ),
                "economic_source": "tushare_raw_reference_fallback",
            }
        )
        rows = (
            mapped.copy()
            if rows.empty
            else pd.concat([rows, mapped], ignore_index=True)
        )
    rows = rows.loc[:, RESOLVED_ACTION_COLUMNS]
    if rows["action_id"].duplicated().any():
        raise CorporateActionDataError("resolved action IDs are duplicate")
    return rows.sort_values(
        ["ex_date", "ticker", "report_period", "dividend_type", "action_id"],
        kind="mergesort",
    ).reset_index(drop=True)


__all__ = [
    "CANONICAL_ACTION_COLUMNS",
    "CNINFO_ACTION_COLUMNS",
    "CNINFO_RESPONSE_COLUMNS",
    "RESOLVED_CNINFO_ACTION_COLUMNS",
    "RESOLVED_ACTION_COLUMNS",
    "TUSHARE_REFERENCE_DIAGNOSTIC_COLUMNS",
    "CorporateActionDataError",
    "DIVIDEND_FIELDS",
    "DIVIDEND_IMPLEMENTED",
    "canonical_implemented_actions",
    "canonical_cninfo_actions",
    "normalize_cninfo_dividend_response",
    "project_akshare_cninfo_response",
    "resolve_cninfo_share_arrivals",
    "resolve_corporate_actions",
    "normalize_dividend_response",
]
