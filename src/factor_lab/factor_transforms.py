from __future__ import annotations

import pandas as pd


def _std(s: pd.Series) -> float:
    value = float(s.std(ddof=0))
    return value if value else 0.0


def zscore_by_date(frame: pd.DataFrame, field: str) -> pd.DataFrame:
    out = frame.copy()
    col = f"{field}_zscore_by_date"
    grouped = out.groupby("date", group_keys=False)[field]
    mean = grouped.transform("mean")
    std = grouped.transform(lambda s: _std(s))
    out[col] = (out[field] - mean) / std.replace(0, pd.NA)
    return out


def rank_by_date(frame: pd.DataFrame, field: str) -> pd.DataFrame:
    out = frame.copy()
    out[f"{field}_rank_by_date"] = out.groupby("date")[field].rank(pct=True)
    return out


def zscore_by_date_industry(frame: pd.DataFrame, field: str, *, min_group_size: int = 3) -> pd.DataFrame:
    out = frame.copy()
    col = f"{field}_zscore_by_date_industry"
    group_cols = ["date", "industry"]
    sizes = out.groupby(group_cols)[field].transform("count")
    mean = out.groupby(group_cols)[field].transform("mean")
    std = out.groupby(group_cols)[field].transform(lambda s: _std(s))
    out[col] = (out[field] - mean) / std.replace(0, pd.NA)
    out.loc[sizes < min_group_size, col] = pd.NA
    return out


def rank_by_date_industry(frame: pd.DataFrame, field: str, *, min_group_size: int = 3) -> pd.DataFrame:
    out = frame.copy()
    col = f"{field}_rank_by_date_industry"
    group_cols = ["date", "industry"]
    sizes = out.groupby(group_cols)[field].transform("count")
    out[col] = out.groupby(group_cols)[field].rank(pct=True)
    out.loc[sizes < min_group_size, col] = pd.NA
    return out


def add_initial_value_transforms(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if {"date", "industry", "book_yield"}.issubset(out.columns):
        out = zscore_by_date_industry(out, "book_yield", min_group_size=2)
        out["industry_relative_book_yield"] = out["book_yield_zscore_by_date_industry"]
    if {"date", "industry", "earnings_yield"}.issubset(out.columns):
        out = zscore_by_date_industry(out, "earnings_yield", min_group_size=2)
        out["industry_relative_earnings_yield"] = out["earnings_yield_zscore_by_date_industry"]
    if {"date", "industry", "pb"}.issubset(out.columns):
        out = zscore_by_date_industry(out, "pb", min_group_size=2)
        out["industry_relative_pb"] = -out["pb_zscore_by_date_industry"]
    if {"date", "industry", "pe_ttm"}.issubset(out.columns):
        out = zscore_by_date_industry(out, "pe_ttm", min_group_size=2)
        out["industry_relative_pe"] = -out["pe_ttm_zscore_by_date_industry"]
    return out


def add_pit_value_trap_transforms(frame: pd.DataFrame) -> pd.DataFrame:
    """Add standardized PIT financial fields used only by repaired value-trap configs."""
    out = frame.copy()
    transform_specs = {
        "operating_cashflow_to_profit": "operating_cashflow_to_profit_zscore_by_date_industry",
        "debt_to_assets": "debt_to_assets_zscore_by_date_industry",
        "debt_to_asset": "debt_to_asset_zscore_by_date_industry",
        "netprofit_yoy": "netprofit_yoy_zscore_by_date_industry",
        "profit_yoy": "profit_yoy_zscore_by_date_industry",
        "tr_yoy": "tr_yoy_zscore_by_date_industry",
        "revenue_yoy": "revenue_yoy_zscore_by_date_industry",
    }
    for field, col in transform_specs.items():
        if {"date", "industry", field}.issubset(out.columns):
            out = zscore_by_date_industry(out, field, min_group_size=2)
            if f"{field}_zscore_by_date_industry" != col and f"{field}_zscore_by_date_industry" in out.columns:
                out[col] = out[f"{field}_zscore_by_date_industry"]
    if "debt_to_assets_zscore_by_date_industry" in out.columns:
        out["low_debt_to_assets_zscore_by_date_industry"] = -out["debt_to_assets_zscore_by_date_industry"]
    elif "debt_to_asset_zscore_by_date_industry" in out.columns:
        out["low_debt_to_assets_zscore_by_date_industry"] = -out["debt_to_asset_zscore_by_date_industry"]
    if "netprofit_yoy_zscore_by_date_industry" in out.columns:
        out["reversed_netprofit_yoy_zscore_by_date_industry"] = -out["netprofit_yoy_zscore_by_date_industry"]
    if "tr_yoy_zscore_by_date_industry" in out.columns:
        out["reversed_tr_yoy_zscore_by_date_industry"] = -out["tr_yoy_zscore_by_date_industry"]
    if "operating_cashflow_to_profit_zscore_by_date_industry" in out.columns:
        out["reversed_operating_cashflow_to_profit_zscore_by_date_industry"] = -out["operating_cashflow_to_profit_zscore_by_date_industry"]
    return out
