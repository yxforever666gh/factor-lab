from __future__ import annotations

from typing import Any

import pandas as pd

from factor_lab.pit_asof import normalize_statement_dates, select_latest_statement_asof


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _blank_series(index: pd.Index, dtype: str = "Float64") -> pd.Series:
    return pd.Series([pd.NA] * len(index), index=index, dtype=dtype)


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> tuple[pd.Series, pd.Series]:
    den = _numeric(denominator)
    num = _numeric(numerator)
    zero = den == 0
    result = num / den.where(~zero)
    return result, zero.fillna(False)


def _first_available(df: pd.DataFrame, names: list[str]) -> pd.Series:
    for name in names:
        if name in df.columns:
            return _numeric(df[name])
    return _blank_series(df.index)


def _table_source_name(table: str) -> str:
    if table.startswith("diemeng."):
        return table
    if table == "financial_indicator":
        return "diemeng.financial_indicator"
    if table == "fina_indicator":
        return "tushare.fina_indicator"
    return f"tushare.{table}"


def _prefix(table: str) -> str:
    return f"{table}__"


def build_pit_financial_features(
    statements: dict[str, pd.DataFrame],
    trade_dates: pd.DataFrame,
    *,
    code_col: str = "ts_code",
    trade_date_col: str = "date",
) -> pd.DataFrame:
    """Build conservative PIT financial features from as-of joined statements."""
    base = trade_dates.copy()
    source_fields = {
        "cashflow": ["net_profit", "n_cashflow_act", "free_cashflow"],
        "diemeng.cashflow": ["net_profit", "n_cashflow_act", "free_cashflow"],
        "income": ["n_income_attr_p", "total_profit", "n_income", "income"],
        "diemeng.income": ["n_income_attr_p", "total_profit", "n_income", "income"],
        "balancesheet": ["total_assets", "total_liab", "total_cur_assets", "total_cur_liab"],
        "diemeng.balancesheet": ["total_assets", "total_liab", "total_cur_assets", "total_cur_liab"],
        "fina_indicator": ["debt_to_assets", "current_ratio", "quick_ratio", "roe", "roe_dt", "grossprofit_margin", "netprofit_margin", "q_netprofit_yoy", "netprofit_yoy", "dt_netprofit_yoy", "q_sales_yoy", "tr_yoy", "or_yoy"],
        "financial_indicator": ["debt_to_assets", "current_ratio", "quick_ratio", "roe", "roe_dt", "grossprofit_margin", "netprofit_margin", "q_netprofit_yoy", "netprofit_yoy", "dt_netprofit_yoy", "q_sales_yoy", "tr_yoy", "or_yoy"],
        "diemeng.financial_indicator": ["debt_to_assets", "current_ratio", "quick_ratio", "roe", "roe_dt", "grossprofit_margin", "netprofit_margin", "q_netprofit_yoy", "netprofit_yoy", "dt_netprofit_yoy", "q_sales_yoy", "tr_yoy", "or_yoy"],
    }

    out = base.copy()
    for table, df in statements.items():
        if df is None or df.empty:
            continue
        normalized = normalize_statement_dates(df)
        joined = select_latest_statement_asof(
            normalized,
            base[[code_col, trade_date_col]],
            code_col=code_col,
            trade_date_col=trade_date_col,
            source_table=_table_source_name(table),
            source_fields=source_fields.get(table, []),
        )
        joined = joined.add_prefix(_prefix(table))
        for col in joined.columns:
            out[col] = joined[col]

    warnings: list[list[str]] = [[] for _ in range(len(out))]
    blocked: list[list[str]] = [[] for _ in range(len(out))]

    def col(table: str, name: str) -> pd.Series:
        key = f"{table}__{name}"
        if key in out.columns:
            return out[key]
        return _blank_series(out.index)

    def valid(table: str) -> pd.Series:
        key = f"{table}__pit_validated"
        if key in out.columns:
            return out[key].fillna(False).astype(bool)
        return pd.Series([False] * len(out), index=out.index)

    def choose(name: str, candidates: list[tuple[str, str, str]]) -> pd.Series:
        chosen = _blank_series(out.index)
        source = pd.Series([pd.NA] * len(out), index=out.index, dtype="object")
        first_valid_value: pd.Series | None = None
        first_source: str | None = None
        for table, field, source_name in candidates:
            values = col(table, field)
            usable = valid(table) & values.notna()
            if first_valid_value is None:
                first_valid_value = values.where(usable)
                first_source = source_name
            else:
                disagree = first_valid_value.notna() & usable & ((first_valid_value - values).abs() > 1e-9)
                for i in list(out.index[disagree]):
                    warnings[int(i)].append(f"{name}:cross_source_disagreement:{first_source}!={source_name}")
            fill = chosen.isna() & usable
            chosen = chosen.where(~fill, values)
            source = source.where(~fill, source_name)
        out[f"{name}_source"] = source
        return chosen

    cashflow_numerator = col("cashflow", "n_cashflow_act")
    cashflow_denominator = choose(
        "cashflow_denominator",
        [
            ("cashflow", "net_profit", "tushare.cashflow.net_profit"),
            ("income", "n_income_attr_p", "tushare.income.n_income_attr_p"),
            ("income", "total_profit", "tushare.income.total_profit"),
            ("diemeng.income", "n_income_attr_p", "diemeng.income.n_income_attr_p"),
            ("diemeng.income", "total_profit", "diemeng.income.total_profit"),
            ("diemeng.cashflow", "net_profit", "diemeng.cashflow.net_profit"),
        ],
    )
    ocfp, ocfp_zero = _safe_div(cashflow_numerator, cashflow_denominator)
    cashflow_denominator_valid = cashflow_denominator.notna()
    ocfp = ocfp.where(valid("cashflow") & cashflow_denominator_valid)
    out["operating_cashflow_to_profit"] = ocfp
    out["pit_cashflow_numerator_raw"] = cashflow_numerator.where(valid("cashflow"))
    out["pit_cashflow_denominator_raw"] = cashflow_denominator.where(valid("cashflow") & cashflow_denominator_valid)
    out["pit_cashflow_denominator_source"] = out.get("cashflow_denominator_source")
    out["pit_cashflow_formula_block_reason"] = ""
    fcta, fcta_zero = _safe_div(col("cashflow", "free_cashflow"), col("balancesheet", "total_assets"))
    fcta = fcta.where(valid("cashflow") & valid("balancesheet"))
    out["free_cashflow_to_assets"] = fcta

    debt_direct = choose("debt_to_assets", [("fina_indicator", "debt_to_assets", "tushare.fina_indicator"), ("diemeng.financial_indicator", "debt_to_assets", "diemeng.financial_indicator"), ("financial_indicator", "debt_to_assets", "diemeng.financial_indicator")])
    debt_calc, debt_zero = _safe_div(col("balancesheet", "total_liab"), col("balancesheet", "total_assets"))
    debt_calc = debt_calc.where(valid("balancesheet"))
    out["debt_to_assets"] = debt_direct.where(debt_direct.notna(), debt_calc)
    out["current_ratio"] = choose("current_ratio", [("fina_indicator", "current_ratio", "tushare.fina_indicator"), ("diemeng.financial_indicator", "current_ratio", "diemeng.financial_indicator"), ("financial_indicator", "current_ratio", "diemeng.financial_indicator")])
    current_calc, current_zero = _safe_div(col("balancesheet", "total_cur_assets"), col("balancesheet", "total_cur_liab"))
    out["current_ratio"] = out["current_ratio"].where(out["current_ratio"].notna(), current_calc.where(valid("balancesheet")))
    out["quick_ratio"] = choose("quick_ratio", [("fina_indicator", "quick_ratio", "tushare.fina_indicator"), ("diemeng.financial_indicator", "quick_ratio", "diemeng.financial_indicator"), ("financial_indicator", "quick_ratio", "diemeng.financial_indicator")])
    out["roe"] = choose("roe", [("fina_indicator", "roe", "tushare.fina_indicator"), ("fina_indicator", "roe_dt", "tushare.fina_indicator"), ("diemeng.financial_indicator", "roe", "diemeng.financial_indicator"), ("financial_indicator", "roe", "diemeng.financial_indicator")])
    out["grossprofit_margin"] = choose("grossprofit_margin", [("fina_indicator", "grossprofit_margin", "tushare.fina_indicator"), ("diemeng.financial_indicator", "grossprofit_margin", "diemeng.financial_indicator"), ("financial_indicator", "grossprofit_margin", "diemeng.financial_indicator")])
    out["netprofit_margin"] = choose("netprofit_margin", [("fina_indicator", "netprofit_margin", "tushare.fina_indicator"), ("diemeng.financial_indicator", "netprofit_margin", "diemeng.financial_indicator"), ("financial_indicator", "netprofit_margin", "diemeng.financial_indicator")])
    out["netprofit_yoy"] = choose("netprofit_yoy", [("fina_indicator", "q_netprofit_yoy", "tushare.fina_indicator"), ("fina_indicator", "netprofit_yoy", "tushare.fina_indicator"), ("diemeng.financial_indicator", "q_netprofit_yoy", "diemeng.financial_indicator"), ("diemeng.financial_indicator", "netprofit_yoy", "diemeng.financial_indicator"), ("financial_indicator", "netprofit_yoy", "diemeng.financial_indicator")])
    out["tr_yoy"] = choose("tr_yoy", [("fina_indicator", "q_sales_yoy", "tushare.fina_indicator"), ("fina_indicator", "tr_yoy", "tushare.fina_indicator"), ("diemeng.financial_indicator", "q_sales_yoy", "diemeng.financial_indicator"), ("diemeng.financial_indicator", "tr_yoy", "diemeng.financial_indicator"), ("financial_indicator", "tr_yoy", "diemeng.financial_indicator")])
    out["profit_growth_ok"] = out["netprofit_yoy"] >= 0
    out["revenue_growth_ok"] = out["tr_yoy"] >= 0

    for i in list(out.index[ocfp_zero & valid("cashflow")]):
        blocked[int(i)].append("operating_cashflow_to_profit:zero_denominator")
        out.at[i, "pit_cashflow_formula_block_reason"] = "zero_denominator"
    numerator_missing = valid("cashflow") & cashflow_numerator.isna()
    for i in list(out.index[numerator_missing & out["pit_cashflow_formula_block_reason"].eq("")]):
        out.at[i, "pit_cashflow_formula_block_reason"] = "missing_numerator"
    denominator_missing = valid("cashflow") & cashflow_denominator.isna()
    for i in list(out.index[denominator_missing & out["pit_cashflow_formula_block_reason"].eq("")]):
        out.at[i, "pit_cashflow_formula_block_reason"] = "missing_denominator"
    for i in list(out.index[fcta_zero & valid("balancesheet")]):
        blocked[int(i)].append("free_cashflow_to_assets:zero_denominator")
    for i in list(out.index[debt_zero & valid("balancesheet")]):
        blocked[int(i)].append("debt_to_assets:zero_denominator")
    for i in list(out.index[current_zero & valid("balancesheet")]):
        blocked[int(i)].append("current_ratio:zero_denominator")

    pit_cols = [c for c in out.columns if c.endswith("__pit_validated")]
    out["pit_feature_validated"] = out[pit_cols].any(axis=1) if pit_cols else False
    for i in range(len(out)):
        if not bool(out.iloc[i]["pit_feature_validated"]):
            blocked[i].append("no_pit_statement_asof")
    out["pit_feature_blocked_reason"] = [";".join(items) for items in blocked]
    out["pit_feature_warnings"] = [";".join(items) for items in warnings]
    return out


def summarize_feature_coverage(features: pd.DataFrame, feature_names: list[str]) -> dict[str, Any]:
    rows = int(len(features))
    summary: dict[str, Any] = {"rows": rows, "features": {}}
    for name in feature_names:
        if name not in features.columns:
            summary["features"][name] = {"available": False, "coverage": 0.0}
            continue
        coverage = float(features[name].notna().mean()) if rows else 0.0
        summary["features"][name] = {"available": True, "coverage": round(coverage, 4)}
    return summary
