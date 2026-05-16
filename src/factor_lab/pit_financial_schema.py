from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable

CANONICAL_TABLE_NAMES = ("income", "balancesheet", "cashflow", "financial_indicator")
CANONICAL_DATE_COLUMNS = ("ann_date", "f_ann_date", "end_date")
P0_FEATURE_NAMES = (
    "operating_cashflow_to_profit",
    "free_cashflow_to_assets",
    "debt_to_assets",
    "current_ratio",
    "quick_ratio",
    "netprofit_yoy",
    "tr_yoy",
    "roe",
    "grossprofit_margin",
    "netprofit_margin",
)

TABLE_ALIASES = {
    ("tushare", "income"): "income",
    ("diemeng", "income"): "income",
    ("tushare", "balancesheet"): "balancesheet",
    ("diemeng", "balancesheet"): "balancesheet",
    ("tushare", "cashflow"): "cashflow",
    ("diemeng", "cashflow"): "cashflow",
    ("tushare", "fina_indicator"): "financial_indicator",
    ("tushare", "financial_indicator"): "financial_indicator",
    ("diemeng", "financial_indicator"): "financial_indicator",
}

SOURCE_FIELD_ALIASES = {
    ("cashflow", "n_cashflow_act"): "n_cashflow_act",
    ("cashflow", "net_profit"): "net_profit",
    ("cashflow", "free_cashflow"): "free_cashflow",
    ("balancesheet", "total_assets"): "total_assets",
    ("balancesheet", "total_liab"): "total_liab",
    ("balancesheet", "money_cap"): "money_cap",
    ("balancesheet", "total_cur_assets"): "total_cur_assets",
    ("balancesheet", "total_cur_liab"): "total_cur_liab",
    ("financial_indicator", "debt_to_assets"): "debt_to_assets",
    ("financial_indicator", "current_ratio"): "current_ratio",
    ("financial_indicator", "quick_ratio"): "quick_ratio",
    ("financial_indicator", "netprofit_yoy"): "netprofit_yoy",
    ("financial_indicator", "q_netprofit_yoy"): "netprofit_yoy",
    ("financial_indicator", "dt_netprofit_yoy"): "netprofit_yoy",
    ("financial_indicator", "tr_yoy"): "tr_yoy",
    ("financial_indicator", "or_yoy"): "tr_yoy",
    ("financial_indicator", "q_sales_yoy"): "tr_yoy",
    ("financial_indicator", "roe"): "roe",
    ("financial_indicator", "roe_dt"): "roe",
    ("financial_indicator", "grossprofit_margin"): "grossprofit_margin",
    ("financial_indicator", "netprofit_margin"): "netprofit_margin",
    ("daily_basic", "pb"): "pb",
    ("stock_basic", "industry"): "industry",
}


@dataclass(frozen=True)
class PITFinancialField:
    name: str
    group: str
    source_table: str
    description: str
    preferred_sources: tuple[str, ...]
    source_fields: tuple[str, ...]
    requires_pit: bool = True
    disclosure_date_policy: str = "require_ann_or_f_ann_date_asof"
    required_for_p0: bool = False

    def __post_init__(self) -> None:
        if not self.disclosure_date_policy:
            raise ValueError(f"PIT field {self.name} missing disclosure-date policy")
        if self.requires_pit and self.disclosure_date_policy == "none":
            raise ValueError(f"PIT field {self.name} cannot disable disclosure-date policy")

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["preferred_sources"] = list(self.preferred_sources)
        data["source_fields"] = list(self.source_fields)
        return data


PIT_FINANCIAL_FIELDS: tuple[PITFinancialField, ...] = (
    PITFinancialField(
        name="operating_cashflow_to_profit",
        group="cashflow_quality",
        source_table="cashflow",
        description="Operating cashflow divided by net profit; filters accounting profit without cash conversion.",
        preferred_sources=("tushare.cashflow", "diemeng.cashflow"),
        source_fields=("n_cashflow_act", "net_profit"),
        required_for_p0=True,
    ),
    PITFinancialField(
        name="free_cashflow_to_assets",
        group="cashflow_quality",
        source_table="cashflow+balancesheet",
        description="Free cashflow divided by total assets; conservative cash-generation quality proxy.",
        preferred_sources=("tushare.cashflow", "diemeng.cashflow", "tushare.balancesheet", "diemeng.balancesheet"),
        source_fields=("free_cashflow", "total_assets"),
        required_for_p0=True,
    ),
    PITFinancialField(
        name="debt_to_assets",
        group="leverage_distress",
        source_table="balancesheet",
        description="Total liabilities divided by total assets; filters financially stressed cheap stocks.",
        preferred_sources=("tushare.balancesheet", "diemeng.balancesheet", "diemeng.financial_indicator"),
        source_fields=("total_liab", "total_assets", "debt_to_assets"),
        required_for_p0=True,
    ),
    PITFinancialField(
        name="current_ratio",
        group="leverage_distress",
        source_table="financial_indicator",
        description="Current assets divided by current liabilities or direct current-ratio field.",
        preferred_sources=("diemeng.financial_indicator", "tushare.fina_indicator", "tushare.balancesheet", "diemeng.balancesheet"),
        source_fields=("current_ratio", "total_cur_assets", "total_cur_liab"),
        required_for_p0=True,
    ),
    PITFinancialField(
        name="quick_ratio",
        group="leverage_distress",
        source_table="financial_indicator",
        description="Quick ratio liquidity pressure proxy.",
        preferred_sources=("diemeng.financial_indicator", "tushare.fina_indicator"),
        source_fields=("quick_ratio",),
        required_for_p0=True,
    ),
    PITFinancialField(
        name="netprofit_yoy",
        group="growth_repair",
        source_table="financial_indicator",
        description="Net profit year-over-year growth.",
        preferred_sources=("tushare.fina_indicator", "diemeng.financial_indicator"),
        source_fields=("q_netprofit_yoy", "netprofit_yoy", "dt_netprofit_yoy"),
        required_for_p0=True,
    ),
    PITFinancialField(
        name="tr_yoy",
        group="growth_repair",
        source_table="financial_indicator",
        description="Revenue year-over-year growth.",
        preferred_sources=("tushare.fina_indicator", "diemeng.financial_indicator"),
        source_fields=("q_sales_yoy", "tr_yoy", "or_yoy"),
        required_for_p0=True,
    ),
    PITFinancialField(
        name="roe",
        group="profitability_quality",
        source_table="financial_indicator",
        description="Return on equity quality proxy.",
        preferred_sources=("tushare.fina_indicator", "diemeng.financial_indicator"),
        source_fields=("roe", "roe_dt"),
        required_for_p0=True,
    ),
    PITFinancialField(
        name="grossprofit_margin",
        group="profitability_quality",
        source_table="financial_indicator",
        description="Gross profit margin quality proxy.",
        preferred_sources=("tushare.fina_indicator", "diemeng.financial_indicator"),
        source_fields=("grossprofit_margin",),
        required_for_p0=True,
    ),
    PITFinancialField(
        name="netprofit_margin",
        group="profitability_quality",
        source_table="financial_indicator",
        description="Net profit margin quality proxy.",
        preferred_sources=("tushare.fina_indicator", "diemeng.financial_indicator"),
        source_fields=("netprofit_margin",),
        required_for_p0=True,
    ),
    PITFinancialField(
        name="profit_growth_ok",
        group="growth_repair",
        source_table="financial_indicator",
        description="Derived flag: profit growth is not materially deteriorating.",
        preferred_sources=("tushare.fina_indicator", "diemeng.financial_indicator"),
        source_fields=("q_netprofit_yoy", "netprofit_yoy", "dt_netprofit_yoy"),
        required_for_p0=True,
    ),
    PITFinancialField(
        name="revenue_growth_ok",
        group="growth_repair",
        source_table="financial_indicator",
        description="Derived flag: revenue growth is not materially deteriorating.",
        preferred_sources=("tushare.fina_indicator", "diemeng.financial_indicator"),
        source_fields=("q_sales_yoy", "tr_yoy", "or_yoy"),
        required_for_p0=True,
    ),
    PITFinancialField(
        name="pb",
        group="valuation_base",
        source_table="daily_basic",
        description="Price-to-book from daily-basic valuation data.",
        preferred_sources=("tushare.daily_basic",),
        source_fields=("pb",),
        requires_pit=False,
        disclosure_date_policy="daily_market_date",
        required_for_p0=True,
    ),
    PITFinancialField(
        name="industry",
        group="valuation_base",
        source_table="stock_basic",
        description="Industry classification for industry-relative value comparisons.",
        preferred_sources=("tushare.stock_basic", "diemeng.stock.list"),
        source_fields=("industry",),
        requires_pit=False,
        disclosure_date_policy="static_or_reference_data_asof",
        required_for_p0=True,
    ),
)


def all_fields() -> list[dict[str, object]]:
    return [field.to_dict() for field in PIT_FINANCIAL_FIELDS]


def field_names(*, p0_only: bool = False) -> set[str]:
    return {f.name for f in PIT_FINANCIAL_FIELDS if not p0_only or f.required_for_p0}


def fields_by_group() -> dict[str, list[dict[str, object]]]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for field in PIT_FINANCIAL_FIELDS:
        grouped.setdefault(field.group, []).append(field.to_dict())
    return grouped


def validate_required_fields(required_fields: Iterable[str]) -> dict[str, object]:
    known = field_names()
    required = list(required_fields)
    missing = [field for field in required if field not in known]
    return {"known": not missing, "missing_fields": missing, "required_fields": required}


def resolve_source_field(source: str, table: str, field: str) -> dict[str, object]:
    canonical_table = TABLE_ALIASES.get((source, table))
    if canonical_table is None:
        return {"known": False, "blocked_reason": "unknown_source_table", "source": source, "table": table, "field": field}
    canonical_field = SOURCE_FIELD_ALIASES.get((canonical_table, field))
    if canonical_field is None:
        return {
            "known": False,
            "blocked_reason": "unknown_source_field",
            "source": source,
            "table": table,
            "canonical_table": canonical_table,
            "field": field,
        }
    return {
        "known": True,
        "source": source,
        "source_table": table,
        "source_field": field,
        "canonical_table": canonical_table,
        "canonical_field": canonical_field,
    }


def source_alternatives_for_feature(feature_name: str) -> list[str]:
    by_name = {field.name: field for field in PIT_FINANCIAL_FIELDS}
    field = by_name.get(feature_name)
    if field is None:
        return []
    alternatives: list[str] = []
    for source_table in field.preferred_sources:
        for source_field in field.source_fields:
            alternatives.append(f"{source_table}.{source_field}")
    return alternatives
