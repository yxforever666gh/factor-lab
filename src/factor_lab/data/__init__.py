"""Lightweight local data services.

The package intentionally has no dependency on Research OS, a database, Docker,
or the retired workflow modules.  ``SampleDataset`` and ``SampleDataGenerator``
remain available here for compatibility with the original MVP imports.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .build import (
    apply_feature_store_migration,
    build_data,
    normalize_legacy_amount_units,
    plan_feature_store_migration,
)
from .catalog import (
    RuntimeLayout,
    audit_parquet,
    audit_top500_store,
    load_data_config,
    parquet_status,
)
from .enrich import (
    build_monthly_reference_state,
    canonical_trading_dates,
    enrich_top500_store,
    prepare_financial_pit,
)
from .diemeng_minutes import (
    DIEMENG_LEVELS,
    DIEMENG_HISTORY_URL,
    DIEMENG_MINUTE_COLUMNS,
    DIEMENG_RAW_COLUMNS,
    DIEMENG_UNIT_CONTRACT_ID,
    DiemengMinuteCapture,
    DiemengMinuteDataError,
    DiemengMinuteHTTPClient,
    DiemengMinuteTransportError,
    audit_diemeng_execution_slice,
    audit_diemeng_full_day,
    audit_diemeng_one_minute_day,
    capture_diemeng_minutes,
    first_five_minute_audit_bar,
    freeze_diemeng_unit_contract,
    normalize_diemeng_minutes,
)
from .diemeng_minute_store import CandidateMinuteStore
from .pit_lineage import (
    DEFAULT_WALK_FORWARD_REQUIRED_FIELDS,
    PIT_CONTRACT_SCHEMA_VERSION,
    PIT_STATUS_UNVERIFIED,
    PIT_STATUS_VERIFIED,
    PITFieldLineage,
    PITLineageContract,
    audit_pit_lineage,
    conservative_default_contract,
    conservative_default_field_lineage,
)
from .opportunity_set import (
    CarriedSuspensionEvidence,
    DailyOpportunitySet,
    DailyOpportunitySetBuilder,
    OpportunitySetDataError,
    SecurityAliasInterval,
)
from .security_master import (
    audit_security_master,
    load_security_master,
    sync_security_master,
)
from .sources import (
    TushareClient,
    create_daily_stock_st_cutoff_checkpoint,
    sync_data,
    sync_daily_stock_st,
    sync_enrichment,
    sync_exact_reference,
    turnover_amount_to_rmb,
)
from .suspensions import audit_suspensions_snapshot, sync_suspensions
from .wide_pricing import CALENDAR_SENTINEL, SparsePricingBuilder, SparsePricingDay


@dataclass
class SampleDataset:
    frame: pd.DataFrame


class SampleDataGenerator:
    """Generate the small deterministic dataset used by legacy MVP tests."""

    def __init__(self, seed: int = 7) -> None:
        self.rng = np.random.default_rng(seed)

    def generate(self, num_stocks: int = 60, num_days: int = 220) -> SampleDataset:
        tickers = [f"STK{i:03d}" for i in range(1, num_stocks + 1)]
        dates = pd.bdate_range("2025-01-01", periods=num_days)
        base_quality = self.rng.normal(0.0, 1.0, num_stocks)
        base_value = self.rng.normal(0.0, 1.0, num_stocks)
        base_liquidity = self.rng.normal(0.0, 1.0, num_stocks)
        prev_close = self.rng.uniform(20.0, 120.0, num_stocks)
        trailing_returns = {ticker: [] for ticker in tickers}
        trailing_turnover = {ticker: [] for ticker in tickers}
        rows: list[pd.DataFrame] = []

        for date in dates:
            market_noise = self.rng.normal(0.0, 0.004)
            base_quality += self.rng.normal(0.0, 0.02, num_stocks)
            base_value += self.rng.normal(0.0, 0.02, num_stocks)
            base_liquidity += self.rng.normal(0.0, 0.03, num_stocks)
            roe = 0.12 + base_quality * 0.03 + self.rng.normal(0.0, 0.01, num_stocks)
            pb = 2.2 - base_value * 0.35 + self.rng.normal(0.0, 0.08, num_stocks)
            earnings_yield = (
                0.055 + base_value * 0.015 + self.rng.normal(0.0, 0.004, num_stocks)
            )
            turnover = np.exp(
                0.6 + base_liquidity * 0.35 + self.rng.normal(0.0, 0.2, num_stocks)
            )
            values: dict[str, list[float]] = {
                "momentum_20": [],
                "momentum_60": [],
                "momentum_120": [],
                "momentum_60_skip_5": [],
                "turnover_shock_5_20": [],
                "return_1d": [],
                "close": [],
            }
            for index, ticker in enumerate(tickers):
                past_returns = trailing_returns[ticker]
                past_turnovers = trailing_turnover[ticker]
                values["momentum_20"].append(float(np.sum(past_returns[-20:])))
                values["momentum_60"].append(float(np.sum(past_returns[-60:])))
                values["momentum_120"].append(float(np.sum(past_returns[-120:])))
                skipped = past_returns[:-5] if len(past_returns) > 5 else []
                values["momentum_60_skip_5"].append(float(np.sum(skipped[-60:])))
                turnover_5 = (
                    float(np.mean(past_turnovers[-5:]))
                    if len(past_turnovers) >= 5
                    else float(turnover[index])
                )
                turnover_20 = (
                    float(np.mean(past_turnovers[-20:]))
                    if len(past_turnovers) >= 20
                    else float(turnover[index])
                )
                shock = (turnover_5 / turnover_20) - 1.0 if turnover_20 else 0.0
                values["turnover_shock_5_20"].append(shock)
                alpha = (
                    0.04 * values["momentum_20"][-1]
                    + 0.60 * (float(earnings_yield[index]) - 0.055)
                    + 0.40 * (float(roe[index]) - 0.12)
                    + 0.01 * shock
                )
                daily_return = float(
                    np.clip(market_noise + alpha + self.rng.normal(0.0, 0.02), -0.12, 0.12)
                )
                close = max(float(prev_close[index] * (1.0 + daily_return)), 1.0)
                values["return_1d"].append(daily_return)
                values["close"].append(close)

            rows.append(
                pd.DataFrame(
                    {
                        "date": date,
                        "ticker": tickers,
                        "roe": roe,
                        "pb": pb,
                        "earnings_yield": earnings_yield,
                        "turnover": turnover,
                        **values,
                    }
                )
            )
            for index, ticker in enumerate(tickers):
                prev_close[index] = values["close"][index]
                trailing_returns[ticker].append(values["return_1d"][index])
                trailing_turnover[ticker].append(float(turnover[index]))

        frame = pd.concat(rows, ignore_index=True)
        frame["forward_return_5d"] = frame.groupby("ticker")["return_1d"].transform(
            lambda series: series.shift(-1).rolling(5).sum()
        )
        return SampleDataset(frame=frame.dropna(subset=["forward_return_5d"]).reset_index(drop=True))


__all__ = [
    "CALENDAR_SENTINEL",
    "CarriedSuspensionEvidence",
    "CandidateMinuteStore",
    "DEFAULT_WALK_FORWARD_REQUIRED_FIELDS",
    "DailyOpportunitySet",
    "DailyOpportunitySetBuilder",
    "DIEMENG_LEVELS",
    "DIEMENG_HISTORY_URL",
    "DIEMENG_MINUTE_COLUMNS",
    "DIEMENG_RAW_COLUMNS",
    "DIEMENG_UNIT_CONTRACT_ID",
    "DiemengMinuteCapture",
    "DiemengMinuteDataError",
    "DiemengMinuteHTTPClient",
    "DiemengMinuteTransportError",
    "PIT_CONTRACT_SCHEMA_VERSION",
    "PIT_STATUS_UNVERIFIED",
    "PIT_STATUS_VERIFIED",
    "OpportunitySetDataError",
    "PITFieldLineage",
    "PITLineageContract",
    "RuntimeLayout",
    "SampleDataGenerator",
    "SampleDataset",
    "SecurityAliasInterval",
    "SparsePricingBuilder",
    "SparsePricingDay",
    "TushareClient",
    "apply_feature_store_migration",
    "audit_parquet",
    "audit_diemeng_execution_slice",
    "audit_diemeng_full_day",
    "audit_diemeng_one_minute_day",
    "audit_pit_lineage",
    "audit_security_master",
    "audit_suspensions_snapshot",
    "audit_top500_store",
    "build_data",
    "build_monthly_reference_state",
    "canonical_trading_dates",
    "capture_diemeng_minutes",
    "conservative_default_contract",
    "conservative_default_field_lineage",
    "create_daily_stock_st_cutoff_checkpoint",
    "enrich_top500_store",
    "first_five_minute_audit_bar",
    "freeze_diemeng_unit_contract",
    "load_data_config",
    "load_security_master",
    "normalize_legacy_amount_units",
    "normalize_diemeng_minutes",
    "parquet_status",
    "plan_feature_store_migration",
    "prepare_financial_pit",
    "sync_data",
    "sync_daily_stock_st",
    "sync_enrichment",
    "sync_exact_reference",
    "sync_security_master",
    "sync_suspensions",
    "turnover_amount_to_rmb",
]
