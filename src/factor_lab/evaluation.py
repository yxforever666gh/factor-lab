from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import List, Optional

import pandas as pd

from .transaction_cost import TransactionCostModel, estimate_turnover_from_ic_decay


@dataclass
class FactorEvaluation:
    factor_name: str
    expression: str
    observations: int
    rank_ic_mean: float
    rank_ic_std: float
    rank_ic_ir: float
    top_bottom_spread_mean: float
    # 新增：交易成本相关字段
    turnover_rate: float
    transaction_cost_bps: float
    sharpe_gross: float
    sharpe_net: float
    net_return_annual: float
    pass_gate: bool
    fail_reason: str

    def to_dict(self):
        return asdict(self)


def _rank_ic_by_date(frame: pd.DataFrame) -> pd.Series:
    pairs: List[tuple] = []
    for date, group in frame.groupby("date", sort=True):
        ranked_factor = group["factor_value"].rank()
        ranked_return = group["forward_return_5d"].rank()
        if ranked_factor.nunique() <= 1 or ranked_return.nunique() <= 1:
            continue
        pairs.append((date, float(ranked_factor.corr(ranked_return))))
    return pd.Series(dict(pairs), name="rank_ic")


def _quintile_spread_by_date(frame: pd.DataFrame) -> pd.Series:
    spreads: List[tuple] = []
    for date, group in frame.groupby("date", sort=True):
        if group["factor_value"].nunique() < 5:
            continue
        ranked = group.assign(bucket=pd.qcut(group["factor_value"].rank(method="first"), 5, labels=False))
        top = ranked.loc[ranked["bucket"] == 4, "forward_return_5d"].mean()
        bottom = ranked.loc[ranked["bucket"] == 0, "forward_return_5d"].mean()
        spreads.append((date, float(top - bottom)))
    return pd.Series(dict(spreads), name="spread")


def evaluate_factor(frame: pd.DataFrame, factor_name: str, expression: str, thresholds: dict) -> FactorEvaluation:
    if frame.empty:
        ic_mean = 0.0
        ic_std = 0.0
        ic_ir = 0.0
        spread_mean = 0.0
        portfolio_returns = pd.Series(dtype=float)
    else:
        work = frame[["date", "factor_value", "forward_return_5d"]].copy()
        work = work.dropna(subset=["date", "factor_value", "forward_return_5d"])

        if work.empty:
            ic_mean = 0.0
            ic_std = 0.0
            ic_ir = 0.0
            spread_mean = 0.0
            portfolio_returns = pd.Series(dtype=float)
        else:
            by_date = work.groupby("date", sort=True)
            work["rank_factor"] = by_date["factor_value"].rank()
            work["rank_return"] = by_date["forward_return_5d"].rank()

            rank_stats = work.groupby("date", sort=True).agg(
                rf_mean=("rank_factor", "mean"),
                rr_mean=("rank_return", "mean"),
                rf_std=("rank_factor", lambda s: float(s.std(ddof=0))),
                rr_std=("rank_return", lambda s: float(s.std(ddof=0))),
                rf_nunique=("rank_factor", "nunique"),
                rr_nunique=("rank_return", "nunique"),
            )
            work = work.join(rank_stats[["rf_mean", "rr_mean"]], on="date")
            work["cov_term"] = (work["rank_factor"] - work["rf_mean"]) * (work["rank_return"] - work["rr_mean"])
            cov_by_date = work.groupby("date", sort=True)["cov_term"].mean()
            corr_by_date = cov_by_date / (rank_stats["rf_std"] * rank_stats["rr_std"])
            valid_corr = corr_by_date[(rank_stats["rf_nunique"] > 1) & (rank_stats["rr_nunique"] > 1)].dropna()

            if valid_corr.empty:
                ic_mean = 0.0
                ic_std = 0.0
                ic_ir = 0.0
            else:
                ic_mean = float(valid_corr.mean())
                ic_std = float(valid_corr.std(ddof=0)) if len(valid_corr) > 1 else 0.0
                ic_ir = ic_mean / ic_std if ic_std > 0 else 0.0

            work["bucket"] = (by_date["factor_value"].rank(method="first", pct=True) * 5).clip(upper=5).apply(lambda x: int(x) - 1)
            bucket_means = work.groupby(["date", "bucket"], sort=True)["forward_return_5d"].mean().unstack()
            if 0 in bucket_means.columns and 4 in bucket_means.columns:
                spreads = (bucket_means[4] - bucket_means[0]).dropna()
                spread_mean = float(spreads.mean()) if not spreads.empty else 0.0
                portfolio_returns = spreads  # 多空组合收益序列
            else:
                spread_mean = 0.0
                portfolio_returns = pd.Series(dtype=float)

    # 计算交易成本和净夏普
    cost_model = TransactionCostModel()
    
    if not portfolio_returns.empty and ic_std > 0:
        # 根据 IC 估算换手率
        turnover_rate = estimate_turnover_from_ic_decay(ic_mean, ic_std)
        
        # 计算扣成本前的夏普
        gross_mean = portfolio_returns.mean()
        gross_std = portfolio_returns.std(ddof=1) if len(portfolio_returns) > 1 else 0.0
        sharpe_gross = (gross_mean / gross_std * (252 ** 0.5)) if gross_std > 0 else 0.0
        
        # 计算交易成本
        cost_result = cost_model.calculate_cost_from_returns(
            returns=portfolio_returns,
            turnover_rate=turnover_rate,
            position_size=1e7,  # 假设 1000 万持仓
            adv=1e8,  # 假设平均日成交额 1 亿
        )
        
        sharpe_net = cost_result['sharpe_net']
        transaction_cost_bps = cost_result['cost_bps']
        net_return_annual = cost_result['net_return_mean'] * 252  # 年化收益
    else:
        turnover_rate = 0.0
        sharpe_gross = 0.0
        sharpe_net = 0.0
        transaction_cost_bps = 0.0
        net_return_annual = 0.0

    fail_reasons = []
    min_rank_ic = thresholds.get("min_rank_ic", 0.03)
    min_spread = thresholds.get("min_top_bottom_spread", 0.0)
    min_sharpe_net = thresholds.get("min_sharpe_net", 1.0)  # 新增：扣成本后夏普阈值
    
    if ic_mean < min_rank_ic:
        fail_reasons.append(f"rank_ic_mean<{min_rank_ic}")
    if spread_mean < min_spread:
        fail_reasons.append(f"top_bottom_spread<{min_spread}")
    if sharpe_net < min_sharpe_net:
        fail_reasons.append(f"sharpe_net<{min_sharpe_net}")

    return FactorEvaluation(
        factor_name=factor_name,
        expression=expression,
        observations=int(len(frame)),
        rank_ic_mean=round(ic_mean, 6),
        rank_ic_std=round(ic_std, 6),
        rank_ic_ir=round(ic_ir, 6),
        top_bottom_spread_mean=round(spread_mean, 6),
        turnover_rate=round(turnover_rate, 4),
        transaction_cost_bps=round(transaction_cost_bps, 2),
        sharpe_gross=round(sharpe_gross, 4),
        sharpe_net=round(sharpe_net, 4),
        net_return_annual=round(net_return_annual, 6),
        pass_gate=not fail_reasons,
        fail_reason="; ".join(fail_reasons) if fail_reasons else "",
    )
