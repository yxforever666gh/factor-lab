"""
交易成本模型

用于计算因子交易的真实成本，包括：
1. 固定佣金
2. 滑点成本
3. 市场冲击成本
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass
class TransactionCostConfig:
    """交易成本配置"""
    commission_rate: float = 0.0003  # 万三佣金
    slippage_bps: float = 5.0  # 5bp 滑点
    impact_coef: float = 0.1  # 冲击成本系数
    
    # A 股特殊参数
    stamp_duty: float = 0.001  # 印花税（卖出时）
    transfer_fee: float = 0.00002  # 过户费（双向）


class TransactionCostModel:
    """交易成本模型"""
    
    def __init__(self, config: Optional[TransactionCostConfig] = None):
        self.config = config or TransactionCostConfig()
    
    def calculate_cost(
        self,
        turnover: float,
        position_size: float = 1.0,
        adv: float = 1e8,
    ) -> dict:
        """
        计算交易成本
        
        Args:
            turnover: 换手率（0-1 之间，1 表示 100% 换手）
            position_size: 持仓规模（元）
            adv: 平均日成交额（元）
        
        Returns:
            包含各项成本的字典
        """
        # 固定成本（佣金 + 印花税 + 过户费）
        # 买入：佣金 + 过户费
        # 卖出：佣金 + 印花税 + 过户费
        buy_cost = (self.config.commission_rate + self.config.transfer_fee) * turnover
        sell_cost = (
            self.config.commission_rate 
            + self.config.stamp_duty 
            + self.config.transfer_fee
        ) * turnover
        fixed_cost = buy_cost + sell_cost
        
        # 滑点成本（双向）
        slippage_cost = (self.config.slippage_bps / 10000) * turnover * 2
        
        # 市场冲击成本（与持仓/流动性比例相关）
        # 使用平方根模型：impact ∝ sqrt(position_size / adv)
        liquidity_ratio = position_size / adv if adv > 0 else 0
        impact_cost = self.config.impact_coef * (liquidity_ratio ** 0.5) * turnover
        
        total_cost = fixed_cost + slippage_cost + impact_cost
        
        return {
            'fixed_cost': fixed_cost,
            'slippage_cost': slippage_cost,
            'impact_cost': impact_cost,
            'total_cost': total_cost,
            'total_cost_bps': total_cost * 10000,  # 转换为 bp
        }
    
    def calculate_cost_from_returns(
        self,
        returns: pd.Series,
        turnover_rate: float = 0.5,
        position_size: float = 1e7,
        adv: float = 1e8,
    ) -> dict:
        """
        从收益序列计算扣成本后的指标
        
        Args:
            returns: 收益率序列（日度或周度）
            turnover_rate: 平均换手率（每期）
            position_size: 持仓规模
            adv: 平均日成交额
        
        Returns:
            包含净收益和夏普的字典
        """
        # 计算单次交易成本
        cost_per_trade = self.calculate_cost(
            turnover=turnover_rate,
            position_size=position_size,
            adv=adv,
        )
        
        # 扣除成本后的收益
        cost_per_period = cost_per_trade['total_cost']
        net_returns = returns - cost_per_period
        
        # 计算夏普比率（假设年化 252 个交易日）
        if len(net_returns) > 1:
            mean_return = net_returns.mean()
            std_return = net_returns.std(ddof=1)
            sharpe = (mean_return / std_return * (252 ** 0.5)) if std_return > 0 else 0.0
        else:
            sharpe = 0.0
        
        return {
            'gross_return_mean': returns.mean(),
            'net_return_mean': net_returns.mean(),
            'cost_per_period': cost_per_period,
            'cost_bps': cost_per_period * 10000,
            'sharpe_net': sharpe,
            'observations': len(returns),
        }


def estimate_turnover_from_ic_decay(ic_mean: float, ic_std: float) -> float:
    """
    根据 IC 衰减估算最优换手率
    
    简化假设：
    - IC 越高，可以承受更高的换手
    - IC 越稳定（std 低），可以承受更高的换手
    
    Args:
        ic_mean: IC 均值
        ic_std: IC 标准差
    
    Returns:
        估算的换手率（0-1）
    """
    # 信息比率
    ir = ic_mean / ic_std if ic_std > 0 else 0
    
    # 根据 IR 估算换手率
    # IR < 0.5: 低频（月度换手，~0.2）
    # IR 0.5-1.0: 中频（周度换手，~0.5）
    # IR > 1.0: 高频（日度换手，~1.0）
    if ir < 0.5:
        return 0.2
    elif ir < 1.0:
        return 0.5
    else:
        return min(1.0, ir * 0.5)
