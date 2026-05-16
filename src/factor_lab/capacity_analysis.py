"""
容量分析模块

估算因子能承载的最大资金量。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any

import pandas as pd
import numpy as np


@dataclass
class CapacityConfig:
    """容量分析配置"""
    max_daily_volume_pct: float = 0.05  # 单日交易量不超过 ADV 的 5%
    top_holdings_pct: float = 0.1  # 前 10% 持仓
    turnover_days: int = 20  # 换手周期（天）
    max_impact_bps: float = 10.0  # 最大允许冲击成本（bp）
    impact_coef: float = 0.1  # 冲击成本系数


class CapacityAnalyzer:
    """容量分析器"""
    
    def __init__(self, config: Optional[CapacityConfig] = None):
        """
        Args:
            config: 容量分析配置
        """
        self.config = config or CapacityConfig()
    
    def estimate_capacity(
        self,
        factor_values: pd.Series,
        market_data: pd.DataFrame,
    ) -> Dict[str, Any]:
        """
        估算因子容量
        
        Args:
            factor_values: 因子值（股票 → 因子值）
            market_data: 市场数据，包含：
                - amount_20d_avg: 20 日平均成交额
                - market_cap: 市值
                - price: 价格
        
        Returns:
            容量分析结果
        """
        # 合并数据
        df = pd.DataFrame({
            'factor_value': factor_values,
            'amount_20d_avg': market_data['amount_20d_avg'],
            'market_cap': market_data.get('market_cap', np.nan),
        }).dropna()
        
        if len(df) == 0:
            return self._empty_result()
        
        # 计算因子排名
        df['rank'] = df['factor_value'].rank(pct=True)
        
        # 前 10% 持仓
        top_holdings = df[df['rank'] >= (1 - self.config.top_holdings_pct)]
        
        if len(top_holdings) == 0:
            return self._empty_result()
        
        # 计算流动性
        total_adv = top_holdings['amount_20d_avg'].sum()
        
        # 单日可交易量
        daily_capacity = total_adv * self.config.max_daily_volume_pct
        
        # 总容量 = 单日容量 * 换手周期
        total_capacity = daily_capacity * self.config.turnover_days
        
        # 考虑市场冲击
        # 冲击成本 = impact_coef * sqrt(position_size / adv)
        # 要求冲击成本 < max_impact_bps
        # position_size < (max_impact_bps / impact_coef)^2 * adv
        max_position_per_stock = (
            (self.config.max_impact_bps / 10000 / self.config.impact_coef) ** 2 
            * top_holdings['amount_20d_avg']
        )
        
        impact_limited_capacity = max_position_per_stock.sum()
        
        # 取两者的最小值
        final_capacity = min(total_capacity, impact_limited_capacity)
        
        # 计算信号集中度
        concentration = self._calculate_concentration(df)
        
        return {
            'capacity_rmb': final_capacity,
            'capacity_million': final_capacity / 1e6,
            'daily_capacity': daily_capacity,
            'total_adv': total_adv,
            'top_holdings_count': len(top_holdings),
            'concentration': concentration,
            'limiting_factor': 'volume' if total_capacity < impact_limited_capacity else 'impact',
            'turnover_days': self.config.turnover_days,
        }
    
    def _calculate_concentration(self, df: pd.DataFrame) -> Dict[str, float]:
        """计算信号集中度"""
        # 前 10% 持仓的权重
        df['rank'] = df['factor_value'].rank(pct=True)
        top_10 = df[df['rank'] >= 0.9]
        
        if len(top_10) == 0:
            return {'top_10_pct': 0.0, 'hhi': 0.0}
        
        # 假设等权持仓
        weights = np.ones(len(top_10)) / len(top_10)
        
        # HHI（赫芬达尔指数）
        hhi = np.sum(weights ** 2)
        
        return {
            'top_10_pct': len(top_10) / len(df),
            'hhi': hhi,
        }
    
    def _empty_result(self) -> Dict[str, Any]:
        """空结果"""
        return {
            'capacity_rmb': 0.0,
            'capacity_million': 0.0,
            'daily_capacity': 0.0,
            'total_adv': 0.0,
            'top_holdings_count': 0,
            'concentration': {'top_10_pct': 0.0, 'hhi': 0.0},
            'limiting_factor': 'unknown',
            'turnover_days': self.config.turnover_days,
        }
    
    def estimate_capacity_by_quantile(
        self,
        factor_values: pd.Series,
        market_data: pd.DataFrame,
        quantiles: list = [0.1, 0.2, 0.3],
    ) -> pd.DataFrame:
        """
        按不同分位数估算容量
        
        Args:
            factor_values: 因子值
            market_data: 市场数据
            quantiles: 分位数列表
        
        Returns:
            不同分位数的容量
        """
        results = []
        
        for q in quantiles:
            config = CapacityConfig(top_holdings_pct=q)
            analyzer = CapacityAnalyzer(config)
            result = analyzer.estimate_capacity(factor_values, market_data)
            
            results.append({
                'top_holdings_pct': q * 100,
                'capacity_million': result['capacity_million'],
                'top_holdings_count': result['top_holdings_count'],
                'limiting_factor': result['limiting_factor'],
            })
        
        return pd.DataFrame(results)


def classify_capacity(capacity_million: float) -> Dict[str, Any]:
    """
    分类容量等级
    
    Args:
        capacity_million: 容量（百万元）
    
    Returns:
        容量等级
    """
    if capacity_million >= 500:
        level = 'very_high'
        level_cn = '极高'
        description = '可承载大型机构资金'
    elif capacity_million >= 100:
        level = 'high'
        level_cn = '高'
        description = '可承载中型机构资金'
    elif capacity_million >= 50:
        level = 'medium'
        level_cn = '中等'
        description = '可承载小型机构或大户资金'
    elif capacity_million >= 10:
        level = 'low'
        level_cn = '低'
        description = '仅适合小规模资金'
    else:
        level = 'very_low'
        level_cn = '极低'
        description = '容量不足，不适合实盘'
    
    return {
        'level': level,
        'level_cn': level_cn,
        'description': description,
        'capacity_million': capacity_million,
    }


def create_capacity_report(
    factor_name: str,
    capacity_result: Dict[str, Any],
) -> Dict[str, Any]:
    """
    创建容量分析报告
    
    Args:
        factor_name: 因子名称
        capacity_result: 容量分析结果
    
    Returns:
        报告
    """
    capacity_million = capacity_result['capacity_million']
    classification = classify_capacity(capacity_million)
    
    return {
        'factor_name': factor_name,
        'capacity_million': capacity_million,
        'capacity_level': classification['level_cn'],
        'description': classification['description'],
        'daily_capacity_million': capacity_result['daily_capacity'] / 1e6,
        'top_holdings_count': capacity_result['top_holdings_count'],
        'concentration_hhi': capacity_result['concentration']['hhi'],
        'limiting_factor': capacity_result['limiting_factor'],
        'turnover_days': capacity_result['turnover_days'],
        'recommendation': _build_recommendation(capacity_result),
    }


def _build_recommendation(capacity_result: Dict[str, Any]) -> str:
    """构建建议"""
    capacity_million = capacity_result['capacity_million']
    limiting_factor = capacity_result['limiting_factor']
    
    if capacity_million < 10:
        return "容量不足，不建议实盘使用"
    elif capacity_million < 50:
        return "容量较低，仅适合小规模资金（< 1000 万）"
    elif capacity_million < 100:
        return "容量中等，适合中小规模资金（1000-5000 万）"
    elif capacity_million < 500:
        return "容量较高，适合中大规模资金（5000 万 - 2 亿）"
    else:
        return "容量极高，可承载大规模资金（> 2 亿）"


def batch_capacity_analysis(
    factors: Dict[str, pd.Series],
    market_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    批量容量分析
    
    Args:
        factors: 因子字典（因子名 → 因子值）
        market_data: 市场数据
    
    Returns:
        容量分析结果表
    """
    analyzer = CapacityAnalyzer()
    results = []
    
    for factor_name, factor_values in factors.items():
        capacity_result = analyzer.estimate_capacity(factor_values, market_data)
        report = create_capacity_report(factor_name, capacity_result)
        results.append(report)
    
    df = pd.DataFrame(results)
    
    # 按容量排序
    df = df.sort_values('capacity_million', ascending=False)
    
    return df
