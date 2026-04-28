"""
多因子组合优化器

从简单加权改为风险优化，考虑因子间的相关性。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict, Any

import pandas as pd
import numpy as np
from scipy.optimize import minimize


@dataclass
class OptimizationConfig:
    """优化配置"""
    risk_aversion: float = 1.0  # 风险厌恶系数
    min_weight: float = 0.0  # 最小权重
    max_weight: float = 0.3  # 最大权重
    target_volatility: Optional[float] = None  # 目标波动率
    industry_neutral: bool = False  # 行业中性
    size_neutral: bool = False  # 市值中性


class PortfolioOptimizer:
    """多因子组合优化器"""
    
    def __init__(self, config: Optional[OptimizationConfig] = None):
        """
        Args:
            config: 优化配置
        """
        self.config = config or OptimizationConfig()
    
    def optimize_weights(
        self,
        factor_returns: pd.DataFrame,
        expected_returns: Optional[pd.Series] = None,
    ) -> Dict[str, Any]:
        """
        优化因子权重
        
        Args:
            factor_returns: 因子收益矩阵（行=日期，列=因子）
            expected_returns: 预期收益（可选，默认使用历史均值）
        
        Returns:
            优化结果
        """
        n_factors = len(factor_returns.columns)
        
        # 计算协方差矩阵
        cov_matrix = factor_returns.cov().values
        
        # 预期收益
        if expected_returns is None:
            expected_returns = factor_returns.mean()
        mu = expected_returns.values
        
        # 初始权重（等权）
        w0 = np.ones(n_factors) / n_factors
        
        # 目标函数：最大化夏普比率
        def objective(w):
            portfolio_return = np.dot(w, mu)
            portfolio_vol = np.sqrt(np.dot(w, np.dot(cov_matrix, w)))
            sharpe = -portfolio_return / portfolio_vol if portfolio_vol > 0 else 0
            return sharpe  # 最小化负夏普
        
        # 约束条件
        constraints = [
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0},  # 权重和为 1
        ]
        
        # 边界
        bounds = [(self.config.min_weight, self.config.max_weight) for _ in range(n_factors)]
        
        # 优化
        result = minimize(
            objective,
            w0,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            options={'maxiter': 1000}
        )
        
        if not result.success:
            # 优化失败，返回等权
            optimal_weights = w0
        else:
            optimal_weights = result.x
        
        # 计算优化后的组合指标
        portfolio_return = np.dot(optimal_weights, mu)
        portfolio_vol = np.sqrt(np.dot(optimal_weights, np.dot(cov_matrix, optimal_weights)))
        sharpe = portfolio_return / portfolio_vol if portfolio_vol > 0 else 0.0
        
        # 计算等权组合的指标（用于对比）
        equal_weight_return = np.dot(w0, mu)
        equal_weight_vol = np.sqrt(np.dot(w0, np.dot(cov_matrix, w0)))
        equal_weight_sharpe = equal_weight_return / equal_weight_vol if equal_weight_vol > 0 else 0.0
        
        return {
            'optimal_weights': dict(zip(factor_returns.columns, optimal_weights)),
            'portfolio_return': portfolio_return,
            'portfolio_volatility': portfolio_vol,
            'portfolio_sharpe': sharpe,
            'equal_weight_sharpe': equal_weight_sharpe,
            'improvement': sharpe - equal_weight_sharpe,
            'optimization_success': result.success,
        }
    
    def optimize_min_variance(
        self,
        factor_returns: pd.DataFrame,
    ) -> Dict[str, Any]:
        """
        最小方差优化
        
        Args:
            factor_returns: 因子收益矩阵
        
        Returns:
            优化结果
        """
        n_factors = len(factor_returns.columns)
        
        # 协方差矩阵
        cov_matrix = factor_returns.cov().values
        
        # 初始权重
        w0 = np.ones(n_factors) / n_factors
        
        # 目标函数：最小化方差
        def objective(w):
            return np.dot(w, np.dot(cov_matrix, w))
        
        # 约束
        constraints = [
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0},
        ]
        
        # 边界
        bounds = [(self.config.min_weight, self.config.max_weight) for _ in range(n_factors)]
        
        # 优化
        result = minimize(
            objective,
            w0,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
        )
        
        optimal_weights = result.x if result.success else w0
        
        # 计算指标
        portfolio_vol = np.sqrt(np.dot(optimal_weights, np.dot(cov_matrix, optimal_weights)))
        equal_weight_vol = np.sqrt(np.dot(w0, np.dot(cov_matrix, w0)))
        
        return {
            'optimal_weights': dict(zip(factor_returns.columns, optimal_weights)),
            'portfolio_volatility': portfolio_vol,
            'equal_weight_volatility': equal_weight_vol,
            'volatility_reduction': equal_weight_vol - portfolio_vol,
            'optimization_success': result.success,
        }
    
    def optimize_risk_parity(
        self,
        factor_returns: pd.DataFrame,
    ) -> Dict[str, Any]:
        """
        风险平价优化
        
        每个因子贡献相同的风险
        
        Args:
            factor_returns: 因子收益矩阵
        
        Returns:
            优化结果
        """
        n_factors = len(factor_returns.columns)
        
        # 协方差矩阵
        cov_matrix = factor_returns.cov().values
        
        # 初始权重
        w0 = np.ones(n_factors) / n_factors
        
        # 目标函数：最小化风险贡献的差异
        def objective(w):
            portfolio_vol = np.sqrt(np.dot(w, np.dot(cov_matrix, w)))
            marginal_contrib = np.dot(cov_matrix, w) / portfolio_vol
            risk_contrib = w * marginal_contrib
            target_risk = portfolio_vol / n_factors
            return np.sum((risk_contrib - target_risk) ** 2)
        
        # 约束
        constraints = [
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0},
        ]
        
        # 边界
        bounds = [(self.config.min_weight, self.config.max_weight) for _ in range(n_factors)]
        
        # 优化
        result = minimize(
            objective,
            w0,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
        )
        
        optimal_weights = result.x if result.success else w0
        
        # 计算风险贡献
        portfolio_vol = np.sqrt(np.dot(optimal_weights, np.dot(cov_matrix, optimal_weights)))
        marginal_contrib = np.dot(cov_matrix, optimal_weights) / portfolio_vol
        risk_contrib = optimal_weights * marginal_contrib
        
        return {
            'optimal_weights': dict(zip(factor_returns.columns, optimal_weights)),
            'portfolio_volatility': portfolio_vol,
            'risk_contributions': dict(zip(factor_returns.columns, risk_contrib)),
            'optimization_success': result.success,
        }


def compare_optimization_methods(
    factor_returns: pd.DataFrame,
    expected_returns: Optional[pd.Series] = None,
) -> pd.DataFrame:
    """
    比较不同优化方法
    
    Args:
        factor_returns: 因子收益矩阵
        expected_returns: 预期收益
    
    Returns:
        对比结果
    """
    optimizer = PortfolioOptimizer()
    
    # 等权
    n_factors = len(factor_returns.columns)
    equal_weights = {col: 1.0/n_factors for col in factor_returns.columns}
    
    # 最大夏普
    max_sharpe = optimizer.optimize_weights(factor_returns, expected_returns)
    
    # 最小方差
    min_var = optimizer.optimize_min_variance(factor_returns)
    
    # 风险平价
    risk_parity = optimizer.optimize_risk_parity(factor_returns)
    
    # 构建对比表
    results = []
    
    for method, weights in [
        ('等权', equal_weights),
        ('最大夏普', max_sharpe['optimal_weights']),
        ('最小方差', min_var['optimal_weights']),
        ('风险平价', risk_parity['optimal_weights']),
    ]:
        w = np.array([weights[col] for col in factor_returns.columns])
        
        # 计算指标
        if expected_returns is not None:
            mu = expected_returns.values
        else:
            mu = factor_returns.mean().values
        
        cov_matrix = factor_returns.cov().values
        
        ret = np.dot(w, mu)
        vol = np.sqrt(np.dot(w, np.dot(cov_matrix, w)))
        sharpe = ret / vol if vol > 0 else 0.0
        
        results.append({
            '方法': method,
            '年化收益': ret * 252,
            '年化波动': vol * np.sqrt(252),
            '夏普比率': sharpe * np.sqrt(252),
        })
    
    return pd.DataFrame(results)


def backtest_portfolio(
    factor_returns: pd.DataFrame,
    weights: Dict[str, float],
    rebalance_frequency: int = 20,
) -> pd.Series:
    """
    回测组合表现
    
    Args:
        factor_returns: 因子收益矩阵
        weights: 因子权重
        rebalance_frequency: 调仓频率（天）
    
    Returns:
        组合收益序列
    """
    portfolio_returns = []
    
    for i in range(len(factor_returns)):
        # 每隔 rebalance_frequency 天调仓
        if i % rebalance_frequency == 0:
            current_weights = np.array([weights.get(col, 0) for col in factor_returns.columns])
        
        # 计算当日收益
        daily_return = np.dot(current_weights, factor_returns.iloc[i].values)
        portfolio_returns.append(daily_return)
    
    return pd.Series(portfolio_returns, index=factor_returns.index)
