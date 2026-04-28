"""
因子归因体系

将因子收益归因到风格因子，判断是否有真正的 alpha。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict, Any

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression


@dataclass
class AttributionConfig:
    """归因配置"""
    style_factors: List[str] = None  # 风格因子列表
    min_observations: int = 30  # 最小观测数
    
    def __post_init__(self):
        if self.style_factors is None:
            # 默认风格因子
            self.style_factors = ['market', 'size', 'value', 'momentum']


class FactorAttribution:
    """因子归因分析器"""
    
    def __init__(self, config: Optional[AttributionConfig] = None):
        """
        Args:
            config: 归因配置
        """
        self.config = config or AttributionConfig()
    
    def attribute_to_styles(
        self,
        factor_returns: pd.Series,
        style_returns: pd.DataFrame,
    ) -> Dict[str, Any]:
        """
        将因子收益归因到风格因子
        
        Args:
            factor_returns: 因子收益序列
            style_returns: 风格因子收益矩阵（列=风格因子）
        
        Returns:
            归因结果
        """
        # 对齐数据
        df = pd.DataFrame({
            'factor': factor_returns,
        }).join(style_returns, how='inner')
        
        df = df.dropna()
        
        if len(df) < self.config.min_observations:
            return self._empty_result()
        
        # 准备数据
        y = df['factor'].values
        X = df[style_returns.columns].values
        
        # 回归分析
        model = LinearRegression()
        model.fit(X, y)
        
        # 预测
        y_pred = model.predict(X)
        
        # 计算 R²
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
        
        # Alpha（截距）
        alpha = model.intercept_
        
        # Beta（系数）
        betas = dict(zip(style_returns.columns, model.coef_))
        
        # 计算各风格因子的贡献
        contributions = {}
        for i, style in enumerate(style_returns.columns):
            style_contribution = model.coef_[i] * style_returns[style].mean()
            contributions[style] = style_contribution
        
        # 残差（alpha 部分）
        residuals = y - y_pred
        alpha_contribution = residuals.mean()
        
        return {
            'alpha': alpha,
            'alpha_annual': alpha * 252,
            'alpha_contribution': alpha_contribution,
            'betas': betas,
            'contributions': contributions,
            'r_squared': r_squared,
            'residual_volatility': residuals.std(),
            'observations': len(df),
            'interpretation': self._interpret_attribution(r_squared, alpha, betas),
        }
    
    def _interpret_attribution(
        self,
        r_squared: float,
        alpha: float,
        betas: Dict[str, float],
    ) -> str:
        """解释归因结果"""
        if r_squared > 0.8:
            # 主要是风格因子的组合
            dominant_style = max(betas.items(), key=lambda x: abs(x[1]))
            return f"主要是风格因子的组合（R²={r_squared:.2f}），主导因子：{dominant_style[0]}"
        elif r_squared > 0.5:
            # 部分来自风格，部分是 alpha
            if alpha > 0:
                return f"部分来自风格因子（R²={r_squared:.2f}），有一定 alpha（{alpha*252*100:.2f}% 年化）"
            else:
                return f"部分来自风格因子（R²={r_squared:.2f}），alpha 为负"
        else:
            # 主要是 alpha
            if alpha > 0:
                return f"主要是 alpha（R²={r_squared:.2f}），年化 alpha {alpha*252*100:.2f}%"
            else:
                return f"低 R²（{r_squared:.2f}），但 alpha 为负，可能是噪音"
    
    def _empty_result(self) -> Dict[str, Any]:
        """空结果"""
        return {
            'alpha': 0.0,
            'alpha_annual': 0.0,
            'alpha_contribution': 0.0,
            'betas': {},
            'contributions': {},
            'r_squared': 0.0,
            'residual_volatility': 0.0,
            'observations': 0,
            'interpretation': '数据不足',
        }
    
    def rolling_attribution(
        self,
        factor_returns: pd.Series,
        style_returns: pd.DataFrame,
        window: int = 60,
    ) -> pd.DataFrame:
        """
        滚动归因分析
        
        Args:
            factor_returns: 因子收益序列
            style_returns: 风格因子收益矩阵
            window: 滚动窗口（天）
        
        Returns:
            滚动归因结果
        """
        results = []
        
        for i in range(window, len(factor_returns)):
            window_factor = factor_returns.iloc[i-window:i]
            window_styles = style_returns.iloc[i-window:i]
            
            attribution = self.attribute_to_styles(window_factor, window_styles)
            
            results.append({
                'date': factor_returns.index[i],
                'alpha': attribution['alpha'],
                'r_squared': attribution['r_squared'],
                **{f'beta_{k}': v for k, v in attribution['betas'].items()},
            })
        
        return pd.DataFrame(results)


def create_style_factors(
    market_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    创建风格因子
    
    Args:
        market_data: 市场数据，包含：
            - date: 日期
            - ticker: 股票代码
            - return: 收益率
            - market_cap: 市值
            - pb: 市净率
            - momentum_20d: 20 日动量
    
    Returns:
        风格因子收益矩阵
    """
    # 按日期分组
    style_returns = []
    
    for date, group in market_data.groupby('date'):
        if len(group) < 10:
            continue
        
        # 市场因子（等权市场收益）
        market_return = group['return'].mean()
        
        # 规模因子（小盘 - 大盘）
        group['size_rank'] = group['market_cap'].rank(pct=True)
        small_cap = group[group['size_rank'] <= 0.3]['return'].mean()
        large_cap = group[group['size_rank'] >= 0.7]['return'].mean()
        size_return = small_cap - large_cap
        
        # 价值因子（高 PB - 低 PB）
        if 'pb' in group.columns:
            group['value_rank'] = group['pb'].rank(pct=True)
            value_stocks = group[group['value_rank'] <= 0.3]['return'].mean()
            growth_stocks = group[group['value_rank'] >= 0.7]['return'].mean()
            value_return = value_stocks - growth_stocks
        else:
            value_return = 0.0
        
        # 动量因子（高动量 - 低动量）
        if 'momentum_20d' in group.columns:
            group['momentum_rank'] = group['momentum_20d'].rank(pct=True)
            high_momentum = group[group['momentum_rank'] >= 0.7]['return'].mean()
            low_momentum = group[group['momentum_rank'] <= 0.3]['return'].mean()
            momentum_return = high_momentum - low_momentum
        else:
            momentum_return = 0.0
        
        style_returns.append({
            'date': date,
            'market': market_return,
            'size': size_return,
            'value': value_return,
            'momentum': momentum_return,
        })
    
    df = pd.DataFrame(style_returns)
    df = df.set_index('date')
    
    return df


def create_attribution_report(
    factor_name: str,
    attribution_result: Dict[str, Any],
) -> Dict[str, Any]:
    """
    创建归因报告
    
    Args:
        factor_name: 因子名称
        attribution_result: 归因结果
    
    Returns:
        报告
    """
    r_squared = attribution_result['r_squared']
    alpha_annual = attribution_result['alpha_annual']
    
    # 判断因子类型
    if r_squared > 0.8:
        factor_type = '风格因子组合'
        recommendation = '主要是风格因子的线性组合，建议检查是否有独特价值'
    elif r_squared > 0.5:
        if alpha_annual > 0.05:
            factor_type = '混合型因子'
            recommendation = '部分来自风格，部分是 alpha，可以使用'
        else:
            factor_type = '弱 alpha 因子'
            recommendation = '风格暴露较高，alpha 较弱'
    else:
        if alpha_annual > 0.05:
            factor_type = '纯 alpha 因子'
            recommendation = '主要是 alpha，风格中性，推荐使用'
        else:
            factor_type = '噪音因子'
            recommendation = '低 R² 且 alpha 为负，可能是噪音'
    
    return {
        'factor_name': factor_name,
        'factor_type': factor_type,
        'alpha_annual_pct': alpha_annual * 100,
        'r_squared': r_squared,
        'betas': attribution_result['betas'],
        'interpretation': attribution_result['interpretation'],
        'recommendation': recommendation,
    }


def batch_attribution_analysis(
    factors: Dict[str, pd.Series],
    style_returns: pd.DataFrame,
) -> pd.DataFrame:
    """
    批量归因分析
    
    Args:
        factors: 因子字典（因子名 → 因子收益）
        style_returns: 风格因子收益矩阵
    
    Returns:
        归因分析结果表
    """
    attributor = FactorAttribution()
    results = []
    
    for factor_name, factor_returns in factors.items():
        attribution = attributor.attribute_to_styles(factor_returns, style_returns)
        report = create_attribution_report(factor_name, attribution)
        results.append(report)
    
    df = pd.DataFrame(results)
    
    # 按 alpha 排序
    df = df.sort_values('alpha_annual_pct', ascending=False)
    
    return df
