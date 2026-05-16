"""
因子衰减分析

分析因子收益的衰减模式，确定最佳持有期和有效期。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd
import numpy as np
from scipy.optimize import curve_fit


@dataclass
class DecayProfile:
    """衰减曲线"""
    lags: List[int]
    ics: List[float]
    half_life_days: float
    optimal_holding_period: int
    decay_rate: float  # lambda in IC(t) = IC(0) * exp(-lambda * t)
    r_squared: float  # 拟合优度


def exponential_decay(t, ic0, lambda_):
    """指数衰减函数: IC(t) = IC(0) * exp(-lambda * t)"""
    return ic0 * np.exp(-lambda_ * t)


def analyze_factor_decay(
    factor_returns: pd.Series,
    max_lag: int = 20,
    min_observations: int = 30,
) -> DecayProfile:
    """
    分析因子收益的衰减模式
    
    Args:
        factor_returns: 因子收益序列（日度或周度）
        max_lag: 最大滞后期
        min_observations: 最小观测数
    
    Returns:
        衰减曲线
    """
    lags = []
    ics = []
    
    # 计算不同滞后期的 IC
    for lag in range(1, max_lag + 1):
        lagged_returns = factor_returns.shift(-lag)
        
        # 去除 NaN
        mask = ~(factor_returns.isna() | lagged_returns.isna())
        if mask.sum() < min_observations:
            break
        
        # 计算相关系数
        ic = factor_returns[mask].corr(lagged_returns[mask])
        
        lags.append(lag)
        ics.append(abs(ic))  # 使用绝对值
    
    if len(lags) < 3:
        # 数据不足，返回默认值
        return DecayProfile(
            lags=lags,
            ics=ics,
            half_life_days=10.0,
            optimal_holding_period=15,
            decay_rate=0.0693,  # ln(2) / 10
            r_squared=0.0,
        )
    
    # 拟合指数衰减曲线
    try:
        # 初始猜测
        ic0_guess = ics[0] if ics else 0.05
        lambda_guess = 0.1
        
        popt, pcov = curve_fit(
            exponential_decay,
            lags,
            ics,
            p0=[ic0_guess, lambda_guess],
            bounds=([0, 0], [1, 1]),
            maxfev=10000,
        )
        
        ic0_fit, lambda_fit = popt
        
        # 计算拟合优度
        ics_pred = exponential_decay(np.array(lags), ic0_fit, lambda_fit)
        ss_res = np.sum((np.array(ics) - ics_pred) ** 2)
        ss_tot = np.sum((np.array(ics) - np.mean(ics)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
        
    except:
        # 拟合失败，使用简单估计
        lambda_fit = 0.1
        r_squared = 0.0
    
    # 计算半衰期: t_half = ln(2) / lambda
    half_life_days = np.log(2) / lambda_fit if lambda_fit > 0 else 10.0
    
    # 最佳持有期 = 1.5 * 半衰期
    optimal_holding_period = int(half_life_days * 1.5)
    
    return DecayProfile(
        lags=lags,
        ics=ics,
        half_life_days=half_life_days,
        optimal_holding_period=optimal_holding_period,
        decay_rate=lambda_fit,
        r_squared=r_squared,
    )


def analyze_factor_decay_from_ic_series(
    ic_series: pd.Series,
    max_lag: int = 20,
) -> DecayProfile:
    """
    从 IC 时间序列分析衰减
    
    Args:
        ic_series: IC 时间序列（按日期）
        max_lag: 最大滞后期
    
    Returns:
        衰减曲线
    """
    lags = []
    ics = []
    
    # 计算不同滞后期的自相关
    for lag in range(1, max_lag + 1):
        lagged_ic = ic_series.shift(-lag)
        
        # 去除 NaN
        mask = ~(ic_series.isna() | lagged_ic.isna())
        if mask.sum() < 10:
            break
        
        # 计算自相关
        autocorr = ic_series[mask].corr(lagged_ic[mask])
        
        lags.append(lag)
        ics.append(abs(autocorr))
    
    if len(lags) < 3:
        return DecayProfile(
            lags=lags,
            ics=ics,
            half_life_days=10.0,
            optimal_holding_period=15,
            decay_rate=0.0693,
            r_squared=0.0,
        )
    
    # 拟合指数衰减
    try:
        popt, _ = curve_fit(
            exponential_decay,
            lags,
            ics,
            p0=[ics[0], 0.1],
            bounds=([0, 0], [1, 1]),
            maxfev=10000,
        )
        
        ic0_fit, lambda_fit = popt
        
        # 计算 R²
        ics_pred = exponential_decay(np.array(lags), ic0_fit, lambda_fit)
        ss_res = np.sum((np.array(ics) - ics_pred) ** 2)
        ss_tot = np.sum((np.array(ics) - np.mean(ics)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
        
    except:
        lambda_fit = 0.1
        r_squared = 0.0
    
    half_life_days = np.log(2) / lambda_fit if lambda_fit > 0 else 10.0
    optimal_holding_period = int(half_life_days * 1.5)
    
    return DecayProfile(
        lags=lags,
        ics=ics,
        half_life_days=half_life_days,
        optimal_holding_period=optimal_holding_period,
        decay_rate=lambda_fit,
        r_squared=r_squared,
    )


def detect_factor_failure(
    current_decay: DecayProfile,
    historical_decay: DecayProfile,
    threshold: float = 0.5,
) -> dict:
    """
    检测因子是否失效
    
    通过比较当前和历史的衰减曲线，判断因子是否失效。
    如果半衰期突然缩短超过阈值，可能表示因子失效。
    
    Args:
        current_decay: 当前衰减曲线
        historical_decay: 历史衰减曲线
        threshold: 阈值（半衰期缩短比例）
    
    Returns:
        检测结果
    """
    half_life_ratio = current_decay.half_life_days / historical_decay.half_life_days
    
    if half_life_ratio < (1 - threshold):
        return {
            'is_failing': True,
            'reason': 'half_life_shortened',
            'current_half_life': current_decay.half_life_days,
            'historical_half_life': historical_decay.half_life_days,
            'ratio': half_life_ratio,
            'severity': 'high' if half_life_ratio < 0.3 else 'medium',
        }
    
    # 检查 IC 水平是否下降
    current_ic0 = current_decay.ics[0] if current_decay.ics else 0.0
    historical_ic0 = historical_decay.ics[0] if historical_decay.ics else 0.0
    
    if current_ic0 < historical_ic0 * (1 - threshold):
        return {
            'is_failing': True,
            'reason': 'ic_level_dropped',
            'current_ic': current_ic0,
            'historical_ic': historical_ic0,
            'ratio': current_ic0 / historical_ic0 if historical_ic0 > 0 else 0.0,
            'severity': 'medium',
        }
    
    return {
        'is_failing': False,
        'reason': 'stable',
        'current_half_life': current_decay.half_life_days,
        'historical_half_life': historical_decay.half_life_days,
    }


def recommend_rebalance_frequency(
    decay_profile: DecayProfile,
    transaction_cost_bps: float,
) -> dict:
    """
    根据衰减曲线和交易成本推荐调仓频率
    
    Args:
        decay_profile: 衰减曲线
        transaction_cost_bps: 交易成本（bp）
    
    Returns:
        推荐结果
    """
    # 最佳持有期
    optimal_days = decay_profile.optimal_holding_period
    
    # 根据交易成本调整
    # 如果成本高，延长持有期
    if transaction_cost_bps > 200:
        adjusted_days = int(optimal_days * 1.5)
        reason = "高交易成本，延长持有期"
    elif transaction_cost_bps > 100:
        adjusted_days = int(optimal_days * 1.2)
        reason = "中等交易成本，适当延长持有期"
    else:
        adjusted_days = optimal_days
        reason = "低交易成本，使用最佳持有期"
    
    # 转换为调仓频率
    if adjusted_days <= 5:
        frequency = "daily"
        frequency_cn = "日度"
    elif adjusted_days <= 10:
        frequency = "weekly"
        frequency_cn = "周度"
    elif adjusted_days <= 30:
        frequency = "biweekly"
        frequency_cn = "双周"
    else:
        frequency = "monthly"
        frequency_cn = "月度"
    
    return {
        'optimal_holding_days': optimal_days,
        'adjusted_holding_days': adjusted_days,
        'rebalance_frequency': frequency,
        'rebalance_frequency_cn': frequency_cn,
        'reason': reason,
        'half_life_days': decay_profile.half_life_days,
    }


def create_decay_report(
    factor_name: str,
    decay_profile: DecayProfile,
    transaction_cost_bps: float,
) -> dict:
    """
    创建衰减分析报告
    
    Args:
        factor_name: 因子名称
        decay_profile: 衰减曲线
        transaction_cost_bps: 交易成本
    
    Returns:
        报告
    """
    rebalance_rec = recommend_rebalance_frequency(decay_profile, transaction_cost_bps)
    
    return {
        'factor_name': factor_name,
        'half_life_days': decay_profile.half_life_days,
        'optimal_holding_period': decay_profile.optimal_holding_period,
        'decay_rate': decay_profile.decay_rate,
        'r_squared': decay_profile.r_squared,
        'rebalance_recommendation': rebalance_rec,
        'decay_profile': {
            'lags': decay_profile.lags,
            'ics': decay_profile.ics,
        },
        'interpretation': _interpret_decay(decay_profile),
    }


def _interpret_decay(decay_profile: DecayProfile) -> str:
    """解释衰减曲线"""
    half_life = decay_profile.half_life_days
    
    if half_life < 5:
        return "快速衰减，适合高频交易"
    elif half_life < 15:
        return "中速衰减，适合周度调仓"
    elif half_life < 30:
        return "慢速衰减，适合双周或月度调仓"
    else:
        return "极慢衰减，适合低频交易"
