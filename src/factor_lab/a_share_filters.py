"""
A 股市场特殊处理模块

处理 A 股市场的特殊规则：
1. 过滤不可交易的股票（ST、停牌、新股等）
2. 处理涨跌停限制
3. 处理 T+1 交易规则
4. 处理其他 A 股特有的市场微观结构
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd
import numpy as np


@dataclass
class AShareFilterConfig:
    """A 股过滤配置"""
    # ST 股票
    exclude_st: bool = True
    
    # 停牌
    max_suspend_days: int = 0  # 最大允许停牌天数
    
    # 新股
    min_list_days: int = 60  # 最小上市天数
    
    # 价格
    min_price: float = 2.0  # 最低价格（避免仙股）
    max_price: Optional[float] = None  # 最高价格
    
    # 流动性
    min_turnover_20d: float = 0.001  # 最小 20 日平均换手率
    min_amount_20d: float = 1e6  # 最小 20 日平均成交额（元）
    
    # 市值
    min_market_cap: Optional[float] = None  # 最小市值（元）
    max_market_cap: Optional[float] = None  # 最大市值（元）
    
    # 涨跌停
    limit_up_pct: float = 0.10  # 涨停幅度（10%）
    limit_down_pct: float = 0.10  # 跌停幅度（10%）
    st_limit_pct: float = 0.05  # ST 股票涨跌停幅度（5%）
    
    # 其他
    exclude_delisted: bool = True  # 排除已退市股票


def filter_tradable_universe(
    df: pd.DataFrame,
    config: Optional[AShareFilterConfig] = None,
) -> pd.DataFrame:
    """
    过滤不可交易的股票
    
    Args:
        df: 股票数据，需要包含以下字段：
            - ticker: 股票代码
            - date: 日期
            - st_flag: ST 标记（0/1）
            - suspend_days: 停牌天数
            - list_days: 上市天数
            - price: 价格
            - turnover_20d: 20 日平均换手率
            - amount_20d: 20 日平均成交额
            - market_cap: 市值（可选）
        config: 过滤配置
    
    Returns:
        过滤后的数据
    """
    if config is None:
        config = AShareFilterConfig()
    
    df = df.copy()
    initial_count = len(df)
    
    # 1. 排除 ST 股票
    if config.exclude_st and 'st_flag' in df.columns:
        df = df[df['st_flag'] == 0]
    
    # 2. 排除停牌股票
    if 'suspend_days' in df.columns:
        df = df[df['suspend_days'] <= config.max_suspend_days]
    
    # 3. 排除新股
    if 'list_days' in df.columns:
        df = df[df['list_days'] > config.min_list_days]
    
    # 4. 价格过滤
    if 'price' in df.columns:
        df = df[df['price'] >= config.min_price]
        if config.max_price is not None:
            df = df[df['price'] <= config.max_price]
    
    # 5. 流动性过滤
    if 'turnover_20d' in df.columns:
        df = df[df['turnover_20d'] > config.min_turnover_20d]
    
    if 'amount_20d' in df.columns:
        df = df[df['amount_20d'] > config.min_amount_20d]
    
    # 6. 市值过滤
    if 'market_cap' in df.columns:
        if config.min_market_cap is not None:
            df = df[df['market_cap'] >= config.min_market_cap]
        if config.max_market_cap is not None:
            df = df[df['market_cap'] <= config.max_market_cap]
    
    # 7. 排除退市股票
    if config.exclude_delisted and 'delisted_flag' in df.columns:
        df = df[df['delisted_flag'] == 0]
    
    final_count = len(df)
    filter_rate = (initial_count - final_count) / initial_count if initial_count > 0 else 0
    
    return df


def adjust_for_limit(
    returns: pd.Series,
    prices: pd.Series,
    prev_prices: pd.Series,
    st_flags: Optional[pd.Series] = None,
    config: Optional[AShareFilterConfig] = None,
) -> tuple[pd.Series, pd.Series]:
    """
    处理涨跌停
    
    在涨跌停日，实际收益可能无法实现（买不到或卖不出）
    
    Args:
        returns: 收益率序列
        prices: 当日价格
        prev_prices: 前一日价格
        st_flags: ST 标记（可选）
        config: 配置
    
    Returns:
        (调整后的收益率, 涨跌停标记)
    """
    if config is None:
        config = AShareFilterConfig()
    
    # 计算涨跌幅
    price_change = (prices - prev_prices) / prev_prices
    
    # 确定涨跌停阈值
    if st_flags is not None:
        limit_threshold = np.where(st_flags == 1, config.st_limit_pct, config.limit_up_pct)
    else:
        limit_threshold = config.limit_up_pct
    
    # 标记涨跌停（价格变动接近限制的 95%）
    limit_up_flags = price_change >= (limit_threshold * 0.95)
    limit_down_flags = price_change <= (-limit_threshold * 0.95)
    limit_flags = limit_up_flags | limit_down_flags
    
    # 调整收益率（涨跌停日收益打折）
    # 假设涨停日只能实现 50% 的收益（因为买不到）
    # 跌停日只能实现 50% 的损失（因为卖不出）
    adjusted_returns = returns.copy()
    adjusted_returns[limit_flags] = adjusted_returns[limit_flags] * 0.5
    
    return adjusted_returns, limit_flags


def handle_t_plus_1(
    signals: pd.DataFrame,
    date_col: str = 'date',
    signal_col: str = 'signal',
) -> pd.DataFrame:
    """
    处理 T+1 交易规则
    
    今天的信号，明天才能交易
    
    Args:
        signals: 信号数据，包含 date 和 signal 列
        date_col: 日期列名
        signal_col: 信号列名
    
    Returns:
        调整后的信号（延迟一天）
    """
    df = signals.copy()
    
    # 按股票分组，信号延迟一天
    if 'ticker' in df.columns:
        df[signal_col] = df.groupby('ticker')[signal_col].shift(1)
    else:
        df[signal_col] = df[signal_col].shift(1)
    
    return df


def calculate_implementable_return(
    factor_values: pd.DataFrame,
    returns: pd.DataFrame,
    prices: pd.DataFrame,
    config: Optional[AShareFilterConfig] = None,
) -> pd.DataFrame:
    """
    计算可实现的收益（考虑 A 股特性）
    
    Args:
        factor_values: 因子值，包含 date, ticker, factor_value
        returns: 收益率，包含 date, ticker, return
        prices: 价格数据，包含 date, ticker, price
        config: 配置
    
    Returns:
        可实现的收益数据
    """
    if config is None:
        config = AShareFilterConfig()
    
    # 1. 过滤可交易股票池
    tradable = filter_tradable_universe(factor_values, config)
    
    # 2. T+1 延迟
    tradable = handle_t_plus_1(tradable, signal_col='factor_value')
    
    # 3. 合并收益和价格
    result = tradable.merge(
        returns[['date', 'ticker', 'return']],
        on=['date', 'ticker'],
        how='left'
    )
    
    result = result.merge(
        prices[['date', 'ticker', 'price']],
        on=['date', 'ticker'],
        how='left'
    )
    
    # 4. 计算前一日价格
    result = result.sort_values(['ticker', 'date'])
    result['prev_price'] = result.groupby('ticker')['price'].shift(1)
    
    # 5. 处理涨跌停
    st_flags = result['st_flag'] if 'st_flag' in result.columns else None
    adjusted_returns, limit_flags = adjust_for_limit(
        returns=result['return'],
        prices=result['price'],
        prev_prices=result['prev_price'],
        st_flags=st_flags,
        config=config,
    )
    
    result['adjusted_return'] = adjusted_returns
    result['limit_flag'] = limit_flags
    
    return result


def get_a_share_statistics(df: pd.DataFrame) -> dict:
    """
    获取 A 股特性统计
    
    Args:
        df: 股票数据
    
    Returns:
        统计信息
    """
    stats = {}
    
    if 'st_flag' in df.columns:
        stats['st_ratio'] = df['st_flag'].mean()
    
    if 'suspend_days' in df.columns:
        stats['suspend_ratio'] = (df['suspend_days'] > 0).mean()
        stats['avg_suspend_days'] = df[df['suspend_days'] > 0]['suspend_days'].mean()
    
    if 'list_days' in df.columns:
        stats['new_stock_ratio'] = (df['list_days'] <= 60).mean()
    
    if 'turnover_20d' in df.columns:
        stats['low_liquidity_ratio'] = (df['turnover_20d'] < 0.001).mean()
    
    if 'limit_flag' in df.columns:
        stats['limit_ratio'] = df['limit_flag'].mean()
    
    return stats


def create_mock_a_share_data(
    n_dates: int = 100,
    n_stocks: int = 500,
    st_ratio: float = 0.05,
    suspend_ratio: float = 0.02,
) -> pd.DataFrame:
    """
    创建模拟的 A 股数据（用于测试）
    
    Args:
        n_dates: 日期数
        n_stocks: 股票数
        st_ratio: ST 股票比例
        suspend_ratio: 停牌比例
    
    Returns:
        模拟数据
    """
    np.random.seed(42)
    
    dates = pd.date_range('2023-01-01', periods=n_dates, freq='D')
    data = []
    
    for date in dates:
        for stock_id in range(n_stocks):
            # 随机生成各种标记
            st_flag = 1 if np.random.random() < st_ratio else 0
            suspend_days = np.random.poisson(0.1) if np.random.random() < suspend_ratio else 0
            list_days = max(0, int(np.random.normal(500, 300)))
            
            price = max(1.0, np.random.lognormal(2.0, 0.5))
            turnover_20d = max(0, np.random.lognormal(-6, 1))
            amount_20d = price * turnover_20d * 1e8
            market_cap = price * 1e8
            
            data.append({
                'date': date,
                'ticker': f'{stock_id:06d}.SZ' if stock_id % 2 == 0 else f'{stock_id:06d}.SH',
                'st_flag': st_flag,
                'suspend_days': suspend_days,
                'list_days': list_days,
                'price': price,
                'turnover_20d': turnover_20d,
                'amount_20d': amount_20d,
                'market_cap': market_cap,
                'delisted_flag': 0,
            })
    
    return pd.DataFrame(data)
