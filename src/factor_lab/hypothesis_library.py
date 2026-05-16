"""
投资假设库

定义经典的投资假设，用于指导因子生成。
每个假设包含：经济学逻辑、预期表现、失效场景。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class FactorHypothesis:
    """投资假设定义"""
    id: str
    name: str
    category: str  # momentum, value, quality, growth, liquidity, technical
    hypothesis: str  # 核心假设
    economic_logic: str  # 经济学逻辑
    expected_ic: float  # 预期 IC
    expected_sharpe: float  # 预期夏普（扣成本后）
    expected_holding_period: str  # 预期持有期
    risk_factors: List[str] = field(default_factory=list)  # 风险因素
    failure_modes: List[str] = field(default_factory=list)  # 失效场景
    data_requirements: List[str] = field(default_factory=list)  # 数据要求
    example_expressions: List[str] = field(default_factory=list)  # 示例表达式
    
    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'name': self.name,
            'category': self.category,
            'hypothesis': self.hypothesis,
            'economic_logic': self.economic_logic,
            'expected_ic': self.expected_ic,
            'expected_sharpe': self.expected_sharpe,
            'expected_holding_period': self.expected_holding_period,
            'risk_factors': self.risk_factors,
            'failure_modes': self.failure_modes,
            'data_requirements': self.data_requirements,
            'example_expressions': self.example_expressions,
        }


# 投资假设库
HYPOTHESIS_LIBRARY = [
    # ========== 动量类 ==========
    FactorHypothesis(
        id='price_momentum',
        name='价格动量',
        category='momentum',
        hypothesis='过去表现好的股票会继续表现好',
        economic_logic='投资者反应不足（under-reaction）导致价格趋势延续',
        expected_ic=0.04,
        expected_sharpe=0.8,
        expected_holding_period='1-3 months',
        risk_factors=['市场反转', '流动性冲击'],
        failure_modes=['熊市末期', '市场极度波动时'],
        data_requirements=['close', 'volume'],
        example_expressions=[
            'close / close_20d - 1',
            'close / close_60d - 1',
            '(close / close_20d - 1) * volume_20d',
        ]
    ),
    
    FactorHypothesis(
        id='earnings_momentum',
        name='盈利动量',
        category='momentum',
        hypothesis='盈利持续改善的公司会有超额收益',
        economic_logic='盈利趋势具有持续性，市场对盈利变化反应不充分',
        expected_ic=0.035,
        expected_sharpe=0.7,
        expected_holding_period='1-2 quarters',
        risk_factors=['财报季节性', '会计调整'],
        failure_modes=['经济周期拐点', '行业政策变化'],
        data_requirements=['net_profit', 'net_profit_yoy'],
        example_expressions=[
            'net_profit_yoy',
            'net_profit / net_profit_4q - 1',
            '(net_profit_yoy + net_profit_yoy_1q) / 2',
        ]
    ),
    
    # ========== 价值类 ==========
    FactorHypothesis(
        id='value_mean_reversion',
        name='价值均值回归',
        category='value',
        hypothesis='低估值股票会向合理估值回归',
        economic_logic='市场过度反应导致估值偏离，长期会回归均值',
        expected_ic=0.03,
        expected_sharpe=0.6,
        expected_holding_period='6-12 months',
        risk_factors=['价值陷阱', '基本面恶化'],
        failure_modes=['成长股牛市', '流动性宽松时期'],
        data_requirements=['pb', 'pe', 'ps'],
        example_expressions=[
            '1 / pb',
            '1 / pe',
            '(1/pb + 1/pe) / 2',
        ]
    ),
    
    FactorHypothesis(
        id='earnings_yield',
        name='盈利收益率',
        category='value',
        hypothesis='高盈利收益率的股票提供更好的风险回报',
        economic_logic='盈利收益率是股票的真实回报率，高收益率补偿风险',
        expected_ic=0.035,
        expected_sharpe=0.7,
        expected_holding_period='3-6 months',
        risk_factors=['盈利质量', '会计操纵'],
        failure_modes=['盈利不可持续', '行业衰退'],
        data_requirements=['net_profit', 'market_cap'],
        example_expressions=[
            'net_profit / market_cap',
            'ebit / market_cap',
            'fcf / market_cap',
        ]
    ),
    
    # ========== 质量类 ==========
    FactorHypothesis(
        id='profitability',
        name='盈利能力',
        category='quality',
        hypothesis='高盈利能力的公司会持续创造价值',
        economic_logic='盈利能力反映竞争优势，具有持续性',
        expected_ic=0.03,
        expected_sharpe=0.6,
        expected_holding_period='6-12 months',
        risk_factors=['行业竞争加剧', '成本上升'],
        failure_modes=['行业周期下行', '技术颠覆'],
        data_requirements=['roa', 'roe', 'gross_margin'],
        example_expressions=[
            'roa',
            'roe',
            '(roa + roe) / 2',
        ]
    ),
    
    FactorHypothesis(
        id='earnings_quality',
        name='盈利质量',
        category='quality',
        hypothesis='高质量盈利（现金流支撑）的公司更可靠',
        economic_logic='现金流比会计利润更真实，高质量盈利不易操纵',
        expected_ic=0.025,
        expected_sharpe=0.5,
        expected_holding_period='6-12 months',
        risk_factors=['行业特性差异', '资本开支周期'],
        failure_modes=['高增长阶段', '并购重组期'],
        data_requirements=['net_profit', 'operating_cash_flow'],
        example_expressions=[
            'operating_cash_flow / net_profit',
            'fcf / net_profit',
            'operating_cash_flow / revenue',
        ]
    ),
    
    # ========== 成长类 ==========
    FactorHypothesis(
        id='earnings_growth',
        name='盈利增长',
        category='growth',
        hypothesis='盈利高增长的公司会有超额收益',
        economic_logic='增长创造价值，市场对增长的定价不充分',
        expected_ic=0.04,
        expected_sharpe=0.8,
        expected_holding_period='1-2 quarters',
        risk_factors=['增长不可持续', '估值过高'],
        failure_modes=['经济下行', '行业增速放缓'],
        data_requirements=['net_profit_yoy', 'revenue_yoy'],
        example_expressions=[
            'net_profit_yoy',
            'revenue_yoy',
            '(net_profit_yoy + revenue_yoy) / 2',
        ]
    ),
    
    FactorHypothesis(
        id='earnings_surprise',
        name='盈利超预期',
        category='growth',
        hypothesis='实际盈利超出预期的公司会有持续超额收益',
        economic_logic='市场对盈利的反应不充分，存在盈利动量效应',
        expected_ic=0.03,
        expected_sharpe=0.7,
        expected_holding_period='1-3 months',
        risk_factors=['财报季节性', '分析师覆盖不足'],
        failure_modes=['牛市中失效', '小盘股数据不可靠'],
        data_requirements=['net_profit', 'net_profit_forecast'],
        example_expressions=[
            'net_profit / net_profit_forecast - 1',
            '(net_profit - net_profit_forecast) / abs(net_profit_forecast)',
        ]
    ),
    
    # ========== 流动性类 ==========
    FactorHypothesis(
        id='liquidity_premium',
        name='流动性溢价',
        category='liquidity',
        hypothesis='低流动性股票要求更高的回报补偿',
        economic_logic='流动性风险需要溢价补偿，低流动性股票长期回报更高',
        expected_ic=0.02,
        expected_sharpe=0.4,
        expected_holding_period='6-12 months',
        risk_factors=['市场流动性危机', '强制平仓风险'],
        failure_modes=['流动性紧缩时期', '市场恐慌时'],
        data_requirements=['turnover', 'amount'],
        example_expressions=[
            '1 / turnover_20d',
            '1 / amount_20d',
            '1 / (turnover_20d * amount_20d)',
        ]
    ),
    
    FactorHypothesis(
        id='attention_effect',
        name='关注度效应',
        category='liquidity',
        hypothesis='高关注度股票短期会有超额收益',
        economic_logic='投资者关注有限，关注度提升带来买盘',
        expected_ic=0.025,
        expected_sharpe=0.5,
        expected_holding_period='1-4 weeks',
        risk_factors=['关注度快速衰减', '炒作风险'],
        failure_modes=['市场理性时期', '监管收紧'],
        data_requirements=['turnover', 'volume'],
        example_expressions=[
            'turnover / turnover_60d - 1',
            'volume / volume_20d - 1',
            '(turnover / turnover_60d) * (volume / volume_20d)',
        ]
    ),
    
    # ========== 技术类 ==========
    FactorHypothesis(
        id='volatility_effect',
        name='波动率效应',
        category='technical',
        hypothesis='低波动率股票长期回报更高（低波动率异象）',
        economic_logic='投资者偏好高波动（彩票效应），导致低波动股票被低估',
        expected_ic=0.025,
        expected_sharpe=0.5,
        expected_holding_period='3-6 months',
        risk_factors=['市场风格切换', '波动率聚集'],
        failure_modes=['牛市加速期', '市场恐慌时'],
        data_requirements=['close', 'high', 'low'],
        example_expressions=[
            '1 / std_20d',
            '1 / (high_20d / low_20d - 1)',
            '1 / atr_20d',
        ]
    ),
    
    FactorHypothesis(
        id='reversal_effect',
        name='短期反转',
        category='technical',
        hypothesis='短期超跌的股票会反弹',
        economic_logic='过度反应导致短期偏离，会快速修正',
        expected_ic=0.03,
        expected_sharpe=0.6,
        expected_holding_period='1-2 weeks',
        risk_factors=['趋势延续', '基本面恶化'],
        failure_modes=['单边市', '流动性危机'],
        data_requirements=['close'],
        example_expressions=[
            '-(close / close_5d - 1)',
            '-(close / close_10d - 1)',
            '-(close_5d / close_10d - 1)',
        ]
    ),
]


def get_hypothesis_by_id(hypothesis_id: str) -> Optional[FactorHypothesis]:
    """根据 ID 获取假设"""
    for hyp in HYPOTHESIS_LIBRARY:
        if hyp.id == hypothesis_id:
            return hyp
    return None


def get_hypotheses_by_category(category: str) -> List[FactorHypothesis]:
    """根据类别获取假设"""
    return [hyp for hyp in HYPOTHESIS_LIBRARY if hyp.category == category]


def list_all_hypotheses() -> List[dict]:
    """列出所有假设（简化版）"""
    return [
        {
            'id': hyp.id,
            'name': hyp.name,
            'category': hyp.category,
            'expected_ic': hyp.expected_ic,
            'expected_sharpe': hyp.expected_sharpe,
        }
        for hyp in HYPOTHESIS_LIBRARY
    ]


def select_hypothesis_for_generation(
    existing_factors: List[str],
    market_regime: str = 'normal',
    target_category: Optional[str] = None,
) -> Optional[FactorHypothesis]:
    """
    选择一个假设用于生成新因子
    
    Args:
        existing_factors: 已有因子列表
        market_regime: 市场环境（normal, bull, bear, volatile）
        target_category: 目标类别（可选）
    
    Returns:
        选中的假设
    """
    # 过滤候选假设
    candidates = HYPOTHESIS_LIBRARY
    
    if target_category:
        candidates = [h for h in candidates if h.category == target_category]
    
    # 根据市场环境调整
    if market_regime == 'bull':
        # 牛市偏好动量和成长
        candidates = [h for h in candidates if h.category in ['momentum', 'growth']]
    elif market_regime == 'bear':
        # 熊市偏好价值和质量
        candidates = [h for h in candidates if h.category in ['value', 'quality']]
    elif market_regime == 'volatile':
        # 波动市偏好低波和反转
        candidates = [h for h in candidates if h.category in ['technical']]
    
    # 避免重复（简化版：检查类别是否已经有很多因子）
    # 这里可以加入更复杂的逻辑
    
    if not candidates:
        return None
    
    # 简单选择：返回第一个
    # 实际应用中可以加入更复杂的选择逻辑（如随机、轮换等）
    return candidates[0]
