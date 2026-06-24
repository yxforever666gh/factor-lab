from __future__ import annotations

from typing import Any

from factor_lab.market_phenomena_schema import build_candidates_report

DEFAULT_MARKET = "cn_equity_daily"


def seed_market_phenomena() -> list[dict[str, Any]]:
    """Return deterministic seed phenomena for the research-agent v1.

    These are deliberately phenomenon descriptions rather than strategy rules.
    They are meant to feed quality/novelty/data-feasibility review, not trading.
    """

    return [
        {
            "phenomenon_id": "quality_repair_delayed_repricing_v1",
            "title": "盈利质量修复后的延迟重估",
            "mechanism_source": "information_delay",
            "participants": ["低频基本面资金", "覆盖不足股票投资者", "行业轮动资金"],
            "participant_constraints": ["信息处理慢", "财报可信度折价", "行业风险偏好约束"],
            "behavioral_story": "盈利质量改善已出现在 PIT 财务数据中，但覆盖不足或风险偏好约束使价格没有同步重估。",
            "temporary_mispricing_reason": "市场先按旧估值和旧行业叙事定价，直到质量修复被更多资金确认。",
            "why_not_immediately_arbitraged": "需要 PIT 财务拼接和跨期确认，容量有限，且错误判断 value trap 的成本较高。",
            "observable_variables": ["profit_yoy", "roe", "debt_to_asset", "operating_cashflow_to_profit", "pb", "industry_return_60d"],
            "prediction_target": "future_60d_relative_return_distribution",
            "expected_horizon": "60d/120d",
            "market_states_where_stronger": ["行业风险偏好修复", "财报后信息扩散期"],
            "failure_conditions": ["现金流质量未确认", "估值已提前修复", "行业趋势转负"],
            "minimal_verification_question": "当盈利质量改善且估值仍被压制时，未来收益和下行风险分布是否优于对照组？",
            "indicator_translation": {"pb": "估值压制代理，不是策略本身", "roe": "盈利质量代理，不是买入规则"},
            "scores": {"mechanism_strength": 8, "observability": 7, "testability": 7, "tradability_potential": 6, "novelty": 6, "crowding_risk": 5, "overfit_risk": 4, "cost_sensitivity": 5},
        },
        {
            "phenomenon_id": "value_trap_escape_after_balance_sheet_repair_v1",
            "title": "资产负债表修复后的价值陷阱脱离",
            "mechanism_source": "constraint_relief",
            "participants": ["困境股持有人", "基本面修复资金", "风险预算受限机构"],
            "participant_constraints": ["杠杆风险约束", "偿债压力", "风险预算限制"],
            "behavioral_story": "低估值股票常被视为 value trap；当负债率下降或现金流覆盖改善，部分风险约束解除。",
            "temporary_mispricing_reason": "市场对困境状态的折价更新滞后，低估值标签掩盖了财务约束缓解。",
            "why_not_immediately_arbitraged": "困境修复需要多期确认，且单期财务改善可能是假信号，套利资金不愿过早承担尾部风险。",
            "observable_variables": ["debt_to_asset", "debt_to_asset_delta", "operating_cashflow_to_profit", "roe", "pb", "industry"],
            "prediction_target": "future_120d_downside_adjusted_return_distribution",
            "expected_horizon": "60d/120d",
            "market_states_where_stronger": ["信用风险偏好回升", "行业盈利改善"],
            "failure_conditions": ["现金流未改善", "行业继续下行", "低估值来自永久性衰退"],
            "minimal_verification_question": "低估值股票中，资产负债表修复组的未来收益/回撤分布是否优于未修复组？",
            "indicator_translation": {"debt_to_asset": "约束缓解代理，不是独立买入规则"},
            "scores": {"mechanism_strength": 8, "observability": 7, "testability": 7, "tradability_potential": 6, "novelty": 7, "crowding_risk": 4, "overfit_risk": 4, "cost_sensitivity": 5},
        },
        {
            "phenomenon_id": "industry_cycle_confirmation_lag_v1",
            "title": "行业景气确认后的滞后扩散",
            "mechanism_source": "cross_sectional_diffusion",
            "participants": ["行业轮动资金", "指数增强资金", "覆盖不足的二线公司投资者"],
            "participant_constraints": ["先交易高流动性龙头", "二线股票信息扩散慢", "行业配置调整有摩擦"],
            "behavioral_story": "行业改善往往先反映在龙头或高流动性股票，二线低估值公司可能滞后反应。",
            "temporary_mispricing_reason": "资金先进入确认度和流动性更高的标的，行业内扩散存在时间差。",
            "why_not_immediately_arbitraged": "扩散交易容量有限，且二线公司可能有基本面质量差异，需要额外过滤。",
            "observable_variables": ["industry_return_60d", "industry_relative_pb", "industry_relative_earnings_yield", "volume", "forward_return_60d"],
            "prediction_target": "future_60d_industry_relative_return_distribution",
            "expected_horizon": "20d/60d",
            "market_states_where_stronger": ["行业动量转正", "市场风险偏好不差"],
            "failure_conditions": ["行业上涨只是 beta", "二线公司质量恶化", "扩散已完成"],
            "minimal_verification_question": "行业转强后，行业内滞后且估值压制的股票是否存在后续扩散收益？",
            "indicator_translation": {"industry_return_60d": "行业资金扩散状态代理，不是动量策略本身"},
            "scores": {"mechanism_strength": 7, "observability": 8, "testability": 8, "tradability_potential": 6, "novelty": 5, "crowding_risk": 6, "overfit_risk": 5, "cost_sensitivity": 5},
        },
        {
            "phenomenon_id": "coverage_neglect_post_report_drift_v1",
            "title": "低关注股票财报后的信息漂移",
            "mechanism_source": "attention_delay",
            "participants": ["卖方覆盖不足公司投资者", "小盘低关注股票持有人", "基本面筛选资金"],
            "participant_constraints": ["注意力有限", "研究覆盖不足", "小市值容量约束"],
            "behavioral_story": "低关注股票的财报信息可能不会立刻被充分处理，基本面改善后的价格反应存在漂移。",
            "temporary_mispricing_reason": "信息公开不等于被定价，低覆盖股票的信息处理和资金进入速度更慢。",
            "why_not_immediately_arbitraged": "容量小、交易成本高、覆盖不足导致套利资金筛选成本较高。",
            "observable_variables": ["report_ann_date", "profit_yoy", "volume", "turnover_rate", "market_cap", "analyst_coverage_proxy"],
            "prediction_target": "future_20d_to_60d_return_distribution",
            "expected_horizon": "20d/60d",
            "market_states_where_stronger": ["财报密集披露后", "市场流动性较好"],
            "failure_conditions": ["公告前已放量重估", "交易成本过高", "财报改善不可持续"],
            "minimal_verification_question": "低关注股票中，财报改善后的未来收益分布是否相对高关注股票更滞后？",
            "indicator_translation": {"turnover_rate": "关注度和资金进入代理，不是换手策略"},
            "scores": {"mechanism_strength": 7, "observability": 5, "testability": 6, "tradability_potential": 5, "novelty": 7, "crowding_risk": 4, "overfit_risk": 5, "cost_sensitivity": 7},
        },
        {
            "phenomenon_id": "liquidity_discount_reversal_after_volume_recovery_v1",
            "title": "流动性折价在成交恢复后的部分修复",
            "mechanism_source": "liquidity_gap",
            "participants": ["流动性受限持有人", "小盘资金", "风险预算受限机构"],
            "participant_constraints": ["冲击成本", "赎回压力", "容量约束"],
            "behavioral_story": "部分股票因流动性枯竭被折价；当成交和盘口承接恢复，流动性折价可能部分消退。",
            "temporary_mispricing_reason": "价格中的流动性折价变化快于基本面变化，但资金确认流动性恢复需要时间。",
            "why_not_immediately_arbitraged": "低流动性资产的建仓/退出成本高，折价修复路径不稳定。",
            "observable_variables": ["volume", "turnover_rate", "amount", "amihud_illiq_proxy", "spread_proxy", "future_return_20d"],
            "prediction_target": "future_20d_return_and_downside_risk_distribution",
            "expected_horizon": "5d/20d",
            "market_states_where_stronger": ["市场流动性改善", "非系统性抛压后"],
            "failure_conditions": ["成交恢复来自继续出货", "基本面恶化", "市场整体流动性收缩"],
            "minimal_verification_question": "低流动性折价股票在成交恢复后，未来收益和下行风险分布是否改善？",
            "indicator_translation": {"volume": "流动性恢复代理，不是放量买入规则"},
            "scores": {"mechanism_strength": 7, "observability": 8, "testability": 7, "tradability_potential": 5, "novelty": 6, "crowding_risk": 5, "overfit_risk": 5, "cost_sensitivity": 8},
        },
    ]


def build_seed_candidates_report(*, run_id: str, market: str = DEFAULT_MARKET) -> dict[str, Any]:
    return build_candidates_report(run_id=run_id, market=market, phenomena=seed_market_phenomena())
