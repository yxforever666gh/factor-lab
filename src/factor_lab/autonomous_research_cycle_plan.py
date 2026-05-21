from __future__ import annotations
from typing import Any
REQUIRED_FIELDS=['roe','pb','pe_ttm','earnings_yield','return_1d','total_mv','turnover']
def defensive_quality_plan_template(cycle_id:str, *, max_experiments:int=2, dataset_path:str|None=None)->dict[str,Any]:
    def fields() -> list[str]:
        return list(REQUIRED_FIELDS)
    ex=[{'experiment_id':'dq_low_vol_quality_filter_v1','type':'simulated_portfolio_repair','objective':'Test low-volatility plus non-distressed quality filters.','required_fields':fields(),'expected_information_gain':'Separate single-name risk filtering effect from market-state drawdown effect.','falsification_criteria':['max_drawdown remains below -0.45','return collapses by more than 50%'],'max_runtime_minutes':20},{'experiment_id':'dq_market_state_de_risk_v1','type':'simulated_portfolio_repair','objective':'Test market-state reduced exposure in downtrend regimes.','required_fields':fields(),'expected_information_gain':'Measure whether regime exposure reduction reduces drawdown.','falsification_criteria':['max_drawdown remains below -0.45','return collapses by more than 50%'],'max_runtime_minutes':20}][:max_experiments]
    p={'schema_version':1,'cycle_id':cycle_id,'mainline':'defensive_quality_risk_layer','research_question':'Can defensive quality / low-risk filters reduce max drawdown without destroying return?','hypothesis':'Lower-volatility, non-distressed, not-overvalued stocks plus market-state risk reduction should reduce drawdown for the long-only small institutional portfolio.','mechanism_id':'defensive_quality_risk_layer','why_now':['current blocker is drawdown_risk_too_high'],'experiments':ex,'budget':{'max_experiments':max_experiments},'stop_conditions':['no drawdown improvement','missing required fields','duplicate equivalent experiment'],'success_criteria':['max_drawdown >= -0.35','holding count remains 50-100','single position cap passes'],'manual_approval_required':False,'live_trading_enabled':False,'broad_daemon_restore_allowed':False}
    if dataset_path: p['dataset_path']=dataset_path
    return p
def validate_cycle_plan(plan:dict[str,Any], config:dict[str,Any])->dict[str,Any]:
    reasons=[]
    if plan.get('mainline') not in config.get('allowed_mainlines',[]): reasons.append('unsupported_mainline')
    if len(plan.get('experiments') or [])>int(config.get('max_experiments_per_cycle',3)): reasons.append('too_many_experiments')
    if not plan.get('hypothesis'): reasons.append('missing_hypothesis')
    if plan.get('live_trading_enabled'): reasons.append('live_trading_requested')
    if plan.get('broad_daemon_restore_allowed'): reasons.append('broad_daemon_restore_requested')
    for e in plan.get('experiments') or []:
        if not e.get('falsification_criteria'): reasons.append('missing_falsification_criteria')
        if not e.get('expected_information_gain'): reasons.append('missing_expected_information_gain')
        if e.get('type')!='simulated_portfolio_repair': reasons.append('unsupported_execution_type')
    return {'valid':not reasons,'reasons':sorted(set(reasons))}
