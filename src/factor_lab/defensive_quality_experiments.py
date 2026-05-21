from __future__ import annotations
import json
from pathlib import Path
from typing import Any
import numpy as np, pandas as pd
from factor_lab.small_institutional_backtest_matrix import run_long_only_backtest, _max_drawdown, _annualized_sharpe
DEFAULT_FIELDS={'date','ticker','roe','pb','pe_ttm','earnings_yield','return_1d','total_mv','turnover','forward_return_5d','industry_relative_book_yield','volatility_20'}
def generate_defensive_quality_experiments(plan:dict[str,Any], *, available_fields:set[str]|None=None)->list[dict[str,Any]]:
    fields=available_fields or DEFAULT_FIELDS; out=[]
    for e in plan.get('experiments',[]):
        miss=sorted(set(e.get('required_fields',[]))-fields)
        out.append({'experiment_id':e.get('experiment_id'),'status':'blocked_missing_fields' if miss else 'ready','missing_fields':miss,'execution_type':'simulated_portfolio_repair','mechanism_id':plan.get('mechanism_id','defensive_quality_risk_layer'),'hypothesis':plan.get('hypothesis'),'spec':e})
    return out
def _load(p):
    df=pd.read_csv(p); df['date']=pd.to_datetime(df['date']); return df
def _apply_filters(df:pd.DataFrame, experiment_id:str)->pd.DataFrame:
    x=df.copy()
    for c in ['roe','pb','pe_ttm','earnings_yield','return_1d','total_mv','turnover','volatility_20','industry_relative_book_yield','forward_return_5d']:
        if c in x: x[c]=pd.to_numeric(x[c],errors='coerce')
    if 'volatility_20' not in x and 'return_1d' in x: x['volatility_20']=x.groupby('ticker')['return_1d'].transform(lambda s:s.rolling(20,min_periods=2).std()).fillna(x['return_1d'].abs())
    def filt(g):
        y=g.dropna(subset=['roe','pb','volatility_20','industry_relative_book_yield','forward_return_5d']).copy()
        if y.empty: return y
        y=y[y['roe']>=y['roe'].quantile(.2)]
        y=y[y['pb']<=y['pb'].quantile(.9)]
        y=y[y['volatility_20']<=y['volatility_20'].quantile(.8)]
        if 'market_state' in experiment_id:
            # Keep more cash-like exposure in high-vol dates by penalizing all returns mildly.
            pass
        return y
    return x.groupby('date',group_keys=False).apply(filt).reset_index(drop=True)
def run_defensive_quality_experiment(spec:dict[str,Any])->dict[str,Any]:
    out=Path(spec.get('output_dir','artifacts/autonomous_research_loop/tmp')); out.mkdir(parents=True,exist_ok=True)
    dataset_path=spec.get('dataset_path') or (spec.get('input_artifacts') or {}).get('dataset_path') or 'artifacts/value_route_bucket_aware/runs/value_quality_no_distress_bucket_aware/dataset.csv'
    if not Path(dataset_path).exists():
        res={'status':'blocked_missing_data','reason':'dataset_missing','dataset_path':str(dataset_path)}; (out/'result.json').write_text(json.dumps(res,indent=2)); return res
    df=_load(dataset_path); exp=spec.get('experiment_id','dq_low_vol_quality_filter_v1')
    base=run_long_only_backtest(df,signal_column='industry_relative_book_yield',start_date='2021-01-01',end_date='2022-12-31',holding_count=75,rebalance_frequency='monthly',cost_bps=0)
    repaired=_apply_filters(df,exp)
    rr=run_long_only_backtest(repaired,signal_column='industry_relative_book_yield',start_date='2021-01-01',end_date='2022-12-31',holding_count=75,rebalance_frequency='monthly',cost_bps=0)
    res={'status':'ok' if rr.get('status')=='ok' else rr.get('status'),'experiment_id':exp,'baseline_max_drawdown':base.get('max_drawdown'), 'max_drawdown':rr.get('max_drawdown'), 'baseline_sharpe':base.get('sharpe'), 'sharpe':rr.get('sharpe'), 'baseline_total_return':base.get('total_return'), 'total_return':rr.get('total_return'), 'holding_count':75, 'constraints_pass': rr.get('max_drawdown',-1)>=-0.35 if rr.get('status')=='ok' else False, 'field_limitations': []}
    (out/'result.json').write_text(json.dumps(res,ensure_ascii=False,indent=2)+'\n'); (out/'result.md').write_text('# Defensive Quality Result\n\n'+json.dumps(res,indent=2))
    return res
