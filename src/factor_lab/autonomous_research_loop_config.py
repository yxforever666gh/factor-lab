from __future__ import annotations
import copy, json
from pathlib import Path
from typing import Any
ROOT=Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH=ROOT/'configs/autonomous_research_loop.json'
DEFAULT_AUTONOMOUS_RESEARCH_LOOP_CONFIG={
 'schema_version':1,'enabled':True,'mode':'dry_run_first','primary_mainline':'defensive_quality_risk_layer','allowed_mainlines':['defensive_quality_risk_layer'],'live_trading_enabled':False,'broad_daemon_restore_allowed':False,'max_experiments_per_cycle':3,'default_experiments_per_cycle':2,'cycle_budget':{'max_cycles_per_day':4,'cooldown_minutes':180},'risk_targets':{'max_drawdown_limit':-0.35,'min_holding_count':50,'max_holding_count':100,'max_single_position_weight':0.02,'min_sharpe_preference':0.8},'manual_approval_required_for':['broad_daemon_restore','increase_budget','new_external_data_source','paper_portfolio_promotion','live_trading']}
def _merge(a,b):
    r=copy.deepcopy(a)
    for k,v in (b or {}).items(): r[k]=_merge(r[k],v) if isinstance(v,dict) and isinstance(r.get(k),dict) else v
    return r
def load_autonomous_research_loop_config(path: str|Path=DEFAULT_CONFIG_PATH, *, allow_unsafe_test_override: bool=False)->dict[str,Any]:
    p=Path(path); overrides={}
    if p.exists():
        overrides=json.loads(p.read_text(encoding='utf-8'))
    cfg=_merge(DEFAULT_AUTONOMOUS_RESEARCH_LOOP_CONFIG, overrides)
    if cfg.get('live_trading_enabled'): raise ValueError('live_trading_enabled is forbidden')
    if cfg.get('broad_daemon_restore_allowed') and not allow_unsafe_test_override: raise ValueError('broad_daemon_restore_allowed is forbidden')
    cfg['live_trading_enabled']=False; cfg['broad_daemon_restore_allowed']=False
    cfg['max_experiments_per_cycle']=min(3, int(cfg.get('max_experiments_per_cycle') or 3))
    if cfg.get('primary_mainline') not in cfg.get('allowed_mainlines',[]): raise ValueError('primary_mainline not allowed')
    return cfg
