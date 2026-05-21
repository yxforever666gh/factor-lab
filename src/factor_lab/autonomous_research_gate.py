from __future__ import annotations
import json
from pathlib import Path
from typing import Any
from factor_lab.autonomous_research_loop_config import DEFAULT_AUTONOMOUS_RESEARCH_LOOP_CONFIG, load_autonomous_research_loop_config
from factor_lab.autonomous_research_cycle_plan import validate_cycle_plan
DEFAULT_AVAILABLE_FIELDS={'date','ticker','industry','close','return_1d','forward_return_5d','turnover','momentum_20','earnings_yield','book_yield','roe','size_inv','pe_ttm','pb','total_mv','volatility_20','volatility_60','industry_relative_book_yield','industry_relative_earnings_yield'}
def check_autonomous_research_cycle_gate(plan:dict[str,Any], *, config:dict[str,Any]|None=None, available_fields:set[str]|None=None, recent_experiment_ids:list[str]|None=None, allow_controlled_execution:bool=False)->dict[str,Any]:
    cfg=config or DEFAULT_AUTONOMOUS_RESEARCH_LOOP_CONFIG; reasons=[]
    v=validate_cycle_plan(plan,cfg); reasons.extend(v['reasons'])
    fields=available_fields or DEFAULT_AVAILABLE_FIELDS
    missing=sorted({f for e in plan.get('experiments',[]) for f in e.get('required_fields',[]) if f not in fields})
    if missing: reasons.append('missing_required_fields')
    recent=set(recent_experiment_ids or [])
    if any(e.get('experiment_id') in recent for e in plan.get('experiments',[])): reasons.append('duplicate_equivalent_experiment')
    if 'drawdown' not in ' '.join(plan.get('why_now') or []).lower() and plan.get('mainline')=='defensive_quality_risk_layer': reasons.append('current_blocker_not_prioritized')
    decision='block' if reasons else ('allow_controlled_execution' if allow_controlled_execution else 'allow_dry_run')
    return {'schema_version':1,'cycle_id':plan.get('cycle_id'),'decision':decision,'reasons':sorted(set(reasons)),'missing_required_fields':missing,'manual_approval_required':False}
def write_gate_decision(plan_path:str|Path, *, root: str|Path|None=None, allow_controlled_execution:bool=False)->dict[str,Any]:
    plan=json.loads(Path(plan_path).read_text(encoding='utf-8')); d=check_autonomous_research_cycle_gate(plan,allow_controlled_execution=allow_controlled_execution)
    out=Path(plan_path).parent/'gate_decision.json'; out.write_text(json.dumps(d,ensure_ascii=False,indent=2)+'\n'); (Path(plan_path).parent/'gate_decision.md').write_text(f"# Gate Decision\n\nDecision: {d['decision']}\nReasons: {d['reasons']}\n")
    return d
