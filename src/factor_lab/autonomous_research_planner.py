from __future__ import annotations
import json
from pathlib import Path
from typing import Any
from factor_lab.autonomous_research_cycle_plan import defensive_quality_plan_template
ROOT=Path(__file__).resolve().parents[2]
def _md(plan):
    return '# Autonomous Research Cycle Plan\n\n'+f"Cycle: {plan.get('cycle_id')}\nMainline: {plan.get('mainline')}\nQuestion: {plan.get('research_question')}\nExperiments: {len(plan.get('experiments') or [])}\n"
def _cycle_dir(root, cid):
    p=Path(root)/'artifacts/autonomous_research_loop'/cid; p.mkdir(parents=True,exist_ok=True); return p
def write_autonomous_research_cycle_plan(*, root: str|Path=ROOT, dry_run:bool=True, cycle_id:str='cycle_0001', dataset_path:str|None=None)->dict[str,Path]:
    d=_cycle_dir(root,cycle_id); plan=defensive_quality_plan_template(cycle_id,max_experiments=2,dataset_path=dataset_path)
    jp=d/'cycle_plan.json'; mp=d/'cycle_plan.md'; jp.write_text(json.dumps(plan,ensure_ascii=False,indent=2)+'\n',encoding='utf-8'); mp.write_text(_md(plan),encoding='utf-8')
    return {'plan_path':jp,'markdown_path':mp}
def write_next_autonomous_research_plan(verdict:dict[str,Any], *, root: str|Path=ROOT, previous_cycle_id:str='cycle_0001')->dict[str,Any]:
    n=int(previous_cycle_id.split('_')[-1])+1; cid=f'cycle_{n:04d}'; d=_cycle_dir(root,cid)
    if verdict.get('verdict')=='stop_no_information_gain':
        p={'schema_version':1,'cycle_id':cid,'plan_status':'stop','reason':'stop_no_information_gain','experiments':[]}
        (d/'next_plan.json').write_text(json.dumps(p,ensure_ascii=False,indent=2)+'\n'); (d/'next_plan.md').write_text('# Stop plan\n')
        return {'plan_status':'stop','plan_path':d/'next_plan.json'}
    plan=defensive_quality_plan_template(cid,max_experiments=1)
    if 'market' in str(verdict.get('best_experiment_id','')): plan['research_question']='Refine market-state defensive overlay after positive evidence.'; plan['experiments'][0]['experiment_id']='dq_market_state_refinement_v2'
    elif verdict.get('verdict')=='modify_experiment_design': plan['experiments'][0]['experiment_id']='dq_threshold_refinement_v2'
    (d/'next_plan.json').write_text(json.dumps(plan,ensure_ascii=False,indent=2)+'\n'); (d/'next_plan.md').write_text(_md(plan))
    return {'plan_status':'planned','plan_path':d/'next_plan.json','markdown_path':d/'next_plan.md'}
