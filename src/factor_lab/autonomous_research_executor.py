from __future__ import annotations
import json, time
from pathlib import Path
from typing import Any
from factor_lab.autonomous_research_execution_manifest import build_execution_manifest
from factor_lab.defensive_quality_experiments import run_defensive_quality_experiment
ROOT=Path(__file__).resolve().parents[2]
def run_autonomous_research_cycle(plan:dict[str,Any], gate_decision:dict[str,Any], *, root: str|Path=ROOT, dry_run:bool=False, allow_controlled_execution:bool=False, max_experiments:int|None=None)->dict[str,Any]:
    root=Path(root); cycle=plan.get('cycle_id','cycle_0001'); cdir=root/'artifacts/autonomous_research_loop'/cycle; cdir.mkdir(parents=True,exist_ok=True)
    if gate_decision.get('decision')=='block':
        m=build_execution_manifest(plan,gate_decision,root=root); (cdir/'execution_manifest.json').write_text(json.dumps(m,ensure_ascii=False,indent=2)+'\n'); return {'execution_status':'blocked','executed_count':0,'started_systemd_daemon':False}
    if dry_run or not allow_controlled_execution or gate_decision.get('decision')!='allow_controlled_execution':
        m=build_execution_manifest(plan, {'decision':'allow_dry_run'}, root=root, execution_mode='dry_run'); (cdir/'execution_manifest.json').write_text(json.dumps(m,ensure_ascii=False,indent=2)+'\n'); return {'execution_status':'dry_run','executed_count':0,'started_systemd_daemon':False,'manifest_path':str(cdir/'execution_manifest.json')}
    m=build_execution_manifest(plan,gate_decision,root=root,execution_mode='controlled_local')
    cap=min(3, int(max_experiments if max_experiments is not None else len(m['experiments'])))
    executed=0; statuses=[]
    for e in m['experiments'][:cap]:
        spec={**e.get('spec',{}), 'experiment_id':e['experiment_id'], 'dataset_path':e['input_artifacts']['dataset_path'], 'output_dir':e['output_dir']}
        t=time.time()
        try: res=run_defensive_quality_experiment(spec); status=res.get('status','unknown')
        except Exception as ex: res={'status':'failed','error':str(ex)}; status='failed'
        e['run_status']=status; e['runtime_seconds']=round(time.time()-t,3); e['result']=res; statuses.append(status); executed+=1
    m['executed_count']=executed; m['execution_status']='completed' if all(s=='ok' for s in statuses) else 'partial'
    (cdir/'execution_manifest.json').write_text(json.dumps(m,ensure_ascii=False,indent=2)+'\n')
    return {'execution_status':m['execution_status'],'executed_count':executed,'started_systemd_daemon':False,'manifest_path':str(cdir/'execution_manifest.json')}
