from __future__ import annotations
from pathlib import Path
from typing import Any
ROOT=Path(__file__).resolve().parents[2]
def build_execution_manifest(plan:dict[str,Any], gate_decision:dict[str,Any], *, root: str|Path=ROOT, execution_mode:str|None=None)->dict[str,Any]:
    if gate_decision.get('decision')=='block': return {'schema_version':1,'cycle_id':plan.get('cycle_id'),'manifest_status':'blocked','experiments':[],'reasons':gate_decision.get('reasons',[])}
    mode=execution_mode or ('controlled_local' if gate_decision.get('decision')=='allow_controlled_execution' else 'dry_run')
    base=Path(root)/'artifacts/autonomous_research_loop'/plan['cycle_id']/ 'runs'
    ex=[]
    for e in plan.get('experiments',[]):
        out=base/e['experiment_id']
        ex.append({'cycle_id':plan['cycle_id'],'experiment_id':e['experiment_id'],'source_plan_path':str(Path(root)/'artifacts/autonomous_research_loop'/plan['cycle_id']/'cycle_plan.json'),'execution_mode':mode,'module':'factor_lab.defensive_quality_experiments.run_defensive_quality_experiment','input_artifacts':{'dataset_path':plan.get('dataset_path','artifacts/value_route_bucket_aware/runs/value_quality_no_distress_bucket_aware/dataset.csv')},'output_dir':str(out),'expected_output_files':[str(out/'result.json')],'timeout_seconds':int(e.get('max_runtime_minutes',20))*60,'admission':{'decision':gate_decision.get('decision')},'spec':e})
    return {'schema_version':1,'cycle_id':plan['cycle_id'],'manifest_status':'ready','execution_mode':mode,'experiments':ex}
