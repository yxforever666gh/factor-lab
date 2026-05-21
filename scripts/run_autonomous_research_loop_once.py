#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
from typing import Any
from factor_lab.autonomous_research_loop_state import write_state_snapshot
from factor_lab.autonomous_research_planner import write_autonomous_research_cycle_plan, write_next_autonomous_research_plan
from factor_lab.autonomous_research_gate import check_autonomous_research_cycle_gate
from factor_lab.autonomous_research_executor import run_autonomous_research_cycle
from factor_lab.autonomous_research_evidence import write_evidence_ledger
from factor_lab.autonomous_research_verdict import write_verdict
ROOT=Path(__file__).resolve().parents[1]
def run_loop_once(*, root: str|Path=ROOT, dry_run:bool=False, allow_controlled_execution:bool=False, max_experiments:int|None=None, dataset_path:str|None=None)->dict[str,Any]:
    root=Path(root); cid='cycle_0001'; base=root/'artifacts/autonomous_research_loop'; cdir=base/cid; cdir.mkdir(parents=True,exist_ok=True)
    write_state_snapshot(cid, root=root)
    pr=write_autonomous_research_cycle_plan(root=root,dry_run=dry_run,cycle_id=cid,dataset_path=dataset_path)
    plan=json.loads(Path(pr['plan_path']).read_text())
    gate=check_autonomous_research_cycle_gate(plan,allow_controlled_execution=allow_controlled_execution)
    (cdir/'gate_decision.json').write_text(json.dumps(gate,ensure_ascii=False,indent=2)+'\n'); (cdir/'gate_decision.md').write_text(f"# Gate Decision\n\nDecision: {gate['decision']}\n")
    ex=run_autonomous_research_cycle(plan,gate,root=root,dry_run=dry_run or not allow_controlled_execution,allow_controlled_execution=allow_controlled_execution,max_experiments=max_experiments)
    write_evidence_ledger(root=root,cycle_id=cid)
    verdict=write_verdict(root=root,cycle_id=cid)
    write_next_autonomous_research_plan(verdict,root=root,previous_cycle_id=cid)
    latest={'cycle_id':cid,'cycle_status':'dry_run_complete' if ex['execution_status']=='dry_run' else 'complete','verdict':verdict.get('verdict'),'next_action':verdict.get('next_action')}
    base.mkdir(parents=True,exist_ok=True); (base/'latest_cycle.json').write_text(json.dumps(latest,ensure_ascii=False,indent=2)+'\n')
    k=root/'knowledge'; k.mkdir(exist_ok=True); (k/'autonomous_research_loop.md').write_text(f"# Autonomous Research Loop\n\nLatest cycle: {cid}\nVerdict: {verdict.get('verdict')}\nNext action: {verdict.get('next_action')}\n",encoding='utf-8')
    return {**latest,'executed_count':ex.get('executed_count',0),'artifacts_dir':str(cdir)}
if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--dry-run',action='store_true'); ap.add_argument('--allow-controlled-execution',action='store_true'); ap.add_argument('--max-experiments',type=int); ap.add_argument('--dataset-path')
    a=ap.parse_args(); print(json.dumps(run_loop_once(dry_run=a.dry_run,allow_controlled_execution=a.allow_controlled_execution,max_experiments=a.max_experiments,dataset_path=a.dataset_path),ensure_ascii=False,indent=2))
