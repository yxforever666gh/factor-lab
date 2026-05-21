from __future__ import annotations
import json
from pathlib import Path
from typing import Any
ROOT=Path(__file__).resolve().parents[2]
def write_verdict_payload(ledger:dict[str,Any], *, previous_no_gain_count:int=0, max_drawdown_limit:float=-0.35)->dict[str,Any]:
    ev=ledger.get('evidence') or []
    if any(e.get('information_gain_class')=='blocked_missing_data' for e in ev): verdict='blocked_needs_data_or_manual_review'; action='manual_review_data_blocker'; manual=True
    else:
        best=min([float(e.get('max_drawdown')) for e in ev if e.get('max_drawdown') is not None], default=None)
        pass_rows=[e for e in ev if e.get('max_drawdown') is not None and float(e['max_drawdown'])>=max_drawdown_limit and e.get('constraints_pass',True)]
        if pass_rows: verdict='promote_to_paper_review_manual_approval'; action='manual_review_before_paper_promotion'; manual=True
        elif any(e.get('information_gain_class')=='positive_progress' for e in ev): verdict='continue_same_mainline'; action='write_next_autonomous_research_plan'; manual=False
        elif previous_no_gain_count>=1: verdict='stop_no_information_gain'; action='stop_or_manual_review'; manual=True
        else: verdict='modify_experiment_design'; action='narrow_or_modify_experiment_design'; manual=False
    best_exp=next((e.get('experiment_id') for e in ev if e.get('information_gain_class')=='positive_progress'), None)
    return {'schema_version':1,'cycle_id':ledger.get('cycle_id'),'verdict':verdict,'next_action':action,'manual_approval_required':manual,'best_experiment_id':best_exp,'best_evidence_class':next((e.get('information_gain_class') for e in ev),None)}
def write_verdict(*, root: str|Path=ROOT, cycle_id:str='cycle_0001')->dict[str,Any]:
    d=Path(root)/'artifacts/autonomous_research_loop'/cycle_id; led=json.loads((d/'evidence_ledger.json').read_text()) if (d/'evidence_ledger.json').exists() else {'cycle_id':cycle_id,'evidence':[]}
    v=write_verdict_payload(led); (d/'verdict.json').write_text(json.dumps(v,ensure_ascii=False,indent=2)+'\n'); (d/'verdict.md').write_text('# Verdict\n\n'+json.dumps(v,indent=2))
    return v
