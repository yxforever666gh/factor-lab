from __future__ import annotations
import json
from pathlib import Path
from typing import Any
ROOT=Path(__file__).resolve().parents[2]
def _load_result(output_dir):
    p=Path(output_dir)/'result.json'
    if not p.exists(): return None
    try: return json.loads(p.read_text(encoding='utf-8'))
    except Exception: return None
def _classify(r):
    if not r: return 'execution_failure'
    if r.get('status')=='blocked_missing_data': return 'blocked_missing_data'
    if r.get('status')!='ok': return 'execution_failure'
    before=r.get('baseline_max_drawdown'); after=r.get('max_drawdown')
    if before is not None and after is not None and float(after)-float(before)>=0.05: return 'positive_progress'
    if before is not None and after is not None: return 'negative_but_informative'
    return 'execution_failure'
def build_evidence_ledger(manifest:dict[str,Any])->dict[str,Any]:
    ev=[]
    for e in manifest.get('experiments',[]):
        r=_load_result(e.get('output_dir',''))
        cls=_classify(r)
        row={'experiment_id':e.get('experiment_id'),'status':(r or {}).get('status','missing_result'),'information_gain_class':cls,'metrics_available':bool(r)}
        if r: row.update({k:r.get(k) for k in ['max_drawdown','baseline_max_drawdown','sharpe','total_return','holding_count','constraints_pass','field_limitations']})
        ev.append(row)
    return {'schema_version':1,'cycle_id':manifest.get('cycle_id'),'evidence':ev,'summary':{'evidence_count':len(ev),'positive_progress_count':sum(1 for x in ev if x['information_gain_class']=='positive_progress')}}
def write_evidence_ledger(*, root: str|Path=ROOT, cycle_id:str='cycle_0001')->dict[str,Any]:
    d=Path(root)/'artifacts/autonomous_research_loop'/cycle_id
    manifest=json.loads((d/'execution_manifest.json').read_text()) if (d/'execution_manifest.json').exists() else {'cycle_id':cycle_id,'experiments':[]}
    led=build_evidence_ledger(manifest); (d/'evidence_ledger.json').write_text(json.dumps(led,ensure_ascii=False,indent=2)+'\n'); (d/'evidence_ledger.md').write_text('# Evidence Ledger\n\n'+json.dumps(led,indent=2))
    return led
