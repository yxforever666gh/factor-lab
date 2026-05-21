from __future__ import annotations
import json
from pathlib import Path
from typing import Any
ROOT=Path(__file__).resolve().parents[2]
def _load(p):
    try: return json.loads(Path(p).read_text()) if Path(p).exists() else {}
    except Exception: return {}
def write_autonomous_research_loop_report(*, root: str|Path=ROOT)->dict[str,Any]:
    root=Path(root); base=root/'artifacts/autonomous_research_loop'; latest=_load(base/'latest_cycle.json'); cid=latest.get('cycle_id') or 'cycle_0001'; d=base/cid
    plan=_load(d/'cycle_plan.json') or _load(d/'next_plan.json'); gate=_load(d/'gate_decision.json'); manifest=_load(d/'execution_manifest.json'); verdict=_load(d/'verdict.json')
    ev=_load(d/'evidence_ledger.json')
    report={'schema_version':1,'latest_cycle_id':cid,'latest_mainline':plan.get('mainline'),'latest_plan_summary':{'research_question':plan.get('research_question'),'experiment_count':len(plan.get('experiments') or [])},'gate_decision':gate.get('decision'),'executed_experiments':manifest.get('executed_count',0),'current_best_drawdown':min([e.get('max_drawdown') for e in ev.get('evidence',[]) if e.get('max_drawdown') is not None], default=None),'verdict':verdict.get('verdict'),'next_action':verdict.get('next_action'),'manual_approval_required':bool(verdict.get('manual_approval_required'))}
    base.mkdir(parents=True,exist_ok=True); (base/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n'); (base/'report.md').write_text('# Autonomous Research Loop Report\n\n'+json.dumps(report,indent=2))
    return report
