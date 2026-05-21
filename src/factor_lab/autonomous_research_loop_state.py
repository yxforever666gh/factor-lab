from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from factor_lab.autonomous_research_loop_config import load_autonomous_research_loop_config
ROOT=Path(__file__).resolve().parents[2]
def _load(path:Path)->dict[str,Any]:
    try: return json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
    except Exception: return {}
def build_autonomous_research_loop_state(*, root: str|Path=ROOT)->dict[str,Any]:
    root=Path(root)
    cfg=load_autonomous_research_loop_config(root/'configs/autonomous_research_loop.json')
    status=_load(root/'artifacts/small_institutionalization/status.json')
    diag=_load(root/'artifacts/small_institutional_simulation/self_diagnosis.json')
    audit=_load(root/'artifacts/runtime_takeover_audit.json')
    dry=_load(root/'artifacts/controlled_restart_dry_run.json')
    latest=_load(root/'artifacts/autonomous_research_loop/latest_cycle.json')
    issue=(status.get('small_institutional_simulation') or {}).get('primary_issue') or diag.get('primary_issue') or 'drawdown_risk_too_high'
    recs=audit.get('recommendations') or []
    runtime='controlled_only_required' if 'pause_broad_daemon' in recs or 'allow_controlled_only_daemon' in recs else 'unknown'
    return {'schema_version':1,'generated_at_utc':datetime.now(timezone.utc).isoformat(),'config':cfg,'small_institutionalization_status':status,'simulation_self_diagnosis':diag,'runtime_audit':audit,'controlled_restart_dry_run':dry,'current_blocker':issue,'runtime_mode':runtime,'last_cycle_id':latest.get('cycle_id'),'last_verdict':latest.get('verdict')}
def write_state_snapshot(cycle_id:str, *, root: str|Path=ROOT)->Path:
    out=Path(root)/'artifacts/autonomous_research_loop'/cycle_id/'state_snapshot.json'; out.parent.mkdir(parents=True,exist_ok=True)
    out.write_text(json.dumps(build_autonomous_research_loop_state(root=root),ensure_ascii=False,indent=2)+'\n',encoding='utf-8'); return out
