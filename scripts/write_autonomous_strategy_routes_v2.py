#!/usr/bin/env python3
from __future__ import annotations
import argparse,json
from pathlib import Path
from factor_lab.autonomous_strategy_routes_v2 import build_route_registry_v2, write_route_registry_v2
ROOT=Path(__file__).resolve().parents[1]; PREVIEW=ROOT/'artifacts/autonomous_strategy_lab/workers/worker_preview_next_mechanism/factor_lab_mechanism_researcher_response.json'
def main(argv=None):
 p=argparse.ArgumentParser(); p.add_argument('--run-id',required=True); p.add_argument('--preview-response',default=str(PREVIEW)); p.add_argument('--config-dir',default=str(ROOT/'configs')); a=p.parse_args(argv)
 resp=json.loads(Path(a.preview_response).read_text())
 reg=build_route_registry_v2(run_id=a.run_id,preview_response=resp)
 paths=write_route_registry_v2(reg,config_dir=a.config_dir)
 print(json.dumps({'decision_recommendation':reg['decision_recommendation'],'route_count':len(reg['routes']),'top_route_id':reg['top_route_id'],'controlled_execution_allowed':reg['controlled_execution_allowed'],'queue_write_allowed':reg['queue_write_allowed'],'json_path':str(paths['json'].relative_to(ROOT))},ensure_ascii=False,indent=2)); return 0
if __name__=='__main__': raise SystemExit(main())
