#!/usr/bin/env python3
from __future__ import annotations
import argparse,json
from pathlib import Path
from factor_lab.autonomous_strategy_mechanism_request_pack import build_mechanism_researcher_request_pack, write_mechanism_researcher_request_pack
ROOT=Path(__file__).resolve().parents[1]; ASL=ROOT/'artifacts/autonomous_strategy_lab'
def main(argv=None):
 p=argparse.ArgumentParser(); p.add_argument('--run-id',required=True); p.add_argument('--output-dir',default=str(ASL)); a=p.parse_args(argv)
 req=json.loads((ASL/'new_mechanism_request.json').read_text())
 pack=build_mechanism_researcher_request_pack(run_id=a.run_id,new_mechanism_request=req)
 paths=write_mechanism_researcher_request_pack(pack,a.output_dir)
 print(json.dumps({'decision':pack['decision'],'worker_task_count':len(pack['worker_tasks']),'candidate_next_mechanism_families':pack['candidate_next_mechanism_families'],'controlled_execution_allowed':pack['controlled_execution_allowed'],'queue_write_allowed':pack['queue_write_allowed'],'json_path':str(paths['json'].relative_to(ROOT))},ensure_ascii=False,indent=2)); return 0
if __name__=='__main__': raise SystemExit(main())
