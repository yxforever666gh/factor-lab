#!/usr/bin/env python3
from __future__ import annotations
import argparse,json
from pathlib import Path
from factor_lab.autonomous_strategy_mechanism_preview import build_mechanism_researcher_preview_response, write_mechanism_researcher_preview_response
ROOT=Path(__file__).resolve().parents[1]; ASL=ROOT/'artifacts/autonomous_strategy_lab'
def main(argv=None):
 p=argparse.ArgumentParser(); p.add_argument('--run-id',required=True); p.add_argument('--output-dir',default=str(ASL/'workers'/'worker_preview_next_mechanism')); a=p.parse_args(argv)
 pack=json.loads((ASL/'mechanism_researcher_request.json').read_text())
 resp=build_mechanism_researcher_preview_response(run_id=a.run_id,request_pack=pack)
 paths=write_mechanism_researcher_preview_response(resp,a.output_dir)
 print(json.dumps({'decision_recommendation':resp['decision_recommendation'],'candidate_count':len(resp['candidate_routes']),'controlled_execution_allowed':resp['controlled_execution_allowed'],'queue_write_allowed':resp['queue_write_allowed'],'json_path':str(paths['json'].relative_to(ROOT))},ensure_ascii=False,indent=2)); return 0
if __name__=='__main__': raise SystemExit(main())
