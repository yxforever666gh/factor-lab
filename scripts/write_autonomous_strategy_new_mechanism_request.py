#!/usr/bin/env python3
from __future__ import annotations
import argparse,json
from pathlib import Path
from factor_lab.autonomous_strategy_new_mechanism_request import build_new_mechanism_request, write_new_mechanism_request
ROOT=Path(__file__).resolve().parents[1]; ASL=ROOT/'artifacts/autonomous_strategy_lab'
def main(argv=None):
 p=argparse.ArgumentParser(); p.add_argument('--run-id',required=True); p.add_argument('--output-dir',default=str(ASL)); a=p.parse_args(argv)
 routes=json.loads((ROOT/'configs/autonomous_strategy_routes.json').read_text())
 stop=json.loads((ASL/'stop_route_state.json').read_text())
 distress=json.loads((ASL/'quality_cashflow_distress_route_verdict.json').read_text())
 report=build_new_mechanism_request(run_id=a.run_id,route_registry=routes,stop_route_state=stop,distress_route_verdict=distress)
 paths=write_new_mechanism_request(report,a.output_dir)
 print(json.dumps({'decision':report['decision'],'stopped_route_count':len(report['stopped_routes']),'candidate_next_mechanism_families':report['candidate_next_mechanism_families'],'controlled_execution_allowed':report['controlled_execution_allowed'],'queue_write_allowed':report['queue_write_allowed'],'json_path':str(paths['json'].relative_to(ROOT))},ensure_ascii=False,indent=2)); return 0
if __name__=='__main__': raise SystemExit(main())
