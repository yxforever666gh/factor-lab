#!/usr/bin/env python3
from __future__ import annotations
import argparse,json
from pathlib import Path
from factor_lab.autonomous_strategy_distress_route_verdict import build_distress_route_verdict, write_distress_route_verdict
ROOT=Path(__file__).resolve().parents[1]; ASL=ROOT/'artifacts/autonomous_strategy_lab'
def main(argv=None):
 p=argparse.ArgumentParser(); p.add_argument('--run-id',required=True); p.add_argument('--output-dir',default=str(ASL)); a=p.parse_args(argv)
 pre=json.loads((ASL/'quality_cashflow_distress_pit_preflight.json').read_text()); screen=json.loads((ASL/'quality_cashflow_distress_cheap_screen.json').read_text())
 v=build_distress_route_verdict(run_id=a.run_id,pit_preflight=pre,distress_screen=screen); paths=write_distress_route_verdict(v,a.output_dir)
 print(json.dumps({'verdict':v['verdict'],'reason_codes':v['reason_codes'],'controlled_execution_allowed':v['controlled_execution_allowed'],'queue_write_allowed':v['queue_write_allowed'],'json_path':str(paths['json'].relative_to(ROOT))},ensure_ascii=False,indent=2)); return 0
if __name__=='__main__': raise SystemExit(main())
