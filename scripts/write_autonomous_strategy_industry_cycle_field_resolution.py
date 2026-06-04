#!/usr/bin/env python3
from __future__ import annotations
import argparse,json
from pathlib import Path
import pandas as pd
from factor_lab.autonomous_strategy_industry_cycle_field_resolution import build_industry_cycle_field_resolution, write_industry_cycle_field_resolution
ROOT=Path(__file__).resolve().parents[1]; ASL=ROOT/'artifacts/autonomous_strategy_lab'; CACHE=ROOT/'artifacts/tushare_cache/tushare_2016-09-09_2023-12-31_97.csv'
def main(argv=None):
 p=argparse.ArgumentParser(); p.add_argument('--run-id',required=True); p.add_argument('--routes-v2',default=str(ROOT/'configs/autonomous_strategy_routes_v2.json')); p.add_argument('--feature-cache',default=str(CACHE)); p.add_argument('--output-dir',default=str(ASL)); a=p.parse_args(argv)
 reg=json.loads(Path(a.routes_v2).read_text()); fields=set(pd.read_csv(a.feature_cache,nrows=0).columns)
 report=build_industry_cycle_field_resolution(run_id=a.run_id,route_registry_v2=reg,available_fields=fields); paths=write_industry_cycle_field_resolution(report,a.output_dir)
 print(json.dumps({'decision':report['decision'],'route_id':report['route_id'],'ready_for_derivation_specs':report['ready_for_derivation_specs'],'field_statuses':{r['field']:r['resolution_status'] for r in report['field_resolutions']},'controlled_execution_allowed':report['controlled_execution_allowed'],'queue_write_allowed':report['queue_write_allowed'],'json_path':str(paths['json'].relative_to(ROOT))},ensure_ascii=False,indent=2)); return 0
if __name__=='__main__': raise SystemExit(main())
