#!/usr/bin/env python3
from __future__ import annotations
import argparse,json
from pathlib import Path
import pandas as pd
from factor_lab.autonomous_strategy_industry_cycle_features import add_industry_cycle_features, build_industry_cycle_feature_derivation_report, write_industry_cycle_feature_derivation
ROOT=Path(__file__).resolve().parents[1]; ASL=ROOT/'artifacts/autonomous_strategy_lab'; CACHE=ROOT/'artifacts/tushare_cache/tushare_2016-09-09_2023-12-31_97.csv'
def main(argv=None):
 p=argparse.ArgumentParser(); p.add_argument('--run-id',required=True); p.add_argument('--feature-cache',default=str(CACHE)); p.add_argument('--output-dir',default=str(ASL)); p.add_argument('--window',type=int,default=60); p.add_argument('--min-periods',type=int,default=40); a=p.parse_args(argv)
 frame=pd.read_csv(a.feature_cache)
 featured=add_industry_cycle_features(frame,window=a.window,min_periods=a.min_periods)
 csv_path=Path(a.output_dir)/'industry_cycle_feature_frame.csv'
 report=build_industry_cycle_feature_derivation_report(run_id=a.run_id,frame=frame,source_path=str(Path(a.feature_cache).relative_to(ROOT)),feature_frame_path=str(csv_path),window=a.window,min_periods=a.min_periods)
 paths=write_industry_cycle_feature_derivation(report,featured,a.output_dir)
 print(json.dumps({'coverage_ratio':report['coverage_ratio'],'ready_for_industry_cycle_screen':report['ready_for_industry_cycle_screen'],'row_count':report['row_count'],'ticker_count':report['ticker_count'],'controlled_execution_allowed':report['controlled_execution_allowed'],'queue_write_allowed':report['queue_write_allowed'],'csv_path':str(paths['csv'].relative_to(ROOT))},ensure_ascii=False,indent=2)); return 0
if __name__=='__main__': raise SystemExit(main())
