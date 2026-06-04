#!/usr/bin/env python3
from __future__ import annotations
import argparse,json
from pathlib import Path
import pandas as pd
from factor_lab.autonomous_strategy_industry_cycle_screen import build_industry_cycle_cheap_screen, write_industry_cycle_cheap_screen
ROOT=Path(__file__).resolve().parents[1]; ASL=ROOT/'artifacts/autonomous_strategy_lab'; FRAME=ASL/'industry_cycle_feature_frame.csv'
def main(argv=None):
 p=argparse.ArgumentParser(); p.add_argument('--run-id',required=True); p.add_argument('--feature-frame',default=str(FRAME)); p.add_argument('--output-dir',default=str(ASL)); p.add_argument('--window',type=int,default=756); p.add_argument('--min-periods',type=int,default=756); p.add_argument('--max-drawdown-limit',type=float,default=-0.35); a=p.parse_args(argv)
 frame=pd.read_csv(a.feature_frame)
 screen=build_industry_cycle_cheap_screen(run_id=a.run_id,frame=frame,source_path=str(Path(a.feature_frame).relative_to(ROOT)),window=a.window,min_periods=a.min_periods,max_drawdown_limit=a.max_drawdown_limit)
 paths=write_industry_cycle_cheap_screen(screen,a.output_dir); best=screen.get('best_candidate') or {}
 print(json.dumps({'overall_status':screen['overall_status'],'recommended_next_step':screen['recommended_next_step'],'best_candidate':best.get('candidate'),'best_mean_daily_spread':best.get('mean_daily_spread'),'best_max_drawdown':best.get('max_drawdown'),'best_risk_pass':best.get('risk_pass'),'controlled_execution_allowed':screen['controlled_execution_allowed'],'queue_write_allowed':screen['queue_write_allowed'],'json_path':str(paths['json'].relative_to(ROOT))},ensure_ascii=False,indent=2)); return 0
if __name__=='__main__': raise SystemExit(main())
