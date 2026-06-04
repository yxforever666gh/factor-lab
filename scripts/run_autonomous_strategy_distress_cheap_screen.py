#!/usr/bin/env python3
from __future__ import annotations

import argparse, json
from pathlib import Path
import pandas as pd
from factor_lab.autonomous_strategy_distress_cheap_screen import build_distress_cheap_screen, write_distress_cheap_screen
ROOT=Path(__file__).resolve().parents[1]
ASL=ROOT/'artifacts/autonomous_strategy_lab'
FEATURE=ROOT/'artifacts/tushare_cache/tushare_2016-09-09_2023-12-31_97.csv'
COLS={'date','ticker','pb','pe_ttm','forward_return_5d','debt_to_asset','operating_cashflow_to_profit','roe'}

def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--run-id',required=True)
    p.add_argument('--feature-cache',default=str(FEATURE))
    p.add_argument('--output-dir',default=str(ASL))
    p.add_argument('--window',type=int,default=756)
    p.add_argument('--min-periods',type=int,default=756)
    p.add_argument('--max-drawdown-limit',type=float,default=-0.35)
    a=p.parse_args(argv)
    pre=json.loads((ASL/'quality_cashflow_distress_pit_preflight.json').read_text())
    frame=pd.read_csv(a.feature_cache,usecols=lambda c:c in COLS)
    screen=build_distress_cheap_screen(run_id=a.run_id, frame=frame, pit_preflight=pre, source_path=str(Path(a.feature_cache).relative_to(ROOT)), window=a.window, min_periods=a.min_periods, max_drawdown_limit=a.max_drawdown_limit)
    paths=write_distress_cheap_screen(screen,a.output_dir)
    best=screen.get('best_candidate') or {}
    print(json.dumps({'overall_status':screen['overall_status'],'recommended_next_step':screen['recommended_next_step'],'best_candidate':best.get('candidate'),'best_mean_daily_spread':best.get('mean_daily_spread'),'best_max_drawdown':best.get('max_drawdown'),'best_risk_pass':best.get('risk_pass'),'controlled_execution_allowed':screen['controlled_execution_allowed'],'queue_write_allowed':screen['queue_write_allowed'],'json_path':str(paths['json'].relative_to(ROOT))},ensure_ascii=False,indent=2))
    return 0
if __name__=='__main__': raise SystemExit(main())
