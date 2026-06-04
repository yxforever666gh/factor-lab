#!/usr/bin/env python3
from __future__ import annotations

import argparse, json
from pathlib import Path
import pandas as pd
from factor_lab.autonomous_strategy_distress_pit_preflight import build_distress_pit_preflight, write_distress_pit_preflight
ROOT=Path(__file__).resolve().parents[1]
ASL=ROOT/'artifacts/autonomous_strategy_lab'
PIT=ROOT/'artifacts/tushare_cache/pit_financial_2020-06-02_2023-12-28_77_96401d85299a_v2.csv'

def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--run-id',required=True)
    p.add_argument('--pit-cache',default=str(PIT))
    p.add_argument('--output-dir',default=str(ASL))
    a=p.parse_args(argv)
    field=json.loads((ASL/'quality_cashflow_distress_field_resolution.json').read_text())
    pit=pd.read_csv(a.pit_cache)
    report=build_distress_pit_preflight(run_id=a.run_id, field_resolution=field, pit_frame=pit)
    paths=write_distress_pit_preflight(report,a.output_dir)
    print(json.dumps({'decision':report['decision'],'ready_for_proxy_distress_screen':report['ready_for_proxy_distress_screen'],'ticker_count':report['ticker_count'],'row_count':report['row_count'],'controlled_execution_allowed':report['controlled_execution_allowed'],'queue_write_allowed':report['queue_write_allowed'],'json_path':str(paths['json'].relative_to(ROOT))},ensure_ascii=False,indent=2))
    return 0
if __name__=='__main__': raise SystemExit(main())
