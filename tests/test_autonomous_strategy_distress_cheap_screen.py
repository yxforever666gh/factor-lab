from __future__ import annotations

import pandas as pd

from factor_lab.autonomous_strategy_distress_cheap_screen import build_distress_cheap_screen


def frame():
    rows=[]
    for ticker,pb,pe,fwd,debt,cf,roe in [("cheap_good",0.8,8,0.05,0.2,1.0,0.2),("cheap_bad",0.7,7,-0.05,0.9,-1.0,0.01),("exp",2.0,20,-0.01,0.3,0.5,0.1)]:
        for day in range(12):
            rows.append({"date":pd.Timestamp('2020-01-01')+pd.Timedelta(days=day),"ticker":ticker,"pb":pb+(1 if ticker.startswith('cheap') and day<6 else 0),"pe_ttm":pe+(10 if ticker.startswith('cheap') and day<6 else 0),"forward_return_5d":fwd,"debt_to_asset":debt,"operating_cashflow_to_profit":cf,"roe":roe})
    return pd.DataFrame(rows)


def test_distress_cheap_screen_blocks_when_preflight_not_ready():
    result=build_distress_cheap_screen(run_id='x',frame=frame(),pit_preflight={"ready_for_proxy_distress_screen":False},source_path='x',window=6,min_periods=6)
    assert result['overall_status']=='blocked'
    assert result['queue_write_allowed'] is False


def test_distress_cheap_screen_runs_candidates_when_preflight_ready():
    result=build_distress_cheap_screen(run_id='x',frame=frame(),pit_preflight={"ready_for_proxy_distress_screen":True},source_path='x',window=6,min_periods=6,min_usable_rows=1)
    names={c['candidate'] for c in result['candidate_results']}
    assert 'baseline' in names
    assert 'combined_debt_cashflow_roe_proxy_filter' in names
    assert result['best_candidate'] is not None
    assert result['controlled_execution_allowed'] is False
