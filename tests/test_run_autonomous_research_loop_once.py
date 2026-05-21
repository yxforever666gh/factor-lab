import json, pandas as pd
from scripts.run_autonomous_research_loop_once import run_loop_once

def test_dry_run_completes_artifacts(tmp_path):
    res=run_loop_once(root=tmp_path, dry_run=True)
    c=tmp_path/'artifacts/autonomous_research_loop/cycle_0001'
    assert res['cycle_status'] in {'dry_run_complete','complete'}
    assert (c/'cycle_plan.json').exists() and (c/'gate_decision.json').exists() and (c/'verdict.json').exists()

def test_latest_cycle_pointer_updated(tmp_path):
    run_loop_once(root=tmp_path, dry_run=True)
    assert json.loads((tmp_path/'artifacts/autonomous_research_loop/latest_cycle.json').read_text())['cycle_id']=='cycle_0001'

def test_controlled_mode_executes_under_fixture(tmp_path):
    df=pd.DataFrame({'date':['2021-01-01','2021-01-08']*60,'ticker':[f'{i:06d}.SZ' for i in range(120)],'industry_relative_book_yield':[i%10 for i in range(120)],'forward_return_5d':[.01]*120,'roe':[.1]*120,'pb':[1]*120,'pe_ttm':[10]*120,'earnings_yield':[.1]*120,'return_1d':[.01]*120,'total_mv':[1]*120,'turnover':[1]*120,'volatility_20':[.1]*120})
    p=tmp_path/'dataset.csv'; df.to_csv(p,index=False)
    res=run_loop_once(root=tmp_path, allow_controlled_execution=True, max_experiments=1, dataset_path=str(p))
    assert res['executed_count']==1
