import json, pandas as pd
from factor_lab.autonomous_research_cycle_plan import defensive_quality_plan_template
from factor_lab.autonomous_research_executor import run_autonomous_research_cycle

def _dataset(tmp_path):
    df=pd.DataFrame({'date':['2021-01-01','2021-01-08']*60,'ticker':[f'{i:06d}.SZ' for i in range(120)],'industry_relative_book_yield':[i%10 for i in range(120)],'forward_return_5d':[0.01 if i%2 else -0.005 for i in range(120)],'roe':[.1]*120,'pb':[1]*120,'pe_ttm':[10]*120,'earnings_yield':[.1]*120,'return_1d':[.01]*120,'total_mv':[1]*120,'turnover':[1]*120,'volatility_20':[.1]*120})
    p=tmp_path/'dataset.csv'; df.to_csv(p,index=False); return str(p)

def test_controlled_executor_runs_one_experiment(tmp_path):
    plan=defensive_quality_plan_template('cycle_0001'); plan['dataset_path']=_dataset(tmp_path)
    result=run_autonomous_research_cycle(plan, {'decision':'allow_controlled_execution'}, root=tmp_path, allow_controlled_execution=True, max_experiments=1)
    assert result['execution_status'] in {'completed','partial'}
    assert result['executed_count']==1

def test_more_than_three_not_executed(tmp_path):
    plan=defensive_quality_plan_template('cycle_0001'); plan['experiments']*=3; plan['dataset_path']=_dataset(tmp_path)
    result=run_autonomous_research_cycle(plan, {'decision':'allow_controlled_execution'}, root=tmp_path, allow_controlled_execution=True, max_experiments=99)
    assert result['executed_count']<=3

def test_no_systemd_daemon_is_started(tmp_path):
    plan=defensive_quality_plan_template('cycle_0001'); plan['dataset_path']=_dataset(tmp_path)
    result=run_autonomous_research_cycle(plan, {'decision':'allow_controlled_execution'}, root=tmp_path, allow_controlled_execution=True, max_experiments=1)
    assert result['started_systemd_daemon'] is False
