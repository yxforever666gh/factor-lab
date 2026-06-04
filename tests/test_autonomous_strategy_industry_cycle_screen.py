import pandas as pd
from factor_lab.autonomous_strategy_industry_cycle_screen import build_industry_cycle_cheap_screen


def frame():
    rows=[]
    for ticker,pb,pe,fwd,cycle in [('cheap_pos',0.8,8,0.05,0.2),('cheap_neg',0.7,7,-0.05,-0.2),('exp_pos',2,20,-0.01,0.2),('exp_neg',2.2,22,0.01,-0.2)]:
        for day in range(12):
            rows.append({'date':pd.Timestamp('2020-01-01')+pd.Timedelta(days=day),'ticker':ticker,'industry':'i','pb':pb+(1 if ticker.startswith('cheap') and day<6 else 0),'pe_ttm':pe+(10 if ticker.startswith('cheap') and day<6 else 0),'forward_return_5d':fwd,'industry_return_60d':cycle,'industry_relative_pb':-0.1 if ticker.startswith('cheap') else 0.2})
    return pd.DataFrame(rows)


def test_industry_cycle_screen_blocks_without_derived_field():
    result=build_industry_cycle_cheap_screen(run_id='x',frame=frame().drop(columns=['industry_return_60d']),source_path='x',window=6,min_periods=6)
    assert result['overall_status']=='blocked'


def test_industry_cycle_screen_runs_bounded_candidates():
    result=build_industry_cycle_cheap_screen(run_id='x',frame=frame(),source_path='x',window=6,min_periods=6,min_usable_rows=1)
    names={c['candidate'] for c in result['candidate_results']}
    assert 'industry_return_60d_positive' in names
    assert 'industry_return_60d_top50_by_date' in names
    assert result['best_candidate'] is not None
    assert result['queue_write_allowed'] is False
