import pandas as pd

from factor_lab.autonomous_strategy_industry_cycle_features import add_industry_cycle_features, build_industry_cycle_feature_derivation_report


def frame():
    rows=[]
    for industry in ['a','b']:
        for day in range(6):
            for ticker in [industry+'1', industry+'2']:
                rows.append({'date':pd.Timestamp('2020-01-01')+pd.Timedelta(days=day),'industry':industry,'ticker':ticker,'return_1d':0.01 if industry=='a' else -0.01})
    return pd.DataFrame(rows)


def test_add_industry_cycle_features_adds_rolling_industry_return():
    out=add_industry_cycle_features(frame(),window=3,min_periods=2)
    assert 'industry_return_1d' in out.columns
    assert 'industry_return_60d' in out.columns
    assert out['industry_return_60d'].notna().sum()>0
    last_a=out[(out['industry']=='a') & (out['date']==pd.Timestamp('2020-01-06'))]['industry_return_60d'].iloc[0]
    assert round(last_a,4)==0.03


def test_industry_cycle_feature_derivation_report_marks_ready_when_coverage_sufficient():
    report=build_industry_cycle_feature_derivation_report(run_id='x',frame=frame(),source_path='x.csv',feature_frame_path='out.csv',window=3,min_periods=2)
    assert report['ready_for_industry_cycle_screen'] is True
    assert report['queue_write_allowed'] is False
