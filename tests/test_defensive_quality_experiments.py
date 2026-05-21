import pandas as pd
from factor_lab.defensive_quality_experiments import generate_defensive_quality_experiments, run_defensive_quality_experiment

def test_generates_configured_experiments():
    specs=generate_defensive_quality_experiments({'experiments':[{'experiment_id':'a','required_fields':['roe']},{'experiment_id':'b','required_fields':['roe']}]}, available_fields={'roe'})
    assert [s['experiment_id'] for s in specs]==['a','b']

def test_blocks_unavailable_fields():
    specs=generate_defensive_quality_experiments({'experiments':[{'experiment_id':'a','required_fields':['missing']} ]}, available_fields={'roe'})
    assert specs[0]['status']=='blocked_missing_fields'

def test_output_records_mechanism_and_simulated_only(tmp_path):
    spec=generate_defensive_quality_experiments({'mechanism_id':'defensive_quality_risk_layer','hypothesis':'h','experiments':[{'experiment_id':'a','required_fields':['roe']} ]}, available_fields={'roe'})[0]
    assert spec['execution_type']=='simulated_portfolio_repair'
    assert spec['mechanism_id']=='defensive_quality_risk_layer'

def test_run_defensive_quality_experiment_writes_metrics(tmp_path):
    df=pd.DataFrame({'date':['2021-01-01','2021-01-01','2021-01-08','2021-01-08'],'ticker':['a','b','a','b'],'industry_relative_book_yield':[1,2,2,1],'forward_return_5d':[.01,-.02,.02,.01],'roe':[.1,.2,.1,.2],'volatility_20':[.1,.9,.1,.9],'pb':[1,4,1,4],'pe_ttm':[10,50,10,50],'earnings_yield':[.1,.02,.1,.02],'return_1d':[.01,.02,.01,.02],'total_mv':[1,2,1,2],'turnover':[1,1,1,1]})
    p=tmp_path/'d.csv'; df.to_csv(p,index=False)
    out=run_defensive_quality_experiment({'experiment_id':'x','dataset_path':str(p),'output_dir':str(tmp_path/'out')})
    assert out['status']=='ok'
    assert (tmp_path/'out/result.json').exists()
