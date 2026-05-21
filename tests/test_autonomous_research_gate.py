from factor_lab.autonomous_research_cycle_plan import defensive_quality_plan_template
from factor_lab.autonomous_research_gate import check_autonomous_research_cycle_gate

def test_defensive_quality_dry_run_allowed():
    d=check_autonomous_research_cycle_gate(defensive_quality_plan_template('cycle_0001'))
    assert d['decision']=='allow_dry_run'

def test_controlled_execution_allowed_when_requested():
    d=check_autonomous_research_cycle_gate(defensive_quality_plan_template('cycle_0001'), allow_controlled_execution=True)
    assert d['decision']=='allow_controlled_execution'

def test_missing_field_blocks():
    p=defensive_quality_plan_template('cycle_0001'); p['experiments'][0]['required_fields'].append('not_a_field')
    d=check_autonomous_research_cycle_gate(p, available_fields={'roe'})
    assert d['decision']=='block' and 'missing_required_fields' in d['reasons']

def test_duplicate_blocks():
    p=defensive_quality_plan_template('cycle_0001')
    d=check_autonomous_research_cycle_gate(p, recent_experiment_ids=[p['experiments'][0]['experiment_id']])
    assert d['decision']=='block' and 'duplicate_equivalent_experiment' in d['reasons']

def test_live_trading_blocks():
    p=defensive_quality_plan_template('cycle_0001'); p['live_trading_enabled']=True
    assert check_autonomous_research_cycle_gate(p)['decision']=='block'
