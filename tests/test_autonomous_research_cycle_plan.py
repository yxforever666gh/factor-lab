import pytest
from factor_lab.autonomous_research_cycle_plan import validate_cycle_plan, defensive_quality_plan_template
from factor_lab.autonomous_research_loop_config import DEFAULT_AUTONOMOUS_RESEARCH_LOOP_CONFIG

def test_valid_defensive_quality_plan_passes():
    plan=defensive_quality_plan_template('cycle_0001', max_experiments=2)
    assert validate_cycle_plan(plan, DEFAULT_AUTONOMOUS_RESEARCH_LOOP_CONFIG)['valid'] is True

def test_unsupported_mainline_rejected():
    plan=defensive_quality_plan_template('cycle_0001'); plan['mainline']='random'
    assert 'unsupported_mainline' in validate_cycle_plan(plan, DEFAULT_AUTONOMOUS_RESEARCH_LOOP_CONFIG)['reasons']

def test_too_many_experiments_rejected():
    plan=defensive_quality_plan_template('cycle_0001'); plan['experiments'] *= 3
    assert 'too_many_experiments' in validate_cycle_plan(plan, DEFAULT_AUTONOMOUS_RESEARCH_LOOP_CONFIG)['reasons']

def test_missing_hypothesis_or_falsification_rejected():
    plan=defensive_quality_plan_template('cycle_0001'); plan['hypothesis']=''; plan['experiments'][0]['falsification_criteria']=[]
    reasons=validate_cycle_plan(plan, DEFAULT_AUTONOMOUS_RESEARCH_LOOP_CONFIG)['reasons']
    assert 'missing_hypothesis' in reasons and 'missing_falsification_criteria' in reasons

def test_live_trading_request_rejected():
    plan=defensive_quality_plan_template('cycle_0001'); plan['live_trading_enabled']=True
    assert 'live_trading_requested' in validate_cycle_plan(plan, DEFAULT_AUTONOMOUS_RESEARCH_LOOP_CONFIG)['reasons']
