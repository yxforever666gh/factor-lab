import json
from factor_lab.autonomous_research_planner import write_next_autonomous_research_plan

def test_positive_market_state_evidence_generates_refinement(tmp_path):
    out=write_next_autonomous_research_plan({'verdict':'continue_same_mainline','best_experiment_id':'dq_market_state_de_risk_v1'}, root=tmp_path, previous_cycle_id='cycle_0001')
    p=json.loads(out['plan_path'].read_text())
    assert 'market' in p['research_question'].lower() or any('market' in e['experiment_id'] for e in p['experiments'])

def test_no_gain_does_not_generate_random_variants(tmp_path):
    out=write_next_autonomous_research_plan({'verdict':'stop_no_information_gain'}, root=tmp_path, previous_cycle_id='cycle_0001')
    assert out['plan_status']=='stop'
