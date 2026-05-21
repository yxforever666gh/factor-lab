import json
from factor_lab.autonomous_research_planner import write_autonomous_research_cycle_plan, write_next_autonomous_research_plan

def test_first_cycle_plan_contains_defensive_quality(tmp_path):
    result=write_autonomous_research_cycle_plan(root=tmp_path, dry_run=True)
    plan=json.loads(result['plan_path'].read_text())
    assert plan['cycle_id']=='cycle_0001'
    assert plan['mainline']=='defensive_quality_risk_layer'
    assert len(plan['experiments'])==2

def test_plan_references_drawdown_blocker(tmp_path):
    result=write_autonomous_research_cycle_plan(root=tmp_path, dry_run=True)
    plan=json.loads(result['plan_path'].read_text())
    assert any('drawdown' in x for x in plan['why_now'])
    assert plan['manual_approval_required'] is False

def test_next_plan_increments_cycle_id(tmp_path):
    verdict={'verdict':'continue_same_mainline','best_evidence_class':'positive_progress'}
    out=write_next_autonomous_research_plan(verdict, root=tmp_path, previous_cycle_id='cycle_0001')
    assert json.loads(out['plan_path'].read_text())['cycle_id']=='cycle_0002'
