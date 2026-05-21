import json
from factor_lab.autonomous_research_loop_state import build_autonomous_research_loop_state

def test_state_missing_optional_artifacts_does_not_crash(tmp_path):
    state=build_autonomous_research_loop_state(root=tmp_path)
    assert state['runtime_mode'] in {'controlled_only_required','unknown'}
    assert state['current_blocker']

def test_state_preserves_drawdown_blocker_from_status(tmp_path):
    d=tmp_path/'artifacts/small_institutionalization'; d.mkdir(parents=True)
    (d/'status.json').write_text(json.dumps({'next_action':'repair_simulated_portfolio_construction','small_institutional_simulation':{'primary_issue':'drawdown_risk_too_high'}}))
    state=build_autonomous_research_loop_state(root=tmp_path)
    assert state['current_blocker']=='drawdown_risk_too_high'

def test_state_marks_controlled_only_when_pause_recommended(tmp_path):
    d=tmp_path/'artifacts'; d.mkdir()
    (d/'runtime_takeover_audit.json').write_text(json.dumps({'recommendations':['pause_broad_daemon','allow_controlled_only_daemon']}))
    state=build_autonomous_research_loop_state(root=tmp_path)
    assert state['runtime_mode']=='controlled_only_required'
