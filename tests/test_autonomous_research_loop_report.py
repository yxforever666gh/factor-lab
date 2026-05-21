import json
from factor_lab.autonomous_research_loop_report import write_autonomous_research_loop_report

def test_report_summarizes_latest_cycle(tmp_path):
    c=tmp_path/'artifacts/autonomous_research_loop/cycle_0001'; c.mkdir(parents=True)
    (tmp_path/'artifacts/autonomous_research_loop/latest_cycle.json').write_text(json.dumps({'cycle_id':'cycle_0001'}))
    (c/'cycle_plan.json').write_text(json.dumps({'cycle_id':'cycle_0001','mainline':'defensive_quality_risk_layer','experiments':[]}))
    (c/'gate_decision.json').write_text(json.dumps({'decision':'allow_dry_run'}))
    (c/'verdict.json').write_text(json.dumps({'verdict':'continue_same_mainline','next_action':'x','manual_approval_required':False}))
    out=write_autonomous_research_loop_report(root=tmp_path)
    assert out['latest_cycle_id']=='cycle_0001'
    assert (tmp_path/'artifacts/autonomous_research_loop/report.json').exists()
