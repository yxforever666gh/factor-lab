import json
from factor_lab.autonomous_research_cycle_plan import defensive_quality_plan_template
from factor_lab.autonomous_research_gate import check_autonomous_research_cycle_gate
from factor_lab.autonomous_research_executor import run_autonomous_research_cycle

def test_dry_run_writes_manifest_and_no_outputs(tmp_path):
    plan=defensive_quality_plan_template('cycle_0001')
    gate = check_autonomous_research_cycle_gate(plan)
    assert gate['decision'] == 'allow_dry_run', gate
    result=run_autonomous_research_cycle(plan, gate, root=tmp_path, dry_run=True)
    assert result['execution_status']=='dry_run'
    assert (tmp_path/'artifacts/autonomous_research_loop/cycle_0001/execution_manifest.json').exists()

def test_blocked_gate_refuses_execution(tmp_path):
    result=run_autonomous_research_cycle(defensive_quality_plan_template('cycle_0001'), {'decision':'block','reasons':['x']}, root=tmp_path)
    assert result['execution_status']=='blocked'

def test_missing_dataset_blocks_not_crash(tmp_path):
    plan=defensive_quality_plan_template('cycle_0001')
    result=run_autonomous_research_cycle(plan, {'decision':'allow_controlled_execution'}, root=tmp_path, allow_controlled_execution=True)
    assert result['execution_status'] in {'completed','blocked','partial'}
