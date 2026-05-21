from pathlib import Path
from factor_lab.autonomous_research_cycle_plan import defensive_quality_plan_template
from factor_lab.autonomous_research_execution_manifest import build_execution_manifest

def test_manifest_generated_from_allowed_plan(tmp_path):
    plan=defensive_quality_plan_template('cycle_0001')
    m=build_execution_manifest(plan, {'decision':'allow_dry_run'}, root=tmp_path)
    assert len(m['experiments'])==2 and m['execution_mode']=='dry_run'

def test_manifest_refuses_blocked_gate(tmp_path):
    m=build_execution_manifest(defensive_quality_plan_template('cycle_0001'), {'decision':'block'}, root=tmp_path)
    assert m['manifest_status']=='blocked'

def test_manifest_paths_under_cycle_runs(tmp_path):
    m=build_execution_manifest(defensive_quality_plan_template('cycle_0001'), {'decision':'allow_dry_run'}, root=tmp_path)
    assert all(str(tmp_path/'artifacts/autonomous_research_loop/cycle_0001/runs') in e['output_dir'] for e in m['experiments'])
