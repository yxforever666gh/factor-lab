import json
from scripts.run_harvest_agent_once import run_harvest_agent_loop, run_harvest_agent_once


def test_run_harvest_agent_once_dry_run_writes_complete_cycle(tmp_path):
    out = run_harvest_agent_once(root=tmp_path, dry_run=True)
    c = tmp_path / 'artifacts/harvest_agent/cycle_0001'
    assert out['cycle_id'] == 'cycle_0001'
    for name in [
        'state_snapshot.json', 'cycle_charter.json', 'proposals.json', 'reviewer_decision.json',
        'gate_decision.json', 'execution_manifest.json', 'evidence_ledger.json', 'verdict.json',
        'knowledge_update.json', 'next_cycle_plan.json'
    ]:
        assert (c / name).exists(), name
    latest = json.loads((tmp_path / 'artifacts/harvest_agent/latest_cycle.json').read_text())
    assert latest['cycle_id'] == 'cycle_0001'
    manifest = json.loads((c / 'execution_manifest.json').read_text())
    assert manifest['execution_mode'] == 'dry_run'
    assert out['started_systemd_daemon'] is False


def test_controlled_execution_is_explicit_and_capped_to_one(tmp_path):
    out = run_harvest_agent_once(root=tmp_path, allow_controlled_execution=True, max_experiments=5)
    manifest = json.loads((tmp_path / 'artifacts/harvest_agent/cycle_0001/execution_manifest.json').read_text())
    assert manifest['execution_mode'] == 'controlled_local'
    assert manifest['executed_count'] <= 1
    assert out['scheduled_timer_enabled'] is False


def test_harvest_agent_loop_chains_next_cycle_until_budget_or_stop(tmp_path):
    out = run_harvest_agent_loop(root=tmp_path, cycles=2, allow_controlled_execution=True, max_experiments=1)

    assert out['loop_status'] == 'complete'
    assert out['cycles_run'] == 2
    assert [c['cycle_id'] for c in out['cycles']] == ['cycle_0001', 'cycle_0002']
    assert out['cycles'][0]['executed_count'] == 1
    assert out['cycles'][1]['executed_count'] == 1
    latest = json.loads((tmp_path / 'artifacts/harvest_agent/latest_cycle.json').read_text())
    assert latest['cycle_id'] == 'cycle_0002'
    assert out['scheduled_timer_enabled'] is False
    assert out['started_systemd_daemon'] is False


def test_harvest_agent_loop_stops_on_manual_review(tmp_path, monkeypatch):
    import scripts.run_harvest_agent_once as runner

    original_verdict = runner._verdict

    def manual_review_verdict(cycle_id, evidence):
        verdict = original_verdict(cycle_id, evidence)
        verdict['manual_approval_required'] = True
        verdict['decision'] = 'manual_review_required'
        verdict['next_action'] = 'wait for human review'
        return verdict

    monkeypatch.setattr(runner, '_verdict', manual_review_verdict)
    out = run_harvest_agent_loop(root=tmp_path, cycles=3, allow_controlled_execution=True, max_experiments=1)

    assert out['cycles_run'] == 1
    assert out['loop_status'] == 'stopped_manual_review'
    assert out['cycles'][0]['manual_approval_required'] is True
