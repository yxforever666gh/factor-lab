import json
from factor_lab.harvest_knowledge import build_knowledge_update, update_harvest_knowledge


def test_knowledge_update_writes_local_files_without_raw_result_dump(tmp_path):
    c = tmp_path / 'artifacts/harvest_agent/cycle_0001'
    c.mkdir(parents=True)
    (c / 'verdict.json').write_text(json.dumps({
        'cycle_id': 'cycle_0001',
        'decision': 'continue_same_mainline',
        'reasoning': ['cost-adjusted evidence incomplete'],
        'next_action': 'run stricter tail-risk validation',
        'manual_approval_required': False,
    }))
    (c / 'evidence_ledger.json').write_text(json.dumps({
        'cycle_id': 'cycle_0001',
        'evidence': [{
            'experiment_id': 'value_quality_cost_sensitivity_v1',
            'mechanism_id': 'value_quality_no_distress',
            'information_gain': 'positive_progress',
            'failure_class': None,
            'metrics': {'rank_ic_mean': 0.03, 'sharpe_net': 0.8},
        }]
    }))
    out = update_harvest_knowledge(root=tmp_path, cycle_id='cycle_0001')
    assert out['cycle_id'] == 'cycle_0001'
    assert (tmp_path / 'knowledge/harvest_agent.md').exists()
    assert (tmp_path / 'knowledge/mechanism_lessons.md').exists()
    assert (tmp_path / 'knowledge/research_waste.md').exists()
    assert json.loads((tmp_path / 'knowledge/data_blockers.json').read_text())['schema_version'] == 1
    text = (tmp_path / 'knowledge/harvest_agent.md').read_text()
    assert 'Latest durable conclusion' in text
    assert 'rank_ic_mean' not in text  # raw metric dumps stay in artifacts, not knowledge notes


def test_knowledge_classifies_data_blockers_and_waste(tmp_path):
    update = build_knowledge_update(
        verdict={'cycle_id': 'cycle_0002', 'decision': 'block_route', 'next_action': 'stop'},
        evidence={'evidence': [
            {'experiment_id': 'x', 'mechanism_id': 'm', 'failure_class': 'missing_required_fields', 'information_gain': 'blocked_missing_data'},
            {'experiment_id': 'dup', 'mechanism_id': 'm', 'failure_class': 'duplicate_equivalent_experiment', 'information_gain': 'duplicate_or_low_information'},
        ]},
    )
    assert update['data_blockers'][0]['failure_class'] == 'missing_required_fields'
    assert update['research_waste'][0]['experiment_id'] == 'dup'
