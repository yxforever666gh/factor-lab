import json

from factor_lab.opportunity_executor import enqueue_opportunities
from factor_lab import research_queue


def test_update_opportunity_runtime_health_triggers_timeout_cooldown(monkeypatch, tmp_path):
    monkeypatch.setattr(research_queue, 'OPPORTUNITY_RUNTIME_HEALTH_PATH', tmp_path / 'runtime.json')
    monkeypatch.setenv('RESEARCH_OPPORTUNITY_RUNTIME_WINDOW', '6')
    monkeypatch.setenv('RESEARCH_OPPORTUNITY_TIMEOUT_COOLDOWN_THRESHOLD', '3')

    task = {
        'task_type': 'generated_batch',
        'payload': {'opportunity_id': 'opp-timeout'},
    }

    research_queue.update_opportunity_runtime_health(task, status='failed', error_text='research task worker timeout after 300s')
    research_queue.update_opportunity_runtime_health(task, status='failed', error_text='research task worker timeout after 300s')
    updated = research_queue.update_opportunity_runtime_health(task, status='failed', error_text='research task worker timeout after 300s')

    assert updated is not None
    assert updated['cooldown_active'] is True
    assert updated['cooldown_reason'] == 'timeout_streak'


def test_enqueue_opportunities_skips_runtime_cooldown(monkeypatch, tmp_path):
    opportunities_path = tmp_path / 'research_opportunities.json'
    output_path = tmp_path / 'opportunity_execution_plan.json'
    runtime_path = tmp_path / 'opportunity_runtime_health.json'
    db_path = tmp_path / 'factor_lab.db'

    opportunities_path.write_text(
        json.dumps(
            {
                'opportunities': [
                    {
                        'opportunity_id': 'opp-cooldown',
                        'opportunity_type': 'recombine',
                        'priority': 0.9,
                        'novelty_score': 0.8,
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding='utf-8',
    )
    runtime_path.write_text(
        json.dumps(
            {
                'opportunities': {
                    'opp-cooldown': {
                        'cooldown_active': True,
                        'cooldown_reason': 'timeout_streak',
                    }
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding='utf-8',
    )

    monkeypatch.setattr('factor_lab.opportunity_executor.OPPORTUNITY_RUNTIME_HEALTH_PATH', runtime_path)
    monkeypatch.setattr('factor_lab.opportunity_executor.sync_opportunities', lambda opportunities: None)
    monkeypatch.setattr('factor_lab.opportunity_executor.update_opportunity_state', lambda *args, **kwargs: None)
    monkeypatch.setattr('factor_lab.opportunity_executor.build_opportunity_review', lambda: {'blocks': {}, 'downweights': {}})
    monkeypatch.setattr('factor_lab.opportunity_executor.should_bypass_recent_fingerprint', lambda opportunity: {'allow_bypass': False, 'reason': None})
    monkeypatch.setattr('factor_lab.opportunity_executor.recently_finished_same_fingerprint', lambda *args, **kwargs: False)
    monkeypatch.setattr('factor_lab.opportunity_executor._queue_counts', lambda store: {'validation': 0, 'exploration': 0})
    monkeypatch.setattr('factor_lab.opportunity_executor._queue_capacity', lambda: {'validation': 2, 'exploration': 2})
    monkeypatch.setattr('factor_lab.opportunity_executor._queue_backlog_targets', lambda: {'validation': 1, 'exploration': 1})
    monkeypatch.setattr(
        'factor_lab.opportunity_executor.map_opportunity_to_task',
        lambda opportunity: {
            'task_type': 'generated_batch',
            'priority': 40,
            'fingerprint': f"gen::{opportunity['opportunity_id']}",
            'worker_note': 'exploration｜test',
            'payload': {'opportunity_id': opportunity['opportunity_id']},
        },
    )

    payload = enqueue_opportunities(opportunities_path, output_path, db_path=db_path, limit=2, queue_aware=True)

    assert payload['injected_count'] == 0
    assert any(row['reason'] == 'runtime_cooldown:timeout_streak' for row in payload['skipped'])
