from factor_lab import adaptive_scheduler
from factor_lab import user_idle_detector


def test_user_idle_snapshot_interactive(monkeypatch):
    monkeypatch.setattr(
        user_idle_detector,
        'session_properties',
        lambda session_id=None: {
            'available': True,
            'session_id': 'c1',
            'active': True,
            'idle_hint': False,
            'locked_hint': False,
            'remote': False,
            'type': 'x11',
            'state': 'active',
        },
    )
    monkeypatch.setattr(user_idle_detector, 'get_user_idle_seconds', lambda: 12.0)
    snapshot = user_idle_detector.user_idle_snapshot()
    assert snapshot['mode'] == 'interactive'
    assert snapshot['interactive_confidence'] >= 0.6


def test_user_idle_snapshot_background_idle(monkeypatch):
    monkeypatch.setattr(
        user_idle_detector,
        'session_properties',
        lambda session_id=None: {
            'available': True,
            'session_id': 'c1',
            'active': True,
            'idle_hint': True,
            'locked_hint': True,
            'remote': False,
            'type': 'x11',
            'state': 'active',
        },
    )
    monkeypatch.setattr(user_idle_detector, 'get_user_idle_seconds', lambda: 600.0)
    snapshot = user_idle_detector.user_idle_snapshot()
    assert snapshot['mode'] == 'background_idle'
    assert snapshot['idle_confidence'] >= 0.6


def test_compute_scheduler_policy_interactive_clamps_network(monkeypatch):
    monkeypatch.setenv('FACTOR_LAB_CPU_BUDGET_MAX', '5')
    monkeypatch.setenv('FACTOR_LAB_NETWORK_BUDGET_MAX', '3')
    monkeypatch.setenv('RESEARCH_OPPORTUNITY_ENQUEUE_LIMIT', '8')
    policy = adaptive_scheduler.compute_scheduler_policy(
        base_max_tasks=3,
        cpu_usage_ratio=0.2,
        mem_pressure=0.4,
        mem_available_mb=4000,
        rss_ratio=0.1,
        idle_snapshot={'mode': 'interactive', 'idle_seconds': 5},
        route_status={'healthy': True, 'resolved_mode': 'direct'},
    )
    assert policy['mode'] == 'interactive'
    assert policy['dynamic_max_tasks'] <= 2
    assert policy['network_budget'] <= 1
    assert policy['queue_caps']['exploration'] == 2
    assert policy['opportunity_enqueue_limit'] >= 2


def test_compute_scheduler_policy_background_idle_expands(monkeypatch):
    monkeypatch.setenv('FACTOR_LAB_CPU_BUDGET_MAX', '5')
    monkeypatch.setenv('FACTOR_LAB_NETWORK_BUDGET_MAX', '3')
    monkeypatch.setenv('RESEARCH_OPPORTUNITY_ENQUEUE_LIMIT', '8')
    policy = adaptive_scheduler.compute_scheduler_policy(
        base_max_tasks=3,
        cpu_usage_ratio=0.25,
        mem_pressure=0.4,
        mem_available_mb=5000,
        rss_ratio=0.1,
        idle_snapshot={'mode': 'background_idle', 'idle_seconds': 900},
        route_status={'healthy': True, 'resolved_mode': 'direct'},
    )
    assert policy['mode'] == 'background_idle'
    assert policy['dynamic_max_tasks'] >= 4
    assert policy['dynamic_batch_workers'] >= 2
    assert policy['opportunity_enqueue_limit'] >= 8


def test_compute_scheduler_policy_io_blocked_prefers_local(monkeypatch):
    monkeypatch.setenv('RESEARCH_OPPORTUNITY_ENQUEUE_LIMIT', '8')
    policy = adaptive_scheduler.compute_scheduler_policy(
        base_max_tasks=3,
        cpu_usage_ratio=0.2,
        mem_pressure=0.4,
        mem_available_mb=5000,
        rss_ratio=0.1,
        idle_snapshot={'mode': 'background_idle', 'idle_seconds': 900},
        route_status={'healthy': False, 'resolved_mode': 'direct', 'last_error': 'timeout'},
    )
    assert policy['mode'] == 'io_blocked'
    assert policy['network_budget'] == 0
    assert policy['queue_caps']['exploration'] == 1


def test_base_env_int_preserves_original_config(monkeypatch):
    monkeypatch.setenv('RESEARCH_OPPORTUNITY_ENQUEUE_LIMIT', '8')
    first = adaptive_scheduler._base_env_int('RESEARCH_OPPORTUNITY_ENQUEUE_LIMIT', 4, minimum=1)
    monkeypatch.setenv('RESEARCH_OPPORTUNITY_ENQUEUE_LIMIT', '1')
    second = adaptive_scheduler._base_env_int('RESEARCH_OPPORTUNITY_ENQUEUE_LIMIT', 4, minimum=1)
    assert first == 8
    assert second == 8
