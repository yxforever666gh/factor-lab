from scripts.render_harvest_agent_timer import render_timer_units, write_timer_preview


def test_rendered_timer_defaults_to_6h_dry_run():
    units = render_timer_units()
    assert 'OnUnitActiveSec=6h' in units['timer']
    assert 'run_harvest_agent_once.py --dry-run' in units['service']
    assert '--allow-controlled-execution' not in units['service']


def test_controlled_preview_caps_to_one_experiment():
    units = render_timer_units(allow_controlled_execution=True, max_experiments=99)
    assert '--allow-controlled-execution --max-experiments 1' in units['service']


def test_preview_writes_artifacts_and_does_not_enable():
    out = write_timer_preview(root='.', output_dir='artifacts/harvest_agent/timer_preview')
    assert out['enabled'] is False
    assert out['installed'] is False
    assert 'systemctl enable' not in out['service']
    assert 'run_research_daemon.py' not in out['service']
