from scripts.render_autonomous_research_loop_timer import render_timer_units

def test_rendered_unit_uses_loop_once():
    units=render_timer_units()
    assert 'run_autonomous_research_loop_once.py' in units['service']

def test_controlled_flags_only_when_requested():
    assert '--allow-controlled-execution' not in render_timer_units()['service']
    assert '--allow-controlled-execution --max-experiments 1' in render_timer_units(allow_controlled_execution=True, max_experiments=1)['service']

def test_rendered_unit_does_not_call_broad_daemon():
    units=render_timer_units(allow_controlled_execution=True)
    assert 'run_research_daemon.py' not in units['service']
