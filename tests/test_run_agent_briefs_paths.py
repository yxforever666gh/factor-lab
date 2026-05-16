import importlib.util
import json
from pathlib import Path


def _load_brief_runner_module():
    path = Path(__file__).resolve().parents[1] / 'scripts' / 'run_agent_briefs.py'
    spec = importlib.util.spec_from_file_location('run_agent_briefs_test', path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class FakeRouter:
    def __init__(self, provider=None, model=None):
        self.provider = provider
        self.model = model

    def generate(self, role, context):
        return {
            'decision_metadata': {
                'effective_source': self.provider or 'heuristic',
                'validation_errors': [],
            },
            'role': role,
            'context_id': context.get('context_id'),
        }


def test_brief_runner_uses_configurable_artifacts_dir(tmp_path, monkeypatch, capsys):
    artifacts = tmp_path / 'custom-artifacts'
    artifacts.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv('FACTOR_LAB_ARTIFACTS_DIR', str(artifacts))
    module = _load_brief_runner_module()

    (artifacts / 'planner_agent_brief.json').write_text(json.dumps({'brief': 'planner'}, ensure_ascii=False), encoding='utf-8')
    (artifacts / 'failure_analyst_brief.json').write_text(json.dumps({'brief': 'failure'}, ensure_ascii=False), encoding='utf-8')

    monkeypatch.setattr(module, 'DecisionProviderRouter', FakeRouter)
    monkeypatch.setattr(module, 'build_planner_decision_context', lambda brief: {'context_id': 'planner-ctx', 'brief': brief})
    monkeypatch.setattr(module, 'build_failure_decision_context', lambda brief: {'context_id': 'failure-ctx', 'brief': brief})
    monkeypatch.setattr(module, '_parse_args', lambda: type('Args', (), {'provider': 'heuristic'})())

    assert module.main() == 0

    assert (artifacts / 'planner_decision_context.json').exists()
    assert (artifacts / 'failure_decision_context.json').exists()
    assert (artifacts / 'planner_agent_response.json').exists()
    assert (artifacts / 'failure_analyst_response.json').exists()
    assert (artifacts / 'agent_responses.json').exists()

    stdout = capsys.readouterr().out.strip()
    payload = json.loads(stdout)
    assert payload['planner_written'] is True
    assert payload['failure_written'] is True
    assert payload['planner_context_id'] == 'planner-ctx'
    assert payload['failure_context_id'] == 'failure-ctx'
