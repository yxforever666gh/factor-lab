import importlib.util
import json
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'sync_memory.py'


def load_module():
    spec = importlib.util.spec_from_file_location('sync_memory', SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parse_results_array_format(tmp_path):
    module = load_module()
    result_file = tmp_path / 'results.json'
    result_file.write_text(json.dumps([
        {
            'factor_name': 'turnover_shock_5_20',
            'expression': 'turnover_shock_5_20',
            'rank_ic_mean': 0.000155,
            'rank_ic_ir': 0.000731,
            'top_bottom_spread_mean': -0.002741,
            'sharpe_net': -0.4945,
            'net_return_annual': -2.417539,
            'pass_gate': False,
            'fail_reason': 'rank_ic_mean<0.03; top_bottom_spread<0.0; sharpe_net<1.0'
        }
    ]))

    records = module.parse_result_file(result_file)

    assert len(records) == 1
    assert records[0]['factor_name'] == 'turnover_shock_5_20'
    assert records[0]['classification'] == 'weak_signal_negative_spread'
    assert 'reverse' in records[0]['lesson'].lower() or 'reversed' in records[0]['lesson'].lower()


def test_parse_results_dict_factor_evaluations_format(tmp_path):
    module = load_module()
    result_file = tmp_path / 'results.json'
    result_file.write_text(json.dumps({
        'factor_evaluations': [
            {
                'factor_name': 'earnings_yield',
                'expression': 'earnings_yield',
                'rank_ic_mean': 0.004147,
                'rank_ic_ir': 0.019538,
                'top_bottom_spread_mean': -0.005905,
                'sharpe_net': -6.2586,
                'net_return_annual': -3.21497,
                'pass_gate': False,
                'fail_reason': 'rank_ic_mean<0.02; top_bottom_spread<0.0005; sharpe_net<1.0'
            }
        ]
    }))

    records = module.parse_result_file(result_file)

    assert len(records) == 1
    assert records[0]['factor_name'] == 'earnings_yield'
    assert records[0]['classification'] in {'weak_signal_negative_spread', 'reject_candidate'}


def test_discover_result_files_skips_synced(tmp_path):
    module = load_module()
    artifacts = tmp_path / 'artifacts'
    out = artifacts / 'run1'
    out.mkdir(parents=True)
    result_file = out / 'results.json'
    result_file.write_text('[]')
    digest = module.sha256_file(result_file)
    state = {'version': 1, 'synced_files': {str(result_file): {'sha256': digest}}}

    files = module.discover_result_files(artifacts, state)

    assert files == []


def test_build_lessons_markdown_contains_factor_name():
    module = load_module()
    records = [
        {
            'factor_name': 'turnover_shock_5_20',
            'classification': 'weak_signal_negative_spread',
            'sharpe_net': -0.4945,
            'rank_ic_mean': 0.000155,
            'lesson': 'test reversed version'
        }
    ]
    md = module.build_lessons_markdown(records)
    assert 'turnover_shock_5_20' in md
    assert 'test reversed version' in md


def test_watchlist_prefers_best_failed_factor():
    module = load_module()
    records = [
        {'factor_name': 'earnings_yield', 'classification': 'reject_candidate', 'sharpe_net': -6.25, 'pass_gate': False},
        {'factor_name': 'turnover_shock_5_20', 'classification': 'weak_signal_negative_spread', 'sharpe_net': -0.49, 'pass_gate': False},
    ]
    watch = module.build_watchlist(records, now='NOW')
    names = [x['factor_name'] for x in watch['factors']]
    assert 'turnover_shock_5_20' in names


def test_append_experiment_records_deduplicates(tmp_path, monkeypatch):
    module = load_module()
    exp_file = tmp_path / 'factor_experiments.jsonl'
    monkeypatch.setattr(module, 'KNOWLEDGE_DIR', tmp_path)
    monkeypatch.setattr(module, 'EXPERIMENTS_FILE', exp_file)
    record = {
        'artifact_path': '/tmp/results.json',
        'artifact_sha256': 'abc',
        'factor_name': 'mom_20',
        'expression': 'momentum_20',
    }

    assert module.append_experiment_records([record]) == 1
    assert module.append_experiment_records([record]) == 0
    assert len(exp_file.read_text().splitlines()) == 1


def test_memory_candidate_mentions_best_candidate():
    module = load_module()
    records = [
        {'factor_name': 'turnover_shock_5_20', 'classification': 'weak_signal_negative_spread', 'sharpe_net': -0.49, 'pass_gate': False, 'lesson': 'try reverse'},
    ]
    candidate = module.build_hermes_memory_candidate(records)
    assert candidate is not None
    assert 'turnover_shock_5_20' in candidate
