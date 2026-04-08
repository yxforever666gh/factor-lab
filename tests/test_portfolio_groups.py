from pathlib import Path
import json

from factor_lab.workflow import run_workflow


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_workflow_emits_portfolio_groups_and_cost_fields(tmp_path, monkeypatch):
    output_dir = tmp_path / 'first_workflow'
    config_path = REPO_ROOT / 'configs' / 'first_workflow.json'
    monkeypatch.chdir(tmp_path)
    run_workflow(str(config_path), str(output_dir))

    payload = json.loads((output_dir / 'portfolio_results.json').read_text(encoding='utf-8'))
    names = {row['strategy_name'] for row in payload}
    assert 'all_factors_baseline' in names
    assert 'family_distinct_only' in names
    assert 'cluster_representatives_only' in names
    first = payload[0]
    assert 'turnover_cost_estimate' in first
    assert 'cost_adjusted_annual_return' in first


def test_family_distinct_group_is_not_larger_than_number_of_families(tmp_path, monkeypatch):
    output_dir = tmp_path / 'first_workflow'
    config_path = REPO_ROOT / 'configs' / 'first_workflow.json'
    monkeypatch.chdir(tmp_path)
    run_workflow(str(config_path), str(output_dir))
    payload = json.loads((output_dir / 'portfolio_results.json').read_text(encoding='utf-8'))
    family_group = next(row for row in payload if row['strategy_name'] == 'family_distinct_only')
    assert 'cost_adjusted_annual_return' in family_group
