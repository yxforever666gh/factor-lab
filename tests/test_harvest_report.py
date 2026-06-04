import json
from factor_lab.harvest_report import build_harvest_report, write_harvest_report


def test_report_summarizes_latest_cycle(tmp_path):
    c = tmp_path / 'artifacts/harvest_agent/cycle_0001'
    c.mkdir(parents=True)
    (tmp_path / 'artifacts/harvest_agent/latest_cycle.json').write_text(json.dumps({'cycle_id': 'cycle_0001'}))
    (c / 'cycle_charter.json').write_text(json.dumps({'cycle_id': 'cycle_0001', 'mainline': 'bucket_aware_oos_followup', 'research_budget': {'max_experiments': 2}}))
    (c / 'proposals.json').write_text(json.dumps({'proposals': [{'proposal_id': 'p1'}]}))
    (c / 'gate_decision.json').write_text(json.dumps({'decision': 'allow_dry_run', 'budget_after_decision': {'remaining_cycle_experiments': 2}}))
    (c / 'execution_manifest.json').write_text(json.dumps({'experiments': [{'experiment_id': 'p1', 'execution_mode': 'dry_run'}], 'executed_count': 0}))
    (c / 'evidence_ledger.json').write_text(json.dumps({'evidence': [{'experiment_id': 'p1', 'information_gain': 'execution_failure'}]}))
    (c / 'verdict.json').write_text(json.dumps({'decision': 'hold_route', 'next_action': 'manual inspect', 'manual_approval_required': True}))
    report = build_harvest_report(root=tmp_path)
    assert report['latest_cycle_id'] == 'cycle_0001'
    assert report['selected_mainline'] == 'bucket_aware_oos_followup'
    assert report['gate_decision'] == 'allow_dry_run'
    assert report['manual_approval_required'] is True
    assert report['safety_status']['timer_enabled'] is False


def test_write_report_creates_json_and_markdown(tmp_path):
    out = write_harvest_report(root=tmp_path)
    assert (tmp_path / 'artifacts/harvest_agent/report.json').exists()
    assert (tmp_path / 'artifacts/harvest_agent/report.md').exists()
    assert out['safety_status']['live_trading_enabled'] is False
