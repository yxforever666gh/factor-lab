from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]


def _load(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding='utf-8')) if path.exists() else default
    except Exception:
        return default


def build_harvest_report(*, root: str | Path = ROOT) -> dict[str, Any]:
    root = Path(root)
    base = root / 'artifacts/harvest_agent'
    latest = _load(base / 'latest_cycle.json', {})
    cid = latest.get('cycle_id') or 'cycle_0001'
    cdir = base / cid
    charter = _load(cdir / 'cycle_charter.json', {})
    proposals_obj = _load(cdir / 'proposals.json', {'proposals': []})
    proposals = proposals_obj.get('proposals') if isinstance(proposals_obj, dict) else proposals_obj
    gate = _load(cdir / 'gate_decision.json', {})
    manifest = _load(cdir / 'execution_manifest.json', {})
    evidence = _load(cdir / 'evidence_ledger.json', {'evidence': []})
    verdict = _load(cdir / 'verdict.json', {})
    executed = [e.get('experiment_id') for e in manifest.get('experiments', []) if e.get('run_status') or manifest.get('execution_mode') == 'dry_run']
    return {
        'schema_version': 1,
        'latest_cycle_id': cid,
        'selected_mainline': charter.get('mainline'),
        'proposals': proposals or [],
        'proposal_count': len(proposals or []),
        'gate_decision': gate.get('decision'),
        'executed_experiments': executed,
        'executed_count': manifest.get('executed_count', 0),
        'evidence_summary': evidence.get('summary') or {'evidence_count': len(evidence.get('evidence') or [])},
        'verdict': verdict.get('decision') or verdict.get('verdict'),
        'next_action': verdict.get('next_action'),
        'manual_approval_required': bool(verdict.get('manual_approval_required') or gate.get('manual_review_required')),
        'budget_status': gate.get('budget_after_decision') or charter.get('research_budget') or {},
        'safety_status': {
            'live_trading_enabled': False,
            'timer_enabled': False,
            'broad_daemon_started': False,
            'controlled_execution_requires_explicit_flag': True,
            'artifact_namespace': 'artifacts/harvest_agent',
        },
    }


def _markdown(report: dict[str, Any]) -> str:
    return (
        '# Harvest Agent Report\n\n'
        f"- Latest cycle: {report.get('latest_cycle_id')}\n"
        f"- Mainline: {report.get('selected_mainline')}\n"
        f"- Gate decision: {report.get('gate_decision')}\n"
        f"- Verdict: {report.get('verdict')}\n"
        f"- Next action: {report.get('next_action')}\n"
        f"- Manual approval required: {report.get('manual_approval_required')}\n\n"
        '## Safety\n\n'
        + json.dumps(report.get('safety_status'), ensure_ascii=False, indent=2)
        + '\n'
    )


def write_harvest_report(*, root: str | Path = ROOT) -> dict[str, Any]:
    root = Path(root)
    report = build_harvest_report(root=root)
    base = root / 'artifacts/harvest_agent'
    base.mkdir(parents=True, exist_ok=True)
    (base / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    (base / 'report.md').write_text(_markdown(report), encoding='utf-8')
    return report
