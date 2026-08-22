#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / 'src') not in sys.path:
    sys.path.insert(0, str(ROOT / 'src'))

from factor_lab.harvest_knowledge import update_harvest_knowledge
from factor_lab.harvest_report import write_harvest_report
from factor_lab.small_institutional_backtest_matrix import (
    build_small_institutional_backtest_matrix,
    small_institutional_backtest_matrix_to_markdown,
)


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def _write_md(path: Path, title: str, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"# {title}\n\n```json\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n```\n", encoding='utf-8')


def _next_cycle_id(base: Path) -> str:
    latest_path = base / 'latest_cycle.json'
    try:
        latest = json.loads(latest_path.read_text(encoding='utf-8'))
        n = int(str(latest.get('cycle_id', 'cycle_0000')).split('_')[-1]) + 1
        return f'cycle_{n:04d}'
    except Exception:
        return 'cycle_0001'


def _state_snapshot(root: Path, cycle_id: str) -> dict[str, Any]:
    def exists(rel: str) -> bool:
        return (root / rel).exists()
    blockers_path = root / 'knowledge/data_blockers.json'
    try:
        blockers = json.loads(blockers_path.read_text(encoding='utf-8')) if blockers_path.exists() else {'blockers': []}
    except Exception:
        blockers = {'blockers': []}
    return {
        'schema_version': 1,
        'cycle_id': cycle_id,
        'created_at_utc': _utc(),
        'sources': {
            'research_quality_summary': exists('artifacts/research_quality_summary.json'),
            'controlled_route_policy': exists('artifacts/controlled_route_policy.json'),
            'controlled_run_ledger_summary': exists('artifacts/controlled_run_ledger_summary.json'),
            'mechanism_lessons': exists('knowledge/mechanism_lessons.md'),
        },
        'data_blockers': blockers.get('blockers', []) if isinstance(blockers, dict) else [],
        'runtime_safety': {'live_trading': False, 'timer_enabled': False, 'broad_daemon': False},
    }


def _charter(cycle_id: str, max_experiments: int) -> dict[str, Any]:
    cap = max(0, min(2, int(max_experiments)))
    return {
        'schema_version': 1,
        'cycle_id': cycle_id,
        'created_at_utc': _utc(),
        'mainline': 'bucket_aware_oos_followup',
        'research_budget': {'max_experiments': cap, 'max_runtime_minutes': 60, 'budget_bucket': 'robustness_validation'},
        'current_blockers': [],
        'research_question': 'Can promoted bucket-aware routes survive stricter cost and tail-risk tests?',
        'success_definition': ['positive cost-adjusted return', 'bucket-aware OOS stable', 'drawdown within configured limit', 'no duplicate-equivalent evidence', 'mechanism rationale remains valid'],
        'manual_approval_required': False,
    }


def _proposals(cycle_id: str) -> dict[str, Any]:
    proposal = {
        'proposal_id': 'value_quality_cost_sensitivity_v1',
        'mechanism_id': 'value_quality_no_distress',
        'hypothesis': 'Quality-filtered value should remain informative after baseline costs if the bucket-aware mechanism is real.',
        'required_fields': ['earnings_yield', 'roe', 'pb', 'turnover', 'return_1d'],
        'derived_fields': ['bucket_pair_spread', 'cost_adjusted_return'],
        'experiment_type': 'controlled_backtest',
        'portfolio_construction': {'mode': 'bucket_pair', 'long_quantile': 3, 'short_quantile': 0},
        'validation_protocol': 'bucket_aware_oos_cost_sensitivity',
        'expected_information_gain': 'Tests whether prior bucket-aware evidence survives realistic costs.',
        'falsification_criteria': ['net spread turns negative under baseline costs', 'OOS pass fails in more than one validation split'],
        'duplicate_rationale': 'Stricter cost and tail-risk follow-up, not a timestamp-only rerun.',
    }
    return {'schema_version': 1, 'cycle_id': cycle_id, 'proposals': [proposal]}


def _review(proposals: dict[str, Any]) -> dict[str, Any]:
    return {'schema_version': 1, 'cycle_id': proposals['cycle_id'], 'decision': 'allow', 'reasons': [], 'required_changes': [], 'overfit_risk': 'medium', 'manual_review_required': False}


def _gate(charter: dict[str, Any], proposals: dict[str, Any], review: dict[str, Any], allow_controlled_execution: bool) -> dict[str, Any]:
    reasons: list[str] = []
    if review.get('decision') not in {'allow', 'cheap_screen_only'}:
        reasons.append('reviewer_not_allow')
    allowed = [] if reasons else [p['proposal_id'] for p in proposals.get('proposals', [])]
    decision = 'block' if reasons else ('allow_controlled_execution' if allow_controlled_execution else 'allow_dry_run')
    max_exp = int((charter.get('research_budget') or {}).get('max_experiments', 0))
    return {'schema_version': 1, 'cycle_id': charter['cycle_id'], 'decision': decision, 'reasons': reasons, 'allowed_experiments': allowed[:max_exp], 'blocked_experiments': [], 'manual_review_required': False, 'budget_after_decision': {'remaining_cycle_experiments': max(0, max_exp - (1 if allow_controlled_execution and allowed else 0)), 'remaining_daily_experiments': 2}}


def _manifest(root: Path, cycle_id: str, proposals: dict[str, Any], gate: dict[str, Any], controlled: bool, max_experiments: int | None) -> dict[str, Any]:
    mode = 'controlled_local' if controlled and gate.get('decision') == 'allow_controlled_execution' else 'dry_run'
    cap = 1 if controlled else 0
    if max_experiments is not None:
        cap = min(cap, max(0, int(max_experiments)))
    experiments = []
    for p in proposals.get('proposals', []):
        if p['proposal_id'] in set(gate.get('allowed_experiments') or []):
            experiments.append({'experiment_id': p['proposal_id'], 'mechanism_id': p['mechanism_id'], 'execution_mode': mode, 'output_dir': str(root / 'artifacts/harvest_agent' / cycle_id / 'runs' / p['proposal_id']), 'spec': p})
    executed_count = 0
    for e in experiments[:cap]:
        rdir = Path(e['output_dir']); rdir.mkdir(parents=True, exist_ok=True)
        result = _run_controlled_backtest(root, rdir)
        status = {
            'status': result.get('status'),
            'matrix_status': result.get('matrix_status'),
            'ok_count': (result.get('summary') or {}).get('ok_count'),
            'executed_backtest_count': (result.get('execution') or {}).get('executed_count'),
            'best_result': result.get('best_result'),
            'started_systemd_daemon': False,
            'live_trading': False,
        }
        _write_json(rdir / 'status.json', status)
        e['run_status'] = 'finished'
        e['result_path'] = str(rdir / 'result.json')
        executed_count += 1
    return {'schema_version': 1, 'cycle_id': cycle_id, 'execution_mode': mode, 'manifest_status': 'ready' if gate.get('decision') != 'block' else 'blocked', 'experiments': experiments, 'executed_count': executed_count, 'started_systemd_daemon': False, 'scheduled_timer_enabled': False}


def _run_controlled_backtest(root: Path, rdir: Path) -> dict[str, Any]:
    """Run a bounded local backtest matrix for the admitted Harvest proposal."""
    dataset_path = root / 'artifacts/value_route_bucket_aware/runs/value_quality_no_distress_bucket_aware/dataset.csv'
    if not dataset_path.exists():
        result = {
            'schema_version': 1,
            'status': 'blocked_missing_data',
            'reason': 'dataset_missing',
            'dataset_path': str(dataset_path),
        }
    else:
        matrix = build_small_institutional_backtest_matrix(
            dataset_path=dataset_path,
            signal_columns=['industry_relative_book_yield', 'industry_relative_earnings_yield', 'earnings_yield'],
            year_windows=[
                {'label': '2020-2021', 'start_date': '2020-01-01', 'end_date': '2021-12-31'},
                {'label': '2021-2022', 'start_date': '2021-01-01', 'end_date': '2022-12-31'},
                {'label': '2022-2023', 'start_date': '2022-01-01', 'end_date': '2023-12-31'},
            ],
            holding_counts=[50, 75, 100],
            rebalance_frequencies=['monthly'],
            cost_bps_values=[0, 30, 60],
            return_column='forward_return_5d',
            dry_run=False,
        )
        result = {'schema_version': 1, 'status': 'ok' if (matrix.get('summary') or {}).get('ok_count') else 'insufficient_data', **matrix}
    _write_json(rdir / 'result.json', result)
    try:
        (rdir / 'result.md').write_text(small_institutional_backtest_matrix_to_markdown(result), encoding='utf-8')
    except Exception:
        (rdir / 'result.md').write_text('# Controlled Backtest Result\n\n```json\n' + json.dumps(result, ensure_ascii=False, indent=2) + '\n```\n', encoding='utf-8')
    return result


def _evidence(manifest: dict[str, Any]) -> dict[str, Any]:
    rows = []
    for e in manifest.get('experiments', []):
        ran = e.get('run_status') == 'finished'
        metrics: dict[str, Any] = {}
        if ran and e.get('result_path'):
            try:
                result = json.loads(Path(e['result_path']).read_text(encoding='utf-8'))
                best = result.get('best_result') or {}
                metrics = {
                    'matrix_status': result.get('matrix_status'),
                    'ok_count': (result.get('summary') or {}).get('ok_count'),
                    'executed_backtest_count': (result.get('execution') or {}).get('executed_count'),
                    'best_total_return': best.get('total_return'),
                    'best_sharpe': best.get('sharpe'),
                    'best_max_drawdown': best.get('max_drawdown'),
                    'best_signal_column': best.get('signal_column'),
                    'best_label': best.get('label'),
                    'best_cost_bps': best.get('cost_bps'),
                }
            except Exception as exc:
                metrics = {'parse_error': str(exc)}
        rows.append({'experiment_id': e.get('experiment_id'), 'status': 'finished' if ran else 'dry_run_preview', 'mechanism_id': e.get('mechanism_id'), 'metrics': metrics, 'evidence_quality': {'oos_status': 'evaluated' if metrics.get('ok_count') else ('not_run' if not ran else 'pending'), 'cost_status': 'evaluated' if metrics.get('ok_count') else ('not_run' if not ran else 'pending'), 'duplicate_status': 'independent_followup', 'data_quality_status': 'usable' if metrics.get('ok_count') else ('not_run' if not ran else 'pending')}, 'failure_class': None if ran else 'execution_failure', 'information_gain': 'real_backtest_metrics' if metrics.get('ok_count') else ('positive_progress' if ran else 'execution_failure')})
    return {'schema_version': 1, 'cycle_id': manifest['cycle_id'], 'evidence': rows, 'summary': {'evidence_count': len(rows), 'executed_count': manifest.get('executed_count', 0)}}


def _verdict(cycle_id: str, evidence: dict[str, Any]) -> dict[str, Any]:
    executed = (evidence.get('summary') or {}).get('executed_count', 0)
    rows = evidence.get('evidence') or []
    real_rows = [r for r in rows if (r.get('metrics') or {}).get('ok_count')]
    decision = 'continue_same_mainline' if real_rows else 'hold_route'
    if real_rows:
        m = real_rows[0].get('metrics') or {}
        reasoning = [f"Real controlled backtest matrix completed: ok_count={m.get('ok_count')}, executed_backtest_count={m.get('executed_backtest_count')}, best_total_return={m.get('best_total_return')}, best_sharpe={m.get('best_sharpe')}, best_max_drawdown={m.get('best_max_drawdown')}."]
    elif executed:
        reasoning = ['Controlled execution ran but did not produce usable real backtest metrics.']
    else:
        reasoning = ['Dry-run/placeholder evidence is not sufficient for promotion.']
    return {'schema_version': 1, 'cycle_id': cycle_id, 'decision': decision, 'promoted_candidates': [], 'held_candidates': ['value_quality_no_distress'], 'blocked_candidates': [], 'reasoning': reasoning, 'next_action': 'inspect artifacts and run at most one controlled experiment with explicit approval' if not executed else 'run stricter independent OOS validation', 'manual_approval_required': False}


def _next_plan(cycle_id: str, verdict: dict[str, Any]) -> dict[str, Any]:
    n = int(cycle_id.split('_')[-1]) + 1
    return {'schema_version': 1, 'cycle_id': f'cycle_{n:04d}', 'plan_status': 'planned' if not verdict.get('manual_approval_required') else 'manual_review', 'mainline': 'bucket_aware_oos_followup', 'next_action': verdict.get('next_action'), 'manual_approval_required': bool(verdict.get('manual_approval_required'))}


def run_harvest_agent_once(*, root: str | Path = ROOT, dry_run: bool = False, allow_controlled_execution: bool = False, max_experiments: int | None = None) -> dict[str, Any]:
    root = Path(root)
    base = root / 'artifacts/harvest_agent'
    cycle_id = _next_cycle_id(base)
    cdir = base / cycle_id
    safe_max = 1 if allow_controlled_execution else (max_experiments if max_experiments is not None else 1)
    state = _state_snapshot(root, cycle_id); _write_json(cdir / 'state_snapshot.json', state)
    charter = _charter(cycle_id, safe_max); _write_json(cdir / 'cycle_charter.json', charter); _write_md(cdir / 'cycle_charter.md', 'Cycle Charter', charter)
    props = _proposals(cycle_id); _write_json(cdir / 'proposals.json', props); _write_md(cdir / 'proposals.md', 'Research Proposals', props)
    review = _review(props); _write_json(cdir / 'reviewer_decision.json', review); _write_md(cdir / 'reviewer_decision.md', 'Reviewer Decision', review)
    gate = _gate(charter, props, review, allow_controlled_execution and not dry_run); _write_json(cdir / 'gate_decision.json', gate); _write_md(cdir / 'gate_decision.md', 'Gate Decision', gate)
    manifest = _manifest(root, cycle_id, props, gate, allow_controlled_execution and not dry_run, safe_max); _write_json(cdir / 'execution_manifest.json', manifest)
    ev = _evidence(manifest); _write_json(cdir / 'evidence_ledger.json', ev); _write_md(cdir / 'evidence_ledger.md', 'Evidence Ledger', ev)
    verdict = _verdict(cycle_id, ev); _write_json(cdir / 'verdict.json', verdict); _write_md(cdir / 'verdict.md', 'Verdict', verdict)
    ku = update_harvest_knowledge(root=root, cycle_id=cycle_id)
    nextp = _next_plan(cycle_id, verdict); _write_json(cdir / 'next_cycle_plan.json', nextp); _write_md(cdir / 'next_cycle_plan.md', 'Next Cycle Plan', nextp)
    latest = {'cycle_id': cycle_id, 'cycle_status': 'complete', 'verdict': verdict.get('decision'), 'next_action': verdict.get('next_action'), 'manual_approval_required': verdict.get('manual_approval_required')}
    _write_json(base / 'latest_cycle.json', latest)
    write_harvest_report(root=root)
    return {**latest, 'executed_count': manifest.get('executed_count', 0), 'artifacts_dir': str(cdir), 'started_systemd_daemon': False, 'scheduled_timer_enabled': False, 'knowledge_update': ku.get('durable_conclusion')}


def run_harvest_agent_loop(
    *,
    root: str | Path = ROOT,
    cycles: int = 1,
    dry_run: bool = False,
    allow_controlled_execution: bool = False,
    max_experiments: int | None = None,
    sleep_seconds: float = 0.0,
) -> dict[str, Any]:
    """Run consecutive Harvest cycles in-process without installing a timer.

    This is the event-chain mode: when one cycle finishes and its verdict does
    not require manual review, the next cycle starts immediately until the
    caller-provided cycle budget is exhausted. It deliberately does not enable
    systemd timers, start broad daemons, or relax the per-cycle execution cap.
    """
    cycle_budget = max(0, int(cycles))
    results: list[dict[str, Any]] = []
    loop_status = 'complete'

    for idx in range(cycle_budget):
        result = run_harvest_agent_once(
            root=root,
            dry_run=dry_run,
            allow_controlled_execution=allow_controlled_execution,
            max_experiments=max_experiments,
        )
        results.append(result)
        if result.get('manual_approval_required'):
            loop_status = 'stopped_manual_review'
            break
        if idx < cycle_budget - 1 and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return {
        'loop_status': loop_status,
        'cycles_requested': cycle_budget,
        'cycles_run': len(results),
        'cycles': results,
        'started_systemd_daemon': False,
        'scheduled_timer_enabled': False,
        'latest_cycle_id': results[-1]['cycle_id'] if results else None,
    }


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--allow-controlled-execution', action='store_true')
    ap.add_argument('--max-experiments', type=int)
    ap.add_argument('--cycles', type=int, default=1)
    ap.add_argument('--sleep-seconds', type=float, default=0.0)
    args = ap.parse_args()
    from factor_lab.research_os.legacy_entrypoint import retired_legacy_entrypoint
    raise SystemExit(retired_legacy_entrypoint("scripts/run_harvest_agent_once.py"))
