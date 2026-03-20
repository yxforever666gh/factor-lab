from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.research_family_generators import (
    read_json,
    make_task,
    build_window_task,
    build_recent_validation_task,
    build_stable_candidate_task,
    build_graveyard_task,
)

ROOT = Path(__file__).resolve().parents[2]


def _candidate_context_by_name(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = snapshot.get('candidate_context', []) or []
    return {row.get('candidate_name'): row for row in rows if row.get('candidate_name')}


def _family_score_map(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = snapshot.get('family_summary', []) or []
    return {row.get('family'): row for row in rows if row.get('family')}


def _cluster_rep_map(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = snapshot.get('cluster_representatives', []) or []
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        rep_name = row.get('representative_candidate')
        primary_name = row.get('primary_candidate')
        if rep_name:
            out[rep_name] = row
        if primary_name and primary_name not in out:
            out[primary_name] = row
    return out


def _priority_adjustment(family_score: float | None, relationship_count: int, lineage_count: int, trial_pressure: float | None = None, false_positive_pressure: float | None = None, fragile_count: int = 0, family_risk_score: float | None = None) -> int:
    adj = 0
    if family_score is not None:
        if family_score >= 100:
            adj -= 5
        elif family_score >= 80:
            adj -= 3
        elif family_score <= 40:
            adj += 2
    if relationship_count >= 4:
        adj -= 2
    if lineage_count >= 2:
        adj -= 2
    if trial_pressure is not None:
        if trial_pressure >= 75:
            adj += 6
        elif trial_pressure >= 50:
            adj += 3
    if false_positive_pressure is not None:
        if false_positive_pressure >= 70:
            adj += 5
        elif false_positive_pressure >= 45:
            adj += 2
    if fragile_count:
        adj -= min(fragile_count * 2, 6)
    if family_risk_score is not None:
        if family_risk_score >= 70:
            adj -= 5
        elif family_risk_score >= 55:
            adj -= 2
    return adj


def _task_family_key(task: dict[str, Any]) -> str:
    worker_note = task.get('worker_note', '') or ''
    if '稳定候选' in worker_note:
        return 'stable_candidate_validation'
    if 'graveyard' in worker_note:
        return 'graveyard_diagnosis'
    if '近期' in worker_note:
        return 'recent_window_validation'
    if '扩窗' in worker_note or 'expanding' in worker_note:
        return 'window_expansion'
    if 'exploration' in worker_note:
        return 'exploration'
    return task.get('category') or 'other'


def _dedupe_signature(task: dict[str, Any]) -> str:
    payload = task.get('payload') or {}
    family_key = _task_family_key(task)
    if task.get('task_type') == 'diagnostic':
        focus = sorted(payload.get('focus_factors') or [])
        return f"{family_key}::diagnostic::{'|'.join(focus)}"
    if task.get('task_type') == 'workflow':
        config_path = payload.get('config_path', '')
        return f'{family_key}::workflow::{Path(config_path).name}'
    if task.get('task_type') == 'generated_batch':
        return f"{family_key}::generated_batch::{payload.get('batch_path', '')}"
    return f"{family_key}::{task.get('fingerprint')}"


def _existing_signatures(snapshot: dict[str, Any]) -> set[str]:
    signatures = set()
    for task in snapshot.get('recent_research_tasks', []) or []:
        if task.get('status') not in {'pending', 'running', 'finished'}:
            continue
        signatures.add(_dedupe_signature(task))
    return signatures


def _prefer_representatives(stable_candidates: list[str], candidate_context_by_name: dict[str, dict[str, Any]], cluster_rep_map: dict[str, dict[str, Any]]) -> tuple[list[str], list[dict[str, Any]]]:
    selected: list[str] = []
    selected_set: set[str] = set()
    suppressed: list[dict[str, Any]] = []
    for name in stable_candidates:
        context = candidate_context_by_name.get(name, {})
        cluster = context.get('cluster') or {}
        rep_candidates = cluster.get('representative_candidates') or [cluster.get('primary_candidate') or name]
        rep_candidates = [rep for rep in rep_candidates if rep]
        if name in rep_candidates:
            chosen = name
        else:
            chosen = rep_candidates[0] if rep_candidates else (cluster.get('primary_candidate') or name)
        if chosen not in selected_set:
            selected.append(chosen)
            selected_set.add(chosen)
        if chosen != name:
            suppressed.append({
                'candidate': name,
                'suppressed_into': chosen,
                'cluster_id': cluster.get('cluster_id'),
                'available_representatives': rep_candidates,
                'reason': 'cluster_representative_retained',
            })
    enriched = []
    for name in selected:
        row = cluster_rep_map.get(name) or {}
        enriched.append({
            'candidate': name,
            'cluster_id': row.get('cluster_id'),
            'representative_rank': row.get('representative_rank'),
            'representative_count': row.get('representative_count'),
            'is_primary_representative': row.get('is_primary_representative'),
            'suppressed_candidates': row.get('suppressed_candidates') or [],
        })
    return selected, suppressed + enriched


def build_research_candidate_pool(snapshot_path: str | Path, output_path: str | Path, branch_plan_path: str | Path | None = None) -> dict[str, Any]:
    snapshot = read_json(snapshot_path)
    registry_path = ROOT / 'artifacts' / 'research_space_registry.json'
    space_map_path = ROOT / 'artifacts' / 'research_space_map.json'
    branch_plan = read_json(branch_plan_path) if branch_plan_path and Path(branch_plan_path).exists() else {}
    space_map = read_json(space_map_path) if space_map_path.exists() else {}

    existing_fingerprints = {task.get('fingerprint') for task in snapshot.get('recent_research_tasks', [])}
    existing_signatures = _existing_signatures(snapshot)
    latest_run = snapshot.get('latest_run') or {}
    generated_configs = set(snapshot.get('generated_configs', []))
    raw_stable_candidates = [row['factor_name'] for row in snapshot.get('stable_candidates', [])[:5]]
    latest_graveyard = snapshot.get('latest_graveyard', [])[:5]
    queue_budget = snapshot.get('queue_budget', {})
    exploration_state = snapshot.get('exploration_state', {})
    failure_state = snapshot.get('failure_state', {})
    candidate_context_by_name = _candidate_context_by_name(snapshot)
    family_score_map = _family_score_map(snapshot)
    cluster_rep_map = _cluster_rep_map(snapshot)
    relationship_summary = snapshot.get('relationship_summary', {}) or {}
    family_recommendations = {row.get('family'): row for row in snapshot.get('family_recommendations', []) if row.get('family')}
    trial_summary = snapshot.get('research_trial_summary', {}) or {}

    stable_candidates, representative_notes = _prefer_representatives(raw_stable_candidates, candidate_context_by_name, cluster_rep_map)

    base_config = read_json(ROOT / 'configs' / 'tushare_workflow.json')
    end_date = latest_run.get('end_date') or base_config['end_date']
    family_progress = space_map.get('family_progress', {})
    selected_families = set(branch_plan.get('selected_families', []))

    candidates: list[dict[str, Any]] = []
    suppressed_tasks: list[dict[str, Any]] = []
    seen_signatures: set[str] = set()

    def append_task(task: dict[str, Any], suppression_reason: str | None = None) -> None:
        signature = _dedupe_signature(task)
        if task.get('fingerprint') in existing_fingerprints or signature in existing_signatures or signature in seen_signatures:
            suppressed_tasks.append({
                'fingerprint': task.get('fingerprint'),
                'signature': signature,
                'worker_note': task.get('worker_note'),
                'reason': suppression_reason or 'duplicate_candidate_suppressed',
            })
            return
        seen_signatures.add(signature)
        task['dedupe_signature'] = signature
        candidates.append(task)

    window_level = (family_progress.get('window_expansion') or {}).get('next_level')
    recent_level = (family_progress.get('recent_window_validation') or {}).get('next_level')
    stable_level = (family_progress.get('stable_candidate_validation') or {}).get('next_level')
    graveyard_level = (family_progress.get('graveyard_diagnosis') or {}).get('next_level')

    if window_level and ('window_expansion' in selected_families or not selected_families):
        window_tasks = build_window_task(window_level, latest_run, end_date, base_config, existing_fingerprints, generated_configs)
        for task in window_tasks:
            max_family_risk = max([float((row.get('family_risk_score') or 0.0)) for row in family_recommendations.values()] or [0.0])
            validate_risk_family_count = len([row for row in family_recommendations.values() if row.get('recommended_action') == 'validate_risk'])
            task['priority_hint'] += 8 if validate_risk_family_count else 0
            task['relationship_signal'] = {
                'hybrid_count': int(relationship_summary.get('hybrid_of', 0)),
                'cluster_count': len(snapshot.get('candidate_clusters', []) or []),
                'family_risk_score': max_family_risk,
                'validate_risk_family_count': validate_risk_family_count,
            }
            task['reason'] += f" 当前候选图中有 {relationship_summary.get('hybrid_of', 0)} 条 hybrid 关系、{len(snapshot.get('candidate_clusters', []) or [])} 个 cluster；但高风险 family={validate_risk_family_count} 个，因此扩窗优先级被下调，先确认结构是否真的跨阶段成立。"
            append_task(task, 'window_expansion_already_covered')
    if recent_level and ('recent_window_validation' in selected_families or not selected_families):
        recent_tasks = build_recent_validation_task(recent_level, latest_run, end_date, base_config, existing_fingerprints, generated_configs)
        for task in recent_tasks:
            fragile_candidates = [row for row in candidate_context_by_name.values() if row.get('fragile')]
            task['priority_hint'] -= 4 if fragile_candidates else 0
            task['relationship_signal'] = {
                'refinement_count': int(relationship_summary.get('refinement_of', 0)),
                'duplicate_count': int(relationship_summary.get('duplicate_of', 0)),
                'fragile_candidate_count': len(fragile_candidates),
            }
            task['reason'] += f" refinement={relationship_summary.get('refinement_of', 0)}、duplicate={relationship_summary.get('duplicate_of', 0)}；fragile 候选={len(fragile_candidates)}，近期窗口优先确认这些分支是稳健延伸还是短期重复。"
            append_task(task, 'recent_window_already_covered')
    if stable_level and ('stable_candidate_validation' in selected_families or not selected_families):
        stable_tasks = build_stable_candidate_task(stable_level, stable_candidates, existing_fingerprints)
        for task in stable_tasks:
            focus_context = [candidate_context_by_name.get(name, {}) for name in stable_candidates]
            family_scores = [row.get('family_score') for row in focus_context if row.get('family_score') is not None]
            relationship_count = sum(int(row.get('relationship_count') or 0) for row in focus_context)
            lineage_count = sum(int(row.get('lineage_count') or 0) for row in focus_context)
            strongest_family = None
            if focus_context:
                strongest_family = max(
                    [row.get('family') for row in focus_context if row.get('family') in family_score_map],
                    key=lambda name: (family_score_map.get(name) or {}).get('family_score', -999),
                    default=None,
                )
            strongest_trial = trial_summary.get(strongest_family or '', {}) if strongest_family else {}
            fragile_count = len([row for row in focus_context if row.get('fragile')])
            family_risk_score = (family_recommendations.get(strongest_family or '') or {}).get('family_risk_score') if strongest_family else None
            task['priority_hint'] += _priority_adjustment(
                max(family_scores) if family_scores else None,
                relationship_count,
                lineage_count,
                strongest_trial.get('trial_pressure'),
                strongest_trial.get('false_positive_pressure'),
                fragile_count,
                family_risk_score,
            )
            task['focus_candidates'] = focus_context
            task['family_focus'] = strongest_family
            task['representative_selection'] = representative_notes
            task['relationship_signal'] = {
                'relationship_count': relationship_count,
                'lineage_count': lineage_count,
                'family_score': max(family_scores) if family_scores else None,
                'duplicate_count': int(relationship_summary.get('duplicate_of', 0)),
                'trial_pressure': strongest_trial.get('trial_pressure'),
                'false_positive_pressure': strongest_trial.get('false_positive_pressure'),
                'trial_count': strongest_trial.get('trial_count'),
                'fragile_candidate_count': fragile_count,
                'family_risk_score': family_risk_score,
                'family_recommended_action': (family_recommendations.get(strongest_family or '') or {}).get('recommended_action') if strongest_family else None,
            }
            if strongest_family and strongest_family in family_recommendations:
                task['family_recommendation'] = family_recommendations[strongest_family]
            task['trial_accounting'] = strongest_trial
            task['reason'] += (
                f" 重点候选累计关系 {relationship_count} 条、lineage {lineage_count} 条"
                + (f"，最强 family={strongest_family}" if strongest_family else "")
                + (f"，trial_pressure={strongest_trial.get('trial_pressure')}，false_positive_pressure={strongest_trial.get('false_positive_pressure')}" if strongest_trial else "")
                + (f"，fragile_candidates={fragile_count}，family_risk_score={family_risk_score}" if fragile_count or family_risk_score is not None else "")
                + f"。保留 cluster representatives 后实际验证 {len(stable_candidates)} 个代表候选，压制 {len([r for r in representative_notes if r.get('suppressed_into')])} 个重复/近重复候选。"
            )
            append_task(task, 'stable_validation_already_covered')
    if graveyard_level and ('graveyard_diagnosis' in selected_families or not selected_families):
        graveyard_tasks = build_graveyard_task(graveyard_level, latest_graveyard, existing_fingerprints)
        for task in graveyard_tasks:
            task['relationship_signal'] = {
                'duplicate_count': int(relationship_summary.get('duplicate_of', 0)),
                'same_family_count': int(relationship_summary.get('same_family', 0)),
            }
            task['reason'] += f" duplicate={relationship_summary.get('duplicate_of', 0)}、same_family={relationship_summary.get('same_family', 0)}，可检查 graveyard 是否集中出现在同构因子支路。"
            append_task(task, 'graveyard_diagnosis_already_covered')

    if not exploration_state.get('should_throttle') and queue_budget.get('exploration', 0) < 1:
        generated_batch_path = ROOT / 'artifacts' / 'generated_batch_from_llm.json'
        if generated_batch_path.exists():
            task = make_task(
                task_type='generated_batch',
                category='exploration',
                priority_hint=55,
                reason='当前 exploration 未被 throttle，可允许一个受控生成 batch 进入候选池。',
                expected_knowledge_gain=['exploration_candidate_survived', 'exploration_graveyard_identified'],
                payload={'batch_path': str(generated_batch_path.relative_to(ROOT)), 'output_dir': 'artifacts/llm_generated_batch_run'},
                worker_note='exploration｜执行 LLM 生成的 batch',
            )
            task['relationship_signal'] = {
                'hybrid_count': int(relationship_summary.get('hybrid_of', 0)),
                'top_family_score': max([row.get('family_score') or 0 for row in family_score_map.values()] or [0]),
            }
            task['reason'] += ' 当前关系图已出现 hybrid 支路，可让 exploration 有针对性地尝试跨 family 组合。'
            append_task(task, 'exploration_batch_already_seen')

    payload = {
        'generated_from_snapshot': str(Path(snapshot_path)),
        'generated_from_registry': str(registry_path),
        'generated_from_space_map': str(space_map_path),
        'generated_from_branch_plan': str(branch_plan_path) if branch_plan_path else None,
        'summary': {
            'latest_run_config': latest_run.get('config_path'),
            'queue_budget': queue_budget,
            'failure_state': failure_state,
            'exploration_state': exploration_state,
            'stable_candidate_count': len(stable_candidates),
            'raw_stable_candidate_count': len(raw_stable_candidates),
            'graveyard_count': len(latest_graveyard),
            'candidate_count': len(candidates),
            'suppressed_candidate_count': len(suppressed_tasks),
            'relationship_summary': relationship_summary,
        },
        'representative_selection': representative_notes,
        'suppressed_tasks': suppressed_tasks,
        'tasks': sorted(candidates, key=lambda item: (item['priority_hint'], item['category'])),
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload
