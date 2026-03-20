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


def _priority_adjustment(family_score: float | None, relationship_count: int, lineage_count: int) -> int:
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
    return adj


def build_research_candidate_pool(snapshot_path: str | Path, output_path: str | Path, branch_plan_path: str | Path | None = None) -> dict[str, Any]:
    snapshot = read_json(snapshot_path)
    registry_path = ROOT / 'artifacts' / 'research_space_registry.json'
    space_map_path = ROOT / 'artifacts' / 'research_space_map.json'
    branch_plan = read_json(branch_plan_path) if branch_plan_path and Path(branch_plan_path).exists() else {}
    space_map = read_json(space_map_path) if space_map_path.exists() else {}

    existing_fingerprints = {task.get('fingerprint') for task in snapshot.get('recent_research_tasks', [])}
    latest_run = snapshot.get('latest_run') or {}
    generated_configs = set(snapshot.get('generated_configs', []))
    stable_candidates = [row['factor_name'] for row in snapshot.get('stable_candidates', [])[:5]]
    latest_graveyard = snapshot.get('latest_graveyard', [])[:5]
    queue_budget = snapshot.get('queue_budget', {})
    exploration_state = snapshot.get('exploration_state', {})
    failure_state = snapshot.get('failure_state', {})
    candidate_context_by_name = _candidate_context_by_name(snapshot)
    family_score_map = _family_score_map(snapshot)
    relationship_summary = snapshot.get('relationship_summary', {}) or {}

    base_config = read_json(ROOT / 'configs' / 'tushare_workflow.json')
    end_date = latest_run.get('end_date') or base_config['end_date']
    family_progress = space_map.get('family_progress', {})
    selected_families = set(branch_plan.get('selected_families', []))

    candidates: list[dict[str, Any]] = []

    window_level = (family_progress.get('window_expansion') or {}).get('next_level')
    recent_level = (family_progress.get('recent_window_validation') or {}).get('next_level')
    stable_level = (family_progress.get('stable_candidate_validation') or {}).get('next_level')
    graveyard_level = (family_progress.get('graveyard_diagnosis') or {}).get('next_level')

    if window_level and ('window_expansion' in selected_families or not selected_families):
        window_tasks = build_window_task(window_level, latest_run, end_date, base_config, existing_fingerprints, generated_configs)
        for task in window_tasks:
            task['relationship_signal'] = {
                'hybrid_count': int(relationship_summary.get('hybrid_of', 0)),
                'cluster_count': len(snapshot.get('candidate_clusters', []) or []),
            }
            task['reason'] += f" 当前候选图中有 {relationship_summary.get('hybrid_of', 0)} 条 hybrid 关系、{len(snapshot.get('candidate_clusters', []) or [])} 个 cluster，适合扩窗检验结构是否跨阶段成立。"
        candidates.extend(window_tasks)
    if recent_level and ('recent_window_validation' in selected_families or not selected_families):
        recent_tasks = build_recent_validation_task(recent_level, latest_run, end_date, base_config, existing_fingerprints, generated_configs)
        for task in recent_tasks:
            task['relationship_signal'] = {
                'refinement_count': int(relationship_summary.get('refinement_of', 0)),
                'duplicate_count': int(relationship_summary.get('duplicate_of', 0)),
            }
            task['reason'] += f" refinement={relationship_summary.get('refinement_of', 0)}、duplicate={relationship_summary.get('duplicate_of', 0)}，近期窗口可验证这些分支是稳健延伸还是短期重复。"
        candidates.extend(recent_tasks)
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
            task['priority_hint'] += _priority_adjustment(
                max(family_scores) if family_scores else None,
                relationship_count,
                lineage_count,
            )
            task['focus_candidates'] = focus_context
            task['family_focus'] = strongest_family
            task['relationship_signal'] = {
                'relationship_count': relationship_count,
                'lineage_count': lineage_count,
                'family_score': max(family_scores) if family_scores else None,
            }
            task['reason'] += (
                f" 重点候选累计关系 {relationship_count} 条、lineage {lineage_count} 条"
                + (f"，最强 family={strongest_family}" if strongest_family else "")
                + "，优先确认这是可复制主线而非偶然候选。"
            )
        candidates.extend(stable_tasks)
    if graveyard_level and ('graveyard_diagnosis' in selected_families or not selected_families):
        graveyard_tasks = build_graveyard_task(graveyard_level, latest_graveyard, existing_fingerprints)
        for task in graveyard_tasks:
            task['relationship_signal'] = {
                'duplicate_count': int(relationship_summary.get('duplicate_of', 0)),
                'same_family_count': int(relationship_summary.get('same_family', 0)),
            }
            task['reason'] += f" duplicate={relationship_summary.get('duplicate_of', 0)}、same_family={relationship_summary.get('same_family', 0)}，可检查 graveyard 是否集中出现在同构因子支路。"
        candidates.extend(graveyard_tasks)

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
            if task['fingerprint'] not in existing_fingerprints:
                candidates.append(task)

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
            'graveyard_count': len(latest_graveyard),
            'candidate_count': len(candidates),
            'relationship_summary': relationship_summary,
        },
        'tasks': sorted(candidates, key=lambda item: (item['priority_hint'], item['category'])),
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload
