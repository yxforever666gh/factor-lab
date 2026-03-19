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
        candidates.extend(build_window_task(window_level, latest_run, end_date, base_config, existing_fingerprints, generated_configs))
    if recent_level and ('recent_window_validation' in selected_families or not selected_families):
        candidates.extend(build_recent_validation_task(recent_level, latest_run, end_date, base_config, existing_fingerprints, generated_configs))
    if stable_level and ('stable_candidate_validation' in selected_families or not selected_families):
        candidates.extend(build_stable_candidate_task(stable_level, stable_candidates, existing_fingerprints))
    if graveyard_level and ('graveyard_diagnosis' in selected_families or not selected_families):
        candidates.extend(build_graveyard_task(graveyard_level, latest_graveyard, existing_fingerprints))

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
        },
        'tasks': sorted(candidates, key=lambda item: (item['priority_hint'], item['category'])),
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload
