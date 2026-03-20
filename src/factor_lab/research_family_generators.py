from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

from factor_lab.dedup import config_fingerprint
from factor_lab.research_families import (
    level_priority,
    stable_candidate_task_name,
    stable_candidate_gain_name,
    stable_candidate_worker_note,
    graveyard_task_name,
    graveyard_gain_name,
    graveyard_worker_note,
)

ROOT = Path(__file__).resolve().parents[2]

WINDOW_LEVEL_SPECS = {
    11: ('rolling_480d_back', '2024-09-01', 'artifacts/generated_rolling_480d_back', 'baseline｜历史扩窗 480 天'),
    12: ('expanding_from_2024_07_01', '2024-07-01', 'artifacts/generated_expanding_2024_07_01', 'baseline｜expanding 窗口 2024-07-01 起'),
}

RECENT_WINDOW_LEVEL_SPECS = {
    5: ('rolling_recent_180d', '2025-09-20', 'artifacts/generated_recent_180d', 'validation｜近期 180 天窗口验证'),
    6: ('rolling_recent_210d', '2025-08-20', 'artifacts/generated_recent_210d', 'validation｜近期 210 天窗口验证'),
    7: ('rolling_recent_240d', '2025-07-20', 'artifacts/generated_recent_240d', 'validation｜近期 240 天窗口验证'),
    8: ('rolling_recent_270d', '2025-06-20', 'artifacts/generated_recent_270d', 'validation｜近期 270 天窗口验证'),
}


def read_json(path: str | Path) -> dict[str, Any]:
    import json
    return json.loads(Path(path).read_text(encoding='utf-8'))


def write_generated_config(config: dict[str, Any], name: str) -> str:
    import json
    out_dir = ROOT / 'artifacts' / 'generated_configs'
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f'{name}.json'
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding='utf-8')
    return str(path.relative_to(ROOT))


def make_task(
    task_type: str,
    category: str,
    priority_hint: int,
    reason: str,
    expected_knowledge_gain: list[str],
    payload: dict[str, Any],
    worker_note: str,
    *,
    goal: str | None = None,
    hypothesis: str | None = None,
    branch_id: str | None = None,
    stop_if: list[str] | None = None,
    promote_if: list[str] | None = None,
    disconfirm_if: list[str] | None = None,
) -> dict[str, Any]:
    import json
    enriched_payload = dict(payload)
    enriched_payload.setdefault('goal', goal or category)
    enriched_payload.setdefault('hypothesis', hypothesis or reason)
    enriched_payload.setdefault('expected_information_gain', expected_knowledge_gain)
    enriched_payload.setdefault('branch_id', branch_id or worker_note)
    enriched_payload.setdefault('stop_if', stop_if or [])
    enriched_payload.setdefault('promote_if', promote_if or [])
    enriched_payload.setdefault('disconfirm_if', disconfirm_if or [])
    if task_type == 'workflow':
        config = read_json(ROOT / enriched_payload['config_path'])
        fingerprint = f"workflow::{config_fingerprint(config)}::{enriched_payload['output_dir']}"
    elif task_type == 'generated_batch':
        batch = read_json(ROOT / enriched_payload['batch_path'])
        fingerprint = f"generated_batch::{config_fingerprint(batch)}::{enriched_payload['output_dir']}"
    elif task_type == 'diagnostic':
        fingerprint = f"diagnostic::{enriched_payload['diagnostic_type']}::{json.dumps(enriched_payload, ensure_ascii=False, sort_keys=True)}"
    else:
        fingerprint = f"{task_type}::{json.dumps(enriched_payload, ensure_ascii=False, sort_keys=True)}"
    return {
        'task_type': task_type,
        'category': category,
        'priority_hint': priority_hint,
        'reason': reason,
        'goal': enriched_payload['goal'],
        'hypothesis': enriched_payload['hypothesis'],
        'branch_id': enriched_payload['branch_id'],
        'expected_knowledge_gain': expected_knowledge_gain,
        'stop_if': enriched_payload['stop_if'],
        'promote_if': enriched_payload['promote_if'],
        'disconfirm_if': enriched_payload['disconfirm_if'],
        'payload': enriched_payload,
        'fingerprint': fingerprint,
        'worker_note': worker_note,
    }


def build_window_task(level: int, latest_run: dict[str, Any], end_date: str, base_config: dict[str, Any], existing_fingerprints: set[str], generated_configs: set[str]) -> list[dict[str, Any]]:
    spec = WINDOW_LEVEL_SPECS.get(level)
    if not spec:
        return []
    name, start_date, output_dir, worker_note = spec
    config = deepcopy(base_config)
    config['start_date'] = start_date
    config['end_date'] = end_date
    config['output_dir'] = output_dir
    config_path = write_generated_config(config, name)
    task = make_task(
        'workflow',
        'baseline',
        level_priority('window_expansion', level),
        f"当前已覆盖到 {latest_run.get('config_path', 'base workflow')}，建议继续拓宽历史窗口 {start_date} → {end_date}。",
        ['window_stability_check'],
        {'config_path': config_path, 'output_dir': output_dir},
        worker_note,
        goal='validate_long_horizon_stability',
        hypothesis=f'更长历史窗口 {start_date} → {end_date} 下，当前强候选的排序与稳健性不会明显崩塌。',
        branch_id=f'window_expansion_level_{level}',
        stop_if=['long_horizon_window_shows_no_incremental_gain_twice'],
        promote_if=['long_horizon_window_confirms_candidate_ordering'],
        disconfirm_if=['top_candidates_drop_out_across_long_horizon_window'],
    )
    return [] if task['fingerprint'] in existing_fingerprints else [task]


def build_recent_validation_task(level: int, latest_run: dict[str, Any], end_date: str, base_config: dict[str, Any], existing_fingerprints: set[str], generated_configs: set[str]) -> list[dict[str, Any]]:
    spec = RECENT_WINDOW_LEVEL_SPECS.get(level)
    if not spec:
        return []
    name, start_date, output_dir, worker_note = spec
    config = deepcopy(base_config)
    config['start_date'] = start_date
    config['end_date'] = end_date
    config['output_dir'] = output_dir
    config_path = write_generated_config(config, name)
    task = make_task(
        'workflow',
        'validation',
        level_priority('recent_window_validation', level),
        f"当前已覆盖到 {latest_run.get('config_path', 'base workflow')}，建议继续拓宽近期窗口 {start_date} → {end_date}。",
        ['window_stability_check'],
        {'config_path': config_path, 'output_dir': output_dir},
        worker_note,
        goal='validate_recent_window_stability',
        hypothesis=f'近期窗口 {start_date} → {end_date} 仍能支持当前候选，不是只在更短窗口偶然有效。',
        branch_id=f'recent_window_validation_level_{level}',
        stop_if=['recent_window_validation_fails_twice'],
        promote_if=['recent_window_confirms_candidate_survival'],
        disconfirm_if=['recent_window_eliminates_current_candidates'],
    )
    return [] if task['fingerprint'] in existing_fingerprints else [task]


def build_stable_candidate_task(level: int, stable_candidates: list[str], existing_fingerprints: set[str]) -> list[dict[str, Any]]:
    if not stable_candidates:
        return []
    diagnostic_type = stable_candidate_task_name(level)
    gain = [stable_candidate_gain_name(level)]
    worker_note = stable_candidate_worker_note(level)
    reason = f'稳定候选当前已完成到第 {level-1} 层，建议进入第 {level} 层验证。'
    payload = {
        'diagnostic_type': diagnostic_type,
        'focus_factors': stable_candidates,
        'reasons': ['stable_candidates_need_deeper_validation'],
        'knowledge_gain': gain,
        'source_output_dir': 'artifacts/tushare_batch',
    }
    task = make_task(
        'diagnostic',
        'validation',
        level_priority('stable_candidate_validation', level),
        reason,
        gain,
        payload,
        worker_note,
        goal='validate_stable_candidates',
        hypothesis='当前稳定候选在更深一层诊断下依然成立，而不是被 cluster/窗口偶然性抬高。',
        branch_id=f'stable_candidate_validation_level_{level}',
        stop_if=['stable_candidate_validation_fails_in_two_more_levels'],
        promote_if=['stable_candidate_validation_confirms_cross_window_robustness'],
        disconfirm_if=['stable_candidate_validation_reclassifies_candidates_as_fragile'],
    )
    return [] if task['fingerprint'] in existing_fingerprints else [task]


def build_graveyard_task(level: int, latest_graveyard: list[str], existing_fingerprints: set[str]) -> list[dict[str, Any]]:
    if not latest_graveyard:
        return []
    diagnostic_type = graveyard_task_name(level)
    gain = [graveyard_gain_name(level)]
    worker_note = graveyard_worker_note(level)
    reason = f'graveyard 诊断当前已完成到第 {level-1} 层，建议进入第 {level} 层。'
    payload = {
        'diagnostic_type': diagnostic_type,
        'focus_factors': latest_graveyard,
        'reasons': ['recent_graveyard_needs_deeper_review'],
        'knowledge_gain': gain,
        'source_output_dir': 'artifacts/tushare_batch',
    }
    task = make_task(
        'diagnostic',
        'validation',
        level_priority('graveyard_diagnosis', level),
        reason,
        gain,
        payload,
        worker_note,
        goal='diagnose_graveyard_failures',
        hypothesis='graveyard 中的失败因子包含可解释的结构性失败模式，而不是随机噪声。',
        branch_id=f'graveyard_diagnosis_level_{level}',
        stop_if=['graveyard_diagnosis_finds_no_new_failure_pattern_twice'],
        promote_if=['graveyard_diagnosis_identifies_actionable_failure_pattern'],
        disconfirm_if=['graveyard_members_behave_inconsistently_without_shared_pattern'],
    )
    return [] if task['fingerprint'] in existing_fingerprints else [task]
