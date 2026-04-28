from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

from factor_lab.dedup import config_fingerprint
from factor_lab.factors import resolve_factor_definitions
from factor_lab.generated_artifacts import upgrade_generated_config
from factor_lab.research_families import (
    level_priority,
    stable_candidate_task_name,
    stable_candidate_gain_name,
    stable_candidate_worker_note,
    graveyard_task_name,
    graveyard_gain_name,
    graveyard_worker_note,
)
from factor_lab.storage import ExperimentStore

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

WATCHLIST_WINDOW_LEVEL_SPECS = {
    1: ('watchlist_recent_45d', '2026-02-01', 'artifacts/watchlist_validation', 'validation｜watchlist 45 天晋级赛'),
    2: ('watchlist_recent_90d', '2025-12-18', 'artifacts/watchlist_validation', 'validation｜watchlist 90 天晋级赛'),
    3: ('watchlist_recent_120d', '2025-11-18', 'artifacts/watchlist_validation', 'validation｜watchlist 120 天晋级赛'),
}

MEDIUM_HORIZON_LEVEL_SPECS = {
    1: ('rolling_60d_back', '2025-11-02', 'artifacts/generated_rolling_60d_back', 'validation｜中窗 60 天晋级赛'),
    2: ('rolling_90d_back', '2025-10-03', 'artifacts/generated_rolling_90d_back', 'validation｜中窗 90 天晋级赛'),
    3: ('rolling_120d_back', '2025-09-03', 'artifacts/generated_rolling_120d_back', 'validation｜中窗 120 天晋级赛'),
}


def read_json(path: str | Path) -> dict[str, Any]:
    import json
    text = Path(path).read_text(encoding='utf-8')
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        try:
            obj, _ = json.JSONDecoder().raw_decode(text)
            return obj
        except Exception:
            return {}


def _materialize_factor_config(config: dict[str, Any]) -> dict[str, Any]:
    normalized = deepcopy(config)
    factor_defs = resolve_factor_definitions(normalized, config_dir=ROOT / 'configs')
    if factor_defs:
        normalized['factors'] = factor_defs
    normalized.pop('factor_family_config', None)
    return normalized


def write_generated_config(config: dict[str, Any], name: str) -> str:
    import json
    out_dir = ROOT / 'artifacts' / 'generated_configs'
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f'{name}.json'
    materialized = _materialize_factor_config(config)
    materialized = upgrade_generated_config(materialized, source='research_family_generators')
    path.write_text(json.dumps(materialized, ensure_ascii=False, indent=2), encoding='utf-8')
    return str(path.relative_to(ROOT))


def _append_focus_candidate_definitions(config: dict[str, Any], focus_candidates: list[str]) -> dict[str, Any]:
    if not focus_candidates:
        return config
    store = ExperimentStore(ROOT / 'artifacts' / 'factor_lab.db')
    candidate_rows = {row.get('name'): row for row in store.list_factor_candidates(limit=5000) if row.get('name')}
    merged_factors = list(config.get('factors') or [])
    existing_names = {row.get('name') for row in merged_factors if row.get('name')}
    for name in focus_candidates:
        if not name or name in existing_names:
            continue
        row = candidate_rows.get(name) or {}
        definition = dict(row.get('definition') or {})
        expression = definition.get('expression') or row.get('expression')
        if not expression:
            continue
        definition.setdefault('name', name)
        definition['expression'] = expression
        merged_factors.append(definition)
        existing_names.add(name)
    config['factors'] = merged_factors
    return config


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


def build_medium_horizon_task(level: int, focus_candidates: list[str], latest_run: dict[str, Any], end_date: str, base_config: dict[str, Any], existing_fingerprints: set[str], generated_configs: set[str]) -> list[dict[str, Any]]:
    if not focus_candidates:
        return []
    spec = MEDIUM_HORIZON_LEVEL_SPECS.get(level)
    if not spec:
        return []
    name, start_date, output_dir, worker_note = spec
    focus_slug = '_'.join(sorted(focus_candidates)[:3]).replace('-', '_')
    config_name = f"medium_horizon__{name}__{focus_slug}".strip('_')
    scoped_output_dir = f"artifacts/medium_horizon_validation/{config_name}"
    config = deepcopy(base_config)
    config['start_date'] = start_date
    config['end_date'] = end_date
    config['output_dir'] = scoped_output_dir
    config = _append_focus_candidate_definitions(config, focus_candidates)
    config_path = write_generated_config(config, config_name)
    task = make_task(
        'workflow',
        'validation',
        level_priority('medium_horizon_validation', level),
        f"soft robust 候选需要在更长的中窗 {start_date} → {end_date} 里跑晋级赛，确认它们不只是 30d/45d 幻觉。",
        ['medium_horizon_promotion_check'],
        {
            'config_path': config_path,
            'output_dir': scoped_output_dir,
            'focus_factors': focus_candidates,
        },
        worker_note,
        goal='validate_medium_horizon_stability',
        hypothesis=f'当前 soft robust 候选在 {start_date} → {end_date} 中窗里仍能保持候选资格，而不是只在更短窗口有效。',
        branch_id=f'medium_horizon_validation_level_{level}',
        stop_if=['medium_horizon_window_eliminates_soft_robust_candidates_twice'],
        promote_if=['medium_horizon_window_confirms_soft_robust_candidate_survival'],
        disconfirm_if=['soft_robust_candidates_fail_entire_medium_horizon_window'],
    )
    return [] if task['fingerprint'] in existing_fingerprints else [task]


def build_watchlist_candidate_task(level: int, watchlist_candidates: list[str], latest_run: dict[str, Any], end_date: str, base_config: dict[str, Any], existing_fingerprints: set[str], generated_configs: set[str]) -> list[dict[str, Any]]:
    if not watchlist_candidates:
        return []
    spec = WATCHLIST_WINDOW_LEVEL_SPECS.get(level)
    if not spec:
        return []
    name, start_date, output_root, worker_note = spec
    focus_slug = '_'.join(sorted(watchlist_candidates)[:3]).replace('-', '_')
    config_name = f"{name}__{focus_slug}".strip('_')
    scoped_output_dir = f"{output_root}/{config_name}"
    config = deepcopy(base_config)
    config['start_date'] = start_date
    config['end_date'] = end_date
    config['output_dir'] = scoped_output_dir
    config = _append_focus_candidate_definitions(config, watchlist_candidates)
    config_path = write_generated_config(config, config_name)
    task = make_task(
        'workflow',
        'validation',
        level_priority('watchlist_candidate_validation', level),
        f"watchlist 候选需要在 {start_date} → {end_date} 的递进窗口里跑晋级赛，确认它们能否从观察名单升到 candidate。",
        ['watchlist_candidate_promoted', 'candidate_survival_check'],
        {
            'config_path': config_path,
            'output_dir': scoped_output_dir,
            'focus_factors': watchlist_candidates,
        },
        worker_note,
        goal='promote_watchlist_candidates',
        hypothesis=f"当前 watchlist 候选在 {start_date} → {end_date} 里仍能保持正向质量，而不是只停留在短窗观察名单。",
        branch_id=f'watchlist_candidate_validation_level_{level}',
        stop_if=['watchlist_candidates_fail_progressive_validation_twice'],
        promote_if=['watchlist_candidates_survive_progressive_validation'],
        disconfirm_if=['watchlist_candidates_collapse_outside_short_window'],
    )
    return [] if task['fingerprint'] in existing_fingerprints else [task]


def build_fragile_candidate_task(level: int, fragile_candidates: list[str], existing_fingerprints: set[str]) -> list[dict[str, Any]]:
    if not fragile_candidates:
        return []
    worker_note = "validation｜fragile 候选加固"
    reason = f'fragile 候选当前需要第 {level} 层专项加固验证，避免长期卡在短窗有效 / 中窗脆弱状态。'
    payload = {
        'diagnostic_type': f'fragile_candidate_hardening_v{level}',
        'focus_factors': fragile_candidates,
        'reasons': ['fragile_candidates_need_hardening'],
        'knowledge_gain': ['candidate_survival_check', 'stable_candidate_confirmed'],
        'source_output_dir': 'artifacts/tushare_batch',
    }
    task = make_task(
        'diagnostic',
        'validation',
        level_priority('fragile_candidate_hardening', level),
        reason,
        ['candidate_survival_check', 'stable_candidate_confirmed'],
        payload,
        worker_note,
        goal='harden_fragile_candidates',
        hypothesis='当前 fragile 候选里至少有一部分并非纯短窗噪声，经过专项加固验证后可向 stable/candidate 晋升。',
        branch_id=f'fragile_candidate_hardening_level_{level}',
        stop_if=['fragile_candidates_fail_hardening_twice'],
        promote_if=['fragile_candidates_survive_hardening_validation'],
        disconfirm_if=['fragile_candidates_collapse_after_hardening'],
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
